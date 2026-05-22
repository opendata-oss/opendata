//! Recall benchmark for the vector database.
//!
//! Splits work into three phases — [`Phase::Ingest`], [`Phase::Cold`], and
//! [`Phase::Warm`]. The phases to run are configured per-dataset via the
//! [`Dataset::phases`] field and can be overridden through the bencher
//! config's `[params.recall]` section. Each phase opens its own fresh
//! `VectorDb` or `VectorDbReader` and closes it before the next phase starts.

mod cold;
mod ingest;
mod warm;

use bencher::{Bench, Benchmark, Params, Summary};
use common::storage::config::SlateDbStorageConfig;
use common::storage::factory::{
    CachedEntry, CachedKey, DbCache, FoyerCache, FoyerCacheOptions, FoyerHybridCache,
};
use common::{
    StorageBuilder, StorageConfig, StorageError, StorageReaderRuntime, create_object_store,
};
use foyer::HybridCachePolicy;
use std::fs::File;
use std::io::{self, BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use tokio::sync::mpsc;
use vector::{Config, DistanceMetric, ReaderConfig, VectorDb};

const DEFAULT_NUM_QUERIES: usize = 100;
/// Default number of queries to run during the cold phase. Queries are
/// cycled if the dataset has fewer loaded queries than this.
const DEFAULT_NUM_COLD_QUERIES: usize = 1000;
/// Default number of concurrent queries during the warm query phase.
const DEFAULT_QUERY_CONCURRENCY: usize = 1;
/// Default ceiling on queries-per-second during the warm query phase.
const DEFAULT_QUERY_QPS_LIMIT: usize = 32;

/// Default phase list when a dataset doesn't specify otherwise.
const DEFAULT_PHASES: &[Phase] = &[Phase::Ingest, Phase::Warm, Phase::Cold];

/// Default on-disk path for the hybrid-cache disk tier. Only used when
/// `block_cache_disk_bytes` is set.
const DEFAULT_BLOCK_CACHE_DISK_PATH: &str = "/mnt/cache/foyer";

/// Default fraction of total system memory used to size the in-memory
/// block cache during the [`Phase::Ingest`] phase when
/// [`Dataset::block_cache_bytes`] is not explicitly set. Ingest needs
/// headroom for write-side state (write coordinator delta + frozen views,
/// slatedb memtables, centroid index updates), so the default is
/// relatively low.
const INGEST_BLOCK_CACHE_MEMORY_FRACTION: f64 = 0.25;

/// Default fraction of total system memory used to size the in-memory
/// block cache during the [`Phase::Warm`] (and [`Phase::Cold`]) phase
/// when [`Dataset::block_cache_bytes`] is not explicitly set. The
/// read-only paths can claim the majority of memory.
const WARM_BLOCK_CACHE_MEMORY_FRACTION: f64 = 2.0 / 3.0;

fn load_vector_config(path: &str) -> Config {
    let contents =
        std::fs::read_to_string(path).unwrap_or_else(|e| panic!("failed to read {}: {}", path, e));
    let config: Config = serde_yaml::from_str(&contents)
        .unwrap_or_else(|e| panic!("failed to parse {}: {}", path, e));
    println!("  Vector config: {:?}", config);
    config
}

fn load_storage_config(path: &str) -> StorageConfig {
    let contents =
        std::fs::read_to_string(path).unwrap_or_else(|e| panic!("failed to read {}: {}", path, e));
    serde_yaml::from_str(&contents).unwrap_or_else(|e| panic!("failed to parse {}: {}", path, e))
}

fn data_dir() -> PathBuf {
    // When running from workspace root via `cargo run -p vector-bench`,
    // CARGO_MANIFEST_DIR points to vector/bench. Data is at vector/bench/data.
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("bench dir has parent")
        .join("bench/data")
}

/// Normalize a vector to the unit hypersphere.
fn normalize_vec(v: &mut [f32]) {
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        v.iter_mut().for_each(|x| *x /= norm);
    }
}

// -- Phases -------------------------------------------------------------------

/// A single phase of the recall benchmark.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    /// Ingest a dataset's base vectors into a fresh `VectorDb`.
    Ingest,
    /// Run a small number of queries against a fresh `VectorDbReader` to
    /// measure cold-start latency.
    Cold,
    /// Warm up a `VectorDb` and then run the rate-limited concurrent query
    /// workload that produces the headline recall/latency metrics.
    Warm,
}

impl Phase {
    fn as_str(self) -> &'static str {
        match self {
            Phase::Ingest => "INGEST",
            Phase::Cold => "COLD",
            Phase::Warm => "WARM",
        }
    }

    fn parse(s: &str) -> Self {
        match s {
            "INGEST" => Phase::Ingest,
            "COLD" => Phase::Cold,
            "WARM" => Phase::Warm,
            _ => panic!("unknown phase: {}", s),
        }
    }
}

fn phases_to_param(phases: &[Phase]) -> String {
    phases
        .iter()
        .map(|p| p.as_str())
        .collect::<Vec<_>>()
        .join(",")
}

fn param_to_phases(s: &str) -> Vec<Phase> {
    s.split(',')
        .filter(|s| !s.is_empty())
        .map(Phase::parse)
        .collect()
}

// -- Vector file readers ------------------------------------------------------

pub(crate) struct VectorFileBatchReader {
    reader: BufReader<File>,
    format: VecFormat,
    dimensions: usize,
    remaining: Option<usize>,
    normalize: bool,
}

impl VectorFileBatchReader {
    fn open(
        path: &Path,
        format: VecFormat,
        dimensions: usize,
        max_vectors: Option<usize>,
        normalize: bool,
    ) -> anyhow::Result<Self> {
        let file =
            File::open(path).unwrap_or_else(|e| panic!("failed to open {}: {}", path.display(), e));
        Ok(Self {
            reader: BufReader::new(file),
            format,
            dimensions,
            remaining: max_vectors,
            normalize,
        })
    }

    fn read_batch(&mut self, max_rows: usize) -> anyhow::Result<Option<Vec<Vec<f32>>>> {
        if self.remaining == Some(0) {
            return Ok(None);
        }

        let batch_cap = self.remaining.unwrap_or(max_rows).min(max_rows);
        let mut rows = Vec::with_capacity(batch_cap);
        while rows.len() < batch_cap {
            let Some(mut values) = self.read_vector()? else {
                break;
            };
            if self.normalize {
                normalize_vec(&mut values);
            }
            rows.push(values);
            if let Some(remaining) = &mut self.remaining {
                *remaining -= 1;
                if *remaining == 0 {
                    break;
                }
            }
        }

        if rows.is_empty() {
            Ok(None)
        } else {
            Ok(Some(rows))
        }
    }

    fn read_vector(&mut self) -> anyhow::Result<Option<Vec<f32>>> {
        let Some(dim) = read_dim_prefix(&mut self.reader)? else {
            return Ok(None);
        };
        if dim != self.dimensions {
            anyhow::bail!(
                "dimension mismatch while streaming vectors: expected {}, got {}",
                self.dimensions,
                dim
            );
        }

        match self.format {
            VecFormat::Fvecs => {
                let mut values = vec![0f32; dim];
                let byte_slice = unsafe {
                    std::slice::from_raw_parts_mut(values.as_mut_ptr() as *mut u8, dim * 4)
                };
                self.reader.read_exact(byte_slice)?;
                Ok(Some(values))
            }
            VecFormat::Bvecs => {
                let mut bytes = vec![0u8; dim];
                self.reader.read_exact(&mut bytes)?;
                Ok(Some(bytes.into_iter().map(|v| v as f32).collect()))
            }
        }
    }
}

fn read_dim_prefix(reader: &mut impl Read) -> anyhow::Result<Option<usize>> {
    let mut dim_buf = [0u8; 4];
    match reader.read_exact(&mut dim_buf) {
        Ok(()) => Ok(Some(u32::from_le_bytes(dim_buf) as usize)),
        Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => Ok(None),
        Err(err) => Err(err.into()),
    }
}

fn read_fvecs(path: &Path) -> Vec<Vec<f32>> {
    let file =
        File::open(path).unwrap_or_else(|e| panic!("failed to open {}: {}", path.display(), e));
    let mut reader = BufReader::new(file);
    let mut vectors = Vec::new();
    let mut dim_buf = [0u8; 4];

    while reader.read_exact(&mut dim_buf).is_ok() {
        let dim = u32::from_le_bytes(dim_buf) as usize;
        let mut values = vec![0f32; dim];
        let byte_slice =
            unsafe { std::slice::from_raw_parts_mut(values.as_mut_ptr() as *mut u8, dim * 4) };
        reader
            .read_exact(byte_slice)
            .expect("failed to read fvecs values");
        vectors.push(values);
    }

    vectors
}

fn read_ivecs(path: &Path) -> Vec<Vec<i32>> {
    let file =
        File::open(path).unwrap_or_else(|e| panic!("failed to open {}: {}", path.display(), e));
    let mut reader = BufReader::new(file);
    let mut vectors = Vec::new();
    let mut dim_buf = [0u8; 4];

    while reader.read_exact(&mut dim_buf).is_ok() {
        let dim = u32::from_le_bytes(dim_buf) as usize;
        let mut values = vec![0i32; dim];
        let byte_slice =
            unsafe { std::slice::from_raw_parts_mut(values.as_mut_ptr() as *mut u8, dim * 4) };
        reader
            .read_exact(byte_slice)
            .expect("failed to read ivecs values");
        vectors.push(values);
    }

    vectors
}

/// Read bvecs format (BigANN). Each record: 4-byte dimension (i32 LE)
/// followed by `dim` unsigned bytes. Values are converted to f32. If
/// `limit` is `Some(n)`, stops after reading `n` vectors.
fn read_bvecs(path: &Path, limit: Option<usize>) -> Vec<Vec<f32>> {
    let file =
        File::open(path).unwrap_or_else(|e| panic!("failed to open {}: {}", path.display(), e));
    let mut reader = BufReader::new(file);
    let mut vectors = Vec::new();
    let mut dim_buf = [0u8; 4];

    while reader.read_exact(&mut dim_buf).is_ok() {
        if let Some(n) = limit
            && vectors.len() >= n
        {
            break;
        }
        let dim = u32::from_le_bytes(dim_buf) as usize;
        let mut bytes = vec![0u8; dim];
        reader
            .read_exact(&mut bytes)
            .expect("failed to read bvecs values");
        let values: Vec<f32> = bytes.iter().map(|&b| b as f32).collect();
        vectors.push(values);
    }

    vectors
}

// -- Percentile helper --------------------------------------------------------

pub(crate) fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = (p / 100.0 * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

// -- Dataset descriptors ------------------------------------------------------

#[derive(Clone, Copy)]
pub(crate) enum VecFormat {
    Fvecs,
    Bvecs,
}

/// Dataset descriptor.
#[derive(Clone)]
pub(crate) struct Dataset {
    pub name: &'static str,
    pub dimensions: u16,
    pub distance_metric: DistanceMetric,
    pub base_file: &'static str,
    pub query_file: &'static str,
    pub ground_truth_file: &'static str,
    pub split_threshold: usize,
    pub merge_threshold: usize,
    pub query_pruning_factor: Option<f32>,
    pub nprobe: usize,
    pub num_queries: usize,
    /// Number of queries to run during the cold phase. Defaults to
    /// [`DEFAULT_NUM_COLD_QUERIES`]. Queries are cycled when this exceeds
    /// the number of loaded queries.
    pub num_cold_queries: usize,
    /// In-memory block-cache size in bytes.
    /// - `None`: derive from the phase-specific default (e.g.,
    ///   [`INGEST_BLOCK_CACHE_MEMORY_FRACTION`] of total system memory for
    ///   ingest, [`WARM_BLOCK_CACHE_MEMORY_FRACTION`] for warm/cold).
    /// - `Some(-1)` (or any negative value): disable the block cache.
    /// - `Some(n)` with `n >= 0`: use `n` bytes for the memory tier.
    ///
    /// When the cache is enabled and [`Dataset::block_cache_disk_bytes`]
    /// is `None`, the bench uses a memory-only foyer cache; when both
    /// are set the bench uses a hybrid memory + disk cache.
    pub block_cache_bytes: Option<i64>,
    /// On-disk block-cache size in bytes. `Some` opts into the hybrid
    /// (memory + disk) foyer cache, with this controlling the disk tier
    /// capacity and [`Dataset::block_cache_disk_path`] controlling its
    /// location. Ignored unless [`Dataset::block_cache_bytes`] is also
    /// `Some`.
    pub block_cache_disk_bytes: Option<u64>,
    /// On-disk path for the hybrid cache's disk tier. Defaults to
    /// [`DEFAULT_BLOCK_CACHE_DISK_PATH`]. Unused unless
    /// [`Dataset::block_cache_disk_bytes`] is `Some`.
    pub block_cache_disk_path: &'static str,
    /// Directory containing the dataset files. When `None`, falls back to
    /// the default data directory resolved from `CARGO_MANIFEST_DIR`.
    pub data_dir: Option<String>,
    /// Path to a YAML file with vector `Config` overrides.
    pub vector_config: Option<String>,
    /// Optional path to a YAML file with a separate StorageConfig for the
    /// cold-reader phase. When both writer and reader storage are SlateDb,
    /// the reader uses the writer's data path/object store and the
    /// override's settings/cache.
    pub reader_storage_config: Option<String>,
    /// File format for base and query vectors.
    pub format: VecFormat,
    /// Maximum number of base vectors to ingest. `None` = all.
    pub max_vectors: Option<usize>,
    pub normalize: bool,
    /// Number of queries allowed in flight concurrently during the warm
    /// query phase. Default: `DEFAULT_QUERY_CONCURRENCY`.
    pub query_concurrency: usize,
    /// Ceiling on queries-per-second during the warm query phase.
    /// Default: `DEFAULT_QUERY_QPS_LIMIT`.
    pub query_qps_limit: usize,
    /// Phases to run, in order. Each phase opens a fresh `VectorDb` /
    /// `VectorDbReader` and closes it before the next phase starts.
    pub phases: &'static [Phase],
}

impl Dataset {
    fn base_path(&self, data_dir: &Path) -> PathBuf {
        data_dir.join(self.base_file)
    }

    fn query_path(&self, data_dir: &Path) -> PathBuf {
        data_dir.join(self.query_file)
    }

    pub(crate) fn estimated_base_vector_count(&self, data_dir: &Path) -> anyhow::Result<usize> {
        let path = self.base_path(data_dir);
        let bytes = std::fs::metadata(&path)?.len() as usize;
        let record_size = match self.format {
            VecFormat::Fvecs => 4 + self.dimensions as usize * 4,
            VecFormat::Bvecs => 4 + self.dimensions as usize,
        };
        if !bytes.is_multiple_of(record_size) {
            anyhow::bail!(
                "file size {} for {} is not a multiple of record size {}",
                bytes,
                path.display(),
                record_size
            );
        }
        let total = bytes / record_size;
        Ok(self.max_vectors.map_or(total, |n| n.min(total)))
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn spawn_base_vector_stream(
        &self,
        data_dir: &Path,
        batch_size: usize,
    ) -> anyhow::Result<(
        mpsc::Receiver<anyhow::Result<Option<Vec<Vec<f32>>>>>,
        thread::JoinHandle<()>,
    )> {
        let path = self.base_path(data_dir);
        let format = self.format;
        let dimensions = self.dimensions as usize;
        let max_vectors = self.max_vectors;
        let normalize = self.normalize;
        let (tx, rx) = mpsc::channel(1);

        let handle = thread::spawn(move || {
            let mut reader = match VectorFileBatchReader::open(
                &path,
                format,
                dimensions,
                max_vectors,
                normalize,
            ) {
                Ok(reader) => reader,
                Err(err) => {
                    let _ = tx.blocking_send(Err(err));
                    return;
                }
            };

            loop {
                match reader.read_batch(batch_size) {
                    Ok(Some(batch)) => {
                        if tx.blocking_send(Ok(Some(batch))).is_err() {
                            return;
                        }
                    }
                    Ok(None) => {
                        let _ = tx.blocking_send(Ok(None));
                        return;
                    }
                    Err(err) => {
                        let _ = tx.blocking_send(Err(err));
                        return;
                    }
                }
            }
        });

        Ok((rx, handle))
    }

    fn resolve_reader_storage_config(
        &self,
        writer_storage: &StorageConfig,
    ) -> anyhow::Result<StorageConfig> {
        let Some(path) = &self.reader_storage_config else {
            return Ok(writer_storage.clone());
        };

        let override_storage = load_storage_config(path);
        match (writer_storage, override_storage) {
            (StorageConfig::SlateDb(writer), StorageConfig::SlateDb(reader)) => {
                Ok(StorageConfig::SlateDb(SlateDbStorageConfig {
                    path: writer.path.clone(),
                    object_store: writer.object_store.clone(),
                    settings_path: reader.settings_path,
                    block_cache: reader.block_cache,
                }))
            }
            (_, storage) => Ok(storage),
        }
    }

    fn load_query_vectors(&self, data_dir: &Path) -> Vec<Vec<f32>> {
        let path = self.query_path(data_dir);
        match self.format {
            VecFormat::Fvecs => read_fvecs(&path)
                .into_iter()
                .take(self.num_queries)
                .collect(),
            VecFormat::Bvecs => read_bvecs(&path, Some(self.num_queries)),
        }
    }
}

fn distance_metric_to_str(m: DistanceMetric) -> &'static str {
    match m {
        DistanceMetric::L2 => "l2",
        DistanceMetric::Cosine => "cosine",
        DistanceMetric::DotProduct => "dot_product",
    }
}

fn str_to_distance_metric(s: &str) -> DistanceMetric {
    match s {
        "l2" => DistanceMetric::L2,
        "cosine" => DistanceMetric::Cosine,
        "dot_product" => DistanceMetric::DotProduct,
        _ => panic!("unknown distance metric: {}", s),
    }
}

fn format_to_str(f: VecFormat) -> &'static str {
    match f {
        VecFormat::Fvecs => "fvecs",
        VecFormat::Bvecs => "bvecs",
    }
}

fn str_to_format(s: &str) -> VecFormat {
    match s {
        "fvecs" => VecFormat::Fvecs,
        "bvecs" => VecFormat::Bvecs,
        _ => panic!("unknown format: {}", s),
    }
}

impl From<&Dataset> for Params {
    fn from(d: &Dataset) -> Self {
        let mut p = Params::new();
        p.insert("dataset", d.name);
        p.insert("dimensions", d.dimensions.to_string());
        p.insert("distance_metric", distance_metric_to_str(d.distance_metric));
        p.insert("base_file", d.base_file);
        p.insert("query_file", d.query_file);
        p.insert("ground_truth_file", d.ground_truth_file);
        p.insert("split_threshold", d.split_threshold.to_string());
        p.insert("merge_threshold", d.merge_threshold.to_string());
        p.insert("nprobe", d.nprobe.to_string());
        p.insert("num_queries", d.num_queries.to_string());
        p.insert("num_cold_queries", d.num_cold_queries.to_string());
        p.insert("query_concurrency", d.query_concurrency.to_string());
        p.insert("query_qps_limit", d.query_qps_limit.to_string());
        p.insert("format", format_to_str(d.format));
        p.insert("phases", phases_to_param(d.phases));
        if let Some(max) = d.max_vectors {
            p.insert("max_vectors", max.to_string());
        }
        if let Some(bytes) = d.block_cache_bytes {
            p.insert("block_cache_bytes", bytes.to_string());
        }
        // No insertion when None — Params absence carries the "use phase
        // default" meaning; `-1` is the explicit "disabled" sentinel.
        if let Some(bytes) = d.block_cache_disk_bytes {
            p.insert("block_cache_disk_bytes", bytes.to_string());
        }
        if d.block_cache_disk_path != DEFAULT_BLOCK_CACHE_DISK_PATH {
            p.insert("block_cache_disk_path", d.block_cache_disk_path);
        }
        if let Some(ref dir) = d.data_dir {
            p.insert("data_dir", dir.clone());
        }
        if let Some(ref path) = d.vector_config {
            p.insert("vector_config", path.clone());
        }
        if let Some(ref path) = d.reader_storage_config {
            p.insert("reader_storage_config", path.clone());
        }
        p
    }
}

impl From<Params> for Dataset {
    fn from(p: Params) -> Self {
        let name = p
            .get("dataset")
            .expect("params missing 'dataset' field")
            .to_string();
        let default = lookup_dataset(&name).unwrap_or_else(|| panic!("unknown dataset: {}", name));

        let query_pruning_factor = p
            .get_parse("query_pruning_factor")
            .ok()
            .or(default.query_pruning_factor)
            .filter(|f| *f > 0.0);
        // `block_cache_bytes` is now `Option<i64>` where `None` means
        // "use the phase default", `Some(-1)` (or any negative) means
        // "disabled", and `Some(n >= 0)` is an explicit byte count.
        let block_cache_bytes = p
            .get_parse::<i64>("block_cache_bytes")
            .ok()
            .or(default.block_cache_bytes);
        let block_cache_disk_bytes = p
            .get_parse::<i64>("block_cache_disk_bytes")
            .ok()
            .or(default.block_cache_disk_bytes.map(|v| v as i64))
            .filter(|b| *b > 0)
            .map(|b| b as u64);
        // `block_cache_disk_path` is stored as `&'static str` for const
        // initialisation, so user overrides are leaked the same way `phases`
        // is. The default path is reused as the static slice when no override
        // is set.
        let block_cache_disk_path: &'static str = match p.get("block_cache_disk_path") {
            Some(s) => Box::leak(s.to_string().into_boxed_str()),
            None => default.block_cache_disk_path,
        };

        // `phases` round-trips through Params as a comma-separated string,
        // but the Dataset struct stores `&'static [Phase]`. We allocate a
        // leaked slice when the user overrides the default; otherwise we
        // reuse the default dataset's static slice.
        let phases: &'static [Phase] = match p.get("phases") {
            Some(s) => {
                let parsed = param_to_phases(s);
                Box::leak(parsed.into_boxed_slice())
            }
            None => default.phases,
        };

        Dataset {
            name: default.name,
            dimensions: p.get_parse("dimensions").unwrap_or(default.dimensions),
            distance_metric: p
                .get("distance_metric")
                .map(str_to_distance_metric)
                .unwrap_or(default.distance_metric),
            base_file: default.base_file,
            query_file: default.query_file,
            ground_truth_file: default.ground_truth_file,
            split_threshold: p
                .get_parse("split_threshold")
                .unwrap_or(default.split_threshold),
            merge_threshold: p
                .get_parse("merge_threshold")
                .unwrap_or(default.merge_threshold),
            query_pruning_factor,
            nprobe: p.get_parse("nprobe").unwrap_or(default.nprobe),
            num_queries: p.get_parse("num_queries").unwrap_or(default.num_queries),
            num_cold_queries: p
                .get_parse("num_cold_queries")
                .unwrap_or(default.num_cold_queries),
            query_concurrency: p
                .get_parse("query_concurrency")
                .unwrap_or(default.query_concurrency),
            query_qps_limit: p
                .get_parse("query_qps_limit")
                .unwrap_or(default.query_qps_limit),
            format: p.get("format").map(str_to_format).unwrap_or(default.format),
            block_cache_bytes,
            block_cache_disk_bytes,
            block_cache_disk_path,
            max_vectors: p.get_parse("max_vectors").ok().or(default.max_vectors),
            data_dir: p
                .get("data_dir")
                .map(|s| s.to_string())
                .or_else(|| default.data_dir.clone()),
            vector_config: p
                .get("vector_config")
                .map(|s| s.to_string())
                .or_else(|| default.vector_config.clone()),
            reader_storage_config: p
                .get("reader_storage_config")
                .map(|s| s.to_string())
                .or_else(|| default.reader_storage_config.clone()),
            normalize: default.normalize,
            phases,
        }
    }
}

const SIFT1M: Dataset = Dataset {
    name: "sift1m",
    dimensions: 128,
    distance_metric: DistanceMetric::L2,
    base_file: "sift/sift_base.fvecs",
    query_file: "sift/sift_query.fvecs",
    ground_truth_file: "sift/sift_groundtruth.ivecs",
    split_threshold: 150,
    merge_threshold: 50,
    nprobe: 15,
    query_pruning_factor: Some(7.0),
    num_queries: DEFAULT_NUM_QUERIES,
    num_cold_queries: DEFAULT_NUM_COLD_QUERIES,
    block_cache_bytes: None,
    block_cache_disk_bytes: None,
    block_cache_disk_path: DEFAULT_BLOCK_CACHE_DISK_PATH,
    data_dir: None,
    vector_config: None,
    reader_storage_config: None,
    format: VecFormat::Fvecs,
    max_vectors: None,
    normalize: false,
    query_concurrency: DEFAULT_QUERY_CONCURRENCY,
    query_qps_limit: DEFAULT_QUERY_QPS_LIMIT,
    phases: DEFAULT_PHASES,
};

// 100K-vector subset of SIFT1M generated by `gen_sift100k_groundtruth`. The
// files live under `vector/tests/data/sift100k` so this dataset is suitable as
// a fast smoke test that doesn't require downloading SIFT1M. The file paths
// are absolute (resolved at compile time via `CARGO_MANIFEST_DIR`) so they
// work regardless of the bench's default data directory.
const SIFT100K: Dataset = Dataset {
    name: "sift100k",
    dimensions: 128,
    distance_metric: DistanceMetric::L2,
    base_file: concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../tests/data/sift100k/base.fvecs"
    ),
    query_file: concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../tests/data/sift100k/query.fvecs"
    ),
    ground_truth_file: concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../tests/data/sift100k/groundtruth.ivecs"
    ),
    split_threshold: 150,
    merge_threshold: 50,
    nprobe: 15,
    query_pruning_factor: Some(7.0),
    num_queries: DEFAULT_NUM_QUERIES,
    num_cold_queries: DEFAULT_NUM_COLD_QUERIES,
    block_cache_bytes: None,
    block_cache_disk_bytes: None,
    block_cache_disk_path: DEFAULT_BLOCK_CACHE_DISK_PATH,
    data_dir: None,
    vector_config: None,
    reader_storage_config: None,
    format: VecFormat::Fvecs,
    max_vectors: None,
    normalize: false,
    query_concurrency: DEFAULT_QUERY_CONCURRENCY,
    query_qps_limit: DEFAULT_QUERY_QPS_LIMIT,
    phases: DEFAULT_PHASES,
};

const COHERE1M: Dataset = Dataset {
    name: "cohere1m",
    dimensions: 768,
    distance_metric: DistanceMetric::Cosine,
    base_file: "cohere/cohere_base.fvecs",
    query_file: "cohere/cohere_query.fvecs",
    ground_truth_file: "cohere/cohere_groundtruth.ivecs",
    split_threshold: 150,
    merge_threshold: 50,
    query_pruning_factor: Some(0.5),
    nprobe: 100,
    num_queries: DEFAULT_NUM_QUERIES,
    num_cold_queries: DEFAULT_NUM_COLD_QUERIES,
    block_cache_bytes: None,
    block_cache_disk_bytes: None,
    block_cache_disk_path: DEFAULT_BLOCK_CACHE_DISK_PATH,
    data_dir: None,
    vector_config: None,
    reader_storage_config: None,
    format: VecFormat::Fvecs,
    max_vectors: None,
    normalize: false,
    query_concurrency: DEFAULT_QUERY_CONCURRENCY,
    query_qps_limit: DEFAULT_QUERY_QPS_LIMIT,
    phases: DEFAULT_PHASES,
};

const DEEP10M: Dataset = Dataset {
    name: "deep10m",
    dimensions: 96,
    distance_metric: DistanceMetric::L2,
    base_file: "deep/deep_base.fvecs",
    query_file: "deep/deep_query.fvecs",
    ground_truth_file: "deep/deep_groundtruth_10M.ivecs",
    split_threshold: 150,
    merge_threshold: 50,
    query_pruning_factor: Some(0.5),
    nprobe: 100,
    num_queries: DEFAULT_NUM_QUERIES,
    num_cold_queries: DEFAULT_NUM_COLD_QUERIES,
    block_cache_bytes: None,
    block_cache_disk_bytes: None,
    block_cache_disk_path: DEFAULT_BLOCK_CACHE_DISK_PATH,
    data_dir: None,
    vector_config: None,
    reader_storage_config: None,
    format: VecFormat::Fvecs,
    max_vectors: Some(10_000_000),
    normalize: false,
    query_concurrency: DEFAULT_QUERY_CONCURRENCY,
    query_qps_limit: DEFAULT_QUERY_QPS_LIMIT,
    phases: DEFAULT_PHASES,
};

const DEEP1B: Dataset = Dataset {
    name: "deep1b",
    dimensions: 96,
    distance_metric: DistanceMetric::L2,
    base_file: "deep/deep_base.fvecs",
    query_file: "deep/deep_query.fvecs",
    ground_truth_file: "deep/deep_groundtruth_1B.ivecs",
    split_threshold: 150,
    merge_threshold: 50,
    query_pruning_factor: Some(0.5),
    nprobe: 100,
    num_queries: DEFAULT_NUM_QUERIES,
    num_cold_queries: DEFAULT_NUM_COLD_QUERIES,
    block_cache_bytes: None,
    block_cache_disk_bytes: None,
    block_cache_disk_path: DEFAULT_BLOCK_CACHE_DISK_PATH,
    data_dir: None,
    vector_config: None,
    reader_storage_config: None,
    format: VecFormat::Fvecs,
    max_vectors: None,
    normalize: false,
    query_concurrency: DEFAULT_QUERY_CONCURRENCY,
    query_qps_limit: DEFAULT_QUERY_QPS_LIMIT,
    phases: DEFAULT_PHASES,
};

const WIKIPEDIA_BGE_M3_EN: Dataset = Dataset {
    name: "wikipedia_bge_m3_en",
    dimensions: 1024,
    distance_metric: DistanceMetric::DotProduct,
    base_file: "wikipedia-bge-m3/en/base.fvecs",
    query_file: "wikipedia-bge-m3/en/query.fvecs",
    ground_truth_file: "wikipedia-bge-m3/en/groundtruth.ivecs",
    split_threshold: 150,
    merge_threshold: 50,
    query_pruning_factor: Some(0.5),
    nprobe: 100,
    num_queries: DEFAULT_NUM_QUERIES,
    num_cold_queries: DEFAULT_NUM_COLD_QUERIES,
    block_cache_bytes: None,
    block_cache_disk_bytes: None,
    block_cache_disk_path: DEFAULT_BLOCK_CACHE_DISK_PATH,
    data_dir: None,
    vector_config: None,
    reader_storage_config: None,
    format: VecFormat::Fvecs,
    max_vectors: None,
    normalize: false,
    query_concurrency: DEFAULT_QUERY_CONCURRENCY,
    query_qps_limit: DEFAULT_QUERY_QPS_LIMIT,
    phases: DEFAULT_PHASES,
};

const COHERE_WIKI_10M: Dataset = Dataset {
    name: "cohere_wiki_10m",
    dimensions: 1024,
    distance_metric: DistanceMetric::Cosine,
    base_file: "cohere-wiki/base.fvecs",
    query_file: "cohere-wiki/query.fvecs",
    ground_truth_file: "cohere-wiki/groundtruth.ivecs",
    split_threshold: 150,
    merge_threshold: 50,
    query_pruning_factor: Some(0.5),
    nprobe: 100,
    num_queries: 1000,
    num_cold_queries: DEFAULT_NUM_COLD_QUERIES,
    block_cache_bytes: None,
    block_cache_disk_bytes: None,
    block_cache_disk_path: DEFAULT_BLOCK_CACHE_DISK_PATH,
    data_dir: None,
    vector_config: None,
    reader_storage_config: None,
    format: VecFormat::Fvecs,
    max_vectors: Some(10_000_000),
    normalize: false,
    query_concurrency: DEFAULT_QUERY_CONCURRENCY,
    query_qps_limit: DEFAULT_QUERY_QPS_LIMIT,
    phases: DEFAULT_PHASES,
};

// BigANN / SIFT1B variants — all share the same base and query files but
// differ in the number of vectors ingested and their ground truth files.

const SIFT10M: Dataset = Dataset {
    name: "sift10m",
    dimensions: 128,
    distance_metric: DistanceMetric::L2,
    base_file: "bigann/bigann_base.bvecs",
    query_file: "bigann/bigann_query.bvecs",
    ground_truth_file: "bigann/bigann_groundtruth_10M.ivecs",
    split_threshold: 150,
    merge_threshold: 50,
    query_pruning_factor: Some(0.5),
    nprobe: 100,
    num_queries: DEFAULT_NUM_QUERIES,
    num_cold_queries: DEFAULT_NUM_COLD_QUERIES,
    block_cache_bytes: None,
    block_cache_disk_bytes: None,
    block_cache_disk_path: DEFAULT_BLOCK_CACHE_DISK_PATH,
    data_dir: None,
    vector_config: None,
    reader_storage_config: None,
    format: VecFormat::Bvecs,
    max_vectors: Some(10_000_000),
    normalize: false,
    query_concurrency: DEFAULT_QUERY_CONCURRENCY,
    query_qps_limit: DEFAULT_QUERY_QPS_LIMIT,
    phases: DEFAULT_PHASES,
};

const SIFT50M: Dataset = Dataset {
    name: "sift50m",
    dimensions: 128,
    distance_metric: DistanceMetric::L2,
    base_file: "bigann/bigann_base.bvecs",
    query_file: "bigann/bigann_query.bvecs",
    ground_truth_file: "bigann/bigann_groundtruth_50M.ivecs",
    split_threshold: 150,
    merge_threshold: 50,
    query_pruning_factor: Some(0.5),
    nprobe: 100,
    num_queries: DEFAULT_NUM_QUERIES,
    num_cold_queries: DEFAULT_NUM_COLD_QUERIES,
    block_cache_bytes: None,
    block_cache_disk_bytes: None,
    block_cache_disk_path: DEFAULT_BLOCK_CACHE_DISK_PATH,
    data_dir: None,
    vector_config: None,
    reader_storage_config: None,
    format: VecFormat::Bvecs,
    max_vectors: Some(50_000_000),
    normalize: false,
    query_concurrency: DEFAULT_QUERY_CONCURRENCY,
    query_qps_limit: DEFAULT_QUERY_QPS_LIMIT,
    phases: DEFAULT_PHASES,
};

const SIFT100M: Dataset = Dataset {
    name: "sift100m",
    dimensions: 128,
    distance_metric: DistanceMetric::L2,
    base_file: "bigann/bigann_base.bvecs",
    query_file: "bigann/bigann_query.bvecs",
    ground_truth_file: "bigann/bigann_groundtruth_100M.ivecs",
    split_threshold: 150,
    merge_threshold: 50,
    query_pruning_factor: Some(0.5),
    nprobe: 100,
    num_queries: DEFAULT_NUM_QUERIES,
    num_cold_queries: DEFAULT_NUM_COLD_QUERIES,
    block_cache_bytes: None,
    block_cache_disk_bytes: None,
    block_cache_disk_path: DEFAULT_BLOCK_CACHE_DISK_PATH,
    data_dir: None,
    vector_config: None,
    reader_storage_config: None,
    format: VecFormat::Bvecs,
    max_vectors: Some(100_000_000),
    normalize: false,
    query_concurrency: DEFAULT_QUERY_CONCURRENCY,
    query_qps_limit: DEFAULT_QUERY_QPS_LIMIT,
    phases: DEFAULT_PHASES,
};

const SIFT1B: Dataset = Dataset {
    name: "sift1b",
    dimensions: 128,
    distance_metric: DistanceMetric::L2,
    base_file: "bigann/bigann_base.bvecs",
    query_file: "bigann/bigann_query.bvecs",
    ground_truth_file: "bigann/bigann_groundtruth_1B.ivecs",
    split_threshold: 150,
    merge_threshold: 50,
    query_pruning_factor: Some(0.5),
    nprobe: 100,
    num_queries: DEFAULT_NUM_QUERIES,
    num_cold_queries: DEFAULT_NUM_COLD_QUERIES,
    block_cache_bytes: None,
    block_cache_disk_bytes: None,
    block_cache_disk_path: DEFAULT_BLOCK_CACHE_DISK_PATH,
    data_dir: None,
    vector_config: None,
    reader_storage_config: None,
    format: VecFormat::Bvecs,
    max_vectors: None,
    normalize: false,
    query_concurrency: DEFAULT_QUERY_CONCURRENCY,
    query_qps_limit: DEFAULT_QUERY_QPS_LIMIT,
    phases: DEFAULT_PHASES,
};

const ALL_DATASETS: &[&Dataset] = &[
    &SIFT1M,
    &SIFT100K,
    &COHERE1M,
    &COHERE_WIKI_10M,
    &DEEP10M,
    &DEEP1B,
    &WIKIPEDIA_BGE_M3_EN,
    &SIFT10M,
    &SIFT50M,
    &SIFT100M,
    &SIFT1B,
];

fn lookup_dataset(name: &str) -> Option<&'static Dataset> {
    ALL_DATASETS.iter().find(|d| d.name == name).copied()
}

// -- Storage helpers shared by phases -----------------------------------------

/// Build a memory-only foyer block cache of the given capacity.
fn memory_only_block_cache(memory_bytes: u64) -> Arc<dyn DbCache> {
    let cache = FoyerCache::new_with_opts(FoyerCacheOptions {
        max_capacity: memory_bytes,
        ..Default::default()
    });
    Arc::new(cache) as Arc<dyn DbCache>
}

/// Build a hybrid memory+disk foyer block cache.
async fn hybrid_block_cache(
    memory_bytes: u64,
    disk_bytes: u64,
    disk_path: &str,
) -> anyhow::Result<Arc<dyn DbCache>> {
    use foyer::{DeviceBuilder, HybridCacheBuilder};

    let memory_capacity = usize::try_from(memory_bytes).map_err(|_| {
        StorageError::Storage(format!(
            "block_cache_bytes {} exceeds usize::MAX on this platform",
            memory_bytes,
        ))
    })?;
    let disk_capacity = usize::try_from(disk_bytes).map_err(|_| {
        StorageError::Storage(format!(
            "block_cache_disk_bytes {} exceeds usize::MAX on this platform",
            disk_bytes,
        ))
    })?;

    let device = foyer::FsDeviceBuilder::new(disk_path)
        .with_capacity(disk_capacity)
        .build()
        .map_err(|e| {
            StorageError::Storage(format!(
                "failed to build foyer disk device at {}: {}",
                disk_path, e
            ))
        })?;

    let cache = HybridCacheBuilder::new()
        .with_name("slatedb_block_cache")
        .with_policy(HybridCachePolicy::WriteOnInsertion)
        .memory(memory_capacity)
        .with_weighter(|_: &CachedKey, v: &CachedEntry| v.size())
        .storage()
        .with_io_engine_config(foyer::PsyncIoEngineConfig::new())
        .with_engine_config(
            foyer::BlockEngineConfig::new(device)
                .with_block_size(16 * 1024 * 1024)
                .with_flushers(4)
                .with_buffer_pool_size(256 * 1024 * 1024)
                .with_submit_queue_size_threshold(1024 * 1024 * 1024),
        )
        .build()
        .await
        .map_err(|e| StorageError::Storage(format!("Failed to create hybrid cache: {}", e)))?;
    Ok(Arc::new(FoyerHybridCache::new_with_cache(cache)) as Arc<dyn DbCache>)
}

/// Resolve the effective memory-tier byte count for the block cache,
/// applying the user-facing convention:
/// - `None` → use `default_memory_bytes` (the phase's computed default).
/// - `Some(n)` with `n < 0` → cache disabled (returns `None`).
/// - `Some(n)` with `n >= 0` → use `n` bytes exactly (no cap).
fn resolve_block_cache_memory(configured: Option<i64>, default_memory_bytes: u64) -> Option<u64> {
    match configured {
        None => Some(default_memory_bytes),
        Some(n) if n < 0 => None,
        Some(n) => Some(n as u64),
    }
}

/// Build the dataset's block cache.
///
/// The memory tier is sized from [`Dataset::block_cache_bytes`] (the
/// explicit value when set, or `default_memory_bytes` when unset; `-1`
/// disables the cache entirely — see [`resolve_block_cache_memory`]).
/// If [`Dataset::block_cache_disk_bytes`] is also set, the cache is a
/// hybrid memory + disk foyer cache; otherwise it is memory-only.
pub(crate) async fn build_block_cache(
    dataset: &Dataset,
    default_memory_bytes: u64,
) -> anyhow::Result<Option<Arc<dyn DbCache>>> {
    let Some(memory_bytes) =
        resolve_block_cache_memory(dataset.block_cache_bytes, default_memory_bytes)
    else {
        return Ok(None);
    };
    if memory_bytes == 0 {
        return Ok(None);
    }
    match dataset.block_cache_disk_bytes {
        None => Ok(Some(memory_only_block_cache(memory_bytes))),
        Some(disk_bytes) => Ok(Some(
            hybrid_block_cache(memory_bytes, disk_bytes, dataset.block_cache_disk_path).await?,
        )),
    }
}

/// Open a fresh `VectorDb` for the given `Config`, layering in the
/// dataset's block cache if one is configured. The `default_memory_bytes`
/// argument supplies the cache size used when the dataset doesn't set
/// `block_cache_bytes` explicitly (see [`ingest_default_memory_bytes`]
/// and [`warm_default_memory_bytes`]).
pub(crate) async fn open_db(
    config: &Config,
    dataset: &Dataset,
    default_memory_bytes: u64,
) -> anyhow::Result<VectorDb> {
    let mut sb = StorageBuilder::new(&config.storage).await?;
    println!("  {}", describe_block_cache(dataset, default_memory_bytes));
    if let Some(cache) = build_block_cache(dataset, default_memory_bytes).await? {
        sb = sb.map_slatedb(move |db| db.with_db_cache(cache));
    }
    Ok(VectorDb::open_with_storage(config.clone(), sb).await?)
}

fn describe_block_cache(dataset: &Dataset, default_memory_bytes: u64) -> String {
    let effective = resolve_block_cache_memory(dataset.block_cache_bytes, default_memory_bytes);
    let source = match dataset.block_cache_bytes {
        None => " (default)",
        Some(_) => "",
    };
    match (effective, dataset.block_cache_disk_bytes) {
        (None, _) => "Block cache: disabled".to_string(),
        (Some(mem), None) => format!("Block cache: memory-only, {} bytes{}", mem, source),
        (Some(mem), Some(disk)) => format!(
            "Block cache: hybrid, {} bytes memory{} + {} bytes disk at {}",
            mem, source, disk, dataset.block_cache_disk_path,
        ),
    }
}

/// Returns total physical memory in bytes, queried via `sysinfo`.
fn total_system_memory_bytes() -> u64 {
    let mut sys = sysinfo::System::new();
    sys.refresh_memory();
    sys.total_memory()
}

/// Default memory-tier size for the block cache when the dataset does
/// not set [`Dataset::block_cache_bytes`] during the ingest phase
/// ([`INGEST_BLOCK_CACHE_MEMORY_FRACTION`] of total system memory).
pub(crate) fn ingest_default_memory_bytes() -> u64 {
    (total_system_memory_bytes() as f64 * INGEST_BLOCK_CACHE_MEMORY_FRACTION) as u64
}

/// Default memory-tier size for the block cache when the dataset does
/// not set [`Dataset::block_cache_bytes`] during the warm (and cold)
/// phases ([`WARM_BLOCK_CACHE_MEMORY_FRACTION`] of total system memory).
pub(crate) fn warm_default_memory_bytes() -> u64 {
    (total_system_memory_bytes() as f64 * WARM_BLOCK_CACHE_MEMORY_FRACTION) as u64
}

/// Build a `StorageReaderRuntime` for the cold phase. Always attaches a
/// **memory-only** foyer cache (sized by [`Dataset::block_cache_bytes`]),
/// regardless of whether the dataset configures a hybrid cache for the
/// writer side. The cold phase tears down and rebuilds this runtime
/// between query groups to clear the cache; a disk tier would survive
/// that teardown and defeat the cold measurement, so we force the
/// memory-only configuration here.
pub(crate) fn build_cold_reader_runtime(
    reader_config: &ReaderConfig,
    dataset: &Dataset,
    default_memory_bytes: u64,
) -> anyhow::Result<StorageReaderRuntime> {
    let object_store = match &reader_config.storage {
        StorageConfig::SlateDb(slate_config) => {
            Some(create_object_store(&slate_config.object_store)?)
        }
        _ => None,
    };
    let mut runtime = StorageReaderRuntime::default();
    if let Some(object_store) = object_store {
        runtime = runtime.with_object_store(object_store);
    }
    if let Some(memory_bytes) =
        resolve_block_cache_memory(dataset.block_cache_bytes, default_memory_bytes)
        && memory_bytes > 0
    {
        runtime = runtime.with_block_cache(memory_only_block_cache(memory_bytes));
    }
    Ok(runtime)
}

// -- Benchmark ----------------------------------------------------------------

pub struct RecallBenchmark;

impl RecallBenchmark {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl Benchmark for RecallBenchmark {
    fn name(&self) -> &str {
        "recall"
    }

    fn default_params(&self) -> Vec<Params> {
        vec![Params::from(&SIFT1M)]
    }

    async fn run(&self, bench: Bench) -> anyhow::Result<()> {
        let dataset: Dataset = bench.spec().params().clone().into();

        let data = dataset
            .data_dir
            .as_ref()
            .map(PathBuf::from)
            .unwrap_or_else(data_dir);
        let k = 10;

        // -- Load query / ground-truth data, used by COLD and WARM ---------
        let queries = dataset.load_query_vectors(&data);
        let ground_truth = read_ivecs(&data.join(dataset.ground_truth_file));
        println!(
            "  Loaded {} queries, {} ground truth entries",
            queries.len(),
            ground_truth.len()
        );

        // -- Build the per-phase Config / ReaderConfig once ----------------
        let config = match &dataset.vector_config {
            Some(path) => load_vector_config(path),
            None => Config {
                storage: bench.spec().data().storage.clone(),
                dimensions: dataset.dimensions,
                distance_metric: dataset.distance_metric,
                split_threshold_vectors: dataset.split_threshold,
                merge_threshold_vectors: dataset.merge_threshold,
                query_pruning_factor: dataset.query_pruning_factor,
                ..Default::default()
            },
        };
        let reader_storage = dataset.resolve_reader_storage_config(&config.storage)?;
        let reader_config = ReaderConfig {
            storage: reader_storage,
            dimensions: config.dimensions,
            distance_metric: config.distance_metric,
            query_pruning_factor: config.query_pruning_factor,
            metadata_fields: config.metadata_fields.clone(),
        };

        // -- Dispatch each configured phase --------------------------------
        let mut summary = Summary::new();
        for phase in dataset.phases.iter().copied() {
            println!("\n=== phase: {} ===", phase.as_str());
            match phase {
                Phase::Ingest => {
                    let s = ingest::run(&dataset, &data, &config).await?;
                    summary = summary
                        .add("num_vectors", s.num_vectors as f64)
                        .add("ingest_secs", s.ingest_secs)
                        .add("ingest_vec_per_sec", s.num_vectors as f64 / s.ingest_secs);
                }
                Phase::Cold => {
                    let s = cold::run(&dataset, &reader_config, &queries, k, &bench).await?;
                    summary = summary
                        .add("cold_p50_latency_us", s.p50)
                        .add("cold_p90_latency_us", s.p90)
                        .add("cold_p99_latency_us", s.p99);
                }
                Phase::Warm => {
                    let s =
                        warm::run(&dataset, &config, &queries, &ground_truth, k, &bench).await?;
                    summary = summary
                        .add("recall_at_k", s.recall_at_k)
                        .add("k", k as f64)
                        .add("qps", s.qps)
                        .add("num_queries", queries.len() as f64)
                        .add("num_centroids", s.num_centroids as f64)
                        .add("p50_latency_us", s.p50)
                        .add("p90_latency_us", s.p90)
                        .add("p99_latency_us", s.p99);
                }
            }
        }

        bench.summarize(summary).await?;
        bench.close().await?;
        Ok(())
    }
}
