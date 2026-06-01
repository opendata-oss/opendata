//! Recall benchmark for the vector database.
//!
//! Splits work into three phases — `INGEST`, `COLD`, and `WARM`. The phases
//! to run are configured per-dataset via the dataset's `phases` field and
//! can be overridden through the bencher config's `[params.recall]`
//! section. Each phase opens its own fresh `VectorDb` or `VectorDbReader`
//! and closes it before the next phase starts.

mod cold;
mod filter;
mod groundtruth;
mod ingest;
mod parquet;
mod warm;

use parquet::ParquetVectorBatchReader;
use vector::{Attribute, Filter};

/// A base-vector row: the embedding plus any metadata attributes read from
/// the configured `metadata_columns`. For fvecs/bvecs `attributes` is empty.
pub(crate) struct BaseRow {
    pub embedding: Vec<f32>,
    pub attributes: Vec<Attribute>,
}

/// A reader that yields batches of base rows (embedding + metadata
/// attributes). Implemented by both the fvecs/bvecs byte reader and the
/// Parquet reader so the ingest stream is format-agnostic.
pub(crate) trait VectorBatchReader: Send {
    /// Returns the next batch of rows, or `None` when exhausted. The
    /// `max_rows` hint is honoured by the fvecs/bvecs reader; the Parquet
    /// reader yields whole arrow batches and treats it as advisory.
    fn read_batch(&mut self, max_rows: usize) -> anyhow::Result<Option<Vec<BaseRow>>>;
}

/// Generate ground-truth files for every `[[params.recall]]` dataset in the
/// given bench config (or the default dataset list when none is configured).
/// Each file is written in the format matching its dataset — `ivecs` for
/// fvecs/bvecs, Parquet for parquet — and is *filtered* when the dataset
/// configures a `filter_spec`.
pub async fn generate_ground_truth(config: &bencher::Config) -> anyhow::Result<()> {
    use bencher::Benchmark;

    let param_sets = config
        .params
        .get("recall")
        .cloned()
        .unwrap_or_else(|| RecallBenchmark::new().default_params());

    for params in param_sets {
        let dataset: Dataset = params.into();
        let data = dataset
            .data_dir
            .as_ref()
            .map(PathBuf::from)
            .unwrap_or_else(data_dir);
        groundtruth::generate_for_dataset(&dataset, &data).await?;
    }
    Ok(())
}

/// A single benchmark query: the embedding plus an optional precomputed
/// filter (present only when the dataset configures a `filter_spec`).
pub(crate) struct BenchQuery {
    pub embedding: Vec<f32>,
    pub filter: Option<Filter>,
}

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

const DEFAULT_NUM_QUERIES: usize = 1000;
/// Default number of queries to run during the cold phase. Queries are
/// cycled if the dataset has fewer loaded queries than this.
const DEFAULT_NUM_COLD_QUERIES: usize = 1000;
/// Default number of concurrent queries during the warm query phase.
const DEFAULT_QUERY_CONCURRENCY: usize = 8;
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
pub(crate) fn normalize_vec(v: &mut [f32]) {
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

    fn read_vector_batch(&mut self, max_rows: usize) -> anyhow::Result<Option<Vec<Vec<f32>>>> {
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
            VecFormat::Parquet => {
                // Parquet is read via `ParquetVectorBatchReader`, not this
                // byte reader, so this arm is never reached.
                unreachable!("parquet datasets use ParquetVectorBatchReader")
            }
        }
    }
}

impl VectorBatchReader for VectorFileBatchReader {
    fn read_batch(&mut self, max_rows: usize) -> anyhow::Result<Option<Vec<BaseRow>>> {
        Ok(self.read_vector_batch(max_rows)?.map(|rows| {
            rows.into_iter()
                .map(|embedding| BaseRow {
                    embedding,
                    attributes: Vec::new(),
                })
                .collect()
        }))
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
    Parquet,
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
    /// Name of the embedding column in the base/query Parquet files. Only
    /// used (and required) when [`Dataset::format`] is
    /// [`VecFormat::Parquet`]; ignored for fvecs/bvecs.
    pub vector_column: Option<String>,
    /// Name of the neighbour-list column in the ground-truth Parquet file
    /// (a `List<Int*>` per query). Only used (and required) when
    /// [`Dataset::format`] is [`VecFormat::Parquet`].
    pub ground_truth_column: Option<String>,
    /// Optional name of the ID column in the base Parquet file. When set,
    /// ground-truth neighbour values are treated as dataset IDs and remapped
    /// to the row index each vector is ingested under. When unset, neighbour
    /// values are assumed to already be 0-based row indices. Only meaningful
    /// when [`Dataset::format`] is [`VecFormat::Parquet`].
    pub id_column: Option<String>,
    /// Base Parquet columns to ingest as metadata attributes on each vector.
    /// Their `FieldType` is inferred from the Parquet schema (Utf8 →
    /// String, Int* → Int64, Float* → Float64, Boolean → Bool). Empty = no
    /// metadata. Only meaningful when [`Dataset::format`] is
    /// [`VecFormat::Parquet`].
    pub metadata_columns: Vec<String>,
    /// Subset of [`Dataset::metadata_columns`] to mark as indexed (i.e.
    /// usable in query filters). When empty, **all** metadata columns are
    /// indexed. Columns listed here that are not in `metadata_columns` are
    /// ignored.
    pub indexed_columns: Vec<String>,
    /// Path to a JSON/YAML filter spec ([`vector::server::JsonFilter`]
    /// shape) applied to every query. Filter values may reference query
    /// columns via the `@column` convention. Only meaningful when
    /// [`Dataset::format`] is [`VecFormat::Parquet`].
    pub filter_spec: Option<String>,
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
        let total = match self.format {
            VecFormat::Fvecs | VecFormat::Bvecs => {
                let bytes = std::fs::metadata(&path)?.len() as usize;
                let record_size = match self.format {
                    VecFormat::Fvecs => 4 + self.dimensions as usize * 4,
                    VecFormat::Bvecs => 4 + self.dimensions as usize,
                    VecFormat::Parquet => unreachable!(),
                };
                if !bytes.is_multiple_of(record_size) {
                    anyhow::bail!(
                        "file size {} for {} is not a multiple of record size {}",
                        bytes,
                        path.display(),
                        record_size
                    );
                }
                bytes / record_size
            }
            VecFormat::Parquet => parquet::row_count(&path)?,
        };
        Ok(self.max_vectors.map_or(total, |n| n.min(total)))
    }

    /// Open a [`VectorBatchReader`] for the base vectors, dispatching on the
    /// dataset's format.
    fn open_base_reader(&self, data_dir: &Path) -> anyhow::Result<Box<dyn VectorBatchReader>> {
        let path = self.base_path(data_dir);
        let dimensions = self.dimensions as usize;
        match self.format {
            VecFormat::Fvecs | VecFormat::Bvecs => Ok(Box::new(VectorFileBatchReader::open(
                &path,
                self.format,
                dimensions,
                self.max_vectors,
                self.normalize,
            )?)),
            VecFormat::Parquet => {
                let column = self.vector_column.as_deref().expect(
                    "parquet dataset missing vector_column (should be validated at parse time)",
                );
                let metadata = parquet::read_field_types(&path, &self.metadata_columns)?;
                Ok(Box::new(ParquetVectorBatchReader::open(
                    &path,
                    column,
                    dimensions,
                    metadata,
                    self.max_vectors,
                    self.normalize,
                )?))
            }
        }
    }

    /// Build the metadata schema (`MetadataFieldSpec`s) for this dataset's
    /// configured `metadata_columns`, inferring each field's type from the
    /// base Parquet schema. A column is marked `indexed` when it appears in
    /// `indexed_columns`, or when `indexed_columns` is empty (index all).
    /// Returns an empty schema for non-parquet datasets or when no metadata
    /// columns are configured.
    pub(crate) fn metadata_field_specs(
        &self,
        data_dir: &Path,
    ) -> anyhow::Result<Vec<vector::MetadataFieldSpec>> {
        if self.metadata_columns.is_empty() {
            return Ok(Vec::new());
        }
        if !matches!(self.format, VecFormat::Parquet) {
            anyhow::bail!("metadata_columns is only supported for parquet datasets");
        }
        let index_all = self.indexed_columns.is_empty();
        let field_types =
            parquet::read_field_types(&self.base_path(data_dir), &self.metadata_columns)?;
        Ok(field_types
            .into_iter()
            .map(|(name, field_type)| {
                let indexed = index_all || self.indexed_columns.iter().any(|c| c == &name);
                vector::MetadataFieldSpec::new(name, field_type, indexed)
            })
            .collect())
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn spawn_base_vector_stream(
        &self,
        data_dir: &Path,
        batch_size: usize,
    ) -> anyhow::Result<(
        mpsc::Receiver<anyhow::Result<Option<Vec<BaseRow>>>>,
        thread::JoinHandle<()>,
    )> {
        let mut reader = self.open_base_reader(data_dir)?;
        let (tx, rx) = mpsc::channel(1);

        let handle = thread::spawn(move || {
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

    /// Load ground-truth neighbour lists, dispatching on the dataset's
    /// format. For Parquet, optionally remaps neighbour IDs to row indices
    /// using [`Dataset::id_column`].
    pub(crate) fn load_ground_truth(&self, data_dir: &Path) -> anyhow::Result<Vec<Vec<i32>>> {
        let path = data_dir.join(self.ground_truth_file);
        match self.format {
            VecFormat::Fvecs | VecFormat::Bvecs => Ok(read_ivecs(&path)),
            VecFormat::Parquet => {
                let column = self.ground_truth_column.as_deref().expect(
                    "parquet dataset missing ground_truth_column \
                     (should be validated at parse time)",
                );
                let id_map = match &self.id_column {
                    Some(id_column) => Some(parquet::read_id_index_map(
                        &self.base_path(data_dir),
                        id_column,
                    )?),
                    None => None,
                };
                parquet::read_ground_truth(&path, column, id_map.as_ref())
            }
        }
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

    fn load_query_vectors(&self, data_dir: &Path) -> anyhow::Result<Vec<Vec<f32>>> {
        let path = self.query_path(data_dir);
        match self.format {
            VecFormat::Fvecs => Ok(read_fvecs(&path)
                .into_iter()
                .take(self.num_queries)
                .collect()),
            VecFormat::Bvecs => Ok(read_bvecs(&path, Some(self.num_queries))),
            VecFormat::Parquet => {
                let column = self.vector_column.as_deref().expect(
                    "parquet dataset missing vector_column (should be validated at parse time)",
                );
                parquet::read_query_vectors(
                    &path,
                    column,
                    self.dimensions as usize,
                    self.num_queries,
                )
            }
        }
    }

    /// Load benchmark queries: embeddings plus, when a `filter_spec` is
    /// configured, a per-query [`Filter`] whose `@column` references are
    /// bound from the query file's columns.
    fn load_queries(&self, data_dir: &Path) -> anyhow::Result<Vec<BenchQuery>> {
        let embeddings = self.load_query_vectors(data_dir)?;

        let Some(filter_spec_path) = &self.filter_spec else {
            return Ok(embeddings
                .into_iter()
                .map(|embedding| BenchQuery {
                    embedding,
                    filter: None,
                })
                .collect());
        };

        if !matches!(self.format, VecFormat::Parquet) {
            anyhow::bail!("filter_spec is only supported for parquet datasets");
        }

        let spec = filter::load_filter_spec(filter_spec_path)?;
        let referenced: Vec<String> = filter::referenced_columns(&spec).into_iter().collect();
        let query_path = self.query_path(data_dir);
        // Per-query values for each referenced column, aligned with embeddings
        // by row order (both read the query file from row 0).
        let columns = parquet::read_attribute_columns(&query_path, &referenced, embeddings.len())?;

        let mut out = Vec::with_capacity(embeddings.len());
        for (i, embedding) in embeddings.into_iter().enumerate() {
            let mut row = std::collections::HashMap::with_capacity(referenced.len());
            for col in &referenced {
                if let Some(Some(value)) = columns.get(col).and_then(|vals| vals.get(i)) {
                    row.insert(col.clone(), value.clone());
                }
            }
            let filter = filter::build_filter(&spec, &row)?;
            out.push(BenchQuery {
                embedding,
                filter: Some(filter),
            });
        }
        Ok(out)
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
        VecFormat::Parquet => "parquet",
    }
}

fn str_to_format(s: &str) -> VecFormat {
    match s {
        "fvecs" => VecFormat::Fvecs,
        "bvecs" => VecFormat::Bvecs,
        "parquet" => VecFormat::Parquet,
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
        if let Some(ref col) = d.vector_column {
            p.insert("vector_column", col.clone());
        }
        if let Some(ref col) = d.ground_truth_column {
            p.insert("ground_truth_column", col.clone());
        }
        if let Some(ref col) = d.id_column {
            p.insert("id_column", col.clone());
        }
        if !d.metadata_columns.is_empty() {
            p.insert("metadata_columns", d.metadata_columns.join(","));
        }
        if !d.indexed_columns.is_empty() {
            p.insert("indexed_columns", d.indexed_columns.join(","));
        }
        if let Some(ref path) = d.filter_spec {
            p.insert("filter_spec", path.clone());
        }
        p
    }
}

/// Parse a comma-separated Params value into a `Vec<String>`, trimming
/// whitespace and dropping empty entries.
fn param_to_string_list(s: &str) -> Vec<String> {
    s.split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
}

impl From<Params> for Dataset {
    fn from(p: Params) -> Self {
        let name_str = p
            .get("dataset")
            .expect("params missing 'dataset' field")
            .to_string();
        // `default` is `Some` for built-in datasets (looked up by name) and
        // `None` for user-defined custom datasets. When `None`, the params
        // must supply enough information to fully define the dataset.
        let default = lookup_dataset(&name_str);
        let is_custom = default.is_none();

        // `name` is `&'static str` — built-in datasets reuse the const's
        // value; custom datasets leak the user-supplied string.
        let name: &'static str = match default {
            Some(d) => d.name,
            None => Box::leak(name_str.clone().into_boxed_str()),
        };

        // For each field that has no sensible fallback for a custom dataset,
        // `unwrap_or_else(|| missing_field(...))` panics with a clear message
        // when the user neither named a built-in dataset nor supplied the
        // field explicitly.
        let dimensions: u16 = p
            .get_parse("dimensions")
            .ok()
            .or_else(|| default.map(|d| d.dimensions))
            .unwrap_or_else(|| missing_field(&name_str, "dimensions"));
        let distance_metric = p
            .get("distance_metric")
            .map(str_to_distance_metric)
            .or_else(|| default.map(|d| d.distance_metric))
            .unwrap_or_else(|| missing_field(&name_str, "distance_metric"));
        let format = p
            .get("format")
            .map(str_to_format)
            .or_else(|| default.map(|d| d.format))
            .unwrap_or_else(|| missing_field(&name_str, "format"));

        // Parquet column mapping. `vector_column` and `ground_truth_column`
        // are required when the format is Parquet; `id_column` is optional.
        let vector_column = p
            .get("vector_column")
            .map(|s| s.to_string())
            .or_else(|| default.and_then(|d| d.vector_column.clone()));
        let ground_truth_column = p
            .get("ground_truth_column")
            .map(|s| s.to_string())
            .or_else(|| default.and_then(|d| d.ground_truth_column.clone()));
        let id_column = p
            .get("id_column")
            .map(|s| s.to_string())
            .or_else(|| default.and_then(|d| d.id_column.clone()));
        let metadata_columns = p
            .get("metadata_columns")
            .map(param_to_string_list)
            .unwrap_or_else(|| {
                default
                    .map(|d| d.metadata_columns.clone())
                    .unwrap_or_default()
            });
        let indexed_columns = p
            .get("indexed_columns")
            .map(param_to_string_list)
            .unwrap_or_else(|| {
                default
                    .map(|d| d.indexed_columns.clone())
                    .unwrap_or_default()
            });
        let filter_spec = p
            .get("filter_spec")
            .map(|s| s.to_string())
            .or_else(|| default.and_then(|d| d.filter_spec.clone()));
        if matches!(format, VecFormat::Parquet) {
            if vector_column.is_none() {
                missing_field(&name_str, "vector_column");
            }
            if ground_truth_column.is_none() {
                missing_field(&name_str, "ground_truth_column");
            }
        }

        // `base_file` / `query_file` / `ground_truth_file` are now overridable
        // via params for built-in datasets too. For custom datasets they are
        // required. They round-trip through `Box::leak` to satisfy
        // `&'static str`.
        let base_file = leak_static_str_field(&p, "base_file", default.map(|d| d.base_file))
            .unwrap_or_else(|| missing_field(&name_str, "base_file"));
        let query_file = leak_static_str_field(&p, "query_file", default.map(|d| d.query_file))
            .unwrap_or_else(|| missing_field(&name_str, "query_file"));
        let ground_truth_file = leak_static_str_field(
            &p,
            "ground_truth_file",
            default.map(|d| d.ground_truth_file),
        )
        .unwrap_or_else(|| missing_field(&name_str, "ground_truth_file"));

        // -- Optional fields: params > built-in default > hardcoded fallback.

        let split_threshold = p
            .get_parse("split_threshold")
            .ok()
            .or_else(|| default.map(|d| d.split_threshold))
            .unwrap_or(1500);
        let merge_threshold = p
            .get_parse("merge_threshold")
            .ok()
            .or_else(|| default.map(|d| d.merge_threshold))
            .unwrap_or(500);
        let query_pruning_factor = p
            .get_parse::<f32>("query_pruning_factor")
            .ok()
            .or_else(|| default.and_then(|d| d.query_pruning_factor))
            .filter(|f| *f > 0.0);
        let nprobe = p
            .get_parse("nprobe")
            .ok()
            .or_else(|| default.map(|d| d.nprobe))
            .unwrap_or(100);
        let num_queries = p
            .get_parse("num_queries")
            .ok()
            .or_else(|| default.map(|d| d.num_queries))
            .unwrap_or(DEFAULT_NUM_QUERIES);
        let num_cold_queries = p
            .get_parse("num_cold_queries")
            .ok()
            .or_else(|| default.map(|d| d.num_cold_queries))
            .unwrap_or(DEFAULT_NUM_COLD_QUERIES);
        let query_concurrency = p
            .get_parse("query_concurrency")
            .ok()
            .or_else(|| default.map(|d| d.query_concurrency))
            .unwrap_or(DEFAULT_QUERY_CONCURRENCY);
        let query_qps_limit = p
            .get_parse("query_qps_limit")
            .ok()
            .or_else(|| default.map(|d| d.query_qps_limit))
            .unwrap_or(DEFAULT_QUERY_QPS_LIMIT);
        let max_vectors = p
            .get_parse("max_vectors")
            .ok()
            .or_else(|| default.and_then(|d| d.max_vectors));
        let normalize = p
            .get_parse("normalize")
            .ok()
            .or_else(|| default.map(|d| d.normalize))
            .unwrap_or(false);

        // `block_cache_bytes`: `Option<i64>` where `None` means "use the
        // phase default", `Some(-1)` (or any negative) means "disabled",
        // and `Some(n >= 0)` is an explicit byte count.
        let block_cache_bytes = p
            .get_parse::<i64>("block_cache_bytes")
            .ok()
            .or_else(|| default.and_then(|d| d.block_cache_bytes));
        let block_cache_disk_bytes = p
            .get_parse::<i64>("block_cache_disk_bytes")
            .ok()
            .or_else(|| {
                default
                    .and_then(|d| d.block_cache_disk_bytes)
                    .map(|v| v as i64)
            })
            .filter(|b| *b > 0)
            .map(|b| b as u64);
        let block_cache_disk_path: &'static str = match p.get("block_cache_disk_path") {
            Some(s) => Box::leak(s.to_string().into_boxed_str()),
            None => default
                .map(|d| d.block_cache_disk_path)
                .unwrap_or(DEFAULT_BLOCK_CACHE_DISK_PATH),
        };

        let data_dir = p
            .get("data_dir")
            .map(|s| s.to_string())
            .or_else(|| default.and_then(|d| d.data_dir.clone()));
        let vector_config = p
            .get("vector_config")
            .map(|s| s.to_string())
            .or_else(|| default.and_then(|d| d.vector_config.clone()));
        let reader_storage_config = p
            .get("reader_storage_config")
            .map(|s| s.to_string())
            .or_else(|| default.and_then(|d| d.reader_storage_config.clone()));

        // `phases` round-trips as a comma-separated string. For built-in
        // datasets we reuse the const's static slice when unset; for custom
        // datasets we fall back to `DEFAULT_PHASES`.
        let phases: &'static [Phase] = match p.get("phases") {
            Some(s) => Box::leak(param_to_phases(s).into_boxed_slice()),
            None => default.map(|d| d.phases).unwrap_or(DEFAULT_PHASES),
        };

        if is_custom {
            println!(
                "  Using custom dataset {:?} (not a built-in name; parameters supplied inline)",
                name,
            );
        }

        Dataset {
            name,
            dimensions,
            distance_metric,
            base_file,
            query_file,
            ground_truth_file,
            split_threshold,
            merge_threshold,
            query_pruning_factor,
            nprobe,
            num_queries,
            num_cold_queries,
            block_cache_bytes,
            block_cache_disk_bytes,
            block_cache_disk_path,
            data_dir,
            vector_config,
            reader_storage_config,
            format,
            vector_column,
            ground_truth_column,
            id_column,
            metadata_columns,
            indexed_columns,
            filter_spec,
            max_vectors,
            normalize,
            query_concurrency,
            query_qps_limit,
            phases,
        }
    }
}

/// Helper for `From<Params> for Dataset` to resolve a `&'static str` field
/// whose source order is: params override > built-in default. A
/// user-supplied override is `Box::leak`ed; the built-in default's slice is
/// reused as-is. Returns `None` when neither source provides a value, so
/// the caller can panic with a field-specific message.
fn leak_static_str_field(
    p: &Params,
    key: &str,
    default: Option<&'static str>,
) -> Option<&'static str> {
    match (p.get(key), default) {
        (Some(s), _) => Some(Box::leak(s.to_string().into_boxed_str())),
        (None, Some(d)) => Some(d),
        (None, None) => None,
    }
}

/// Panic with a uniform message when a custom dataset omits a required
/// `[[params.recall]]` field.
fn missing_field(dataset_name: &str, field: &str) -> ! {
    panic!(
        "custom dataset {:?} requires the '{}' field in [[params.recall]] \
         (or use one of the built-in dataset names)",
        dataset_name, field,
    )
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
    query_pruning_factor: None,
    num_queries: DEFAULT_NUM_QUERIES,
    num_cold_queries: DEFAULT_NUM_COLD_QUERIES,
    block_cache_bytes: None,
    block_cache_disk_bytes: None,
    block_cache_disk_path: DEFAULT_BLOCK_CACHE_DISK_PATH,
    data_dir: None,
    vector_config: None,
    reader_storage_config: None,
    format: VecFormat::Fvecs,
    vector_column: None,
    ground_truth_column: None,
    id_column: None,
    metadata_columns: Vec::new(),
    indexed_columns: Vec::new(),
    filter_spec: None,
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
    nprobe: 100,
    query_pruning_factor: None,
    num_queries: DEFAULT_NUM_QUERIES,
    num_cold_queries: DEFAULT_NUM_COLD_QUERIES,
    block_cache_bytes: None,
    block_cache_disk_bytes: None,
    block_cache_disk_path: DEFAULT_BLOCK_CACHE_DISK_PATH,
    data_dir: None,
    vector_config: None,
    reader_storage_config: None,
    format: VecFormat::Fvecs,
    vector_column: None,
    ground_truth_column: None,
    id_column: None,
    metadata_columns: Vec::new(),
    indexed_columns: Vec::new(),
    filter_spec: None,
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
    query_pruning_factor: None,
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
    vector_column: None,
    ground_truth_column: None,
    id_column: None,
    metadata_columns: Vec::new(),
    indexed_columns: Vec::new(),
    filter_spec: None,
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
    query_pruning_factor: None,
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
    vector_column: None,
    ground_truth_column: None,
    id_column: None,
    metadata_columns: Vec::new(),
    indexed_columns: Vec::new(),
    filter_spec: None,
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
    query_pruning_factor: None,
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
    vector_column: None,
    ground_truth_column: None,
    id_column: None,
    metadata_columns: Vec::new(),
    indexed_columns: Vec::new(),
    filter_spec: None,
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
    query_pruning_factor: None,
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
    vector_column: None,
    ground_truth_column: None,
    id_column: None,
    metadata_columns: Vec::new(),
    indexed_columns: Vec::new(),
    filter_spec: None,
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
    query_pruning_factor: None,
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
    vector_column: None,
    ground_truth_column: None,
    id_column: None,
    metadata_columns: Vec::new(),
    indexed_columns: Vec::new(),
    filter_spec: None,
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
    query_pruning_factor: None,
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
    vector_column: None,
    ground_truth_column: None,
    id_column: None,
    metadata_columns: Vec::new(),
    indexed_columns: Vec::new(),
    filter_spec: None,
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
    query_pruning_factor: None,
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
    vector_column: None,
    ground_truth_column: None,
    id_column: None,
    metadata_columns: Vec::new(),
    indexed_columns: Vec::new(),
    filter_spec: None,
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
    query_pruning_factor: None,
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
    vector_column: None,
    ground_truth_column: None,
    id_column: None,
    metadata_columns: Vec::new(),
    indexed_columns: Vec::new(),
    filter_spec: None,
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
    query_pruning_factor: None,
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
    vector_column: None,
    ground_truth_column: None,
    id_column: None,
    metadata_columns: Vec::new(),
    indexed_columns: Vec::new(),
    filter_spec: None,
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

impl Default for RecallBenchmark {
    fn default() -> Self {
        Self::new()
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
        let queries = dataset.load_queries(&data)?;
        let ground_truth = dataset.load_ground_truth(&data)?;
        println!(
            "  Loaded {} queries, {} ground truth entries",
            queries.len(),
            ground_truth.len()
        );

        // -- Build the per-phase Config / ReaderConfig once ----------------
        // Metadata schema (from configured `metadata_columns`) is applied to
        // both the constructed and the file-loaded Config so filtered queries
        // have indexed attributes to match against.
        let metadata_fields = dataset.metadata_field_specs(&data)?;
        let mut config = match &dataset.vector_config {
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
        if !metadata_fields.is_empty() {
            config.metadata_fields = metadata_fields;
        }
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
