//! Recall benchmark for the vector database.
//!
//! Ingests a dataset, then runs queries and measures recall@k against ground truth.
//!
//! All benchmark parameters (dataset, nprobe, num_queries, block_cache_bytes, etc.)
//! are configured via the bencher config file's `[params.recall]` section. When no
//! config-level params are provided, `default_params` runs all datasets whose data
//! files are present.
//!
//! Environment variables:
//! - `VECTOR_BENCH_SKIP_INGEST`: Set to `1` to skip the ingest phase and query
//!   an existing database (requires persistent storage via `vector_config`).

use std::collections::HashSet;
use std::fs::File;
use std::io::{self, BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use bencher::{Bench, Benchmark, Params, Summary};
use chrono::{DateTime, Local};
use common::{StorageConfig, StorageError};
use common::storage::config::SlateDbStorageConfig;
use common::storage::factory::{CachedEntry, CachedKey, FoyerCache, FoyerCacheOptions};
use common::{StorageBuilder, StorageReaderRuntime, create_object_store};
use tokio::sync::{mpsc, oneshot};
use vector::{
    Config, DistanceMetric, Query, ReaderConfig, SearchOptions, SearchResult, Vector, VectorDb,
    VectorDbRead, VectorDbReader,
};

const DEFAULT_NUM_QUERIES: usize = 100;
const BASE_VECTOR_CHUNK_SIZE: usize = 1_000_000;
const INGEST_WRITE_BATCH_SIZE: usize = 10;
/// Default number of concurrent queries during the warm query phase.
const DEFAULT_QUERY_CONCURRENCY: usize = 1;
/// Default ceiling on queries-per-second during the warm query phase.
const DEFAULT_QUERY_QPS_LIMIT: usize = 32;

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

fn skip_ingest() -> bool {
    std::env::var("VECTOR_BENCH_SKIP_INGEST")
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

fn data_dir() -> PathBuf {
    // When running from workspace root via `cargo run -p vector-bench`,
    // CARGO_MANIFEST_DIR points to vector/bench. Data is at vector/bench/data.
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("bench dir has parent")
        .join("bench/data")
}

/// normalize a vector to the unit hypersphere
fn normalize_vec(v: &mut [f32]) {
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        v.iter_mut().for_each(|x| *x /= norm);
    }
}

// -- Vector file readers ------------------------------------------------------

struct VectorFileBatchReader {
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

/// Read bvecs format (BigANN). Each record: 4-byte dimension (i32 LE) followed
/// by `dim` unsigned bytes. Values are converted to f32.
/// If `limit` is `Some(n)`, stops after reading `n` vectors.
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

// -- Ingest throughput monitor ------------------------------------------------

/// One sample produced by the ingest throughput monitor.
struct IngestThroughputSample {
    /// Wall-clock timestamp at which the sample was taken.
    wall_time: DateTime<Local>,
    /// Throughput (vectors/sec) over the preceding window (up to 5 min).
    vec_per_sec: f64,
    /// Length of the window the throughput was computed over, in seconds.
    window_secs: f64,
}

/// Spawn a task that samples the ingest counter once per minute and reports
/// the throughput over the preceding 5 minutes (or shorter, if ingest has been
/// running for less than 5 minutes). Returns a join handle that yields the
/// per-minute samples once `stop_rx` is signaled.
fn spawn_ingest_throughput_monitor(
    counter: Arc<AtomicU64>,
    ingest_start: Instant,
    mut stop_rx: oneshot::Receiver<()>,
) -> tokio::task::JoinHandle<Vec<IngestThroughputSample>> {
    const SAMPLE_PERIOD: Duration = Duration::from_secs(60);
    const WINDOW: Duration = Duration::from_secs(300);

    tokio::spawn(async move {
        // Ring of (sampled_at, cumulative_count) anchored at ingest start so
        // the first ~5 windows can fall back to the start point.
        let mut history: Vec<(Instant, u64)> = vec![(ingest_start, 0)];
        let mut samples: Vec<IngestThroughputSample> = Vec::new();
        let mut interval =
            tokio::time::interval_at(tokio::time::Instant::now() + SAMPLE_PERIOD, SAMPLE_PERIOD);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let now = Instant::now();
                    let count = counter.load(Ordering::Relaxed);
                    history.push((now, count));

                    // Pick the oldest history entry within the 5-minute window.
                    let cutoff = now.checked_sub(WINDOW);
                    let baseline = match cutoff {
                        Some(cutoff) => history
                            .iter()
                            .find(|(t, _)| *t >= cutoff)
                            .copied()
                            .unwrap_or(history[0]),
                        None => history[0],
                    };
                    let window_secs = now.duration_since(baseline.0).as_secs_f64();
                    let delta = count.saturating_sub(baseline.1);
                    let vec_per_sec = if window_secs > 0.0 {
                        delta as f64 / window_secs
                    } else {
                        0.0
                    };
                    let wall_time = Local::now();

                    println!(
                        "  [ingest throughput] {} | {:>10.1} vec/s (preceding {:.0}s, {} vectors)",
                        wall_time.format("%Y-%m-%d %H:%M:%S"),
                        vec_per_sec,
                        window_secs,
                        delta,
                    );

                    samples.push(IngestThroughputSample {
                        wall_time,
                        vec_per_sec,
                        window_secs,
                    });

                    // Trim history older than the window (keep one entry just
                    // outside so the next sample can still measure correctly).
                    if let Some(cutoff) = cutoff {
                        let mut keep_from = 0;
                        for (i, (t, _)) in history.iter().enumerate() {
                            if *t >= cutoff {
                                keep_from = i.saturating_sub(1);
                                break;
                            }
                        }
                        if keep_from > 0 {
                            history.drain(0..keep_from);
                        }
                    }
                }
                _ = &mut stop_rx => break,
            }
        }
        samples
    })
}

fn print_ingest_throughput_table(samples: &[IngestThroughputSample]) {
    if samples.is_empty() {
        println!("\n  Ingest throughput table: (no samples — ingest finished in under 60s)");
        return;
    }
    println!("\n  Ingest throughput (5-minute trailing window, sampled every minute):");
    println!("  | timestamp           | throughput (vectors/sec) | window (s) |");
    println!("  |---------------------|--------------------------|------------|");
    for s in samples {
        println!(
            "  | {} | {:>24.1} | {:>10.0} |",
            s.wall_time.format("%Y-%m-%d %H:%M:%S"),
            s.vec_per_sec,
            s.window_secs,
        );
    }
}

// -- Recall / percentile helpers ----------------------------------------------

fn recall_at_k(results: &[SearchResult], ground_truth: &[i32], k: usize) -> f64 {
    let gt_set: HashSet<i32> = ground_truth.iter().take(k).copied().collect();
    let found = results
        .iter()
        .take(k)
        .filter(|r| {
            r.vector
                .id
                .parse::<i32>()
                .map(|id| gt_set.contains(&id))
                .unwrap_or(false)
        })
        .count();
    found as f64 / k as f64
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = (p / 100.0 * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

// -- Dataset descriptors ------------------------------------------------------

#[derive(Clone, Copy)]
enum VecFormat {
    Fvecs,
    Bvecs,
}

/// Dataset descriptor.
#[derive(Clone)]
struct Dataset {
    name: &'static str,
    dimensions: u16,
    distance_metric: DistanceMetric,
    base_file: &'static str,
    query_file: &'static str,
    ground_truth_file: &'static str,
    split_threshold: usize,
    merge_threshold: usize,
    query_pruning_factor: Option<f32>,
    nprobe: usize,
    num_queries: usize,
    /// Block cache size in bytes. `None` = no cache.
    block_cache_bytes: Option<u64>,
    /// Directory containing the dataset files. When `None`, falls back to
    /// the default data directory resolved from `CARGO_MANIFEST_DIR`.
    data_dir: Option<String>,
    /// Path to a YAML file with vector `Config` overrides. When set, the config
    /// from this file is used instead of constructing one from dataset fields.
    vector_config: Option<String>,
    /// Optional path to a YAML file with a separate StorageConfig for cold-reader
    /// queries. When both writer and reader storage are SlateDb, the reader uses
    /// the writer's data path/object store and the override's settings/cache.
    reader_storage_config: Option<String>,
    /// File format for base and query vectors.
    format: VecFormat,
    /// Maximum number of base vectors to ingest. `None` = all.
    max_vectors: Option<usize>,
    normalize: bool,
    /// Number of queries allowed in flight concurrently during the warm
    /// query phase. Default: `DEFAULT_QUERY_CONCURRENCY`.
    query_concurrency: usize,
    /// Ceiling on queries-per-second during the warm query phase. The bench
    /// paces new query submissions to stay at or below this rate.
    /// Default: `DEFAULT_QUERY_QPS_LIMIT`.
    query_qps_limit: usize,
}

impl Dataset {
    fn base_path(&self, data_dir: &Path) -> PathBuf {
        data_dir.join(self.base_file)
    }

    fn query_path(&self, data_dir: &Path) -> PathBuf {
        data_dir.join(self.query_file)
    }

    fn estimated_base_vector_count(&self, data_dir: &Path) -> anyhow::Result<usize> {
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
    fn spawn_base_vector_stream(
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

    /// Load query vectors, respecting `format`.
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
        p.insert("query_concurrency", d.query_concurrency.to_string());
        p.insert("query_qps_limit", d.query_qps_limit.to_string());
        p.insert("format", format_to_str(d.format));
        if let Some(max) = d.max_vectors {
            p.insert("max_vectors", max.to_string());
        }
        if let Some(bytes) = d.block_cache_bytes {
            p.insert("block_cache_bytes", bytes.to_string());
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
        let block_cache_bytes = p
            .get_parse::<i64>("block_cache_bytes")
            .ok()
            .or(default.block_cache_bytes.map(|v| v as i64))
            .filter(|b| *b > 0)
            .map(|b| b as u64);

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
            query_concurrency: p
                .get_parse("query_concurrency")
                .unwrap_or(default.query_concurrency),
            query_qps_limit: p
                .get_parse("query_qps_limit")
                .unwrap_or(default.query_qps_limit),
            format: p.get("format").map(str_to_format).unwrap_or(default.format),
            block_cache_bytes,
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
    split_threshold: 1500,
    merge_threshold: 500,
    nprobe: 15,
    query_pruning_factor: Some(7.0),
    num_queries: DEFAULT_NUM_QUERIES,
    block_cache_bytes: Some(1073741824),
    data_dir: None,
    vector_config: None,
    reader_storage_config: None,
    format: VecFormat::Fvecs,
    max_vectors: None,
    normalize: false,
    query_concurrency: DEFAULT_QUERY_CONCURRENCY,
    query_qps_limit: DEFAULT_QUERY_QPS_LIMIT,
};

const COHERE1M: Dataset = Dataset {
    name: "cohere1m",
    dimensions: 768,
    distance_metric: DistanceMetric::Cosine,
    base_file: "cohere/cohere_base.fvecs",
    query_file: "cohere/cohere_query.fvecs",
    ground_truth_file: "cohere/cohere_groundtruth.ivecs",
    split_threshold: 200,
    merge_threshold: 50,
    query_pruning_factor: Some(0.5),
    nprobe: 100,
    num_queries: DEFAULT_NUM_QUERIES,
    block_cache_bytes: None,
    data_dir: None,
    vector_config: None,
    reader_storage_config: None,
    format: VecFormat::Fvecs,
    max_vectors: None,
    normalize: false,
    query_concurrency: DEFAULT_QUERY_CONCURRENCY,
    query_qps_limit: DEFAULT_QUERY_QPS_LIMIT,
};

const DEEP10M: Dataset = Dataset {
    name: "deep10m",
    dimensions: 96,
    distance_metric: DistanceMetric::L2,
    base_file: "deep/deep_base.fvecs",
    query_file: "deep/deep_query.fvecs",
    ground_truth_file: "deep/deep_groundtruth_10M.ivecs",
    split_threshold: 1500,
    merge_threshold: 500,
    query_pruning_factor: Some(0.5),
    nprobe: 100,
    num_queries: DEFAULT_NUM_QUERIES,
    block_cache_bytes: None,
    data_dir: None,
    vector_config: None,
    reader_storage_config: None,
    format: VecFormat::Fvecs,
    max_vectors: Some(10_000_000),
    normalize: false,
    query_concurrency: DEFAULT_QUERY_CONCURRENCY,
    query_qps_limit: DEFAULT_QUERY_QPS_LIMIT,
};

const DEEP1B: Dataset = Dataset {
    name: "deep1b",
    dimensions: 96,
    distance_metric: DistanceMetric::L2,
    base_file: "deep/deep_base.fvecs",
    query_file: "deep/deep_query.fvecs",
    ground_truth_file: "deep/deep_groundtruth_1B.ivecs",
    split_threshold: 1500,
    merge_threshold: 500,
    query_pruning_factor: Some(0.5),
    nprobe: 100,
    num_queries: DEFAULT_NUM_QUERIES,
    block_cache_bytes: None,
    data_dir: None,
    vector_config: None,
    reader_storage_config: None,
    format: VecFormat::Fvecs,
    max_vectors: None,
    normalize: false,
    query_concurrency: DEFAULT_QUERY_CONCURRENCY,
    query_qps_limit: DEFAULT_QUERY_QPS_LIMIT,
};

const WIKIPEDIA_BGE_M3_EN: Dataset = Dataset {
    name: "wikipedia_bge_m3_en",
    dimensions: 1024,
    distance_metric: DistanceMetric::DotProduct,
    base_file: "wikipedia-bge-m3/en/base.fvecs",
    query_file: "wikipedia-bge-m3/en/query.fvecs",
    ground_truth_file: "wikipedia-bge-m3/en/groundtruth.ivecs",
    split_threshold: 1500,
    merge_threshold: 500,
    query_pruning_factor: Some(0.5),
    nprobe: 100,
    num_queries: DEFAULT_NUM_QUERIES,
    block_cache_bytes: None,
    data_dir: None,
    vector_config: None,
    reader_storage_config: None,
    format: VecFormat::Fvecs,
    max_vectors: None,
    normalize: false,
    query_concurrency: DEFAULT_QUERY_CONCURRENCY,
    query_qps_limit: DEFAULT_QUERY_QPS_LIMIT,
};

const COHERE_WIKI_10M: Dataset = Dataset {
    name: "cohere_wiki_10m",
    dimensions: 1024,
    distance_metric: DistanceMetric::Cosine,
    base_file: "cohere-wiki/base.fvecs",
    query_file: "cohere-wiki/query.fvecs",
    ground_truth_file: "cohere-wiki/groundtruth.ivecs",
    split_threshold: 1500,
    merge_threshold: 500,
    query_pruning_factor: Some(0.5),
    nprobe: 100,
    num_queries: 1000,
    block_cache_bytes: None,
    data_dir: None,
    vector_config: None,
    reader_storage_config: None,
    format: VecFormat::Fvecs,
    max_vectors: Some(10_000_000),
    normalize: false,
    query_concurrency: DEFAULT_QUERY_CONCURRENCY,
    query_qps_limit: DEFAULT_QUERY_QPS_LIMIT,
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
    split_threshold: 1500,
    merge_threshold: 500,
    query_pruning_factor: Some(0.5),
    nprobe: 100,
    num_queries: DEFAULT_NUM_QUERIES,
    block_cache_bytes: None,
    data_dir: None,
    vector_config: None,
    reader_storage_config: None,
    format: VecFormat::Bvecs,
    max_vectors: Some(10_000_000),
    normalize: false,
    query_concurrency: DEFAULT_QUERY_CONCURRENCY,
    query_qps_limit: DEFAULT_QUERY_QPS_LIMIT,
};

const SIFT50M: Dataset = Dataset {
    name: "sift50m",
    dimensions: 128,
    distance_metric: DistanceMetric::L2,
    base_file: "bigann/bigann_base.bvecs",
    query_file: "bigann/bigann_query.bvecs",
    ground_truth_file: "bigann/bigann_groundtruth_50M.ivecs",
    split_threshold: 1500,
    merge_threshold: 500,
    query_pruning_factor: Some(0.5),
    nprobe: 100,
    num_queries: DEFAULT_NUM_QUERIES,
    block_cache_bytes: None,
    data_dir: None,
    vector_config: None,
    reader_storage_config: None,
    format: VecFormat::Bvecs,
    max_vectors: Some(50_000_000),
    normalize: false,
    query_concurrency: DEFAULT_QUERY_CONCURRENCY,
    query_qps_limit: DEFAULT_QUERY_QPS_LIMIT,
};

const SIFT100M: Dataset = Dataset {
    name: "sift100m",
    dimensions: 128,
    distance_metric: DistanceMetric::L2,
    base_file: "bigann/bigann_base.bvecs",
    query_file: "bigann/bigann_query.bvecs",
    ground_truth_file: "bigann/bigann_groundtruth_100M.ivecs",
    split_threshold: 1500,
    merge_threshold: 500,
    query_pruning_factor: Some(0.5),
    nprobe: 100,
    num_queries: DEFAULT_NUM_QUERIES,
    block_cache_bytes: None,
    data_dir: None,
    vector_config: None,
    reader_storage_config: None,
    format: VecFormat::Bvecs,
    max_vectors: Some(100_000_000),
    normalize: false,
    query_concurrency: DEFAULT_QUERY_CONCURRENCY,
    query_qps_limit: DEFAULT_QUERY_QPS_LIMIT,
};

const SIFT1B: Dataset = Dataset {
    name: "sift1b",
    dimensions: 128,
    distance_metric: DistanceMetric::L2,
    base_file: "bigann/bigann_base.bvecs",
    query_file: "bigann/bigann_query.bvecs",
    ground_truth_file: "bigann/bigann_groundtruth_1B.ivecs",
    split_threshold: 1500,
    merge_threshold: 500,
    query_pruning_factor: Some(0.5),
    nprobe: 100,
    num_queries: DEFAULT_NUM_QUERIES,
    block_cache_bytes: None,
    data_dir: None,
    vector_config: None,
    reader_storage_config: None,
    format: VecFormat::Bvecs,
    max_vectors: None,
    normalize: false,
    query_concurrency: DEFAULT_QUERY_CONCURRENCY,
    query_qps_limit: DEFAULT_QUERY_QPS_LIMIT,
};

const ALL_DATASETS: &[&Dataset] = &[
    &SIFT1M,
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
        let skip = skip_ingest();

        // -- Load query data --------------------------------------------------
        let queries = dataset.load_query_vectors(&data);
        let ground_truth = read_ivecs(&data.join(dataset.ground_truth_file));
        println!(
            "  Loaded {} queries, {} ground truth entries",
            queries.len(),
            ground_truth.len()
        );

        // -- Open database ----------------------------------------------------
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
        let mut sb = StorageBuilder::new(&config.storage).await?;
        if let Some(bytes) = dataset.block_cache_bytes {
            use foyer::{
                Engine, HybridCacheBuilder, HybridCachePolicy, DeviceBuilder,
            };

            let memory_capacity = usize::try_from(bytes).map_err(|_| {
                StorageError::Storage(format!(
                    "memory_capacity {} exceeds usize::MAX on this platform",
                    bytes,
                ))
            })?;
            let disk_capacity = usize::try_from(1024u64 * 1024 * 1024 * 250).map_err(|_| {
                StorageError::Storage(format!(
                    "disk_capacity {} exceeds usize::MAX on this platform",
                    1024u64 * 1024 * 1024 * 250
                ))
            })?;

            let cache = HybridCacheBuilder::new()
                .with_name("slatedb_block_cache")
                //.with_policy(policy)
                .memory(memory_capacity)
                .with_weighter(|_: &CachedKey, v: &CachedEntry| v.size())
                .storage()
                .with_io_engine_config(foyer::PsyncIoEngineConfig::new())
                .with_engine_config(
                    foyer::BlockEngineConfig::new(
                        foyer::FsDeviceBuilder::new("/mnt/cache/foyer")
                            .with_capacity(disk_capacity)
                            .build()
                            .unwrap(),
                    )
                        .with_block_size(64 * 1024)
                        .with_flushers(4)
                        .with_buffer_pool_size(256 * 1024 * 1024)
                        .with_submit_queue_size_threshold(1024 * 1024 * 1024),
                )
                .build()
                .await
                .map_err(|e| {
                    StorageError::Storage(format!("Failed to create hybrid cache: {}", e))
                })?;
            let cache = FoyerCache::new_with_opts(FoyerCacheOptions {
                max_capacity: bytes,
                shards: 16,
            });
            sb = sb.map_slatedb(|db| db.with_db_cache(std::sync::Arc::new(cache)));
            println!("  Block cache: {} bytes", bytes);
        }
        let reader_storage = dataset.resolve_reader_storage_config(&config.storage)?;
        let reader_config = ReaderConfig {
            storage: reader_storage,
            dimensions: config.dimensions,
            distance_metric: config.distance_metric,
            query_pruning_factor: config.query_pruning_factor,
            metadata_fields: config.metadata_fields.clone(),
        };
        let db = VectorDb::open_with_storage(config, sb).await?;

        // -- Ingest -----------------------------------------------------------
        let mut ingest_secs = None;
        let mut num_vectors = 0u64;

        if skip {
            println!("  Skipping ingest (VECTOR_BENCH_SKIP_INGEST=1)");
        } else {
            num_vectors = dataset.estimated_base_vector_count(&data)? as u64;
            println!(
                "  Streaming {} base vectors for {} (dim={}) in chunks of {}",
                num_vectors, dataset.name, dataset.dimensions, BASE_VECTOR_CHUNK_SIZE
            );
            let (mut stream, reader_thread) =
                dataset.spawn_base_vector_stream(&data, BASE_VECTOR_CHUNK_SIZE)?;
            let Some(first_message) = stream.recv().await else {
                anyhow::bail!("base vector stream closed before yielding any data");
            };
            let Some(mut base_vectors) = first_message? else {
                anyhow::bail!("dataset {} has no base vectors to ingest", dataset.name);
            };

            let ingest_start = Instant::now();
            let ingested_counter = Arc::new(AtomicU64::new(0));
            let (stop_tx, stop_rx) = oneshot::channel::<()>();
            let monitor_handle =
                spawn_ingest_throughput_monitor(ingested_counter.clone(), ingest_start, stop_rx);

            let num_batches = (num_vectors as usize).div_ceil(INGEST_WRITE_BATCH_SIZE);
            let mut batch_idx = 0usize;
            let mut vector_offset = 0usize;
            loop {
                println!(
                    "  Loaded chunk: {} vectors ({} / {})",
                    base_vectors.len(),
                    vector_offset + base_vectors.len(),
                    num_vectors
                );
                for (chunk_idx, chunk) in base_vectors.chunks(INGEST_WRITE_BATCH_SIZE).enumerate() {
                    let batch: Vec<Vector> = chunk
                        .iter()
                        .enumerate()
                        .map(|(i, values)| {
                            let index = vector_offset + chunk_idx * INGEST_WRITE_BATCH_SIZE + i;
                            Vector::new(index.to_string(), values.clone())
                        })
                        .collect();
                    let written = batch.len() as u64;
                    db.write(batch).await?;
                    ingested_counter.fetch_add(written, Ordering::Relaxed);
                    batch_idx += 1;
                    if batch_idx.is_multiple_of(10_000) {
                        println!("  Written batch {}/{}", batch_idx, num_batches);
                    }
                }
                vector_offset += base_vectors.len();
                let Some(message) = stream.recv().await else {
                    break;
                };
                let Some(next_batch) = message? else {
                    break;
                };
                base_vectors = next_batch;
            }
            reader_thread
                .join()
                .map_err(|_| anyhow::anyhow!("base vector streaming thread panicked"))?;
            if vector_offset != num_vectors as usize {
                anyhow::bail!(
                    "streamed {} vectors but expected {}",
                    vector_offset,
                    num_vectors
                );
            }
            db.flush().await?;
            ingest_secs = Some(ingest_start.elapsed().as_secs_f64());

            let _ = stop_tx.send(());
            let throughput_table = monitor_handle
                .await
                .map_err(|e| anyhow::anyhow!("ingest throughput monitor panicked: {e}"))?;
            print_ingest_throughput_table(&throughput_table);

            println!(
                "  Ingested {} vectors in {:.1}s ({:.0} vec/s)",
                num_vectors,
                ingest_secs.unwrap(),
                num_vectors as f64 / ingest_secs.unwrap(),
            );
        }
        println!("  Num centroids: {}", db.num_centroids());

        // -- Cold reader queries ----------------------------------------------
        println!("start cold reader phase");
        let cold_query_latency = bench.histogram("cold_query_latency_us");
        let mut cold_latencies_us = Vec::with_capacity(queries.len());
        let object_store = match &reader_config.storage {
            StorageConfig::SlateDb(slate_config) => {
                println!("CREATE STATIC OBJECT STORE");
                Some(create_object_store(&slate_config.object_store)?)
            }
            _ => None,
        };
        let mut runtime = StorageReaderRuntime::default();
        if let Some(object_store) = object_store {
            runtime = runtime.with_object_store(object_store);
        }
        if let Some(bytes) = dataset.block_cache_bytes {
                let cache = FoyerCache::new_with_opts(FoyerCacheOptions {
                    max_capacity: bytes,
                    ..Default::default()
                });
                runtime = runtime.with_block_cache(Arc::new(cache));
                println!("  Block cache: {} bytes", bytes);
        }
        for query in queries.iter().take(10) {
            let reader =
                VectorDbReader::open_with_runtime(reader_config.clone(), runtime.clone()).await?;
            let t = std::time::Instant::now();
            let q = Query::new(query.clone()).with_limit(k);
            let _ = reader
                .search_with_options(
                    &q,
                    SearchOptions {
                        nprobe: Some(dataset.nprobe),
                    },
                )
                .await?;
            let elapsed_us = t.elapsed().as_secs_f64() * 1_000_000.0;
            cold_query_latency.record(elapsed_us);
            cold_latencies_us.push(elapsed_us);
        }
        cold_latencies_us.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let cold_p50 = percentile(&cold_latencies_us, 50.0);
        let cold_p90 = percentile(&cold_latencies_us, 90.0);
        let cold_p99 = percentile(&cold_latencies_us, 99.0);
        println!(
            "  cold reader p50 = {:.2} ms, p90 = {:.2} ms, p99 = {:.2} ms",
            cold_p50 / 1000.0,
            cold_p90 / 1000.0,
            cold_p99 / 1000.0
        );
        println!("end cold reader phase");

        // -- Warmup -----------------------------------------------------------
        println!("start warmup");
        let warm_query_latency = bench.histogram("warm_query_latency_us");
        let mut warm_latencies_us = Vec::with_capacity(queries.len());
        for query in queries.iter() {
            let t = std::time::Instant::now();
            let q = Query::new(query.clone()).with_limit(k);
            let _ = db
                .search_with_options(
                    &q,
                    SearchOptions {
                        nprobe: Some(dataset.nprobe),
                    },
                )
                .await?;
            let elapsed_us = t.elapsed().as_secs_f64() * 1_000_000.0;
            warm_query_latency.record(elapsed_us);
            warm_latencies_us.push(elapsed_us);
        }
        warm_latencies_us.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let warm_p90 = percentile(&warm_latencies_us, 90.0);
        println!("warm p90 = {:.2}", warm_p90 / 1000.0);
        println!("end warmup");

        let query_latency = bench.histogram("query_latency_us");

        let concurrency = dataset.query_concurrency.max(1);
        let qps_limit = dataset.query_qps_limit.max(1);
        let tick_period = std::time::Duration::from_secs_f64(1.0 / qps_limit as f64);
        println!(
            "  warm query phase: concurrency = {}, qps_limit = {}",
            concurrency, qps_limit,
        );

        let query_start = std::time::Instant::now();
        let mut total_recall = 0.0;
        let mut latencies_us = Vec::with_capacity(queries.len());

        // Producer/consumer rate limiter:
        //   - One producer pushes a (index, vector) into the channel every
        //     `tick_period` (1/qps_limit), then closes the channel when the
        //     input is exhausted.
        //   - `concurrency` consumers pull from the channel. Each consumer
        //     loops calling `rx.recv().await` and issues the query; the
        //     `async_channel` MPMC receiver lets them compete for work
        //     without a shared lock.
        //
        // Channel is bounded to `concurrency` so a slow DB back-pressures
        // the producer rather than letting the pending-query queue grow.
        let (tx, rx) = async_channel::bounded::<(usize, Vec<f32>)>(concurrency);

        let producer = async {
            let mut ticker = tokio::time::interval(tick_period);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            for (i, query) in queries.iter().enumerate() {
                ticker.tick().await;
                // send() only errors when every receiver has dropped — that
                // only happens at shutdown, so treat it as a shutdown signal.
                if tx.send((i, query.clone())).await.is_err() {
                    break;
                }
            }
            drop(tx);
        };

        let nprobe = dataset.nprobe;
        let db_ref = &db;
        let consumers: Vec<_> = (0..concurrency)
            .map(|_| {
                let rx = rx.clone();
                async move {
                    let mut results = Vec::new();
                    while let Ok((i, query)) = rx.recv().await {
                        let q = Query::new(query).with_limit(k);
                        let t = std::time::Instant::now();
                        let search_results = db_ref
                            .search_with_options(
                                &q,
                                SearchOptions {
                                    nprobe: Some(nprobe),
                                },
                            )
                            .await?;
                        let elapsed_us = t.elapsed().as_secs_f64() * 1_000_000.0;
                        results.push((i, elapsed_us, search_results));
                    }
                    anyhow::Ok::<Vec<(usize, f64, Vec<SearchResult>)>>(results)
                }
            })
            .collect();
        // `rx` is kept alive in this scope but never consumed here; that's
        // fine — `async_channel` closes the channel when the last sender
        // drops (the producer), so consumers break out of `recv().await`
        // regardless.
        drop(rx);

        let (_, consumer_results) =
            tokio::join!(producer, futures::future::try_join_all(consumers));
        let consumer_results = consumer_results?;

        for batch in consumer_results {
            for (i, elapsed_us, results) in batch {
                query_latency.record(elapsed_us);
                latencies_us.push(elapsed_us);
                total_recall += recall_at_k(&results, &ground_truth[i], k);
            }
        }
        let query_secs = query_start.elapsed().as_secs_f64();
        let avg_recall = total_recall / queries.len() as f64;
        let qps = queries.len() as f64 / query_secs;

        latencies_us.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p50 = percentile(&latencies_us, 50.0);
        let p90 = percentile(&latencies_us, 90.0);
        let p99 = percentile(&latencies_us, 99.0);

        println!(
            "  recall@{} = {:.4}, QPS = {:.1}, avg = {:.2} ms, p50 = {:.2} ms, p90 = {:.2} ms, p99 = {:.2} ms",
            k,
            avg_recall,
            qps,
            (query_secs / queries.len() as f64) * 1000.0,
            p50 / 1000.0,
            p90 / 1000.0,
            p99 / 1000.0,
        );

        let mut summary = Summary::new()
            .add("recall_at_k", avg_recall)
            .add("k", k as f64)
            .add("qps", qps)
            .add("num_queries", queries.len() as f64)
            .add("num_centroids", db.num_centroids() as f64)
            .add("cold_p50_latency_us", cold_p50)
            .add("cold_p90_latency_us", cold_p90)
            .add("cold_p99_latency_us", cold_p99)
            .add("p50_latency_us", p50)
            .add("p90_latency_us", p90)
            .add("p99_latency_us", p99);
        if let Some(secs) = ingest_secs {
            summary = summary
                .add("num_vectors", num_vectors as f64)
                .add("ingest_secs", secs)
                .add("ingest_vec_per_sec", num_vectors as f64 / secs);
        }
        bench.summarize(summary).await?;

        bench.close().await?;
        Ok(())
    }
}
