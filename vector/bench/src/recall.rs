//! Recall benchmark for the vector database.
//!
//! Ingests a dataset (sift1m or cohere1m), then runs queries and measures
//! recall@k against ground truth.
//!
//! Environment variables:
//! - `VECTOR_BENCH_DATASET`: Target dataset (`sift1m`, `cohere1m`, `sift10m`,
//!   `sift50m`, `sift100m`, `sift1b`). If unset, runs all datasets whose data
//!   files are present.
//! - `VECTOR_BENCH_NUM_QUERIES`: Number of queries to run (default: 100).
//! - `VECTOR_BENCH_CONFIG`: Path to a YAML file with vector `Config` overrides
//!   (storage, thresholds, etc.). When set, the config from this file is used
//!   instead of the defaults. This allows persistent storage for reuse across runs.
//! - `VECTOR_BENCH_SKIP_INGEST`: Set to `1` to skip the ingest phase and query
//!   an existing database (requires persistent storage via `VECTOR_BENCH_CONFIG`).
//! - `SLATEDB_BLOCK_CACHE`: Block cache size in bytes (e.g. `536870912` for 512MB).

use std::collections::HashSet;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};

use bencher::{Bench, Benchmark, Params, Summary};
use common::StorageRuntime;
use common::storage::factory::{FoyerCache, FoyerCacheOptions};
use vector::{Config, DistanceMetric, SearchResult, Vector, VectorDb};

const DEFAULT_NUM_QUERIES: usize = 100;

fn num_queries() -> usize {
    std::env::var("VECTOR_BENCH_NUM_QUERIES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_NUM_QUERIES)
}

fn load_config_file() -> Option<Config> {
    let path = std::env::var("VECTOR_BENCH_CONFIG").ok()?;
    let contents =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("failed to read {}: {}", path, e));
    let config: Config = serde_yaml::from_str(&contents)
        .unwrap_or_else(|e| panic!("failed to parse {}: {}", path, e));
    println!("  Config: {:?}", config);
    Some(config)
}

fn skip_ingest() -> bool {
    std::env::var("VECTOR_BENCH_SKIP_INGEST")
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

fn data_dir() -> PathBuf {
    // When running from workspace root via `cargo run -p vector-bench`,
    // CARGO_MANIFEST_DIR points to vector/bench. Data is at vector/tests/data.
    // TODO: fix me and make me configurable
    /*Path::new(env!("CARGO_MANIFEST_DIR"))
    .parent()
    .expect("bench dir has parent")
    .join("tests/data")*/
    PathBuf::from("/mnt/cache")
}

// -- Vector file readers ------------------------------------------------------

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
        if let Some(n) = limit {
            if vectors.len() >= n {
                break;
            }
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

// -- Recall / percentile helpers ----------------------------------------------

fn recall_at_k(results: &[SearchResult], ground_truth: &[i32], k: usize) -> f64 {
    let gt_set: HashSet<i32> = ground_truth.iter().take(k).copied().collect();
    let found = results
        .iter()
        .take(k)
        .filter(|r| {
            r.external_id
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
struct Dataset {
    name: &'static str,
    dimensions: u16,
    distance_metric: DistanceMetric,
    base_file: &'static str,
    query_file: &'static str,
    ground_truth_file: &'static str,
    split_threshold: usize,
    merge_threshold: usize,
    nprobe: usize,
    /// File format for base and query vectors.
    format: VecFormat,
    /// Maximum number of base vectors to ingest. `None` = all.
    max_vectors: Option<usize>,
}

impl Dataset {
    /// Load base vectors, respecting `format` and `max_vectors`.
    fn load_base_vectors(&self, data_dir: &Path) -> Vec<Vec<f32>> {
        let path = data_dir.join(self.base_file);
        match self.format {
            VecFormat::Fvecs => {
                let vecs = read_fvecs(&path);
                match self.max_vectors {
                    Some(n) => vecs.into_iter().take(n).collect(),
                    None => vecs,
                }
            }
            VecFormat::Bvecs => read_bvecs(&path, self.max_vectors),
        }
    }

    /// Load query vectors, respecting `format`.
    fn load_query_vectors(&self, data_dir: &Path, nqueries: usize) -> Vec<Vec<f32>> {
        let path = data_dir.join(self.query_file);
        match self.format {
            VecFormat::Fvecs => read_fvecs(&path).into_iter().take(nqueries).collect(),
            VecFormat::Bvecs => read_bvecs(&path, Some(nqueries)),
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
    nprobe: 100,
    format: VecFormat::Fvecs,
    max_vectors: None,
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
    nprobe: 100,
    format: VecFormat::Fvecs,
    max_vectors: None,
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
    nprobe: 100,
    format: VecFormat::Bvecs,
    max_vectors: Some(10_000_000),
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
    nprobe: 100,
    format: VecFormat::Bvecs,
    max_vectors: Some(50_000_000),
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
    nprobe: 100,
    format: VecFormat::Bvecs,
    max_vectors: Some(100_000_000),
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
    nprobe: 100,
    format: VecFormat::Bvecs,
    max_vectors: None,
};

const ALL_DATASETS: &[&Dataset] = &[&SIFT1M, &COHERE1M, &SIFT10M, &SIFT50M, &SIFT100M, &SIFT1B];

fn target_dataset() -> Option<String> {
    std::env::var("VECTOR_BENCH_DATASET").ok()
}

fn lookup_dataset(name: &str) -> Option<&'static Dataset> {
    ALL_DATASETS.iter().find(|d| d.name == name).copied()
}

fn make_params(dataset: &Dataset) -> Params {
    let mut params = Params::new();
    params.insert("dataset", dataset.name.to_string());
    params.insert("nprobe", dataset.nprobe.to_string());
    params.insert("num_queries", num_queries().to_string());
    params
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
        let data = data_dir();
        let datasets: Vec<&Dataset> = match target_dataset() {
            Some(name) => match lookup_dataset(&name) {
                Some(d) => vec![d],
                None => {
                    let valid: Vec<_> = ALL_DATASETS.iter().map(|d| d.name).collect();
                    panic!(
                        "unknown VECTOR_BENCH_DATASET '{}'. valid: {}",
                        name,
                        valid.join(", ")
                    );
                }
            },
            None => ALL_DATASETS.to_vec(),
        };
        datasets
            .into_iter()
            .filter(|d| data.join(d.base_file).exists())
            .map(make_params)
            .collect()
    }

    async fn run(&self, bench: Bench) -> anyhow::Result<()> {
        let dataset_name: String = bench.spec().params().get_parse("dataset")?;
        let nprobe: usize = bench.spec().params().get_parse("nprobe")?;
        let nqueries: usize = bench.spec().params().get_parse("num_queries")?;

        let dataset = lookup_dataset(&dataset_name)
            .ok_or_else(|| anyhow::anyhow!("unknown dataset: {}", dataset_name))?;

        let data = data_dir();
        let k = 10;
        let skip = skip_ingest();

        // -- Load query data --------------------------------------------------
        let queries = dataset.load_query_vectors(&data, nqueries);
        let ground_truth = read_ivecs(&data.join(dataset.ground_truth_file));
        println!(
            "  Loaded {} queries, {} ground truth entries",
            queries.len(),
            ground_truth.len()
        );

        // -- Open database ----------------------------------------------------
        let config = match load_config_file() {
            Some(config) => config,
            None => Config {
                storage: bench.spec().data().storage.clone(),
                dimensions: dataset.dimensions,
                distance_metric: dataset.distance_metric,
                split_threshold_vectors: dataset.split_threshold,
                merge_threshold_vectors: dataset.merge_threshold,
                ..Default::default()
            },
        };
        let mut runtime = StorageRuntime::new();
        if let Ok(val) = std::env::var("SLATEDB_BLOCK_CACHE") {
            let bytes: u64 = val
                .parse()
                .expect("SLATEDB_BLOCK_CACHE must be a byte count");
            let cache = FoyerCache::new_with_opts(FoyerCacheOptions {
                max_capacity: bytes,
                ..Default::default()
            });
            runtime = runtime.with_block_cache(std::sync::Arc::new(cache));
            println!("  Block cache: {} bytes", bytes);
        }
        let db = VectorDb::open_with_runtime(config, runtime).await?;

        // -- Ingest -----------------------------------------------------------
        let mut ingest_secs = None;
        let mut num_vectors = 0u64;
        if skip {
            println!("  Skipping ingest (VECTOR_BENCH_SKIP_INGEST=1)");
        } else {
            println!("  Loading {} base vectors...", dataset.name);
            let base_vectors = dataset.load_base_vectors(&data);
            println!(
                "  Loaded {} base vectors (dim={})",
                base_vectors.len(),
                dataset.dimensions
            );
            num_vectors = base_vectors.len() as u64;

            let ingest_start = std::time::Instant::now();
            let batch_size = 10;
            let num_batches = base_vectors.len().div_ceil(batch_size);
            for (batch_idx, chunk) in base_vectors.chunks(batch_size).enumerate() {
                let batch: Vec<Vector> = chunk
                    .iter()
                    .enumerate()
                    .map(|(i, values)| {
                        let index = batch_idx * batch_size + i;
                        Vector::new(index.to_string(), values.clone())
                    })
                    .collect();
                db.write(batch).await?;
                if (batch_idx + 1) % 10_000 == 0 {
                    println!("  Written batch {}/{}", batch_idx + 1, num_batches);
                }
            }
            db.flush().await?;
            ingest_secs = Some(ingest_start.elapsed().as_secs_f64());
            println!(
                "  Ingested {} vectors in {:.1}s ({:.0} vec/s)",
                num_vectors,
                ingest_secs.unwrap(),
                num_vectors as f64 / ingest_secs.unwrap(),
            );
        }
        println!("  Num centroids: {}", db.num_centroids());

        // -- Query & measure recall -------------------------------------------
        println!("start warmup");
        let query_latency = bench.histogram("cold_query_latency_us");
        let mut cold_latencies_us = Vec::with_capacity(queries.len());
        for (_, query) in queries.iter().enumerate() {
            let t = std::time::Instant::now();
            let _ = db.search_with_nprobe(query, k, nprobe).await?;
            let elapsed_us = t.elapsed().as_secs_f64() * 1_000_000.0;
            query_latency.record(elapsed_us);
            cold_latencies_us.push(elapsed_us);
        }
        let p90 = percentile(&cold_latencies_us, 90.0);
        println!("p90 = {:.2}", p90 / 1000.0);
        println!("end warmup");

        let query_latency = bench.histogram("query_latency_us");

        let query_start = std::time::Instant::now();
        let mut total_recall = 0.0;
        let mut latencies_us = Vec::with_capacity(queries.len());
        for (i, query) in queries.iter().enumerate() {
            let t = std::time::Instant::now();
            let results = db.search_with_nprobe(query, k, nprobe).await?;
            let elapsed_us = t.elapsed().as_secs_f64() * 1_000_000.0;
            query_latency.record(elapsed_us);
            latencies_us.push(elapsed_us);
            total_recall += recall_at_k(&results, &ground_truth[i], k);
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
