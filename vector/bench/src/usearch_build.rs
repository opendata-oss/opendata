use std::collections::HashSet;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};

use anyhow::{Context, ensure};
use bencher::{Bench, Benchmark, Params, Summary};
use vector::DistanceMetric;
use vector::hnsw::{CentroidGraph, UsearchBuildOptions, UsearchCentroidGraph};
use vector::serde::centroid_chunk::CentroidEntry;

const COHERE_BASE_FILE: &str = "cohere/cohere_base.fvecs";
const COHERE_QUERY_FILE: &str = "cohere/cohere_query.fvecs";
const DEFAULT_K: usize = 10;
const DEFAULT_NUM_QUERIES: usize = 100;

pub(crate) struct UsearchBenchmark;

impl UsearchBenchmark {
    pub(crate) fn new() -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl Benchmark for UsearchBenchmark {
    fn name(&self) -> &str {
        "usearch"
    }

    fn aliases(&self) -> &[&str] {
        &["usearch_build"]
    }

    fn default_params(&self) -> Vec<Params> {
        [1_000usize, 10_000, 50_000, 100_000]
            .into_iter()
            .map(|num_vectors| {
                let mut params = Params::new();
                params.insert("num_vectors", num_vectors.to_string());
                params.insert("num_queries", DEFAULT_NUM_QUERIES.to_string());
                params.insert("k", DEFAULT_K.to_string());
                params.insert("parallel_inserts", "true");
                params
            })
            .collect()
    }

    async fn run(&self, bench: Bench) -> anyhow::Result<()> {
        let params = bench.spec().params();
        let num_vectors: usize = params.get_parse("num_vectors")?;
        let num_queries: usize = params
            .get_parse("num_queries")
            .unwrap_or(DEFAULT_NUM_QUERIES);
        let requested_k: usize = params.get_parse("k").unwrap_or(DEFAULT_K);
        let parallel_inserts: bool = params.get_parse("parallel_inserts").unwrap_or(true);
        let data = params
            .get("data_dir")
            .map(PathBuf::from)
            .unwrap_or_else(default_data_dir);

        ensure!(num_vectors > 0, "num_vectors must be > 0");
        ensure!(num_queries > 0, "num_queries must be > 0");

        println!(
            "Building usearch graph with {} Cohere vectors (queries={}, k={}, parallel_inserts={})",
            num_vectors, num_queries, requested_k, parallel_inserts
        );

        let mut base_vectors = read_fvecs_limit(&data.join(COHERE_BASE_FILE), num_vectors)?;
        ensure!(
            base_vectors.len() == num_vectors,
            "requested {} base vectors, found {} in {}",
            num_vectors,
            base_vectors.len(),
            data.join(COHERE_BASE_FILE).display()
        );
        for vector in &mut base_vectors {
            normalize_vec(vector);
        }

        let mut query_vectors = read_fvecs_limit(&data.join(COHERE_QUERY_FILE), num_queries)?;
        ensure!(
            query_vectors.len() == num_queries,
            "requested {} query vectors, found {} in {}",
            num_queries,
            query_vectors.len(),
            data.join(COHERE_QUERY_FILE).display()
        );
        for vector in &mut query_vectors {
            normalize_vec(vector);
        }

        let dimensions = base_vectors[0].len();
        if let Ok(expected_dimensions) = params.get_parse::<usize>("dimensions") {
            ensure!(
                expected_dimensions == dimensions,
                "configured dimensions {} did not match Cohere dataset dimensions {}",
                expected_dimensions,
                dimensions
            );
        }
        ensure!(
            query_vectors
                .iter()
                .all(|vector| vector.len() == dimensions),
            "query vectors had inconsistent dimensions"
        );

        let k = requested_k.min(num_vectors);
        ensure!(k > 0, "k must be > 0");

        let centroids: Vec<_> = base_vectors
            .iter()
            .enumerate()
            .map(|(centroid_id, vector)| CentroidEntry::new(centroid_id as u64, vector.clone()))
            .collect();

        let build_start = std::time::Instant::now();
        let graph = UsearchCentroidGraph::build_with_options(
            centroids,
            DistanceMetric::DotProduct,
            UsearchBuildOptions { parallel_inserts },
        )?;
        let build_secs = build_start.elapsed().as_secs_f64();
        let throughput = num_vectors as f64 / build_secs.max(f64::MIN_POSITIVE);

        println!(
            "  built {} vectors in {:.3}s ({:.0} vectors/s)",
            graph.len(),
            build_secs,
            throughput
        );

        let exact_results: Vec<Vec<u64>> = query_vectors
            .iter()
            .map(|query| exact_top_k(&base_vectors, query, k))
            .collect();

        let query_start = std::time::Instant::now();
        let mut total_recall = 0.0;
        let mut latencies_us = Vec::with_capacity(query_vectors.len());
        for (query, exact) in query_vectors.iter().zip(exact_results.iter()) {
            let start = std::time::Instant::now();
            let results = graph.search(query, k);
            let elapsed_us = start.elapsed().as_secs_f64() * 1_000_000.0;
            latencies_us.push(elapsed_us);
            total_recall += recall_at_k(&results, exact, k);
        }
        let query_secs = query_start.elapsed().as_secs_f64();
        let avg_recall = total_recall / query_vectors.len() as f64;
        let qps = query_vectors.len() as f64 / query_secs.max(f64::MIN_POSITIVE);

        latencies_us.sort_by(|left, right| left.total_cmp(right));
        let p50 = percentile(&latencies_us, 50.0);
        let p90 = percentile(&latencies_us, 90.0);
        let p99 = percentile(&latencies_us, 99.0);
        let avg_latency_us = latencies_us.iter().sum::<f64>() / latencies_us.len() as f64;

        println!(
            "  recall@{} = {:.4}, QPS = {:.1}, avg = {:.2} ms, p50 = {:.2} ms, p90 = {:.2} ms, p99 = {:.2} ms",
            k,
            avg_recall,
            qps,
            avg_latency_us / 1000.0,
            p50 / 1000.0,
            p90 / 1000.0,
            p99 / 1000.0,
        );

        bench
            .summarize(
                Summary::new()
                    .add("num_vectors", num_vectors as f64)
                    .add("num_queries", query_vectors.len() as f64)
                    .add("dimensions", dimensions as f64)
                    .add("k", k as f64)
                    .add(
                        "parallel_inserts_enabled",
                        if parallel_inserts { 1.0 } else { 0.0 },
                    )
                    .add("build_secs", build_secs)
                    .add("build_vectors_per_sec", throughput)
                    .add("built_vectors", graph.len() as f64)
                    .add("recall_at_k", avg_recall)
                    .add("qps", qps)
                    .add("avg_query_latency_us", avg_latency_us)
                    .add("p50_query_latency_us", p50)
                    .add("p90_query_latency_us", p90)
                    .add("p99_query_latency_us", p99),
            )
            .await?;
        bench.close().await?;
        Ok(())
    }
}

fn default_data_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("bench dir has parent")
        .join("bench/data")
}

fn read_fvecs_limit(path: &Path, limit: usize) -> anyhow::Result<Vec<Vec<f32>>> {
    let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut vectors = Vec::with_capacity(limit);
    let mut dim_buf = [0u8; 4];

    while vectors.len() < limit && reader.read_exact(&mut dim_buf).is_ok() {
        let dim = u32::from_le_bytes(dim_buf) as usize;
        let mut values = vec![0f32; dim];
        let byte_slice =
            unsafe { std::slice::from_raw_parts_mut(values.as_mut_ptr() as *mut u8, dim * 4) };
        reader
            .read_exact(byte_slice)
            .with_context(|| format!("failed to read fvecs values from {}", path.display()))?;
        vectors.push(values);
    }

    Ok(vectors)
}

fn normalize_vec(vector: &mut [f32]) {
    let norm: f32 = vector.iter().map(|value| value * value).sum::<f32>().sqrt();
    if norm > 0.0 {
        vector.iter_mut().for_each(|value| *value /= norm);
    }
}

fn exact_top_k(base_vectors: &[Vec<f32>], query: &[f32], k: usize) -> Vec<u64> {
    let mut scored = Vec::with_capacity(base_vectors.len());
    for (vector_id, candidate) in base_vectors.iter().enumerate() {
        let distance = dot_product_distance(query, candidate);
        scored.push((vector_id as u64, distance));
    }

    scored.sort_unstable_by(|left, right| {
        left.1
            .total_cmp(&right.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    scored
        .into_iter()
        .take(k)
        .map(|(vector_id, _)| vector_id)
        .collect()
}

fn dot_product_distance(left: &[f32], right: &[f32]) -> f32 {
    1.0 - left
        .iter()
        .zip(right.iter())
        .map(|(l, r)| l * r)
        .sum::<f32>()
}

fn recall_at_k(results: &[u64], ground_truth: &[u64], k: usize) -> f64 {
    let ground_truth: HashSet<u64> = ground_truth.iter().take(k).copied().collect();
    let found = results
        .iter()
        .take(k)
        .filter(|vector_id| ground_truth.contains(vector_id))
        .count();
    found as f64 / k as f64
}

fn percentile(sorted: &[f64], percentile: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }

    let index = (percentile / 100.0 * (sorted.len() - 1) as f64).round() as usize;
    sorted[index.min(sorted.len() - 1)]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_compute_recall_at_k() {
        // given
        let results = vec![1, 2, 3];
        let ground_truth = vec![2, 4, 1];

        // when
        let recall = recall_at_k(&results, &ground_truth, 2);

        // then
        assert_eq!(recall, 0.5);
    }

    #[test]
    fn should_return_exact_top_k_using_dot_product_distance() {
        // given
        let base_vectors = vec![vec![1.0, 0.0], vec![0.0, 1.0], vec![0.8, 0.2]];
        let query = vec![1.0, 0.0];

        // when
        let nearest = exact_top_k(&base_vectors, &query, 2);

        // then
        assert_eq!(nearest, vec![0, 2]);
    }
}
