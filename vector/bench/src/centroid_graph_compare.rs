use std::hint::black_box;

use bencher::{Bench, Benchmark, Params, Summary};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use vector::DistanceMetric;
use vector::hnsw::{CentroidGraph, SptagCentroidGraph, UsearchCentroidGraph};
use vector::serde::centroid_chunk::CentroidEntry;

const DEFAULT_DIMENSIONS: usize = 768;
const DEFAULT_NUM_QUERIES: usize = 200;
const DEFAULT_K: usize = 25;
const DEFAULT_SEED: u64 = 7;

pub(crate) struct CentroidGraphCompareBenchmark;

impl CentroidGraphCompareBenchmark {
    pub(crate) fn new() -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl Benchmark for CentroidGraphCompareBenchmark {
    fn name(&self) -> &str {
        "centroid_graph_compare"
    }

    fn default_params(&self) -> Vec<Params> {
        [1_000usize, 10_000, 50_000, 100_000]
            .into_iter()
            .map(|num_vectors| {
                let mut params = Params::new();
                params.insert("num_vectors", num_vectors.to_string());
                params.insert("dimensions", DEFAULT_DIMENSIONS.to_string());
                params.insert("num_queries", DEFAULT_NUM_QUERIES.to_string());
                params.insert("k", DEFAULT_K.to_string());
                params.insert("seed", DEFAULT_SEED.to_string());
                params
            })
            .collect()
    }

    async fn run(&self, bench: Bench) -> anyhow::Result<()> {
        let num_vectors: usize = bench.spec().params().get_parse("num_vectors")?;
        let dimensions: usize = bench.spec().params().get_parse("dimensions")?;
        let num_queries: usize = bench.spec().params().get_parse("num_queries")?;
        let k: usize = bench.spec().params().get_parse("k")?;
        let seed: u64 = bench.spec().params().get_parse("seed")?;

        println!(
            "Comparing centroid graphs with {} centroids, {} queries (dim={}, k={}, seed={})",
            num_vectors, num_queries, dimensions, k, seed
        );

        let centroids = generate_vectors(num_vectors, dimensions, seed);
        let queries = generate_queries(num_queries, dimensions, seed.wrapping_add(1));

        let usearch_build_start = std::time::Instant::now();
        let usearch = UsearchCentroidGraph::build(centroids.clone(), DistanceMetric::L2)?;
        let usearch_build_secs = usearch_build_start.elapsed().as_secs_f64();

        let sptag_build_start = std::time::Instant::now();
        let sptag = SptagCentroidGraph::build(centroids, DistanceMetric::L2)?;
        let sptag_build_secs = sptag_build_start.elapsed().as_secs_f64();

        let (usearch_query_secs, usearch_checksum) = time_queries(&usearch, &queries, k);
        let (sptag_query_secs, sptag_checksum) = time_queries(&sptag, &queries, k);

        let usearch_build_rate = num_vectors as f64 / usearch_build_secs.max(f64::MIN_POSITIVE);
        let sptag_build_rate = num_vectors as f64 / sptag_build_secs.max(f64::MIN_POSITIVE);
        let usearch_qps = num_queries as f64 / usearch_query_secs.max(f64::MIN_POSITIVE);
        let sptag_qps = num_queries as f64 / sptag_query_secs.max(f64::MIN_POSITIVE);

        println!(
            "  usearch: build {:.3}s, query {:.3}s, qps {:.0}",
            usearch_build_secs, usearch_query_secs, usearch_qps
        );
        println!(
            "  sptag:   build {:.3}s, query {:.3}s, qps {:.0}",
            sptag_build_secs, sptag_query_secs, sptag_qps
        );

        bench
            .summarize(
                Summary::new()
                    .add("num_vectors", num_vectors as f64)
                    .add("dimensions", dimensions as f64)
                    .add("num_queries", num_queries as f64)
                    .add("k", k as f64)
                    .add("usearch_build_secs", usearch_build_secs)
                    .add("sptag_build_secs", sptag_build_secs)
                    .add(
                        "sptag_vs_usearch_build_ratio",
                        sptag_build_secs / usearch_build_secs.max(f64::MIN_POSITIVE),
                    )
                    .add("usearch_build_centroids_per_sec", usearch_build_rate)
                    .add("sptag_build_centroids_per_sec", sptag_build_rate)
                    .add("usearch_query_secs", usearch_query_secs)
                    .add("sptag_query_secs", sptag_query_secs)
                    .add(
                        "sptag_vs_usearch_query_ratio",
                        sptag_query_secs / usearch_query_secs.max(f64::MIN_POSITIVE),
                    )
                    .add("usearch_qps", usearch_qps)
                    .add("sptag_qps", sptag_qps)
                    .add("usearch_checksum", usearch_checksum as f64)
                    .add("sptag_checksum", sptag_checksum as f64),
            )
            .await?;
        bench.close().await?;
        Ok(())
    }
}

fn time_queries<G: CentroidGraph>(graph: &G, queries: &[Vec<f32>], k: usize) -> (f64, u64) {
    let start = std::time::Instant::now();
    let mut checksum = 0u64;
    for query in queries {
        let results = black_box(graph.search(query, k));
        for centroid_id in results {
            checksum = checksum.wrapping_add(centroid_id);
        }
    }
    (start.elapsed().as_secs_f64(), checksum)
}

fn generate_vectors(num_vectors: usize, dimensions: usize, seed: u64) -> Vec<CentroidEntry> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut centroids = Vec::with_capacity(num_vectors);

    for centroid_id in 0..num_vectors {
        let mut vector = Vec::with_capacity(dimensions);
        let mut norm_sq = 0.0f32;
        for _ in 0..dimensions {
            let value = rng.gen_range(-1.0f32..1.0f32);
            norm_sq += value * value;
            vector.push(value);
        }

        let norm = norm_sq.sqrt();
        if norm > 0.0 {
            for value in &mut vector {
                *value /= norm;
            }
        }

        centroids.push(CentroidEntry::new(centroid_id as u64, vector));
    }

    centroids
}

fn generate_queries(num_queries: usize, dimensions: usize, seed: u64) -> Vec<Vec<f32>> {
    generate_vectors(num_queries, dimensions, seed)
        .into_iter()
        .map(|entry| entry.vector)
        .collect()
}
