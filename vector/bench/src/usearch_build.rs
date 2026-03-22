use bencher::{Bench, Benchmark, Params, Summary};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use vector::DistanceMetric;
use vector::hnsw::{CentroidGraph, UsearchCentroidGraph};
use vector::serde::centroid_chunk::CentroidEntry;

const DEFAULT_DIMENSIONS: usize = 768;
const DEFAULT_SEED: u64 = 7;

pub(crate) struct UsearchBuildBenchmark;

impl UsearchBuildBenchmark {
    pub(crate) fn new() -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl Benchmark for UsearchBuildBenchmark {
    fn name(&self) -> &str {
        "usearch_build"
    }

    fn default_params(&self) -> Vec<Params> {
        [1_000usize, 10_000, 50_000, 100_000]
            .into_iter()
            .map(|num_vectors| {
                let mut params = Params::new();
                params.insert("num_vectors", num_vectors.to_string());
                params.insert("dimensions", DEFAULT_DIMENSIONS.to_string());
                params.insert("seed", DEFAULT_SEED.to_string());
                params
            })
            .collect()
    }

    async fn run(&self, bench: Bench) -> anyhow::Result<()> {
        let num_vectors: usize = bench.spec().params().get_parse("num_vectors")?;
        let dimensions: usize = bench.spec().params().get_parse("dimensions")?;
        let seed: u64 = bench.spec().params().get_parse("seed")?;

        println!(
            "Building usearch graph with {} centroids (dim={}, seed={})",
            num_vectors, dimensions, seed
        );

        let centroids = generate_centroids(num_vectors, dimensions, seed);

        let start = std::time::Instant::now();
        let graph = UsearchCentroidGraph::build(centroids, DistanceMetric::L2)?;
        let build_secs = start.elapsed().as_secs_f64();
        let throughput = num_vectors as f64 / build_secs.max(f64::MIN_POSITIVE);

        println!(
            "  built {} centroids in {:.3}s ({:.0} centroids/s)",
            graph.len(),
            build_secs,
            throughput
        );

        bench
            .summarize(
                Summary::new()
                    .add("num_vectors", num_vectors as f64)
                    .add("dimensions", dimensions as f64)
                    .add("build_secs", build_secs)
                    .add("build_centroids_per_sec", throughput)
                    .add("built_centroids", graph.len() as f64),
            )
            .await?;
        bench.close().await?;
        Ok(())
    }
}

fn generate_centroids(num_vectors: usize, dimensions: usize, seed: u64) -> Vec<CentroidEntry> {
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
