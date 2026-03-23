//! Criterion microbenchmark for 1024-dimensional L2 distance.
//!
//! Run with:
//!   cargo bench -p opendata-vector --features bench-internals --bench l2_distance

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use rand::{Rng, SeedableRng, rngs::StdRng};
use rayon::prelude::*;

const DIMENSIONS: usize = 1024;
const DATASET_SIZE: usize = 10_000;

fn make_vector(multiplier: f32, offset: f32) -> Vec<f32> {
    (0..DIMENSIONS)
        .map(|i| ((i % 97) as f32 * multiplier) + offset)
        .collect()
}

fn make_random_vector(seed: u64) -> Vec<f32> {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..DIMENSIONS).map(|_| rng.gen_range(-1.0..1.0)).collect()
}

fn make_random_dataset(seed: u64) -> Vec<f32> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut dataset = Vec::with_capacity(DATASET_SIZE * DIMENSIONS);
    for _ in 0..(DATASET_SIZE * DIMENSIONS) {
        dataset.push(rng.gen_range(-1.0..1.0));
    }
    dataset
}

fn score_dataset_scalar(query: &[f32], dataset: &[f32]) -> f32 {
    dataset
        .chunks_exact(DIMENSIONS)
        .map(|candidate| vector::distance::bench_l2_distance_scalar(query, candidate))
        .sum()
}

fn score_dataset_avx(query: &[f32], dataset: &[f32]) -> Option<f32> {
    let mut sum = 0.0;
    for candidate in dataset.chunks_exact(DIMENSIONS) {
        sum += vector::distance::bench_l2_distance_avx(query, candidate)?;
    }
    Some(sum)
}

fn score_dataset_rayon(query: &[f32], dataset: &[f32]) -> f32 {
    dataset
        .par_chunks_exact(DIMENSIONS)
        .map(|candidate| vector::distance::bench_l2_distance_runtime(query, candidate))
        .sum()
}

fn score_dataset_rayon_scalar(query: &[f32], dataset: &[f32]) -> f32 {
    dataset
        .par_chunks_exact(DIMENSIONS)
        .map(|candidate| vector::distance::bench_l2_distance_scalar(query, candidate))
        .sum()
}

fn bench_l2_distance_single_pair(c: &mut Criterion) {
    // given
    let a = make_vector(0.03125, -2.0);
    let b = make_vector(0.046875, 1.5);

    let mut group = c.benchmark_group("distance/l2/1024d");
    group.throughput(Throughput::Elements(DIMENSIONS as u64));

    group.bench_with_input(
        BenchmarkId::new("scalar", "avx_off"),
        &(&a, &b),
        |bench, (lhs, rhs)| {
            bench.iter(|| {
                black_box(vector::distance::bench_l2_distance_scalar(
                    black_box(lhs),
                    black_box(rhs),
                ))
            })
        },
    );

    if vector::distance::bench_l2_distance_avx_available() {
        group.bench_with_input(
            BenchmarkId::new("avx", "avx_on"),
            &(&a, &b),
            |bench, (lhs, rhs)| {
                bench.iter(|| {
                    black_box(
                        vector::distance::bench_l2_distance_avx(black_box(lhs), black_box(rhs))
                            .expect("AVX benchmark should only run when AVX is available"),
                    )
                })
            },
        );
    }

    group.finish();
}

fn bench_l2_distance_dataset(c: &mut Criterion) {
    // given
    let query = make_random_vector(42);
    let dataset = make_random_dataset(7);

    let mut group = c.benchmark_group("distance/l2/1024d_dataset_10k");
    group.throughput(Throughput::Elements(DATASET_SIZE as u64));

    group.bench_with_input(
        BenchmarkId::new("scalar", "avx_off"),
        &(&query, &dataset),
        |bench, (query, dataset)| {
            bench.iter(|| {
                black_box(score_dataset_scalar(
                    black_box(query.as_slice()),
                    black_box(dataset.as_slice()),
                ))
            })
        },
    );

    if vector::distance::bench_l2_distance_avx_available() {
        group.bench_with_input(
            BenchmarkId::new("avx", "avx_on"),
            &(&query, &dataset),
            |bench, (query, dataset)| {
                bench.iter(|| {
                    black_box(
                        score_dataset_avx(
                            black_box(query.as_slice()),
                            black_box(dataset.as_slice()),
                        )
                        .expect("AVX benchmark should only run when AVX is available"),
                    )
                })
            },
        );
    }

    group.bench_with_input(
        BenchmarkId::new("rayon", "runtime_dispatch"),
        &(&query, &dataset),
        |bench, (query, dataset)| {
            bench.iter(|| {
                black_box(score_dataset_rayon(
                    black_box(query.as_slice()),
                    black_box(dataset.as_slice()),
                ))
            })
        },
    );

    group.bench_with_input(
        BenchmarkId::new("rayon_scalar", "avx_off"),
        &(&query, &dataset),
        |bench, (query, dataset)| {
            bench.iter(|| {
                black_box(score_dataset_rayon_scalar(
                    black_box(query.as_slice()),
                    black_box(dataset.as_slice()),
                ))
            })
        },
    );

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(20);
    targets = bench_l2_distance_single_pair, bench_l2_distance_dataset
}
criterion_main!(benches);
