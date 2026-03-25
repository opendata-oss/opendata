//! Criterion microbenchmark for scoring and ranking 10K 1024-dimensional vectors.
//!
//! Run with:
//!   cargo bench -p opendata-vector --features bench-internals --bench l2_score_and_rank

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use rand::{Rng, SeedableRng, rngs::StdRng};
use rayon::prelude::*;
use std::cmp::Ordering;

const DIMENSIONS: usize = 1024;
const DATASET_SIZE: usize = 10_000;

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

fn score_and_rank_scalar(query: &[f32], dataset: &[f32]) -> Vec<(u32, f32)> {
    let mut scored: Vec<(u32, f32)> = dataset
        .chunks_exact(DIMENSIONS)
        .enumerate()
        .map(|(idx, candidate)| {
            (
                idx as u32,
                vector::distance::bench_l2_distance_scalar(query, candidate),
            )
        })
        .collect();

    scored.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
    scored
}

fn score_and_rank_avx(query: &[f32], dataset: &[f32]) -> Option<Vec<(u32, f32)>> {
    let mut scored: Vec<(u32, f32)> = dataset
        .chunks_exact(DIMENSIONS)
        .enumerate()
        .map(|(idx, candidate)| {
            Ok::<_, ()>((
                idx as u32,
                vector::distance::bench_l2_distance_avx(query, candidate).ok_or(())?,
            ))
        })
        .collect::<Result<_, _>>()
        .ok()?;

    scored.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
    Some(scored)
}

fn score_and_rank_rayon(query: &[f32], dataset: &[f32]) -> Vec<(u32, f32)> {
    let mut scored: Vec<(u32, f32)> = dataset
        .par_chunks_exact(DIMENSIONS)
        .enumerate()
        .map(|(idx, candidate)| {
            (
                idx as u32,
                vector::distance::bench_l2_distance_runtime(query, candidate),
            )
        })
        .collect();

    scored.par_sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
    scored
}

fn score_and_rank_rayon_scalar(query: &[f32], dataset: &[f32]) -> Vec<(u32, f32)> {
    let mut scored: Vec<(u32, f32)> = dataset
        .par_chunks_exact(DIMENSIONS)
        .enumerate()
        .map(|(idx, candidate)| {
            (
                idx as u32,
                vector::distance::bench_l2_distance_scalar(query, candidate),
            )
        })
        .collect();

    scored.par_sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal));
    scored
}

fn bench_l2_score_and_rank(c: &mut Criterion) {
    // given
    let query = make_random_vector(42);
    let dataset = make_random_dataset(7);

    let mut group = c.benchmark_group("distance/l2_score_and_rank/1024d_dataset_10k");
    group.throughput(Throughput::Elements(DATASET_SIZE as u64));

    group.bench_with_input(
        BenchmarkId::new("scalar", "avx_off"),
        &(&query, &dataset),
        |bench, (query, dataset)| {
            bench.iter(|| {
                black_box(score_and_rank_scalar(
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
                        score_and_rank_avx(
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
                black_box(score_and_rank_rayon(
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
                black_box(score_and_rank_rayon_scalar(
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
    targets = bench_l2_score_and_rank
}
criterion_main!(benches);
