//! Benchmarks comparing variable-length integer encoding vs fixed-length encoding.
//!
//! These benchmarks measure the performance of the lexicographically-ordered varint
//! encoding against fixed-length big-endian encoding for u32 and u64 values.

use bytes::{BufMut, BytesMut};
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};

use common::serde::varint::{var_u32, var_u64};

/// Values spanning var_u32 length codes 0-4
const U32_VALUES: &[u32] = &[
    0,           // length code 0 (1 byte)
    31,          // length code 0 boundary
    32,          // length code 1 (2 bytes)
    8_191,       // length code 1 boundary
    8_192,       // length code 2 (3 bytes)
    2_097_151,   // length code 2 boundary
    2_097_152,   // length code 3 (4 bytes)
    536_870_911, // length code 3 boundary
    536_870_912, // length code 4 (5 bytes)
    u32::MAX,    // length code 4 boundary
];

/// Values spanning var_u64 length codes 0-8
const U64_VALUES: &[u64] = &[
    0,                         // length code 0 (1 byte)
    15,                        // length code 0 boundary
    16,                        // length code 1 (2 bytes)
    4_095,                     // length code 1 boundary
    4_096,                     // length code 2 (3 bytes)
    1_048_575,                 // length code 2 boundary
    1_048_576,                 // length code 3 (4 bytes)
    268_435_455,               // length code 3 boundary
    268_435_456,               // length code 4 (5 bytes)
    68_719_476_735,            // length code 4 boundary
    68_719_476_736,            // length code 5 (6 bytes)
    17_592_186_044_415,        // length code 5 boundary
    17_592_186_044_416,        // length code 6 (7 bytes)
    4_503_599_627_370_495,     // length code 6 boundary
    4_503_599_627_370_496,     // length code 7 (8 bytes)
    1_152_921_504_606_846_975, // length code 7 boundary
    1_152_921_504_606_846_976, // length code 8 (9 bytes)
    u64::MAX,                  // length code 8 boundary
];

fn fixed_u32_serialize(value: u32, buf: &mut BytesMut) {
    buf.put_u32(value);
}

fn fixed_u32_deserialize(buf: &mut &[u8]) -> u32 {
    let value = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    *buf = &buf[4..];
    value
}

fn fixed_u64_serialize(value: u64, buf: &mut BytesMut) {
    buf.put_u64(value);
}

fn fixed_u64_deserialize(buf: &mut &[u8]) -> u64 {
    let value = u64::from_be_bytes([
        buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
    ]);
    *buf = &buf[8..];
    value
}

/// Returns the encoded byte length for a var_u32 value.
fn var_u32_len(value: u32) -> usize {
    match value {
        0..32 => 1,
        32..8_192 => 2,
        8_192..2_097_152 => 3,
        2_097_152..536_870_912 => 4,
        _ => 5,
    }
}

/// Returns the encoded byte length for a var_u64 value.
fn var_u64_len(value: u64) -> usize {
    match value {
        0..16 => 1,
        16..4_096 => 2,
        4_096..1_048_576 => 3,
        1_048_576..268_435_456 => 4,
        268_435_456..68_719_476_736 => 5,
        68_719_476_736..17_592_186_044_416 => 6,
        17_592_186_044_416..4_503_599_627_370_496 => 7,
        4_503_599_627_370_496..1_152_921_504_606_846_976 => 8,
        _ => 9,
    }
}

fn bench_var_u32_serialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("var_u32/serialize");

    for &value in U32_VALUES {
        let encoded_len = var_u32_len(value);
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("varint", format!("{}b/{:#x}", encoded_len, value)),
            &value,
            |b, &value| {
                let mut buf = BytesMut::with_capacity(8);
                b.iter(|| {
                    buf.clear();
                    var_u32::serialize(black_box(value), &mut buf);
                    black_box(&buf);
                });
            },
        );
    }

    group.finish();
}

fn bench_var_u32_deserialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("var_u32/deserialize");

    for &value in U32_VALUES {
        let encoded_len = var_u32_len(value);

        // Pre-encode the value
        let mut encoded = BytesMut::with_capacity(8);
        var_u32::serialize(value, &mut encoded);
        let encoded_bytes = encoded.freeze();

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("varint", format!("{}b/{:#x}", encoded_len, value)),
            &encoded_bytes,
            |b, encoded| {
                b.iter(|| {
                    let mut slice = encoded.as_ref();
                    let result = var_u32::deserialize(black_box(&mut slice)).unwrap();
                    black_box(result)
                });
            },
        );
    }

    group.finish();
}

fn bench_fixed_u32_serialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("fixed_u32/serialize");

    for &value in U32_VALUES {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("fixed", format!("{:#x}", value)),
            &value,
            |b, &value| {
                let mut buf = BytesMut::with_capacity(8);
                b.iter(|| {
                    buf.clear();
                    fixed_u32_serialize(black_box(value), &mut buf);
                    black_box(&buf);
                });
            },
        );
    }

    group.finish();
}

fn bench_fixed_u32_deserialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("fixed_u32/deserialize");

    for &value in U32_VALUES {
        // Pre-encode the value
        let mut encoded = BytesMut::with_capacity(8);
        fixed_u32_serialize(value, &mut encoded);
        let encoded_bytes = encoded.freeze();

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("fixed", format!("{:#x}", value)),
            &encoded_bytes,
            |b, encoded| {
                b.iter(|| {
                    let mut slice = encoded.as_ref();
                    let result = fixed_u32_deserialize(black_box(&mut slice));
                    black_box(result)
                });
            },
        );
    }

    group.finish();
}

fn bench_var_u64_serialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("var_u64/serialize");

    for &value in U64_VALUES {
        let encoded_len = var_u64_len(value);
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("varint", format!("{}b/{:#x}", encoded_len, value)),
            &value,
            |b, &value| {
                let mut buf = BytesMut::with_capacity(16);
                b.iter(|| {
                    buf.clear();
                    var_u64::serialize(black_box(value), &mut buf);
                    black_box(&buf);
                });
            },
        );
    }

    group.finish();
}

fn bench_var_u64_deserialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("var_u64/deserialize");

    for &value in U64_VALUES {
        let encoded_len = var_u64_len(value);

        // Pre-encode the value
        let mut encoded = BytesMut::with_capacity(16);
        var_u64::serialize(value, &mut encoded);
        let encoded_bytes = encoded.freeze();

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("varint", format!("{}b/{:#x}", encoded_len, value)),
            &encoded_bytes,
            |b, encoded| {
                b.iter(|| {
                    let mut slice = encoded.as_ref();
                    let result = var_u64::deserialize(black_box(&mut slice)).unwrap();
                    black_box(result)
                });
            },
        );
    }

    group.finish();
}

fn bench_fixed_u64_serialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("fixed_u64/serialize");

    for &value in U64_VALUES {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("fixed", format!("{:#x}", value)),
            &value,
            |b, &value| {
                let mut buf = BytesMut::with_capacity(16);
                b.iter(|| {
                    buf.clear();
                    fixed_u64_serialize(black_box(value), &mut buf);
                    black_box(&buf);
                });
            },
        );
    }

    group.finish();
}

fn bench_fixed_u64_deserialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("fixed_u64/deserialize");

    for &value in U64_VALUES {
        // Pre-encode the value
        let mut encoded = BytesMut::with_capacity(16);
        fixed_u64_serialize(value, &mut encoded);
        let encoded_bytes = encoded.freeze();

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("fixed", format!("{:#x}", value)),
            &encoded_bytes,
            |b, encoded| {
                b.iter(|| {
                    let mut slice = encoded.as_ref();
                    let result = fixed_u64_deserialize(black_box(&mut slice));
                    black_box(result)
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    var_u32_benches,
    bench_var_u32_serialize,
    bench_var_u32_deserialize,
    bench_fixed_u32_serialize,
    bench_fixed_u32_deserialize,
);

criterion_group!(
    var_u64_benches,
    bench_var_u64_serialize,
    bench_var_u64_deserialize,
    bench_fixed_u64_serialize,
    bench_fixed_u64_deserialize,
);

criterion_main!(var_u32_benches, var_u64_benches);
