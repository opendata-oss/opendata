//! Generate exact ground truth for DEEP-style datasets from `.fbin` inputs.
//!
//! Usage:
//!   cargo run -p opendata-vector --release --bin gen_deep_groundtruth -- \
//!     --base-fbin vector/bench/data/deep/base.10M.fbin \
//!     --query-fbin vector/bench/data/deep/query.public.10K.fbin \
//!     --output-ivecs vector/bench/data/deep/deep_groundtruth_10M.ivecs \
//!     --top-k 100

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use rayon::prelude::*;

const DEFAULT_TOP_K: usize = 100;
const DEFAULT_CHUNK_VECTORS: usize = 16_384;
const DEFAULT_MAX_QUERIES: usize = 1_000;
const QUERY_PROGRESS_STEP: usize = 10;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DistanceMetric {
    L2,
    DotProduct,
}

impl DistanceMetric {
    fn parse(s: &str) -> Self {
        match s {
            "l2" => Self::L2,
            "dot_product" => Self::DotProduct,
            _ => panic!("unsupported distance metric: {s}"),
        }
    }
}

#[derive(Clone, Debug)]
struct Args {
    base_fbin: PathBuf,
    query_fbin: PathBuf,
    output_ivecs: PathBuf,
    top_k: usize,
    chunk_vectors: usize,
    distance_metric: DistanceMetric,
    max_base: Option<usize>,
    max_queries: Option<usize>,
}

#[derive(Clone, Copy, Debug)]
struct Candidate {
    id: u32,
    score: f32,
}

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.score.to_bits() == other.score.to_bits()
    }
}

impl Eq for Candidate {}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| self.id.cmp(&other.id))
    }
}

struct FbinHeader {
    count: usize,
    dims: usize,
}

fn main() -> anyhow::Result<()> {
    let args = parse_args();

    println!("Loading queries from {}", args.query_fbin.display());
    let (queries, query_dims) = read_all_fbin(&args.query_fbin, args.max_queries)?;
    println!(
        "Loaded {} query vectors (dims={})",
        queries.len(),
        query_dims
    );

    let header = read_fbin_header(&args.base_fbin)?;
    let total_base = args.max_base.unwrap_or(header.count).min(header.count);
    if header.dims != query_dims {
        anyhow::bail!(
            "dimension mismatch: base dims={}, query dims={}",
            header.dims,
            query_dims
        );
    }
    if args.top_k == 0 {
        anyhow::bail!("--top-k must be > 0");
    }
    if total_base < args.top_k {
        anyhow::bail!(
            "--top-k ({}) exceeds base vector count ({})",
            args.top_k,
            total_base
        );
    }

    println!(
        "Computing exhaustive ground truth for {} base vectors x {} queries, top_k={}, metric={:?}, chunk_vectors={}",
        total_base,
        queries.len(),
        args.top_k,
        args.distance_metric,
        args.chunk_vectors
    );

    let ground_truth = compute_ground_truth(
        &args.base_fbin,
        &queries,
        header.dims,
        total_base,
        args.top_k,
        args.chunk_vectors,
        args.distance_metric,
    )?;

    write_ivecs(&args.output_ivecs, &ground_truth)?;
    println!(
        "Wrote {} query rows to {}",
        ground_truth.len(),
        args.output_ivecs.display()
    );

    Ok(())
}

fn parse_args() -> Args {
    let mut base_fbin = None;
    let mut query_fbin = None;
    let mut output_ivecs = None;
    let mut top_k = DEFAULT_TOP_K;
    let mut chunk_vectors = DEFAULT_CHUNK_VECTORS;
    let mut distance_metric = DistanceMetric::L2;
    let mut max_base = None;
    let mut max_queries = Some(DEFAULT_MAX_QUERIES);

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--base-fbin" => base_fbin = Some(PathBuf::from(args.next().expect("missing value"))),
            "--query-fbin" => query_fbin = Some(PathBuf::from(args.next().expect("missing value"))),
            "--output-ivecs" => {
                output_ivecs = Some(PathBuf::from(args.next().expect("missing value")))
            }
            "--top-k" => {
                top_k = args
                    .next()
                    .expect("missing value")
                    .parse()
                    .expect("invalid k")
            }
            "--chunk-vectors" => {
                chunk_vectors = args
                    .next()
                    .expect("missing value")
                    .parse()
                    .expect("invalid chunk size")
            }
            "--distance-metric" => {
                distance_metric = DistanceMetric::parse(&args.next().expect("missing value"))
            }
            "--max-base" => {
                max_base = Some(
                    args.next()
                        .expect("missing value")
                        .parse()
                        .expect("invalid max_base"),
                )
            }
            "--max-queries" => {
                max_queries = Some(
                    args.next()
                        .expect("missing value")
                        .parse()
                        .expect("invalid max_queries"),
                )
            }
            "--help" | "-h" => {
                print_usage_and_exit();
            }
            other => panic!("unknown argument: {other}"),
        }
    }

    Args {
        base_fbin: base_fbin.unwrap_or_else(|| panic!("missing --base-fbin")),
        query_fbin: query_fbin.unwrap_or_else(|| panic!("missing --query-fbin")),
        output_ivecs: output_ivecs.unwrap_or_else(|| panic!("missing --output-ivecs")),
        top_k,
        chunk_vectors,
        distance_metric,
        max_base,
        max_queries,
    }
}

fn print_usage_and_exit() -> ! {
    eprintln!(
        "Usage:
  cargo run -p opendata-vector --release --bin gen_deep_groundtruth -- \\
    --base-fbin <path> \\
    --query-fbin <path> \\
    --output-ivecs <path> \\
    [--top-k 100] \\
    [--chunk-vectors 16384] \\
    [--distance-metric l2|dot_product] \\
    [--max-base N] \\
    [--max-queries N]

Defaults:
  --max-queries 1000"
    );
    std::process::exit(0);
}

fn read_fbin_header(path: &Path) -> anyhow::Result<FbinHeader> {
    let mut reader = BufReader::new(File::open(path)?);
    let count = read_u32(&mut reader)? as usize;
    let dims = read_u32(&mut reader)? as usize;
    Ok(FbinHeader { count, dims })
}

fn read_all_fbin(path: &Path, limit: Option<usize>) -> anyhow::Result<(Vec<Vec<f32>>, usize)> {
    let mut reader = BufReader::new(File::open(path)?);
    let count = read_u32(&mut reader)? as usize;
    let dims = read_u32(&mut reader)? as usize;
    let rows = limit.unwrap_or(count).min(count);
    let mut bytes = vec![0u8; rows * dims * 4];
    reader.read_exact(&mut bytes)?;

    let mut values = Vec::with_capacity(rows);
    for row in 0..rows {
        let start = row * dims * 4;
        let end = start + dims * 4;
        let mut vector = vec![0f32; dims];
        let dst =
            unsafe { std::slice::from_raw_parts_mut(vector.as_mut_ptr() as *mut u8, dims * 4) };
        dst.copy_from_slice(&bytes[start..end]);
        values.push(vector);
    }

    Ok((values, dims))
}

fn compute_ground_truth(
    base_fbin: &Path,
    queries: &[Vec<f32>],
    dims: usize,
    total_base: usize,
    top_k: usize,
    chunk_vectors: usize,
    metric: DistanceMetric,
) -> anyhow::Result<Vec<Vec<i32>>> {
    let mut reader = BufReader::new(File::open(base_fbin)?);
    let header_count = read_u32(&mut reader)? as usize;
    let header_dims = read_u32(&mut reader)? as usize;
    if header_dims != dims {
        anyhow::bail!(
            "base dims changed between reads: expected {}, got {}",
            dims,
            header_dims
        );
    }
    if total_base > header_count {
        anyhow::bail!(
            "requested {} base vectors but file only has {}",
            total_base,
            header_count
        );
    }

    let mut heaps = vec![BinaryHeap::<Candidate>::with_capacity(top_k + 1); queries.len()];
    let bytes_per_vector = dims * 4;
    let mut processed = 0usize;
    let mut reported_queries = 0usize;

    while processed < total_base {
        let rows = (total_base - processed).min(chunk_vectors);
        let mut bytes = vec![0u8; rows * bytes_per_vector];
        reader.read_exact(&mut bytes)?;

        let chunk_vectors = bytes_to_vectors(&bytes, rows, dims);
        let start_id = processed as u32;

        heaps
            .par_iter_mut()
            .enumerate()
            .for_each(|(query_idx, heap)| {
                process_query_chunk(
                    heap,
                    &queries[query_idx],
                    &chunk_vectors,
                    start_id,
                    top_k,
                    metric,
                )
            });

        processed += rows;
        let completed_queries = processed * queries.len() / total_base;
        while reported_queries + QUERY_PROGRESS_STEP <= completed_queries {
            reported_queries += QUERY_PROGRESS_STEP;
            println!(
                "  processed ~{}/{} query vectors ({:.1}%)",
                reported_queries,
                queries.len(),
                processed as f64 * 100.0 / total_base as f64
            );
        }
    }

    if reported_queries < queries.len() {
        println!(
            "  processed {}/{} query vectors (100.0%)",
            queries.len(),
            queries.len()
        );
    }

    heaps
        .into_iter()
        .map(|heap| finalize_heap(heap, top_k))
        .collect::<anyhow::Result<Vec<_>>>()
}

fn bytes_to_vectors(bytes: &[u8], rows: usize, dims: usize) -> Vec<Vec<f32>> {
    let mut out = Vec::with_capacity(rows);
    for row in 0..rows {
        let start = row * dims * 4;
        let end = start + dims * 4;
        let mut vector = vec![0f32; dims];
        let dst =
            unsafe { std::slice::from_raw_parts_mut(vector.as_mut_ptr() as *mut u8, dims * 4) };
        dst.copy_from_slice(&bytes[start..end]);
        out.push(vector);
    }
    out
}

fn process_query_chunk(
    heap: &mut BinaryHeap<Candidate>,
    query: &[f32],
    base_chunk: &[Vec<f32>],
    start_id: u32,
    top_k: usize,
    metric: DistanceMetric,
) {
    for (offset, base) in base_chunk.iter().enumerate() {
        let score = match metric {
            DistanceMetric::L2 => l2_distance(query, base),
            DistanceMetric::DotProduct => -dot_product(query, base),
        };
        let candidate = Candidate {
            id: start_id + offset as u32,
            score,
        };

        if heap.len() < top_k {
            heap.push(candidate);
            continue;
        }

        if let Some(worst) = heap.peek()
            && candidate < *worst
        {
            heap.pop();
            heap.push(candidate);
        }
    }
}

fn finalize_heap(mut heap: BinaryHeap<Candidate>, top_k: usize) -> anyhow::Result<Vec<i32>> {
    if heap.len() != top_k {
        anyhow::bail!(
            "expected heap size {}, got {} while finalizing ground truth",
            top_k,
            heap.len()
        );
    }

    let mut values = heap.drain().collect::<Vec<_>>();
    values.sort_by(|a, b| a.score.total_cmp(&b.score).then_with(|| a.id.cmp(&b.id)));
    Ok(values.into_iter().map(|c| c.id as i32).collect())
}

fn l2_distance(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| (x - y) * (x - y)).sum()
}

fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}

fn read_u32(reader: &mut impl Read) -> io::Result<u32> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

fn write_ivecs(path: &Path, rows: &[Vec<i32>]) -> anyhow::Result<()> {
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);
    for row in rows {
        writer.write_all(&(row.len() as u32).to_le_bytes())?;
        let bytes = unsafe { std::slice::from_raw_parts(row.as_ptr() as *const u8, row.len() * 4) };
        writer.write_all(bytes)?;
    }
    writer.flush()?;
    Ok(())
}
