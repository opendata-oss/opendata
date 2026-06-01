//! Exact ground-truth generation for the recall benchmark.
//!
//! Streams a dataset's base vectors, brute-forces the top-`k` nearest
//! neighbours for each query, and writes a ground-truth file in the format
//! that matches the dataset (`ivecs` for fvecs/bvecs, Parquet for parquet).
//! When a query carries a filter (from the dataset's `filter_spec`), only
//! base rows whose metadata satisfies the filter are considered — producing
//! *filtered* ground truth.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use arrow::array::{Int32Array, ListArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use rayon::prelude::*;
use vector::{Attribute, AttributeValue, DistanceMetric};

use crate::recall::{Dataset, VecFormat};

/// Depth of generated ground truth (neighbours per query).
const GROUNDTRUTH_DEPTH: usize = 10;
/// Base-vector chunk size for streaming.
const CHUNK_VECTORS: usize = 16_384;

/// Generate and write the ground-truth file for a single dataset.
pub(crate) async fn generate_for_dataset(dataset: &Dataset, data_dir: &Path) -> anyhow::Result<()> {
    let gt_path = data_dir.join(dataset.ground_truth_file);
    let queries = dataset.load_queries(data_dir)?;
    if queries.is_empty() {
        anyhow::bail!("dataset {} has no queries", dataset.name);
    }
    let k = GROUNDTRUTH_DEPTH;
    let metric = dataset.distance_metric;
    let has_filter = queries.iter().any(|q| q.filter.is_some());

    println!(
        "  Generating top-{} ground truth for {} ({} queries{}) -> {}",
        k,
        dataset.name,
        queries.len(),
        if has_filter { ", filtered" } else { "" },
        gt_path.display(),
    );

    let mut heaps = vec![BinaryHeap::<Candidate>::with_capacity(k + 1); queries.len()];

    let (mut stream, reader_thread) = dataset.spawn_base_vector_stream(data_dir, CHUNK_VECTORS)?;
    let mut offset: u32 = 0;
    while let Some(message) = stream.recv().await {
        let Some(batch) = message? else { break };

        // Precompute attribute maps for the chunk once (shared across all
        // queries) when any query filters.
        let attr_maps: Vec<HashMap<String, AttributeValue>> = if has_filter {
            batch.iter().map(|r| attrs_to_map(&r.attributes)).collect()
        } else {
            Vec::new()
        };

        heaps.par_iter_mut().enumerate().for_each(|(qi, heap)| {
            let query = &queries[qi];
            for (j, row) in batch.iter().enumerate() {
                if let Some(filter) = &query.filter
                    && !filter.matches(&attr_maps[j])
                {
                    continue;
                }
                let score = score(metric, &query.embedding, &row.embedding);
                push_candidate(
                    heap,
                    Candidate {
                        id: offset + j as u32,
                        score,
                    },
                    k,
                );
            }
        });

        offset += batch.len() as u32;
    }
    reader_thread
        .join()
        .map_err(|_| anyhow::anyhow!("base vector streaming thread panicked"))?;

    let ground_truth: Vec<Vec<i32>> = heaps.into_iter().map(finalize_heap).collect();

    let short = ground_truth.iter().filter(|r| r.len() < k).count();
    if short > 0 {
        println!(
            "  note: {} queries have fewer than {} matching base vectors \
             (filter selectivity); their ground-truth rows are shorter",
            short, k
        );
    }

    match dataset.format {
        VecFormat::Fvecs | VecFormat::Bvecs => write_ivecs(&gt_path, &ground_truth)?,
        VecFormat::Parquet => {
            let column = dataset
                .ground_truth_column
                .as_deref()
                .context("parquet dataset missing ground_truth_column (required to write GT)")?;
            write_parquet_ground_truth(&gt_path, column, &ground_truth)?;
        }
    }
    println!("  Wrote {} ground-truth rows", ground_truth.len());
    Ok(())
}

#[derive(Clone, Copy)]
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
        // Lower score = better. Max-heap keeps the k smallest scores.
        self.score
            .total_cmp(&other.score)
            .then_with(|| self.id.cmp(&other.id))
    }
}

fn push_candidate(heap: &mut BinaryHeap<Candidate>, candidate: Candidate, k: usize) {
    if heap.len() < k {
        heap.push(candidate);
    } else if let Some(worst) = heap.peek()
        && candidate < *worst
    {
        heap.pop();
        heap.push(candidate);
    }
}

fn finalize_heap(heap: BinaryHeap<Candidate>) -> Vec<i32> {
    let mut values: Vec<Candidate> = heap.into_vec();
    values.sort_by(|a, b| a.score.total_cmp(&b.score).then_with(|| a.id.cmp(&b.id)));
    values.into_iter().map(|c| c.id as i32).collect()
}

/// Lower = closer. Matches the DB's ranking for each metric.
fn score(metric: DistanceMetric, query: &[f32], base: &[f32]) -> f32 {
    match metric {
        DistanceMetric::L2 => l2(query, base),
        DistanceMetric::DotProduct => -dot(query, base),
        DistanceMetric::Cosine => -cosine_similarity(query, base),
    }
}

fn l2(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum()
}

fn dot(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| x * y).sum()
}

fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    let na = dot(a, a).sqrt();
    let nb = dot(b, b).sqrt();
    if na == 0.0 || nb == 0.0 {
        0.0
    } else {
        dot(a, b) / (na * nb)
    }
}

fn attrs_to_map(attrs: &[Attribute]) -> HashMap<String, AttributeValue> {
    attrs
        .iter()
        .map(|a| (a.name.clone(), a.value.clone()))
        .collect()
}

fn write_ivecs(path: &Path, rows: &[Vec<i32>]) -> anyhow::Result<()> {
    let mut writer = BufWriter::new(File::create(path)?);
    for row in rows {
        writer.write_all(&(row.len() as u32).to_le_bytes())?;
        let bytes = unsafe { std::slice::from_raw_parts(row.as_ptr() as *const u8, row.len() * 4) };
        writer.write_all(bytes)?;
    }
    writer.flush()?;
    Ok(())
}

fn write_parquet_ground_truth(path: &Path, column: &str, rows: &[Vec<i32>]) -> anyhow::Result<()> {
    let mut offsets = Vec::with_capacity(rows.len() + 1);
    offsets.push(0i32);
    let mut flat = Vec::new();
    for row in rows {
        flat.extend_from_slice(row);
        offsets.push(flat.len() as i32);
    }
    let item_field = Arc::new(Field::new("item", DataType::Int32, true));
    let list = ListArray::new(
        item_field.clone(),
        OffsetBuffer::new(offsets.into()),
        Arc::new(Int32Array::from(flat)),
        None,
    );
    let schema = Arc::new(Schema::new(vec![Field::new(
        column,
        DataType::List(item_field),
        false,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(list)])?;
    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}
