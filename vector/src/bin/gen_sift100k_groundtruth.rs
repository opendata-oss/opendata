//! Generates a random 100K subset of SIFT1M and its ground truth.
//!
//! Usage:
//!   cargo run -p opendata-vector --release --bin gen_sift100k_groundtruth
//!
//! Requires the SIFT1M dataset in tests/data/sift/{sift_base.fvecs, sift_query.fvecs}.
//!
//! Writes:
//!   tests/data/sift100k/base.fvecs        — 100K randomly sampled base vectors
//!   tests/data/sift100k/query.fvecs       — first 1000 query vectors (copied from SIFT1M)
//!   tests/data/sift100k/groundtruth.ivecs — top-100 neighbors per query (positional IDs)

use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;

const NUM_BASE: usize = 100_000;
const TOTAL_BASE: usize = 1_000_000;
const NUM_QUERIES: usize = 1_000;
const K: usize = 100; // standard ground truth depth
const SEED: u64 = 42;

fn read_all_fvecs(path: &Path) -> Vec<Vec<f32>> {
    let file = File::open(path).expect("failed to open fvecs file");
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

fn read_fvecs(path: &Path, limit: usize) -> Vec<Vec<f32>> {
    let file = File::open(path).expect("failed to open fvecs file");
    let mut reader = BufReader::new(file);
    let mut vectors = Vec::new();
    let mut dim_buf = [0u8; 4];

    while vectors.len() < limit && reader.read_exact(&mut dim_buf).is_ok() {
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

fn write_ivecs(path: &Path, data: &[Vec<i32>]) {
    let file = File::create(path).expect("failed to create ivecs file");
    let mut writer = BufWriter::new(file);

    for row in data {
        let dim = row.len() as u32;
        writer
            .write_all(&dim.to_le_bytes())
            .expect("failed to write dim");
        let byte_slice =
            unsafe { std::slice::from_raw_parts(row.as_ptr() as *const u8, row.len() * 4) };
        writer
            .write_all(byte_slice)
            .expect("failed to write values");
    }
}

fn write_fvecs(path: &Path, data: &[&Vec<f32>]) {
    let file = File::create(path).expect("failed to create fvecs file");
    let mut writer = BufWriter::new(file);

    for row in data {
        let dim = row.len() as u32;
        writer
            .write_all(&dim.to_le_bytes())
            .expect("failed to write dim");
        let byte_slice =
            unsafe { std::slice::from_raw_parts(row.as_ptr() as *const u8, row.len() * 4) };
        writer
            .write_all(byte_slice)
            .expect("failed to write values");
    }
}

fn l2_distance(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| (x - y) * (x - y)).sum()
}

fn main() {
    let sift_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/sift");
    let out_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/sift100k");
    std::fs::create_dir_all(&out_dir).expect("failed to create output directory");

    // Randomly sample 100K indices from 1M with a fixed seed
    let mut rng = StdRng::seed_from_u64(SEED);
    let mut all_indices: Vec<usize> = (0..TOTAL_BASE).collect();
    all_indices.shuffle(&mut rng);
    let selected_indices: Vec<usize> = all_indices.into_iter().take(NUM_BASE).collect();

    // Load all 1M base vectors and select the subset
    println!("Loading all base vectors...");
    let all_base = read_all_fvecs(&sift_dir.join("sift_base.fvecs"));
    println!(
        "Loaded {} base vectors (dim={})",
        all_base.len(),
        all_base[0].len()
    );

    let base: Vec<&Vec<f32>> = selected_indices.iter().map(|&i| &all_base[i]).collect();
    println!("Selected {} random base vectors", base.len());

    // Write the selected base vectors
    let base_path = out_dir.join("base.fvecs");
    println!("Writing {} base vectors to {:?}", base.len(), base_path);
    write_fvecs(&base_path, &base);

    // Copy query vectors
    println!("Loading queries (first {})...", NUM_QUERIES);
    let queries = read_fvecs(&sift_dir.join("sift_query.fvecs"), NUM_QUERIES);
    println!("Loaded {} queries", queries.len());

    let query_refs: Vec<&Vec<f32>> = queries.iter().collect();
    let query_path = out_dir.join("query.fvecs");
    println!("Writing {} queries to {:?}", queries.len(), query_path);
    write_fvecs(&query_path, &query_refs);

    // Compute ground truth using positional IDs (0, 1, 2, ...)
    println!("Computing ground truth (top-{} for each query)...", K);
    let mut ground_truth = Vec::with_capacity(queries.len());

    for (qi, query) in queries.iter().enumerate() {
        let mut dists: Vec<(usize, f32)> = base
            .iter()
            .enumerate()
            .map(|(i, b)| (i, l2_distance(query, b)))
            .collect();

        // Partial sort: only need top K
        dists.select_nth_unstable_by(K - 1, |a, b| a.1.partial_cmp(&b.1).unwrap());
        dists.truncate(K);
        dists.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

        let ids: Vec<i32> = dists.iter().map(|(i, _)| *i as i32).collect();
        ground_truth.push(ids);

        if (qi + 1) % 100 == 0 {
            println!("  {}/{} queries done", qi + 1, queries.len());
        }
    }

    let gt_path = out_dir.join("groundtruth.ivecs");
    println!("Writing ground truth to {:?}", gt_path);
    write_ivecs(&gt_path, &ground_truth);
    println!(
        "Done. To update the tarball:\n  cd {} && tar czf sift100k.tgz base.fvecs query.fvecs groundtruth.ivecs",
        out_dir.display()
    );
}
