//! Generates a random 100K subset of SIFT1M and its ground truth.
//!
//! Usage:
//!   cargo run -p opendata-vector --release --bin gen_sift100k_groundtruth
//!
//! Reads from tests/data/sift/{sift_base.fvecs, sift_query.fvecs}
//! Writes:
//!   tests/data/sift/sift100k_indices.ivecs  — single row of 100K randomly selected base indices
//!   tests/data/sift/sift100k_groundtruth.ivecs — top-100 neighbors (by index into sift100k_indices)

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

fn l2_distance(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| (x - y) * (x - y)).sum()
}

fn main() {
    let data_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/sift");

    // Randomly sample 100K indices from 1M with a fixed seed
    let mut rng = StdRng::seed_from_u64(SEED);
    let mut all_indices: Vec<usize> = (0..TOTAL_BASE).collect();
    all_indices.shuffle(&mut rng);
    let selected_indices: Vec<usize> = all_indices.into_iter().take(NUM_BASE).collect();

    // Save selected indices (as a single ivecs row)
    let indices_path = data_dir.join("sift100k_indices.ivecs");
    println!("Saving {} selected indices to {:?}", NUM_BASE, indices_path);
    let indices_row = vec![
        selected_indices
            .iter()
            .map(|&i| i as i32)
            .collect::<Vec<i32>>(),
    ];
    write_ivecs(&indices_path, &indices_row);

    // Load all 1M base vectors and select the subset
    println!("Loading all base vectors...");
    let all_base = read_all_fvecs(&data_dir.join("sift_base.fvecs"));
    println!(
        "Loaded {} base vectors (dim={})",
        all_base.len(),
        all_base[0].len()
    );

    let base: Vec<&Vec<f32>> = selected_indices.iter().map(|&i| &all_base[i]).collect();
    println!("Selected {} random base vectors", base.len());

    println!("Loading queries (first {})...", NUM_QUERIES);
    let queries = read_fvecs(&data_dir.join("sift_query.fvecs"), NUM_QUERIES);
    println!("Loaded {} queries", queries.len());

    // Compute ground truth — IDs are the original sift_base indices (selected_indices[i])
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

        // Store the original sift_base index as the ID (matches external_id in the test)
        let ids: Vec<i32> = dists
            .iter()
            .map(|(i, _)| selected_indices[*i] as i32)
            .collect();
        ground_truth.push(ids);

        if (qi + 1) % 100 == 0 {
            println!("  {}/{} queries done", qi + 1, queries.len());
        }
    }

    let out_path = data_dir.join("sift100k_groundtruth.ivecs");
    println!("Writing ground truth to {:?}", out_path);
    write_ivecs(&out_path, &ground_truth);
    println!("Done.");
}
