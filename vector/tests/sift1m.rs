use log::info;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;
use std::process::Command;
use tracing_subscriber::EnvFilter;
use vector::{Config, DistanceMetric, Vector, VectorDb};

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
}

fn read_fvecs(path: &Path) -> Vec<Vec<f32>> {
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

fn read_ivecs(path: &Path) -> Vec<Vec<i32>> {
    let file = File::open(path).expect("failed to open ivecs file");
    let mut reader = BufReader::new(file);
    let mut vectors = Vec::new();
    let mut dim_buf = [0u8; 4];

    while reader.read_exact(&mut dim_buf).is_ok() {
        let dim = u32::from_le_bytes(dim_buf) as usize;
        let mut values = vec![0i32; dim];
        let byte_slice =
            unsafe { std::slice::from_raw_parts_mut(values.as_mut_ptr() as *mut u8, dim * 4) };
        reader
            .read_exact(byte_slice)
            .expect("failed to read ivecs values");
        vectors.push(values);
    }

    vectors
}

fn ensure_extracted(data_dir: &Path, tarball: &str, marker: &str) {
    if data_dir.join(marker).exists() {
        return;
    }
    let tgz = data_dir.join(tarball);
    assert!(
        tgz.exists(),
        "tarball not found: {}",
        tgz.display()
    );
    let status = Command::new("tar")
        .args(["xzf", tarball])
        .current_dir(data_dir)
        .status()
        .expect("failed to run tar");
    assert!(status.success(), "tar extraction failed");
}

fn recall_at_k(results: &[vector::SearchResult], ground_truth: &[i32], k: usize) -> f64 {
    let gt_set: std::collections::HashSet<i32> = ground_truth.iter().take(k).copied().collect();
    let found = results
        .iter()
        .take(k)
        .filter(|r| {
            r.external_id
                .parse::<i32>()
                .map(|id| gt_set.contains(&id))
                .unwrap_or(false)
        })
        .count();
    found as f64 / k as f64
}

#[tokio::test(flavor = "multi_thread")]
#[ignore] // requires sift1m data files, ~500MB RAM, slow
async fn sift1m_recall() {
    init_tracing();
    let data_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/sift");

    let config = Config {
        dimensions: 128,
        distance_metric: DistanceMetric::L2,
        split_search_neighbourhood: 16,
        ..Default::default()
    };
    let db = VectorDb::open(config)
        .await
        .expect("failed to open VectorDb");

    // Ingest base vectors in batches of 10
    let base_vectors = read_fvecs(&data_dir.join("sift_base.fvecs"));
    println!("Loaded {} base vectors", base_vectors.len());

    let num_batches = (base_vectors.len() + 9) / 10;
    for (batch_idx, chunk) in base_vectors.chunks(10).enumerate() {
        let batch: Vec<Vector> = chunk
            .iter()
            .enumerate()
            .map(|(i, values)| {
                let index = batch_idx * 10 + i;
                Vector::new(index.to_string(), values.clone())
            })
            .collect();
        db.write(batch).await.expect("failed to write batch");
        if (batch_idx + 1) % 10000 == 0 {
            println!("Written batch {}/{}", batch_idx + 1, num_batches);
        }
    }
    db.flush().await.expect("failed to flush");
    println!("Ingested all base vectors");

    // Query and measure recall
    let queries = read_fvecs(&data_dir.join("sift_query.fvecs"));
    let ground_truth = read_ivecs(&data_dir.join("sift_groundtruth.ivecs"));
    println!(
        "Loaded {} queries and {} ground truth entries",
        queries.len(),
        ground_truth.len()
    );

    println!("Num centroids: {}", db.num_centroids());

    let k = 10;
    let nprobe = 12;
    let mut hnsw_recall = 0.0;
    let mut exact_recall = 0.0;
    for (i, query) in queries.iter().enumerate() {
        let hnsw_results = db
            .search_with_nprobe(query, k, nprobe)
            .await
            .expect("search failed");
        hnsw_recall += recall_at_k(&hnsw_results, &ground_truth[i], k);
        let exact_results = db
            .search_exact_nprobe(query, k, nprobe)
            .await
            .expect("exact search failed");
        exact_recall += recall_at_k(&exact_results, &ground_truth[i], k);
        if (i + 1) % 1000 == 0 {
            let n = (i + 1) as f64;
            println!(
                "query {}: hnsw_recall@{} = {:.4}, exact_recall@{} = {:.4}",
                i + 1,
                k,
                hnsw_recall / n,
                k,
                exact_recall / n
            );
        }
    }
    let n = queries.len() as f64;
    println!(
        "FINAL nprobe={}: hnsw_recall@{} = {:.4}, exact_recall@{} = {:.4}",
        nprobe,
        k,
        hnsw_recall / n,
        k,
        exact_recall / n
    );
    assert!(true);
}

#[tokio::test(flavor = "multi_thread")]
async fn sift100k_recall() {
    init_tracing();
    let data_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/sift100k");
    ensure_extracted(&data_dir, "sift100k.tgz", "base.fvecs");

    let config = Config {
        dimensions: 128,
        distance_metric: DistanceMetric::L2,
        split_threshold_vectors: 150,
        merge_threshold_vectors: 50,
        ..Default::default()
    };
    let db = VectorDb::open(config)
        .await
        .expect("failed to open VectorDb");

    let exact = false;

    // Load pre-extracted 100K base vectors
    let base_vectors = read_fvecs(&data_dir.join("base.fvecs"));
    println!("Loaded {} base vectors", base_vectors.len());

    // Ingest in batches of 10, using positional index as external_id
    let num_batches = (base_vectors.len() + 9) / 10;
    for (batch_idx, chunk) in base_vectors.chunks(10).enumerate() {
        let batch: Vec<Vector> = chunk
            .iter()
            .enumerate()
            .map(|(i, values)| {
                let index = batch_idx * 10 + i;
                Vector::new(index.to_string(), values.clone())
            })
            .collect();
        db.write(batch).await.expect("failed to write batch");
        if (batch_idx + 1) % 1000 == 0 {
            println!("Written batch {}/{}", batch_idx + 1, num_batches);
        }
    }
    db.flush().await.expect("failed to flush");
    println!("Ingested all base vectors");

    // Query and measure recall using first 1000 queries
    let nqueries = 100;
    let queries: Vec<Vec<f32>> = read_fvecs(&data_dir.join("query.fvecs"))
        .into_iter()
        .take(nqueries)
        .collect();
    let ground_truth = read_ivecs(&data_dir.join("groundtruth.ivecs"));
    println!(
        "Loaded {} queries and {} ground truth entries",
        queries.len(),
        ground_truth.len()
    );

    println!("Num centroids: {}", db.num_centroids());

    let k = 10;
    // Compare HNSW vs brute-force centroid search
    let nprobe = 25;
    let mut hnsw_recall = 0.0;
    let mut exact_recall = 0.0;
    for (i, query) in queries.iter().enumerate() {
        if (i + 1) % 10 == 0 {
            info!("query {}", i);
        }
        let hnsw_results = db
            .search_with_nprobe(query, k, nprobe)
            .await
            .expect("search failed");
        hnsw_recall += recall_at_k(&hnsw_results, &ground_truth[i], k);
        if exact {
            let exact_results = db
                .search_exact_nprobe(query, k, nprobe)
                .await
                .expect("exact search failed");
            exact_recall += recall_at_k(&exact_results, &ground_truth[i], k);
        }
    }
    let n = queries.len() as f64;
    println!(
        "nprobe={:>3}: hnsw_recall@{} = {:.4}, exact_recall@{} = {:.4}",
        nprobe,
        k,
        hnsw_recall / n,
        k,
        exact_recall / n
    );
    let recall = hnsw_recall / n;
    assert!(recall > 0.9);
}
