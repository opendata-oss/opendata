//! COLD phase: measure query latency against repeatedly re-opened
//! `VectorDbReader`s with empty caches.
//!
//! Runs [`COLD_NUM_QUERIES`] queries in groups of [`COLD_QUERIES_PER_READER`].
//! Each group opens a fresh `VectorDbReader` with a freshly allocated block
//! cache so the first query in every group pays the full open + cold-cache
//! cost, and later queries in the group see whatever the first few queries
//! warmed up. If the dataset has fewer loaded queries than
//! `COLD_NUM_QUERIES`, queries are cycled.

use std::time::Instant;

use bencher::Bench;
use vector::{Query, ReaderConfig, SearchOptions, VectorDbRead, VectorDbReader};

use crate::recall::{Dataset, build_reader_runtime, percentile};

/// Total number of queries to run during the cold phase.
const COLD_NUM_QUERIES: usize = 1000;
/// Number of queries per fresh reader. After this many queries, the reader
/// is dropped and re-opened with a fresh cache.
const COLD_QUERIES_PER_READER: usize = 10;

/// Metrics produced by the cold phase.
pub struct ColdSummary {
    pub p50: f64,
    pub p90: f64,
    pub p99: f64,
}

pub async fn run(
    dataset: &Dataset,
    reader_config: &ReaderConfig,
    queries: &[Vec<f32>],
    k: usize,
    bench: &Bench,
) -> anyhow::Result<ColdSummary> {
    if queries.is_empty() {
        anyhow::bail!("cold phase requires at least one query vector");
    }

    println!("start cold reader phase");
    println!(
        "  running {} queries in groups of {} (reader re-opened between groups to clear cache)",
        COLD_NUM_QUERIES, COLD_QUERIES_PER_READER
    );

    let cold_query_latency = bench.histogram("cold_query_latency_us");
    let mut latencies_us = Vec::with_capacity(COLD_NUM_QUERIES);

    // Cycle through loaded queries if the dataset has fewer than
    // COLD_NUM_QUERIES (e.g. sift100k loads 100, we want 1000).
    let mut query_iter = queries.iter().cycle();
    let mut remaining = COLD_NUM_QUERIES;

    while remaining > 0 {
        // Fresh runtime → fresh block cache for this group.
        let runtime = build_reader_runtime(reader_config, dataset).await?;
        let cache = runtime.block_cache();
        let reader = VectorDbReader::open_with_runtime(reader_config.clone(), runtime).await?;

        let group_size = remaining.min(COLD_QUERIES_PER_READER);
        for _ in 0..group_size {
            let query = query_iter.next().expect("queries.iter().cycle() is infinite");
            let t = Instant::now();
            let q = Query::new(query.clone()).with_limit(k);
            let _ = reader
                .search_with_options(
                    &q,
                    SearchOptions {
                        nprobe: Some(dataset.nprobe),
                    },
                )
                .await?;
            let elapsed_us = t.elapsed().as_secs_f64() * 1_000_000.0;
            cold_query_latency.record(elapsed_us);
            latencies_us.push(elapsed_us);
        }
        remaining -= group_size;

        drop(reader);
        if let Some(cache) = cache {
            cache.close().await?;
        }
    }

    latencies_us.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p50 = percentile(&latencies_us, 50.0);
    let p90 = percentile(&latencies_us, 90.0);
    let p99 = percentile(&latencies_us, 99.0);
    println!(
        "  cold reader p50 = {:.2} ms, p90 = {:.2} ms, p99 = {:.2} ms ({} queries)",
        p50 / 1000.0,
        p90 / 1000.0,
        p99 / 1000.0,
        latencies_us.len(),
    );
    println!("end cold reader phase");

    Ok(ColdSummary { p50, p90, p99 })
}
