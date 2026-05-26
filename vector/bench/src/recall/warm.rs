//! WARM phase: open a fresh `VectorDb`, run a warmup pass over all queries,
//! then run the rate-limited concurrent query workload that produces the
//! recall@k and latency metrics for the benchmark.

use std::collections::HashSet;
use std::time::Instant;

use bencher::Bench;
use vector::{Config, Filter, Query, SearchOptions, SearchResult, VectorDb, VectorDbRead};

use crate::recall::{BenchQuery, Dataset, open_db, percentile, warm_default_memory_bytes};

/// Metrics produced by the warm phase.
pub struct WarmSummary {
    pub recall_at_k: f64,
    pub qps: f64,
    pub num_centroids: usize,
    pub p50: f64,
    pub p90: f64,
    pub p99: f64,
}

pub async fn run(
    dataset: &Dataset,
    config: &Config,
    queries: &[BenchQuery],
    ground_truth: &[Vec<i32>],
    k: usize,
    bench: &Bench,
) -> anyhow::Result<WarmSummary> {
    let db = open_db(config, dataset, warm_default_memory_bytes()).await?;
    let summary = warm(dataset, &db, queries, ground_truth, k, bench).await?;
    db.close().await?;
    Ok(summary)
}

async fn warm(
    dataset: &Dataset,
    db: &VectorDb,
    queries: &[BenchQuery],
    ground_truth: &[Vec<i32>],
    k: usize,
    bench: &Bench,
) -> anyhow::Result<WarmSummary> {
    println!("  Num centroids: {}", db.num_centroids());

    // -- Warmup -----------------------------------------------------------
    println!("start warmup");
    let warm_query_latency = bench.histogram("warm_query_latency_us");
    let mut warm_latencies_us = Vec::with_capacity(queries.len());
    for query in queries.iter() {
        let t = Instant::now();
        let mut q = Query::new(query.embedding.clone()).with_limit(k);
        if let Some(filter) = &query.filter {
            q = q.with_filter(filter.clone());
        }
        let _ = db
            .search_with_options(
                &q,
                SearchOptions {
                    nprobe: Some(dataset.nprobe),
                },
            )
            .await?;
        let elapsed_us = t.elapsed().as_secs_f64() * 1_000_000.0;
        warm_query_latency.record(elapsed_us);
        warm_latencies_us.push(elapsed_us);
    }
    warm_latencies_us.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let warm_p90 = percentile(&warm_latencies_us, 90.0);
    println!("warm p90 = {:.2}", warm_p90 / 1000.0);
    println!("end warmup");

    // -- Measured concurrent query workload -------------------------------
    let query_latency = bench.histogram("query_latency_us");

    let concurrency = dataset.query_concurrency.max(1);
    let qps_limit = dataset.query_qps_limit.max(1);
    let tick_period = std::time::Duration::from_secs_f64(1.0 / qps_limit as f64);
    println!(
        "  warm query phase: concurrency = {}, qps_limit = {}",
        concurrency, qps_limit,
    );

    let query_start = Instant::now();
    let mut total_recall = 0.0;
    let mut latencies_us = Vec::with_capacity(queries.len());

    // Producer/consumer rate limiter:
    //   - One producer pushes a (index, vector) into the channel every
    //     `tick_period` (1/qps_limit), then closes the channel when the
    //     input is exhausted.
    //   - `concurrency` consumers pull from the channel. Each consumer
    //     loops calling `rx.recv().await` and issues the query; the
    //     `async_channel` MPMC receiver lets them compete for work
    //     without a shared lock.
    //
    // Channel is bounded to `concurrency` so a slow DB back-pressures
    // the producer rather than letting the pending-query queue grow.
    let (tx, rx) = async_channel::bounded::<(usize, Vec<f32>, Option<Filter>)>(concurrency);

    let producer = async {
        let mut ticker = tokio::time::interval(tick_period);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        for (i, query) in queries.iter().enumerate() {
            ticker.tick().await;
            if tx
                .send((i, query.embedding.clone(), query.filter.clone()))
                .await
                .is_err()
            {
                break;
            }
        }
        drop(tx);
    };

    let nprobe = dataset.nprobe;
    let db_ref = db;
    let consumers: Vec<_> = (0..concurrency)
        .map(|_| {
            let rx = rx.clone();
            async move {
                let mut results = Vec::new();
                while let Ok((i, query, filter)) = rx.recv().await {
                    let mut q = Query::new(query).with_limit(k);
                    if let Some(filter) = filter {
                        q = q.with_filter(filter);
                    }
                    let t = Instant::now();
                    let search_results = db_ref
                        .search_with_options(
                            &q,
                            SearchOptions {
                                nprobe: Some(nprobe),
                            },
                        )
                        .await?;
                    let elapsed_us = t.elapsed().as_secs_f64() * 1_000_000.0;
                    results.push((i, elapsed_us, search_results));
                }
                anyhow::Ok::<Vec<(usize, f64, Vec<SearchResult>)>>(results)
            }
        })
        .collect();
    drop(rx);

    let (_, consumer_results) = tokio::join!(producer, futures::future::try_join_all(consumers));
    let consumer_results = consumer_results?;

    for batch in consumer_results {
        for (i, elapsed_us, results) in batch {
            query_latency.record(elapsed_us);
            latencies_us.push(elapsed_us);
            total_recall += recall_at_k(&results, &ground_truth[i], k);
        }
    }
    let query_secs = query_start.elapsed().as_secs_f64();
    let avg_recall = total_recall / queries.len() as f64;
    let qps = queries.len() as f64 / query_secs;

    latencies_us.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p50 = percentile(&latencies_us, 50.0);
    let p90 = percentile(&latencies_us, 90.0);
    let p99 = percentile(&latencies_us, 99.0);

    println!(
        "  recall@{} = {:.4}, QPS = {:.1}, avg = {:.2} ms, p50 = {:.2} ms, p90 = {:.2} ms, p99 = {:.2} ms",
        k,
        avg_recall,
        qps,
        (query_secs / queries.len() as f64) * 1000.0,
        p50 / 1000.0,
        p90 / 1000.0,
        p99 / 1000.0,
    );

    Ok(WarmSummary {
        recall_at_k: avg_recall,
        qps,
        num_centroids: db.num_centroids(),
        p50,
        p90,
        p99,
    })
}

fn recall_at_k(results: &[SearchResult], ground_truth: &[i32], k: usize) -> f64 {
    let gt_set: HashSet<i32> = ground_truth.iter().take(k).copied().collect();
    let found = results
        .iter()
        .take(k)
        .filter(|r| {
            r.vector
                .id
                .parse::<i32>()
                .map(|id| gt_set.contains(&id))
                .unwrap_or(false)
        })
        .count();
    found as f64 / k as f64
}
