//! WARM phase: open a fresh `VectorDb` and, for each configured scorer, run
//! an unmeasured warmup pass over all queries followed by a measured
//! sequential pass (concurrency 1, unthrottled) that produces the per-scorer
//! latency metrics. The first few queries' result lists are cross-checked
//! between scorers to catch BlockMax/Exhaustive divergence.

use std::time::Instant;

use bencher::Bench;
use vector::{Bm25Scorer, Config, Query, SearchOptions, VectorDb, VectorDbRead};

use crate::fts::{BODY_FIELD, FtsSpec, open_db, scorer_name};
use crate::recall::{percentile, warm_default_memory_bytes};

/// Number of leading queries whose result lists are compared across scorers.
const CONSISTENCY_CHECK_QUERIES: usize = 20;

/// Latency metrics for one scorer's measured pass.
pub struct ScorerSummary {
    pub p50: f64,
    pub p90: f64,
    pub p99: f64,
    pub mean: f64,
    pub qps: f64,
}

/// Metrics produced by the warm phase.
pub struct WarmSummary {
    /// Per-scorer latency summaries, in the spec's scorer order.
    pub scorers: Vec<(&'static str, ScorerSummary)>,
    /// Number of (query, scorer-pair) result-list divergences observed.
    pub result_mismatches: u64,
}

pub async fn run(
    spec: &FtsSpec,
    config: &Config,
    queries: &[String],
    bench: &Bench,
) -> anyhow::Result<WarmSummary> {
    let db = open_db(config, spec, warm_default_memory_bytes()).await?;
    let summary = warm(spec, &db, queries, bench).await?;
    db.close().await?;
    Ok(summary)
}

async fn warm(
    spec: &FtsSpec,
    db: &VectorDb,
    queries: &[String],
    bench: &Bench,
) -> anyhow::Result<WarmSummary> {
    let check_n = queries.len().min(CONSISTENCY_CHECK_QUERIES);
    // Result-id lists for the first `check_n` queries, per scorer, used for
    // the cross-scorer consistency check.
    let mut check_results: Vec<(&'static str, Vec<Vec<String>>)> =
        Vec::with_capacity(spec.scorers.len());
    let mut summaries = Vec::with_capacity(spec.scorers.len());

    for scorer in spec.scorers.iter().copied() {
        let name = scorer_name(scorer);

        // -- Warmup (unmeasured) -------------------------------------------
        println!("  [{}] start warmup", name);
        for query in queries {
            let _ = search(db, spec, query, scorer).await?;
        }
        println!("  [{}] end warmup", name);

        // -- Measured sequential pass --------------------------------------
        let latency_hist = bench.histogram(latency_metric(scorer));
        let mut latencies_us = Vec::with_capacity(queries.len());
        let mut ids: Vec<Vec<String>> = Vec::with_capacity(check_n);
        let pass_start = Instant::now();
        for (i, query) in queries.iter().enumerate() {
            let t = Instant::now();
            let results = search(db, spec, query, scorer).await?;
            let elapsed_us = t.elapsed().as_secs_f64() * 1_000_000.0;
            latency_hist.record(elapsed_us);
            latencies_us.push(elapsed_us);
            if i < check_n {
                ids.push(results);
            }
        }
        let pass_secs = pass_start.elapsed().as_secs_f64();
        let qps = queries.len() as f64 / pass_secs;
        let mean = latencies_us.iter().sum::<f64>() / latencies_us.len().max(1) as f64;

        latencies_us.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p50 = percentile(&latencies_us, 50.0);
        let p90 = percentile(&latencies_us, 90.0);
        let p99 = percentile(&latencies_us, 99.0);

        println!(
            "  [{}] QPS = {:.1}, mean = {:.2} ms, p50 = {:.2} ms, p90 = {:.2} ms, p99 = {:.2} ms",
            name,
            qps,
            mean / 1000.0,
            p50 / 1000.0,
            p90 / 1000.0,
            p99 / 1000.0,
        );

        check_results.push((name, ids));
        summaries.push((
            name,
            ScorerSummary {
                p50,
                p90,
                p99,
                mean,
                qps,
            },
        ));
    }

    let result_mismatches = count_result_mismatches(queries, &check_results, check_n);

    Ok(WarmSummary {
        scorers: summaries,
        result_mismatches,
    })
}

/// Run one BM25 query against the `body` field, returning the result doc
/// ids in rank order.
async fn search(
    db: &VectorDb,
    spec: &FtsSpec,
    query: &str,
    scorer: Bm25Scorer,
) -> anyhow::Result<Vec<String>> {
    let q = Query::bm25(BODY_FIELD, query).with_limit(spec.limit);
    let results = db
        .search_with_options(
            &q,
            SearchOptions {
                nprobe: None,
                bm25_scorer: Some(scorer),
            },
        )
        .await?;
    Ok(results.into_iter().map(|r| r.vector.id).collect())
}

/// Per-scorer histogram name (must be `&'static str` for the bench recorder).
fn latency_metric(scorer: Bm25Scorer) -> &'static str {
    match scorer {
        Bm25Scorer::BlockMax => "block_max_query_latency_us",
        Bm25Scorer::BlockMaxSparse => "block_max_sparse_query_latency_us",
        Bm25Scorer::Exhaustive => "exhaustive_query_latency_us",
    }
}

/// Order-sensitive comparison of the first `check_n` queries' result-id
/// lists between the first scorer and every other scorer. Mismatches are
/// logged (not fatal) and counted.
fn count_result_mismatches(
    queries: &[String],
    check_results: &[(&'static str, Vec<Vec<String>>)],
    check_n: usize,
) -> u64 {
    let Some((base_name, base)) = check_results.first() else {
        return 0;
    };
    let mut mismatches = 0u64;
    for (name, ids) in &check_results[1..] {
        for i in 0..check_n {
            if ids[i] != base[i] {
                mismatches += 1;
                eprintln!(
                    "  result mismatch on query {:?}: {}={:?} vs {}={:?}",
                    queries[i], base_name, base[i], name, ids[i],
                );
            }
        }
    }
    if mismatches == 0 && check_results.len() > 1 {
        println!(
            "  consistency check: {} queries identical across {} scorers",
            check_n,
            check_results.len(),
        );
    }
    mismatches
}
