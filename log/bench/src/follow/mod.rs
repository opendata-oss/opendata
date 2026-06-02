//! The follow (poll) benchmark for LogDb — RFC 0006's cardinality benchmark.
//!
//! Milestone M0: the [`LogStore`] interface and its LogDb adapter, per-key lag
//! tracking, and a trivial **single-threaded** driver that exercises the
//! append/poll/cursor/lag machinery end to end. There is no open-loop scheduler
//! yet (M2), no lag-bucketed metrics yet (M3), and object-store GET counting is
//! deferred. This driver's purpose is to validate the read/cursor/lag path on a
//! tiny run before the concurrent harness is built on top of it.

mod lag;
mod store;

use bencher::{Bench, Benchmark, Params, Summary};
use bytes::Bytes;
use lag::LagTracker;
use log::{Config, LogDb};
use store::{Cursor, LogDbStore, LogStore};

const MICROS_PER_SEC: f64 = 1_000_000.0;

fn make_params(
    key_cardinality: usize,
    key_length: usize,
    value_size: usize,
    page_size: usize,
    prefill_per_key: usize,
) -> Params {
    let mut params = Params::new();
    params.insert("key_cardinality", key_cardinality.to_string());
    params.insert("key_length", key_length.to_string());
    params.insert("value_size", value_size.to_string());
    params.insert("page_size", page_size.to_string());
    params.insert("prefill_per_key", prefill_per_key.to_string());
    params
}

/// The follow/poll benchmark.
pub struct FollowBenchmark;

impl FollowBenchmark {
    pub fn new() -> Self {
        Self
    }
}

impl Default for FollowBenchmark {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl Benchmark for FollowBenchmark {
    fn name(&self) -> &str {
        "follow"
    }

    fn default_params(&self) -> Vec<Params> {
        // A single fast smoke set: small cardinality and a per-key backlog larger
        // than the page size, so the driver exercises multi-poll catch-up drains.
        vec![make_params(256, 16, 128, 32, 64)]
    }

    async fn run(&self, bench: Bench) -> anyhow::Result<()> {
        let cardinality: usize = bench.spec().params().get_parse("key_cardinality")?;
        let key_length: usize = bench.spec().params().get_parse("key_length")?;
        let value_size: usize = bench.spec().params().get_parse("value_size")?;
        let page_size: usize = bench.spec().params().get_parse("page_size")?;
        let prefill_per_key: usize = bench.spec().params().get_parse("prefill_per_key")?;

        let config = Config {
            storage: bench.spec().data().storage.clone(),
            ..Default::default()
        };
        let store = LogDbStore::new(LogDb::open(config).await?);

        // Dense integer keys, formatted like the ingest benchmark.
        let keys: Vec<Bytes> = (0..cardinality)
            .map(|i| Bytes::from(format!("{:0>width$}", i, width = key_length)))
            .collect();
        let value = Bytes::from(vec![b'x'; value_size]);

        let lag = LagTracker::new(cardinality);
        // Per-key resume position; each key is touched by only this thread.
        let mut cursors = vec![Cursor::default(); cardinality];

        // Pre-fill: give every key a backlog behind the tail.
        for (id, key) in keys.iter().enumerate() {
            for _ in 0..prefill_per_key {
                store.append(key.clone(), value.clone()).await?;
            }
            lag.record_appended(id, prefill_per_key as u64);
        }

        // Live metrics.
        let polls = bench.counter("polls");
        let records_consumed = bench.counter("records_consumed");
        let arrivals = bench.counter("arrivals");
        let poll_service = bench.histogram("poll_service_us");

        let runner = bench.start();
        let mut tick: usize = 0;
        let mut total_polls: u64 = 0;
        while runner.keep_running() {
            let id = tick % cardinality;
            let key = &keys[id];

            // One arrival, then one poll for the same key (single-threaded model).
            store.append(key.clone(), value.clone()).await?;
            lag.record_appended(id, 1);
            arrivals.increment(1);

            let start = std::time::Instant::now();
            let out = store.poll(key.clone(), cursors[id], page_size).await?;
            poll_service.record(start.elapsed().as_secs_f64() * MICROS_PER_SEC);

            cursors[id] = out.cursor;
            // The per-key lag at poll time (`lag.lag(id)`) is the analysis axis:
            // from M3 it will label the latency and GETs/poll histograms so backend
            // cost is reported as a function of lag. M0 records only aggregate
            // counters, so lag is updated here but not yet bucketed.
            lag.record_acked(id, out.n_records as u64);
            records_consumed.increment(out.n_records as u64);
            polls.increment(1);
            total_polls += 1;
            tick += 1;
        }
        store.db().flush().await?;

        let elapsed_secs = runner.elapsed().as_secs_f64();
        // Residual backlog is a drain sanity-check, not a performance result: it
        // confirms the harness consumed roughly what it appended. Lag is a workload
        // condition (see the `lag` module), so we never report it as a score.
        let residual_backlog = lag.total_lag();
        bench
            .summarize(
                Summary::new()
                    .add("polls_per_sec", total_polls as f64 / elapsed_secs)
                    .add("total_polls", total_polls as f64)
                    .add("residual_backlog_records", residual_backlog as f64)
                    .add("elapsed_ms", runner.elapsed().as_millis() as f64),
            )
            .await?;

        store.into_db().close().await?;
        bench.close().await?;
        Ok(())
    }
}
