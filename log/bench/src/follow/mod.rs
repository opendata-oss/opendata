//! The follow (poll) benchmark for LogDb — RFC 0006's cardinality benchmark.
//!
//! Milestone M3: the run proceeds through three phases over one live database —
//! **pre-fill** (build a per-key backlog and report ingest throughput),
//! **warm-up** (run arrivals and polls but discard metrics while the cache reaches
//! steady state), and **measure** (record everything). Poll latency and service
//! time are bucketed by lag at poll time, carrying the lag bucket as a metric
//! label. The measure window is the bencher `--duration`; warm-up is its own
//! parameter.
//!
//! Object-store GET counting is still deferred, so `poll_gets` is not yet recorded.

mod lag;
mod metrics;
mod scheduler;
mod store;
mod writer;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::time::Duration;

use bencher::{Bench, Benchmark, Params, Summary};
use bytes::Bytes;
use log::{Config, LogDb};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use lag::{LAG_BUCKET_LABELS, LagTracker, NUM_LAG_BUCKETS};
use metrics::FollowMetrics;
use scheduler::{PollDist, PollJob, run_shard, run_worker};
use store::{LogDbStore, LogStore};
use writer::{run_prefill, run_writer};

/// Phase while warming up: workload runs, metrics are discarded.
const PHASE_WARMUP: u8 = 0;
/// Phase while measuring: all metrics are recorded.
const PHASE_MEASURE: u8 = 1;
/// Phase after the measure window: recording stops again, so polls that complete
/// while the dispatch queue drains during shutdown are not counted.
const PHASE_DONE: u8 = 2;

/// State shared (read-only or via atomics) across the scheduler shards, the
/// execution-pool workers, and the arrivals writer. Child modules access its
/// fields directly.
struct FollowState {
    store: Arc<dyn LogStore>,
    keys: Arc<Vec<Bytes>>,
    /// Per-key resume position (next sequence). A key is only ever accessed by one
    /// in-flight poll at a time (enforced by `in_flight`). The handshake that hands
    /// a key to the next poll — the worker's `in_flight` release, the shard's
    /// acquiring claim, and the channel send/recv — establishes happens-before from
    /// one poll's cursor write to the next poll's read, so relaxed access is safe.
    cursors: Vec<AtomicU64>,
    /// Per-key guard: set while a poll for that key is in flight, so a consumer
    /// never runs two concurrent polls.
    in_flight: Vec<AtomicBool>,
    lag: Arc<LagTracker>,
    metrics: FollowMetrics,
    /// Readable scheduling-outcome totals for the end-of-run summary. The
    /// `metrics` counters mirror these for the live reporter, but `metrics` handles
    /// are write-only, so we keep our own readable totals here. Every scheduled
    /// poll resolves to exactly one of: completed, coalesced (consumer's prior poll
    /// still in flight), or dropped (bounded pool full).
    polls_completed: AtomicU64,
    polls_coalesced: AtomicU64,
    polls_dropped: AtomicU64,
    /// Completed polls per lag bucket (measure phase only), for the lag
    /// distribution in the summary.
    bucket_polls: Vec<AtomicU64>,
    page_size: usize,
    /// Current phase ([`PHASE_WARMUP`] or [`PHASE_MEASURE`]); gates metric recording.
    phase: AtomicU8,
    /// Run-wide scheduling constants, shared by every shard.
    dist: PollDist,
    start: Instant,
    seed: u64,
}

impl FollowState {
    /// Whether metrics should be recorded now (true only in the measure phase).
    fn recording(&self) -> bool {
        self.phase.load(Ordering::Relaxed) == PHASE_MEASURE
    }
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

/// The single fast smoke parameter set; full scenario grids come via `--config`.
fn smoke_params() -> Params {
    let mut p = Params::new();
    // Workload (backend-agnostic).
    p.insert("key_cardinality", "1000");
    p.insert("key_length", "16");
    p.insert("value_size", "128");
    p.insert("page_size", "32");
    p.insert("prefill_per_key", "16");
    p.insert("prefill_concurrency", "8");
    p.insert("warmup_secs", "2");
    p.insert("arrival_rate_per_key", "5.0");
    p.insert("poll_interval_mean_ms", "200");
    p.insert("offline_prob", "0.05");
    p.insert("offline_duration_ms", "2000");
    p.insert("seed", "42");
    // Harness sizing.
    p.insert("num_scheduler_shards", "4");
    p.insert("exec_concurrency", "8");
    p.insert("num_writer_tasks", "2");
    p.insert("dispatch_queue_capacity", "4096");
    p
}

#[async_trait::async_trait]
impl Benchmark for FollowBenchmark {
    fn name(&self) -> &str {
        "follow"
    }

    fn default_params(&self) -> Vec<Params> {
        vec![smoke_params()]
    }

    async fn run(&self, bench: Bench) -> anyhow::Result<()> {
        let params = bench.spec().params();
        let cardinality: usize = params.get_parse("key_cardinality")?;
        let key_length: usize = params.get_parse("key_length")?;
        let value_size: usize = params.get_parse("value_size")?;
        let page_size: usize = params.get_parse("page_size")?;
        let prefill_per_key: usize = params.get_parse("prefill_per_key")?;
        let prefill_concurrency: usize = params.get_parse("prefill_concurrency")?;
        let warmup_secs: f64 = params.get_parse("warmup_secs")?;
        let arrival_rate_per_key: f64 = params.get_parse("arrival_rate_per_key")?;
        let poll_interval_mean_ms: u64 = params.get_parse("poll_interval_mean_ms")?;
        let offline_prob: f64 = params.get_parse("offline_prob")?;
        let offline_duration_ms: u64 = params.get_parse("offline_duration_ms")?;
        let seed: u64 = params.get_parse("seed")?;
        let num_shards: usize = params.get_parse("num_scheduler_shards")?;
        let exec_concurrency: usize = params.get_parse("exec_concurrency")?;
        let num_writers: usize = params.get_parse::<usize>("num_writer_tasks")?.max(1);
        let dispatch_queue_capacity: usize = params.get_parse("dispatch_queue_capacity")?;

        let config = Config {
            storage: bench.spec().data().storage.clone(),
            ..Default::default()
        };
        let logdb_store = Arc::new(LogDbStore::new(LogDb::open(config).await?));
        let queue_full = logdb_store.queue_full_counter();

        let keys = Arc::new(
            (0..cardinality)
                .map(|i| Bytes::from(format!("{:0>width$}", i, width = key_length)))
                .collect::<Vec<Bytes>>(),
        );
        let value = Bytes::from(vec![b'x'; value_size]);

        let lag = Arc::new(LagTracker::new(cardinality));
        let follow_metrics = FollowMetrics::new(&bench);

        // Phase 1 — Pre-fill: give every key a backlog behind the tail so consumers
        // have something to catch up on, and segments/compaction exist before
        // measurement. Parallel + batched so it scales to large cardinalities.
        // Timed to report ingest throughput (ingest conventions).
        let record_size = (key_length + value_size) as u64;
        let prefill_records = (cardinality * prefill_per_key) as u64;
        let prefill_start = Instant::now();
        run_prefill(
            logdb_store.clone(),
            keys.clone(),
            value.clone(),
            prefill_per_key,
            prefill_concurrency,
            lag.clone(),
        )
        .await?;
        logdb_store.db().flush().await?;
        let prefill_secs = prefill_start.elapsed().as_secs_f64().max(f64::MIN_POSITIVE);
        let prefill_records_per_sec = prefill_records as f64 / prefill_secs;
        let prefill_bytes_per_sec = (prefill_records * record_size) as f64 / prefill_secs;

        let dist = PollDist {
            mean: Duration::from_millis(poll_interval_mean_ms),
            offline_prob,
            offline: Duration::from_millis(offline_duration_ms),
        };
        let store_dyn: Arc<dyn LogStore> = logdb_store.clone();
        let state = Arc::new(FollowState {
            store: store_dyn,
            keys,
            cursors: (0..cardinality).map(|_| AtomicU64::new(0)).collect(),
            in_flight: (0..cardinality).map(|_| AtomicBool::new(false)).collect(),
            lag,
            metrics: follow_metrics,
            polls_completed: AtomicU64::new(0),
            polls_coalesced: AtomicU64::new(0),
            polls_dropped: AtomicU64::new(0),
            bucket_polls: (0..NUM_LAG_BUCKETS).map(|_| AtomicU64::new(0)).collect(),
            page_size,
            phase: AtomicU8::new(PHASE_WARMUP),
            dist,
            start: Instant::now(),
            seed,
        });

        let (tx, rx) = async_channel::bounded::<PollJob>(dispatch_queue_capacity);
        let cancel = CancellationToken::new();

        // Execution pool: `exec_concurrency` workers pulling from the bounded queue.
        let mut worker_handles = Vec::with_capacity(exec_concurrency);
        for _ in 0..exec_concurrency {
            worker_handles.push(tokio::spawn(run_worker(rx.clone(), state.clone())));
        }
        drop(rx);

        // Scheduler shards: partition keys round-robin across shards.
        let mut shard_handles = Vec::with_capacity(num_shards);
        for s in 0..num_shards {
            let key_ids: Vec<usize> = (s..cardinality).step_by(num_shards).collect();
            shard_handles.push(tokio::spawn(run_shard(
                s,
                key_ids,
                tx.clone(),
                state.clone(),
                cancel.clone(),
            )));
        }
        // Keep one sender as a backlog probe; the shards hold the rest.
        let backlog_probe = tx.clone();
        drop(tx);

        // Arrivals writers.
        let per_writer_rate = (cardinality as f64 * arrival_rate_per_key) / num_writers as f64;
        let mut writer_handles = Vec::with_capacity(num_writers);
        for w in 0..num_writers {
            writer_handles.push(tokio::spawn(run_writer(
                w,
                num_writers,
                per_writer_rate,
                value.clone(),
                state.clone(),
                cancel.clone(),
            )));
        }

        // Phase 2 — Warm-up: workload runs but metrics are discarded (phase is still
        // PHASE_WARMUP) while the block cache reaches steady state.
        tokio::time::sleep(Duration::from_secs_f64(warmup_secs)).await;

        // Phase 3 — Measure: flip the phase so workers/shards/writers begin recording,
        // and measure for the bencher `--duration` window. Snapshot the queue-full
        // counter so the summary reflects only the measure window.
        let queue_full_at_measure = queue_full.load(Ordering::Relaxed);
        state.phase.store(PHASE_MEASURE, Ordering::Relaxed);
        let runner = bench.start();
        while runner.keep_running() {
            state
                .metrics
                .dispatch_backlog
                .set(backlog_probe.len() as f64);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let elapsed_secs = runner.elapsed().as_secs_f64();

        // Stop recording before shutting down: polls that complete while the
        // dispatch queue drains must not be counted past the measure window.
        state.phase.store(PHASE_DONE, Ordering::Relaxed);

        // Shut down: stop the clocks and writers, then let the pool drain.
        cancel.cancel();
        for h in shard_handles {
            h.await?;
        }
        for h in writer_handles {
            h.await??;
        }
        // All shard senders are now dropped; dropping the probe closes the channel,
        // so workers drain any queued jobs and exit.
        drop(backlog_probe);
        for h in worker_handles {
            h.await??;
        }

        logdb_store.db().flush().await?;

        let polls_total = state.polls_completed.load(Ordering::Relaxed);
        let coalesced = state.polls_coalesced.load(Ordering::Relaxed);
        let dropped = state.polls_dropped.load(Ordering::Relaxed);
        let residual_backlog = state.lag.total_lag();
        let queue_full_total = queue_full
            .load(Ordering::Relaxed)
            .saturating_sub(queue_full_at_measure);

        // Every scheduled poll is completed, coalesced, or dropped. The drop
        // fraction is the keeping-up signal: drops happen only when the bounded
        // execution pool is full, so a low fraction means the backend is generally
        // servicing the offered poll load. We report the fraction rather than a
        // pass/fail threshold — interpreting it is left to external analysis.
        let scheduled = polls_total + coalesced + dropped;
        let drop_fraction = if scheduled > 0 {
            dropped as f64 / scheduled as f64
        } else {
            0.0
        };

        let mut summary = Summary::new()
            .add("prefill_records_per_sec", prefill_records_per_sec)
            .add("prefill_bytes_per_sec", prefill_bytes_per_sec)
            .add("total_polls", polls_total as f64)
            .add("polls_per_sec", polls_total as f64 / elapsed_secs)
            .add("scheduled_polls", scheduled as f64)
            .add("dispatch_drop_fraction", drop_fraction)
            // `*_total` to avoid colliding with the same-named live counters when a
            // reporter is configured (those are cumulative series, these are scalars).
            .add("dispatch_dropped_total", dropped as f64)
            .add("polls_coalesced_total", coalesced as f64)
            .add("residual_backlog_records", residual_backlog as f64)
            .add("append_queue_full_total", queue_full_total as f64)
            .add("elapsed_ms", runner.elapsed().as_millis() as f64);

        // Lag distribution: completed polls per lag bucket over the measure window.
        // This characterizes the workload (which lag regimes were exercised); the
        // per-bucket latency quantiles come from the labeled histograms via a
        // configured reporter. Bucket labels carry a `polls_lag_` prefix.
        for (b, label) in LAG_BUCKET_LABELS.iter().enumerate() {
            let count = state.bucket_polls[b].load(Ordering::Relaxed);
            summary = summary.add(format!("polls_lag_{label}"), count as f64);
        }
        bench.summarize(summary).await?;

        // Drop the shared state so `logdb_store` is the sole owner, then close.
        drop(state);
        if let Ok(store) = Arc::try_unwrap(logdb_store) {
            store.into_db().close().await?;
        }
        bench.close().await?;
        Ok(())
    }
}
