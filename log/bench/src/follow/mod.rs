//! The follow (poll) benchmark for LogDb — RFC 0006's cardinality benchmark.
//!
//! Models a large population of key-addressable logs, each actively followed by a
//! consumer that tails it **closed-loop**: poll for what is new, wait for the
//! response, and poll again — immediately if a full page came back (a backlog
//! remains), or after a short idle delay once caught up to the tail. The number of
//! independently followed logs (`key_cardinality`) is the primary scaling axis.
//!
//! A run proceeds through three phases over one live database:
//! 1. **Pre-fill** — append a per-key backlog (parallel + batched); reports ingest
//!    throughput.
//! 2. **Warm-up** — run arrivals and follows for `warmup_secs`, discarding metrics
//!    while the cache reaches steady state.
//! 3. **Measure** — record everything for the `--duration` window.
//!
//! Poll latency and service time are bucketed by lag at poll time. Object-store GET
//! counting is deferred, so `poll_gets` is not yet recorded.

mod follower;
mod lag;
mod metrics;
mod store;
mod writer;

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::time::Duration;

use anyhow::bail;
use bencher::{Bench, Benchmark, Params, Summary};
use bytes::Bytes;
use common::StorageConfig;
use common::storage::config::ObjectStoreConfig;
use log::{Config, LogDb, LogDbReader, ReadVisibility, ReaderConfig};
use metrics_util::Summary as Sketch;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use follower::run_follower;
use lag::{LAG_BUCKET_LABELS, LagTracker, NUM_LAG_BUCKETS};
use metrics::FollowMetrics;
use store::{LogDbStore, LogStore};
use writer::{run_prefill, run_writer};

use crate::workload;

/// Phase while warming up: workload runs, metrics are discarded.
const PHASE_WARMUP: u8 = 0;
/// Phase while measuring: all metrics are recorded.
const PHASE_MEASURE: u8 = 1;
/// Phase after the measure window: recording stops again, so polls that complete
/// during shutdown are not counted past the measure window.
const PHASE_DONE: u8 = 2;

/// State shared (read-only or via atomics) across the follower runners and the
/// arrivals writer. Child modules access its fields directly.
struct FollowState {
    store: Arc<dyn LogStore>,
    keys: Arc<Vec<Bytes>>,
    /// Per-key resume position (next sequence). A key is owned by exactly one
    /// runner and polled one at a time (closed-loop), so relaxed access is safe.
    cursors: Vec<AtomicU64>,
    lag: Arc<LagTracker>,
    metrics: FollowMetrics,
    /// Readable running total of completed polls (measure phase). `metrics.polls`
    /// is the live counter for the reporter, but `metrics` handles are write-only,
    /// so we keep our own total here for the summary.
    polls_completed: AtomicU64,
    /// Readable running total of records appended by the arrivals writers
    /// (measure phase). Mirrors `metrics.arrivals` for the same reason
    /// `polls_completed` mirrors `metrics.polls`.
    arrivals_completed: AtomicU64,
    /// Completed polls per lag bucket (measure phase only), for the lag
    /// distribution in the summary.
    bucket_polls: Vec<AtomicU64>,
    /// Per-lag-bucket read-service latency (microseconds), measure phase only.
    /// Kept alongside `metrics.poll_service_us` so percentiles can be printed in
    /// the console summary without a configured reporter.
    service_us: Vec<Mutex<Sketch>>,
    page_size: usize,
    /// Delay before a caught-up follower re-polls (the only polling cadence).
    idle_interval: Duration,
    /// Current phase ([`PHASE_WARMUP`] / [`PHASE_MEASURE`] / [`PHASE_DONE`]).
    phase: AtomicU8,
    /// Run start time, shared by every runner for initial scheduling.
    start: Instant,
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
    p.insert("arrival_rate_per_key", "5.0");
    p.insert("idle_poll_interval_ms", "100");
    // Phase / harness sizing.
    p.insert("prefill_per_key", "16");
    p.insert("prefill_concurrency", "8");
    p.insert("warmup_secs", "2");
    p.insert("reader_concurrency", "16");
    p.insert("num_writer_tasks", "4");
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
        let page_size: usize = params.get_parse::<usize>("page_size")?.max(1);
        let arrival_rate_per_key: f64 = params.get_parse("arrival_rate_per_key")?;
        let idle_poll_interval_ms: u64 = params.get_parse("idle_poll_interval_ms")?;
        let prefill_per_key: usize = params.get_parse("prefill_per_key")?;
        let prefill_concurrency: usize = params.get_parse("prefill_concurrency")?;
        let warmup_secs: f64 = params.get_parse("warmup_secs")?;
        let reader_concurrency: usize = params.get_parse::<usize>("reader_concurrency")?.max(1);
        let num_writers: usize = params.get_parse::<usize>("num_writer_tasks")?.max(1);

        // Read-path knobs (optional; default to reading through the writer).
        // `read_path = writer` polls the writer's own handle; `read_path = reader`
        // serves polls from a pool of `reader_instances` independent readers over
        // the shared object store, each refreshing every `refresh_interval_ms`.
        let read_path = params.get("read_path").unwrap_or("writer").to_string();
        let reader_instances = match params.get("reader_instances") {
            Some(v) => v.parse::<usize>()?.max(1),
            None => 1,
        };
        let refresh_interval_ms = match params.get("refresh_interval_ms") {
            Some(v) => v.parse::<u64>()?,
            None => 1000,
        };
        // Size of a single in-memory block cache *shared* across the reader pool
        // (MiB). 0 (default) keeps the current behavior: each reader builds its
        // own cache, so the pool re-fetches shared SST blocks per reader.
        let block_cache_mb = match params.get("block_cache_mb") {
            Some(v) => v.parse::<u64>()?,
            None => 0,
        };

        // Writer read visibility (only relevant when `read_path = writer`; pooled
        // readers always read the object store). `remote` restricts the writer's
        // own reads to object-store-durable data instead of MEMORY-visible writes.
        let read_visibility = match params.get("read_visibility").unwrap_or("memory") {
            "memory" => ReadVisibility::Memory,
            "remote" => ReadVisibility::Remote,
            other => bail!("unknown read_visibility '{other}' (expected 'memory' or 'remote')"),
        };

        let storage_config = bench.spec().data().storage.clone();
        let config = Config {
            storage: storage_config.clone(),
            read_visibility,
            ..Default::default()
        };
        let writer = LogDb::open(config).await?;
        let logdb_store = match read_path.as_str() {
            "writer" => Arc::new(LogDbStore::new(writer)),
            "reader" => {
                if !object_store_is_shared(&storage_config) {
                    bail!(
                        "read_path=reader requires a shared object store (Local or Aws); \
                         the configured store is in-memory and per-handle, so readers \
                         would observe none of the writer's data"
                    );
                }
                let refresh = Duration::from_millis(refresh_interval_ms);
                // One shared block cache for the whole pool when requested, so a
                // block fetched by any reader is reused by the rest.
                let shared_cache = if block_cache_mb > 0 {
                    Some(common::create_in_memory_block_cache(
                        block_cache_mb * 1024 * 1024,
                    ))
                } else {
                    None
                };
                let mut readers = Vec::with_capacity(reader_instances);
                for _ in 0..reader_instances {
                    let reader_config = ReaderConfig {
                        storage: storage_config.clone(),
                        refresh_interval: refresh,
                    };
                    let reader = match &shared_cache {
                        Some(cache) => {
                            LogDbReader::open_with_block_cache(reader_config, cache.clone()).await?
                        }
                        None => LogDbReader::open(reader_config).await?,
                    };
                    readers.push(reader);
                }
                Arc::new(LogDbStore::with_readers(writer, readers))
            }
            other => bail!("unknown read_path '{other}' (expected 'writer' or 'reader')"),
        };
        let queue_full = logdb_store.queue_full_counter();

        let keys = Arc::new(workload::keys(cardinality, key_length));
        let value = workload::value_template(value_size);

        let lag = Arc::new(LagTracker::new(cardinality));
        let follow_metrics = FollowMetrics::new(&bench);

        // Phase 1 — Pre-fill: give every key a backlog behind the tail so followers
        // have something to catch up on, and segments/compaction exist before
        // measurement. Parallel + batched so it scales to large cardinalities.
        let record_size = workload::record_size(key_length, value_size) as u64;
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

        let store_dyn: Arc<dyn LogStore> = logdb_store.clone();
        let state = Arc::new(FollowState {
            store: store_dyn,
            keys,
            cursors: (0..cardinality).map(|_| AtomicU64::new(0)).collect(),
            lag,
            metrics: follow_metrics,
            polls_completed: AtomicU64::new(0),
            arrivals_completed: AtomicU64::new(0),
            bucket_polls: (0..NUM_LAG_BUCKETS).map(|_| AtomicU64::new(0)).collect(),
            service_us: (0..NUM_LAG_BUCKETS)
                .map(|_| Mutex::new(Sketch::with_defaults()))
                .collect(),
            page_size,
            idle_interval: Duration::from_millis(idle_poll_interval_ms),
            phase: AtomicU8::new(PHASE_WARMUP),
            start: Instant::now(),
        });

        let cancel = CancellationToken::new();

        // Follower runners: partition keys round-robin; each runner follows its keys
        // closed-loop, one poll in flight at a time, so concurrency == runner count.
        let mut follower_handles = Vec::with_capacity(reader_concurrency);
        for r in 0..reader_concurrency {
            let key_ids: Vec<usize> = (r..cardinality).step_by(reader_concurrency).collect();
            follower_handles.push(tokio::spawn(run_follower(
                key_ids,
                state.clone(),
                cancel.clone(),
            )));
        }

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

        // Phase 2 — Warm-up: workload runs but metrics are discarded.
        tokio::time::sleep(Duration::from_secs_f64(warmup_secs)).await;

        // Phase 3 — Measure: flip the phase so runners/writers begin recording, and
        // measure for the bencher `--duration` window. Snapshot the queue-full
        // counter so the summary reflects only the measure window.
        let queue_full_at_measure = queue_full.load(Ordering::Relaxed);
        let gets_at_measure = common::object_store_gets();
        let get_bytes_at_measure = common::object_store_get_bytes();
        state.phase.store(PHASE_MEASURE, Ordering::Relaxed);
        let runner = bench.start();
        while runner.keep_running() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let elapsed_secs = runner.elapsed().as_secs_f64();

        // Stop recording before shutting down, then stop the runners and writers.
        state.phase.store(PHASE_DONE, Ordering::Relaxed);
        // Snapshot object-store GETs over the measure window (process-global; in
        // reader mode this is dominated by the reader pool, plus the writer's
        // background compaction reads, which are independent of reader_instances).
        let gets_total = common::object_store_gets().saturating_sub(gets_at_measure);
        let get_bytes_total = common::object_store_get_bytes().saturating_sub(get_bytes_at_measure);
        cancel.cancel();
        for h in follower_handles {
            h.await??;
        }
        for h in writer_handles {
            h.await??;
        }

        logdb_store.db().flush().await?;

        let polls_total = state.polls_completed.load(Ordering::Relaxed);
        let arrivals_total = state.arrivals_completed.load(Ordering::Relaxed);
        let residual_backlog = state.lag.total_lag();
        let queue_full_total = queue_full
            .load(Ordering::Relaxed)
            .saturating_sub(queue_full_at_measure);

        let mut summary = Summary::new()
            .add("prefill_records_per_sec", prefill_records_per_sec)
            .add("prefill_bytes_per_sec", prefill_bytes_per_sec)
            .add("records_per_sec", arrivals_total as f64 / elapsed_secs)
            .add(
                "bytes_per_sec",
                (arrivals_total * record_size) as f64 / elapsed_secs,
            )
            .add("total_polls", polls_total as f64)
            .add("polls_per_sec", polls_total as f64 / elapsed_secs)
            .add("object_store_gets", gets_total as f64)
            .add(
                "gets_per_poll",
                if polls_total > 0 {
                    gets_total as f64 / polls_total as f64
                } else {
                    0.0
                },
            )
            .add("get_bytes_total", get_bytes_total as f64)
            .add("residual_backlog_records", residual_backlog as f64)
            .add("append_queue_full_total", queue_full_total as f64)
            .add("elapsed_ms", runner.elapsed().as_millis() as f64);

        // Lag distribution: completed polls per lag bucket over the measure window.
        // This characterizes how far behind followers were when they polled; the
        // per-bucket latency quantiles come from the labeled histograms via a
        // configured reporter.
        for (b, label) in LAG_BUCKET_LABELS.iter().enumerate() {
            let count = state.bucket_polls[b].load(Ordering::Relaxed);
            summary = summary.add(format!("polls_lag_{label}"), count as f64);
        }

        // Per-lag-bucket read-service latency percentiles (microseconds). The lag
        // bucket is the RFC's read-cost axis — comparing the same bucket across
        // cardinalities shows whether per-poll cost stays flat. Only buckets that
        // saw polls are emitted.
        for (b, label) in LAG_BUCKET_LABELS.iter().enumerate() {
            let sketch = state.service_us[b].lock().expect("service latency mutex");
            if sketch.count() == 0 {
                continue;
            }
            let q = |quantile: f64| sketch.quantile(quantile).unwrap_or(0.0);
            summary = summary
                .add(format!("service_us_lag_{label}_p50"), q(0.5))
                .add(format!("service_us_lag_{label}_p90"), q(0.9))
                .add(format!("service_us_lag_{label}_p99"), q(0.99))
                .add(format!("service_us_lag_{label}_max"), sketch.max());
        }
        bench.summarize(summary).await?;

        // Drop the shared state so `logdb_store` is the sole owner, then close
        // (shuts down the reader pool, if any, then the writer).
        drop(state);
        if let Ok(store) = Arc::try_unwrap(logdb_store) {
            store.close().await?;
        }
        bench.close().await?;
        Ok(())
    }
}

/// Whether `storage` uses a shared object store (Local/Aws) that an independent
/// [`LogDbReader`] can observe. An in-memory object store is per-handle, so the
/// reader pool would see none of the writer's data.
fn object_store_is_shared(storage: &StorageConfig) -> bool {
    match storage {
        StorageConfig::InMemory => false,
        StorageConfig::SlateDb(c) => !matches!(c.object_store, ObjectStoreConfig::InMemory),
    }
}
