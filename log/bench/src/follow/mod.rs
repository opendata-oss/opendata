//! The follow (poll) benchmark for LogDb — RFC 0006's cardinality benchmark,
//! session model.
//!
//! Models a large population of key-addressable logs followed not by permanent
//! per-key consumers but by **sessions**: a session wakes for one key, polls it for
//! a while, and closes. Session arrivals are a **global open-loop** Poisson stream
//! at `session_rate`, each picking a uniform-random key and executing on a bounded
//! pool of `max_active_sessions` slots. Decoupling read load from the key population
//! this way is the point: the number of concurrently active sessions can meet a
//! target bound even as `key_cardinality` grows to millions.
//!
//! A session is a bare poll loop: it polls its key every `poll_interval` for
//! `session_duration`, then closes. `session_duration = -1` polls forever;
//! continuous following is that case with `max_active_sessions ≥ key_cardinality`
//! and `poll_interval = 0`, so every key acquires a permanent session. (Catch-up /
//! idle behavior is left out for now, to be built up as needed.)
//!
//! "Sustain" is the active-session occupancy at the aggregate-goodput knee: sweep
//! `session_rate` up at fixed cardinality, and where aggregate read goodput
//! plateaus (per-session goodput falling as ~1/occupancy) is the sustainable
//! number of concurrent sessions. Session scheduling lag (queueing in the pool) is
//! the hard-saturation backstop.
//!
//! A run proceeds through three phases over one live database:
//! 1. **Pre-fill** — append a per-key backlog (parallel + batched); reports ingest
//!    throughput.
//! 2. **Warm-up** — run arrivals and sessions for `warmup_secs`, discarding metrics
//!    while the cache reaches steady state.
//! 3. **Measure** — record everything for the `--duration` window.

mod generator;
mod lag;
mod metrics;
mod session;
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
use tokio::sync::Semaphore;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use generator::run_generator;
use lag::{LAG_BUCKET_LABELS, LagTracker, NUM_LAG_BUCKETS};
use metrics::FollowMetrics;
use store::{LogDbStore, LogStore};
use writer::{run_prefill, run_writer};

use crate::workload;

/// Phase while warming up: workload runs, metrics are discarded.
const PHASE_WARMUP: u8 = 0;
/// Phase while measuring: all metrics are recorded.
const PHASE_MEASURE: u8 = 1;
/// Phase after the measure window: recording stops again, so work that completes
/// during shutdown is not counted past the measure window.
const PHASE_DONE: u8 = 2;

/// State shared (read-only or via atomics) across the session tasks and the
/// arrivals writer. Child modules access its fields directly.
pub struct FollowState {
    store: Arc<dyn LogStore>,
    keys: Arc<Vec<Bytes>>,
    /// Per-key resume position (next sequence), **persistent across sessions** so a
    /// returning session resumes where the last one left off. With short sessions a
    /// key is almost never drained by two sessions at once (uniform-random arrivals
    /// make a concurrent repeat rare); under a long `session_duration` two sessions can share
    /// a key, but the cursor is only ever advanced — overlapping reads at worst
    /// re-read a few records, which is harmless for a read benchmark — so relaxed
    /// access is safe.
    cursors: Vec<AtomicU64>,
    lag: Arc<LagTracker>,
    metrics: FollowMetrics,

    // ---- Session workload parameters ----
    page_size: usize,
    /// How long a session keeps polling, measured from session start: `Some(d)`
    /// stops at/after `d`, `None` polls forever (continuous following).
    session_duration: Option<Duration>,
    /// Gap between successive polls within a session (`0` = poll back-to-back).
    poll_interval: Duration,

    // ---- Phase ----
    phase: AtomicU8,
    /// Run start time, shared for the open-loop arrival schedule.
    start: Instant,

    // ---- Aggregate measure-phase tallies ----
    /// Completed polls (for `total_polls` / `gets_per_poll`).
    polls_completed: AtomicU64,
    /// Records appended by the arrivals writers.
    arrivals_completed: AtomicU64,
    /// Records / bytes drained across all sessions (aggregate read goodput).
    records_consumed: AtomicU64,
    bytes_consumed: AtomicU64,
    /// Sessions completed in the measure window.
    sessions_completed: AtomicU64,
    /// Sessions refused because the in-flight cap was hit (overload backstop).
    refused_sessions: AtomicU64,

    // ---- Occupancy (the headline "active sessions") ----
    /// Currently active sessions (holding a pool slot).
    active_sessions: AtomicU64,
    /// High-water mark of `active_sessions`.
    peak_active: AtomicU64,
    /// Time-integral of active sessions over the measure window, microseconds: the
    /// sum of each session's active duration. Mean occupancy = this / elapsed. This
    /// is accurate when sessions are short relative to the window; under a long
    /// `session_duration`, where sessions span the window boundary, `peak_active`
    /// is the meaningful occupancy signal instead.
    active_time_us: AtomicU64,

    // ---- Per-poll, bucketed by lag at poll time (RFC read-cost axis) ----
    bucket_polls: Vec<AtomicU64>,
    service_us: Vec<Mutex<Sketch>>,

    // ---- Session-level, bucketed by backlog at session start ----
    bucket_sessions: Vec<AtomicU64>,
    bucket_session_polls: Vec<AtomicU64>,
    /// Per-session goodput over DB-poll time (records/s); the contention signal.
    db_rate: Vec<Mutex<Sketch>>,

    /// Session scheduling lag (scheduled arrival → slot acquisition), microseconds.
    session_sched_lag_us: Mutex<Sketch>,
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
    // Session model.
    p.insert("session_rate", "500.0");
    p.insert("max_active_sessions", "256");
    p.insert("session_duration_ms", "100");
    p.insert("poll_interval_ms", "10");
    p.insert("seed", "1");
    // Phase / harness sizing.
    p.insert("prefill_per_key", "16");
    p.insert("prefill_concurrency", "8");
    p.insert("warmup_secs", "2");
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
        let prefill_per_key: usize = params.get_parse("prefill_per_key")?;
        let prefill_concurrency: usize = params.get_parse("prefill_concurrency")?;
        let warmup_secs: f64 = params.get_parse("warmup_secs")?;
        let num_writers: usize = params.get_parse::<usize>("num_writer_tasks")?.max(1);

        // Session model.
        let session_rate: f64 = params.get_parse("session_rate")?;
        let max_active_sessions: usize = params.get_parse::<usize>("max_active_sessions")?.max(1);
        // Gap between successive polls within a session (`0` = back-to-back).
        let poll_interval_ms: u64 = match params.get("poll_interval_ms") {
            Some(v) => v.parse()?,
            None => 0,
        };
        // `session_duration_ms`: -1 = poll forever (continuous following); n>=0 =
        // poll for n ms (0 does ~nothing). Omitted defaults to 100ms. `None` in the
        // parsed value means "forever".
        let session_duration = match params.get("session_duration_ms").map(str::parse::<i64>) {
            None => Some(Duration::from_millis(100)),
            Some(Ok(n)) if n < 0 => None,
            Some(Ok(n)) => Some(Duration::from_millis(n as u64)),
            Some(Err(e)) => return Err(e.into()),
        };
        let seed: u64 = match params.get("seed") {
            Some(v) => v.parse()?,
            None => 1,
        };
        // Backstop against unbounded queued sessions under overload.
        let max_inflight: usize = match params.get("max_inflight_sessions") {
            Some(v) => v.parse::<usize>()?.max(1),
            None => max_active_sessions.saturating_mul(16).max(1024),
        };

        // Read-path knobs (optional; default to reading through the writer).
        let read_path = params.get("read_path").unwrap_or("writer").to_string();
        let reader_instances = match params.get("reader_instances") {
            Some(v) => v.parse::<usize>()?.max(1),
            None => 1,
        };
        let refresh_interval_ms = match params.get("refresh_interval_ms") {
            Some(v) => v.parse::<u64>()?,
            None => 1000,
        };
        let block_cache_mb = match params.get("block_cache_mb") {
            Some(v) => v.parse::<u64>()?,
            None => 0,
        };

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

        // Phase 1 — Pre-fill: give every key a backlog behind the tail.
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

        let new_buckets = || (0..NUM_LAG_BUCKETS).map(|_| AtomicU64::new(0)).collect();
        let new_sketches = || {
            (0..NUM_LAG_BUCKETS)
                .map(|_| Mutex::new(Sketch::with_defaults()))
                .collect()
        };

        let store_dyn: Arc<dyn LogStore> = logdb_store.clone();
        let state = Arc::new(FollowState {
            store: store_dyn,
            keys,
            cursors: (0..cardinality).map(|_| AtomicU64::new(0)).collect(),
            lag,
            metrics: follow_metrics,
            page_size,
            session_duration,
            poll_interval: Duration::from_millis(poll_interval_ms),
            phase: AtomicU8::new(PHASE_WARMUP),
            start: Instant::now(),
            polls_completed: AtomicU64::new(0),
            arrivals_completed: AtomicU64::new(0),
            records_consumed: AtomicU64::new(0),
            bytes_consumed: AtomicU64::new(0),
            sessions_completed: AtomicU64::new(0),
            refused_sessions: AtomicU64::new(0),
            active_sessions: AtomicU64::new(0),
            peak_active: AtomicU64::new(0),
            active_time_us: AtomicU64::new(0),
            bucket_polls: new_buckets(),
            service_us: new_sketches(),
            bucket_sessions: new_buckets(),
            bucket_session_polls: new_buckets(),
            db_rate: new_sketches(),
            session_sched_lag_us: Mutex::new(Sketch::with_defaults()),
        });

        let cancel = CancellationToken::new();

        // Session generator: open-loop arrivals onto a bounded pool.
        let semaphore = Arc::new(Semaphore::new(max_active_sessions));
        let generator_handle = tokio::spawn(run_generator(
            state.clone(),
            cancel.clone(),
            session_rate,
            seed,
            semaphore,
            max_inflight,
        ));

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

        // Phase 3 — Measure.
        let queue_full_at_measure = queue_full.load(Ordering::Relaxed);
        let gets_at_measure = common::object_store_gets();
        let get_bytes_at_measure = common::object_store_get_bytes();
        let segment_scans_at_measure = log::segment_scans();
        let empty_segment_scans_at_measure = log::empty_segment_scans();
        state.phase.store(PHASE_MEASURE, Ordering::Relaxed);
        let runner = bench.start();
        while runner.keep_running() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let elapsed_secs = runner.elapsed().as_secs_f64().max(f64::MIN_POSITIVE);

        // Stop recording before shutting down, then stop the generator and writers.
        state.phase.store(PHASE_DONE, Ordering::Relaxed);
        let gets_total = common::object_store_gets().saturating_sub(gets_at_measure);
        let get_bytes_total = common::object_store_get_bytes().saturating_sub(get_bytes_at_measure);
        let segment_scans_total = log::segment_scans().saturating_sub(segment_scans_at_measure);
        let empty_segment_scans_total =
            log::empty_segment_scans().saturating_sub(empty_segment_scans_at_measure);
        cancel.cancel();
        generator_handle.await??;
        for h in writer_handles {
            h.await??;
        }

        logdb_store.db().flush().await?;

        let summary = build_summary(SummaryInputs {
            state: &state,
            elapsed_secs,
            record_size,
            session_rate,
            prefill_records_per_sec,
            prefill_bytes_per_sec,
            gets_total,
            get_bytes_total,
            segment_scans_total,
            empty_segment_scans_total,
            queue_full_total: queue_full
                .load(Ordering::Relaxed)
                .saturating_sub(queue_full_at_measure),
            elapsed_ms: runner.elapsed().as_millis() as f64,
        });
        bench.summarize(summary).await?;

        // Drop the shared state so `logdb_store` is the sole owner, then close.
        drop(state);
        if let Ok(store) = Arc::try_unwrap(logdb_store) {
            store.close().await?;
        }
        bench.close().await?;
        Ok(())
    }
}

/// Bundle of values the summary is built from.
struct SummaryInputs<'a> {
    state: &'a FollowState,
    elapsed_secs: f64,
    record_size: u64,
    session_rate: f64,
    prefill_records_per_sec: f64,
    prefill_bytes_per_sec: f64,
    gets_total: u64,
    get_bytes_total: u64,
    segment_scans_total: u64,
    empty_segment_scans_total: u64,
    queue_full_total: u64,
    elapsed_ms: f64,
}

/// Assemble the console summary from the measure-window tallies.
fn build_summary(inp: SummaryInputs<'_>) -> Summary {
    let s = inp.state;
    let elapsed = inp.elapsed_secs;

    let polls_total = s.polls_completed.load(Ordering::Relaxed);
    let arrivals_total = s.arrivals_completed.load(Ordering::Relaxed);
    let records_consumed = s.records_consumed.load(Ordering::Relaxed);
    let bytes_consumed = s.bytes_consumed.load(Ordering::Relaxed);
    let sessions_total = s.sessions_completed.load(Ordering::Relaxed);
    let active_time_us = s.active_time_us.load(Ordering::Relaxed);
    let mean_active = active_time_us as f64 / (elapsed * 1_000_000.0);

    // Read-amplification breakdown. The entry key is segment-major, so one poll
    // opens a `scan_entries` per segment covering its sequence range; these
    // ratios separate "how many segments per poll" (span) from "how dense each
    // segment is for the key" (locality), and how many scans were pure waste.
    let segment_scans = inp.segment_scans_total;
    let per = |num: u64, den: u64| {
        if den > 0 {
            num as f64 / den as f64
        } else {
            0.0
        }
    };
    let segments_per_poll = per(segment_scans, polls_total);
    let empty_segment_scan_frac = per(inp.empty_segment_scans_total, segment_scans);
    let records_per_segment_scan = per(records_consumed, segment_scans);
    let gets_per_segment_scan = per(inp.gets_total, segment_scans);
    // Bytes fetched from the object store per useful (value) byte delivered to a
    // consumer — 1.0 is perfect locality; higher means we over-read.
    let read_amplification = per(inp.get_bytes_total, bytes_consumed);

    let mut summary = Summary::new()
        .add("prefill_records_per_sec", inp.prefill_records_per_sec)
        .add("prefill_bytes_per_sec", inp.prefill_bytes_per_sec)
        // Offered write load actually achieved.
        .add("records_per_sec", arrivals_total as f64 / elapsed)
        .add(
            "bytes_per_sec",
            (arrivals_total * inp.record_size) as f64 / elapsed,
        )
        // Aggregate read goodput — the axis whose knee defines sustainable sessions.
        .add(
            "aggregate_goodput_records_per_sec",
            records_consumed as f64 / elapsed,
        )
        .add(
            "aggregate_goodput_bytes_per_sec",
            bytes_consumed as f64 / elapsed,
        )
        // Sessions and occupancy.
        .add("session_rate_offered", inp.session_rate)
        .add("sessions_completed", sessions_total as f64)
        .add("sessions_per_sec", sessions_total as f64 / elapsed)
        .add("mean_active_sessions", mean_active)
        .add(
            "peak_active_sessions",
            s.peak_active.load(Ordering::Relaxed) as f64,
        )
        .add(
            "refused_sessions",
            s.refused_sessions.load(Ordering::Relaxed) as f64,
        )
        // Polls and object-store cost.
        .add("total_polls", polls_total as f64)
        .add("polls_per_sec", polls_total as f64 / elapsed)
        .add("object_store_gets", inp.gets_total as f64)
        .add(
            "gets_per_poll",
            if polls_total > 0 {
                inp.gets_total as f64 / polls_total as f64
            } else {
                0.0
            },
        )
        .add("get_bytes_total", inp.get_bytes_total as f64)
        // Read-amplification breakdown (segment-major entry key): segment span per
        // poll, how many scans hit nothing, key density per segment, GETs per
        // segment scan, and object-store bytes fetched per useful byte delivered.
        .add("segment_scans", segment_scans as f64)
        .add("segments_per_poll", segments_per_poll)
        .add("empty_segment_scan_frac", empty_segment_scan_frac)
        .add("records_per_segment_scan", records_per_segment_scan)
        .add("gets_per_segment_scan", gets_per_segment_scan)
        .add("read_amplification", read_amplification)
        // Sanity check only — dominated by idle keys, not a health signal.
        .add("residual_backlog_records", s.lag.total_lag() as f64)
        .add("append_queue_full_total", inp.queue_full_total as f64)
        .add("elapsed_ms", inp.elapsed_ms);

    // Session scheduling lag — the hard-saturation backstop.
    {
        let sketch = s.session_sched_lag_us.lock().expect("sched lag mutex");
        if sketch.count() > 0 {
            summary = summary
                .add(
                    "session_sched_lag_us_p50",
                    sketch.quantile(0.5).unwrap_or(0.0),
                )
                .add(
                    "session_sched_lag_us_p90",
                    sketch.quantile(0.9).unwrap_or(0.0),
                )
                .add(
                    "session_sched_lag_us_p99",
                    sketch.quantile(0.99).unwrap_or(0.0),
                )
                .add("session_sched_lag_us_max", sketch.max());
        }
    }

    // Per-poll lag distribution (how far behind reads were at poll time).
    for (b, label) in LAG_BUCKET_LABELS.iter().enumerate() {
        let count = s.bucket_polls[b].load(Ordering::Relaxed);
        summary = summary.add(format!("polls_lag_{label}"), count as f64);
    }

    // Per-poll service latency percentiles, bucketed by lag at poll time (the RFC
    // read-cost axis). Only buckets that saw polls are emitted.
    for (b, label) in LAG_BUCKET_LABELS.iter().enumerate() {
        let sketch = s.service_us[b].lock().expect("service latency mutex");
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

    // Session-level progress, bucketed by backlog at session start: session count,
    // average polls per session, and per-session DB-rate goodput (records/s over
    // DB-poll time — the contention signal). Only buckets that saw sessions are
    // emitted.
    for (b, label) in LAG_BUCKET_LABELS.iter().enumerate() {
        let sessions = s.bucket_sessions[b].load(Ordering::Relaxed);
        if sessions == 0 {
            continue;
        }
        let polls = s.bucket_session_polls[b].load(Ordering::Relaxed);
        summary = summary
            .add(format!("sessions_backlog_{label}"), sessions as f64)
            .add(
                format!("avg_polls_per_session_backlog_{label}"),
                polls as f64 / sessions as f64,
            );

        let db = s.db_rate[b].lock().expect("db rate mutex");
        if db.count() > 0 {
            summary = summary
                .add(
                    format!("db_rate_backlog_{label}_p50"),
                    db.quantile(0.5).unwrap_or(0.0),
                )
                .add(
                    format!("db_rate_backlog_{label}_p90"),
                    db.quantile(0.9).unwrap_or(0.0),
                )
                .add(
                    format!("db_rate_backlog_{label}_p99"),
                    db.quantile(0.99).unwrap_or(0.0),
                );
        }
    }

    summary
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
