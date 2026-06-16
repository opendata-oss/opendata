//! The burst read-capacity benchmark for LogDb.
//!
//! Where the `follow` benchmark models an open-loop *population* of sessions
//! (Poisson arrivals, emergent occupancy) over a live arrival stream, this
//! benchmark asks a steady-state capacity question: with a **fixed** pool of
//! `active_sessions` concurrent readers over a **static** high-cardinality corpus,
//! how cheaply can a single scalable [`LogDbReader`] serve scans, and does that
//! hold as the key population grows? The headline is a capacity surface over
//! `(active_sessions, session_duration, key_cardinality)` — "how many logs can be
//! actively queried relative to the number of keys."
//!
//! Readers are independent of writers (no concurrent arrivals): the corpus is
//! built once in a prefill phase and is static during measurement. The burst
//! write pattern's only role here is to shape the on-disk layout — prefill
//! scatters each key's records across segments (see [`prefill`]) so the reader
//! pays realistic segment fan-out.
//!
//! `session_duration` walks between two read patterns over that fixed pool:
//! long/forever ⇒ a consistent set of readers consuming few keys deeply; short ⇒
//! the active set sweeping the population, resuming each key from its stored
//! cursor. A run proceeds prefill → warm-up → measure over one live database, with
//! polls served by one standalone reader (the layer that scales).

mod driver;
mod metrics;
mod prefill;

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::time::Duration;

use anyhow::bail;
use bencher::{Bench, Benchmark, Params, Summary};
use bytes::Bytes;
use common::StorageConfig;
use common::storage::config::ObjectStoreConfig;
use log::{Config, LogDb, LogDbReader, ReaderConfig};
use metrics_util::Summary as Sketch;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::read::lag::{LAG_BUCKET_LABELS, LagTracker, NUM_LAG_BUCKETS};
use crate::read::store::{LogDbStore, LogStore};
use crate::workload;
use metrics::ReadMetrics;

/// Phase while warming up: workload runs, metrics are discarded.
const PHASE_WARMUP: u8 = 0;
/// Phase while measuring: all metrics are recorded.
const PHASE_MEASURE: u8 = 1;
/// Phase after the measure window: recording stops again.
const PHASE_DONE: u8 = 2;

/// State shared (read-only or via atomics) across the session workers.
pub struct ReadState {
    store: Arc<dyn LogStore>,
    keys: Arc<Vec<Bytes>>,
    /// Per-key resume position (next sequence), **persistent across sessions** so a
    /// returning session resumes where the last left off. Stride-sharded workers
    /// mean each key's cursor is only ever touched by one worker — relaxed access
    /// is safe with no claim.
    cursors: Vec<AtomicU64>,
    /// Per-key "polled during the measure window" flag, for `distinct_keys_touched`.
    touched: Vec<AtomicBool>,
    lag: Arc<LagTracker>,
    metrics: ReadMetrics,

    // ---- Workload parameters ----
    page_size: usize,
    /// How long a session polls one key: `Some(d)` stops at/after `d`, `None` polls
    /// forever (within the measure window) — a consistent reader glued to one key.
    session_duration: Option<Duration>,
    /// Gap between successive polls within a session (`0` = back-to-back).
    poll_interval: Duration,

    // ---- Phase ----
    phase: AtomicU8,

    // ---- Measure-window tallies ----
    polls_completed: AtomicU64,
    records_consumed: AtomicU64,
    bytes_consumed: AtomicU64,
    sessions_completed: AtomicU64,
    distinct_keys_touched: AtomicU64,

    // ---- Per-poll, bucketed by lag at poll time (RFC read-cost axis) ----
    bucket_polls: Vec<AtomicU64>,
    service_us: Vec<Mutex<Sketch>>,
}

impl ReadState {
    /// Whether metrics should be recorded now (true only in the measure phase).
    fn recording(&self) -> bool {
        self.phase.load(Ordering::Relaxed) == PHASE_MEASURE
    }
}

/// The burst read-capacity benchmark.
pub struct BurstReadBenchmark;

impl BurstReadBenchmark {
    pub fn new() -> Self {
        Self
    }
}

impl Default for BurstReadBenchmark {
    fn default() -> Self {
        Self::new()
    }
}

/// The single fast smoke parameter set; full scenario grids come via `--config`.
/// Requires a shared object store (Local/Aws), since the standalone reader is the
/// system under test — the in-memory default store will be rejected.
fn smoke_params() -> Params {
    let mut p = Params::new();
    p.insert("key_cardinality", "1000");
    p.insert("key_length", "16");
    p.insert("value_size", "128");
    p.insert("page_size", "32");
    // Read pool.
    p.insert("active_sessions", "64");
    p.insert("session_duration_ms", "50");
    p.insert("poll_interval_ms", "0");
    // Corpus shape: 64 records/key across 8 bursts of 8 → scattered over segments.
    p.insert("prefill_per_key", "64");
    p.insert("burst_size", "8");
    p.insert("prefill_concurrency", "8");
    p.insert("seal_interval_ms", "500");
    // Reader.
    p.insert("block_cache_mb", "0");
    p.insert("refresh_interval_ms", "1000");
    p.insert("warmup_secs", "3");
    p
}

#[async_trait::async_trait]
impl Benchmark for BurstReadBenchmark {
    fn name(&self) -> &str {
        "burst-read"
    }

    fn default_params(&self) -> Vec<Params> {
        vec![smoke_params()]
    }

    async fn run(&self, bench: Bench) -> anyhow::Result<()> {
        let params = bench.spec().params();
        let cardinality: usize = params.get_parse::<usize>("key_cardinality")?.max(1);
        let key_length: usize = params.get_parse("key_length")?;
        let value_size: usize = params.get_parse("value_size")?;
        let page_size: usize = params.get_parse::<usize>("page_size")?.max(1);

        // Fixed read concurrency, capped at the population (a worker needs >= 1 key).
        let active_sessions: usize = params
            .get_parse::<usize>("active_sessions")?
            .max(1)
            .min(cardinality);
        // `session_duration_ms`: -1 = poll forever (consistent reader on one key);
        // n >= 0 = poll for n ms. `None` in the parsed value means "forever".
        let session_duration = match params.get("session_duration_ms").map(str::parse::<i64>) {
            None => Some(Duration::from_millis(100)),
            Some(Ok(n)) if n < 0 => None,
            Some(Ok(n)) => Some(Duration::from_millis(n as u64)),
            Some(Err(e)) => return Err(e.into()),
        };
        let poll_interval_ms: u64 = match params.get("poll_interval_ms") {
            Some(v) => v.parse()?,
            None => 0,
        };

        // Corpus shape.
        let prefill_per_key: usize = params.get_parse::<usize>("prefill_per_key")?.max(1);
        let burst_size: usize = params
            .get_parse::<usize>("burst_size")?
            .max(1)
            .min(prefill_per_key);
        let prefill_concurrency: usize = params.get_parse::<usize>("prefill_concurrency")?.max(1);
        // Rounds = passes over the population; each pass writes one `burst_size`
        // burst per key, so per-key depth is `rounds * burst_size`. Scattering each
        // key's bursts across rounds (and seals) is what produces segment fan-out.
        let rounds = (prefill_per_key / burst_size).max(1);
        let effective_per_key = rounds * burst_size;
        // Segment seal interval (ms) during prefill: cuts the corpus into segments
        // so a key's per-round bursts land in different ones. 0/omitted = no sealing
        // (one segment — no segment fan-out, defeating the point).
        let seal_interval = match params.get("seal_interval_ms") {
            Some(v) => match v.parse::<u64>()? {
                0 => None,
                n => Some(Duration::from_millis(n)),
            },
            None => None,
        };

        let warmup_secs: f64 = params.get_parse("warmup_secs")?;
        // Standalone reader: refresh interval (ms) and block cache size (MiB; 0 =
        // reader default). On a static corpus the refresh only matters for the
        // reader to first observe the prefill, which warm-up covers.
        let refresh_interval_ms: u64 = match params.get("refresh_interval_ms") {
            Some(v) => v.parse()?,
            None => 1000,
        };
        let block_cache_mb: u64 = match params.get("block_cache_mb") {
            Some(v) => v.parse()?,
            None => 0,
        };

        // The standalone reader is the system under test, so a shared object store
        // (Local/Aws) is required — an in-memory store is per-handle and the reader
        // would observe none of the writer's prefill.
        let storage_config = bench.spec().data().storage.clone();
        if !object_store_is_shared(&storage_config) {
            bail!(
                "burst-read requires a shared object store (Local or Aws); the configured \
                 store is in-memory and per-handle, so the standalone LogDbReader (the layer \
                 under test) would observe none of the prefilled corpus"
            );
        }

        let mut config = Config {
            storage: storage_config.clone(),
            ..Default::default()
        };
        config.segmentation.seal_interval = seal_interval;
        let writer = LogDb::open(config).await?;
        let reader_config = ReaderConfig {
            storage: storage_config,
            refresh_interval: Duration::from_millis(refresh_interval_ms),
        };
        let reader = if block_cache_mb > 0 {
            let cache = common::create_in_memory_block_cache(block_cache_mb * 1024 * 1024);
            LogDbReader::open_with_block_cache(reader_config, cache).await?
        } else {
            LogDbReader::open(reader_config).await?
        };
        let logdb_store = Arc::new(LogDbStore::with_reader(writer, reader));

        let keys = Arc::new(workload::keys(cardinality, key_length));
        let value = workload::value_template(value_size);
        let record_size = workload::record_size(key_length, value_size) as u64;
        let lag = Arc::new(LagTracker::new(cardinality));
        let read_metrics = ReadMetrics::new(&bench);

        // Phase 1 — Pre-fill: write the scattered, deep, per-key corpus.
        let prefill_records = (cardinality * effective_per_key) as u64;
        let prefill_start = Instant::now();
        prefill::run_prefill(
            logdb_store.clone(),
            keys.clone(),
            value.clone(),
            rounds,
            burst_size,
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
        let state = Arc::new(ReadState {
            store: store_dyn,
            keys,
            cursors: (0..cardinality).map(|_| AtomicU64::new(0)).collect(),
            touched: (0..cardinality).map(|_| AtomicBool::new(false)).collect(),
            lag,
            metrics: read_metrics,
            page_size,
            session_duration,
            poll_interval: Duration::from_millis(poll_interval_ms),
            phase: AtomicU8::new(PHASE_WARMUP),
            polls_completed: AtomicU64::new(0),
            records_consumed: AtomicU64::new(0),
            bytes_consumed: AtomicU64::new(0),
            sessions_completed: AtomicU64::new(0),
            distinct_keys_touched: AtomicU64::new(0),
            bucket_polls: new_buckets(),
            service_us: new_sketches(),
        });

        // Launch the fixed pool of session workers.
        let cancel = CancellationToken::new();
        let mut handles = Vec::with_capacity(active_sessions);
        for w in 0..active_sessions {
            handles.push(tokio::spawn(driver::run_worker(
                w,
                active_sessions,
                state.clone(),
                cancel.clone(),
            )));
        }

        // Phase 2 — Warm-up: workers run, metrics discarded; the reader picks up the
        // prefilled corpus and the block cache reaches steady state.
        tokio::time::sleep(Duration::from_secs_f64(warmup_secs)).await;

        // Phase 3 — Measure.
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

        // Stop recording, snapshot cost counters, then stop the workers.
        state.phase.store(PHASE_DONE, Ordering::Relaxed);
        let gets_total = common::object_store_gets().saturating_sub(gets_at_measure);
        let get_bytes_total = common::object_store_get_bytes().saturating_sub(get_bytes_at_measure);
        let segment_scans_total = log::segment_scans().saturating_sub(segment_scans_at_measure);
        let empty_segment_scans_total =
            log::empty_segment_scans().saturating_sub(empty_segment_scans_at_measure);
        cancel.cancel();
        for h in handles {
            h.await??;
        }

        let summary = build_summary(SummaryInputs {
            state: &state,
            elapsed_secs,
            active_sessions,
            session_duration,
            effective_per_key,
            rounds,
            prefill_records_per_sec,
            prefill_bytes_per_sec,
            gets_total,
            get_bytes_total,
            segment_scans_total,
            empty_segment_scans_total,
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
    state: &'a ReadState,
    elapsed_secs: f64,
    active_sessions: usize,
    session_duration: Option<Duration>,
    effective_per_key: usize,
    rounds: usize,
    prefill_records_per_sec: f64,
    prefill_bytes_per_sec: f64,
    gets_total: u64,
    get_bytes_total: u64,
    segment_scans_total: u64,
    empty_segment_scans_total: u64,
    elapsed_ms: f64,
}

/// Assemble the console summary from the measure-window tallies.
fn build_summary(inp: SummaryInputs<'_>) -> Summary {
    let s = inp.state;
    let elapsed = inp.elapsed_secs;

    let polls_total = s.polls_completed.load(Ordering::Relaxed);
    let records_consumed = s.records_consumed.load(Ordering::Relaxed);
    let bytes_consumed = s.bytes_consumed.load(Ordering::Relaxed);

    let per = |num: u64, den: u64| {
        if den > 0 {
            num as f64 / den as f64
        } else {
            0.0
        }
    };
    let segment_scans = inp.segment_scans_total;

    let mut summary = Summary::new()
        // Corpus / configuration echo.
        .add("active_sessions", inp.active_sessions as f64)
        .add(
            "session_duration_ms",
            inp.session_duration.map_or(-1.0, |d| d.as_millis() as f64),
        )
        .add("prefill_per_key", inp.effective_per_key as f64)
        .add("prefill_rounds", inp.rounds as f64)
        .add("prefill_records_per_sec", inp.prefill_records_per_sec)
        .add("prefill_bytes_per_sec", inp.prefill_bytes_per_sec)
        // Workload realization.
        .add(
            "distinct_keys_touched",
            s.distinct_keys_touched.load(Ordering::Relaxed) as f64,
        )
        .add(
            "sessions_completed",
            s.sessions_completed.load(Ordering::Relaxed) as f64,
        )
        // Read capacity.
        .add("total_polls", polls_total as f64)
        .add("polls_per_sec", polls_total as f64 / elapsed)
        .add(
            "aggregate_goodput_records_per_sec",
            records_consumed as f64 / elapsed,
        )
        .add(
            "aggregate_goodput_bytes_per_sec",
            bytes_consumed as f64 / elapsed,
        )
        // Object-store cost.
        .add("object_store_gets", inp.gets_total as f64)
        .add("gets_per_poll", per(inp.gets_total, polls_total))
        .add("get_bytes_total", inp.get_bytes_total as f64)
        // Read-amplification breakdown (segment-major entry key).
        .add("segment_scans", segment_scans as f64)
        .add("segments_per_poll", per(segment_scans, polls_total))
        .add(
            "empty_segment_scan_frac",
            per(inp.empty_segment_scans_total, segment_scans),
        )
        .add(
            "records_per_segment_scan",
            per(records_consumed, segment_scans),
        )
        .add("gets_per_segment_scan", per(inp.gets_total, segment_scans))
        // Object-store bytes fetched per useful byte delivered (1.0 = perfect locality).
        .add(
            "read_amplification",
            per(inp.get_bytes_total, bytes_consumed),
        )
        .add("residual_backlog_records", s.lag.total_lag() as f64)
        .add("elapsed_ms", inp.elapsed_ms);

    // Per-poll lag distribution (how far behind reads were at poll time).
    for (b, label) in LAG_BUCKET_LABELS.iter().enumerate() {
        let count = s.bucket_polls[b].load(Ordering::Relaxed);
        summary = summary.add(format!("polls_lag_{label}"), count as f64);
    }

    // Per-poll service latency percentiles, bucketed by lag at poll time (the read-
    // cost axis). Only buckets that saw polls are emitted.
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

    summary
}

/// Whether `storage` uses a shared object store (Local/Aws) that a standalone
/// [`LogDbReader`] can observe. An in-memory object store is per-handle, so the
/// reader would see none of the writer's data.
fn object_store_is_shared(storage: &StorageConfig) -> bool {
    match storage {
        StorageConfig::InMemory => false,
        StorageConfig::SlateDb(c) => !matches!(c.object_store, ObjectStoreConfig::InMemory),
    }
}
