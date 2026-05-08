//! Phase 1.3 baseline: Rust Buffer consumer fetch + decode.
//!
//! Drives `Consumer::next_batch` through a pre-populated in-memory
//! object store and emits schema-v2 benchmark artifacts under the
//! configured output directory:
//!
//! - `metadata.json`
//! - `results.json`
//! - `raw/run-N.metrics.jsonl`
//!
//! See `plans/odb-high-throughput/benchmarks.md` for the schema.
//!
//! The bench is single-threaded by design: this is the *current*
//! Buffer consumer API (RFC 0003 not yet implemented), and
//! `Consumer::next_batch` is `&mut self`. Phase 6 of the impl plan
//! will produce a parallel-fetch counterpart that uses
//! `ConsumerFetchHandle::fetch`.

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use bytes::Bytes;
use clap::Parser;
use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};
use serde::Serialize;
use serde_json::json;
use slatedb::object_store::ObjectStore;
use slatedb::object_store::memory::InMemory;

use buffer::{CompressionType, Consumer, ConsumerConfig, Producer, ProducerConfig};
use common::ObjectStoreConfig;
use common::clock::SystemClock;

#[derive(Parser, Debug)]
#[command(name = "buffer-bench-consumer-fetch-decode")]
struct Args {
    /// Number of records per batch (entries in one Buffer batch).
    #[arg(long, default_value_t = 1000)]
    records_per_batch: usize,

    /// Number of batches the workload generator writes.
    #[arg(long, default_value_t = 100)]
    batches: usize,

    /// Approximate bytes per record (deterministic synthetic payload).
    #[arg(long, default_value_t = 320)]
    record_bytes: usize,

    /// Iterations to run for statistical aggregation.
    #[arg(long, default_value_t = 5)]
    iterations: usize,

    /// PRNG seed for deterministic payload generation.
    #[arg(long, default_value_t = 42)]
    seed: u64,

    /// Output directory where metadata.json / results.json / raw/ land.
    #[arg(long)]
    output_dir: PathBuf,

    /// Phase id from the impl plan (default 1.3).
    #[arg(long, default_value = "1.3")]
    unit_id: String,

    /// Free-form change-slug used in the run dir name.
    #[arg(long, default_value = "baseline")]
    change_slug: String,

    /// Optional notes for metadata.json.
    #[arg(long, default_value = "")]
    notes: String,
}

// =============================================================================
// Metrics recorder: captures buffer.* histogram observations into a Vec<f64>.
// =============================================================================

#[derive(Default, Clone)]
struct CapturedMetrics {
    histograms: Arc<Mutex<HashMap<String, Vec<f64>>>>,
    counters: Arc<Mutex<HashMap<String, u64>>>,
}

impl CapturedMetrics {
    fn reset(&self) {
        self.histograms.lock().unwrap().clear();
        self.counters.lock().unwrap().clear();
    }

    fn drain_histograms(&self) -> HashMap<String, Vec<f64>> {
        std::mem::take(&mut *self.histograms.lock().unwrap())
    }

    fn drain_counters(&self) -> HashMap<String, u64> {
        std::mem::take(&mut *self.counters.lock().unwrap())
    }
}

struct CapturingRecorder {
    captured: CapturedMetrics,
}

impl Recorder for CapturingRecorder {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> Counter {
        let name = key.name().to_string();
        let counters = Arc::clone(&self.captured.counters);
        Counter::from_arc(Arc::new(CounterHandle { name, counters }))
    }

    fn register_gauge(&self, _: &Key, _: &Metadata<'_>) -> Gauge {
        Gauge::noop()
    }

    fn register_histogram(&self, key: &Key, _: &Metadata<'_>) -> Histogram {
        let name = key.name().to_string();
        let histograms = Arc::clone(&self.captured.histograms);
        Histogram::from_arc(Arc::new(HistogramHandle { name, histograms }))
    }
}

struct CounterHandle {
    name: String,
    counters: Arc<Mutex<HashMap<String, u64>>>,
}

impl metrics::CounterFn for CounterHandle {
    fn increment(&self, value: u64) {
        let mut g = self.counters.lock().unwrap();
        *g.entry(self.name.clone()).or_insert(0) += value;
    }
    fn absolute(&self, value: u64) {
        self.counters.lock().unwrap().insert(self.name.clone(), value);
    }
}

struct HistogramHandle {
    name: String,
    histograms: Arc<Mutex<HashMap<String, Vec<f64>>>>,
}

impl metrics::HistogramFn for HistogramHandle {
    fn record(&self, value: f64) {
        self.histograms
            .lock()
            .unwrap()
            .entry(self.name.clone())
            .or_default()
            .push(value);
    }
}

// =============================================================================
// Workload synthesis (deterministic from --seed).
// =============================================================================

/// Lightweight LCG so we don't pull in a PRNG crate.
fn lcg_next(state: &mut u64) -> u64 {
    *state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    *state
}

fn synth_record(state: &mut u64, target_bytes: usize) -> Bytes {
    // Deterministic byte pattern; not realistic but reproducible.
    let mut buf = Vec::with_capacity(target_bytes);
    while buf.len() < target_bytes {
        let v = lcg_next(state);
        buf.extend_from_slice(&v.to_le_bytes());
    }
    buf.truncate(target_bytes);
    Bytes::from(buf)
}

fn synth_batch(seed: u64, batch_idx: usize, records_per_batch: usize, record_bytes: usize) -> Vec<Bytes> {
    let mut state = seed
        .wrapping_add(0x9E37_79B9_7F4A_7C15)
        .wrapping_mul(batch_idx as u64 + 1);
    (0..records_per_batch)
        .map(|_| synth_record(&mut state, record_bytes))
        .collect()
}

// =============================================================================
// Per-iteration timing.
// =============================================================================

#[derive(Debug, Clone, Serialize)]
struct IterationResult {
    iteration: usize,
    started_unix_ms: u64,
    elapsed_seconds: f64,
    records_processed: usize,
    bytes_processed: u64,
    cpu_user_seconds: f64,
    cpu_sys_seconds: f64,
    next_batch_seconds: Vec<f64>,
    fetch_duration_seconds: Vec<f64>,
    counters: HashMap<String, u64>,
}

// Self-process CPU usage. RUSAGE_SELF on macOS/Linux returns user + sys
// CPU time for the calling process (no thread granularity on macOS).
fn rusage_self() -> (f64, f64) {
    unsafe {
        let mut ru: libc::rusage = std::mem::zeroed();
        if libc::getrusage(libc::RUSAGE_SELF, &mut ru) != 0 {
            return (0.0, 0.0);
        }
        let u = ru.ru_utime.tv_sec as f64 + ru.ru_utime.tv_usec as f64 * 1e-6;
        let s = ru.ru_stime.tv_sec as f64 + ru.ru_stime.tv_usec as f64 * 1e-6;
        (u, s)
    }
}

async fn populate_store(
    store: Arc<dyn ObjectStore>,
    manifest_path: &str,
    data_prefix: &str,
    args: &Args,
) -> Result<u64> {
    // Use the high-level Producer with a tiny flush threshold so each
    // produce() call results in exactly one batch on disk. This keeps
    // the consumer-side test deterministic (one Buffer batch =
    // records_per_batch records).
    let producer_config = ProducerConfig {
        object_store: ObjectStoreConfig::InMemory,
        data_path_prefix: data_prefix.to_string(),
        manifest_path: manifest_path.to_string(),
        // Force per-call flush; flush_interval is the upper-bound timer
        // and flush_size_bytes triggers on size. Setting size to 1
        // forces flush after every produce().
        flush_interval: std::time::Duration::from_millis(50),
        flush_size_bytes: 1,
        max_buffered_inputs: 1024,
        batch_compression: CompressionType::None,
    };
    let producer = Producer::with_object_store(
        producer_config,
        store.clone(),
        Arc::new(SystemClock) as Arc<dyn common::clock::Clock>,
    )
    .context("Producer::with_object_store")?;

    let mut handles = Vec::with_capacity(args.batches);
    let mut total_bytes_written = 0u64;
    for batch_idx in 0..args.batches {
        let entries = synth_batch(args.seed, batch_idx, args.records_per_batch, args.record_bytes);
        total_bytes_written += entries.iter().map(|e| e.len() as u64).sum::<u64>();
        let handle = producer
            .produce(entries, Bytes::new())
            .await
            .context("producer produce")?;
        handles.push(handle);
    }
    producer.flush().await.context("producer flush")?;
    for mut h in handles {
        h.watcher.await_durable().await.context("await_durable")?;
    }
    producer.close().await.context("producer close")?;
    Ok(total_bytes_written)
}

async fn run_iteration(
    iteration: usize,
    captured: CapturedMetrics,
    args: &Args,
) -> Result<IterationResult> {
    captured.reset();

    // Each iteration uses a fresh in-memory object store + manifest path
    // so consumer state is reset cleanly. Workload is deterministic given
    // (seed, records_per_batch, batches, record_bytes).
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let manifest_path = format!("bench-1.3/iter-{iteration}/manifest");
    let data_prefix = format!("bench-1.3/iter-{iteration}/data");

    let bytes_in_store = populate_store(store.clone(), &manifest_path, &data_prefix, args).await?;

    let consumer_config = ConsumerConfig {
        object_store: ObjectStoreConfig::InMemory,
        manifest_path: manifest_path.clone(),
        data_path_prefix: data_prefix,
        gc_interval: std::time::Duration::from_secs(60 * 60),
        gc_grace_period: std::time::Duration::from_secs(60 * 60),
    };
    let mut consumer =
        Consumer::with_object_store(consumer_config, store.clone(), None).await?;

    let started_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let timed_start = Instant::now();
    let (u0, s0) = rusage_self();
    let mut next_batch_seconds = Vec::with_capacity(args.batches);
    let mut records_processed = 0usize;
    let mut bytes_processed = 0u64;

    for _ in 0..args.batches {
        let t0 = Instant::now();
        let batch = consumer.next_batch().await?.context("expected batch")?;
        next_batch_seconds.push(t0.elapsed().as_secs_f64());
        records_processed += batch.entries.len();
        bytes_processed += batch.entries.iter().map(|e| e.len() as u64).sum::<u64>();
        consumer.ack(batch.sequence).await?;
    }

    let elapsed = timed_start.elapsed().as_secs_f64();
    let (u1, s1) = rusage_self();
    consumer.flush().await?;

    let _ = bytes_in_store;

    let mut histograms = captured.drain_histograms();
    let counters = captured.drain_counters();
    let fetch_duration_seconds = histograms
        .remove("buffer.fetch_duration_seconds")
        .unwrap_or_default();

    Ok(IterationResult {
        iteration,
        started_unix_ms,
        elapsed_seconds: elapsed,
        records_processed,
        bytes_processed,
        cpu_user_seconds: (u1 - u0).max(0.0),
        cpu_sys_seconds: (s1 - s0).max(0.0),
        next_batch_seconds,
        fetch_duration_seconds,
        counters,
    })
}

// =============================================================================
// Aggregation + schema-v2 emission.
// =============================================================================

#[derive(Serialize)]
struct ScalarAggregate {
    median: f64,
    p10: f64,
    p90: f64,
    n: usize,
}

fn aggregate(samples: &[f64]) -> ScalarAggregate {
    if samples.is_empty() {
        return ScalarAggregate { median: 0.0, p10: 0.0, p90: 0.0, n: 0 };
    }
    let mut s: Vec<f64> = samples.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let q = |frac: f64| -> f64 {
        let idx = ((s.len() as f64 - 1.0) * frac) as usize;
        s[idx]
    };
    ScalarAggregate { median: q(0.5), p10: q(0.1), p90: q(0.9), n: s.len() }
}

fn ts_now_iso() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let (y, mo, d, h, mi, se) = epoch_to_parts(secs as i64);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{se:02}Z")
}

fn epoch_to_parts(secs: i64) -> (i64, u32, u32, u32, u32, u32) {
    let day_seconds = 86_400i64;
    let mut days = secs.div_euclid(day_seconds);
    let mut sec_of_day = secs.rem_euclid(day_seconds);
    let h = (sec_of_day / 3600) as u32;
    sec_of_day %= 3600;
    let mi = (sec_of_day / 60) as u32;
    let se = (sec_of_day % 60) as u32;
    let mut y: i64 = 1970;
    let is_leap = |y: i64| (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    loop {
        let dy = if is_leap(y) { 366 } else { 365 };
        if days >= dy { days -= dy; y += 1; } else { break; }
    }
    let dim = |y: i64, m: u32| -> u32 {
        match m {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            2 => if is_leap(y) { 29 } else { 28 },
            _ => 0,
        }
    };
    let mut m: u32 = 1;
    loop {
        let d = dim(y, m) as i64;
        if days >= d { days -= d; m += 1; } else { break; }
    }
    (y, m, (days + 1) as u32, h, mi, se)
}

fn run_id_slug(change_slug: &str) -> String {
    // YYYY-MM-DDTHHmm-<slug>
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let (y, mo, d, h, mi, _) = epoch_to_parts(secs);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}{mi:02}-{change_slug}")
}

fn git_info(repo: &str) -> serde_json::Value {
    let rev = std::process::Command::new("git")
        .args(["-C", repo, "rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim()[..7.min(s.trim().len())].to_string())
        .unwrap_or_default();
    let branch = std::process::Command::new("git")
        .args(["-C", repo, "rev-parse", "--abbrev-ref", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_default();
    let dirty = std::process::Command::new("git")
        .args(["-C", repo, "status", "--porcelain"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| !o.stdout.is_empty())
        .unwrap_or(false);
    json!({ "rev": rev, "branch": branch, "dirty": dirty })
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();

    let captured = CapturedMetrics::default();
    let recorder = CapturingRecorder { captured: captured.clone() };
    metrics::set_global_recorder(recorder)
        .map_err(|e| anyhow::anyhow!("metrics::set_global_recorder: {e}"))?;

    // Run-dir setup.
    let run_dir = args.output_dir.join(run_id_slug(&args.change_slug));
    let raw_dir = run_dir.join("raw");
    std::fs::create_dir_all(&raw_dir).context("mkdir -p run_dir/raw")?;

    let started_iso = ts_now_iso();

    let mut iterations: Vec<IterationResult> = Vec::with_capacity(args.iterations);
    for iter in 1..=args.iterations {
        eprintln!(
            "iteration {iter}/{}: {} batches × {} records/batch × {} bytes/record",
            args.iterations, args.batches, args.records_per_batch, args.record_bytes
        );
        let r = run_iteration(iter, captured.clone(), &args).await?;
        // Per-iteration JSONL metric stream (one line per next_batch call).
        let raw_path = raw_dir.join(format!("run-{iter}.metrics.jsonl"));
        let mut buf = String::new();
        for (i, secs) in r.next_batch_seconds.iter().enumerate() {
            let line = json!({
                "ts_unix_ms": r.started_unix_ms + (i as u64),
                "metric": "stage.next_batch.duration_seconds",
                "value": secs,
                "labels": { "iteration": iter, "batch_index": i }
            });
            buf.push_str(&serde_json::to_string(&line)?);
            buf.push('\n');
        }
        for (i, secs) in r.fetch_duration_seconds.iter().enumerate() {
            let line = json!({
                "ts_unix_ms": r.started_unix_ms + (i as u64),
                "metric": "buffer.fetch_duration_seconds",
                "value": secs,
                "labels": { "iteration": iter, "obs_index": i }
            });
            buf.push_str(&serde_json::to_string(&line)?);
            buf.push('\n');
        }
        std::fs::write(&raw_path, buf)
            .with_context(|| format!("write {raw_path:?}"))?;
        iterations.push(r);
    }

    let ended_iso = ts_now_iso();

    // Aggregate scalars and stages.
    let total_records: Vec<f64> = iterations
        .iter()
        .map(|r| r.records_processed as f64)
        .collect();
    let throughput_records_per_sec: Vec<f64> = iterations
        .iter()
        .map(|r| r.records_processed as f64 / r.elapsed_seconds.max(1e-9))
        .collect();
    let throughput_bytes_per_sec: Vec<f64> = iterations
        .iter()
        .map(|r| r.bytes_processed as f64 / r.elapsed_seconds.max(1e-9))
        .collect();

    // Per-batch latency stats: aggregate medians across iterations.
    let per_iter_p99_next_batch_ms: Vec<f64> = iterations
        .iter()
        .map(|r| {
            let mut s = r.next_batch_seconds.clone();
            s.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let idx = ((s.len() as f64 - 1.0) * 0.99) as usize;
            s[idx] * 1000.0
        })
        .collect();
    let per_iter_median_next_batch_ms: Vec<f64> = iterations
        .iter()
        .map(|r| {
            let mut s = r.next_batch_seconds.clone();
            s.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let idx = s.len() / 2;
            s[idx] * 1000.0
        })
        .collect();

    // Stage attribution: fetch_duration_seconds is observed inside the
    // buffer crate (object_get + decode_batch combined). Manifest read
    // cost is approximated as next_batch_total - fetch_duration.
    let per_iter_fetch_median_ms: Vec<f64> = iterations
        .iter()
        .map(|r| {
            if r.fetch_duration_seconds.is_empty() {
                return 0.0;
            }
            let mut s = r.fetch_duration_seconds.clone();
            s.sort_by(|a, b| a.partial_cmp(b).unwrap());
            s[s.len() / 2] * 1000.0
        })
        .collect();
    let per_iter_manifest_median_ms: Vec<f64> = iterations
        .iter()
        .map(|r| {
            let mut next: Vec<f64> = r.next_batch_seconds.clone();
            next.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let mut fetch: Vec<f64> = r.fetch_duration_seconds.clone();
            fetch.sort_by(|a, b| a.partial_cmp(b).unwrap());
            if next.is_empty() || fetch.is_empty() {
                return 0.0;
            }
            let nb_med = next[next.len() / 2];
            let f_med = fetch[fetch.len() / 2];
            (nb_med - f_med).max(0.0) * 1000.0
        })
        .collect();

    let ops_per_sec_next_batch: Vec<f64> = iterations
        .iter()
        .map(|r| args.batches as f64 / r.elapsed_seconds.max(1e-9))
        .collect();

    let cpu_user_per_iter: Vec<f64> = iterations.iter().map(|r| r.cpu_user_seconds).collect();
    let cpu_sys_per_iter:  Vec<f64> = iterations.iter().map(|r| r.cpu_sys_seconds).collect();
    let scalars = json!({
        "total_throughput_records_per_sec": agg_value(&throughput_records_per_sec),
        "total_throughput_bytes_per_sec":   agg_value(&throughput_bytes_per_sec),
        "p99_next_batch_latency_ms":        agg_value(&per_iter_p99_next_batch_ms),
        "median_next_batch_latency_ms":     agg_value(&per_iter_median_next_batch_ms),
        "iteration_records_processed":      agg_value(&total_records),
        "iteration_elapsed_seconds":        agg_value(
            &iterations.iter().map(|r| r.elapsed_seconds).collect::<Vec<_>>()
        ),
        "cpu_user_seconds":                 agg_value(&cpu_user_per_iter),
        "cpu_sys_seconds":                  agg_value(&cpu_sys_per_iter),
    });

    let stages = json!([
        {
            "name": "manifest_read",
            "median_ms_per_op": median(&per_iter_manifest_median_ms),
            "p10":              percentile(&per_iter_manifest_median_ms, 0.1),
            "p90":              percentile(&per_iter_manifest_median_ms, 0.9),
            "ops_per_sec":      median(&ops_per_sec_next_batch),
            "notes": "Approximated as next_batch_total - buffer.fetch_duration_seconds; the current Buffer API does not expose manifest_read as a separate timing point. Phase 2 + Phase 6 will produce a finer breakdown."
        },
        {
            "name": "fetch_object_decode",
            "median_ms_per_op": median(&per_iter_fetch_median_ms),
            "p10":              percentile(&per_iter_fetch_median_ms, 0.1),
            "p90":              percentile(&per_iter_fetch_median_ms, 0.9),
            "ops_per_sec":      median(&ops_per_sec_next_batch),
            "notes": "Observed via the buffer.fetch_duration_seconds histogram. Combines object_store.get + decode_batch; the current API does not separate them."
        },
        {
            "name": "next_batch_total",
            "median_ms_per_op": median(&per_iter_median_next_batch_ms),
            "p10":              percentile(&per_iter_median_next_batch_ms, 0.1),
            "p90":              percentile(&per_iter_median_next_batch_ms, 0.9),
            "ops_per_sec":      median(&ops_per_sec_next_batch),
            "notes": "End-to-end Consumer::next_batch latency."
        }
    ]);

    let per_iteration_json: Vec<serde_json::Value> = iterations
        .iter()
        .enumerate()
        .map(|(idx, r)| {
            json!({
                "iteration": r.iteration,
                "scalars": {
                    "iteration_elapsed_seconds": r.elapsed_seconds,
                    "iteration_throughput_records_per_sec": throughput_records_per_sec[idx],
                    "iteration_throughput_bytes_per_sec":   throughput_bytes_per_sec[idx],
                    "iteration_records_processed":          r.records_processed as f64,
                },
                "stages": [
                    { "name": "manifest_read",       "median_ms_per_op": per_iter_manifest_median_ms[idx] },
                    { "name": "fetch_object_decode", "median_ms_per_op": per_iter_fetch_median_ms[idx]    },
                    { "name": "next_batch_total",    "median_ms_per_op": per_iter_median_next_batch_ms[idx] },
                ],
                "histograms": {
                    "next_batch_seconds": {
                        "buckets": [0.0001, 0.001, 0.01, 0.05, 0.1, 0.5, 1.0],
                        "counts": bucket_counts(
                            &r.next_batch_seconds,
                            &[0.0001, 0.001, 0.01, 0.05, 0.1, 0.5, 1.0]
                        )
                    }
                },
                "raw_log":     format!("raw/run-{}.metrics.jsonl", r.iteration),
                "raw_metrics": format!("raw/run-{}.metrics.jsonl", r.iteration),
            })
        })
        .collect();

    let results = json!({
        "schema_version": 2,
        "scalars": scalars,
        "stages": stages,
        "histograms": {
            "next_batch_seconds_aggregate": {
                "buckets": [0.0001, 0.001, 0.01, 0.05, 0.1, 0.5, 1.0],
                "counts":  bucket_counts(
                    &iterations.iter().flat_map(|r| r.next_batch_seconds.clone()).collect::<Vec<_>>(),
                    &[0.0001, 0.001, 0.01, 0.05, 0.1, 0.5, 1.0]
                )
            }
        },
        "iterations": per_iteration_json,
    });

    std::fs::write(run_dir.join("results.json"), serde_json::to_string_pretty(&results)?)?;

    // timeseries.json — schema v2 required for unit 1.3.
    //
    // Each iteration produces one observation per Buffer batch (in
    // sequence order). Cross-iteration aggregation is by sample index
    // (the i-th batch across iterations), with t_offset_ms approximated
    // as i × median_next_batch_latency. The bench is deterministic
    // and fast enough that per-batch wall-clock offsets are noisy at
    // sub-microsecond scales; this offset is the best honest signal.
    let series_next_batch = build_aggregated_series(
        "stage.next_batch.duration_seconds",
        &iterations.iter().map(|r| r.next_batch_seconds.clone()).collect::<Vec<_>>(),
    );
    let series_fetch = build_aggregated_series(
        "buffer.fetch_duration_seconds",
        &iterations.iter().map(|r| r.fetch_duration_seconds.clone()).collect::<Vec<_>>(),
    );
    let iteration_starts: Vec<u64> = iterations.iter().map(|r| r.started_unix_ms).collect();
    let timeseries = json!({
        "schema_version": 2,
        "window_seconds": 1.0,
        "iterations_aggregated": iterations.len(),
        "iteration_starts_unix_ms": iteration_starts,
        "series": [series_next_batch, series_fetch],
    });
    std::fs::write(run_dir.join("timeseries.json"), serde_json::to_string_pretty(&timeseries)?)?;

    // Build metadata.json
    let workload_canonical = json!({
        "kind": "buffer-synthetic-bytes",
        "records_per_batch": args.records_per_batch,
        "batches": args.batches,
        "record_bytes": args.record_bytes,
        "compression": "none",
        "schema": "buffer-bytes-v1",
        "generator": "buffer-bench-consumer-fetch-decode",
    });
    let canonical_path = raw_dir.join("workload.canonical.json");
    std::fs::write(
        &canonical_path,
        serde_json::to_string_pretty(&workload_canonical)?,
    )?;
    let workload_canonical_str = serde_json::to_string(&workload_canonical)?;
    let fingerprint_hash = sha256_hex(workload_canonical_str.as_bytes());
    let schema_hash = sha256_hex(b"buffer-bytes-v1");

    let host = json!({
        "machine":            std::env::var("HOSTNAME").unwrap_or_default(),
        "os":                 std::env::consts::OS.to_string() + " " + std::env::consts::ARCH,
        "cpu_model":          std::env::var("CPU_MODEL").unwrap_or_else(|_| "unknown".into()),
        "cpu_count_logical":  num_cpus_or_default(),
        "memory_gb":          std::env::var("MEMORY_GB")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .map(|n| n as i64)
            .unwrap_or(-1),
        "container":          serde_json::Value::Null,
        "low_perturbation":   true,
    });

    let opendata_repo = std::env::var("OPENDATA_REPO_PATH").unwrap_or_else(|_| ".".into());
    let metadata = json!({
        "schema_version": 2,
        "phase": "phase01-baseline",
        "unit_id": args.unit_id,
        "unit_title": "Benchmark current Rust Buffer consumer fetch + decode",
        "owner": "Benchmark/Perf Implementor",
        "started_at": started_iso,
        "ended_at":   ended_iso,
        "experiment": {
            "kind": "ab",
            "dimensions": [],
            "fixed_controls": {
                "records_per_batch": args.records_per_batch,
                "batches":           args.batches,
                "record_bytes":      args.record_bytes,
            },
            "matrix_file": serde_json::Value::Null
        },
        "varied_param": serde_json::Value::Null,
        "git": {
            "opendata":         git_info(&opendata_repo),
            "opendata-go":      serde_json::Value::Null,
            "opendata-contrib": serde_json::Value::Null,
        },
        "host": host,
        "binary": {
            "name": "buffer-bench-consumer-fetch-decode",
            "build_command": "cargo build --release -p buffer-bench --bin buffer-bench-consumer-fetch-decode",
            "build_rev": "see git.opendata.rev",
            "build_flags": "release; jemalloc"
        },
        "config": {
            "runtime_yaml_path": serde_json::Value::Null,
            "effective_yaml_hash": "n/a",
            "effective_yaml": {
                "object_store": "InMemory",
                "decode_compression": "none"
            }
        },
        "services": {
            "clickhouse":   serde_json::Value::Null,
            "object_store": { "kind": "in-memory", "endpoint": null, "region": null, "bucket": null, "container_image": null },
            "iceberg":      serde_json::Value::Null
        },
        "workload": {
            "generator":           "buffer-bench-consumer-fetch-decode",
            "generator_rev":       "see git.opendata.rev",
            "seed":                args.seed,
            "schema":              "buffer-bytes-v1",
            "schema_hash":         format!("sha256:{schema_hash}"),
            "fingerprint":         format!(
                "buffer-{}rec-per-batch-{}batches-{}bytes-no-compression",
                args.records_per_batch, args.batches, args.record_bytes
            ),
            "fingerprint_hash":    format!("sha256:{fingerprint_hash}"),
            "canonical_path":      "raw/workload.canonical.json",
            "records_total":       (args.records_per_batch * args.batches) as i64,
            "batches_total":       args.batches as i64,
            "records_per_batch":   args.records_per_batch as i64,
            "approx_bytes_per_record": args.record_bytes as i64,
            "encoding":            "raw-bytes",
            "compression":         "none",
            "attribute_cardinality": serde_json::Value::Null
        },
        "iterations": args.iterations,
        "baseline_run": serde_json::Value::Null,
        "notes": args.notes,
    });

    std::fs::write(
        run_dir.join("metadata.json"),
        serde_json::to_string_pretty(&metadata)?,
    )?;

    println!("Wrote artifacts to {}", run_dir.display());
    Ok(())
}

fn agg_value(samples: &[f64]) -> serde_json::Value {
    let a = aggregate(samples);
    json!({ "median": a.median, "p10": a.p10, "p90": a.p90, "n": a.n })
}

fn median(samples: &[f64]) -> f64 {
    if samples.is_empty() { return 0.0; }
    let mut s = samples.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    s[s.len() / 2]
}

fn percentile(samples: &[f64], frac: f64) -> f64 {
    if samples.is_empty() { return 0.0; }
    let mut s = samples.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = ((s.len() as f64 - 1.0) * frac) as usize;
    s[idx]
}

/// Aggregate per-iteration series into a schema-v2 timeseries entry,
/// aligned on sample index (one observation per Buffer batch). See
/// `plans/odb-high-throughput/benchmarks.md` rev 5: each sample
/// carries `sample_index` (not `t_offset_ms`) and the series envelope
/// declares `labels.alignment = "sample_index"`. Sample-index
/// alignment is the right axis when individual observations are
/// sub-millisecond — wall-clock offsets would round to integer-ms
/// noise.
fn build_aggregated_series(metric: &str, per_iter: &[Vec<f64>]) -> serde_json::Value {
    let max_len = per_iter.iter().map(|v| v.len()).max().unwrap_or(0);
    let mut samples: Vec<serde_json::Value> = Vec::with_capacity(max_len);
    for j in 0..max_len {
        let mut values: Vec<f64> = Vec::with_capacity(per_iter.len());
        for it in per_iter {
            if let Some(v) = it.get(j).copied() {
                values.push(v);
            }
        }
        if values.is_empty() {
            continue;
        }
        values.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let med = values[values.len() / 2];
        let p90_idx = ((values.len() as f64 - 1.0) * 0.9) as usize;
        let p90 = values[p90_idx];
        let max = *values.last().unwrap();
        samples.push(json!({
            "sample_index": j as i64,
            "median": med,
            "p90":    p90,
            "max":    max,
            "n":      values.len(),
        }));
    }
    json!({
        "metric": metric,
        "labels": { "alignment": "sample_index" },
        "samples": samples,
    })
}

fn bucket_counts(samples: &[f64], buckets: &[f64]) -> Vec<u64> {
    let mut counts = vec![0u64; buckets.len()];
    for v in samples {
        for (i, b) in buckets.iter().enumerate() {
            if *v < *b {
                counts[i] += 1;
                break;
            }
        }
    }
    counts
}

fn num_cpus_or_default() -> i64 {
    std::thread::available_parallelism()
        .map(|n| n.get() as i64)
        .unwrap_or(1)
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::Digest;
    let mut h = sha2::Sha256::new();
    h.update(bytes);
    let out = h.finalize();
    let mut s = String::with_capacity(64);
    for b in out {
        use std::fmt::Write;
        let _ = write!(&mut s, "{b:02x}");
    }
    s
}
