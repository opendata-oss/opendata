//! Ingest throughput benchmark for the log database.
//!
//! The benchmark sweeps `num_keys` (key cardinality) and asks whether write
//! throughput holds as the key population grows. It runs in one of two modes,
//! selected by `target_mb_per_sec`:
//!
//! - **Closed-loop** (`target_mb_per_sec` absent or `<= 0`): writers append as
//!   fast as the database accepts, measuring the throughput *ceiling* — it
//!   answers "does the ceiling move with cardinality?"
//! - **Open-loop** (`target_mb_per_sec > 0`): writers pace appends against a
//!   fixed offered write rate, so the offered load is what we configured rather
//!   than whatever the database happened to accept. For the cardinality sweep the
//!   headline question is whether that fixed rate is *sustained* as `num_keys`
//!   grows — i.e. achieved throughput stays `~= target` and the schedule lag
//!   (how far behind the open-loop schedule a batch fires) stays bounded.
//!
//! Either mode runs across `num_writer_tasks` concurrent appenders. Each task
//! owns a strided slice of the key space (`id % num_writer_tasks == task_idx`)
//! and round-robins through it, so the whole key space is exercised and tasks
//! never write the same key.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bencher::{Bench, Benchmark, Counter, Histogram, Params, Summary};
use bytes::Bytes;
use log::{AppendError, Config, LogDb, LogDbBuilder, Record, SstBlockSize};
use tokio::sync::Notify;
use tokio::time::Instant;

use crate::workload;

const MICROS_PER_SEC: f64 = 1_000_000.0;
/// Bytes per "MB" for the throughput target — decimal megabytes, matching how
/// the achieved `throughput_bytes` is reported (raw bytes / elapsed seconds).
const BYTES_PER_MB: f64 = 1_000_000.0;

/// Create a parameter set for the ingest benchmark.
fn make_params(batch_size: usize, value_size: usize, key_length: usize, num_keys: usize) -> Params {
    let mut params = Params::new();
    params.insert("batch_size", batch_size.to_string());
    params.insert("value_size", value_size.to_string());
    params.insert("key_length", key_length.to_string());
    params.insert("num_keys", num_keys.to_string());
    params
}

/// Per-task tallies, folded together after all writers finish.
#[derive(Default)]
struct WriterStats {
    /// Records accepted by the database (excludes `QueueFull` rejections).
    records: u64,
    /// Bytes accepted (records × record_size).
    bytes: u64,
    /// Batches that returned `QueueFull` (offered but rejected back-pressure).
    queue_full: u64,
    /// Per-batch append service time, microseconds: running sum, max, and count.
    latency_sum_us: f64,
    latency_max_us: f64,
    latency_count: u64,
    /// Open-loop schedule lag (how late a batch fired vs its slot), microseconds.
    /// Zero in closed-loop mode (no schedule).
    lag_sum_us: f64,
    lag_max_us: f64,
    lag_count: u64,
}

impl WriterStats {
    fn merge(&mut self, other: WriterStats) {
        self.records += other.records;
        self.bytes += other.bytes;
        self.queue_full += other.queue_full;
        self.latency_sum_us += other.latency_sum_us;
        self.latency_max_us = self.latency_max_us.max(other.latency_max_us);
        self.latency_count += other.latency_count;
        self.lag_sum_us += other.lag_sum_us;
        self.lag_max_us = self.lag_max_us.max(other.lag_max_us);
        self.lag_count += other.lag_count;
    }
}

/// Handles to the live (continuously-updated) metric streams, cloned into each
/// writer task. These feed a configured `[reporter]`; the console summary is
/// computed separately from the folded [`WriterStats`].
#[derive(Clone)]
struct LiveMetrics {
    records: Counter,
    bytes: Counter,
    batch_latency: Histogram,
}

/// Run one writer task until signalled to stop.
///
/// `per_task_rate` is this task's share of the offered rate in records/sec; a
/// value `<= 0` means closed-loop (no pacing — append as fast as accepted).
#[allow(clippy::too_many_arguments)]
async fn run_writer(
    log: Arc<LogDb>,
    keys: Arc<Vec<Bytes>>,
    value: Bytes,
    record_size: usize,
    batch_size: usize,
    task_idx: usize,
    num_tasks: usize,
    per_task_rate: f64,
    pace_start: Instant,
    stop: Arc<AtomicBool>,
    stop_signal: Arc<Notify>,
    live: LiveMetrics,
) -> anyhow::Result<WriterStats> {
    let mut stats = WriterStats::default();

    // Keys this task owns: task_idx, task_idx + num_tasks, ... below num_keys.
    let n = keys.len();
    let owned = if task_idx < n {
        (n - task_idx).div_ceil(num_tasks)
    } else {
        0 // more tasks than keys: this one has nothing to write.
    };
    if owned == 0 {
        return Ok(stats);
    }

    let paced = per_task_rate > 0.0;
    let period = if paced {
        Duration::from_secs_f64(batch_size as f64 / per_task_rate)
    } else {
        Duration::ZERO
    };
    let mut next = pace_start;
    let mut cursor = 0usize;

    while !stop.load(Ordering::Relaxed) {
        // Open-loop: wait for this batch's scheduled slot. The schedule advances
        // on a fixed cadence regardless of how long appends take, so a database
        // that can't keep up shows up as growing schedule lag (no coordinated
        // omission) rather than as a silently lowered offered rate.
        if paced {
            tokio::select! {
                _ = tokio::time::sleep_until(next) => {}
                _ = stop_signal.notified() => break,
            }
        }

        // Build a batch, round-robining through this task's strided key slice.
        let records: Vec<Record> = (0..batch_size)
            .map(|_| {
                let id = task_idx + (cursor % owned) * num_tasks;
                cursor += 1;
                Record {
                    key: keys[id].clone(),
                    value: value.clone(),
                }
            })
            .collect();

        let batch_start = Instant::now();
        let outcome = log.try_append(records).await;
        let elapsed_us = batch_start.elapsed().as_secs_f64() * MICROS_PER_SEC;

        match outcome {
            Ok(_) => {
                live.records.increment(batch_size as u64);
                live.bytes.increment((batch_size * record_size) as u64);
                live.batch_latency.record(elapsed_us);
                stats.records += batch_size as u64;
                stats.bytes += (batch_size * record_size) as u64;
                stats.latency_sum_us += elapsed_us;
                stats.latency_max_us = stats.latency_max_us.max(elapsed_us);
                stats.latency_count += 1;
            }
            // Back-pressure: the offered batch was rejected because the write
            // queue is full. This IS the saturation signal (the database can't
            // accept the offered rate), so count it and carry on rather than
            // aborting the run; the batch is dropped.
            Err(AppendError::QueueFull(_)) => {
                stats.queue_full += 1;
            }
            // Any other append error is terminal.
            Err(e) => return Err(e.into()),
        }

        if paced {
            // Schedule lag: how far past its slot this batch actually fired.
            let lag_us = batch_start.saturating_duration_since(next).as_secs_f64() * MICROS_PER_SEC;
            stats.lag_sum_us += lag_us;
            stats.lag_max_us = stats.lag_max_us.max(lag_us);
            stats.lag_count += 1;
            next += period;
        }
    }

    Ok(stats)
}

/// Benchmark for log ingest throughput.
pub struct IngestBenchmark;

impl IngestBenchmark {
    pub fn new() -> Self {
        Self
    }
}

impl Default for IngestBenchmark {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl Benchmark for IngestBenchmark {
    fn name(&self) -> &str {
        "ingest"
    }

    fn default_params(&self) -> Vec<Params> {
        vec![
            // Vary batch size
            make_params(1, 256, 16, 10),
            make_params(10, 256, 16, 10),
            make_params(100, 256, 16, 10),
            // Vary value size
            make_params(100, 64, 16, 10),
            make_params(100, 512, 16, 10),
            make_params(100, 1024, 16, 10),
        ]
    }

    async fn run(&self, bench: Bench) -> anyhow::Result<()> {
        let params = bench.spec().params();
        let batch_size: usize = params.get_parse::<usize>("batch_size")?.max(1);
        let value_size: usize = params.get_parse("value_size")?;
        let key_length: usize = params.get_parse("key_length")?;
        let num_keys: usize = params.get_parse::<usize>("num_keys")?.max(1);

        // Offered write rate, MB/s. Absent or <= 0 → closed-loop saturation.
        let target_mb_per_sec: f64 = match params.get("target_mb_per_sec") {
            Some(v) => v.parse()?,
            None => 0.0,
        };
        // Concurrent appenders. Capped at num_keys so every task owns >= 1 key
        // (an idle task would otherwise forfeit its share of the offered rate).
        let num_writer_tasks = match params.get("num_writer_tasks") {
            Some(v) => v.parse::<usize>()?.max(1),
            None => 1,
        }
        .min(num_keys);
        // Compaction policy: when set, LogDb skips the one-shot consolidation of
        // sealed steady-state segments and keeps only L0 relief (RFC 0005). A
        // write-path lever worth sweeping alongside cardinality.
        let l0_only: bool = match params.get("l0_only") {
            Some(v) => v.parse()?,
            None => false,
        };
        // Automatic segment seal interval (ms). Absent ⇒ None: sealing disabled,
        // everything stays in segment 0. Time-based sealing controls how often a
        // new segment is cut, which shapes the write/compaction path.
        let seal_interval = match params.get("seal_interval_ms") {
            Some(v) => Some(Duration::from_millis(v.parse()?)),
            None => None,
        };
        // SlateDB SST block size, in bytes. A DbBuilder-time setting (not part of
        // SlateDB Settings), so it is applied via LogDbBuilder below. Must be on
        // SlateDB's ladder (1/2/4/8/16/32/64 KiB) — any other value is an error;
        // absent ⇒ SlateDB default (4 KiB).
        let block_size: Option<SstBlockSize> = match params.get("block_size_bytes") {
            Some(v) => Some(match v.parse::<usize>()? {
                1024 => SstBlockSize::Block1Kib,
                2048 => SstBlockSize::Block2Kib,
                4096 => SstBlockSize::Block4Kib,
                8192 => SstBlockSize::Block8Kib,
                16384 => SstBlockSize::Block16Kib,
                32768 => SstBlockSize::Block32Kib,
                65536 => SstBlockSize::Block64Kib,
                other => anyhow::bail!(
                    "unsupported block_size_bytes {other}; use one of \
                     1024, 2048, 4096, 8192, 16384, 32768, 65536"
                ),
            }),
            None => None,
        };

        let live = LiveMetrics {
            records: bench.counter("records"),
            bytes: bench.counter("bytes"),
            batch_latency: bench.histogram("batch_latency_us"),
        };

        // Initialize log with fresh storage from the bench's data config.
        let mut config = Config {
            storage: bench.spec().data().storage.clone(),
            ..Default::default()
        };
        config.compaction.l0_only = l0_only;
        config.segmentation.seal_interval = seal_interval;
        let log = Arc::new(
            LogDbBuilder::new(config)
                .with_sst_block_size(block_size)
                .build()
                .await?,
        );

        // Deterministic key space, shared (read-only) across all writer tasks.
        let keys = Arc::new(workload::keys(num_keys, key_length));
        let value = workload::value_template(value_size);
        let record_size = workload::record_size(key_length, value_size);

        // Convert the MB/s target into an aggregate records/sec rate, then split
        // it evenly across the writer tasks. `per_task_rate <= 0` ⇒ closed-loop.
        let target_bytes_per_sec = target_mb_per_sec * BYTES_PER_MB;
        let target_records_per_sec = target_bytes_per_sec / record_size as f64;
        let per_task_rate = if target_records_per_sec > 0.0 {
            target_records_per_sec / num_writer_tasks as f64
        } else {
            0.0
        };

        // Start the timed window, then launch the writers against it.
        let runner = bench.start();
        let pace_start = Instant::now();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_signal = Arc::new(Notify::new());

        let mut handles = Vec::with_capacity(num_writer_tasks);
        for task_idx in 0..num_writer_tasks {
            handles.push(tokio::spawn(run_writer(
                log.clone(),
                keys.clone(),
                value.clone(),
                record_size,
                batch_size,
                task_idx,
                num_writer_tasks,
                per_task_rate,
                pace_start,
                stop.clone(),
                stop_signal.clone(),
                live.clone(),
            )));
        }

        // Hold the window open until the deadline, then signal the writers to stop.
        while runner.keep_running() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let elapsed_secs = runner.elapsed().as_secs_f64().max(f64::MIN_POSITIVE);
        stop.store(true, Ordering::Relaxed);
        stop_signal.notify_waiters();

        let mut stats = WriterStats::default();
        for h in handles {
            stats.merge(h.await??);
        }

        log.flush().await?;

        // Achieved throughput (over accepted records) and, for open-loop, how much
        // of the offered target was sustained.
        let ops_per_sec = stats.records as f64 / elapsed_secs;
        let bytes_per_sec = stats.bytes as f64 / elapsed_secs;
        let mean = |sum: f64, count: u64| if count > 0 { sum / count as f64 } else { 0.0 };

        let mut summary = Summary::new()
            .add("num_writer_tasks", num_writer_tasks as f64)
            .add("l0_only", if l0_only { 1.0 } else { 0.0 })
            .add(
                "block_size_bytes",
                block_size.map_or(0.0, |b| b.as_bytes() as f64),
            )
            .add(
                "seal_interval_ms",
                seal_interval.map_or(0.0, |d| d.as_millis() as f64),
            )
            .add("target_records_per_sec", target_records_per_sec)
            .add("target_bytes_per_sec", target_bytes_per_sec)
            .add("throughput_ops", ops_per_sec)
            .add("throughput_bytes", bytes_per_sec)
            .add("records_written", stats.records as f64)
            .add("queue_full_batches", stats.queue_full as f64)
            .add(
                "batch_latency_us_mean",
                mean(stats.latency_sum_us, stats.latency_count),
            )
            .add("batch_latency_us_max", stats.latency_max_us);

        // Open-loop only: fraction of the offered rate actually sustained, and the
        // schedule lag — the signals for "did the fixed target hold at this N?".
        if per_task_rate > 0.0 {
            let sustained = if target_bytes_per_sec > 0.0 {
                bytes_per_sec / target_bytes_per_sec
            } else {
                0.0
            };
            summary = summary
                .add("sustained_frac", sustained)
                .add("sched_lag_us_mean", mean(stats.lag_sum_us, stats.lag_count))
                .add("sched_lag_us_max", stats.lag_max_us);
        }

        summary = summary.add("elapsed_ms", runner.elapsed().as_millis() as f64);
        bench.summarize(summary).await?;

        // Close the log to release the SlateDB fence. `close` consumes `self`, so
        // the Arc must be uniquely owned — every writer task (which held a clone)
        // has been joined above, so this should succeed.
        match Arc::try_unwrap(log) {
            Ok(log) => log.close().await?,
            Err(_) => anyhow::bail!("log still shared at close; a writer task outlived the run"),
        }

        bench.close().await?;
        Ok(())
    }
}
