//! High-cardinality bursty-write benchmark for the log database.
//!
//! Where the `ingest` benchmark writes *flatly* — every key is live at all
//! times and writers round-robin uniformly across the whole population — this
//! benchmark models the opposite arrival shape: a large key population in which
//! only a small, drifting **active set** is being written at any moment, and
//! each active key receives a short **burst** of records before going cold.
//!
//! The pattern is "high cardinality, low instantaneous fan-out, bursty per
//! key": at any instant ~`active_keys` distinct keys are mid-burst, kept alive
//! by only `num_writer_tasks` appenders (tasks ≪ active set is allowed — the
//! distribution matters more than write concurrency). Over the run, keys churn
//! through the active set, so the working set drifts across the population.
//!
//! Three concepts that `ingest` collapses into `num_keys` are separated here:
//!
//! - `num_keys` (N) — the total population a burst can draw from. This is the
//!   *regime* knob. Large N ⇒ admission rarely repeats ⇒ most keys burst once
//!   ⇒ wide key spread, light per-key fragmentation. Small N ⇒ keys are
//!   re-admitted with long idle gaps between bursts ⇒ a single key's records
//!   scatter across many segments — the write-side cause of segment fan-out.
//! - `active_keys` (H) — how many distinct keys are mid-burst at any instant.
//! - `burst_size` — records a key receives before it is evicted and replaced.
//!
//! Set `active_keys == num_keys` and the active set never churns: every key
//! stays live and writers round-robin all of them, recovering the flat
//! `ingest` baseline as a sanity check.
//!
//! Like `ingest`, the run is closed-loop by default and open-loop when
//! `target_mb_per_sec > 0` (writers pace against a fixed offered rate). Each
//! task owns a strided shard of the key space, so a key is only ever written by
//! one task and per-key ordering holds even across re-admissions. Key admission
//! is seeded (`seed`) so the arrival stream is reproducible run to run.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bencher::{Bench, Benchmark, Counter, Histogram, Params, Summary};
use bytes::Bytes;
use log::{AppendError, Config, LogDb, Record};
use tokio::sync::Notify;
use tokio::time::Instant;

const MICROS_PER_SEC: f64 = 1_000_000.0;
/// Bytes per "MB" for the throughput target — decimal megabytes, matching how
/// the achieved `throughput_bytes` is reported (raw bytes / elapsed seconds).
const BYTES_PER_MB: f64 = 1_000_000.0;

/// SplitMix64 — a tiny, fast, seedable PRNG. Used only to pick which cold key a
/// task admits next; we want reproducibility, not cryptographic quality, and
/// this avoids pulling `rand` into the workspace.
struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `[0, n)`. `n` must be non-zero.
    fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }
}

/// Derive a decorrelated per-task seed from the run seed and task index, so
/// each task's admission stream is independent yet the whole run is
/// reproducible from a single `seed`.
fn task_seed(run_seed: u64, task_idx: usize) -> u64 {
    run_seed
        ^ (task_idx as u64)
            .wrapping_add(1)
            .wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

/// Create a parameter set for the burst benchmark.
fn make_params(
    num_keys: usize,
    active_keys: usize,
    burst_size: usize,
    batch_size: usize,
) -> Params {
    let mut params = Params::new();
    params.insert("num_keys", num_keys.to_string());
    params.insert("active_keys", active_keys.to_string());
    params.insert("burst_size", burst_size.to_string());
    params.insert("batch_size", batch_size.to_string());
    params.insert("value_size", "256".to_string());
    params.insert("key_length", "16".to_string());
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
    /// Bursts started (initial admissions + every re-arm/re-admission).
    bursts: u64,
    /// Distinct keys this task wrote to at least once (validates key spread).
    distinct_keys: u64,
    /// Most records any single key received (peak per-key accumulation).
    max_per_key: u64,
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
        self.bursts += other.bursts;
        self.distinct_keys += other.distinct_keys;
        self.max_per_key = self.max_per_key.max(other.max_per_key);
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

/// One key currently mid-burst: its index within the task's shard and how many
/// more records it should receive before being evicted.
struct ActiveSlot {
    local: usize,
    remaining: usize,
}

/// Run one bursty writer task until signalled to stop.
///
/// The task owns a strided shard of the key space and keeps `active_count` of
/// those keys mid-burst. Each iteration appends one batch to the next active
/// key (round-robin); when a key has received its `burst_size` records it is
/// evicted and a fresh cold key from the shard is admitted in its place. If the
/// shard has no cold keys (active set spans the whole shard) the same key is
/// re-armed instead — this is the flat `ingest`-style degenerate case.
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
    burst_size: usize,
    active_count: usize,
    task_idx: usize,
    num_tasks: usize,
    per_task_rate: f64,
    seed: u64,
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

    // Per-shard-key state, indexed by local key index (0..owned). `appended`
    // also serves as the readers' eventual lag bookkeeping (per-key history).
    let mut appended = vec![0u64; owned];
    let mut is_active = vec![false; owned];
    let mut rng = SplitMix64::new(seed);

    // Admit a cold key (one not currently active) chosen uniformly from the
    // shard. With H ≪ N the shard is sparsely active, so random probing finds a
    // cold key in O(1) expected; callers must only invoke this when one exists.
    let admit = |rng: &mut SplitMix64, is_active: &mut [bool]| -> usize {
        loop {
            let c = rng.below(owned);
            if !is_active[c] {
                is_active[c] = true;
                return c;
            }
        }
    };

    // Seed the active set. `active_count` is already capped at `owned`.
    let active_count = active_count.min(owned).max(1);
    let can_churn = active_count < owned;
    let mut active: Vec<ActiveSlot> = (0..active_count)
        .map(|_| ActiveSlot {
            local: admit(&mut rng, &mut is_active),
            remaining: burst_size,
        })
        .collect();
    stats.bursts += active.len() as u64;

    let paced = per_task_rate > 0.0;
    let period = if paced {
        Duration::from_secs_f64(batch_size as f64 / per_task_rate)
    } else {
        Duration::ZERO
    };
    let mut next = pace_start;
    let mut robin = 0usize;

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

        // This batch is one chunk of the current active key's burst — all
        // records target the same key, so a burst lands contiguously within a
        // segment while the round-robin smears the active keys across the
        // global sequence.
        let local = active[robin].local;
        let key = keys[task_idx + local * num_tasks].clone();
        let records: Vec<Record> = (0..batch_size)
            .map(|_| Record {
                key: key.clone(),
                value: value.clone(),
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

                appended[local] += batch_size as u64;
                let slot = &mut active[robin];
                slot.remaining = slot.remaining.saturating_sub(batch_size);
                if slot.remaining == 0 {
                    // Burst complete: evict this key and admit a fresh cold one,
                    // or re-arm in place when the shard has no cold keys.
                    if can_churn {
                        is_active[slot.local] = false;
                        slot.local = admit(&mut rng, &mut is_active);
                    }
                    slot.remaining = burst_size;
                    stats.bursts += 1;
                }
            }
            // Back-pressure: the offered batch was rejected because the write
            // queue is full. This IS the saturation signal (the database can't
            // accept the offered rate), so count it and carry on rather than
            // aborting the run; the batch is dropped and the burst not advanced.
            Err(AppendError::QueueFull(_)) => {
                stats.queue_full += 1;
            }
            // Any other append error is terminal.
            Err(e) => return Err(e.into()),
        }

        // Advance to the next active key regardless of outcome, so writers keep
        // the whole active set moving rather than stalling on one hot key.
        robin = (robin + 1) % active.len();

        if paced {
            // Schedule lag: how far past its slot this batch actually fired.
            let lag_us = batch_start.saturating_duration_since(next).as_secs_f64() * MICROS_PER_SEC;
            stats.lag_sum_us += lag_us;
            stats.lag_max_us = stats.lag_max_us.max(lag_us);
            stats.lag_count += 1;
            next += period;
        }
    }

    // Fold the per-key history into spread metrics before returning.
    for &count in &appended {
        if count > 0 {
            stats.distinct_keys += 1;
            stats.max_per_key = stats.max_per_key.max(count);
        }
    }

    Ok(stats)
}

/// Benchmark for high-cardinality bursty log writes.
pub struct BurstBenchmark;

impl BurstBenchmark {
    pub fn new() -> Self {
        Self
    }
}

impl Default for BurstBenchmark {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl Benchmark for BurstBenchmark {
    fn name(&self) -> &str {
        "burst"
    }

    fn default_params(&self) -> Vec<Params> {
        // Cardinality sweep at a fixed active set and burst size: does write
        // throughput hold as the population grows while the working set stays
        // small? The last row collapses to the flat `ingest` baseline.
        vec![
            make_params(10_000, 256, 64, 10),
            make_params(100_000, 256, 64, 10),
            make_params(1_000_000, 256, 64, 10),
            make_params(256, 256, 64, 10), // active_keys == num_keys: no churn
        ]
    }

    async fn run(&self, bench: Bench) -> anyhow::Result<()> {
        let params = bench.spec().params();
        let batch_size: usize = params.get_parse::<usize>("batch_size")?.max(1);
        let value_size: usize = params.get_parse("value_size")?;
        let key_length: usize = params.get_parse("key_length")?;
        let num_keys: usize = params.get_parse::<usize>("num_keys")?.max(1);

        // Active set (H): keys mid-burst at any instant. Capped at the
        // population — at the cap the active set spans everything and never
        // churns (flat ingest baseline).
        let active_keys: usize = params
            .get_parse::<usize>("active_keys")
            .unwrap_or(num_keys)
            .max(1)
            .min(num_keys);
        // Records a key receives before it is evicted and replaced.
        let burst_size: usize = params.get_parse::<usize>("burst_size").unwrap_or(64).max(1);
        // Seed for the key-admission RNG, so the arrival stream is reproducible.
        let seed: u64 = match params.get("seed") {
            Some(v) => v.parse()?,
            None => 0,
        };

        // Offered write rate, MB/s. Absent or <= 0 → closed-loop saturation.
        let target_mb_per_sec: f64 = match params.get("target_mb_per_sec") {
            Some(v) => v.parse()?,
            None => 0.0,
        };
        // Concurrent appenders. Capped at active_keys so every task drives at
        // least one mid-burst key; the active set, not the task count, sets the
        // write distribution, so tasks ≪ active_keys is expected.
        let num_writer_tasks = match params.get("num_writer_tasks") {
            Some(v) => v.parse::<usize>()?.max(1),
            None => 1,
        }
        .min(active_keys);
        // Compaction policy: when set, LogDb skips the one-shot consolidation of
        // sealed steady-state segments and keeps only L0 relief (RFC 0005).
        let l0_only: bool = match params.get("l0_only") {
            Some(v) => v.parse()?,
            None => false,
        };
        // Automatic segment seal interval (ms). Absent ⇒ None: sealing disabled,
        // everything stays in segment 0.
        let seal_interval = match params.get("seal_interval_ms") {
            Some(v) => Some(Duration::from_millis(v.parse()?)),
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
        let log = Arc::new(LogDb::open(config).await?);

        // Deterministic key space, shared (read-only) across all writer tasks.
        let keys = Arc::new(
            (0..num_keys)
                .map(|i| Bytes::from(format!("{:0>width$}", i, width = key_length)))
                .collect::<Vec<Bytes>>(),
        );
        let value = Bytes::from(vec![b'x'; value_size]);
        let record_size = key_length + value_size;

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
            // Distribute the active set across tasks, handing the first
            // `active_keys % num_writer_tasks` tasks one extra so the per-task
            // counts sum to exactly `active_keys`.
            let base = active_keys / num_writer_tasks;
            let extra = active_keys % num_writer_tasks;
            let active_count = base + if task_idx < extra { 1 } else { 0 };

            handles.push(tokio::spawn(run_writer(
                log.clone(),
                keys.clone(),
                value.clone(),
                record_size,
                batch_size,
                burst_size,
                active_count,
                task_idx,
                num_writer_tasks,
                per_task_rate,
                task_seed(seed, task_idx),
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
        // Mean bursts per touched key: ~1 means most keys burst once (large-N,
        // wide-spread regime); >1 means keys are re-admitted with gaps (small-N,
        // fragmentation regime).
        let bursts_per_key = if stats.distinct_keys > 0 {
            stats.bursts as f64 / stats.distinct_keys as f64
        } else {
            0.0
        };

        let mut summary = Summary::new()
            .add("num_writer_tasks", num_writer_tasks as f64)
            .add("active_keys", active_keys as f64)
            .add("burst_size", burst_size as f64)
            .add("seed", seed as f64)
            .add("l0_only", if l0_only { 1.0 } else { 0.0 })
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
            // Workload-shape validation: how much of the population was touched,
            // how many bursts ran, and how often keys recurred.
            .add("distinct_keys_written", stats.distinct_keys as f64)
            .add("bursts", stats.bursts as f64)
            .add("bursts_per_key", bursts_per_key)
            .add("max_records_per_key", stats.max_per_key as f64)
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

        // Close the log to release the SlateDB fence. The writer tasks held the
        // only other clones of the Arc and have all been joined, so it is now
        // uniquely owned (`close` consumes `self`).
        match Arc::try_unwrap(log) {
            Ok(log) => log.close().await?,
            Err(_) => anyhow::bail!("log still shared at close; a writer task outlived the run"),
        }

        bench.close().await?;
        Ok(())
    }
}
