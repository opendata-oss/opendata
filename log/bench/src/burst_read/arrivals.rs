//! The concurrent live write load: a drifting hot active-set of bursty writers.
//!
//! Where [`prefill`](super::prefill) builds the *static* backlog once (the deep,
//! segment-scattered corpus that makes catch-up expensive), these writers run
//! *during* the warm-up and measure windows so a reader that catches up then
//! follows a live-growing tail — turning what was an empty tail-poll on a static
//! corpus into a real incremental read.
//!
//! The arrival shape is the same high-cardinality bursty pattern as the
//! `burst` write benchmark: at any instant ~`active_keys` distinct keys are
//! mid-burst, kept alive by `num_writers` appenders (writers ≪ active set is
//! fine — the distribution matters more than write concurrency). A key receives
//! `arrival_burst_size` records (in `arrival_batch_size` chunks) before it is
//! evicted and a fresh cold key is admitted in its place, so the hot set drifts
//! across the population. A reader bound to a key therefore sees live bursts
//! while that key is hot and empty tail-polls once it churns out — the partial,
//! bursty reader/writer overlap is intrinsic to this model, not a flaw.
//!
//! Each writer owns a strided shard of the key space (`id % num_writers ==
//! writer_idx`), so a key is only ever written by one writer and per-key
//! ordering holds across re-admissions. Arrivals drive the shared
//! [`LagTracker`] in every phase (workload state), but the arrival *tallies* are
//! recorded only in the measure phase. Like `burst`, the load is closed-loop by
//! default and open-loop when a target rate is set (writers pace against a fixed
//! schedule, so a database that can't keep up shows up as schedule lag rather
//! than a silently lowered offered rate — no coordinated omission).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use log::Record;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::PHASE_MEASURE;
use super::metrics::WriteMetrics;
use crate::read::lag::LagTracker;
use crate::read::store::LogDbStore;

/// Microseconds per second, for rate/latency conversions.
const MICROS_PER_SEC: f64 = 1_000_000.0;

/// A tiny seedable PRNG (SplitMix64) for reproducible cold-key admission. We
/// want reproducibility, not cryptographic quality, and this avoids a `rand`
/// dependency (the follow generator and `burst` bench carry their own copies).
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

    /// Uniform in `[0, n)`; `n` must be non-zero.
    fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }
}

/// Decorrelate a per-writer seed from the run seed and writer index, so each
/// writer's admission stream is independent yet the run is reproducible from one
/// `seed`.
pub fn writer_seed(run_seed: u64, writer_idx: usize) -> u64 {
    run_seed
        ^ (writer_idx as u64)
            .wrapping_add(1)
            .wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

/// Shared (atomics / read-only) state for the arrival writers.
pub struct ArrivalState {
    store: Arc<LogDbStore>,
    keys: Arc<Vec<Bytes>>,
    value: Bytes,
    record_size: u64,
    /// Records a key receives before it is evicted and replaced.
    burst_size: usize,
    /// Append granularity within a burst; smaller keeps a key hot longer.
    batch_size: usize,
    num_writers: usize,
    lag: Arc<LagTracker>,
    /// Shared with the reader pool; arrivals only record tallies while it reads
    /// [`PHASE_MEASURE`].
    phase: Arc<std::sync::atomic::AtomicU8>,
    metrics: WriteMetrics,

    // ---- Measure-window tallies ----
    records_written: AtomicU64,
    bytes_written: AtomicU64,
    bursts: AtomicU64,
    /// Batches dropped because the write queue was full (the saturation signal).
    queue_full_batches: AtomicU64,
    // Open-loop schedule lag (microseconds): how late batches fired vs their slot.
    sched_lag_us_sum: AtomicU64,
    sched_lag_us_count: AtomicU64,
    sched_lag_us_max: AtomicU64,
}

impl ArrivalState {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        store: Arc<LogDbStore>,
        keys: Arc<Vec<Bytes>>,
        value: Bytes,
        record_size: u64,
        burst_size: usize,
        batch_size: usize,
        num_writers: usize,
        lag: Arc<LagTracker>,
        phase: Arc<std::sync::atomic::AtomicU8>,
        metrics: WriteMetrics,
    ) -> Self {
        Self {
            store,
            keys,
            value,
            record_size,
            burst_size,
            batch_size,
            num_writers,
            lag,
            phase,
            metrics,
            records_written: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            bursts: AtomicU64::new(0),
            queue_full_batches: AtomicU64::new(0),
            sched_lag_us_sum: AtomicU64::new(0),
            sched_lag_us_count: AtomicU64::new(0),
            sched_lag_us_max: AtomicU64::new(0),
        }
    }

    fn recording(&self) -> bool {
        self.phase.load(Ordering::Relaxed) == PHASE_MEASURE
    }

    pub fn records_written(&self) -> u64 {
        self.records_written.load(Ordering::Relaxed)
    }
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written.load(Ordering::Relaxed)
    }
    pub fn bursts(&self) -> u64 {
        self.bursts.load(Ordering::Relaxed)
    }
    pub fn queue_full_batches(&self) -> u64 {
        self.queue_full_batches.load(Ordering::Relaxed)
    }
    /// Mean open-loop schedule lag (µs); 0 in closed-loop mode (no schedule).
    pub fn sched_lag_us_mean(&self) -> f64 {
        let n = self.sched_lag_us_count.load(Ordering::Relaxed);
        if n == 0 {
            0.0
        } else {
            self.sched_lag_us_sum.load(Ordering::Relaxed) as f64 / n as f64
        }
    }
    pub fn sched_lag_us_max(&self) -> f64 {
        self.sched_lag_us_max.load(Ordering::Relaxed) as f64
    }
}

/// One key currently mid-burst: its index within the writer's shard and how many
/// more records it should receive before being evicted.
struct ActiveSlot {
    local: usize,
    remaining: usize,
}

/// Run one arrival writer until cancelled.
///
/// Owns the strided shard `{writer_idx, writer_idx + num_writers, …}`, keeps
/// `active_count` of those keys mid-burst, and round-robins one batch to each.
/// When a key completes its burst it is evicted and a fresh cold key admitted
/// (or re-armed in place if the shard has no cold keys — the degenerate
/// `active_keys == shard` case). `per_writer_rate` is this writer's share of the
/// offered records/sec; `<= 0` means closed-loop (append as fast as accepted).
#[allow(clippy::too_many_arguments)]
pub async fn run_writer(
    writer_idx: usize,
    active_count: usize,
    per_writer_rate: f64,
    seed: u64,
    pace_start: Instant,
    state: Arc<ArrivalState>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let n = state.keys.len();
    let owned = if writer_idx < n {
        (n - writer_idx).div_ceil(state.num_writers)
    } else {
        0
    };
    if owned == 0 {
        cancel.cancelled().await;
        return Ok(());
    }

    let mut is_active = vec![false; owned];
    let mut rng = SplitMix64::new(seed);

    // Admit a cold (not-currently-active) key chosen uniformly from the shard.
    // With active_count ≪ owned the shard is sparsely active, so random probing
    // finds a cold key in O(1) expected; only called when one exists.
    let admit = |rng: &mut SplitMix64, is_active: &mut [bool]| -> usize {
        loop {
            let c = rng.below(owned);
            if !is_active[c] {
                is_active[c] = true;
                return c;
            }
        }
    };

    let active_count = active_count.min(owned).max(1);
    let can_churn = active_count < owned;
    let mut active: Vec<ActiveSlot> = (0..active_count)
        .map(|_| ActiveSlot {
            local: admit(&mut rng, &mut is_active),
            remaining: state.burst_size,
        })
        .collect();
    if state.recording() {
        state.bursts.fetch_add(active.len() as u64, Ordering::Relaxed);
    }

    let paced = per_writer_rate > 0.0;
    let period = if paced {
        Duration::from_secs_f64(state.batch_size as f64 / per_writer_rate)
    } else {
        Duration::ZERO
    };
    let mut next = pace_start;
    let mut robin = 0usize;

    loop {
        if cancel.is_cancelled() {
            break;
        }
        if paced {
            tokio::select! {
                _ = tokio::time::sleep_until(next) => {}
                _ = cancel.cancelled() => break,
            }
        }

        // One batch of the current active key's burst — all records target the
        // same key, so a burst lands contiguously within a segment while the
        // round-robin smears the active keys across the global sequence.
        let local = active[robin].local;
        let global_id = writer_idx + local * state.num_writers;
        let key = state.keys[global_id].clone();
        let batch: Vec<Record> = (0..state.batch_size)
            .map(|_| Record {
                key: key.clone(),
                value: state.value.clone(),
            })
            .collect();

        let batch_start = Instant::now();
        let accepted = state.store.try_arrival(batch).await?;
        let elapsed_us = batch_start.elapsed().as_secs_f64() * MICROS_PER_SEC;

        if accepted {
            // Appends drive lag in every phase (a returning reader must see the
            // backlog that grew while it was away); tallies are measure-only.
            state.lag.record_appended(global_id, state.batch_size as u64);
            if state.recording() {
                state
                    .records_written
                    .fetch_add(state.batch_size as u64, Ordering::Relaxed);
                state.bytes_written.fetch_add(
                    state.batch_size as u64 * state.record_size,
                    Ordering::Relaxed,
                );
                state.metrics.records.increment(state.batch_size as u64);
                state
                    .metrics
                    .bytes
                    .increment(state.batch_size as u64 * state.record_size);
                state.metrics.batch_latency_us.record(elapsed_us);
            }

            let slot = &mut active[robin];
            slot.remaining = slot.remaining.saturating_sub(state.batch_size);
            if slot.remaining == 0 {
                if can_churn {
                    is_active[slot.local] = false;
                    slot.local = admit(&mut rng, &mut is_active);
                }
                slot.remaining = state.burst_size;
                if state.recording() {
                    state.bursts.fetch_add(1, Ordering::Relaxed);
                }
            }
        } else if state.recording() {
            // Back-pressure: the offered batch was rejected (write queue full).
            // That IS saturation, so count it and carry on — the burst does not
            // advance and the batch is dropped (no retry → no coordinated omission).
            state.queue_full_batches.fetch_add(1, Ordering::Relaxed);
        }

        // Advance to the next active key regardless of outcome, so the writer
        // keeps the whole active set moving rather than stalling on one hot key.
        robin = (robin + 1) % active.len();

        if paced {
            if state.recording() {
                let lag_us = batch_start.saturating_duration_since(next).as_micros() as u64;
                state.sched_lag_us_sum.fetch_add(lag_us, Ordering::Relaxed);
                state.sched_lag_us_count.fetch_add(1, Ordering::Relaxed);
                state.sched_lag_us_max.fetch_max(lag_us, Ordering::Relaxed);
            }
            next += period;
        }
    }
    Ok(())
}
