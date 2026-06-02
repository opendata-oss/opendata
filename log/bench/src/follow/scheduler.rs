//! The open-loop, sharded poll scheduler and the bounded execution pool.
//!
//! Scheduling is open-loop (RFC 0006): a consumer's poll times follow its own
//! cadence, independent of how long any poll takes — matching real users who poll
//! on their own schedule regardless of server speed.
//!
//! - **Sharded clocks.** Keys are partitioned across shards; each shard owns a
//!   min-heap of `(due_time, key)` and runs its own clock, so there is no global
//!   heap lock at high poll rates.
//! - **Clock separate from execution.** A shard sleeps until the next poll is due,
//!   hands it to a bounded execution pool (an `async_channel` feeding a fixed set
//!   of workers), immediately reschedules the consumer, and continues — it never
//!   blocks on execution. The bounded channel sizes the concurrency the database
//!   sees and is where backpressure appears.
//! - **One poll per consumer at a time.** A consumer whose previous poll is still
//!   running does not start a second concurrent one; the tick is coalesced (and
//!   counted) and the consumer catches up later. This keeps each key's cursor under
//!   a single accessor at a time and models a real follower.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::FollowState;
use super::store::Cursor;

/// A small, fast, seedable PRNG (SplitMix64). Used to generate the workload's
/// poll cadence deterministically from a seed, so a run is reproducible even
/// though its wall-clock performance is not.
pub struct SplitMix64(u64);

impl SplitMix64 {
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform `f64` in `[0, 1)` using the top 53 bits.
    pub fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
}

/// The per-consumer polling cadence: exponentially-distributed inter-poll
/// intervals (Poisson polling) with a mean, plus an offline model. This is the
/// knob that shapes the lag distribution the read workload approximates.
#[derive(Clone, Copy)]
pub struct PollDist {
    pub mean: Duration,
    pub offline_prob: f64,
    pub offline: Duration,
}

impl PollDist {
    /// Draw the next inter-poll interval. With probability `offline_prob` the
    /// consumer goes offline for `offline`, letting lag accumulate; otherwise the
    /// interval is exponential with mean `mean`.
    pub fn sample(&self, rng: &mut SplitMix64) -> Duration {
        if self.offline_prob > 0.0 && rng.next_f64() < self.offline_prob {
            return self.offline;
        }
        // Inverse-CDF of the exponential: -mean * ln(1 - u). Clamp the tail so an
        // unlucky draw near 1.0 cannot overflow the Duration multiply.
        let u = rng.next_f64();
        let factor = (-(1.0 - u).ln()).min(20.0);
        self.mean.mul_f64(factor)
    }
}

/// A poll dispatched from a shard to the execution pool, carrying the time it was
/// *scheduled* to run (for coordinated-omission-correct latency).
pub struct PollJob {
    pub key_id: usize,
    pub scheduled_due: Instant,
}

/// Run one scheduler shard until cancelled.
///
/// The shard owns `key_ids`, keeps a min-heap of their due times, and dispatches
/// each due poll into `tx` without ever blocking on execution. The polling
/// distribution, run start time, and base seed are read from the shared state.
pub async fn run_shard(
    shard_idx: usize,
    key_ids: Vec<usize>,
    tx: async_channel::Sender<PollJob>,
    state: Arc<FollowState>,
    cancel: CancellationToken,
) {
    let dist = state.dist;
    let mut rng =
        SplitMix64::new(state.seed ^ (shard_idx as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15));

    // Stagger each key's first poll across its first sampled interval so the whole
    // population does not fire in lockstep at t0.
    let mut heap: BinaryHeap<Reverse<(Instant, usize)>> = BinaryHeap::with_capacity(key_ids.len());
    for &id in &key_ids {
        heap.push(Reverse((state.start + dist.sample(&mut rng), id)));
    }

    while let Some(&Reverse((due, key_id))) = heap.peek() {
        tokio::select! {
            _ = tokio::time::sleep_until(due) => {}
            _ = cancel.cancelled() => break,
        }
        heap.pop();

        // Claim the consumer: only dispatch if its previous poll is not still in
        // flight. `compare_exchange` makes the claim atomic against the worker that
        // clears the flag on completion.
        let claimed = state.in_flight[key_id]
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok();
        if claimed {
            match tx.try_send(PollJob {
                key_id,
                scheduled_due: due,
            }) {
                Ok(()) => state.metrics.dispatch_backlog.set(tx.len() as f64),
                Err(async_channel::TrySendError::Full(_)) => {
                    // Saturation: no room in the bounded pool. Release the claim and
                    // count the drop; the consumer retries at its next due time.
                    state.in_flight[key_id].store(false, Ordering::Release);
                    state.polls_dropped.fetch_add(1, Ordering::Relaxed);
                    state.metrics.dispatch_dropped.increment(1);
                }
                Err(async_channel::TrySendError::Closed(_)) => {
                    state.in_flight[key_id].store(false, Ordering::Release);
                    break;
                }
            }
        } else {
            state.polls_coalesced.fetch_add(1, Ordering::Relaxed);
            state.metrics.polls_coalesced.increment(1);
        }

        // Reschedule from the due time, not from now: the cadence is independent of
        // execution. If the system is behind, the next due may already be in the
        // past and fire immediately — which is the saturation signal we want.
        let next = due + dist.sample(&mut rng);
        heap.push(Reverse((next, key_id)));
    }
}

/// Run one execution-pool worker until the dispatch channel is closed and drained.
///
/// Returns an error if a poll fails, which aborts the benchmark.
pub async fn run_worker(
    rx: async_channel::Receiver<PollJob>,
    state: Arc<FollowState>,
) -> anyhow::Result<()> {
    while let Ok(job) = rx.recv().await {
        let id = job.key_id;
        let service_start = Instant::now();
        let scheduling_lag = service_start.saturating_duration_since(job.scheduled_due);

        let cursor = Cursor(state.cursors[id].load(Ordering::Relaxed));
        let result = state
            .store
            .poll(state.keys[id].clone(), cursor, state.page_size)
            .await;

        // Always release the claim, even on error, so we never wedge a consumer.
        let out = match result {
            Ok(out) => out,
            Err(e) => {
                state.in_flight[id].store(false, Ordering::Release);
                return Err(e);
            }
        };
        state.cursors[id].store(out.cursor.0, Ordering::Relaxed);
        state.in_flight[id].store(false, Ordering::Release);

        let completed = Instant::now();
        state.lag.record_acked(id, out.n_records as u64);
        state.polls_completed.fetch_add(1, Ordering::Relaxed);
        state
            .metrics
            .records_consumed
            .increment(out.n_records as u64);
        state.metrics.polls.increment(1);
        state.metrics.poll_latency_us.record(
            completed
                .saturating_duration_since(job.scheduled_due)
                .as_micros() as f64,
        );
        state.metrics.poll_service_us.record(
            completed
                .saturating_duration_since(service_start)
                .as_micros() as f64,
        );
        state
            .metrics
            .scheduling_lag_us
            .record(scheduling_lag.as_micros() as f64);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splitmix_is_deterministic_for_a_seed() {
        let mut a = SplitMix64::new(42);
        let mut b = SplitMix64::new(42);
        let seq_a: Vec<u64> = (0..8).map(|_| a.next_u64()).collect();
        let seq_b: Vec<u64> = (0..8).map(|_| b.next_u64()).collect();
        assert_eq!(seq_a, seq_b);
        // Different seeds diverge.
        let mut c = SplitMix64::new(43);
        assert_ne!(seq_a[0], c.next_u64());
    }

    #[test]
    fn next_f64_stays_in_unit_interval() {
        let mut rng = SplitMix64::new(7);
        for _ in 0..10_000 {
            let u = rng.next_f64();
            assert!((0.0..1.0).contains(&u), "u={u}");
        }
    }

    #[test]
    fn offline_probability_one_always_returns_offline() {
        let dist = PollDist {
            mean: Duration::from_millis(100),
            offline_prob: 1.0,
            offline: Duration::from_secs(5),
        };
        let mut rng = SplitMix64::new(1);
        for _ in 0..100 {
            assert_eq!(dist.sample(&mut rng), Duration::from_secs(5));
        }
    }

    #[test]
    fn exponential_mean_is_approximately_correct() {
        let dist = PollDist {
            mean: Duration::from_millis(200),
            offline_prob: 0.0,
            offline: Duration::ZERO,
        };
        let mut rng = SplitMix64::new(99);
        let n = 50_000;
        let total: f64 = (0..n).map(|_| dist.sample(&mut rng).as_secs_f64()).sum();
        let mean = total / n as f64;
        // Loose bound: the sample mean should land near 0.2s (clamped tail pulls it
        // very slightly down, but well within 10%).
        assert!((0.18..0.22).contains(&mean), "mean={mean}");
    }
}
