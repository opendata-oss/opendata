//! The open-loop session arrival generator.
//!
//! Models the population waking up: sessions arrive on a **global open-loop
//! schedule** at aggregate rate `session_rate`, each targeting a uniform-random
//! key. Open-loop means arrivals fire on schedule regardless of how busy the
//! database is — a slow database does not stop new users from waking — so the
//! offered read load is what we configured, and a slow database shows up as
//! session scheduling lag rather than as suppressed arrivals (no coordinated
//! omission; see RFC 0006 "Scheduling").
//!
//! Each arrival is spawned as its own session task; there is no concurrency cap, so
//! the read concurrency the database sees is emergent — `session_rate ×
//! session_duration` (Little's law) — and stays bounded by those two knobs even as
//! the key population grows. (A session's lifetime is a fixed wall-clock deadline,
//! so occupancy is independent of DB speed; no pool ceiling is needed to control
//! it.) `max_inflight` is only a runaway safety backstop against a fat-fingered
//! `rate × duration`, counting refusals if it ever trips.
//!
//! Inter-arrival times are exponential (a Poisson process — the superposition of
//! independent per-user wakeups), drawn from a small seeded PRNG so the workload
//! is reproducible (RFC 0006 keeps the *workload* seed-reproducible while accepting
//! machine-dependent timing).

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use super::FollowState;
use super::session::run_one_session;

/// A small, fast, seedable PRNG (SplitMix64). Enough for uniform key selection and
/// exponential inter-arrivals without pulling in a `rand` dependency.
pub struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A uniform `f64` in the open interval (0, 1).
    pub fn next_f64(&mut self) -> f64 {
        // Top 53 bits → mantissa; the `+ 0.5` offset keeps it strictly inside (0,1)
        // so `ln` below never sees 0.
        ((self.next_u64() >> 11) as f64 + 0.5) * (1.0 / (1u64 << 53) as f64)
    }

    /// An exponential inter-arrival gap for a Poisson process of rate `rate`/sec.
    fn exp_interval(&mut self, rate: f64) -> Duration {
        let gap = -self.next_f64().ln() / rate;
        Duration::from_secs_f64(gap)
    }
}

/// Run the open-loop session generator until cancelled.
///
/// Spawns one session task per arrival; on shutdown, drains outstanding sessions,
/// surfacing the first error (a failed poll aborts the benchmark).
pub async fn run_generator(
    state: Arc<FollowState>,
    cancel: CancellationToken,
    session_rate: f64,
    seed: u64,
    max_inflight: usize,
    session_key_range: usize,
) -> anyhow::Result<()> {
    // Sessions select uniformly from the first `session_key_range` keys (a hot
    // subrange); the writer still populates all `state.keys`. Clamp to the key
    // count so an over-large value just means "the whole keyspace".
    let cardinality = (session_key_range as u64).min(state.keys.len() as u64);
    if cardinality == 0 || session_rate <= 0.0 {
        cancel.cancelled().await;
        return Ok(());
    }

    let mut rng = SplitMix64::new(seed);
    let mut sessions: JoinSet<anyhow::Result<()>> = JoinSet::new();
    let mut next = state.start;

    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(next) => {}
            _ = cancel.cancelled() => break,
        }
        let scheduled_due = next;
        next += rng.exp_interval(session_rate);

        // Reap finished sessions so the in-flight set stays pruned, and surface any
        // session error immediately.
        while let Some(joined) = sessions.try_join_next() {
            joined??;
        }

        // Runaway safety backstop only: in-flight self-bounds at ~rate*duration
        // since sessions retire on their deadline, so this trips only on a
        // fat-fingered rate*duration.
        if sessions.len() >= max_inflight {
            if state.recording() {
                state.refused_sessions.fetch_add(1, Ordering::Relaxed);
            }
            continue;
        }

        // Enforce one session per key: claim the key (false -> true). If it is
        // already being followed, drop this arrival (uniform selection preserved;
        // realized rate = offered rate minus these skips). The session clears the
        // claim when it ends; the AcqRel/Release pair also orders the per-key cursor
        // handoff from one session to the next.
        let id = (rng.next_u64() % cardinality) as usize;
        if state.key_busy[id].swap(true, Ordering::AcqRel) {
            if state.recording() {
                state.skipped_busy_sessions.fetch_add(1, Ordering::Relaxed);
            }
            continue;
        }
        sessions.spawn(run_one_session(
            id,
            scheduled_due,
            state.clone(),
            cancel.clone(),
        ));
    }

    // Drain outstanding sessions, surfacing the first error.
    while let Some(joined) = sessions.join_next().await {
        joined??;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prng_is_deterministic_per_seed_and_advances() {
        let a: Vec<u64> = (0..8)
            .scan(SplitMix64::new(42), |r, _| Some(r.next_u64()))
            .collect();
        let b: Vec<u64> = (0..8)
            .scan(SplitMix64::new(42), |r, _| Some(r.next_u64()))
            .collect();
        assert_eq!(a, b, "same seed reproduces the same sequence");
        // The stream advances (not a constant) and a different seed diverges.
        assert!(a.windows(2).any(|w| w[0] != w[1]));
        let c: Vec<u64> = (0..8)
            .scan(SplitMix64::new(43), |r, _| Some(r.next_u64()))
            .collect();
        assert_ne!(a, c, "different seeds diverge");
    }

    #[test]
    fn next_f64_is_strictly_inside_unit_interval() {
        let mut rng = SplitMix64::new(7);
        for _ in 0..100_000 {
            let u = rng.next_f64();
            assert!(u > 0.0 && u < 1.0, "u={u} escaped (0,1)");
        }
    }

    #[test]
    fn exp_interval_mean_approximates_inverse_rate() {
        let rate = 250.0;
        let n = 200_000;
        let mut rng = SplitMix64::new(123);
        let mut total = 0.0;
        for _ in 0..n {
            let gap = rng.exp_interval(rate).as_secs_f64();
            assert!(gap > 0.0, "inter-arrival gap must be positive");
            total += gap;
        }
        let mean = total / n as f64;
        let expected = 1.0 / rate;
        // The sample mean of an exponential converges to 1/rate; a 5% band is ample
        // for 200k draws and keeps the test deterministic (fixed seed).
        assert!(
            (mean - expected).abs() / expected < 0.05,
            "mean {mean} not within 5% of {expected}"
        );
    }
}
