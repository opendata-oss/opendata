//! Best-effort cardinality estimator for "active series in the last 15 minutes".
//!
//! Implemented as a ring of 15 small HyperLogLog sketches (~4 KB each). The
//! "active" bucket is tagged with its minute-since-UNIX-epoch; inserts always
//! land in `buckets[active_minute % 15]`. The window is advanced by calls to
//! [`ActiveSeriesTracker::refresh`] (driven by the flusher) — there is no
//! background task; if no flushes happen the gauge simply goes stale.

use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Number of HLL sketches in the ring. With one rotation per minute this
/// covers a ~15-minute sliding window (the bucket currently filling plus
/// 14 previously-filled buckets).
const BUCKET_COUNT: usize = 15;

/// log2 of the number of registers per HLL. 12 → 4096 registers → ~1.6%
/// standard error, ~4 KB per sketch (~60 KB total across the ring).
const P: u32 = 12;
const M: usize = 1 << P;
const REGISTER_MASK: u64 = (M as u64) - 1;

struct Hll {
    registers: Vec<AtomicU8>,
}

impl Hll {
    fn new() -> Self {
        Self {
            registers: (0..M).map(|_| AtomicU8::new(0)).collect(),
        }
    }

    fn insert(&self, h: u64) {
        let j = (h & REGISTER_MASK) as usize;
        let tail = h >> P;
        // tail occupies the low 64-P bits of u64; its leading_zeros is therefore
        // always at least P. Rank is the 1-indexed position of the leftmost set
        // bit within the 64-P-bit window. For tail == 0 the formula yields
        // (64 - P) + 1, which is the maximum representable rank.
        let rank = (tail.leading_zeros() + 1 - P) as u8;
        self.registers[j].fetch_max(rank, Ordering::Relaxed);
    }

    fn reset(&self) {
        for r in &self.registers {
            r.store(0, Ordering::Relaxed);
        }
    }
}

pub(crate) struct ActiveSeriesTracker {
    buckets: Vec<Hll>,
    /// Minute-since-UNIX-epoch of the currently-active bucket. Inserts target
    /// `buckets[active_minute % BUCKET_COUNT]`. Updated by `refresh`.
    active_minute: AtomicU64,
}

impl ActiveSeriesTracker {
    /// Create a tracker with `now_minute` as the initial active minute. Pass
    /// [`current_unix_minute`] in production; tests can pass any baseline.
    pub(crate) fn new(now_minute: u64) -> Self {
        Self {
            buckets: (0..BUCKET_COUNT).map(|_| Hll::new()).collect(),
            active_minute: AtomicU64::new(now_minute),
        }
    }

    pub(crate) fn record(&self, fingerprint: u128) {
        let h = hash_fingerprint(fingerprint);
        let m = self.active_minute.load(Ordering::Acquire);
        self.buckets[(m as usize) % BUCKET_COUNT].insert(h);
    }

    /// Catch the ring up to `now_minute` and return the merged cardinality
    /// estimate. Each elapsed minute advances `active_minute` by one and resets
    /// the bucket rolling in. Inserts that race a reset may land in the bucket
    /// just rotated out — that's fine, it stays in the window for another
    /// `BUCKET_COUNT - 1` ticks.
    pub(crate) fn refresh(&self, now_minute: u64) -> u64 {
        let prev = self.active_minute.load(Ordering::Relaxed);
        if now_minute > prev {
            let advance = (now_minute - prev).min(BUCKET_COUNT as u64);
            for step in 1..=advance {
                let target = ((prev + step) as usize) % BUCKET_COUNT;
                self.buckets[target].reset();
            }
            self.active_minute.store(now_minute, Ordering::Release);
        }
        self.estimate()
    }

    fn estimate(&self) -> u64 {
        let mut sum = 0.0f64;
        let mut zeros = 0u32;
        for j in 0..M {
            let mut max_r: u8 = 0;
            for bucket in &self.buckets {
                let r = bucket.registers[j].load(Ordering::Relaxed);
                if r > max_r {
                    max_r = r;
                }
            }
            if max_r == 0 {
                zeros += 1;
            }
            sum += 2f64.powi(-(max_r as i32));
        }

        let m = M as f64;
        let alpha = 0.7213 / (1.0 + 1.079 / m);
        let raw = alpha * m * m / sum;
        // Large-range correction domain matches the hash bit width (64).
        // Classic HLL used 2^32 because it assumed a 32-bit hash; with our
        // 64-bit hash that threshold is astronomically far away and the
        // correction effectively never fires, but the math is only valid
        // when the domain matches the hash space.
        let pow_l = 2f64.powi(64);
        let estimate = if raw <= 2.5 * m {
            if zeros > 0 {
                m * (m / zeros as f64).ln()
            } else {
                raw
            }
        } else if raw <= pow_l / 30.0 {
            raw
        } else if raw < pow_l {
            -pow_l * (1.0 - raw / pow_l).ln()
        } else {
            // Pathological: raw exceeds the hash domain. ln() would be NaN;
            // fall back to the raw estimate rather than emit garbage.
            raw
        };
        estimate.round().max(0.0) as u64
    }
}

/// Wall-clock minutes since the UNIX epoch. Returns 0 if the system clock is
/// before the epoch (pre-1970), which is impossible in practice.
pub(crate) fn current_unix_minute() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() / 60)
        .unwrap_or(0)
}

/// Fold a 128-bit blake3 fingerprint into 64 uniformly-random bits.
fn hash_fingerprint(fp: u128) -> u64 {
    (fp as u64) ^ ((fp >> 64) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Produce u128 values whose bits are uniformly random by hashing the
    /// input with blake3, the same construction the real fingerprint uses.
    fn fingerprint(seed: u128) -> u128 {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&seed.to_le_bytes());
        let bytes = hasher.finalize();
        let mut buf = [0u8; 16];
        buf.copy_from_slice(&bytes.as_bytes()[..16]);
        u128::from_le_bytes(buf)
    }

    #[test]
    fn should_estimate_cardinality_within_error_bound() {
        // given
        let tracker = ActiveSeriesTracker::new(0);
        let n: u64 = 50_000;

        // when
        for i in 0..n {
            tracker.record(fingerprint(i as u128));
        }

        // then: HLL with 4096 registers gives ~1.6% std error; allow 5%.
        let est = tracker.refresh(0);
        let rel_err = (est as i64 - n as i64).unsigned_abs() as f64 / n as f64;
        assert!(
            rel_err < 0.05,
            "estimate {} too far from {} (rel_err = {:.3})",
            est,
            n,
            rel_err
        );
    }

    #[test]
    fn should_estimate_one_for_small_cardinality() {
        // given
        let tracker = ActiveSeriesTracker::new(0);

        // when
        tracker.record(fingerprint(42));

        // then: linear counting kicks in for tiny cardinalities
        assert_eq!(tracker.refresh(0), 1);
    }

    #[test]
    fn should_forget_series_after_full_window() {
        // given
        let tracker = ActiveSeriesTracker::new(0);
        for i in 0..10_000u128 {
            tracker.record(fingerprint(i));
        }
        assert!(tracker.refresh(0) > 5_000);

        // when: advance by an entire window — every bucket gets reset
        let est = tracker.refresh(BUCKET_COUNT as u64);

        // then
        assert_eq!(est, 0);
    }

    #[test]
    fn should_count_recurring_series_once_across_rotations() {
        // given
        let tracker = ActiveSeriesTracker::new(0);
        let fp = fingerprint(42);

        // when: insert the same fingerprint into every bucket
        for m in 0..BUCKET_COUNT as u64 {
            tracker.record(fp);
            tracker.refresh(m + 1);
        }

        // then: merged estimate is ~1, not BUCKET_COUNT
        let est = tracker.refresh(BUCKET_COUNT as u64);
        assert!(est <= 2, "expected ~1, got {}", est);
    }

    #[test]
    fn should_retain_series_within_window() {
        // given
        let tracker = ActiveSeriesTracker::new(0);
        for i in 0..5_000u128 {
            tracker.record(fingerprint(i));
        }

        // when: advance fewer than BUCKET_COUNT minutes — original data still
        // sits in one of the remaining buckets
        let est = tracker.refresh(BUCKET_COUNT as u64 - 1);

        // then
        let rel_err = (est as i64 - 5_000).unsigned_abs() as f64 / 5_000.0;
        assert!(rel_err < 0.05, "estimate {} drifted: {:.3}", est, rel_err);
    }

    #[test]
    fn should_clamp_advance_to_bucket_count() {
        // given
        let tracker = ActiveSeriesTracker::new(0);
        for i in 0..5_000u128 {
            tracker.record(fingerprint(i));
        }

        // when: advance by far more than BUCKET_COUNT minutes (long idle)
        let est = tracker.refresh(1_000_000);

        // then: every bucket gets reset exactly once and the estimate is 0
        assert_eq!(est, 0);
    }
}
