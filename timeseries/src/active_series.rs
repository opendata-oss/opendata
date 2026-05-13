//! Best-effort cardinality estimator for "active series in the last 15 minutes".
//!
//! Implemented as a ring of 15 small HyperLogLog sketches (~4 KB each). Each
//! sketch holds inserts for one minute; the ring is advanced once per minute,
//! resetting the bucket that rolls in. `estimate()` merges all sketches.

use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

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
    current: AtomicUsize,
}

impl ActiveSeriesTracker {
    pub(crate) fn new() -> Self {
        Self {
            buckets: (0..BUCKET_COUNT).map(|_| Hll::new()).collect(),
            current: AtomicUsize::new(0),
        }
    }

    pub(crate) fn record(&self, fingerprint: u128) {
        let h = hash_fingerprint(fingerprint);
        let idx = self.current.load(Ordering::Acquire);
        self.buckets[idx].insert(h);
    }

    /// Reset the next bucket and make it the active one. Inserts racing with
    /// this call may land in the *old* bucket — that's fine since the old
    /// bucket remains part of the window for another BUCKET_COUNT-1 ticks.
    pub(crate) fn rotate(&self) {
        let cur = self.current.load(Ordering::Relaxed);
        let next = (cur + 1) % BUCKET_COUNT;
        self.buckets[next].reset();
        self.current.store(next, Ordering::Release);
    }

    /// Merge all sketches in the ring and return the estimated cardinality.
    pub(crate) fn estimate(&self) -> u64 {
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
        let tracker = ActiveSeriesTracker::new();
        let n: u64 = 50_000;

        // when
        for i in 0..n {
            tracker.record(fingerprint(i as u128));
        }

        // then: HLL with 4096 registers gives ~1.6% std error; allow 5%.
        let est = tracker.estimate();
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
        let tracker = ActiveSeriesTracker::new();

        // when
        tracker.record(fingerprint(42));

        // then: linear counting kicks in for tiny cardinalities
        assert_eq!(tracker.estimate(), 1);
    }

    #[test]
    fn should_forget_series_after_full_rotation() {
        // given
        let tracker = ActiveSeriesTracker::new();
        for i in 0..10_000u128 {
            tracker.record(fingerprint(i));
        }
        assert!(tracker.estimate() > 5_000);

        // when: rotate enough times to reset every bucket
        for _ in 0..BUCKET_COUNT {
            tracker.rotate();
        }

        // then
        assert_eq!(tracker.estimate(), 0);
    }

    #[test]
    fn should_count_recurring_series_once_across_rotations() {
        // given
        let tracker = ActiveSeriesTracker::new();
        let fp = fingerprint(42);

        // when: insert the same fingerprint into every bucket
        for _ in 0..BUCKET_COUNT {
            tracker.record(fp);
            tracker.rotate();
        }

        // then: merged estimate is ~1, not BUCKET_COUNT
        let est = tracker.estimate();
        assert!(est <= 2, "expected ~1, got {}", est);
    }

    #[test]
    fn should_retain_series_within_window() {
        // given
        let tracker = ActiveSeriesTracker::new();
        for i in 0..5_000u128 {
            tracker.record(fingerprint(i));
        }

        // when: rotate fewer than BUCKET_COUNT times — original data still
        // sits in the oldest bucket
        for _ in 0..(BUCKET_COUNT - 1) {
            tracker.rotate();
        }

        // then: estimate is still close to 5000
        let est = tracker.estimate();
        let rel_err = (est as i64 - 5_000).unsigned_abs() as f64 / 5_000.0;
        assert!(rel_err < 0.05, "estimate {} drifted: {:.3}", est, rel_err);
    }
}
