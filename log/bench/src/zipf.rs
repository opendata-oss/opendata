//! A seedable PRNG and a table-free bounded Zipf sampler.
//!
//! The keyscale workload samples keys from a population of up to millions under a
//! Zipf (power-law) distribution, so an inverse-CDF table (O(N) memory) is out.
//! We use the rejection-inversion method of Hörmann & Derflinger (1996), which
//! draws a Zipf sample in O(1) time and memory regardless of population size.
//! Both writers and readers sample independently from the same `Zipf` so the hot
//! head they share emerges from the distribution, not from coordination.

/// A tiny seedable PRNG (SplitMix64), matching the other benches' generators so
/// runs are reproducible from `(seed, worker)`.
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

    /// A uniform double in `[0, 1)`.
    pub fn next_f64(&mut self) -> f64 {
        ((self.next_u64() >> 11) as f64 + 0.5) * (1.0 / (1u64 << 53) as f64)
    }

    /// An exponentially-distributed interval (seconds) at the given rate
    /// (events/sec), for Poisson arrivals. Returns infinity for a non-positive
    /// rate.
    pub fn exp_interval_secs(&mut self, rate: f64) -> f64 {
        if rate <= 0.0 {
            return f64::INFINITY;
        }
        // Inverse-CDF: -ln(1-u)/rate. next_f64() ∈ [0,1) so (1-u) ∈ (0,1].
        -(1.0 - self.next_f64()).ln() / rate
    }
}

/// `ln(1 + x) / x`, accurate near zero (limit 1).
fn helper1(x: f64) -> f64 {
    if x.abs() > 1e-8 {
        x.ln_1p() / x
    } else {
        1.0 - x * (0.5 - x * (1.0 / 3.0 - 0.25 * x))
    }
}

/// `(exp(x) - 1) / x`, accurate near zero (limit 1).
fn helper2(x: f64) -> f64 {
    if x.abs() > 1e-8 {
        x.exp_m1() / x
    } else {
        1.0 + x * (0.5 + x * (1.0 / 6.0 + x * (1.0 / 24.0)))
    }
}

/// A bounded Zipf distribution over ranks `1..=n` with exponent (skew) `q`,
/// sampled by rejection-inversion. `sample` returns a 0-based index in `[0, n)`,
/// rank 1 (index 0) being the most frequent.
///
/// The unified `helper2`/`helper1` forms make the `q == 1` case fall out
/// correctly (the integral of `x^-1` is `ln x`), so any `q > 0` is valid.
pub struct Zipf {
    n: f64,
    q: f64,
    h_integral_x1: f64,
    h_integral_n: f64,
    s: f64,
}

impl Zipf {
    /// Build a sampler over `n` elements with skew `q` (`q > 0`). `n == 0` is
    /// treated as a single-element population (always samples index 0).
    pub fn new(n: usize, q: f64) -> Self {
        let n = n.max(1) as f64;
        let mut z = Self {
            n,
            q,
            h_integral_x1: 0.0,
            h_integral_n: 0.0,
            s: 0.0,
        };
        z.h_integral_x1 = z.h_integral(1.5) - 1.0;
        z.h_integral_n = z.h_integral(n + 0.5);
        z.s = 2.0 - z.h_integral_inverse(z.h_integral(2.5) - z.h(2.0));
        z
    }

    /// `x^(-q)`.
    fn h(&self, x: f64) -> f64 {
        (-self.q * x.ln()).exp()
    }

    /// The integral of `h` from 1 to `x`: `(x^(1-q) - 1) / (1 - q)`, computed via
    /// `helper2` so `q == 1` yields `ln x`.
    fn h_integral(&self, x: f64) -> f64 {
        let log_x = x.ln();
        helper2((1.0 - self.q) * log_x) * log_x
    }

    /// Inverse of [`Self::h_integral`].
    fn h_integral_inverse(&self, x: f64) -> f64 {
        let mut t = x * (1.0 - self.q);
        if t < -1.0 {
            // Numerical guard: `t` can dip just below -1 from rounding.
            t = -1.0;
        }
        (helper1(t) * x).exp()
    }

    /// Draw a sample, returning a 0-based index in `[0, n)`.
    pub fn sample(&self, rng: &mut SplitMix64) -> usize {
        if self.n <= 1.0 {
            return 0;
        }
        loop {
            let u = self.h_integral_n + rng.next_f64() * (self.h_integral_x1 - self.h_integral_n);
            let x = self.h_integral_inverse(u);
            let mut k = (x + 0.5) as i64;
            if k < 1 {
                k = 1;
            } else if k as f64 > self.n {
                k = self.n as i64;
            }
            if (k as f64) - x <= self.s || u >= self.h_integral(k as f64 + 0.5) - self.h(k as f64) {
                return (k - 1) as usize;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prng_is_deterministic_per_seed_and_advances() {
        let a: Vec<u64> = (0..5)
            .scan(SplitMix64::new(42), |r, _| Some(r.next_u64()))
            .collect();
        let b: Vec<u64> = (0..5)
            .scan(SplitMix64::new(42), |r, _| Some(r.next_u64()))
            .collect();
        assert_eq!(a, b, "same seed reproduces the same sequence");
        let c: Vec<u64> = (0..5)
            .scan(SplitMix64::new(43), |r, _| Some(r.next_u64()))
            .collect();
        assert_ne!(a, c, "different seeds diverge");
    }

    fn histogram(n: usize, q: f64, draws: usize, seed: u64) -> Vec<u64> {
        let zipf = Zipf::new(n, q);
        let mut rng = SplitMix64::new(seed);
        let mut counts = vec![0u64; n];
        for _ in 0..draws {
            counts[zipf.sample(&mut rng)] += 1;
        }
        counts
    }

    #[test]
    fn samples_stay_in_range() {
        let zipf = Zipf::new(1000, 1.1);
        let mut rng = SplitMix64::new(7);
        for _ in 0..100_000 {
            assert!(zipf.sample(&mut rng) < 1000);
        }
    }

    #[test]
    fn single_element_population_always_zero() {
        let zipf = Zipf::new(1, 1.2);
        let mut rng = SplitMix64::new(1);
        for _ in 0..100 {
            assert_eq!(zipf.sample(&mut rng), 0);
        }
    }

    #[test]
    fn head_is_hotter_than_tail() {
        let counts = histogram(100, 1.2, 200_000, 99);
        // The most popular key dominates, and the head far outweighs the tail.
        assert!(counts[0] > counts[1], "rank 0 should beat rank 1");
        let head: u64 = counts[..10].iter().sum();
        let tail: u64 = counts[90..].iter().sum();
        assert!(
            head > tail * 10,
            "head {head} should dominate tail {tail} under skew 1.2"
        );
    }

    #[test]
    fn higher_skew_concentrates_more() {
        // The top-key share grows with the exponent.
        let low = histogram(1000, 0.8, 200_000, 5);
        let high = histogram(1000, 1.5, 200_000, 5);
        assert!(
            high[0] > low[0],
            "skew 1.5 top share {} should exceed skew 0.8 {}",
            high[0],
            low[0]
        );
    }

    #[test]
    fn covers_a_growing_distinct_set() {
        // Over many draws the distinct set grows (coupon-collector on the tail),
        // which is what drives the population growth in the benchmark.
        let counts = histogram(10_000, 1.1, 500_000, 3);
        let distinct = counts.iter().filter(|&&c| c > 0).count();
        assert!(distinct > 100, "expected broad coverage, got {distinct}");
    }
}
