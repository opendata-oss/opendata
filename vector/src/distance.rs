//! Distance computation functions for vector similarity.
//!
//! This module provides distance and similarity functions used for scoring
//! candidates during similarity search.

use crate::serde::collection_meta::DistanceMetric;
#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::{
    _mm256_add_ps, _mm256_fmadd_ps, _mm256_loadu_ps, _mm256_mul_ps, _mm256_setzero_ps,
    _mm256_storeu_ps, _mm256_sub_ps,
};
#[cfg(all(target_arch = "x86_64", feature = "avx512"))]
use std::arch::x86_64::{
    _mm512_add_ps, _mm512_fmadd_ps, _mm512_loadu_ps, _mm512_mul_ps, _mm512_setzero_ps,
    _mm512_storeu_ps, _mm512_sub_ps,
};
use std::cmp::Ordering;

/// Compute distance/similarity between two vectors.
///
/// # Arguments
/// * `a` - First vector
/// * `b` - Second vector
/// * `metric` - Distance metric to use
///
/// # Returns
/// Distance/similarity score. Higher scores indicate more similar vectors,
/// except for L2 distance where lower scores indicate more similar vectors.
///
/// # Panics
/// Panics if the vectors have different lengths.
pub(crate) fn compute_distance(a: &[f32], b: &[f32], metric: DistanceMetric) -> VectorDistance {
    assert_eq!(
        a.len(),
        b.len(),
        "Cannot compute distance between vectors of different lengths"
    );

    let v = match metric {
        DistanceMetric::L2 => l2_distance(a, b),
        DistanceMetric::DotProduct => dot_product(a, b),
    };
    VectorDistance { score: v, metric }
}

/// Compute a uniform distance where lower = closer, suitable for comparing
/// across distance metrics in the boundary replication formula.
pub(crate) fn raw_distance(a: &[f32], b: &[f32], metric: DistanceMetric) -> f32 {
    match metric {
        DistanceMetric::L2 => compute_distance(a, b, metric).score(),
        DistanceMetric::DotProduct => -compute_distance(a, b, metric).score(),
    }
}

/// Compute squared L2 distance between two vectors.
///
/// Formula: sum((a[i] - b[i])²)
///
/// Lower scores indicate more similar vectors.
fn l2_distance(a: &[f32], b: &[f32]) -> f32 {
    #[cfg(all(target_arch = "x86_64", feature = "avx512"))]
    {
        if std::is_x86_feature_detected!("avx512f") {
            // SAFETY: AVX-512F support is checked at runtime before calling the AVX-512 variant.
            return unsafe { l2_distance_avx512(a, b) };
        }
    }

    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx") && std::is_x86_feature_detected!("fma") {
            // SAFETY: AVX and FMA support are checked at runtime before calling the FMA variant.
            return unsafe { l2_distance_avx_fma(a, b) };
        }

        if std::is_x86_feature_detected!("avx") {
            // SAFETY: AVX support is checked at runtime before calling the AVX variant.
            return unsafe { l2_distance_avx(a, b) };
        }
    }

    l2_distance_scalar(a, b)
}

fn l2_distance_scalar(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| (x - y).powi(2)).sum()
}

/// Compute dot product between two vectors.
///
/// Formula: sum(a[i] * b[i])
///
/// Higher scores indicate more similar vectors (for normalized vectors).
fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    #[cfg(all(target_arch = "x86_64", feature = "avx512"))]
    {
        if std::is_x86_feature_detected!("avx512f") {
            // SAFETY: AVX-512F support is checked at runtime before calling the AVX-512 variant.
            return unsafe { dot_product_avx512(a, b) };
        }
    }

    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx") {
            // SAFETY: AVX support is checked at runtime before calling the AVX variant.
            return unsafe { dot_product_avx(a, b) };
        }
    }

    dot_product_scalar(a, b)
}

fn dot_product_scalar(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}

#[cfg(feature = "bench-internals")]
#[doc(hidden)]
pub fn bench_l2_distance_scalar(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(
        a.len(),
        b.len(),
        "Cannot compute distance between vectors of different lengths"
    );

    l2_distance_scalar(a, b)
}

#[cfg(feature = "bench-internals")]
#[doc(hidden)]
pub fn bench_l2_distance_runtime(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(
        a.len(),
        b.len(),
        "Cannot compute distance between vectors of different lengths"
    );

    l2_distance(a, b)
}

#[allow(clippy::needless_return)]
#[cfg(feature = "bench-internals")]
#[doc(hidden)]
pub fn bench_l2_distance_avx_available() -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        return std::is_x86_feature_detected!("avx");
    }

    #[cfg(not(target_arch = "x86_64"))]
    {
        false
    }
}

#[cfg(feature = "bench-internals")]
#[doc(hidden)]
pub fn bench_l2_distance_avx(a: &[f32], b: &[f32]) -> Option<f32> {
    assert_eq!(
        a.len(),
        b.len(),
        "Cannot compute distance between vectors of different lengths"
    );

    #[cfg(all(target_arch = "x86_64", feature = "avx512"))]
    {
        if std::is_x86_feature_detected!("avx512f") {
            // SAFETY: AVX-512F support is checked at runtime before calling the AVX-512 variant.
            return Some(unsafe { l2_distance_avx512(a, b) });
        }
    }

    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx") && std::is_x86_feature_detected!("fma") {
            // SAFETY: AVX and FMA support are checked at runtime before calling the FMA variant.
            return Some(unsafe { l2_distance_avx_fma(a, b) });
        }

        if std::is_x86_feature_detected!("avx") {
            // SAFETY: AVX support is checked at runtime before calling the AVX variant.
            return Some(unsafe { l2_distance_avx(a, b) });
        }
    }

    None
}

#[cfg(all(target_arch = "x86_64", feature = "avx512"))]
#[target_feature(enable = "avx512f")]
unsafe fn l2_distance_avx512(a: &[f32], b: &[f32]) -> f32 {
    let mut acc = _mm512_setzero_ps();
    let mut idx = 0;
    let len = a.len();

    while idx + 16 <= len {
        // SAFETY: `idx + 16 <= len` guarantees each load reads 16 initialized `f32`s.
        let lhs = unsafe { _mm512_loadu_ps(a.as_ptr().add(idx)) };
        // SAFETY: `idx + 16 <= len` guarantees each load reads 16 initialized `f32`s.
        let rhs = unsafe { _mm512_loadu_ps(b.as_ptr().add(idx)) };
        let diff = _mm512_sub_ps(lhs, rhs);
        acc = _mm512_fmadd_ps(diff, diff, acc);
        idx += 16;
    }

    let mut lanes = [0.0; 16];
    // SAFETY: `lanes` is a valid contiguous buffer for 16 `f32`s.
    unsafe { _mm512_storeu_ps(lanes.as_mut_ptr(), acc) };

    let mut sum = lanes.into_iter().sum::<f32>();
    for (x, y) in a[idx..].iter().zip(&b[idx..]) {
        let diff = x - y;
        sum += diff * diff;
    }

    sum
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx,fma")]
unsafe fn l2_distance_avx_fma(a: &[f32], b: &[f32]) -> f32 {
    let mut acc = _mm256_setzero_ps();
    let mut idx = 0;
    let len = a.len();

    while idx + 8 <= len {
        // SAFETY: `idx + 8 <= len` guarantees each load reads 8 initialized `f32`s.
        let lhs = unsafe { _mm256_loadu_ps(a.as_ptr().add(idx)) };
        // SAFETY: `idx + 8 <= len` guarantees each load reads 8 initialized `f32`s.
        let rhs = unsafe { _mm256_loadu_ps(b.as_ptr().add(idx)) };
        let diff = _mm256_sub_ps(lhs, rhs);
        acc = _mm256_fmadd_ps(diff, diff, acc);
        idx += 8;
    }

    let mut lanes = [0.0; 8];
    // SAFETY: `lanes` is a valid contiguous buffer for 8 `f32`s.
    unsafe { _mm256_storeu_ps(lanes.as_mut_ptr(), acc) };

    let mut sum = lanes.into_iter().sum::<f32>();
    for (x, y) in a[idx..].iter().zip(&b[idx..]) {
        let diff = x - y;
        sum += diff * diff;
    }

    sum
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx")]
unsafe fn l2_distance_avx(a: &[f32], b: &[f32]) -> f32 {
    let mut acc = _mm256_setzero_ps();
    let mut idx = 0;
    let len = a.len();

    while idx + 8 <= len {
        // SAFETY: `idx + 8 <= len` guarantees each load reads 8 initialized `f32`s.
        let lhs = unsafe { _mm256_loadu_ps(a.as_ptr().add(idx)) };
        // SAFETY: `idx + 8 <= len` guarantees each load reads 8 initialized `f32`s.
        let rhs = unsafe { _mm256_loadu_ps(b.as_ptr().add(idx)) };
        let diff = _mm256_sub_ps(lhs, rhs);
        acc = _mm256_add_ps(acc, _mm256_mul_ps(diff, diff));
        idx += 8;
    }

    let mut lanes = [0.0; 8];
    // SAFETY: `lanes` is a valid contiguous buffer for 8 `f32`s.
    unsafe { _mm256_storeu_ps(lanes.as_mut_ptr(), acc) };

    let mut sum = lanes.into_iter().sum::<f32>();
    for (x, y) in a[idx..].iter().zip(&b[idx..]) {
        let diff = x - y;
        sum += diff * diff;
    }

    sum
}

#[cfg(all(target_arch = "x86_64", feature = "avx512"))]
#[target_feature(enable = "avx512f")]
unsafe fn dot_product_avx512(a: &[f32], b: &[f32]) -> f32 {
    let mut acc = _mm512_setzero_ps();
    let mut idx = 0;
    let len = a.len();

    while idx + 16 <= len {
        // SAFETY: `idx + 16 <= len` guarantees each load reads 16 initialized `f32`s.
        let lhs = unsafe { _mm512_loadu_ps(a.as_ptr().add(idx)) };
        // SAFETY: `idx + 16 <= len` guarantees each load reads 16 initialized `f32`s.
        let rhs = unsafe { _mm512_loadu_ps(b.as_ptr().add(idx)) };
        acc = _mm512_add_ps(acc, _mm512_mul_ps(lhs, rhs));
        idx += 16;
    }

    let mut lanes = [0.0; 16];
    // SAFETY: `lanes` is a valid contiguous buffer for 16 `f32`s.
    unsafe { _mm512_storeu_ps(lanes.as_mut_ptr(), acc) };

    let mut sum = lanes.into_iter().sum::<f32>();
    for (x, y) in a[idx..].iter().zip(&b[idx..]) {
        sum += x * y;
    }

    sum
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx")]
unsafe fn dot_product_avx(a: &[f32], b: &[f32]) -> f32 {
    let mut acc = _mm256_setzero_ps();
    let mut idx = 0;
    let len = a.len();

    while idx + 8 <= len {
        // SAFETY: `idx + 8 <= len` guarantees each load reads 8 initialized `f32`s.
        let lhs = unsafe { _mm256_loadu_ps(a.as_ptr().add(idx)) };
        // SAFETY: `idx + 8 <= len` guarantees each load reads 8 initialized `f32`s.
        let rhs = unsafe { _mm256_loadu_ps(b.as_ptr().add(idx)) };
        acc = _mm256_add_ps(acc, _mm256_mul_ps(lhs, rhs));
        idx += 8;
    }

    let mut lanes = [0.0; 8];
    // SAFETY: `lanes` is a valid contiguous buffer for 8 `f32`s.
    unsafe { _mm256_storeu_ps(lanes.as_mut_ptr(), acc) };

    let mut sum = lanes.into_iter().sum::<f32>();
    for (x, y) in a[idx..].iter().zip(&b[idx..]) {
        sum += x * y;
    }

    sum
}

/// A distance/similarity score between two vectors, with metric-aware ordering.
///
/// Ordering is defined so that `a < b` means `a` is **more similar** than `b`.
/// This abstracts over the direction of each metric:
/// - L2: lower raw value = more similar (natural order)
/// - DotProduct: higher raw value = more similar (reversed order)
#[derive(Copy, Clone, Debug)]
pub(crate) struct VectorDistance {
    score: f32,
    metric: DistanceMetric,
}

impl VectorDistance {
    /// Returns the raw distance/similarity value.
    pub(crate) fn score(&self) -> f32 {
        self.score
    }
}

impl PartialEq for VectorDistance {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for VectorDistance {}

impl PartialOrd for VectorDistance {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for VectorDistance {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.metric {
            // L2: lower value = more similar, so natural order
            DistanceMetric::L2 => self.score.total_cmp(&other.score),
            // DotProduct: higher value = more similar, so reverse order
            DistanceMetric::DotProduct => other.score.total_cmp(&self.score),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    // Parameterized tests for distance functions
    #[rstest]
    #[case(vec![1.0, 2.0, 3.0], vec![4.0, 5.0, 6.0], 27.0, "different vectors")]
    #[case(vec![1.0, 2.0, 3.0], vec![1.0, 2.0, 3.0], 0.0, "identical vectors")]
    fn should_compute_l2_distance(
        #[case] a: Vec<f32>,
        #[case] b: Vec<f32>,
        #[case] expected: f32,
        #[case] _desc: &str,
    ) {
        // when
        let distance = l2_distance(&a, &b);

        // then
        assert!((distance - expected).abs() < 0.01);
    }

    #[rstest]
    #[case(vec![1.0, 2.0, 3.0], vec![4.0, 5.0, 6.0], 32.0, "normal vectors")]
    #[case(vec![1.0, 0.0], vec![0.0, 1.0], 0.0, "orthogonal vectors")]
    fn should_compute_dot_product(
        #[case] a: Vec<f32>,
        #[case] b: Vec<f32>,
        #[case] expected: f32,
        #[case] _desc: &str,
    ) {
        // when
        let dot = dot_product(&a, &b);

        // then
        assert_eq!(dot, expected);
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn should_match_scalar_l2_distance_with_avx() {
        if !std::is_x86_feature_detected!("avx") {
            return;
        }

        // given
        let a: Vec<f32> = (0..23).map(|i| (i as f32 * 0.25) - 2.0).collect();
        let b: Vec<f32> = (0..23).map(|i| ((i % 7) as f32 * 1.5) - 1.0).collect();

        // when
        let scalar = l2_distance_scalar(&a, &b);
        // SAFETY: AVX support is checked above for this test process.
        let avx = unsafe { l2_distance_avx(&a, &b) };

        // then
        assert!((scalar - avx).abs() < 1e-5);
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn should_match_scalar_l2_distance_with_avx_fma() {
        if !(std::is_x86_feature_detected!("avx") && std::is_x86_feature_detected!("fma")) {
            return;
        }

        // given
        let a: Vec<f32> = (0..29).map(|i| (i as f32 * 0.25) - 2.0).collect();
        let b: Vec<f32> = (0..29).map(|i| ((i % 7) as f32 * 1.5) - 1.0).collect();

        // when
        let scalar = l2_distance_scalar(&a, &b);
        // SAFETY: AVX and FMA support are checked above for this test process.
        let avx_fma = unsafe { l2_distance_avx_fma(&a, &b) };

        // then
        assert!((scalar - avx_fma).abs() < 1e-5);
    }

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn should_match_scalar_dot_product_with_avx() {
        if !std::is_x86_feature_detected!("avx") {
            return;
        }

        // given
        let a: Vec<f32> = (0..17).map(|i| (i as f32 * 0.5) - 4.0).collect();
        let b: Vec<f32> = (0..17).map(|i| ((i % 5) as f32 * 0.75) + 0.5).collect();

        // when
        let scalar = dot_product_scalar(&a, &b);
        // SAFETY: AVX support is checked above for this test process.
        let avx = unsafe { dot_product_avx(&a, &b) };

        // then
        assert!((scalar - avx).abs() < 1e-5);
    }

    #[cfg(all(target_arch = "x86_64", feature = "avx512"))]
    #[test]
    fn should_match_scalar_l2_distance_with_avx512() {
        if !std::is_x86_feature_detected!("avx512f") {
            return;
        }

        // given
        let a: Vec<f32> = (0..37).map(|i| (i as f32 * 0.25) - 2.0).collect();
        let b: Vec<f32> = (0..37).map(|i| ((i % 11) as f32 * 1.5) - 1.0).collect();

        // when
        let scalar = l2_distance_scalar(&a, &b);
        // SAFETY: AVX-512F support is checked above for this test process.
        let avx512 = unsafe { l2_distance_avx512(&a, &b) };

        // then
        assert!((scalar - avx512).abs() < 1e-5);
    }

    #[cfg(all(target_arch = "x86_64", feature = "avx512"))]
    #[test]
    fn should_match_scalar_dot_product_with_avx512() {
        if !std::is_x86_feature_detected!("avx512f") {
            return;
        }

        // given
        let a: Vec<f32> = (0..41).map(|i| (i as f32 * 0.5) - 4.0).collect();
        let b: Vec<f32> = (0..41).map(|i| ((i % 9) as f32 * 0.75) + 0.5).collect();

        // when
        let scalar = dot_product_scalar(&a, &b);
        // SAFETY: AVX-512F support is checked above for this test process.
        let avx512 = unsafe { dot_product_avx512(&a, &b) };

        // then
        assert!((scalar - avx512).abs() < 1e-5);
    }

    #[rstest]
    #[case(DistanceMetric::L2, "L2")]
    #[case(DistanceMetric::DotProduct, "DotProduct")]
    fn should_use_correct_metric(#[case] metric: DistanceMetric, #[case] _desc: &str) {
        // given
        let a = vec![1.0, 2.0];
        let b = vec![3.0, 4.0];

        // when
        let result = compute_distance(&a, &b, metric);

        // then - verify result matches direct function call
        let expected = match metric {
            DistanceMetric::L2 => l2_distance(&a, &b),
            DistanceMetric::DotProduct => dot_product(&a, &b),
        };
        assert_eq!(result.score(), expected);
    }

    #[test]
    #[should_panic(expected = "Cannot compute distance between vectors of different lengths")]
    fn should_panic_on_mismatched_dimensions() {
        // given - vectors with different lengths
        let a = vec![1.0, 2.0];
        let b = vec![1.0, 2.0, 3.0];

        // when - attempt to compute distance
        compute_distance(&a, &b, DistanceMetric::L2);

        // then - should panic
    }

    // ---- VectorDistance ordering ----

    #[test]
    fn should_order_l2_by_lower_is_more_similar() {
        // given
        let closer = compute_distance(&[0.0, 0.0], &[1.0, 0.0], DistanceMetric::L2);
        let farther = compute_distance(&[0.0, 0.0], &[3.0, 0.0], DistanceMetric::L2);

        // then - closer (lower L2) should be "less than" farther
        assert!(closer < farther);
        assert!(farther > closer);
        assert_ne!(closer, farther);
    }

    #[test]
    fn should_order_dot_product_by_higher_is_more_similar() {
        // given
        let more_similar = compute_distance(&[3.0, 0.0], &[2.0, 0.0], DistanceMetric::DotProduct);
        let less_similar = compute_distance(&[3.0, 0.0], &[0.0, 2.0], DistanceMetric::DotProduct);

        // then - higher dot product should be "less than" (more similar)
        assert!(more_similar < less_similar);
    }

    #[test]
    fn should_consider_equal_distances_equal() {
        // given
        let d1 = compute_distance(&[1.0, 0.0], &[0.0, 1.0], DistanceMetric::L2);
        let d2 = compute_distance(&[0.0, 1.0], &[1.0, 0.0], DistanceMetric::L2);

        // then
        assert_eq!(d1, d2);
    }

    #[test]
    fn should_sort_vector_distances_most_similar_first() {
        // given - three L2 distances
        let d_far = compute_distance(&[0.0], &[10.0], DistanceMetric::L2);
        let d_mid = compute_distance(&[0.0], &[5.0], DistanceMetric::L2);
        let d_near = compute_distance(&[0.0], &[1.0], DistanceMetric::L2);
        let mut distances = [d_far, d_mid, d_near];

        // when
        distances.sort();

        // then - most similar (nearest) first
        assert_eq!(distances[0].score(), d_near.score());
        assert_eq!(distances[1].score(), d_mid.score());
        assert_eq!(distances[2].score(), d_far.score());
    }
}
