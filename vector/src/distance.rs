//! Distance computation functions for vector similarity.
//!
//! This module provides distance and similarity functions used for scoring
//! candidates during similarity search.

use crate::serde::collection_meta::DistanceMetric;

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
pub fn compute_distance(a: &[f32], b: &[f32], metric: DistanceMetric) -> f32 {
    assert_eq!(
        a.len(),
        b.len(),
        "Cannot compute distance between vectors of different lengths"
    );

    match metric {
        DistanceMetric::L2 => l2_distance(a, b),
        DistanceMetric::Cosine => cosine_similarity(a, b),
        DistanceMetric::DotProduct => dot_product(a, b),
    }
}

/// Compute L2 (Euclidean) distance between two vectors.
///
/// Formula: sqrt(sum((a[i] - b[i])²))
///
/// Lower scores indicate more similar vectors.
fn l2_distance(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| (x - y).powi(2))
        .sum::<f32>()
        .sqrt()
}

/// Compute cosine similarity between two vectors.
///
/// Formula: dot(a, b) / (||a|| * ||b||)
///
/// Higher scores indicate more similar vectors.
/// Returns 0 if either vector has zero magnitude.
fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x.powi(2)).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x.powi(2)).sum::<f32>().sqrt();

    if norm_a == 0.0 || norm_b == 0.0 {
        0.0
    } else {
        dot / (norm_a * norm_b)
    }
}

/// Compute dot product between two vectors.
///
/// Formula: sum(a[i] * b[i])
///
/// Higher scores indicate more similar vectors (for normalized vectors).
fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    // Parameterized tests for distance functions
    #[rstest]
    #[case(vec![1.0, 2.0, 3.0], vec![4.0, 5.0, 6.0], 5.196, "different vectors")]
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
    #[case(vec![1.0, 0.0, 0.0], vec![1.0, 0.0, 0.0], 1.0, "identical vectors")]
    #[case(vec![1.0, 0.0], vec![0.0, 1.0], 0.0, "orthogonal vectors")]
    #[case(vec![1.0, 0.0], vec![-1.0, 0.0], -1.0, "opposite vectors")]
    #[case(vec![1.0, 2.0], vec![0.0, 0.0], 0.0, "zero vector")]
    fn should_compute_cosine_similarity(
        #[case] a: Vec<f32>,
        #[case] b: Vec<f32>,
        #[case] expected: f32,
        #[case] _desc: &str,
    ) {
        // when
        let similarity = cosine_similarity(&a, &b);

        // then
        assert!((similarity - expected).abs() < 0.0001);
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

    #[rstest]
    #[case(DistanceMetric::L2, "L2")]
    #[case(DistanceMetric::Cosine, "Cosine")]
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
            DistanceMetric::Cosine => cosine_similarity(&a, &b),
            DistanceMetric::DotProduct => dot_product(&a, &b),
        };
        assert_eq!(result, expected);
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
}
