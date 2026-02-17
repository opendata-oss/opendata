//! Simple 2-means clustering for centroid splitting.

use crate::distance;
use crate::serde::collection_meta::DistanceMetric;

/// Compute the mean vector of a set of vectors.
fn mean_vector(vectors: &[&[f32]], dimensions: usize) -> Vec<f32> {
    let mut mean = vec![0.0f32; dimensions];
    if vectors.is_empty() {
        return mean;
    }
    for v in vectors {
        for (i, &val) in v.iter().enumerate() {
            mean[i] += val;
        }
    }
    let n = vectors.len() as f32;
    for val in &mut mean {
        *val /= n;
    }
    mean
}

/// Split a set of vectors into two clusters using k-means with k=2.
///
/// Returns the two centroid vectors `(c0, c1)`.
///
/// # Arguments
/// * `vectors` - Slice of `(vector_id, vector_data)` tuples
/// * `dimensions` - Number of dimensions per vector
/// * `distance_metric` - Distance metric for assignment
///
/// # Panics
/// Panics if fewer than 2 vectors are provided.
#[allow(dead_code)]
pub(crate) fn two_means(
    vectors: &[(u64, &[f32])],
    dimensions: usize,
    distance_metric: DistanceMetric,
) -> (Vec<f32>, Vec<f32>) {
    assert!(vectors.len() >= 2, "two_means requires at least 2 vectors");

    // Initialize: pick the two farthest vectors as initial centroids (using L2)
    let mut best_dist: Option<distance::VectorDistance> = None;
    let mut idx_a = 0;
    let mut idx_b = 1;

    // For small sets, do full pairwise; for large sets, sample against first vector
    if vectors.len() <= 100 {
        for i in 0..vectors.len() {
            for j in (i + 1)..vectors.len() {
                let d = distance::compute_distance(vectors[i].1, vectors[j].1, DistanceMetric::L2);
                if best_dist.is_none_or(|best| d > best) {
                    best_dist = Some(d);
                    idx_a = i;
                    idx_b = j;
                }
            }
        }
    } else {
        // Heuristic: find farthest from first, then farthest from that
        let first = vectors[0].1;
        let mut farthest_idx = 1;
        let mut farthest_dist: Option<distance::VectorDistance> = None;
        for (i, (_, v)) in vectors.iter().enumerate().skip(1) {
            let d = distance::compute_distance(first, v, DistanceMetric::L2);
            if farthest_dist.is_none_or(|best| d > best) {
                farthest_dist = Some(d);
                farthest_idx = i;
            }
        }
        idx_a = farthest_idx;
        let pivot = vectors[idx_a].1;
        farthest_dist = None;
        for (i, (_, v)) in vectors.iter().enumerate() {
            if i == idx_a {
                continue;
            }
            let d = distance::compute_distance(pivot, v, DistanceMetric::L2);
            if farthest_dist.is_none_or(|best| d > best) {
                farthest_dist = Some(d);
                farthest_idx = i;
            }
        }
        idx_b = farthest_idx;
    }

    let mut c0 = vectors[idx_a].1.to_vec();
    let mut c1 = vectors[idx_b].1.to_vec();

    let max_iterations = 20;
    let mut prev_assignments: Vec<bool> = Vec::new(); // false = c0, true = c1

    for _ in 0..max_iterations {
        // Assign each vector to the closer centroid
        let assignments: Vec<bool> = vectors
            .iter()
            .map(|(_, v)| {
                let d0 = distance::compute_distance(v, &c0, distance_metric);
                let d1 = distance::compute_distance(v, &c1, distance_metric);
                d1 < d0 // true if d1 is more similar (closer to c1)
            })
            .collect();

        // Check convergence
        if assignments == prev_assignments {
            break;
        }
        prev_assignments = assignments.clone();

        // Recompute centroids
        let c0_vecs: Vec<&[f32]> = vectors
            .iter()
            .zip(assignments.iter())
            .filter(|(_, a)| !**a)
            .map(|((_, v), _)| *v)
            .collect();

        let c1_vecs: Vec<&[f32]> = vectors
            .iter()
            .zip(assignments.iter())
            .filter(|(_, a)| **a)
            .map(|((_, v), _)| *v)
            .collect();

        // Only update if cluster is non-empty
        if !c0_vecs.is_empty() {
            c0 = mean_vector(&c0_vecs, dimensions);
        }
        if !c1_vecs.is_empty() {
            c1 = mean_vector(&c1_vecs, dimensions);
        }
    }

    (c0, c1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_split_well_separated_clusters() {
        // given - two well-separated groups
        let vectors: Vec<(u64, Vec<f32>)> = vec![
            (1, vec![1.0, 0.0]),
            (2, vec![1.1, 0.1]),
            (3, vec![0.9, -0.1]),
            (4, vec![-1.0, 0.0]),
            (5, vec![-1.1, 0.1]),
            (6, vec![-0.9, -0.1]),
        ];
        let refs: Vec<(u64, &[f32])> = vectors.iter().map(|(id, v)| (*id, v.as_slice())).collect();

        // when
        let (c0, c1) = two_means(&refs, 2, DistanceMetric::L2);

        // then - centroids should be near [1, 0] and [-1, 0]
        let near_positive = (c0[0] > 0.5 && c1[0] < -0.5) || (c1[0] > 0.5 && c0[0] < -0.5);
        assert!(
            near_positive,
            "centroids should separate: c0={:?}, c1={:?}",
            c0, c1
        );
    }

    #[test]
    fn should_handle_two_vectors() {
        // given - minimum case
        let vectors: Vec<(u64, Vec<f32>)> = vec![(1, vec![0.0, 0.0]), (2, vec![10.0, 10.0])];
        let refs: Vec<(u64, &[f32])> = vectors.iter().map(|(id, v)| (*id, v.as_slice())).collect();

        // when
        let (c0, c1) = two_means(&refs, 2, DistanceMetric::L2);

        // then - each centroid should be at one of the input vectors
        assert!(
            (c0 == vec![0.0, 0.0] && c1 == vec![10.0, 10.0])
                || (c0 == vec![10.0, 10.0] && c1 == vec![0.0, 0.0]),
            "centroids should match inputs: c0={:?}, c1={:?}",
            c0,
            c1
        );
    }

    #[test]
    fn should_work_with_cosine_metric() {
        // given - vectors pointing in different directions
        let vectors: Vec<(u64, Vec<f32>)> = vec![
            (1, vec![1.0, 0.0, 0.0]),
            (2, vec![0.9, 0.1, 0.0]),
            (3, vec![0.0, 0.0, 1.0]),
            (4, vec![0.0, 0.1, 0.9]),
        ];
        let refs: Vec<(u64, &[f32])> = vectors.iter().map(|(id, v)| (*id, v.as_slice())).collect();

        // when
        let (c0, c1) = two_means(&refs, 3, DistanceMetric::Cosine);

        // then - centroids should separate into two groups
        assert_ne!(c0, c1, "centroids should be different");
    }

    #[test]
    #[should_panic(expected = "two_means requires at least 2 vectors")]
    fn should_panic_on_single_vector() {
        let vectors: Vec<(u64, Vec<f32>)> = vec![(1, vec![1.0])];
        let refs: Vec<(u64, &[f32])> = vectors.iter().map(|(id, v)| (*id, v.as_slice())).collect();
        two_means(&refs, 1, DistanceMetric::L2);
    }
}
