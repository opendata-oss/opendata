//! Simple 2-means clustering for centroid splitting.

use rand::Rng;

use crate::distance;
use crate::serde::collection_meta::DistanceMetric;

/// Trait for splitting a set of vectors into two clusters.
#[allow(dead_code)]
pub(crate) trait Clustering {
    fn two_means(&self, vectors: &[(u64, &[f32])], dimensions: usize) -> (Vec<f32>, Vec<f32>);
}

/// K-means with L2-based initialization and arithmetic mean centroids.
/// Suitable for L2 and DotProduct metrics.
#[allow(dead_code)]
pub(crate) struct KMeansPP {
    metric: DistanceMetric,
}

/// Spherical k-means: cosine-aware initialization and L2-normalized centroids.
/// Suitable for Cosine metric.
#[allow(dead_code)]
pub(crate) struct SphericalKMeans;

/// Create a clustering strategy appropriate for the given distance metric.
#[allow(dead_code)]
pub(crate) fn for_metric(metric: DistanceMetric) -> Box<dyn Clustering + Send> {
    match metric {
        DistanceMetric::Cosine => Box::new(SphericalKMeans),
        _ => Box::new(KMeansPP { metric }),
    }
}

/// Compute the arithmetic mean vector of a set of vectors.
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

/// L2-normalize a vector in place.
fn normalize(v: &mut [f32]) {
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        v.iter_mut().for_each(|x| *x /= norm);
    }
}

/// Farthest-pair initialization using a given distance metric.
/// Finds the two most distant vectors by exhaustive pairwise comparison.
fn farthest_pair_init(vectors: &[(u64, &[f32])], metric: DistanceMetric) -> (usize, usize) {
    let mut best_dist: Option<distance::VectorDistance> = None;
    let mut idx_a = 0;
    let mut idx_b = 1;
    for i in 0..vectors.len() {
        for j in (i + 1)..vectors.len() {
            let d = distance::compute_distance(vectors[i].1, vectors[j].1, metric);
            if best_dist.is_none_or(|best| d > best) {
                best_dist = Some(d);
                idx_a = i;
                idx_b = j;
            }
        }
    }
    (idx_a, idx_b)
}

/// K-means++ initialization using squared L2 distance.
/// Picks first centroid randomly, then picks second with probability proportional
/// to squared L2 distance from the first.
fn kmeans_pp_init_l2(vectors: &[(u64, &[f32])]) -> (usize, usize) {
    let mut rng = rand::thread_rng();
    let idx_a = rng.gen_range(0..vectors.len());

    let distances: Vec<f32> = vectors
        .iter()
        .map(|(_, v)| {
            v.iter()
                .zip(vectors[idx_a].1.iter())
                .map(|(a, b)| (a - b) * (a - b))
                .sum()
        })
        .collect();
    let total: f32 = distances.iter().sum();

    let idx_b = if total > 0.0 {
        let threshold = rng.gen_range(0.0..total);
        let mut cumsum = 0.0;
        let mut chosen = 0;
        for (i, &d) in distances.iter().enumerate() {
            cumsum += d;
            if cumsum >= threshold {
                chosen = i;
                break;
            }
        }
        chosen
    } else if idx_a == 0 {
        1
    } else {
        0
    };

    (idx_a, idx_b)
}

/// K-means++ initialization using cosine dissimilarity (1 - cosine_similarity).
/// Picks first centroid randomly, then picks second with probability proportional
/// to `1.0 - cosine_similarity` from the first.
fn kmeans_pp_init_cosine(vectors: &[(u64, &[f32])]) -> (usize, usize) {
    let mut rng = rand::thread_rng();
    let idx_a = rng.gen_range(0..vectors.len());

    let distances: Vec<f32> = vectors
        .iter()
        .map(|(_, v)| {
            let dot: f32 = v
                .iter()
                .zip(vectors[idx_a].1.iter())
                .map(|(a, b)| a * b)
                .sum();
            let norm_v: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            let norm_a: f32 = vectors[idx_a].1.iter().map(|x| x * x).sum::<f32>().sqrt();
            let sim = if norm_v > 0.0 && norm_a > 0.0 {
                dot / (norm_v * norm_a)
            } else {
                0.0
            };
            (1.0 - sim).max(0.0)
        })
        .collect();
    let total: f32 = distances.iter().sum();

    let idx_b = if total > 0.0 {
        let threshold = rng.gen_range(0.0..total);
        let mut cumsum = 0.0;
        let mut chosen = 0;
        for (i, &d) in distances.iter().enumerate() {
            cumsum += d;
            if cumsum >= threshold {
                chosen = i;
                break;
            }
        }
        chosen
    } else if idx_a == 0 {
        1
    } else {
        0
    };

    (idx_a, idx_b)
}

/// Run the k-means loop: assign vectors to centroids, recompute centroids.
/// `update_centroid` is called on each centroid after computing the arithmetic mean.
fn kmeans_loop(
    vectors: &[(u64, &[f32])],
    dimensions: usize,
    metric: DistanceMetric,
    c0: &mut Vec<f32>,
    c1: &mut Vec<f32>,
    update_centroid: fn(&mut [f32]),
) {
    let max_iterations = 20;
    let mut prev_assignments: Vec<bool> = Vec::new();

    for _ in 0..max_iterations {
        // Assign each vector to the closer centroid
        let assignments: Vec<bool> = vectors
            .iter()
            .map(|(_, v)| {
                let d0 = distance::compute_distance(v, c0, metric);
                let d1 = distance::compute_distance(v, c1, metric);
                d1 < d0
            })
            .collect();

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

        if !c0_vecs.is_empty() {
            *c0 = mean_vector(&c0_vecs, dimensions);
            update_centroid(c0);
        }
        if !c1_vecs.is_empty() {
            *c1 = mean_vector(&c1_vecs, dimensions);
            update_centroid(c1);
        }
    }
}

/// Identity update — no post-processing after computing the mean.
fn identity(_v: &mut [f32]) {}

/// Normalize the centroid vector to unit length.
fn normalize_centroid(v: &mut [f32]) {
    normalize(v);
}

impl Clustering for KMeansPP {
    #[allow(dead_code)]
    fn two_means(&self, vectors: &[(u64, &[f32])], dimensions: usize) -> (Vec<f32>, Vec<f32>) {
        assert!(vectors.len() >= 2, "two_means requires at least 2 vectors");

        let (idx_a, idx_b) = if vectors.len() <= 100 {
            farthest_pair_init(vectors, DistanceMetric::L2)
        } else {
            kmeans_pp_init_l2(vectors)
        };

        let mut c0 = vectors[idx_a].1.to_vec();
        let mut c1 = vectors[idx_b].1.to_vec();

        kmeans_loop(vectors, dimensions, self.metric, &mut c0, &mut c1, identity);

        (c0, c1)
    }
}

impl Clustering for SphericalKMeans {
    #[allow(dead_code)]
    fn two_means(&self, vectors: &[(u64, &[f32])], dimensions: usize) -> (Vec<f32>, Vec<f32>) {
        assert!(vectors.len() >= 2, "two_means requires at least 2 vectors");

        let (idx_a, idx_b) = if vectors.len() <= 100 {
            farthest_pair_init(vectors, DistanceMetric::Cosine)
        } else {
            kmeans_pp_init_cosine(vectors)
        };

        let mut c0 = vectors[idx_a].1.to_vec();
        let mut c1 = vectors[idx_b].1.to_vec();

        kmeans_loop(
            vectors,
            dimensions,
            DistanceMetric::Cosine,
            &mut c0,
            &mut c1,
            normalize_centroid,
        );

        (c0, c1)
    }
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
        let clustering = for_metric(DistanceMetric::L2);
        let (c0, c1) = clustering.two_means(&refs, 2);

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
        let (c0, c1) = KMeansPP {
            metric: DistanceMetric::L2,
        }
        .two_means(&refs, 2);

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
        let clustering = for_metric(DistanceMetric::Cosine);
        let (c0, c1) = clustering.two_means(&refs, 3);

        // then - centroids should separate into two groups
        assert_ne!(c0, c1, "centroids should be different");
    }

    #[test]
    fn should_produce_normalized_centroids_for_spherical_kmeans() {
        // given - vectors in different directions (not unit length)
        let vectors: Vec<(u64, Vec<f32>)> = vec![
            (1, vec![3.0, 0.0, 0.0]),
            (2, vec![2.7, 0.3, 0.0]),
            (3, vec![0.0, 0.0, 5.0]),
            (4, vec![0.0, 0.5, 4.5]),
        ];
        let refs: Vec<(u64, &[f32])> = vectors.iter().map(|(id, v)| (*id, v.as_slice())).collect();

        // when
        let (c0, c1) = SphericalKMeans.two_means(&refs, 3);

        // then - both centroids should be approximately unit length
        let norm_c0: f32 = c0.iter().map(|x| x * x).sum::<f32>().sqrt();
        let norm_c1: f32 = c1.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!(
            (norm_c0 - 1.0).abs() < 1e-5,
            "c0 should be unit length, got norm {}",
            norm_c0
        );
        assert!(
            (norm_c1 - 1.0).abs() < 1e-5,
            "c1 should be unit length, got norm {}",
            norm_c1
        );
    }

    #[test]
    #[should_panic(expected = "two_means requires at least 2 vectors")]
    fn should_panic_on_single_vector() {
        let vectors: Vec<(u64, Vec<f32>)> = vec![(1, vec![1.0])];
        let refs: Vec<(u64, &[f32])> = vectors.iter().map(|(id, v)| (*id, v.as_slice())).collect();
        for_metric(DistanceMetric::L2).two_means(&refs, 1);
    }
}
