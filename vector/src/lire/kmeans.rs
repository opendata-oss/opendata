//! Simple k-means clustering for centroid splitting.

use rand::Rng;

use crate::distance;
use crate::serde::collection_meta::DistanceMetric;

/// Trait for clustering a set of vectors.
pub(crate) trait Clustering {
    fn k_means(&self, vectors: &[(u64, &[f32])], dimensions: usize, k: usize) -> Vec<Vec<f32>>;

    fn two_means(&self, vectors: &[(u64, &[f32])], dimensions: usize) -> (Vec<f32>, Vec<f32>);
}

/// K-means with L2-based initialization and arithmetic mean centroids.
pub(crate) struct KMeansPP {
    metric: DistanceMetric,
}

/// Create a clustering strategy appropriate for the given distance metric.
pub(crate) fn for_metric(metric: DistanceMetric) -> Box<dyn Clustering + Send> {
    Box::new(KMeansPP { metric })
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

fn farthest_pair_init(vectors: &[(u64, &[f32])], metric: DistanceMetric) -> Vec<usize> {
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
    vec![idx_a, idx_b]
}

/// K-means++ initialization using squared L2 distance.
fn kmeans_pp_init_l2(vectors: &[(u64, &[f32])], k: usize) -> Vec<usize> {
    assert!(k >= 1, "kmeans++ requires at least one centroid");
    let mut rng = rand::thread_rng();
    let mut centroids = vec![rng.gen_range(0..vectors.len())];

    while centroids.len() < k {
        let distances: Vec<f32> = vectors
            .iter()
            .enumerate()
            .map(|(idx, (_, v))| {
                if centroids.contains(&idx) {
                    return 0.0;
                }
                centroids
                    .iter()
                    .map(|&centroid_idx| {
                        v.iter()
                            .zip(vectors[centroid_idx].1.iter())
                            .map(|(a, b)| (a - b) * (a - b))
                            .sum::<f32>()
                    })
                    .fold(f32::INFINITY, f32::min)
            })
            .collect();
        let total: f32 = distances.iter().sum();

        let next_idx = if total > 0.0 {
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
        } else {
            (0..vectors.len())
                .find(|idx| !centroids.contains(idx))
                .unwrap_or(0)
        };
        centroids.push(next_idx);
    }

    centroids
}

/// Run the k-means loop: assign vectors to centroids, recompute centroids.
fn kmeans_loop(
    vectors: &[(u64, &[f32])],
    dimensions: usize,
    metric: DistanceMetric,
    centroids: &mut [Vec<f32>],
    update_centroid: fn(&mut [f32]),
) -> Vec<usize> {
    let max_iterations = 20;
    let mut assignments = vec![0usize; vectors.len()];
    let mut prev_assignments: Vec<usize> = Vec::new();

    for _ in 0..max_iterations {
        // Assign each vector to the closest centroid
        assignments = vectors
            .iter()
            .map(|(_, v)| {
                centroids
                    .iter()
                    .enumerate()
                    .map(|(idx, centroid)| (idx, distance::compute_distance(v, centroid, metric)))
                    .min_by(|a, b| a.1.cmp(&b.1))
                    .map(|(idx, _)| idx)
                    .expect("kmeans requires at least one centroid")
            })
            .collect();

        if assignments == prev_assignments {
            break;
        }
        prev_assignments = assignments.clone();

        // Recompute centroids
        let mut grouped: Vec<Vec<&[f32]>> = vec![Vec::new(); centroids.len()];
        for ((_, vector), &assignment) in vectors.iter().zip(assignments.iter()) {
            grouped[assignment].push(*vector);
        }

        for (centroid, assigned) in centroids.iter_mut().zip(grouped.iter()) {
            if assigned.is_empty() {
                continue;
            }
            *centroid = mean_vector(assigned, dimensions);
            update_centroid(centroid);
        }
    }

    assignments
}

/// Identity update — no post-processing after computing the mean.
fn identity(_v: &mut [f32]) {}

impl Clustering for KMeansPP {
    fn k_means(&self, vectors: &[(u64, &[f32])], dimensions: usize, k: usize) -> Vec<Vec<f32>> {
        assert!(
            vectors.len() >= k,
            "k_means requires at least k vectors: k={}, len={}",
            k,
            vectors.len()
        );

        let init = if k == 2 && vectors.len() <= 100 {
            farthest_pair_init(vectors, DistanceMetric::L2)
        } else {
            kmeans_pp_init_l2(vectors, k)
        };

        let mut centroids: Vec<Vec<f32>> = init
            .into_iter()
            .map(|idx| vectors[idx].1.to_vec())
            .collect();
        let assignments = kmeans_loop(vectors, dimensions, self.metric, &mut centroids, identity);

        // If a centroid ends up empty, replace it with an assigned vector to keep k outputs.
        let mut counts = vec![0usize; centroids.len()];
        for assignment in assignments {
            counts[assignment] += 1;
        }
        for (idx, count) in counts.into_iter().enumerate() {
            if count == 0 {
                centroids[idx] = vectors[idx].1.to_vec();
            }
        }

        centroids
    }

    fn two_means(&self, vectors: &[(u64, &[f32])], dimensions: usize) -> (Vec<f32>, Vec<f32>) {
        assert!(vectors.len() >= 2, "two_means requires at least 2 vectors");
        let mut centroids = self.k_means(vectors, dimensions, 2);
        let c1 = centroids.pop().expect("missing centroid");
        let c0 = centroids.pop().expect("missing centroid");
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
    #[should_panic(expected = "two_means requires at least 2 vectors")]
    fn should_panic_on_single_vector() {
        let vectors: Vec<(u64, Vec<f32>)> = vec![(1, vec![1.0])];
        let refs: Vec<(u64, &[f32])> = vectors.iter().map(|(id, v)| (*id, v.as_slice())).collect();
        for_metric(DistanceMetric::L2).two_means(&refs, 1);
    }

    #[test]
    fn should_produce_k_centroids() {
        let vectors: Vec<(u64, Vec<f32>)> = vec![
            (1, vec![1.0, 0.0]),
            (2, vec![1.1, 0.1]),
            (3, vec![0.9, -0.1]),
            (4, vec![-1.0, 0.0]),
            (5, vec![-1.1, 0.1]),
            (6, vec![-0.9, -0.1]),
        ];
        let refs: Vec<(u64, &[f32])> = vectors.iter().map(|(id, v)| (*id, v.as_slice())).collect();

        let centroids = for_metric(DistanceMetric::L2).k_means(&refs, 2, 3);

        assert_eq!(centroids.len(), 3);
    }
}
