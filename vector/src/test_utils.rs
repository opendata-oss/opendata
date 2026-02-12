//! Test utilities for vector database testing.
//!
//! This module provides helper functions for generating test data and
//! setting up test databases.

#[cfg(test)]
use crate::serde::centroid_chunk::CentroidEntry;
#[cfg(test)]
use rand::Rng;

/// Generate random unit vectors for testing.
///
/// # Arguments
/// * `count` - Number of vectors to generate
/// * `dimensions` - Dimensionality of each vector
///
/// # Returns
/// Vector of randomly generated unit vectors
#[cfg(test)]
pub fn random_unit_vectors(count: usize, dimensions: usize) -> Vec<Vec<f32>> {
    use rand::Rng;
    let mut rng = rand::thread_rng();

    (0..count)
        .map(|_| {
            let mut vec: Vec<f32> = (0..dimensions).map(|_| rng.gen_range(-1.0..1.0)).collect();

            // Normalize to unit vector
            let norm: f32 = vec.iter().map(|x| x * x).sum::<f32>().sqrt();
            if norm > 0.0 {
                vec.iter_mut().for_each(|x| *x /= norm);
            }

            vec
        })
        .collect()
}

/// Generate centroids using k-means++ initialization.
///
/// This is a simplified k-means++ that just picks well-distributed initial centroids
/// without running full k-means clustering. Good enough for testing.
///
/// # Arguments
/// * `vectors` - Input vectors to cluster
/// * `k` - Number of centroids to generate
///
/// # Returns
/// Vector of k centroid vectors
#[cfg(test)]
pub fn generate_centroids(vectors: &[Vec<f32>], k: usize) -> Vec<Vec<f32>> {
    use rand::seq::SliceRandom;
    use rand::thread_rng;

    if vectors.is_empty() || k == 0 {
        return Vec::new();
    }

    let mut rng = thread_rng();
    let mut centroids = Vec::new();

    // Pick first centroid randomly
    centroids.push(vectors.choose(&mut rng).unwrap().clone());

    // Pick remaining centroids using k-means++ logic
    while centroids.len() < k && centroids.len() < vectors.len() {
        // For each vector, compute distance to nearest existing centroid
        let distances: Vec<f32> = vectors
            .iter()
            .map(|v| {
                centroids
                    .iter()
                    .map(|c| {
                        // L2 distance
                        v.iter()
                            .zip(c.iter())
                            .map(|(a, b)| (a - b).powi(2))
                            .sum::<f32>()
                    })
                    .fold(f32::INFINITY, f32::min)
            })
            .collect();

        // Pick next centroid with probability proportional to distance²
        let total: f32 = distances.iter().sum();
        if total == 0.0 {
            break; // All vectors are identical
        }

        let mut choice = rng.gen_range(0.0..total);
        for (i, &dist) in distances.iter().enumerate() {
            choice -= dist;
            if choice <= 0.0 {
                centroids.push(vectors[i].clone());
                break;
            }
        }
    }

    centroids
}

/// Create centroid entries with sequential IDs.
///
/// # Arguments
/// * `vectors` - Centroid vectors
///
/// # Returns
/// Vector of CentroidEntry with IDs starting from 1
#[cfg(test)]
pub fn create_centroid_entries(vectors: Vec<Vec<f32>>) -> Vec<CentroidEntry> {
    vectors
        .into_iter()
        .enumerate()
        .map(|(i, vec)| CentroidEntry::new((i + 1) as u64, vec))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_generate_random_unit_vectors() {
        // given
        let count = 10;
        let dimensions = 128;

        // when
        let vectors = random_unit_vectors(count, dimensions);

        // then
        assert_eq!(vectors.len(), count);
        for vec in &vectors {
            assert_eq!(vec.len(), dimensions);

            // Check that it's approximately a unit vector (norm ≈ 1.0)
            let norm: f32 = vec.iter().map(|x| x * x).sum::<f32>().sqrt();
            assert!((norm - 1.0).abs() < 0.01);
        }
    }

    #[test]
    fn should_generate_centroids() {
        // given - 100 random vectors
        let vectors = random_unit_vectors(100, 3);

        // when - generate 5 centroids
        let centroids = generate_centroids(&vectors, 5);

        // then
        assert_eq!(centroids.len(), 5);
        for centroid in &centroids {
            assert_eq!(centroid.len(), 3);
        }
    }

    #[test]
    fn should_handle_edge_cases_in_centroid_generation() {
        // Empty vectors
        let centroids = generate_centroids(&[], 5);
        assert_eq!(centroids.len(), 0);

        // k=0
        let vectors = random_unit_vectors(10, 3);
        let centroids = generate_centroids(&vectors, 0);
        assert_eq!(centroids.len(), 0);

        // k > num vectors
        let vectors = random_unit_vectors(3, 2);
        let centroids = generate_centroids(&vectors, 10);
        assert_eq!(centroids.len(), 3); // Can't generate more centroids than vectors
    }

    #[test]
    fn should_create_centroid_entries_with_sequential_ids() {
        // given
        let vectors = vec![vec![1.0, 2.0], vec![3.0, 4.0], vec![5.0, 6.0]];

        // when
        let entries = create_centroid_entries(vectors.clone());

        // then
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].centroid_id, 1);
        assert_eq!(entries[1].centroid_id, 2);
        assert_eq!(entries[2].centroid_id, 3);
        assert_eq!(entries[0].vector, vec![1.0, 2.0]);
        assert_eq!(entries[1].vector, vec![3.0, 4.0]);
        assert_eq!(entries[2].vector, vec![5.0, 6.0]);
    }
}
