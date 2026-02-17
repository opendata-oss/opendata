//! SPFresh split heuristics for centroid rebalancing.

use crate::distance;
use crate::serde::collection_meta::DistanceMetric;

/// SPFresh split heuristic: returns true if vector `v` may need reassignment
/// after centroid `c` was split into `c0` and `c1`.
///
/// A vector needs reassignment consideration when the old (deleted) centroid
/// `c` is closer to `v` than both new centroids. In that case, a neighboring
/// centroid could be closer than either new centroid. If either new centroid
/// is closer than the old one, the vector is already better off and cannot
/// need reassignment (by the NPA triangle inequality argument).
#[allow(dead_code)]
pub(crate) fn split_heuristic(
    vector: &[f32],
    c_vector: &[f32],
    c0_vector: &[f32],
    c1_vector: &[f32],
    distance_metric: DistanceMetric,
) -> bool {
    let d_old = distance::compute_distance(vector, c_vector, distance_metric);
    let d0 = distance::compute_distance(vector, c0_vector, distance_metric);
    let d1 = distance::compute_distance(vector, c1_vector, distance_metric);

    // Old centroid is more similar (closer) than both new centroids
    d_old <= d0 && d_old <= d1
}

/// SPFresh neighbour split heuristic: returns true if vector `v` in a
/// nearby posting (centroid `B`) may need reassignment after centroid `c`
/// was split into `c0` and `c1`.
///
/// A vector needs reassignment consideration when at least one new centroid
/// is closer to `v` than the old (deleted) centroid was. This means the
/// split brought a centroid closer to `v`, so it might now be closer than
/// `v`'s current centroid `B`. Conversely, if both new centroids are farther
/// than the old one, the split only made things worse for `v` and its
/// existing assignment to `B` (which was already closer than `A_o`) remains
/// optimal.
#[allow(dead_code)]
pub(crate) fn neighbour_split_heuristic(
    vector: &[f32],
    c_vector: &[f32],
    c0_vector: &[f32],
    c1_vector: &[f32],
    distance_metric: DistanceMetric,
) -> bool {
    let d_old = distance::compute_distance(vector, c_vector, distance_metric);
    let d0 = distance::compute_distance(vector, c0_vector, distance_metric);
    let d1 = distance::compute_distance(vector, c1_vector, distance_metric);

    // At least one new centroid is more similar (closer) than the old centroid
    d0 <= d_old || d1 <= d_old
}

#[cfg(test)]
mod tests {
    use super::*;

    // Use L2 for all heuristic tests. VectorDistance ordering across metrics
    // is tested in distance::tests.
    const METRIC: DistanceMetric = DistanceMetric::L2;

    // ---- split_heuristic ----

    #[test]
    fn should_flag_when_old_centroid_closer_than_both_new() {
        // given - vector at origin, old centroid nearest, both new farther
        let v = [0.0, 0.0];
        let c_old = [1.0, 0.0];
        let c0 = [2.0, 0.0];
        let c1 = [0.0, 2.0];

        // when/then
        assert!(split_heuristic(&v, &c_old, &c0, &c1, METRIC));
    }

    #[test]
    fn should_not_flag_when_first_new_centroid_is_closer() {
        // given - c0 closer to v than old centroid
        let v = [2.0, 0.0];
        let c_old = [0.0, 0.0];
        let c0 = [2.5, 0.0];
        let c1 = [0.0, 3.0];

        // when/then
        assert!(!split_heuristic(&v, &c_old, &c0, &c1, METRIC));
    }

    #[test]
    fn should_not_flag_when_second_new_centroid_is_closer() {
        // given - c1 closer to v than old centroid
        let v = [0.0, 3.0];
        let c_old = [0.0, 0.0];
        let c0 = [5.0, 0.0];
        let c1 = [0.0, 2.5];

        // when/then
        assert!(!split_heuristic(&v, &c_old, &c0, &c1, METRIC));
    }

    #[test]
    fn should_flag_when_equidistant_to_old_and_new() {
        // given - v equidistant from all centroids
        let v = [0.0, 0.0];
        let c_old = [1.0, 0.0];
        let c0 = [0.0, 1.0];
        let c1 = [-1.0, 0.0];

        // when/then - d_old <= d0 && d_old <= d1 (equal)
        assert!(split_heuristic(&v, &c_old, &c0, &c1, METRIC));
    }

    // ---- neighbour_split_heuristic ----

    #[test]
    fn should_flag_neighbour_when_first_new_centroid_is_closer_than_old() {
        // given - c0 closer to v than old centroid
        let v = [2.0, 0.0];
        let c_old = [0.0, 0.0];
        let c0 = [2.5, 0.0];
        let c1 = [0.0, 5.0];

        // when/then
        assert!(neighbour_split_heuristic(&v, &c_old, &c0, &c1, METRIC));
    }

    #[test]
    fn should_flag_neighbour_when_second_new_centroid_is_closer_than_old() {
        // given - c1 closer to v than old centroid
        let v = [0.0, 4.0];
        let c_old = [0.0, 0.0];
        let c0 = [5.0, 0.0];
        let c1 = [0.0, 3.5];

        // when/then
        assert!(neighbour_split_heuristic(&v, &c_old, &c0, &c1, METRIC));
    }

    #[test]
    fn should_not_flag_neighbour_when_both_new_are_farther_than_old() {
        // given - both new centroids farther from v than old
        let v = [0.0, 0.0];
        let c_old = [1.0, 0.0];
        let c0 = [3.0, 0.0];
        let c1 = [0.0, 3.0];

        // when/then
        assert!(!neighbour_split_heuristic(&v, &c_old, &c0, &c1, METRIC));
    }

    #[test]
    fn should_flag_neighbour_when_equidistant_to_old() {
        // given - c0 equidistant to old centroid
        let v = [0.0, 0.0];
        let c_old = [1.0, 0.0];
        let c0 = [0.0, 1.0];
        let c1 = [3.0, 0.0];

        // when/then - d0 <= d_old (equal)
        assert!(neighbour_split_heuristic(&v, &c_old, &c0, &c1, METRIC));
    }
}
