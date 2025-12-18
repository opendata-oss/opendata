use dashmap::{DashMap, mapref::one::Ref};
use roaring::RoaringBitmap;
use std::collections::HashMap;

use crate::model::{Attribute, SeriesId, SeriesSpec};

/// Trait for looking up series specs by ID.
/// This allows both ForwardIndex and view types to be used interchangeably.
pub(crate) trait ForwardIndexLookup {
    /// Get the series spec for a given series ID.
    /// Returns None if the series is not found.
    fn get_spec(&self, series_id: &SeriesId) -> Option<SeriesSpec>;

    /// Get all series specs in the forward index.
    /// Returns a vector of (series_id, spec) pairs.
    fn all_series(&self) -> Vec<(SeriesId, SeriesSpec)>;
}

impl<T: ForwardIndexLookup + ?Sized> ForwardIndexLookup for Box<T> {
    fn get_spec(&self, series_id: &SeriesId) -> Option<SeriesSpec> {
        (**self).get_spec(series_id)
    }

    fn all_series(&self) -> Vec<(SeriesId, SeriesSpec)> {
        (**self).all_series()
    }
}

/// Trait for querying inverted index data.
/// This allows both InvertedIndex and view types to be used interchangeably.
pub(crate) trait InvertedIndexLookup {
    /// Intersect posting lists for the given terms.
    /// Returns series IDs that match ALL terms.
    fn intersect(&self, terms: Vec<Attribute>) -> RoaringBitmap;

    /// Get all attribute keys in the inverted index.
    fn all_keys(&self) -> Vec<Attribute>;
}

impl<T: InvertedIndexLookup + ?Sized> InvertedIndexLookup for Box<T> {
    fn intersect(&self, terms: Vec<Attribute>) -> RoaringBitmap {
        (**self).intersect(terms)
    }

    fn all_keys(&self) -> Vec<Attribute> {
        (**self).all_keys()
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ForwardIndex {
    pub(crate) series: DashMap<SeriesId, SeriesSpec>,
}

impl ForwardIndex {
    pub(crate) fn merge(&self, other: &ForwardIndex) {
        for entry in other.series.iter() {
            let series_id = entry.key();
            let series_spec = entry.value().clone();
            self.series.insert(*series_id, series_spec.clone());
        }
    }
}

impl ForwardIndexLookup for ForwardIndex {
    fn get_spec(&self, series_id: &SeriesId) -> Option<SeriesSpec> {
        self.series.get(series_id).map(|r| r.value().clone())
    }

    fn all_series(&self) -> Vec<(SeriesId, SeriesSpec)> {
        self.series
            .iter()
            .map(|entry| (*entry.key(), entry.value().clone()))
            .collect()
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct InvertedIndex {
    /// Maps Attribute (key, value) to the list of series_id values containing it.
    pub(crate) postings: DashMap<Attribute, RoaringBitmap>,
}

impl InvertedIndexLookup for InvertedIndex {
    fn intersect(&self, terms: Vec<Attribute>) -> RoaringBitmap {
        if terms.is_empty() {
            return RoaringBitmap::new();
        }

        let mut bitmaps: Vec<Ref<'_, Attribute, RoaringBitmap>> = Vec::new();

        for term in &terms {
            match self.postings.get(term) {
                Some(bitmap) => bitmaps.push(bitmap),
                None => {
                    return RoaringBitmap::new();
                }
            }
        }

        bitmaps.sort_by_key(|b| b.value().len());

        let mut result = bitmaps[0].value().clone();
        for bitmap in &bitmaps[1..] {
            result &= bitmap.value();
        }

        result
    }

    fn all_keys(&self) -> Vec<Attribute> {
        self.postings
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }
}

impl InvertedIndex {
    pub(crate) fn union(&self, terms: Vec<Attribute>) -> RoaringBitmap {
        if terms.is_empty() {
            return RoaringBitmap::new();
        }

        let mut result = RoaringBitmap::new();

        for term in terms {
            if let Some(bitmap) = self.postings.get(&term) {
                result |= bitmap.value();
            }
        }

        result
    }

    fn insert(&self, term: Attribute, postings: RoaringBitmap) {
        let mut entry = self.postings.entry(term).or_default();
        let value = entry.value_mut();
        *value |= postings;
    }

    pub(crate) fn merge(&self, other: InvertedIndex) {
        for (term, postings) in other.postings {
            self.insert(term, postings);
        }
    }

    pub(crate) fn merge_from_map(&self, other: HashMap<Attribute, RoaringBitmap>) {
        for (term, postings) in other {
            self.insert(term, postings);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case::empty_terms(
        vec![],
        vec![],
        "empty terms list returns empty result"
    )]
    #[case::empty_posting_list(
        vec![vec![1, 2, 3], vec![]],
        vec![],
        "empty posting list returns empty result"
    )]
    #[case::single_value_in_all(
        vec![vec![5], vec![5], vec![5]],
        vec![5],
        "single value in all lists returns that value"
    )]
    #[case::two_lists_full_overlap(
        vec![vec![1, 2, 3, 4, 5], vec![1, 2, 3, 4, 5]],
        vec![1, 2, 3, 4, 5],
        "two lists with full overlap returns all values"
    )]
    #[case::two_lists_partial_overlap(
        vec![vec![1, 2, 3, 4, 5], vec![3, 4, 5, 6, 7]],
        vec![3, 4, 5],
        "two lists with partial overlap returns intersection"
    )]
    #[case::three_lists_single_common(
        vec![vec![1, 2, 10, 20], vec![5, 10, 15], vec![10, 30, 40]],
        vec![10],
        "three lists with single common value returns that value"
    )]
    #[case::multiple_lists_multiple_common(
        vec![vec![1, 5, 10, 15, 20], vec![5, 10, 15, 25], vec![5, 10, 15, 30]],
        vec![5, 10, 15],
        "multiple lists with multiple common values returns all common values"
    )]
    #[case::non_overlapping_lists(
        vec![vec![1, 2, 3], vec![4, 5, 6], vec![7, 8, 9]],
        vec![],
        "non-overlapping lists return empty result"
    )]
    #[case::disjoint_ranges(
        vec![vec![1, 2, 3], vec![100, 200, 300]],
        vec![],
        "disjoint ranges return empty result"
    )]
    #[case::single_posting_list(
        vec![vec![1, 2, 3, 4, 5]],
        vec![1, 2, 3, 4, 5],
        "single posting list returns all its values"
    )]
    #[case::inserts_not_sorted(
        vec![vec![5, 4, 3, 2, 1]],
        vec![1, 2, 3, 4, 5],
        "inserts not sorted returns values in ascending order"
    )]
    #[case::lists_with_gaps(
        vec![vec![1, 10, 100, 1000], vec![10, 50, 100, 500, 1000]],
        vec![10, 100, 1000],
        "lists with gaps correctly skip and find intersections"
    )]
    fn should_intersect_posting_lists_correctly(
        #[case] posting_lists: Vec<Vec<u32>>,
        #[case] expected: Vec<u32>,
        #[case] description: &str,
    ) {
        // Given: an inverted index with posting lists
        let index = InvertedIndex::default();

        for (i, list) in posting_lists.iter().enumerate() {
            let mut bitmap = RoaringBitmap::new();
            for &value in list {
                bitmap.insert(value);
            }
            let term = Attribute {
                key: format!("attr_{}", i),
                value: "value_0".to_string(),
            };
            index.postings.insert(term.clone(), bitmap);
        }

        // When: intersecting all terms
        let terms: Vec<Attribute> = (0..posting_lists.len())
            .map(|i| Attribute {
                key: format!("attr_{}", i),
                value: "value_0".to_string(),
            })
            .collect();

        let result: Vec<u32> = index.intersect(terms).iter().collect();

        // Then: the result matches the expected intersection
        assert_eq!(result, expected, "Failed: {}", description);
    }

    #[test]
    fn should_return_empty_when_term_missing_from_index() {
        // Given: an inverted index with one posting list
        let index = InvertedIndex::default();
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(1);
        bitmap.insert(2);
        bitmap.insert(3);

        let term = Attribute {
            key: "attr_0".to_string(),
            value: "value_0".to_string(),
        };
        index.postings.insert(term.clone(), bitmap);

        // When: intersecting with a term that doesn't exist in the index
        let terms = vec![
            Attribute {
                key: "attr_0".to_string(),
                value: "value_0".to_string(),
            },
            Attribute {
                key: "attr_1".to_string(),
                value: "value_0".to_string(),
            }, // This term doesn't exist
        ];

        let result: Vec<u32> = index.intersect(terms).iter().collect();

        // Then: the result is empty
        assert_eq!(result, Vec::<u32>::new());
    }

    #[rstest]
    #[case::empty_terms(
        vec![],
        vec![],
        "empty terms list returns empty result"
    )]
    #[case::empty_posting_list(
        vec![vec![1, 2, 3], vec![]],
        vec![1, 2, 3],
        "empty posting list is skipped"
    )]
    #[case::single_posting_list(
        vec![vec![1, 2, 3, 4, 5]],
        vec![1, 2, 3, 4, 5],
        "single posting list returns all its values"
    )]
    #[case::two_lists_no_overlap(
        vec![vec![1, 2, 3], vec![4, 5, 6]],
        vec![1, 2, 3, 4, 5, 6],
        "two lists with no overlap returns all values"
    )]
    #[case::two_lists_with_overlap(
        vec![vec![1, 2, 3, 4, 5], vec![3, 4, 5, 6, 7]],
        vec![1, 2, 3, 4, 5, 6, 7],
        "two lists with overlap returns unique values"
    )]
    #[case::two_lists_full_overlap(
        vec![vec![1, 2, 3], vec![1, 2, 3]],
        vec![1, 2, 3],
        "two lists with full overlap returns values once"
    )]
    #[case::three_lists_mixed_overlap(
        vec![vec![1, 3, 5], vec![2, 3, 4], vec![3, 5, 6]],
        vec![1, 2, 3, 4, 5, 6],
        "three lists with mixed overlap returns all unique values"
    )]
    #[case::lists_with_gaps(
        vec![vec![1, 10, 100], vec![5, 50, 500]],
        vec![1, 5, 10, 50, 100, 500],
        "lists with gaps returns all values in order"
    )]
    #[case::interleaved_values(
        vec![vec![1, 3, 5, 7, 9], vec![2, 4, 6, 8, 10]],
        vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        "interleaved values returns all in order"
    )]
    fn should_union_posting_lists_correctly(
        #[case] posting_lists: Vec<Vec<u32>>,
        #[case] expected: Vec<u32>,
        #[case] description: &str,
    ) {
        // Given: an inverted index with posting lists
        let index = InvertedIndex::default();

        for (i, list) in posting_lists.iter().enumerate() {
            let mut bitmap = RoaringBitmap::new();
            for &value in list {
                bitmap.insert(value);
            }
            let term = Attribute {
                key: format!("attr_{}", i),
                value: "value_0".to_string(),
            };
            index.postings.insert(term, bitmap);
        }

        // When: taking the union of all terms
        let terms: Vec<Attribute> = (0..posting_lists.len())
            .map(|i| Attribute {
                key: format!("attr_{}", i),
                value: "value_0".to_string(),
            })
            .collect();

        let result: Vec<u32> = index.union(terms).iter().collect();

        // Then: the result matches the expected union
        assert_eq!(result, expected, "Failed: {}", description);
    }

    #[test]
    fn should_merge_empty_into_empty() {
        // Given: two empty inverted indexes
        let index1 = InvertedIndex::default();
        let index2 = InvertedIndex::default();

        // When: merging index2 into index1
        index1.merge(index2);

        // Then: index1 remains empty
        assert_eq!(index1.postings.len(), 0);
    }

    #[test]
    fn should_merge_empty_into_non_empty() {
        // Given: a non-empty index and an empty index
        let index1 = InvertedIndex::default();
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(1);
        bitmap.insert(2);
        bitmap.insert(3);
        let term = Attribute {
            key: "attr_0".to_string(),
            value: "value_0".to_string(),
        };
        index1.postings.insert(term.clone(), bitmap.clone());

        let index2 = InvertedIndex::default();

        // When: merging empty index into non-empty index
        index1.merge(index2);

        // Then: index1 remains unchanged
        assert_eq!(index1.postings.len(), 1);
        let result: Vec<u32> = index1.postings.get(&term).unwrap().value().iter().collect();
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[test]
    fn should_merge_non_empty_into_empty() {
        // Given: an empty index and a non-empty index
        let index1 = InvertedIndex::default();

        let index2 = InvertedIndex::default();
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(10);
        bitmap.insert(20);
        bitmap.insert(30);
        let term = Attribute {
            key: "attr_1".to_string(),
            value: "value_1".to_string(),
        };
        index2.postings.insert(term.clone(), bitmap.clone());

        // When: merging non-empty index into empty index
        index1.merge(index2);

        // Then: index1 contains the data from index2
        assert_eq!(index1.postings.len(), 1);
        let result: Vec<u32> = index1.postings.get(&term).unwrap().value().iter().collect();
        assert_eq!(result, vec![10, 20, 30]);
    }

    #[test]
    fn should_merge_non_overlapping_terms() {
        // Given: two indexes with completely different terms
        let index1 = InvertedIndex::default();
        let mut bitmap1 = RoaringBitmap::new();
        bitmap1.insert(1);
        bitmap1.insert(2);
        let term1 = Attribute {
            key: "attr_0".to_string(),
            value: "value_0".to_string(),
        };
        index1.postings.insert(term1.clone(), bitmap1);

        let index2 = InvertedIndex::default();
        let mut bitmap2 = RoaringBitmap::new();
        bitmap2.insert(10);
        bitmap2.insert(20);
        let term2 = Attribute {
            key: "attr_1".to_string(),
            value: "value_1".to_string(),
        };
        index2.postings.insert(term2.clone(), bitmap2);

        // When: merging indexes with different terms
        index1.merge(index2);

        // Then: index1 contains both terms
        assert_eq!(index1.postings.len(), 2);

        let result1: Vec<u32> = index1
            .postings
            .get(&term1)
            .unwrap()
            .value()
            .iter()
            .collect();
        assert_eq!(result1, vec![1, 2]);

        let result2: Vec<u32> = index1
            .postings
            .get(&term2)
            .unwrap()
            .value()
            .iter()
            .collect();
        assert_eq!(result2, vec![10, 20]);
    }

    #[test]
    fn should_merge_same_term_with_disjoint_series() {
        // Given: two indexes with the same term but disjoint series IDs
        let term = Attribute {
            key: "attr_5".to_string(),
            value: "value_10".to_string(),
        };

        let index1 = InvertedIndex::default();
        let mut bitmap1 = RoaringBitmap::new();
        bitmap1.insert(1);
        bitmap1.insert(2);
        bitmap1.insert(3);
        index1.postings.insert(term.clone(), bitmap1);

        let index2 = InvertedIndex::default();
        let mut bitmap2 = RoaringBitmap::new();
        bitmap2.insert(100);
        bitmap2.insert(200);
        bitmap2.insert(300);
        index2.postings.insert(term.clone(), bitmap2);

        // When: merging indexes with same term
        index1.merge(index2);

        // Then: the series IDs are unioned
        assert_eq!(index1.postings.len(), 1);
        let result: Vec<u32> = index1.postings.get(&term).unwrap().value().iter().collect();
        assert_eq!(result, vec![1, 2, 3, 100, 200, 300]);
    }

    #[test]
    fn should_merge_same_term_with_overlapping_series() {
        // Given: two indexes with the same term and overlapping series IDs
        let term = Attribute {
            key: "attr_7".to_string(),
            value: "value_14".to_string(),
        };

        let index1 = InvertedIndex::default();
        let mut bitmap1 = RoaringBitmap::new();
        bitmap1.insert(1);
        bitmap1.insert(2);
        bitmap1.insert(3);
        bitmap1.insert(4);
        bitmap1.insert(5);
        index1.postings.insert(term.clone(), bitmap1);

        let index2 = InvertedIndex::default();
        let mut bitmap2 = RoaringBitmap::new();
        bitmap2.insert(3);
        bitmap2.insert(4);
        bitmap2.insert(5);
        bitmap2.insert(6);
        bitmap2.insert(7);
        index2.postings.insert(term.clone(), bitmap2);

        // When: merging indexes with overlapping series IDs
        index1.merge(index2);

        // Then: the series IDs are unioned (duplicates removed)
        assert_eq!(index1.postings.len(), 1);
        let result: Vec<u32> = index1.postings.get(&term).unwrap().value().iter().collect();
        assert_eq!(result, vec![1, 2, 3, 4, 5, 6, 7]);
    }

    #[test]
    fn should_merge_same_term_with_identical_series() {
        // Given: two indexes with the same term and identical series IDs
        let term = Attribute {
            key: "attr_8".to_string(),
            value: "value_16".to_string(),
        };

        let index1 = InvertedIndex::default();
        let mut bitmap1 = RoaringBitmap::new();
        bitmap1.insert(1);
        bitmap1.insert(2);
        bitmap1.insert(3);
        index1.postings.insert(term.clone(), bitmap1);

        let index2 = InvertedIndex::default();
        let mut bitmap2 = RoaringBitmap::new();
        bitmap2.insert(1);
        bitmap2.insert(2);
        bitmap2.insert(3);
        index2.postings.insert(term.clone(), bitmap2);

        // When: merging indexes with identical series IDs
        index1.merge(index2);

        // Then: the result is the same as the original (union is idempotent)
        assert_eq!(index1.postings.len(), 1);
        let result: Vec<u32> = index1.postings.get(&term).unwrap().value().iter().collect();
        assert_eq!(result, vec![1, 2, 3]);
    }

    #[test]
    fn should_merge_partial_overlap_terms() {
        // Given: two indexes with some overlapping and some unique terms
        let shared_term = Attribute {
            key: "attr_0".to_string(),
            value: "value_0".to_string(),
        };
        let term1_only = Attribute {
            key: "attr_1".to_string(),
            value: "value_1".to_string(),
        };
        let term2_only = Attribute {
            key: "attr_2".to_string(),
            value: "value_2".to_string(),
        };

        let index1 = InvertedIndex::default();
        let mut bitmap_shared1 = RoaringBitmap::new();
        bitmap_shared1.insert(1);
        bitmap_shared1.insert(2);
        index1.postings.insert(shared_term.clone(), bitmap_shared1);

        let mut bitmap1 = RoaringBitmap::new();
        bitmap1.insert(10);
        index1.postings.insert(term1_only.clone(), bitmap1);

        let index2 = InvertedIndex::default();
        let mut bitmap_shared2 = RoaringBitmap::new();
        bitmap_shared2.insert(3);
        bitmap_shared2.insert(4);
        index2.postings.insert(shared_term.clone(), bitmap_shared2);

        let mut bitmap2 = RoaringBitmap::new();
        bitmap2.insert(20);
        index2.postings.insert(term2_only.clone(), bitmap2);

        // When: merging indexes with partial overlap
        index1.merge(index2);

        // Then: all terms are present with correct merged values
        assert_eq!(index1.postings.len(), 3);

        let shared_result: Vec<u32> = index1
            .postings
            .get(&shared_term)
            .unwrap()
            .value()
            .iter()
            .collect();
        assert_eq!(shared_result, vec![1, 2, 3, 4]);

        let term1_result: Vec<u32> = index1
            .postings
            .get(&term1_only)
            .unwrap()
            .value()
            .iter()
            .collect();
        assert_eq!(term1_result, vec![10]);

        let term2_result: Vec<u32> = index1
            .postings
            .get(&term2_only)
            .unwrap()
            .value()
            .iter()
            .collect();
        assert_eq!(term2_result, vec![20]);
    }

    #[test]
    fn should_merge_multiple_times_successively() {
        // Given: three indexes to merge successively
        let term = Attribute {
            key: "attr_9".to_string(),
            value: "value_18".to_string(),
        };

        let index1 = InvertedIndex::default();
        let mut bitmap1 = RoaringBitmap::new();
        bitmap1.insert(1);
        bitmap1.insert(2);
        index1.postings.insert(term.clone(), bitmap1);

        let index2 = InvertedIndex::default();
        let mut bitmap2 = RoaringBitmap::new();
        bitmap2.insert(3);
        bitmap2.insert(4);
        index2.postings.insert(term.clone(), bitmap2);

        let index3 = InvertedIndex::default();
        let mut bitmap3 = RoaringBitmap::new();
        bitmap3.insert(5);
        bitmap3.insert(6);
        index3.postings.insert(term.clone(), bitmap3);

        // When: merging multiple indexes successively
        index1.merge(index2);
        index1.merge(index3);

        // Then: all series IDs are accumulated
        assert_eq!(index1.postings.len(), 1);
        let result: Vec<u32> = index1.postings.get(&term).unwrap().value().iter().collect();
        assert_eq!(result, vec![1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn should_merge_with_many_terms() {
        // Given: two indexes each with many terms
        let index1 = InvertedIndex::default();
        let index2 = InvertedIndex::default();

        // Add 100 terms to index1
        for i in 0..100 {
            let term = Attribute {
                key: format!("attr_{}", i),
                value: format!("value_{}", i),
            };
            let mut bitmap = RoaringBitmap::new();
            bitmap.insert(i);
            index1.postings.insert(term, bitmap);
        }

        // Add 100 terms to index2 (50 overlapping, 50 unique)
        for i in 50..150 {
            let term = Attribute {
                key: format!("attr_{}", i),
                value: format!("value_{}", i),
            };
            let mut bitmap = RoaringBitmap::new();
            bitmap.insert(i + 1000); // Different series IDs
            index2.postings.insert(term, bitmap);
        }

        // When: merging large indexes
        index1.merge(index2);

        // Then: all terms are present
        assert_eq!(index1.postings.len(), 150);

        // Verify a few specific cases:
        // - Term only in index1
        let term_only_1 = Attribute {
            key: "attr_10".to_string(),
            value: "value_10".to_string(),
        };
        let result: Vec<u32> = index1
            .postings
            .get(&term_only_1)
            .unwrap()
            .value()
            .iter()
            .collect();
        assert_eq!(result, vec![10]);

        // - Term in both indexes
        let term_both = Attribute {
            key: "attr_75".to_string(),
            value: "value_75".to_string(),
        };
        let result: Vec<u32> = index1
            .postings
            .get(&term_both)
            .unwrap()
            .value()
            .iter()
            .collect();
        assert_eq!(result, vec![75, 1075]); // Both series IDs present

        // - Term only in index2
        let term_only_2 = Attribute {
            key: "attr_140".to_string(),
            value: "value_140".to_string(),
        };
        let result: Vec<u32> = index1
            .postings
            .get(&term_only_2)
            .unwrap()
            .value()
            .iter()
            .collect();
        assert_eq!(result, vec![1140]);
    }

    #[test]
    fn should_merge_with_large_bitmaps() {
        // Given: two indexes with the same term containing large bitmaps
        let term = Attribute {
            key: "attr_99".to_string(),
            value: "value_99".to_string(),
        };

        let index1 = InvertedIndex::default();
        let mut bitmap1 = RoaringBitmap::new();
        for i in 0..10000 {
            bitmap1.insert(i * 2); // Even numbers
        }
        index1.postings.insert(term.clone(), bitmap1);

        let index2 = InvertedIndex::default();
        let mut bitmap2 = RoaringBitmap::new();
        for i in 0..10000 {
            bitmap2.insert(i * 2 + 1); // Odd numbers
        }
        index2.postings.insert(term.clone(), bitmap2);

        // When: merging indexes with large bitmaps
        index1.merge(index2);

        // Then: the merged bitmap contains all values
        assert_eq!(index1.postings.len(), 1);
        let result_ref = index1.postings.get(&term).unwrap();
        let result_bitmap = result_ref.value();
        assert_eq!(result_bitmap.len(), 20000);

        // Verify a few specific values
        assert!(result_bitmap.contains(0)); // Even
        assert!(result_bitmap.contains(1)); // Odd
        assert!(result_bitmap.contains(1000)); // Even
        assert!(result_bitmap.contains(1001)); // Odd
        assert!(result_bitmap.contains(19998)); // Even
        assert!(result_bitmap.contains(19999)); // Odd
    }

    #[test]
    fn should_return_empty_keys_for_empty_index() {
        // given
        let index = InvertedIndex::default();

        // when
        let keys = index.all_keys();

        // then
        assert!(keys.is_empty());
    }

    #[test]
    fn should_return_all_keys_from_index() {
        // given
        let index = InvertedIndex::default();

        let term1 = Attribute {
            key: "env".to_string(),
            value: "prod".to_string(),
        };
        let term2 = Attribute {
            key: "env".to_string(),
            value: "staging".to_string(),
        };
        let term3 = Attribute {
            key: "method".to_string(),
            value: "GET".to_string(),
        };

        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(1);
        index.postings.insert(term1.clone(), bitmap.clone());
        index.postings.insert(term2.clone(), bitmap.clone());
        index.postings.insert(term3.clone(), bitmap);

        // when
        let keys = index.all_keys();

        // then
        assert_eq!(keys.len(), 3);
        assert!(keys.contains(&term1));
        assert!(keys.contains(&term2));
        assert!(keys.contains(&term3));
    }
}
