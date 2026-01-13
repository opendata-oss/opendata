//! MetadataIndex value encoding/decoding.
//!
//! Inverted index mapping metadata field/value pairs to vector IDs.
//!
//! The value format is identical to `PostingListValue` - both store a set of
//! vector IDs using RoaringTreemap. The semantic difference is:
//!
//! - `PostingListValue`: Maps centroid IDs to vectors in that cluster
//! - `MetadataIndexValue`: Maps metadata field/value pairs to matching vectors
//!
//! Both support the same set operations (union, intersection, difference) used
//! for query evaluation.

/// MetadataIndex value storing vector IDs for a metadata value.
///
/// This is a type alias to `PostingListValue` since both store vector ID sets
/// with the same RoaringTreemap format and operations. The alias provides
/// semantic clarity when working with metadata indexes vs centroid posting lists.
///
/// See `PostingListValue` for available methods.
pub type MetadataIndexValue = super::posting_list::PostingListValue;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_encode_and_decode_empty_metadata_index() {
        // given
        let value = MetadataIndexValue::new();

        // when
        let encoded = value.encode_to_bytes().unwrap();
        let decoded = MetadataIndexValue::decode_from_bytes(&encoded).unwrap();

        // then
        assert!(decoded.is_empty());
    }

    #[test]
    fn should_encode_and_decode_metadata_index_with_ids() {
        // given
        let mut value = MetadataIndexValue::new();
        value.insert(1);
        value.insert(100);
        value.insert(10000);

        // when
        let encoded = value.encode_to_bytes().unwrap();
        let decoded = MetadataIndexValue::decode_from_bytes(&encoded).unwrap();

        // then
        assert_eq!(decoded.len(), 3);
        assert!(decoded.contains(1));
        assert!(decoded.contains(100));
        assert!(decoded.contains(10000));
    }

    #[test]
    fn should_perform_intersection_for_filtering() {
        // given - vectors with category="shoes"
        let mut shoes = MetadataIndexValue::new();
        shoes.insert(1);
        shoes.insert(2);
        shoes.insert(3);

        // vectors with price<100
        let mut cheap = MetadataIndexValue::new();
        cheap.insert(2);
        cheap.insert(3);
        cheap.insert(4);

        // when - find vectors where category="shoes" AND price<100
        shoes.intersect_with(&cheap);

        // then
        assert_eq!(shoes.len(), 2);
        assert!(shoes.contains(2));
        assert!(shoes.contains(3));
    }
}
