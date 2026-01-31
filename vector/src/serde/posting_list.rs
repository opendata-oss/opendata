//! PostingList value encoding/decoding.
//!
//! Maps centroid IDs to the list of vectors assigned to that cluster, including
//! their full vector data.
//!
//! ## Role in SPANN Index
//!
//! In a SPANN-style vector index, vectors are clustered around centroids. Each
//! centroid has an associated **posting list** containing the vectors assigned
//! to that cluster.
//!
//! During search:
//! 1. Find the k nearest centroids to the query vector
//! 2. Load the posting lists for those centroids
//! 3. Compute exact distances using the embedded vectors
//! 4. Return top results
//!
//! ## Value Format
//!
//! The value is a FixedElementArray of PostingUpdate entries. Each entry contains:
//! - `posting_type`: 0x0 for Append, 0x1 for Delete
//! - `id`: Internal vector ID (u64)
//! - `vector`: The full vector data (dimensions * f32)
//!
//! ## Merge Operators
//!
//! Posting lists use SlateDB merge operators:
//! - New vectors are added as a value with all Append entries
//! - Vectors removed by LIRE are added as a value with all Delete entries
//! - Same-type merges are simple buffer concatenation
//! - Mixed-type merges require filtering

use super::EncodingError;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::collections::{HashSet, VecDeque};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Posting {
    id: u64,
    vector: Vec<f32>,
}

impl Posting {
    fn new(id: u64, vector: Vec<f32>) -> Self {
        Self { id, vector }
    }

    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    #[allow(dead_code)]
    pub(crate) fn vector(&self) -> &[f32] {
        self.vector.as_slice()
    }
}

pub(crate) type PostingList = Vec<Posting>;

impl From<PostingListValue> for PostingList {
    fn from(value: PostingListValue) -> Self {
        // Process in reverse order so later entries take precedence
        let mut postings = VecDeque::with_capacity(value.postings.len());
        let mut seen = HashSet::with_capacity(value.postings.len());
        for posting in value.postings.into_iter().rev() {
            let id = match posting {
                PostingUpdate::Append { id, vector } => {
                    if !seen.contains(&id) {
                        postings.push_front(Posting::new(id, vector));
                    }
                    id
                }
                PostingUpdate::Delete { id } => id,
            };
            seen.insert(id);
        }
        postings.into()
    }
}

/// Byte value for append posting type in encoded format.
pub(crate) const POSTING_UPDATE_TYPE_APPEND_BYTE: u8 = 0x0;

/// Byte value for delete posting type in encoded format.
pub(crate) const POSTING_UPDATE_TYPE_DELETE_BYTE: u8 = 0x1;

/// A single posting update entry containing vector data.
///
/// Each entry represents either an append or delete operation on a vector
/// within a centroid's posting list.
#[derive(Debug, Clone, PartialEq)]
pub enum PostingUpdate {
    /// The type of update: Append or Delete.
    Append {
        /// Internal vector ID.
        id: u64,
        /// The full vector data.
        vector: Vec<f32>,
    },
    Delete {
        /// Internal vector ID
        id: u64,
    },
}

impl PostingUpdate {
    /// Create a new append posting update.
    pub fn append(id: u64, vector: Vec<f32>) -> Self {
        Self::Append { id, vector }
    }

    /// Create a new delete posting update.
    pub fn delete(id: u64) -> Self {
        Self::Delete { id }
    }

    /// Returns true if this is an append operation.
    pub fn is_append(&self) -> bool {
        matches!(self, Self::Append { .. })
    }

    /// Returns true if this is a delete operation.
    pub fn is_delete(&self) -> bool {
        matches!(self, Self::Delete { .. })
    }

    /// Returns the vector ID.
    #[cfg(test)]
    pub fn id(&self) -> u64 {
        match self {
            Self::Append { id, .. } => *id,
            Self::Delete { id } => *id,
        }
    }

    /// Returns the vector data if this is an Append, None if Delete.
    #[cfg(test)]
    pub fn vector(&self) -> Option<&[f32]> {
        match self {
            Self::Append { vector, .. } => Some(vector.as_slice()),
            Self::Delete { .. } => None,
        }
    }

    /// Encode this posting update to bytes.
    ///
    /// Format: type (1 byte) + id (8 bytes LE) + vector (N*4 bytes, each f32 LE)
    pub fn encode(&self, buf: &mut BytesMut) {
        match self {
            Self::Append { id, vector } => {
                buf.put_u8(POSTING_UPDATE_TYPE_APPEND_BYTE);
                buf.put_u64_le(*id);
                for &val in vector {
                    buf.put_f32_le(val);
                }
            }
            Self::Delete { id } => {
                buf.put_u8(POSTING_UPDATE_TYPE_DELETE_BYTE);
                buf.put_u64_le(*id);
            }
        }
    }

    /// Decode a posting update from bytes.
    ///
    /// Requires knowing the dimensions to determine vector size for Append entries.
    pub fn decode(buf: &mut impl Buf, dimensions: usize) -> Result<Self, EncodingError> {
        let min_size = 1 + 8; // type + id
        if buf.remaining() < min_size {
            return Err(EncodingError {
                message: format!(
                    "Buffer too short for PostingUpdate: expected at least {} bytes, got {}",
                    min_size,
                    buf.remaining()
                ),
            });
        }

        let posting_type = buf.get_u8();
        let id = buf.get_u64_le();

        if posting_type == POSTING_UPDATE_TYPE_APPEND_BYTE {
            let vector_size = dimensions * 4;
            if buf.remaining() < vector_size {
                return Err(EncodingError {
                    message: format!(
                        "Buffer too short for Append PostingUpdate vector: expected {} bytes, got {}",
                        vector_size,
                        buf.remaining()
                    ),
                });
            }

            let mut vector = Vec::with_capacity(dimensions);
            for _ in 0..dimensions {
                vector.push(buf.get_f32_le());
            }

            Ok(PostingUpdate::Append { id, vector })
        } else if posting_type == POSTING_UPDATE_TYPE_DELETE_BYTE {
            Ok(PostingUpdate::Delete { id })
        } else {
            Err(EncodingError {
                message: format!("Invalid posting type: 0x{:02x}", posting_type),
            })
        }
    }

    /// Returns the encoded size in bytes for an Append entry with given dimensionality.
    pub fn encoded_size_append(dimensions: usize) -> usize {
        1 + 8 + (dimensions * 4) // type + id + vector
    }

    /// Returns the encoded size in bytes for a Delete entry.
    pub fn encoded_size_delete() -> usize {
        1 + 8 // type + id
    }

    /// Returns the encoded size of this posting update.
    pub fn encoded_size(&self) -> usize {
        match self {
            PostingUpdate::Append { vector, .. } => 1 + 8 + (vector.len() * 4),
            PostingUpdate::Delete { .. } => 1 + 8,
        }
    }
}

/// PostingList value storing vector updates for a centroid cluster.
///
/// Each posting list maps a single centroid ID to the list of vector updates
/// (appends or deletes) for that cluster.
///
/// ## Value Layout
///
/// ```text
/// +----------------------------------------------------------------+
/// |  postings: FixedElementArray<PostingUpdate>                    |
/// |            (no count prefix, elements back-to-back)            |
/// +----------------------------------------------------------------+
/// ```
///
/// Each PostingUpdate:
/// ```text
/// +--------+----------+----------------------------------+
/// | type   | id       | vector                           |
/// | 1 byte | 8 bytes  | dimensions * 4 bytes             |
/// +--------+----------+----------------------------------+
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct PostingListValue {
    /// List of posting updates (appends or deletes).
    pub postings: Vec<PostingUpdate>,
}

impl PostingListValue {
    /// Create an empty posting list.
    pub fn new() -> Self {
        Self {
            postings: Vec::new(),
        }
    }

    /// Create a posting list from a vector of updates.
    pub fn from_posting_updates(posting_updates: Vec<PostingUpdate>) -> Self {
        Self {
            postings: posting_updates,
        }
    }

    /// Returns the number of posting updates.
    pub fn len(&self) -> usize {
        self.postings.len()
    }

    /// Returns true if the posting list is empty.
    pub fn is_empty(&self) -> bool {
        self.postings.is_empty()
    }

    /// Returns an iterator over the posting updates.
    pub fn iter(&self) -> impl Iterator<Item = &PostingUpdate> {
        self.postings.iter()
    }

    /// Encode to bytes (variable-length entries).
    pub fn encode_to_bytes(&self) -> Bytes {
        if self.postings.is_empty() {
            return Bytes::new();
        }

        let total_size: usize = self.postings.iter().map(|p| p.encoded_size()).sum();
        let mut buf = BytesMut::with_capacity(total_size);

        for posting in &self.postings {
            posting.encode(&mut buf);
        }

        buf.freeze()
    }

    /// Decode from bytes (variable-length entries).
    ///
    /// Requires knowing the dimensions to determine vector size for Append entries.
    pub fn decode_from_bytes(buf: &[u8], dimensions: usize) -> Result<Self, EncodingError> {
        if buf.is_empty() {
            return Ok(PostingListValue::new());
        }

        let mut buf = buf;
        let mut postings = Vec::new();

        while buf.has_remaining() {
            let posting = PostingUpdate::decode(&mut buf, dimensions)?;
            postings.push(posting);
        }

        Ok(PostingListValue { postings })
    }
}

impl Default for PostingListValue {
    fn default() -> Self {
        Self::new()
    }
}

/// Merge two PostingList values by deduplicating entries by id.
///
/// PostingList values contain PostingUpdate entries. The merge strategy keeps
/// only the last update for each id, where "last" means the entry from the new
/// value takes precedence over entries in the existing value.
///
/// This implementation avoids full decode/re-encode by working directly with
/// the raw bytes, only parsing the type byte and id to track locations.
pub(crate) fn merge_posting_list(existing: Bytes, new_value: Bytes, dimensions: usize) -> Bytes {
    use std::collections::HashMap;

    let vector_size = dimensions * 4;
    let append_size = 1 + 8 + vector_size; // type + id + vector
    let delete_size = 1 + 8; // type + id

    // Map from id -> (buffer_index, offset, entry_size)
    // buffer_index: 0 = existing, 1 = new_value
    let mut locations: HashMap<u64, (usize, usize, usize)> = HashMap::new();
    let mut ids: Vec<u64> = Vec::new();

    let mut collect_locations = |data: &[u8], buf_idx: usize| {
        let mut buf = data;
        let original_len = buf.len();
        while buf.has_remaining() {
            let offset = original_len - buf.remaining();
            let entry_type = buf.get_u8();
            let id = buf.get_u64_le();
            let entry_size = if entry_type == POSTING_UPDATE_TYPE_APPEND_BYTE {
                buf.advance(vector_size);
                append_size
            } else {
                delete_size
            };
            if locations
                .insert(id, (buf_idx, offset, entry_size))
                .is_none()
            {
                ids.push(id);
            }
        }
    };

    collect_locations(&existing, 0);
    collect_locations(&new_value, 1);

    let buffers = [&existing, &new_value];
    let total_size: usize = locations.values().map(|(_, _, size)| size).sum();
    let mut result = BytesMut::with_capacity(total_size);
    for id in ids {
        let (buf_idx, offset, size) = locations[&id];
        result.put_slice(&buffers[buf_idx][offset..offset + size]);
    }
    result.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_encode_and_decode_empty_posting_list() {
        // given
        let value = PostingListValue::new();

        // when
        let encoded = value.encode_to_bytes();
        let decoded = PostingListValue::decode_from_bytes(&encoded, 3).unwrap();

        // then
        assert!(decoded.is_empty());
    }

    #[test]
    fn should_encode_and_decode_posting_list_with_appends() {
        // given
        let postings = vec![
            PostingUpdate::append(1, vec![1.0, 2.0, 3.0]),
            PostingUpdate::append(2, vec![4.0, 5.0, 6.0]),
        ];
        let value = PostingListValue::from_posting_updates(postings);

        // when
        let encoded = value.encode_to_bytes();
        let decoded = PostingListValue::decode_from_bytes(&encoded, 3).unwrap();

        // then
        assert_eq!(decoded.len(), 2);
        assert!(decoded.postings[0].is_append());
        assert_eq!(decoded.postings[0].id(), 1);
        assert_eq!(decoded.postings[0].vector().unwrap(), &[1.0, 2.0, 3.0]);
        assert!(decoded.postings[1].is_append());
        assert_eq!(decoded.postings[1].id(), 2);
        assert_eq!(decoded.postings[1].vector().unwrap(), &[4.0, 5.0, 6.0]);
    }

    #[test]
    fn should_encode_and_decode_posting_list_with_deletes() {
        // given
        let postings = vec![PostingUpdate::delete(1), PostingUpdate::delete(2)];
        let value = PostingListValue::from_posting_updates(postings);

        // when
        let encoded = value.encode_to_bytes();
        let decoded = PostingListValue::decode_from_bytes(&encoded, 3).unwrap();

        // then
        assert_eq!(decoded.len(), 2);
        assert!(decoded.postings[0].is_delete());
        assert_eq!(decoded.postings[0].id(), 1);
        assert!(decoded.postings[1].is_delete());
        assert_eq!(decoded.postings[1].id(), 2);
    }

    #[test]
    fn should_create_posting_update_append() {
        // given / when
        let update = PostingUpdate::append(42, vec![1.0, 2.0]);

        // then
        assert!(update.is_append());
        assert!(!update.is_delete());
        assert_eq!(update.id(), 42);
        assert_eq!(update.vector().unwrap(), &[1.0, 2.0]);
    }

    #[test]
    fn should_create_posting_update_delete() {
        // given / when
        let update = PostingUpdate::delete(42);

        // then
        assert!(!update.is_append());
        assert!(update.is_delete());
        assert_eq!(update.id(), 42);
    }

    #[test]
    fn should_encode_and_decode_single_posting_update() {
        // given
        let update = PostingUpdate::append(12345, vec![1.5, 2.5, 3.5, 4.5]);
        let mut buf = BytesMut::new();
        update.encode(&mut buf);

        // when
        let mut buf_ref: &[u8] = &buf;
        let decoded = PostingUpdate::decode(&mut buf_ref, 4).unwrap();

        // then
        assert!(decoded.is_append());
        assert_eq!(decoded.id(), 12345);
        assert_eq!(decoded.vector().unwrap(), &[1.5, 2.5, 3.5, 4.5]);
    }

    #[test]
    fn should_calculate_correct_encoded_size() {
        // when / then
        assert_eq!(PostingUpdate::encoded_size_append(3), 1 + 8 + 12); // 21 bytes
        assert_eq!(PostingUpdate::encoded_size_append(128), 1 + 8 + 512); // 521 bytes
        assert_eq!(PostingUpdate::encoded_size_delete(), 1 + 8); // 9 bytes
    }

    #[test]
    fn should_reject_invalid_posting_type() {
        // given
        let mut buf = BytesMut::new();
        buf.put_u8(0xFF); // Invalid type
        buf.put_u64_le(1);
        buf.put_f32_le(1.0);

        // when
        let mut buf_ref: &[u8] = &buf;
        let result = PostingUpdate::decode(&mut buf_ref, 1);

        // then
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains("Invalid posting type"));
    }

    #[test]
    fn should_reject_buffer_too_short() {
        // given
        let buf = [0u8; 5]; // Too short for any valid posting

        // when
        let mut buf_ref: &[u8] = &buf;
        let result = PostingUpdate::decode(&mut buf_ref, 3);

        // then
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains("Buffer too short"));
    }

    #[test]
    fn should_convert_value_to_postings() {
        // given
        let postings = vec![
            PostingUpdate::append(1, vec![1.0]),
            PostingUpdate::append(2, vec![2.0]),
            PostingUpdate::append(3, vec![3.0]),
        ];
        let value = PostingListValue::from_posting_updates(postings);

        // when:
        let postings: PostingList = value.into();

        // then:
        assert_eq!(
            postings,
            vec![
                Posting::new(1, vec![1.0]),
                Posting::new(2, vec![2.0]),
                Posting::new(3, vec![3.0])
            ]
        );
    }

    #[test]
    fn should_drop_deleted_posting_when_convert_value_to_postings() {
        // given - deletes interspersed with appends
        let postings = vec![
            PostingUpdate::append(1, vec![1.0]),
            PostingUpdate::delete(99),
            PostingUpdate::append(2, vec![2.0]),
        ];
        let value = PostingListValue::from_posting_updates(postings);

        // when
        let postings: PostingList = value.into();

        // then - delete entry not in result
        assert_eq!(
            postings,
            vec![Posting::new(1, vec![1.0]), Posting::new(2, vec![2.0])]
        );
    }

    #[test]
    fn should_drop_deleted_posting_and_mask_earlier_when_convert_value_to_postings() {
        // given - append followed by delete of same id
        let postings = vec![
            PostingUpdate::append(1, vec![1.0]),
            PostingUpdate::append(2, vec![2.0]),
            PostingUpdate::delete(1),
        ];
        let value = PostingListValue::from_posting_updates(postings);

        // when
        let postings: PostingList = value.into();

        // then - id 1 is masked by the delete
        assert_eq!(postings, vec![Posting::new(2, vec![2.0])]);
    }

    #[test]
    fn should_mask_earlier_when_convert_value_to_postings() {
        // given - two appends with same id, later one wins
        let postings = vec![
            PostingUpdate::append(1, vec![1.0]),
            PostingUpdate::append(2, vec![2.0]),
            PostingUpdate::append(1, vec![100.0]),
        ];
        let value = PostingListValue::from_posting_updates(postings);

        // when
        let postings: PostingList = value.into();

        // then - id 1 appears only once with the later vector
        assert_eq!(
            postings,
            vec![Posting::new(2, vec![2.0]), Posting::new(1, vec![100.0])]
        );
    }

    #[test]
    fn should_merge_posting_list_with_deduplication() {
        // given
        let existing_postings = vec![
            PostingUpdate::append(1, vec![1.0, 2.0, 3.0]),
            PostingUpdate::append(2, vec![4.0, 5.0, 6.0]),
        ];
        let existing_value =
            PostingListValue::from_posting_updates(existing_postings).encode_to_bytes();

        let new_postings = vec![PostingUpdate::append(3, vec![7.0, 8.0, 9.0])];
        let new_value = PostingListValue::from_posting_updates(new_postings).encode_to_bytes();

        // when
        let merged = merge_posting_list(existing_value, new_value, 3);
        let decoded = PostingListValue::decode_from_bytes(&merged, 3).unwrap();

        // then - all 3 unique ids preserved
        assert_eq!(decoded.len(), 3);
    }

    #[test]
    fn should_merge_with_delete_masking_old_append() {
        // given - existing has append, new has delete for same id
        let existing_postings = vec![
            PostingUpdate::append(1, vec![1.0, 2.0]),
            PostingUpdate::append(2, vec![3.0, 4.0]),
        ];
        let existing_value =
            PostingListValue::from_posting_updates(existing_postings).encode_to_bytes();

        let new_postings = vec![PostingUpdate::delete(1)];
        let new_value = PostingListValue::from_posting_updates(new_postings).encode_to_bytes();

        // when
        let merged = merge_posting_list(existing_value, new_value, 2);
        let decoded = PostingListValue::decode_from_bytes(&merged, 2).unwrap();

        // then - id 1 is now a delete (keeps original position), id 2 unchanged
        assert_eq!(decoded.len(), 2);
        assert!(decoded.postings[0].is_delete());
        assert_eq!(decoded.postings[0].id(), 1);
        assert!(decoded.postings[1].is_append());
        assert_eq!(decoded.postings[1].id(), 2);
    }

    #[test]
    fn should_merge_with_append_masking_old_append() {
        // given - existing has append, new has append for same id with different vector
        let existing_postings = vec![
            PostingUpdate::append(1, vec![1.0, 2.0]),
            PostingUpdate::append(2, vec![3.0, 4.0]),
        ];
        let existing_value =
            PostingListValue::from_posting_updates(existing_postings).encode_to_bytes();

        let new_postings = vec![PostingUpdate::append(1, vec![100.0, 200.0])];
        let new_value = PostingListValue::from_posting_updates(new_postings).encode_to_bytes();

        // when
        let merged = merge_posting_list(existing_value, new_value, 2);
        let decoded = PostingListValue::decode_from_bytes(&merged, 2).unwrap();

        // then - id 1 has new vector (keeps original position), id 2 unchanged
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded.postings[0].id(), 1);
        assert_eq!(decoded.postings[0].vector().unwrap(), &[100.0, 200.0]);
        assert_eq!(decoded.postings[1].id(), 2);
        assert_eq!(decoded.postings[1].vector().unwrap(), &[3.0, 4.0]);
    }

    #[test]
    fn should_merge_empty_existing_with_new() {
        // given - empty existing, non-empty new
        let existing_value = PostingListValue::new().encode_to_bytes();

        let new_postings = vec![
            PostingUpdate::append(1, vec![1.0, 2.0]),
            PostingUpdate::append(2, vec![3.0, 4.0]),
        ];
        let new_value = PostingListValue::from_posting_updates(new_postings).encode_to_bytes();

        // when
        let merged = merge_posting_list(existing_value, new_value, 2);
        let decoded = PostingListValue::decode_from_bytes(&merged, 2).unwrap();

        // then - new entries are preserved
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded.postings[0].id(), 1);
        assert_eq!(decoded.postings[1].id(), 2);
    }

    #[test]
    fn should_merge_existing_with_empty_new() {
        // given - non-empty existing, empty new
        let existing_postings = vec![
            PostingUpdate::append(1, vec![1.0, 2.0]),
            PostingUpdate::append(2, vec![3.0, 4.0]),
        ];
        let existing_value =
            PostingListValue::from_posting_updates(existing_postings).encode_to_bytes();

        let new_value = PostingListValue::new().encode_to_bytes();

        // when
        let merged = merge_posting_list(existing_value, new_value, 2);
        let decoded = PostingListValue::decode_from_bytes(&merged, 2).unwrap();

        // then - existing entries are preserved
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded.postings[0].id(), 1);
        assert_eq!(decoded.postings[1].id(), 2);
    }

    #[test]
    fn should_merge_both_empty() {
        // given - both empty
        let existing_value = PostingListValue::new().encode_to_bytes();
        let new_value = PostingListValue::new().encode_to_bytes();

        // when
        let merged = merge_posting_list(existing_value, new_value, 2);
        let decoded = PostingListValue::decode_from_bytes(&merged, 2).unwrap();

        // then - result is empty
        assert!(decoded.is_empty());
    }

    #[test]
    fn should_merge_preserving_order_of_first_occurrence() {
        // given - entries appear in specific order
        let existing_postings = vec![
            PostingUpdate::append(1, vec![1.0]),
            PostingUpdate::append(2, vec![2.0]),
            PostingUpdate::append(3, vec![3.0]),
        ];
        let existing_value =
            PostingListValue::from_posting_updates(existing_postings).encode_to_bytes();

        let new_postings = vec![
            PostingUpdate::append(4, vec![4.0]),
            PostingUpdate::append(2, vec![20.0]), // overwrites id 2
            PostingUpdate::append(5, vec![5.0]),
        ];
        let new_value = PostingListValue::from_posting_updates(new_postings).encode_to_bytes();

        // when
        let merged = merge_posting_list(existing_value, new_value, 1);
        let decoded = PostingListValue::decode_from_bytes(&merged, 1).unwrap();

        // then - order preserved: 1, 2 (updated), 3, 4, 5
        assert_eq!(decoded.len(), 5);
        assert_eq!(decoded.postings[0].id(), 1);
        assert_eq!(decoded.postings[1].id(), 2);
        assert_eq!(decoded.postings[1].vector().unwrap(), &[20.0]); // updated value
        assert_eq!(decoded.postings[2].id(), 3);
        assert_eq!(decoded.postings[3].id(), 4);
        assert_eq!(decoded.postings[4].id(), 5);
    }
}
