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
//! The value is a FixedElementArray of PostingUpdate entries, **sorted by id**.
//! Each entry contains:
//! - `posting_type`: 0x0 for Append, 0x1 for Delete
//! - `id`: Internal vector ID (u64)
//! - `vector`: The full vector data (dimensions * f32)
//!
//! ## Merge Operators
//!
//! Posting lists use SlateDB merge operators with sort-merge semantics:
//! - Both existing and new values are sorted by id
//! - Merge performs a sort-merge, keeping the newer entry when ids match
//! - Output remains sorted by id

use super::EncodingError;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::collections::HashSet;

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
        let mut seen = HashSet::new();
        value
            .postings
            .into_iter()
            .filter_map(|posting| {
                assert!(seen.insert(posting.id()));
                match posting {
                    PostingUpdate::Append { id, vector } => Some(Posting::new(id, vector)),
                    PostingUpdate::Delete { .. } => None,
                }
            })
            .collect()
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
    pub fn id(&self) -> u64 {
        match self {
            Self::Append { id, .. } => *id,
            Self::Delete { id } => *id,
        }
    }

    /// Returns the vector data if this is an Append, None if Delete.
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
    postings: Vec<PostingUpdate>,
}

impl PostingListValue {
    /// Create an empty posting list.
    pub fn new() -> Self {
        Self {
            postings: Vec::new(),
        }
    }

    /// Create a posting list from a vector of updates.
    ///
    /// The updates are sorted by id to maintain the invariant that postings
    /// are always ordered by id.
    ///
    /// # Errors
    ///
    /// Returns an error if there are any entries with repeated ids.
    pub fn from_posting_updates(
        mut posting_updates: Vec<PostingUpdate>,
    ) -> Result<Self, EncodingError> {
        posting_updates.sort_by_key(|p| p.id());

        // Check for duplicate ids
        for window in posting_updates.windows(2) {
            if window[0].id() == window[1].id() {
                return Err(EncodingError {
                    message: format!("Duplicate posting id: {}", window[0].id()),
                });
            }
        }

        Ok(Self {
            postings: posting_updates,
        })
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

        PostingListValue::from_posting_updates(postings)
    }
}

impl Default for PostingListValue {
    fn default() -> Self {
        Self::new()
    }
}

/// Merge two PostingList values using sort-merge.
///
/// Both input PostingList values are assumed to be sorted by id. The merge
/// performs a sort-merge operation:
/// - When ids match, the entry from new_value takes precedence
/// - Output remains sorted by id
///
/// This implementation works directly with raw bytes for efficiency,
/// only parsing the type byte and id to determine merge decisions.
pub(crate) fn merge_posting_list(
    mut existing: Bytes,
    mut new_value: Bytes,
    dimensions: usize,
) -> Bytes {
    let vector_size = dimensions * 4;
    let append_size = 1 + 8 + vector_size; // type + id + vector
    let delete_size = 1 + 8; // type + id

    let peek_entry = |buf: &[u8]| -> Option<(u64, usize)> {
        if buf.is_empty() {
            return None;
        }
        assert!(buf.len() >= delete_size);
        let entry_type = buf[0];
        let id = u64::from_le_bytes([
            buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7], buf[8],
        ]);
        let entry_size = if entry_type == POSTING_UPDATE_TYPE_APPEND_BYTE {
            append_size
        } else {
            delete_size
        };
        Some((id, entry_size))
    };

    // Estimate capacity
    let total_size = existing.len() + new_value.len();
    let mut result = BytesMut::with_capacity(total_size);
    let mut last_id = None;

    loop {
        let existing_entry = peek_entry(&existing);
        let new_entry = peek_entry(&new_value);

        let id = match (existing_entry, new_entry) {
            (None, None) => break,
            (Some((id, size)), None) => {
                // Only existing has entries left
                result.put_slice(&existing[..size]);
                existing.advance(size);
                id
            }
            (None, Some((id, size))) => {
                // Only new has entries left
                result.put_slice(&new_value[..size]);
                new_value.advance(size);
                id
            }
            (Some((existing_id, existing_size)), Some((new_id, new_size))) => {
                if existing_id < new_id {
                    // Take from existing
                    result.put_slice(&existing[..existing_size]);
                    existing.advance(existing_size);
                    existing_id
                } else if new_id < existing_id {
                    // Take from new
                    result.put_slice(&new_value[..new_size]);
                    new_value.advance(new_size);
                    new_id
                } else {
                    // Same id - new wins, skip existing
                    result.put_slice(&new_value[..new_size]);
                    new_value.advance(new_size);
                    existing.advance(existing_size);
                    new_id
                }
            }
        };
        assert!(last_id.is_none_or(|last_id| last_id < id));
        last_id = Some(id);
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
        let value = PostingListValue::from_posting_updates(postings)
            .expect("unexpected error creating posting updates");

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
        let value = PostingListValue::from_posting_updates(postings)
            .expect("unexpected error creating posting updates");

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
        let value = PostingListValue::from_posting_updates(postings)
            .expect("unexpected error creating posting updates");

        // when
        let postings: PostingList = value.into();

        // then - sorted by id
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
        // given - deletes interspersed with appends (will be sorted by id)
        let postings = vec![
            PostingUpdate::append(1, vec![1.0]),
            PostingUpdate::delete(99),
            PostingUpdate::append(2, vec![2.0]),
        ];
        let value = PostingListValue::from_posting_updates(postings)
            .expect("unexpected error creating posting updates");

        // when
        let postings: PostingList = value.into();

        // then - delete entry not in result, sorted by id
        assert_eq!(
            postings,
            vec![Posting::new(1, vec![1.0]), Posting::new(2, vec![2.0])]
        );
    }

    #[test]
    fn should_merge_posting_list_sorted_by_id() {
        // given - existing and new both sorted
        let existing_postings = vec![
            PostingUpdate::append(1, vec![1.0, 2.0, 3.0]),
            PostingUpdate::append(3, vec![4.0, 5.0, 6.0]),
        ];
        let existing_value = PostingListValue::from_posting_updates(existing_postings)
            .expect("unexpected error creating posting updates")
            .encode_to_bytes();

        let new_postings = vec![PostingUpdate::append(2, vec![7.0, 8.0, 9.0])];
        let new_value = PostingListValue::from_posting_updates(new_postings)
            .expect("unexpected error creating posting updates")
            .encode_to_bytes();

        // when
        let merged = merge_posting_list(existing_value, new_value, 3);
        let decoded = PostingListValue::decode_from_bytes(&merged, 3).unwrap();

        // then - all 3 unique ids preserved in sorted order
        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded.postings[0].id(), 1);
        assert_eq!(decoded.postings[1].id(), 2);
        assert_eq!(decoded.postings[2].id(), 3);
    }

    #[test]
    fn should_merge_with_delete_masking_old_append() {
        // given - existing has append, new has delete for same id
        let existing_postings = vec![
            PostingUpdate::append(1, vec![1.0, 2.0]),
            PostingUpdate::append(2, vec![3.0, 4.0]),
        ];
        let existing_value = PostingListValue::from_posting_updates(existing_postings)
            .expect("unexpected error creating posting updates")
            .encode_to_bytes();

        let new_postings = vec![PostingUpdate::delete(1)];
        let new_value = PostingListValue::from_posting_updates(new_postings)
            .expect("unexpected error creating posting updates")
            .encode_to_bytes();

        // when
        let merged = merge_posting_list(existing_value, new_value, 2);
        let decoded = PostingListValue::decode_from_bytes(&merged, 2).unwrap();

        // then - id 1 is now a delete, id 2 unchanged, sorted order
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
        let existing_value = PostingListValue::from_posting_updates(existing_postings)
            .expect("unexpected error creating posting updates")
            .encode_to_bytes();

        let new_postings = vec![PostingUpdate::append(1, vec![100.0, 200.0])];
        let new_value = PostingListValue::from_posting_updates(new_postings)
            .expect("unexpected error creating posting updates")
            .encode_to_bytes();

        // when
        let merged = merge_posting_list(existing_value, new_value, 2);
        let decoded = PostingListValue::decode_from_bytes(&merged, 2).unwrap();

        // then - id 1 has new vector, id 2 unchanged, sorted order
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
            PostingUpdate::append(2, vec![3.0, 4.0]),
            PostingUpdate::append(1, vec![1.0, 2.0]),
        ];
        let new_value = PostingListValue::from_posting_updates(new_postings)
            .expect("unexpected error creating posting updates")
            .encode_to_bytes();

        // when
        let merged = merge_posting_list(existing_value, new_value, 2);
        let decoded = PostingListValue::decode_from_bytes(&merged, 2).unwrap();

        // then - new entries are preserved in sorted order
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
        let existing_value = PostingListValue::from_posting_updates(existing_postings)
            .expect("unexpected error creating posting updates")
            .encode_to_bytes();

        let new_value = PostingListValue::new().encode_to_bytes();

        // when
        let merged = merge_posting_list(existing_value, new_value, 2);
        let decoded = PostingListValue::decode_from_bytes(&merged, 2).unwrap();

        // then - existing entries are preserved in sorted order
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
    fn should_merge_interleaved_ids_in_sorted_order() {
        // given - existing has odd ids, new has even ids
        let existing_postings = vec![
            PostingUpdate::append(1, vec![1.0]),
            PostingUpdate::append(3, vec![3.0]),
            PostingUpdate::append(5, vec![5.0]),
        ];
        let existing_value = PostingListValue::from_posting_updates(existing_postings)
            .expect("unexpected error creating posting updates")
            .encode_to_bytes();

        let new_postings = vec![
            PostingUpdate::append(2, vec![2.0]),
            PostingUpdate::append(4, vec![4.0]),
        ];
        let new_value = PostingListValue::from_posting_updates(new_postings)
            .expect("unexpected error creating posting updates")
            .encode_to_bytes();

        // when
        let merged = merge_posting_list(existing_value, new_value, 1);
        let decoded = PostingListValue::decode_from_bytes(&merged, 1).unwrap();

        // then - all entries in sorted order: 1, 2, 3, 4, 5
        assert_eq!(decoded.len(), 5);
        assert_eq!(decoded.postings[0].id(), 1);
        assert_eq!(decoded.postings[1].id(), 2);
        assert_eq!(decoded.postings[2].id(), 3);
        assert_eq!(decoded.postings[3].id(), 4);
        assert_eq!(decoded.postings[4].id(), 5);
    }

    #[test]
    fn should_sort_postings_by_id_in_from_posting_updates() {
        // given - postings in unsorted order
        let postings = vec![
            PostingUpdate::append(5, vec![5.0]),
            PostingUpdate::append(1, vec![1.0]),
            PostingUpdate::append(3, vec![3.0]),
            PostingUpdate::append(2, vec![2.0]),
            PostingUpdate::append(4, vec![4.0]),
        ];

        // when
        let value = PostingListValue::from_posting_updates(postings)
            .expect("unexpected error creating posting updates");

        // then - postings are sorted by id
        assert_eq!(value.len(), 5);
        assert_eq!(value.postings[0].id(), 1);
        assert_eq!(value.postings[1].id(), 2);
        assert_eq!(value.postings[2].id(), 3);
        assert_eq!(value.postings[3].id(), 4);
        assert_eq!(value.postings[4].id(), 5);
    }

    #[test]
    fn should_serialize_in_id_order() {
        // given - postings created from unsorted input
        let postings = vec![
            PostingUpdate::append(3, vec![3.0, 3.0]),
            PostingUpdate::append(1, vec![1.0, 1.0]),
            PostingUpdate::append(2, vec![2.0, 2.0]),
        ];
        let value = PostingListValue::from_posting_updates(postings)
            .expect("unexpected error creating posting updates");

        // when - encode and decode
        let encoded = value.encode_to_bytes();
        let decoded = PostingListValue::decode_from_bytes(&encoded, 2).unwrap();

        // then - decoded postings are in id order
        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded.postings[0].id(), 1);
        assert_eq!(decoded.postings[1].id(), 2);
        assert_eq!(decoded.postings[2].id(), 3);
    }

    #[test]
    fn should_maintain_id_order_after_merge() {
        // given - two sorted posting lists
        let existing_postings = vec![
            PostingUpdate::append(10, vec![10.0]),
            PostingUpdate::append(30, vec![30.0]),
            PostingUpdate::append(50, vec![50.0]),
        ];
        let existing_value = PostingListValue::from_posting_updates(existing_postings)
            .expect("unexpected error creating posting updates")
            .encode_to_bytes();

        let new_postings = vec![
            PostingUpdate::append(20, vec![20.0]),
            PostingUpdate::append(30, vec![300.0]), // overwrites
            PostingUpdate::append(40, vec![40.0]),
        ];
        let new_value = PostingListValue::from_posting_updates(new_postings)
            .expect("unexpected error creating posting updates")
            .encode_to_bytes();

        // when
        let merged = merge_posting_list(existing_value, new_value, 1);
        let decoded = PostingListValue::decode_from_bytes(&merged, 1).unwrap();

        // then - result is in sorted order
        assert_eq!(decoded.len(), 5);
        assert_eq!(decoded.postings[0].id(), 10);
        assert_eq!(decoded.postings[1].id(), 20);
        assert_eq!(decoded.postings[2].id(), 30);
        assert_eq!(decoded.postings[2].vector().unwrap(), &[300.0]); // new value won
        assert_eq!(decoded.postings[3].id(), 40);
        assert_eq!(decoded.postings[4].id(), 50);
    }

    #[test]
    fn should_convert_to_posting_list_in_id_order() {
        // given - postings with mixed types, unsorted input
        let postings = vec![
            PostingUpdate::append(5, vec![5.0]),
            PostingUpdate::delete(3),
            PostingUpdate::append(1, vec![1.0]),
            PostingUpdate::append(4, vec![4.0]),
            PostingUpdate::append(2, vec![2.0]),
        ];
        let value = PostingListValue::from_posting_updates(postings)
            .expect("unexpected error creating posting updates");

        // when
        let posting_list: PostingList = value.into();

        // then - only appends, in sorted order (delete for id 3 is filtered out)
        assert_eq!(posting_list.len(), 4);
        assert_eq!(posting_list[0].id(), 1);
        assert_eq!(posting_list[1].id(), 2);
        assert_eq!(posting_list[2].id(), 4);
        assert_eq!(posting_list[3].id(), 5);
    }
}
