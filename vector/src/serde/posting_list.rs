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
//! The stored value is a split layout: a sorted-by-id header of fixed-size
//! `(id, offset)` entries, followed by a contiguous vector data blob.
//!
//! ```text
//! +-----------------------------------------------------------------+
//! |  count:    u32 LE                                               |
//! |  postings: count * (id: u64 BE, offset: u32 LE)                 |
//! |  data:     FixedElementArray<f32>  (LE f32s, back-to-back)      |
//! +-----------------------------------------------------------------+
//! ```
//!
//! `offset` is a byte offset into the `data` section. The sentinel value
//! `0xFFFFFFFF` (`u32::MAX`) marks a delete entry — delete entries occupy a
//! slot in the header but contribute no bytes to `data`.
//!
//! ## Merge Operators
//!
//! Posting lists use SlateDB merge operators with sort-merge semantics:
//! - Both existing and new values are sorted by id
//! - Merge performs a sort-merge, keeping the newer entry when ids match
//! - Output remains sorted by id

use super::{Decode, Encode, EncodingError};
use crate::serde::vector_id::VectorId;
use bytes::{BufMut, Bytes, BytesMut};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use tracing::debug_span;

/// Offset sentinel in the posting header meaning "this is a delete entry".
pub(crate) const POSTING_DELETE_OFFSET: u32 = u32::MAX;

/// Size of a single posting header entry: `id(u64) + offset(u32)`.
const POSTING_ENTRY_SIZE: usize = 8 + 4;

/// Size of the `count` prefix at the start of the encoded value.
const COUNT_PREFIX_SIZE: usize = 4;

/// A single posting update entry containing vector data.
///
/// Each entry represents either an append or delete operation on a vector
/// within a centroid's posting list.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PostingUpdate {
    /// Append a vector to the posting list (or overwrite if id already exists).
    Append {
        /// Internal vector ID.
        id: VectorId,
        /// Raw f32 vector bytes. Length is always `dimensions * 4`.
        ///
        /// The bytes are produced from a `Vec<f32>` via [`Bytes::from_owner`]
        /// so they are guaranteed to have `align_of::<f32>()` alignment.
        /// Consumers can `bytemuck::cast_slice` them back to `&[f32]`.
        vector: Bytes,
    },
    /// Delete the entry for `id` from the posting list.
    Delete {
        /// Internal vector ID
        id: VectorId,
    },
}

impl PostingUpdate {
    /// Create a new append posting update from a `Vec<f32>`.
    ///
    /// The `Vec<f32>` is moved into a `Bytes` via [`Bytes::from_owner`] so the
    /// underlying storage keeps its f32 alignment.
    pub(crate) fn append(id: VectorId, vector: Vec<f32>) -> Self {
        Self::Append {
            id,
            vector: aligned_f32_bytes_from_vec(vector),
        }
    }

    /// Create a new delete posting update.
    pub(crate) fn delete(id: VectorId) -> Self {
        Self::Delete { id }
    }

    /// Returns true if this is an append operation.
    #[allow(dead_code)]
    pub(crate) fn is_append(&self) -> bool {
        matches!(self, Self::Append { .. })
    }

    /// Returns true if this is a delete operation.
    #[allow(dead_code)]
    pub(crate) fn is_delete(&self) -> bool {
        matches!(self, Self::Delete { .. })
    }

    /// Returns the vector ID.
    pub(crate) fn id(&self) -> VectorId {
        match self {
            Self::Append { id, .. } => *id,
            Self::Delete { id } => *id,
        }
    }

    /// Returns the vector data if this is an Append, None if Delete.
    ///
    /// # Panics
    /// Panics if the backing `Bytes` is not aligned to `align_of::<f32>()`.
    /// The public constructors (`append`, decode) all produce aligned
    /// buffers, so this only fires on hand-constructed misaligned values.
    #[allow(dead_code)]
    pub(crate) fn vector(&self) -> Option<&[f32]> {
        match self {
            Self::Append { vector, .. } => Some(f32_slice_from_aligned_bytes(vector)),
            Self::Delete { .. } => None,
        }
    }
}

/// PostingList value storing vector updates for a centroid cluster.
///
/// Each posting list maps a single centroid ID to the list of vector updates
/// (appends or deletes) for that cluster.
///
/// On-disk layout is documented at the module level.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PostingListValue {
    /// List of posting updates (appends or deletes), sorted by id.
    pub(crate) postings: Vec<PostingUpdate>,
}

impl PostingListValue {
    /// Create an empty posting list.
    pub(crate) fn new() -> Self {
        Self {
            postings: Vec::new(),
        }
    }

    /// Create a posting list from a vector of updates.
    ///
    /// The updates are sorted by id to maintain the invariant that postings
    /// are always ordered by id. When multiple updates share the same id,
    /// the last occurrence wins (last-write-wins). This supports the case
    /// where a vector is appended to a posting list and then deleted within
    /// the same delta (e.g. during split reassignment).
    pub(crate) fn from_posting_updates(
        posting_updates: Vec<PostingUpdate>,
    ) -> Result<Self, EncodingError> {
        // Deduplicate by id, keeping the last occurrence (most recent operation).
        let mut last_by_id = std::collections::HashMap::new();
        for (idx, update) in posting_updates.iter().enumerate() {
            last_by_id.insert(update.id(), idx);
        }
        let mut deduped: Vec<PostingUpdate> = last_by_id
            .into_values()
            .map(|idx| posting_updates[idx].clone())
            .collect();
        deduped.sort_by_key(|p| p.id());

        Ok(Self { postings: deduped })
    }

    /// Returns the number of posting updates.
    #[allow(dead_code)]
    pub(crate) fn len(&self) -> usize {
        self.postings.len()
    }

    /// Returns true if the posting list is empty.
    #[allow(dead_code)]
    pub(crate) fn is_empty(&self) -> bool {
        self.postings.is_empty()
    }

    /// Returns an iterator over the posting updates.
    #[allow(dead_code)]
    pub(crate) fn iter(&self) -> impl Iterator<Item = &PostingUpdate> {
        self.postings.iter()
    }

    /// Encode to bytes in the split header/data layout described in the module docs.
    pub(crate) fn encode_to_bytes(&self) -> Bytes {
        if self.postings.is_empty() {
            return Bytes::new();
        }

        let count = u32::try_from(self.postings.len())
            .expect("posting list length exceeds u32::MAX entries");
        let header_size = COUNT_PREFIX_SIZE + POSTING_ENTRY_SIZE * self.postings.len();
        let data_size: usize = self
            .postings
            .iter()
            .map(|p| match p {
                PostingUpdate::Append { vector, .. } => vector.len(),
                PostingUpdate::Delete { .. } => 0,
            })
            .sum();

        let mut buf = BytesMut::with_capacity(header_size + data_size);
        buf.put_u32_le(count);

        // Write postings header. For each append, record its byte offset in the
        // yet-to-be-written data section.
        let mut data_cursor: u32 = 0;
        for p in &self.postings {
            match p {
                PostingUpdate::Append { id, vector } => {
                    id.encode(&mut buf);
                    buf.put_u32_le(data_cursor);
                    data_cursor = data_cursor
                        .checked_add(u32::try_from(vector.len()).expect("vector too large"))
                        .expect("posting data section exceeds u32::MAX bytes");
                }
                PostingUpdate::Delete { id } => {
                    id.encode(&mut buf);
                    buf.put_u32_le(POSTING_DELETE_OFFSET);
                }
            }
        }

        // Data section: append vectors back-to-back in header order.
        for p in &self.postings {
            if let PostingUpdate::Append { vector, .. } = p {
                buf.extend_from_slice(vector);
            }
        }

        buf.freeze()
    }

    /// Decode from bytes.
    ///
    /// Requires `dimensions` to know how many f32s belong to each append
    /// entry.
    ///
    /// Takes `&Bytes` rather than `&[u8]` so each decoded `Append` can share
    /// ownership of the input allocation: whenever the input's pointer plus
    /// the entry offset lands on an f32-aligned address, we hand out a
    /// `Bytes::slice` into the same buffer with no copy. When the resulting
    /// address isn't aligned we fall back to copying that entry's vector
    /// bytes into a fresh aligned allocation.
    pub(crate) fn decode_from_bytes(buf: &Bytes, dimensions: usize) -> Result<Self, EncodingError> {
        if buf.is_empty() {
            return Ok(PostingListValue::new());
        }
        if buf.len() < COUNT_PREFIX_SIZE {
            return Err(EncodingError {
                message: format!(
                    "Buffer too short for PostingListValue count: expected at least {} bytes, got {}",
                    COUNT_PREFIX_SIZE,
                    buf.len()
                ),
            });
        }

        let count = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        let header_size = COUNT_PREFIX_SIZE + POSTING_ENTRY_SIZE * count;
        if buf.len() < header_size {
            return Err(EncodingError {
                message: format!(
                    "Buffer too short for {} postings: expected at least {} bytes, got {}",
                    count,
                    header_size,
                    buf.len()
                ),
            });
        }

        let data_start = header_size;
        let data_len = buf.len() - data_start;
        let vector_bytes = dimensions * 4;
        let align = std::mem::align_of::<f32>();
        let buf_ptr = buf.as_ptr() as usize;

        let mut cursor = &buf[COUNT_PREFIX_SIZE..header_size];
        let mut postings = Vec::with_capacity(count);
        for _ in 0..count {
            let id = VectorId::decode(&mut cursor)?;
            let offset = u32::from_le_bytes([cursor[0], cursor[1], cursor[2], cursor[3]]);
            cursor = &cursor[4..];

            if offset == POSTING_DELETE_OFFSET {
                postings.push(PostingUpdate::Delete { id });
                continue;
            }

            let start = offset as usize;
            let end = start
                .checked_add(vector_bytes)
                .ok_or_else(|| EncodingError {
                    message: format!(
                        "posting offset overflow: offset={}, vector_bytes={}",
                        offset, vector_bytes
                    ),
                })?;
            if end > data_len {
                return Err(EncodingError {
                    message: format!(
                        "posting offset {} out of bounds: data size {}, entry ends at {}",
                        offset, data_len, end
                    ),
                });
            }
            let abs_start = data_start + start;
            let abs_end = data_start + end;
            let vector = if (buf_ptr + abs_start).is_multiple_of(align) {
                // Entry sits on an f32-aligned address — share the input
                // buffer directly.
                buf.slice(abs_start..abs_end)
            } else {
                aligned_f32_bytes_from_slice(&buf[abs_start..abs_end])
            };
            postings.push(PostingUpdate::Append { id, vector });
        }

        Ok(Self { postings })
    }
}

impl Default for PostingListValue {
    fn default() -> Self {
        Self::new()
    }
}

/// K-way merge for posting lists across multiple operands in the new format.
///
/// Merges `existing` (oldest, priority 0) with `operands` (priority 1..N, newest last)
/// by sort-merging the fixed-size header entries on id, then copying each
/// winner's vector bytes into a new data section with rewritten offsets.
/// For equal IDs, the highest-priority (newest) entry wins. Operands are
/// ordered oldest-to-newest per SlateDB convention.
///
/// Short-circuits: returns the single input as-is when only one source is present.
pub(crate) fn merge_batch_posting_list(
    existing: Option<Bytes>,
    operands: &[Bytes],
    dimensions: usize,
) -> Bytes {
    let n_records = operands.len() + existing.is_some() as usize;
    let _span = debug_span!("merge_posting_list", n_records = n_records).entered();

    // Short-circuit: single source, no merge needed
    if operands.is_empty() {
        return existing.unwrap_or_default();
    }
    if operands.len() == 1 && existing.is_none() {
        return operands[0].clone();
    }

    let vector_bytes = dimensions * 4;

    struct Cursor {
        buf: Bytes,
        /// Byte offset of the next unread posting header entry into `buf`.
        header_pos: usize,
        /// One past the last header entry byte in `buf`.
        header_end: usize,
        /// Byte offset of the data section in `buf`.
        data_start: usize,
        priority: usize,
    }

    fn parse_header(buf: Bytes, priority: usize) -> Option<Cursor> {
        if buf.len() < COUNT_PREFIX_SIZE {
            return None;
        }
        let count = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        if count == 0 {
            return None;
        }
        let header_end = COUNT_PREFIX_SIZE + POSTING_ENTRY_SIZE * count;
        if buf.len() < header_end {
            return None;
        }
        Some(Cursor {
            buf,
            header_pos: COUNT_PREFIX_SIZE,
            header_end,
            data_start: header_end,
            priority,
        })
    }

    fn peek_entry(cursor: &Cursor) -> Option<(VectorId, u32)> {
        if cursor.header_pos >= cursor.header_end {
            return None;
        }
        let entry = &cursor.buf[cursor.header_pos..cursor.header_pos + POSTING_ENTRY_SIZE];
        let mut id_buf = &entry[..8];
        let id = VectorId::decode(&mut id_buf).expect("failed to decode vector id");
        let offset = u32::from_le_bytes([entry[8], entry[9], entry[10], entry[11]]);
        Some((id, offset))
    }

    let mut cursors: Vec<Cursor> = Vec::with_capacity(1 + operands.len());
    if let Some(ex) = existing
        && let Some(c) = parse_header(ex, 0)
    {
        cursors.push(c);
    }
    for (i, op) in operands.iter().enumerate() {
        if let Some(c) = parse_header(op.clone(), i + 1) {
            cursors.push(c);
        }
    }

    if cursors.is_empty() {
        return Bytes::new();
    }

    // Heap: min-heap by id, max-heap by priority (newest wins on ties).
    #[derive(Eq, PartialEq)]
    struct HeapEntry {
        id: VectorId,
        priority: usize,
        cursor_idx: usize,
        offset: u32,
    }

    impl Ord for HeapEntry {
        fn cmp(&self, other: &Self) -> Ordering {
            other
                .id
                .cmp(&self.id)
                .then_with(|| self.priority.cmp(&other.priority))
        }
    }

    impl PartialOrd for HeapEntry {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    let mut heap = BinaryHeap::with_capacity(cursors.len());
    for (idx, cursor) in cursors.iter().enumerate() {
        if let Some((id, offset)) = peek_entry(cursor) {
            heap.push(HeapEntry {
                id,
                priority: cursor.priority,
                cursor_idx: idx,
                offset,
            });
        }
    }

    // Estimate output size as the sum of all inputs — an upper bound since
    // duplicate ids collapse into a single output entry.
    let total_header_entries: usize = cursors
        .iter()
        .map(|c| (c.header_end - COUNT_PREFIX_SIZE) / POSTING_ENTRY_SIZE)
        .sum();
    let mut out_header = BytesMut::with_capacity(POSTING_ENTRY_SIZE * total_header_entries);
    let total_data_bytes: usize = cursors.iter().map(|c| c.buf.len() - c.data_start).sum();
    let mut out_data = BytesMut::with_capacity(total_data_bytes);

    let mut out_count: u32 = 0;

    while let Some(winner) = heap.pop() {
        let id = winner.id;

        // Emit winner's entry with rewritten offset.
        winner_id_encode(&id, &mut out_header);
        if winner.offset == POSTING_DELETE_OFFSET {
            out_header.put_u32_le(POSTING_DELETE_OFFSET);
        } else {
            let new_offset =
                u32::try_from(out_data.len()).expect("merged data section exceeds u32::MAX bytes");
            out_header.put_u32_le(new_offset);
            let cursor = &cursors[winner.cursor_idx];
            let src_start = cursor.data_start + winner.offset as usize;
            let src_end = src_start + vector_bytes;
            assert!(
                src_end <= cursor.buf.len(),
                "winner vector slice out of bounds: {}..{} of {}",
                src_start,
                src_end,
                cursor.buf.len()
            );
            out_data.extend_from_slice(&cursor.buf[src_start..src_end]);
        }
        out_count += 1;

        // Advance the winner's cursor.
        let cursor = &mut cursors[winner.cursor_idx];
        cursor.header_pos += POSTING_ENTRY_SIZE;
        if let Some((next_id, next_offset)) = peek_entry(cursor) {
            heap.push(HeapEntry {
                id: next_id,
                priority: cursor.priority,
                cursor_idx: winner.cursor_idx,
                offset: next_offset,
            });
        }

        // Skip all other entries with the same id (losers).
        while heap.peek().is_some_and(|e| e.id == id) {
            let loser = heap.pop().unwrap();
            let cursor = &mut cursors[loser.cursor_idx];
            cursor.header_pos += POSTING_ENTRY_SIZE;
            if let Some((next_id, next_offset)) = peek_entry(cursor) {
                heap.push(HeapEntry {
                    id: next_id,
                    priority: cursor.priority,
                    cursor_idx: loser.cursor_idx,
                    offset: next_offset,
                });
            }
        }
    }

    if out_count == 0 {
        return Bytes::new();
    }

    // Assemble: count || header || data.
    //
    // Over-allocate by `align - 1` bytes so we can shift the visible payload
    // forward by up to that many bytes and land the returned `Bytes` pointer
    // on an f32-aligned address, regardless of what the allocator gave us.
    // Downstream readers (e.g. `PostingList::from_value`) can then read the
    // data section via `bytemuck::cast_slice` with no extra copy.
    let payload_size = COUNT_PREFIX_SIZE + out_header.len() + out_data.len();
    let align = std::mem::align_of::<f32>();
    let mut out = BytesMut::with_capacity(payload_size + align - 1);
    // `align_offset` returns `usize::MAX` only for pointer kinds we never see
    // from a real allocation (ZSTs, vtables). Treat it as "already aligned".
    let skip = match out.as_ptr().align_offset(align) {
        usize::MAX => 0,
        n => n,
    };
    for _ in 0..skip {
        out.put_u8(0);
    }
    out.put_u32_le(out_count);
    out.extend_from_slice(&out_header);
    out.extend_from_slice(&out_data);
    // `BytesMut::with_capacity(payload_size + align - 1)` above guarantees
    // the allocation is large enough for `skip + payload_size` bytes, so no
    // reallocation happened while writing — the pointer we sampled is still
    // valid. Slicing past the padding yields an aligned-start `Bytes`.
    out.freeze().slice(skip..)
}

/// Helper: write a VectorId header via its `Encode` impl.
fn winner_id_encode(id: &VectorId, buf: &mut BytesMut) {
    id.encode(buf);
}

/// Owner wrapper for a `Vec<f32>` exposed as its raw byte representation, so
/// we can hand ownership to `Bytes::from_owner` while preserving f32
/// alignment of the underlying allocation.
struct AlignedF32Buf(Vec<f32>);

impl AsRef<[u8]> for AlignedF32Buf {
    fn as_ref(&self) -> &[u8] {
        bytemuck::cast_slice(&self.0)
    }
}

/// Turn a `Vec<f32>` into a `Bytes` whose pointer is guaranteed to have
/// `align_of::<f32>()` alignment (the Vec's natural allocator alignment).
fn aligned_f32_bytes_from_vec(vector: Vec<f32>) -> Bytes {
    Bytes::from_owner(AlignedF32Buf(vector))
}

/// Copy an arbitrary `&[u8]` of vector bytes into a fresh, f32-aligned
/// `Bytes`. Used during decode where the source bytes might start at any
/// byte offset within the larger posting list buffer.
fn aligned_f32_bytes_from_slice(bytes: &[u8]) -> Bytes {
    assert!(
        bytes.len().is_multiple_of(std::mem::size_of::<f32>()),
        "vector byte slice not a multiple of sizeof::<f32>(): {}",
        bytes.len()
    );
    let mut v = vec![0f32; bytes.len() / std::mem::size_of::<f32>()];
    bytemuck::cast_slice_mut::<f32, u8>(&mut v).copy_from_slice(bytes);
    aligned_f32_bytes_from_vec(v)
}

/// Cast a guaranteed-aligned `Bytes` payload to `&[f32]`.
///
/// # Panics
/// Panics if the payload isn't 4-byte aligned. Our constructors never
/// produce misaligned vectors (see [`aligned_f32_bytes_from_vec`]).
fn f32_slice_from_aligned_bytes(bytes: &Bytes) -> &[f32] {
    assert!(
        (bytes.as_ptr() as usize).is_multiple_of(std::mem::align_of::<f32>()),
        "vector bytes are not aligned to f32; PostingUpdate constructors must produce aligned buffers"
    );
    bytemuck::cast_slice(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::write::indexer::tree::posting_list::PostingList;

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

    fn id(id: u64) -> VectorId {
        VectorId::data_vector_id(id)
    }

    trait CompatId {
        fn into_vector_id(self) -> VectorId;
    }

    impl CompatId for u64 {
        fn into_vector_id(self) -> VectorId {
            id(self)
        }
    }

    impl CompatId for VectorId {
        fn into_vector_id(self) -> VectorId {
            self
        }
    }

    struct PostingUpdate;

    impl PostingUpdate {
        fn append(id_num: impl CompatId, vector: Vec<f32>) -> super::PostingUpdate {
            super::PostingUpdate::append(id_num.into_vector_id(), vector)
        }

        fn delete(id_num: impl CompatId) -> super::PostingUpdate {
            super::PostingUpdate::delete(id_num.into_vector_id())
        }
    }

    struct Posting;

    impl Posting {
        #[allow(clippy::new_ret_no_self)]
        fn new(
            id_num: impl CompatId,
            vector: Vec<f32>,
        ) -> crate::write::indexer::tree::posting_list::Posting {
            crate::write::indexer::tree::posting_list::Posting::new(id_num.into_vector_id(), vector)
        }
    }

    #[test]
    fn should_encode_and_decode_posting_list_with_appends() {
        // given
        let postings = vec![
            PostingUpdate::append(id(1), vec![1.0, 2.0, 3.0]),
            PostingUpdate::append(id(2), vec![4.0, 5.0, 6.0]),
        ];
        let value = PostingListValue::from_posting_updates(postings)
            .expect("unexpected error creating posting updates");

        // when
        let encoded = value.encode_to_bytes();
        let decoded = PostingListValue::decode_from_bytes(&encoded, 3).unwrap();

        // then
        assert_eq!(decoded.len(), 2);
        assert!(decoded.postings[0].is_append());
        assert_eq!(decoded.postings[0].id(), id(1));
        assert_eq!(decoded.postings[0].vector().unwrap(), &[1.0, 2.0, 3.0]);
        assert!(decoded.postings[1].is_append());
        assert_eq!(decoded.postings[1].id(), id(2));
        assert_eq!(decoded.postings[1].vector().unwrap(), &[4.0, 5.0, 6.0]);
    }

    #[test]
    fn should_encode_and_decode_posting_list_with_deletes() {
        // given
        let postings = vec![PostingUpdate::delete(id(1)), PostingUpdate::delete(id(2))];
        let value = PostingListValue::from_posting_updates(postings)
            .expect("unexpected error creating posting updates");

        // when
        let encoded = value.encode_to_bytes();
        let decoded = PostingListValue::decode_from_bytes(&encoded, 3).unwrap();

        // then
        assert_eq!(decoded.len(), 2);
        assert!(decoded.postings[0].is_delete());
        assert_eq!(decoded.postings[0].id(), id(1));
        assert!(decoded.postings[1].is_delete());
        assert_eq!(decoded.postings[1].id(), id(2));
    }

    #[test]
    fn should_interleave_appends_and_deletes() {
        // given - mixed entries in the same value
        let postings = vec![
            PostingUpdate::append(1, vec![1.0, 1.0]),
            PostingUpdate::delete(2),
            PostingUpdate::append(3, vec![3.0, 3.0]),
        ];
        let value = PostingListValue::from_posting_updates(postings).unwrap();

        // when
        let encoded = value.encode_to_bytes();
        let decoded = PostingListValue::decode_from_bytes(&encoded, 2).unwrap();

        // then
        assert_eq!(decoded.len(), 3);
        assert!(decoded.postings[0].is_append());
        assert_eq!(decoded.postings[0].vector().unwrap(), &[1.0, 1.0]);
        assert!(decoded.postings[1].is_delete());
        assert!(decoded.postings[2].is_append());
        assert_eq!(decoded.postings[2].vector().unwrap(), &[3.0, 3.0]);
    }

    #[test]
    fn should_create_posting_update_append() {
        // given / when
        let update = PostingUpdate::append(42, vec![1.0, 2.0]);

        // then
        assert!(update.is_append());
        assert!(!update.is_delete());
        assert_eq!(update.id(), id(42));
        assert_eq!(update.vector().unwrap(), &[1.0, 2.0]);
    }

    #[test]
    fn should_create_posting_update_delete() {
        // given / when
        let update = PostingUpdate::delete(42);

        // then
        assert!(!update.is_append());
        assert!(update.is_delete());
        assert_eq!(update.id(), id(42));
    }

    #[test]
    fn should_reject_buffer_too_short_for_header() {
        // given - only 2 bytes, not enough for u32 count
        let buf = Bytes::copy_from_slice(&[0u8; 2]);

        // when
        let result = PostingListValue::decode_from_bytes(&buf, 3);

        // then
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains("Buffer too short"));
    }

    #[test]
    fn should_reject_buffer_too_short_for_postings() {
        // given - claims 2 postings but header is truncated
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&2u32.to_le_bytes());
        buf.extend_from_slice(&[0u8; 5]); // less than 2 * 12 bytes
        let buf = buf.freeze();

        // when
        let result = PostingListValue::decode_from_bytes(&buf, 3);

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
        let postings: PostingList = PostingList::from_value(value);

        // then - sorted by id
        assert_eq!(
            postings.into_iter().collect::<Vec<_>>(),
            vec![
                Posting::new(id(1), vec![1.0]),
                Posting::new(id(2), vec![2.0]),
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
        let postings: PostingList = PostingList::from_value(value);

        // then - delete entry not in result, sorted by id
        assert_eq!(
            postings.into_iter().collect::<Vec<_>>(),
            vec![
                Posting::new(id(1), vec![1.0]),
                Posting::new(id(2), vec![2.0])
            ]
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
        let merged = merge_batch_posting_list(Some(existing_value), &[new_value], 3);
        let decoded = PostingListValue::decode_from_bytes(&merged, 3).unwrap();

        // then - all 3 unique ids preserved in sorted order
        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded.postings[0].id(), id(1));
        assert_eq!(decoded.postings[1].id(), id(2));
        assert_eq!(decoded.postings[2].id(), id(3));
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
        let merged = merge_batch_posting_list(Some(existing_value), &[new_value], 2);
        let decoded = PostingListValue::decode_from_bytes(&merged, 2).unwrap();

        // then - id 1 is now a delete, id 2 unchanged, sorted order
        assert_eq!(decoded.len(), 2);
        assert!(decoded.postings[0].is_delete());
        assert_eq!(decoded.postings[0].id(), id(1));
        assert!(decoded.postings[1].is_append());
        assert_eq!(decoded.postings[1].id(), id(2));
    }

    #[test]
    fn should_merge_with_multiple_deletes_masking_old_append() {
        // given - existing has append, new has delete for same id
        let existing_postings = vec![
            PostingUpdate::append(VectorId::centroid_id(2, 4496), vec![1.0, 2.0]),
            PostingUpdate::append(VectorId::centroid_id(2, 4497), vec![3.0, 4.0]),
        ];
        let existing_value = PostingListValue::from_posting_updates(existing_postings)
            .expect("unexpected error creating posting updates")
            .encode_to_bytes();

        let new_postings = vec![
            PostingUpdate::delete(VectorId::centroid_id(2, 4496)),
            PostingUpdate::delete(VectorId::centroid_id(2, 4497)),
            PostingUpdate::append(VectorId::centroid_id(2, 4728), vec![4.0, 5.0]),
            PostingUpdate::append(VectorId::centroid_id(2, 4729), vec![4.0, 5.0]),
            PostingUpdate::append(VectorId::centroid_id(2, 4730), vec![4.0, 5.0]),
            PostingUpdate::append(VectorId::centroid_id(2, 4731), vec![4.0, 5.0]),
        ];
        let new_value = PostingListValue::from_posting_updates(new_postings)
            .expect("unexpected error creating posting updates")
            .encode_to_bytes();

        // when
        let merged = merge_batch_posting_list(None, &[existing_value, new_value], 2);
        let decoded = PostingListValue::decode_from_bytes(&merged, 2).unwrap();

        // then - id 1/2 is now a delete, id 3 unchanged, sorted order
        assert_eq!(decoded.len(), 6);
        assert_eq!(decoded.postings[0].id(), VectorId::centroid_id(2, 4496));
        assert!(decoded.postings[0].is_delete());
        assert_eq!(decoded.postings[1].id(), VectorId::centroid_id(2, 4497));
        assert!(decoded.postings[1].is_delete());
        assert_eq!(decoded.postings[2].id(), VectorId::centroid_id(2, 4728));
        assert!(decoded.postings[2].is_append());
        assert_eq!(decoded.postings[3].id(), VectorId::centroid_id(2, 4729));
        assert!(decoded.postings[3].is_append());
        assert_eq!(decoded.postings[4].id(), VectorId::centroid_id(2, 4730));
        assert!(decoded.postings[4].is_append());
        assert_eq!(decoded.postings[5].id(), VectorId::centroid_id(2, 4731));
        assert!(decoded.postings[5].is_append());
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
        let merged = merge_batch_posting_list(Some(existing_value), &[new_value], 2);
        let decoded = PostingListValue::decode_from_bytes(&merged, 2).unwrap();

        // then - id 1 has new vector, id 2 unchanged, sorted order
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded.postings[0].id(), id(1));
        assert_eq!(decoded.postings[0].vector().unwrap(), &[100.0, 200.0]);
        assert_eq!(decoded.postings[1].id(), id(2));
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
        let merged = merge_batch_posting_list(Some(existing_value), &[new_value], 2);
        let decoded = PostingListValue::decode_from_bytes(&merged, 2).unwrap();

        // then - new entries are preserved in sorted order
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded.postings[0].id(), id(1));
        assert_eq!(decoded.postings[1].id(), id(2));
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
        let merged = merge_batch_posting_list(Some(existing_value), &[new_value], 2);
        let decoded = PostingListValue::decode_from_bytes(&merged, 2).unwrap();

        // then - existing entries are preserved in sorted order
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded.postings[0].id(), id(1));
        assert_eq!(decoded.postings[1].id(), id(2));
    }

    #[test]
    fn should_merge_both_empty() {
        // given - both empty
        let existing_value = PostingListValue::new().encode_to_bytes();
        let new_value = PostingListValue::new().encode_to_bytes();

        // when
        let merged = merge_batch_posting_list(Some(existing_value), &[new_value], 2);
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
        let merged = merge_batch_posting_list(Some(existing_value), &[new_value], 1);
        let decoded = PostingListValue::decode_from_bytes(&merged, 1).unwrap();

        // then - all entries in sorted order: 1, 2, 3, 4, 5
        assert_eq!(decoded.len(), 5);
        assert_eq!(decoded.postings[0].id(), id(1));
        assert_eq!(decoded.postings[1].id(), id(2));
        assert_eq!(decoded.postings[2].id(), id(3));
        assert_eq!(decoded.postings[3].id(), id(4));
        assert_eq!(decoded.postings[4].id(), id(5));
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
        assert_eq!(value.postings[0].id(), id(1));
        assert_eq!(value.postings[1].id(), id(2));
        assert_eq!(value.postings[2].id(), id(3));
        assert_eq!(value.postings[3].id(), id(4));
        assert_eq!(value.postings[4].id(), id(5));
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
        assert_eq!(decoded.postings[0].id(), id(1));
        assert_eq!(decoded.postings[1].id(), id(2));
        assert_eq!(decoded.postings[2].id(), id(3));
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
        let merged = merge_batch_posting_list(Some(existing_value), &[new_value], 1);
        let decoded = PostingListValue::decode_from_bytes(&merged, 1).unwrap();

        // then - result is in sorted order
        assert_eq!(decoded.len(), 5);
        assert_eq!(decoded.postings[0].id(), id(10));
        assert_eq!(decoded.postings[1].id(), id(20));
        assert_eq!(decoded.postings[2].id(), id(30));
        assert_eq!(decoded.postings[2].vector().unwrap(), &[300.0]); // new value won
        assert_eq!(decoded.postings[3].id(), id(40));
        assert_eq!(decoded.postings[4].id(), id(50));
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
        let posting_list = PostingList::from_value(value)
            .into_iter()
            .collect::<Vec<_>>();

        // then - only appends, in sorted order (delete for id 3 is filtered out)
        assert_eq!(posting_list.len(), 4);
        assert_eq!(posting_list[0].id(), id(1));
        assert_eq!(posting_list[1].id(), id(2));
        assert_eq!(posting_list[2].id(), id(4));
        assert_eq!(posting_list[3].id(), id(5));
    }

    #[test]
    fn should_merge_batch_posting_list_newer_operand_wins() {
        // given - 3 operands with overlapping IDs; operands[2] is newest
        let op0 = PostingListValue::from_posting_updates(vec![
            PostingUpdate::append(1, vec![10.0]),
            PostingUpdate::append(2, vec![20.0]),
        ])
        .unwrap()
        .encode_to_bytes();
        let op1 = PostingListValue::from_posting_updates(vec![
            PostingUpdate::append(2, vec![200.0]),
            PostingUpdate::append(3, vec![30.0]),
        ])
        .unwrap()
        .encode_to_bytes();
        let op2 = PostingListValue::from_posting_updates(vec![
            PostingUpdate::append(2, vec![2000.0]),
            PostingUpdate::append(4, vec![40.0]),
        ])
        .unwrap()
        .encode_to_bytes();

        // when
        let merged = merge_batch_posting_list(None, &[op0, op1, op2], 1);
        let decoded = PostingListValue::decode_from_bytes(&merged, 1).unwrap();

        // then - id 2 has value from op2 (newest), all 4 ids present
        assert_eq!(decoded.len(), 4);
        assert_eq!(decoded.postings[0].id(), id(1));
        assert_eq!(decoded.postings[0].vector().unwrap(), &[10.0]);
        assert_eq!(decoded.postings[1].id(), id(2));
        assert_eq!(decoded.postings[1].vector().unwrap(), &[2000.0]);
        assert_eq!(decoded.postings[2].id(), id(3));
        assert_eq!(decoded.postings[2].vector().unwrap(), &[30.0]);
        assert_eq!(decoded.postings[3].id(), id(4));
        assert_eq!(decoded.postings[3].vector().unwrap(), &[40.0]);
    }

    #[test]
    fn should_merge_batch_posting_list_with_existing_value() {
        // given - existing + 2 operands
        let existing = PostingListValue::from_posting_updates(vec![
            PostingUpdate::append(1, vec![1.0, 2.0]),
            PostingUpdate::append(3, vec![3.0, 4.0]),
        ])
        .unwrap()
        .encode_to_bytes();
        let op0 =
            PostingListValue::from_posting_updates(vec![PostingUpdate::append(2, vec![5.0, 6.0])])
                .unwrap()
                .encode_to_bytes();
        let op1 =
            PostingListValue::from_posting_updates(vec![PostingUpdate::append(4, vec![7.0, 8.0])])
                .unwrap()
                .encode_to_bytes();

        // when
        let merged = merge_batch_posting_list(Some(existing), &[op0, op1], 2);
        let decoded = PostingListValue::decode_from_bytes(&merged, 2).unwrap();

        // then - all 4 ids present in sorted order
        assert_eq!(decoded.len(), 4);
        assert_eq!(decoded.postings[0].id(), id(1));
        assert_eq!(decoded.postings[1].id(), id(2));
        assert_eq!(decoded.postings[2].id(), id(3));
        assert_eq!(decoded.postings[3].id(), id(4));
    }

    #[test]
    fn should_merge_batch_posting_list_single_operand_no_existing() {
        // given - single operand, no existing
        let op = PostingListValue::from_posting_updates(vec![
            PostingUpdate::append(1, vec![1.0]),
            PostingUpdate::append(2, vec![2.0]),
        ])
        .unwrap()
        .encode_to_bytes();
        let original = op.clone();

        // when
        let merged = merge_batch_posting_list(None, &[op], 1);

        // then - returns operand as-is (short-circuit)
        assert_eq!(merged, original);
    }

    #[test]
    fn should_merge_batch_posting_list_no_operands_returns_existing() {
        // given - existing value, no operands
        let existing = PostingListValue::from_posting_updates(vec![
            PostingUpdate::append(1, vec![1.0]),
            PostingUpdate::append(2, vec![2.0]),
        ])
        .unwrap()
        .encode_to_bytes();
        let original = existing.clone();

        // when
        let merged = merge_batch_posting_list(Some(existing), &[], 1);

        // then - returns existing as-is
        assert_eq!(merged, original);
    }

    #[test]
    fn should_merge_batch_posting_list_delete_in_newer_wins() {
        // given - older operand appends id 2, newer operand deletes id 2
        let op0 = PostingListValue::from_posting_updates(vec![
            PostingUpdate::append(1, vec![1.0]),
            PostingUpdate::append(2, vec![2.0]),
            PostingUpdate::append(3, vec![3.0]),
        ])
        .unwrap()
        .encode_to_bytes();
        let op1 = PostingListValue::from_posting_updates(vec![PostingUpdate::delete(2)])
            .unwrap()
            .encode_to_bytes();

        // when
        let merged = merge_batch_posting_list(None, &[op0, op1], 1);
        let decoded = PostingListValue::decode_from_bytes(&merged, 1).unwrap();

        // then - id 2 is a delete (from newer operand)
        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded.postings[0].id(), id(1));
        assert!(decoded.postings[0].is_append());
        assert_eq!(decoded.postings[1].id(), id(2));
        assert!(decoded.postings[1].is_delete());
        assert_eq!(decoded.postings[2].id(), id(3));
        assert!(decoded.postings[2].is_append());
    }

    #[test]
    fn should_reuse_input_buffer_when_decoding_aligned_bytes() {
        // given - encode something so the value's data section is aligned
        // relative to the buffer start (encode_to_bytes produces a
        // header whose size is a multiple of 4, so data starts at a 4-aligned
        // offset from `buf.as_ptr()`; combined with an aligned allocation
        // this means every append should be zero-copy).
        let value = PostingListValue::from_posting_updates(vec![
            PostingUpdate::append(1, vec![1.0, 2.0]),
            PostingUpdate::append(2, vec![3.0, 4.0]),
        ])
        .unwrap();
        let encoded = value.encode_to_bytes();

        // Skip the test if the allocator happened to give us an unaligned
        // buffer — decode still succeeds (it'd fall back to copying) but the
        // pointer-identity assertion wouldn't apply.
        if !(encoded.as_ptr() as usize).is_multiple_of(std::mem::align_of::<f32>()) {
            return;
        }
        let encoded_ptr = encoded.as_ptr() as usize;
        let encoded_end = encoded_ptr + encoded.len();

        // when
        let decoded = PostingListValue::decode_from_bytes(&encoded, 2).unwrap();

        // then - each Append's vector Bytes points into the encoded buffer.
        let mut seen_appends = 0;
        for p in &decoded.postings {
            if let super::PostingUpdate::Append { vector, .. } = p {
                let ptr = vector.as_ptr() as usize;
                assert!(
                    ptr >= encoded_ptr && ptr + vector.len() <= encoded_end,
                    "append vector Bytes ptr {:#x} is not a slice of encoded buffer {:#x}..{:#x}",
                    ptr,
                    encoded_ptr,
                    encoded_end,
                );
                seen_appends += 1;
            }
        }
        assert_eq!(seen_appends, 2);
    }

    #[test]
    fn should_copy_when_decoding_unaligned_bytes() {
        // given - an aligned encoded value plus a single byte of leading
        // padding, then a slice past the padding so the "outer" buffer is
        // still the original allocation but the viewed start is offset by 1.
        // The data section's absolute address is therefore 1 mod 4, which
        // forces the fallback copy path.
        let value =
            PostingListValue::from_posting_updates(vec![PostingUpdate::append(1, vec![1.0, 2.0])])
                .unwrap();
        let encoded = value.encode_to_bytes();

        // Stitch one byte of padding in front by re-encoding into a larger
        // buffer; we don't use BytesMut::with_capacity + 1 leading byte
        // because whether that actually produces an unaligned pointer
        // depends on the allocator. Instead we concatenate and then slice
        // past the padding.
        let mut padded = BytesMut::with_capacity(encoded.len() + 1);
        padded.put_u8(0);
        padded.extend_from_slice(&encoded);
        let padded = padded.freeze().slice(1..);
        if (padded.as_ptr() as usize).is_multiple_of(std::mem::align_of::<f32>()) {
            // Allocator happened to give us a pointer that's aligned even
            // after the 1-byte shift — skip; this test is about the
            // misaligned branch.
            return;
        }
        let padded_ptr_range = padded.as_ptr() as usize..padded.as_ptr() as usize + padded.len();

        // when
        let decoded = PostingListValue::decode_from_bytes(&padded, 2).unwrap();

        // then - the append's Bytes is a fresh allocation, not a slice of padded
        let super::PostingUpdate::Append { vector, .. } = &decoded.postings[0] else {
            panic!("expected Append")
        };
        let vp = vector.as_ptr() as usize;
        assert!(
            !padded_ptr_range.contains(&vp),
            "expected copy, but vector ptr {:#x} is still inside input range {:#x}..{:#x}",
            vp,
            padded_ptr_range.start,
            padded_ptr_range.end,
        );
        // and the values round-trip correctly
        assert_eq!(decoded.postings[0].vector().unwrap(), &[1.0, 2.0]);
    }

    #[test]
    fn should_return_f32_aligned_bytes_from_merge() {
        // given - a handful of inputs so we exercise the assembly path
        // (short-circuits don't go through the padding logic)
        let existing = PostingListValue::from_posting_updates(vec![
            PostingUpdate::append(1, vec![1.0, 2.0, 3.0]),
            PostingUpdate::append(3, vec![4.0, 5.0, 6.0]),
        ])
        .unwrap()
        .encode_to_bytes();
        let op0 = PostingListValue::from_posting_updates(vec![PostingUpdate::append(
            2,
            vec![7.0, 8.0, 9.0],
        )])
        .unwrap()
        .encode_to_bytes();
        let op1 = PostingListValue::from_posting_updates(vec![PostingUpdate::delete(3)])
            .unwrap()
            .encode_to_bytes();

        // when — run merge enough times to exercise every residue class of
        // the allocator's pointer mod 4. 16 rounds is plenty; each iteration
        // independently allocates so we see a variety of start pointers.
        for _ in 0..16 {
            let merged =
                merge_batch_posting_list(Some(existing.clone()), &[op0.clone(), op1.clone()], 3);
            assert!(
                (merged.as_ptr() as usize).is_multiple_of(std::mem::align_of::<f32>()),
                "merge output not f32-aligned: ptr={:p}",
                merged.as_ptr()
            );
        }
    }
}
