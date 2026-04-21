use crate::serde::posting_list::{PostingListValue, PostingUpdate};
use crate::serde::vector_id::VectorId;
use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;

/// Compile-time assertion that this build target is little-endian. The
/// on-disk posting list format stores f32 values as raw bytes and we expose
/// them to callers via `bytemuck::cast_slice`; that reinterpret-cast only
/// preserves values on little-endian targets.
const _: () = assert!(
    cfg!(target_endian = "little"),
    "opendata-vector assumes a little-endian target"
);

/// An in-memory format for posting lists optimized for exhaustive ANN search
/// over all postings. Every vector for a given posting list lives in a single
/// shared `Bytes` buffer, and each [`Posting`] holds a byte offset into it.
/// The buffer is always `align_of::<f32>()`-aligned so [`Posting::vector`]
/// can reinterpret the bytes as `&[f32]` without a copy.
#[derive(Clone)]
pub(crate) struct Posting {
    id: VectorId,
    buffer: Bytes,
    /// Byte offset into `buffer` where this posting's vector bytes start.
    offset: usize,
    /// Number of f32 elements (not bytes) that make up this posting's vector.
    dimensions: usize,
}

impl Posting {
    pub(crate) fn new(id: VectorId, vector: Vec<f32>) -> Self {
        let dimensions = vector.len();
        Self {
            id,
            buffer: aligned_bytes_from_f32_vec(vector),
            offset: 0,
            dimensions,
        }
    }

    fn from_shared_buffer(id: VectorId, buffer: Bytes, offset: usize, dimensions: usize) -> Self {
        Self {
            id,
            buffer,
            offset,
            dimensions,
        }
    }

    pub(crate) fn id(&self) -> VectorId {
        self.id
    }

    /// Returns this posting's vector as `&[f32]`.
    ///
    /// # Panics
    /// Panics if the byte slice is not aligned to `align_of::<f32>()`.
    /// [`PostingList`] constructors guarantee alignment by copying raw bytes
    /// into a fresh `Vec<f32>` when the source isn't aligned, so this
    /// assertion should always hold.
    pub(crate) fn vector(&self) -> &[f32] {
        let byte_len = self.dimensions * std::mem::size_of::<f32>();
        let bytes = &self.buffer[self.offset..self.offset + byte_len];
        assert!(
            (bytes.as_ptr() as usize).is_multiple_of(std::mem::align_of::<f32>()),
            "posting vector bytes are not f32-aligned; PostingList constructors must produce aligned buffers",
        );
        bytemuck::cast_slice(bytes)
    }
}

impl PartialEq for Posting {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.vector() == other.vector()
    }
}

impl std::fmt::Debug for Posting {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Posting")
            .field("id", &self.id)
            .field("vector", &self.vector())
            .finish()
    }
}

impl From<Posting> for PostingUpdate {
    fn from(value: Posting) -> Self {
        PostingUpdate::append(value.id, value.vector().to_vec())
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct PostingList {
    postings: Vec<Posting>,
}

impl PostingList {
    pub(crate) fn empty() -> Self {
        Self { postings: vec![] }
    }

    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            postings: Vec::with_capacity(capacity),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.postings.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.postings.is_empty()
    }

    pub(crate) fn push(&mut self, posting: Posting) {
        self.postings.push(posting);
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &Posting> {
        self.postings.iter()
    }

    pub(crate) fn update_in_place(&self, updates: Vec<PostingUpdate>) -> Self {
        let updates = dedupe_updates(updates);
        if updates.is_empty() {
            return self.clone();
        }

        let updated_ids: HashSet<VectorId> = updates.iter().map(PostingUpdate::id).collect();
        let mut postings = self
            .postings
            .iter()
            .filter(|posting| !updated_ids.contains(&posting.id))
            .cloned()
            .collect::<Vec<_>>();
        postings.extend(updates.into_iter().filter_map(|update| match update {
            PostingUpdate::Append { id, vector } => {
                let buffer = ensure_aligned(vector);
                let dimensions = buffer.len() / std::mem::size_of::<f32>();
                Some(Posting::from_shared_buffer(id, buffer, 0, dimensions))
            }
            PostingUpdate::Delete { .. } => None,
        }));
        Self { postings }
    }

    pub(crate) fn update_and_flatten(&self, updates: Vec<PostingUpdate>) -> Self {
        let updates = dedupe_updates(updates);
        if updates.is_empty() {
            return self.flatten();
        }

        let updated_ids: HashSet<VectorId> = updates.iter().map(PostingUpdate::id).collect();
        let retained = self
            .postings
            .iter()
            .filter(|posting| !updated_ids.contains(&posting.id))
            .map(|posting| (posting.id, posting.vector()));
        let mut append_vectors: Vec<(VectorId, Vec<f32>)> = Vec::new();
        for update in updates {
            if let PostingUpdate::Append { id, vector } = update {
                append_vectors.push((id, f32_vec_from_aligned_bytes(&vector)));
            }
        }
        let appended = append_vectors.iter().map(|(id, v)| (*id, v.as_slice()));
        Self::from_vectors(retained.chain(appended).collect())
    }

    fn flatten(&self) -> Self {
        Self::from_vectors(
            self.postings
                .iter()
                .map(|posting| (posting.id, posting.vector()))
                .collect(),
        )
    }

    fn from_vectors(vectors: Vec<(VectorId, &[f32])>) -> Self {
        let total_len: usize = vectors.iter().map(|(_, vector)| vector.len()).sum();
        let mut buffer: Vec<f32> = Vec::with_capacity(total_len);
        let mut offsets = Vec::with_capacity(vectors.len());
        for (id, vector) in vectors {
            let element_offset = buffer.len();
            let length = vector.len();
            buffer.extend_from_slice(vector);
            offsets.push((id, element_offset, length));
        }
        let buffer = aligned_bytes_from_f32_vec(buffer);
        let postings = offsets
            .into_iter()
            .map(|(id, element_offset, dimensions)| {
                Posting::from_shared_buffer(
                    id,
                    buffer.clone(),
                    element_offset * std::mem::size_of::<f32>(),
                    dimensions,
                )
            })
            .collect();
        Self { postings }
    }

    pub(crate) fn from_value(value: PostingListValue) -> Self {
        let mut seen = HashSet::new();
        let postings = value
            .postings
            .into_iter()
            .filter_map(|posting| {
                assert!(seen.insert(posting.id()));
                match posting {
                    PostingUpdate::Append { id, vector } => {
                        // Reuse the `Bytes` already sitting in the posting
                        // update — no serialization round-trip. If the buffer
                        // isn't f32-aligned (can't happen with the current
                        // constructors, but the type doesn't guarantee it)
                        // fall back to copying into a fresh aligned
                        // allocation.
                        let buffer = ensure_aligned(vector);
                        let dimensions = buffer.len() / std::mem::size_of::<f32>();
                        Some(Posting::from_shared_buffer(id, buffer, 0, dimensions))
                    }
                    PostingUpdate::Delete { .. } => None,
                }
            })
            .collect::<Vec<_>>();
        Self { postings }
    }
}

impl FromIterator<Posting> for PostingList {
    fn from_iter<T: IntoIterator<Item = Posting>>(iter: T) -> Self {
        Self {
            postings: iter.into_iter().collect(),
        }
    }
}

impl IntoIterator for PostingList {
    type Item = Posting;
    type IntoIter = std::vec::IntoIter<Posting>;

    fn into_iter(self) -> Self::IntoIter {
        self.postings.into_iter()
    }
}

impl<'a> IntoIterator for &'a PostingList {
    type Item = &'a Posting;
    type IntoIter = std::slice::Iter<'a, Posting>;

    fn into_iter(self) -> Self::IntoIter {
        self.postings.iter()
    }
}

impl PartialEq for PostingList {
    fn eq(&self, other: &Self) -> bool {
        self.postings == other.postings
    }
}

fn dedupe_updates(updates: Vec<PostingUpdate>) -> Vec<PostingUpdate> {
    let mut last_by_id = HashMap::with_capacity(updates.len());
    for (idx, update) in updates.iter().enumerate() {
        last_by_id.insert(update.id(), idx);
    }

    updates
        .into_iter()
        .enumerate()
        .filter_map(|(idx, update)| {
            if last_by_id.get(&update.id()) == Some(&idx) {
                Some(update)
            } else {
                None
            }
        })
        .collect()
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

fn aligned_bytes_from_f32_vec(vec: Vec<f32>) -> Bytes {
    Bytes::from_owner(AlignedF32Buf(vec))
}

/// Ensure the given `Bytes` payload is f32-aligned. If it already is, return
/// it unchanged; otherwise copy into a fresh aligned allocation.
fn ensure_aligned(bytes: Bytes) -> Bytes {
    if (bytes.as_ptr() as usize).is_multiple_of(std::mem::align_of::<f32>()) {
        bytes
    } else {
        let mut v = vec![0f32; bytes.len() / std::mem::size_of::<f32>()];
        bytemuck::cast_slice_mut::<f32, u8>(&mut v).copy_from_slice(&bytes);
        aligned_bytes_from_f32_vec(v)
    }
}

/// Copy a potentially-unaligned `Bytes` payload back to a `Vec<f32>`.
fn f32_vec_from_aligned_bytes(bytes: &Bytes) -> Vec<f32> {
    let mut v = vec![0f32; bytes.len() / std::mem::size_of::<f32>()];
    bytemuck::cast_slice_mut::<f32, u8>(&mut v).copy_from_slice(bytes);
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    fn posting_list(entries: Vec<(u64, Vec<f32>)>) -> PostingList {
        entries
            .into_iter()
            .map(|(id, vector)| Posting::new(VectorId::data_vector_id(id), vector))
            .collect()
    }

    #[test]
    fn should_convert_posting_list_value_by_reusing_append_bytes() {
        // given - two appends built from distinct Vec<f32>s
        let append_one = PostingUpdate::append(VectorId::data_vector_id(1), vec![1.0, 2.0]);
        let append_two = PostingUpdate::append(VectorId::data_vector_id(2), vec![3.0, 4.0]);
        let PostingUpdate::Append {
            vector: vector_one, ..
        } = &append_one
        else {
            unreachable!()
        };
        let PostingUpdate::Append {
            vector: vector_two, ..
        } = &append_two
        else {
            unreachable!()
        };
        let expected_one_ptr = vector_one.as_ptr();
        let expected_two_ptr = vector_two.as_ptr();

        let value = PostingListValue::from_posting_updates(vec![append_one, append_two]).unwrap();

        // when
        let postings = PostingList::from_value(value);

        // then - ids and values preserved
        let collected: Vec<_> = postings
            .iter()
            .map(|posting| (posting.id(), posting.vector().to_vec()))
            .collect();
        assert_eq!(
            collected,
            vec![
                (VectorId::data_vector_id(1), vec![1.0, 2.0]),
                (VectorId::data_vector_id(2), vec![3.0, 4.0]),
            ]
        );
        // and - each Posting reuses the exact Bytes that lived on its source
        // PostingUpdate (no flatten-into-shared-buffer copy happens here).
        assert_eq!(postings.postings[0].buffer.as_ptr(), expected_one_ptr);
        assert_eq!(postings.postings[1].buffer.as_ptr(), expected_two_ptr);
    }

    #[test]
    fn should_update_in_place_without_rewriting_untouched_postings() {
        // given
        let postings = posting_list(vec![
            (1, vec![1.0, 0.0]),
            (2, vec![2.0, 0.0]),
            (3, vec![3.0, 0.0]),
        ]);
        let untouched_ptr = postings.postings[0].buffer.as_ptr();

        // when
        let updated = postings.update_in_place(vec![
            PostingUpdate::delete(VectorId::data_vector_id(2)),
            PostingUpdate::append(VectorId::data_vector_id(4), vec![4.0, 0.0]),
        ]);

        // then
        let collected: Vec<_> = updated
            .iter()
            .map(|posting| (posting.id(), posting.vector().to_vec()))
            .collect();
        assert_eq!(
            collected,
            vec![
                (VectorId::data_vector_id(1), vec![1.0, 0.0]),
                (VectorId::data_vector_id(3), vec![3.0, 0.0]),
                (VectorId::data_vector_id(4), vec![4.0, 0.0])
            ]
        );
        assert_eq!(updated.postings[0].buffer.as_ptr(), untouched_ptr);
        assert_ne!(
            updated.postings[0].buffer.as_ptr(),
            updated.postings[2].buffer.as_ptr()
        );
    }

    #[test]
    fn should_update_and_flatten_into_single_shared_buffer() {
        // given
        let postings = posting_list(vec![(1, vec![1.0, 0.0]), (2, vec![2.0, 0.0])]);

        // when
        let updated = postings.update_and_flatten(vec![
            PostingUpdate::delete(VectorId::data_vector_id(1)),
            PostingUpdate::append(VectorId::data_vector_id(3), vec![3.0, 0.0]),
        ]);

        // then
        let collected: Vec<_> = updated
            .iter()
            .map(|posting| (posting.id(), posting.vector().to_vec()))
            .collect();
        assert_eq!(
            collected,
            vec![
                (VectorId::data_vector_id(2), vec![2.0, 0.0]),
                (VectorId::data_vector_id(3), vec![3.0, 0.0])
            ]
        );
        assert_eq!(
            updated.postings[0].buffer.as_ptr(),
            updated.postings[1].buffer.as_ptr(),
        );
    }

    #[test]
    fn should_apply_last_write_wins_semantics_to_updates() {
        // given
        let postings = posting_list(vec![(1, vec![1.0, 0.0]), (2, vec![2.0, 0.0])]);

        // when
        let updated = postings.update_in_place(vec![
            PostingUpdate::append(VectorId::data_vector_id(2), vec![20.0, 0.0]),
            PostingUpdate::delete(VectorId::data_vector_id(2)),
            PostingUpdate::delete(VectorId::data_vector_id(3)),
            PostingUpdate::append(VectorId::data_vector_id(3), vec![30.0, 0.0]),
        ]);

        // then
        let collected: Vec<_> = updated
            .iter()
            .map(|posting| (posting.id(), posting.vector().to_vec()))
            .collect();
        assert_eq!(
            collected,
            vec![
                (VectorId::data_vector_id(1), vec![1.0, 0.0]),
                (VectorId::data_vector_id(3), vec![30.0, 0.0])
            ]
        );
    }
}
