use crate::serde::posting_list::{PostingListValue, PostingUpdate};
use crate::serde::vector_id::VectorId;
use crate::utils::aligned_bytes_from_vec;
use bytemuck::{AnyBitPattern, NoUninit};
use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;

/// An in-memory format for posting lists optimized for exhaustive ANN search
/// over all postings. Every vector for a given posting list lives in a
/// shared `Bytes` buffer, and each [`Posting`] holds a slice into it. This allows
/// for vectors to be adjacent in memory and accessible directly from raw bytes.
/// The buffer is always `align_of::<f32>()`-aligned so [`Posting::vector`]
/// can reinterpret the bytes as `&[f32]` without a copy.
#[derive(Clone)]
pub(crate) struct Posting {
    id: VectorId,
    buffer: Bytes,
    /// Number of f32 elements (not bytes) that make up this posting's vector.
    dimensions: usize,
}

impl Posting {
    pub(crate) fn from_vec(id: VectorId, vector: Vec<f32>) -> Self {
        let dimensions = vector.len();
        Self {
            id,
            buffer: aligned_bytes_from_vec(vector),
            dimensions,
        }
    }

    fn from_bytes(id: VectorId, buffer: Bytes, dimensions: usize) -> Self {
        Self {
            id,
            buffer,
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
    pub(crate) fn vector(&self) -> &[f32] {
        self.try_vector()
            .expect("vector bytes cannot be read as &[f32]")
    }

    fn try_vector(&self) -> Result<&[f32], String> {
        let byte_len = self.dimensions * size_of::<f32>();
        let bytes = &self.buffer[..byte_len];
        bytemuck::try_cast_slice(bytes).map_err(|e| e.to_string())
    }
}

impl PartialEq for Posting {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.buffer == other.buffer
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

/// In-memory collection of [`Posting`]s for a single centroid cluster, used
/// on the write/scoring path.
///
/// Differs from [`PostingListValue`] (the on-disk representation): this type
/// holds resolved appends only — deletes have already been applied — and each
/// posting's vector bytes are guaranteed `align_of::<f32>()`-aligned so
/// scoring can read them as `&[f32]` without copying.
///
/// Use [`PostingList::from_value`] to materialize one from a stored
/// `PostingListValue`, and [`PostingList::clone_with_updates`] to apply a
/// batch of [`PostingUpdate`]s. Both accept a `realloc` flag that, when set,
/// flattens all postings into a single contiguous buffer for cache-friendly
/// scoring and bounded allocation size.
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

    /// Creates a copy of the posting list with the result of applying the provided updates,
    /// and optionally reallocate the underlying vector allocation. Reallocating serves 2 purposes.
    /// First, it limits the size of the underlying allocation. Note that individual vectors may
    /// still be reallocated to ensure proper alignment even if `realloc` is false. Second,
    /// contiguous storage ensure efficient loading of vectors in the posting list for scoring.
    pub(crate) fn clone_with_updates(&self, updates: Vec<PostingUpdate>, realloc: bool) -> Self {
        let mut posting_list = if updates.is_empty() {
            self.clone()
        } else {
            let updates = Self::dedupe_updates(updates);
            let updated_ids: HashSet<VectorId> = updates.iter().map(PostingUpdate::id).collect();
            let mut postings = self
                .postings
                .iter()
                .filter(|posting| !updated_ids.contains(&posting.id))
                .cloned()
                .collect::<Vec<_>>();
            postings.extend(updates.into_iter().filter_map(|update| match update {
                PostingUpdate::Append { id, vector } => {
                    let buffer = ensure_aligned::<f32>(vector);
                    let dimensions = buffer.len() / std::mem::size_of::<f32>();
                    Some(Posting::from_bytes(id, buffer, dimensions))
                }
                PostingUpdate::Delete { .. } => None,
            }));
            Self { postings }
        };
        if realloc {
            posting_list.realloc();
        }
        posting_list
    }

    /// create from a [`PostingListValue`] read from storage and optionally reallocate the
    /// underlying vector allocation. Reallocating serves 2 purposes. First, it limits the size of
    /// the underlying allocation. When reading from storage, vectors may be part of a larger buffer
    /// allocated by the storage layer. Note that individual vectors may still be reallocated to
    /// ensure proper alignment even if `realloc` is false. Second, contiguous storage ensure
    /// efficient loading of vectors in the posting list for scoring. This is generally not
    /// important when loading from storage as the storage layer itself tries to keep vectors
    /// contiguous.
    pub(crate) fn from_value(value: PostingListValue, realloc: bool) -> Self {
        let mut seen = HashSet::new();
        let postings = value
            .postings
            .into_iter()
            .filter_map(|posting| {
                assert!(seen.insert(posting.id()));
                match posting {
                    PostingUpdate::Append { id, vector } => {
                        let buffer = ensure_aligned::<f32>(vector);
                        let dimensions = buffer.len() / size_of::<f32>();
                        Some(Posting::from_bytes(id, buffer, dimensions))
                    }
                    PostingUpdate::Delete { .. } => None,
                }
            })
            .collect::<Vec<_>>();
        let mut posting_list = Self { postings };
        if realloc {
            posting_list.realloc();
        }
        posting_list
    }

    /// reallocates memory for the stored vectors so that they are all slices of a single
    /// contiguous buffer. This serves 2 purposes. First, it limits the memory referenced by the
    /// underlying allocation. When reading from storage, vectors may be part of a larger buffer
    /// allocated by the storage layer. Second, contiguous storage supports efficient loading of
    /// vectors in the posting list for scoring.
    fn realloc(&mut self) {
        let total_len: usize = self.postings.iter().map(|p| p.vector().len()).sum();
        let mut buffer: Vec<f32> = Vec::with_capacity(total_len);
        let mut offsets = Vec::with_capacity(self.postings.len());
        for posting in &self.postings {
            let element_offset = buffer.len();
            let vector = posting.vector();
            let length = vector.len();
            buffer.extend_from_slice(vector);
            offsets.push((posting.id(), element_offset, length));
        }
        let buffer = aligned_bytes_from_vec(buffer);
        let postings = offsets
            .into_iter()
            .map(|(id, element_offset, dimensions)| {
                let start = element_offset * size_of::<f32>();
                let end = start + dimensions * size_of::<f32>();
                Posting::from_bytes(id, buffer.slice(start..end), dimensions)
            })
            .collect();
        self.postings = postings;
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

/// Ensure the given `Bytes` payload is f32-aligned. If it already is, return
/// it unchanged; otherwise copy into a fresh aligned allocation.
fn ensure_aligned<T: NoUninit + AnyBitPattern + Send + 'static>(bytes: Bytes) -> Bytes {
    if (bytes.as_ptr() as usize).is_multiple_of(align_of::<T>()) {
        bytes
    } else {
        let mut v = vec![T::zeroed(); bytes.len() / size_of::<T>()];
        bytemuck::cast_slice_mut::<T, u8>(&mut v).copy_from_slice(&bytes);
        aligned_bytes_from_vec(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn posting_list(entries: Vec<(u64, Vec<f32>)>) -> PostingList {
        entries
            .into_iter()
            .map(|(id, vector)| Posting::from_vec(VectorId::data_vector_id(id), vector))
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
        let postings = PostingList::from_value(value, false);

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
        let updated = postings.clone_with_updates(
            vec![
                PostingUpdate::delete(VectorId::data_vector_id(2)),
                PostingUpdate::append(VectorId::data_vector_id(4), vec![4.0, 0.0]),
            ],
            false,
        );

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
        let updated = postings.clone_with_updates(
            vec![
                PostingUpdate::delete(VectorId::data_vector_id(1)),
                PostingUpdate::append(VectorId::data_vector_id(3), vec![3.0, 0.0]),
            ],
            true,
        );

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
        let p0_addr = updated.postings[0].buffer.as_ptr() as u64;
        let p1_addr = updated.postings[1].buffer.as_ptr() as u64;
        assert_eq!(p0_addr + 8, p1_addr);
    }

    #[test]
    fn should_apply_last_write_wins_semantics_to_updates() {
        // given
        let postings = posting_list(vec![(1, vec![1.0, 0.0]), (2, vec![2.0, 0.0])]);

        // when
        let updated = postings.clone_with_updates(
            vec![
                PostingUpdate::append(VectorId::data_vector_id(2), vec![20.0, 0.0]),
                PostingUpdate::delete(VectorId::data_vector_id(2)),
                PostingUpdate::delete(VectorId::data_vector_id(3)),
                PostingUpdate::append(VectorId::data_vector_id(3), vec![30.0, 0.0]),
            ],
            false,
        );

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
