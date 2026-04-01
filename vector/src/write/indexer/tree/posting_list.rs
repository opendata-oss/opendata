use crate::serde::posting_list::{PostingListValue, PostingUpdate};
use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct Posting {
    id: u64,
    buffer: Arc<Vec<f32>>,
    offset: usize,
    length: usize,
}

impl Posting {
    pub(crate) fn new(id: u64, vector: Vec<f32>) -> Self {
        let length = vector.len();
        Self {
            id,
            buffer: Arc::new(vector),
            offset: 0,
            length,
        }
    }

    fn from_shared_buffer(id: u64, buffer: Arc<Vec<f32>>, offset: usize, length: usize) -> Self {
        Self {
            id,
            buffer,
            offset,
            length,
        }
    }

    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    pub(crate) fn vector(&self) -> &[f32] {
        &self.buffer[self.offset..self.offset + self.length]
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
    pub(crate) fn new() -> Self {
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

        let updated_ids: HashSet<u64> = updates.iter().map(PostingUpdate::id).collect();
        let mut postings = self
            .postings
            .iter()
            .filter(|posting| !updated_ids.contains(&posting.id))
            .cloned()
            .collect::<Vec<_>>();
        postings.extend(updates.into_iter().filter_map(|update| match update {
            PostingUpdate::Append { id, vector } => Some(Posting::new(id, vector)),
            PostingUpdate::Delete { .. } => None,
        }));
        Self { postings }
    }

    pub(crate) fn update_and_flatten(&self, updates: Vec<PostingUpdate>) -> Self {
        let updates = dedupe_updates(updates);
        if updates.is_empty() {
            return self.flatten();
        }

        let updated_ids: HashSet<u64> = updates.iter().map(PostingUpdate::id).collect();
        let retained = self
            .postings
            .iter()
            .filter(|posting| !updated_ids.contains(&posting.id))
            .map(|posting| (posting.id, posting.vector().to_vec()));
        let appended = updates.into_iter().filter_map(|update| match update {
            PostingUpdate::Append { id, vector } => Some((id, vector)),
            PostingUpdate::Delete { .. } => None,
        });
        Self::from_vectors(retained.chain(appended).collect())
    }

    fn flatten(&self) -> Self {
        Self::from_vectors(
            self.postings
                .iter()
                .map(|posting| (posting.id, posting.vector().to_vec()))
                .collect(),
        )
    }

    fn from_vectors(vectors: Vec<(u64, Vec<f32>)>) -> Self {
        let total_len = vectors.iter().map(|(_, vector)| vector.len()).sum();
        let mut buffer = Vec::with_capacity(total_len);
        let mut offsets = Vec::with_capacity(vectors.len());
        for (id, vector) in vectors {
            let offset = buffer.len();
            let length = vector.len();
            buffer.extend(vector);
            offsets.push((id, offset, length));
        }
        let buffer = Arc::new(buffer);
        let postings = offsets
            .into_iter()
            .map(|(id, offset, length)| {
                Posting::from_shared_buffer(id, buffer.clone(), offset, length)
            })
            .collect();
        Self { postings }
    }

    pub(crate) fn from_value(value: PostingListValue) -> Self {
        let mut seen = HashSet::new();
        let vectors = value
            .postings
            .into_iter()
            .filter_map(|posting| {
                assert!(seen.insert(posting.id()));
                match posting {
                    PostingUpdate::Append { id, vector } => Some((id, vector)),
                    PostingUpdate::Delete { .. } => None,
                }
            })
            .collect();
        Self::from_vectors(vectors)
    }
}

impl From<Vec<crate::serde::posting_list::Posting>> for PostingList {
    fn from(value: Vec<crate::serde::posting_list::Posting>) -> Self {
        value
            .into_iter()
            .map(|posting| Posting::new(posting.id(), posting.vector().to_vec()))
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

pub(crate) trait IntoTreePostingList: Send + Sync {
    fn clone_into_tree(&self) -> PostingList;
}

impl IntoTreePostingList for PostingList {
    fn clone_into_tree(&self) -> PostingList {
        self.clone()
    }
}

impl IntoTreePostingList for Vec<crate::serde::posting_list::Posting> {
    fn clone_into_tree(&self) -> PostingList {
        self.clone().into()
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

#[cfg(test)]
mod tests {
    use super::*;

    fn posting_list(entries: Vec<(u64, Vec<f32>)>) -> PostingList {
        entries
            .into_iter()
            .map(|(id, vector)| Posting::new(id, vector))
            .collect()
    }

    #[test]
    fn should_convert_posting_list_value_into_flattened_postings() {
        // given
        let value = PostingListValue::from_posting_updates(vec![
            PostingUpdate::append(1, vec![1.0, 2.0]),
            PostingUpdate::append(2, vec![3.0, 4.0]),
        ])
        .unwrap();

        // when
        let postings = PostingList::from_value(value);

        // then
        let collected: Vec<_> = postings.iter().map(|posting| posting.id()).collect();
        assert_eq!(collected, vec![1, 2]);
        assert!(Arc::ptr_eq(
            &postings.postings[0].buffer,
            &postings.postings[1].buffer
        ));
    }

    #[test]
    fn should_update_in_place_without_rewriting_untouched_postings() {
        // given
        let postings = posting_list(vec![
            (1, vec![1.0, 0.0]),
            (2, vec![2.0, 0.0]),
            (3, vec![3.0, 0.0]),
        ]);
        let untouched_buffer = postings.postings[0].buffer.clone();

        // when
        let updated = postings.update_in_place(vec![
            PostingUpdate::delete(2),
            PostingUpdate::append(4, vec![4.0, 0.0]),
        ]);

        // then
        let collected: Vec<_> = updated
            .iter()
            .map(|posting| (posting.id(), posting.vector().to_vec()))
            .collect();
        assert_eq!(
            collected,
            vec![
                (1, vec![1.0, 0.0]),
                (3, vec![3.0, 0.0]),
                (4, vec![4.0, 0.0])
            ]
        );
        assert!(Arc::ptr_eq(&untouched_buffer, &updated.postings[0].buffer));
        assert!(!Arc::ptr_eq(
            &updated.postings[0].buffer,
            &updated.postings[2].buffer
        ));
    }

    #[test]
    fn should_update_and_flatten_into_single_shared_buffer() {
        // given
        let postings = posting_list(vec![(1, vec![1.0, 0.0]), (2, vec![2.0, 0.0])]);

        // when
        let updated = postings.update_and_flatten(vec![
            PostingUpdate::delete(1),
            PostingUpdate::append(3, vec![3.0, 0.0]),
        ]);

        // then
        let collected: Vec<_> = updated
            .iter()
            .map(|posting| (posting.id(), posting.vector().to_vec()))
            .collect();
        assert_eq!(collected, vec![(2, vec![2.0, 0.0]), (3, vec![3.0, 0.0])]);
        assert!(Arc::ptr_eq(
            &updated.postings[0].buffer,
            &updated.postings[1].buffer
        ));
    }

    #[test]
    fn should_apply_last_write_wins_semantics_to_updates() {
        // given
        let postings = posting_list(vec![(1, vec![1.0, 0.0]), (2, vec![2.0, 0.0])]);

        // when
        let updated = postings.update_in_place(vec![
            PostingUpdate::append(2, vec![20.0, 0.0]),
            PostingUpdate::delete(2),
            PostingUpdate::delete(3),
            PostingUpdate::append(3, vec![30.0, 0.0]),
        ]);

        // then
        let collected: Vec<_> = updated
            .iter()
            .map(|posting| (posting.id(), posting.vector().to_vec()))
            .collect();
        assert_eq!(collected, vec![(1, vec![1.0, 0.0]), (3, vec![30.0, 0.0])]);
    }
}
