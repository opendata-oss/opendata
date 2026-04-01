use std::collections::HashSet;
use std::sync::Arc;
use crate::serde::posting_list::{PostingListValue, PostingUpdate};

pub(crate) struct Posting {
    id: u64,
    buffer: Arc<Vec<f32>>,
    offset: usize,
    length: usize,
}

impl Posting {
    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    pub(crate) fn vector(&self) -> &[f32] {
        &self.buffer[self.offset..self.offset + self.length]
    }
}

pub(crate) struct PostingList {
    shared_buffer: Arc<Vec<f32>>,
    postings: Vec<Posting>
}

impl From<PostingListValue> for PostingList {
    fn from(value: PostingListValue) -> Self {
        let mut seen = HashSet::new();
        let buf_sz = value
            .postings
            .iter()
            .map(|p| p.vector().map(|v| v.len()).unwrap_or(0))
            .sum();
        let mut buffer = Vec::with_capacity(buf_sz);
        let mut postings = Vec::with_capacity(value.len());
        for posting in value.postings {
            assert!(seen.insert(posting.id()));
            let PostingUpdate::Append { id, vector } = posting else {
                continue;
            };
            let offset = buffer.len();
            let length = vector.len();
            buffer.extend(vector);
            postings.push((id, offset, length))
        }
        let buffer = Arc::new(buffer);
        let postings = postings
            .into_iter()
            .map(|(id, offset, length)| Posting {
                buffer: buffer.clone(),
                id,
                offset,
                length
            })
            .collect();
        Self {
            shared_buffer: buffer,
            postings,
        }
    }
}

impl PostingList {
    pub(crate) fn iter(&self) -> impl Iterator<Item=&Posting> {
        self.postings.iter()
    }

    pub(crate) fn update_in_place(&self, updates: Vec<PostingUpdate>) -> Self {
        // update by retaining the same shared buffer, and appending
        // any new postings by using a dedicated buffer for each posting
        // make sure to remove any vectors for which there is a delete in updates
        todo!()
    }

    pub(crate) fn update_and_flatten(&self, updates: Vec<PostingUpdate>) -> Self {
        // update by allocating a new shared buffer for all vectors
        // make sure to remove any vectors for which there is a delete in updates
        todo!()
    }
}