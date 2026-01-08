use bytes::{Bytes, BytesMut};
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::ops::{Bound, RangeBounds};

#[derive(Clone, Debug)]
pub struct BytesRange {
    pub start: Bound<Bytes>,
    pub end: Bound<Bytes>,
}

impl BytesRange {
    pub fn new(start: Bound<Bytes>, end: Bound<Bytes>) -> Self {
        Self { start, end }
    }

    /// Creates a range that includes all keys with the given prefix.
    pub fn prefix(prefix: Bytes) -> Self {
        if prefix.is_empty() {
            Self::unbounded()
        } else {
            let mut end = BytesMut::from(&prefix[..]);
            for i in (0..end.len()).rev() {
                if end[i] < 0xFF {
                    end[i] += 1;
                    end.truncate(i + 1);
                    return Self {
                        start: Included(prefix),
                        end: Excluded(end.freeze()),
                    };
                }
            }
            Self {
                start: Included(prefix),
                end: Unbounded,
            }
        }
    }

    pub fn contains(&self, k: &[u8]) -> bool {
        (match &self.start {
            Included(s) => k >= s,
            Excluded(s) => k > s,
            Unbounded => true,
        }) && (match &self.end {
            Included(e) => k <= e,
            Excluded(e) => k < e,
            Unbounded => true,
        })
    }

    /// Creates a range that scans everything.
    pub fn unbounded() -> Self {
        Self {
            start: Unbounded,
            end: Unbounded,
        }
    }
}

impl RangeBounds<Bytes> for BytesRange {
    fn start_bound(&self) -> Bound<&Bytes> {
        self.start.as_ref()
    }
    fn end_bound(&self) -> Bound<&Bytes> {
        self.end.as_ref()
    }
}
