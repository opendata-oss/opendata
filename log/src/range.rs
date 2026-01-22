//! Range utilities.
//!
//! # Convention
//!
//! Public APIs (`Log`, `LogReader` via `LogRead`) accept `impl RangeBounds<T>`
//! for ergonomic range syntax (`..`, `5..`, `..10`, `5..10`, etc.).
//!
//! Internal code uses concrete `Range<T>` types to avoid generics proliferation
//! and simplify implementation.
//!
//! Conversion from `RangeBounds` to `Range` happens at the public API boundary
//! using [`normalize_sequence`] and [`normalize_segment_id`].

use std::ops::{Bound, Range, RangeBounds};

use crate::model::{SegmentId, Sequence};

/// Converts any `RangeBounds<Sequence>` to a normalized `Range<Sequence>`.
pub(crate) fn normalize_sequence<R: RangeBounds<Sequence>>(range: &R) -> Range<Sequence> {
    let start = match range.start_bound() {
        Bound::Included(&s) => s,
        Bound::Excluded(&s) => s.saturating_add(1),
        Bound::Unbounded => 0,
    };
    let end = match range.end_bound() {
        Bound::Included(&e) => e.saturating_add(1),
        Bound::Excluded(&e) => e,
        Bound::Unbounded => Sequence::MAX,
    };
    start..end
}

/// Converts any `RangeBounds<SegmentId>` to a normalized `Range<SegmentId>`.
pub(crate) fn normalize_segment_id<R: RangeBounds<SegmentId>>(range: &R) -> Range<SegmentId> {
    let start = match range.start_bound() {
        Bound::Included(&s) => s,
        Bound::Excluded(&s) => s.saturating_add(1),
        Bound::Unbounded => 0,
    };
    let end = match range.end_bound() {
        Bound::Included(&e) => e.saturating_add(1),
        Bound::Excluded(&e) => e,
        Bound::Unbounded => SegmentId::MAX,
    };
    start..end
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::Bound;

    #[test]
    fn should_normalize_full_range() {
        let range = normalize_sequence(&(..));
        assert_eq!(range, 0..u64::MAX);
    }

    #[test]
    fn should_normalize_range_from() {
        let range = normalize_sequence(&(100u64..));
        assert_eq!(range, 100..u64::MAX);
    }

    #[test]
    fn should_normalize_range_to() {
        let range = normalize_sequence(&(..100u64));
        assert_eq!(range, 0..100);
    }

    #[test]
    fn should_normalize_range() {
        let range = normalize_sequence(&(50u64..150));
        assert_eq!(range, 50..150);
    }

    #[test]
    fn should_normalize_range_inclusive() {
        let range = normalize_sequence(&(50u64..=150));
        assert_eq!(range, 50..151);
    }

    #[test]
    fn should_normalize_range_to_inclusive() {
        let range = normalize_sequence(&(..=100u64));
        assert_eq!(range, 0..101);
    }

    #[test]
    fn should_handle_max_value_inclusive() {
        let range = normalize_sequence(&(0..=u64::MAX));
        // saturating_add prevents overflow
        assert_eq!(range, 0..u64::MAX);
    }

    #[test]
    fn should_handle_excluded_start() {
        let range = normalize_sequence(&(Bound::Excluded(10u64), Bound::Unbounded));
        assert_eq!(range, 11..u64::MAX);
    }

    #[test]
    fn should_normalize_segment_id_full_range() {
        // given/when
        let range = normalize_segment_id(&(..));

        // then
        assert_eq!(range, 0..u32::MAX);
    }

    #[test]
    fn should_normalize_segment_id_range_from() {
        // given/when
        let range = normalize_segment_id(&(10u32..));

        // then
        assert_eq!(range, 10..u32::MAX);
    }

    #[test]
    fn should_normalize_segment_id_range_to() {
        // given/when
        let range = normalize_segment_id(&(..100u32));

        // then
        assert_eq!(range, 0..100);
    }

    #[test]
    fn should_normalize_segment_id_range() {
        // given/when
        let range = normalize_segment_id(&(5u32..15));

        // then
        assert_eq!(range, 5..15);
    }

    #[test]
    fn should_normalize_segment_id_range_inclusive() {
        // given/when
        let range = normalize_segment_id(&(5u32..=15));

        // then
        assert_eq!(range, 5..16);
    }

    #[test]
    fn should_normalize_segment_id_range_to_inclusive() {
        // given/when
        let range = normalize_segment_id(&(..=10u32));

        // then
        assert_eq!(range, 0..11);
    }

    #[test]
    fn should_normalize_segment_id_handle_max_value_inclusive() {
        // given/when
        let range = normalize_segment_id(&(0..=u32::MAX));

        // then - saturating_add prevents overflow
        assert_eq!(range, 0..u32::MAX);
    }

    #[test]
    fn should_normalize_segment_id_handle_excluded_start() {
        // given/when
        let range = normalize_segment_id(&(Bound::Excluded(10u32), Bound::Unbounded));

        // then
        assert_eq!(range, 11..u32::MAX);
    }
}
