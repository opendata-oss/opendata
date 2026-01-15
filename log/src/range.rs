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
//! using [`normalize`].

use std::ops::{Bound, Range, RangeBounds};

/// Converts any `RangeBounds<u64>` to a normalized `Range<u64>`.
pub(crate) fn normalize<R: RangeBounds<u64>>(range: &R) -> Range<u64> {
    let start = match range.start_bound() {
        Bound::Included(&s) => s,
        Bound::Excluded(&s) => s.saturating_add(1),
        Bound::Unbounded => 0,
    };
    let end = match range.end_bound() {
        Bound::Included(&e) => e.saturating_add(1),
        Bound::Excluded(&e) => e,
        Bound::Unbounded => u64::MAX,
    };
    start..end
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::Bound;

    #[test]
    fn should_normalize_full_range() {
        let range = normalize(&(..));
        assert_eq!(range, 0..u64::MAX);
    }

    #[test]
    fn should_normalize_range_from() {
        let range = normalize(&(100u64..));
        assert_eq!(range, 100..u64::MAX);
    }

    #[test]
    fn should_normalize_range_to() {
        let range = normalize(&(..100u64));
        assert_eq!(range, 0..100);
    }

    #[test]
    fn should_normalize_range() {
        let range = normalize(&(50u64..150));
        assert_eq!(range, 50..150);
    }

    #[test]
    fn should_normalize_range_inclusive() {
        let range = normalize(&(50u64..=150));
        assert_eq!(range, 50..151);
    }

    #[test]
    fn should_normalize_range_to_inclusive() {
        let range = normalize(&(..=100u64));
        assert_eq!(range, 0..101);
    }

    #[test]
    fn should_handle_max_value_inclusive() {
        let range = normalize(&(0..=u64::MAX));
        // saturating_add prevents overflow
        assert_eq!(range, 0..u64::MAX);
    }

    #[test]
    fn should_handle_excluded_start() {
        let range = normalize(&(Bound::Excluded(10u64), Bound::Unbounded));
        assert_eq!(range, 11..u64::MAX);
    }
}
