//! Process-global counters for the segment-scan read path, mirroring the
//! object-store GET counters in `common::storage::counting`.
//!
//! A single read (one [`LogIterator`](crate::LogIterator)) opens one
//! `scan_entries` per segment that covers its sequence range, because the entry
//! key is segment-major (`[.., segment_id, .., key, seq]`) — a key's records are
//! striped across every segment it appears in. These tallies expose the read
//! amplification the GET count alone hides:
//!
//! - [`segment_scans`] — how many segments a read must touch, and
//! - [`empty_segment_scans`] — how many of those touches returned nothing for
//!   the key (a segment covered the sequence range but held no records for it),
//!   which is pure waste.
//!
//! Dividing `segment_scans` by polls gives segments-per-poll; dividing consumed
//! records by `segment_scans` gives the per-segment key density. Overhead is one
//! relaxed atomic add per segment scan — negligible against the object-store
//! round-trip each scan may trigger.

use std::sync::atomic::{AtomicU64, Ordering};

static SEGMENT_SCANS: AtomicU64 = AtomicU64::new(0);
static EMPTY_SEGMENT_SCANS: AtomicU64 = AtomicU64::new(0);

/// Total segment scans (`scan_entries` opens) issued on the read path so far.
pub fn segment_scans() -> u64 {
    SEGMENT_SCANS.load(Ordering::Relaxed)
}

/// Subset of [`segment_scans`] that yielded no entries for the scanned key —
/// pure read amplification.
pub fn empty_segment_scans() -> u64 {
    EMPTY_SEGMENT_SCANS.load(Ordering::Relaxed)
}

pub(crate) fn record_segment_scan() {
    SEGMENT_SCANS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_empty_segment_scan() {
    EMPTY_SEGMENT_SCANS.fetch_add(1, Ordering::Relaxed);
}
