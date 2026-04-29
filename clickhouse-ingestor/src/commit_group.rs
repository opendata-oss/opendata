//! Commit group: coalesce decoded records across Buffer batches.
//!
//! The runtime feeds Buffer batches into [`CommitGroup::append`] one at a
//! time, then asks [`CommitGroup::should_flush`] after each append and on
//! a timer wakeup. When a threshold trips, [`CommitGroup::drain`] hands a
//! [`CommitGroupBatch`] downstream.
//!
//! Two properties pinned down by the RFC:
//!
//! - **Input progress is independent of output rows.** `append` always
//!   advances the high-watermark even when `records.is_empty()`, so a
//!   Buffer batch that decoded to zero rows still ack-progresses.
//! - **Max-age is enforced via a timer.** [`CommitGroup::time_until_max_age`]
//!   tells the runtime how long it can `select!` on a sleep before the
//!   group must flush regardless of whether new batches arrive.

use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitGroupThresholds {
    pub max_rows: usize,
    pub max_bytes: usize,
    pub max_age: Duration,
}

impl Default for CommitGroupThresholds {
    fn default() -> Self {
        Self {
            max_rows: 100_000,
            max_bytes: 32 * 1024 * 1024,
            max_age: Duration::from_secs(1),
        }
    }
}

/// Trait that decoded records implement so the commit group can size its
/// in-memory buffer in bytes without crossing a payload-format boundary.
pub trait RecordSize {
    fn approx_size_bytes(&self) -> usize;
}

impl RecordSize for crate::signal::DecodedLogRecord {
    fn approx_size_bytes(&self) -> usize {
        let mut sz = std::mem::size_of::<Self>();
        sz += self.severity_text.len();
        sz += self.body.len();
        sz += self.service_name.as_ref().map_or(0, |s| s.len());
        sz += self.scope_name.as_ref().map_or(0, |s| s.len());
        sz += self.trace_id_hex.len() + self.span_id_hex.len();
        for (k, v) in &self.resource_attributes {
            sz += k.len() + v.len();
        }
        for (k, v) in &self.log_attributes {
            sz += k.len() + v.len();
        }
        sz
    }
}

#[derive(Debug)]
pub struct CommitGroup<R> {
    records: Vec<R>,
    low_sequence: Option<u64>,
    high_sequence: Option<u64>,
    bytes: usize,
    opened_at: Option<Instant>,
    thresholds: CommitGroupThresholds,
}

impl<R: RecordSize> CommitGroup<R> {
    pub fn new(thresholds: CommitGroupThresholds) -> Self {
        Self {
            records: Vec::new(),
            low_sequence: None,
            high_sequence: None,
            bytes: 0,
            opened_at: None,
            thresholds,
        }
    }

    /// Record the successful processing of a Buffer sequence. Always
    /// advances the input high-watermark, even when `records` is empty.
    pub fn append(&mut self, records: Vec<R>, sequence: u64) {
        if self.opened_at.is_none() {
            self.opened_at = Some(Instant::now());
        }
        if self.low_sequence.is_none() {
            self.low_sequence = Some(sequence);
        }
        self.high_sequence = Some(sequence);
        for r in records {
            self.bytes += r.approx_size_bytes();
            self.records.push(r);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty() && self.high_sequence.is_none()
    }

    pub fn rows(&self) -> usize {
        self.records.len()
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }

    pub fn opened_at(&self) -> Option<Instant> {
        self.opened_at
    }

    pub fn high_watermark(&self) -> Option<u64> {
        self.high_sequence
    }

    pub fn low_watermark(&self) -> Option<u64> {
        self.low_sequence
    }

    pub fn should_flush(&self) -> bool {
        self.should_flush_at(Instant::now())
    }

    pub fn should_flush_at(&self, now: Instant) -> bool {
        if self.high_sequence.is_none() {
            // Nothing absorbed yet; nothing to flush.
            return false;
        }
        if self.records.len() >= self.thresholds.max_rows {
            return true;
        }
        if self.bytes >= self.thresholds.max_bytes {
            return true;
        }
        if let Some(opened) = self.opened_at
            && now.duration_since(opened) >= self.thresholds.max_age
        {
            return true;
        }
        false
    }

    /// Returns the time remaining until the open group's max_age elapses,
    /// or `None` if no batch has been absorbed yet (in which case the
    /// runtime should block on `consumer.next_batch()` rather than a
    /// timer). Used by the runtime's `select!` loop.
    pub fn time_until_max_age(&self, now: Instant) -> Option<Duration> {
        let opened = self.opened_at?;
        let elapsed = now.saturating_duration_since(opened);
        Some(self.thresholds.max_age.saturating_sub(elapsed))
    }

    /// Drain the group's contents, leaving an empty group ready to absorb
    /// the next batch. `bytes` on the returned batch is the sum of
    /// `RecordSize::approx_size_bytes` at drain time so the runtime can
    /// record the `commit_group_size_bytes` histogram without re-summing.
    pub fn drain(&mut self) -> CommitGroupBatch<R> {
        let records = std::mem::take(&mut self.records);
        let low = self.low_sequence.expect("drain called on empty group");
        let high = self.high_sequence.expect("drain called on empty group");
        let bytes = self.bytes;
        self.low_sequence = None;
        self.high_sequence = None;
        self.bytes = 0;
        self.opened_at = None;
        CommitGroupBatch {
            records,
            low_sequence: low,
            high_sequence: high,
            bytes,
        }
    }
}

#[derive(Debug)]
pub struct CommitGroupBatch<R> {
    pub records: Vec<R>,
    pub low_sequence: u64,
    pub high_sequence: u64,
    /// Sum of `RecordSize::approx_size_bytes` over the drained records.
    pub bytes: usize,
}

impl<R> CommitGroupBatch<R> {
    pub fn rows(&self) -> usize {
        self.records.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Lightweight test record: just carries a known size so we can drive
    /// byte thresholds directly.
    struct TestRecord {
        size: usize,
    }
    impl RecordSize for TestRecord {
        fn approx_size_bytes(&self) -> usize {
            self.size
        }
    }
    fn rec(size: usize) -> TestRecord {
        TestRecord { size }
    }

    fn thresholds(rows: usize, bytes: usize, age_ms: u64) -> CommitGroupThresholds {
        CommitGroupThresholds {
            max_rows: rows,
            max_bytes: bytes,
            max_age: Duration::from_millis(age_ms),
        }
    }

    #[test]
    fn empty_group_does_not_flush() {
        let g = CommitGroup::<TestRecord>::new(thresholds(10, 1024, 1000));
        assert!(!g.should_flush());
        assert!(g.high_watermark().is_none());
        assert!(g.is_empty());
    }

    #[test]
    fn append_advances_high_watermark_even_when_empty() {
        let mut g = CommitGroup::<TestRecord>::new(thresholds(10, 1024, 1000));
        g.append(vec![], 5);
        assert_eq!(g.low_watermark(), Some(5));
        assert_eq!(g.high_watermark(), Some(5));
        g.append(vec![], 6);
        assert_eq!(g.low_watermark(), Some(5));
        assert_eq!(g.high_watermark(), Some(6));
        assert_eq!(g.rows(), 0);
        assert!(
            !g.is_empty(),
            "group must not look empty just because rows=0"
        );
    }

    #[test]
    fn row_threshold_triggers_flush() {
        let mut g = CommitGroup::<TestRecord>::new(thresholds(2, 10_000, 60_000));
        g.append(vec![rec(10), rec(10)], 1);
        assert!(g.should_flush());
    }

    #[test]
    fn byte_threshold_triggers_flush() {
        let mut g = CommitGroup::<TestRecord>::new(thresholds(100, 50, 60_000));
        g.append(vec![rec(40)], 1);
        assert!(!g.should_flush());
        g.append(vec![rec(20)], 2);
        assert!(g.should_flush());
    }

    #[test]
    fn age_threshold_triggers_flush() {
        let mut g = CommitGroup::<TestRecord>::new(thresholds(100, 10_000, 50));
        g.append(vec![rec(10)], 1);
        // Synthesize a clock advance.
        let opened = g.opened_at().unwrap();
        let later = opened + Duration::from_millis(60);
        assert!(g.should_flush_at(later));
        // And the timer reports zero remaining.
        assert_eq!(g.time_until_max_age(later), Some(Duration::ZERO));
    }

    #[test]
    fn time_until_max_age_decreases_with_clock_advance() {
        let mut g = CommitGroup::<TestRecord>::new(thresholds(100, 10_000, 1000));
        g.append(vec![rec(10)], 1);
        let opened = g.opened_at().unwrap();
        let halfway = opened + Duration::from_millis(400);
        let remaining = g.time_until_max_age(halfway).unwrap();
        assert!(remaining >= Duration::from_millis(599));
        assert!(remaining <= Duration::from_millis(601));
    }

    #[test]
    fn drain_returns_records_and_resets() {
        let mut g = CommitGroup::<TestRecord>::new(thresholds(2, 10_000, 1000));
        g.append(vec![rec(5), rec(7)], 9);
        let drained = g.drain();
        assert_eq!(drained.records.len(), 2);
        assert_eq!(drained.low_sequence, 9);
        assert_eq!(drained.high_sequence, 9);
        assert!(g.is_empty());
        assert_eq!(g.bytes(), 0);
        assert!(g.high_watermark().is_none());
    }

    #[test]
    fn drain_after_zero_row_appends_returns_empty_batch_with_progress() {
        // The zero-row case: Buffer batches absorbed without any decoded
        // rows still flush a CommitGroupBatch with low/high set so the
        // ack controller advances Buffer past those sequences.
        let mut g = CommitGroup::<TestRecord>::new(thresholds(10, 10_000, 0));
        g.append(vec![], 3);
        g.append(vec![], 4);
        // age threshold of 0 always trips after any append
        assert!(g.should_flush_at(g.opened_at().unwrap() + Duration::from_millis(1)));
        let drained = g.drain();
        assert!(drained.records.is_empty());
        assert_eq!(drained.low_sequence, 3);
        assert_eq!(drained.high_sequence, 4);
    }
}
