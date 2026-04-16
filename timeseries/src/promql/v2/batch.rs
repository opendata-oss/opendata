//! Core columnar data types for the v2 execution engine.
//!
//! Defines:
//! - [`StepBatch`]: the universal on-the-wire shape between operators (a
//!   contiguous chunk of series × a contiguous range of output steps,
//!   columnar, float-only).
//! - [`SeriesSchema`]: the per-query series roster referenced by every
//!   batch via `Arc`. Labels and fingerprints are dense-indexed by
//!   `series_idx: u32`.
//! - [`SchemaRef`]: the handle operators publish via `Operator::schema`.
//!   `Static` covers every operator; `Deferred` is the RFC's one named
//!   escape hatch for `count_values`, whose output labelset is derived
//!   from runtime sample values.
//! - [`BitSet`]: hand-rolled `Vec<u64>` bitset used for sample validity
//!   (no new workspace dependency).
//!
//! See RFC 0007 §"Core Data Model".
//!
//! Subsequent units extend these types — notably unit 1.4
//! ([`super::memory::MemoryReservation`]) will add `try_grow`-routed
//! allocation helpers, and the planner units will populate `SeriesSchema`
//! with group maps and fingerprint indexes.

use std::ops::Range;
use std::sync::Arc;

use crate::model::Labels;

/// A contiguous rectangle of values for `series_range × step_range`,
/// streamed between operators.
///
/// Invariants (checked in debug by [`StepBatch::new`]):
/// - `step_range` is a sub-range of `0..step_timestamps.len()`.
/// - `series_range` is a sub-range of `0..series.len()` when `series` is
///   [`SchemaRef::Static`]. For [`SchemaRef::Deferred`] the range is only
///   valid after the deferred child has produced its first batch.
/// - `values.len() == validity.len() == step_count * series_count`.
///
/// See RFC 0007 §"Core Data Model".
#[derive(Debug, Clone)]
pub struct StepBatch {
    /// Absolute evaluation timestamps (ms). Shared by every batch in a
    /// query via `Arc`. One allocation per query, pointer-copied here.
    pub step_timestamps: Arc<[i64]>,
    /// Slice of [`Self::step_timestamps`] covered by this batch.
    pub step_range: Range<usize>,

    /// Series roster. `Arc`-shared across every batch emitted from a
    /// single operator (and usually across the whole plan).
    pub series: SchemaRef,
    /// Slice of the series roster covered by this batch.
    pub series_range: Range<usize>,

    /// Sample values, SoA, float-only in v1.
    ///
    /// Layout: **row-major by step**. The sample for the `s`th series in
    /// this batch (0-indexed within `series_range`) at the `t`th step in
    /// this batch (0-indexed within `step_range`) lives at
    /// `values[t * series_count + s]`, where `series_count = series_range.len()`.
    ///
    /// Length is always `step_count * series_count`.
    pub values: Vec<f64>,
    /// Per-cell validity. Bit `i` of [`Self::validity`] is set iff
    /// `values[i]` is a sample; cleared means "no sample in lookback
    /// window" (distinct from `NaN`, which is a real value, and from
    /// `STALE_NAN`, which is a real explicit marker preserved by the
    /// source).
    pub validity: BitSet,
}

impl StepBatch {
    /// Construct a new batch. In debug builds, verifies shape invariants.
    pub fn new(
        step_timestamps: Arc<[i64]>,
        step_range: Range<usize>,
        series: SchemaRef,
        series_range: Range<usize>,
        values: Vec<f64>,
        validity: BitSet,
    ) -> Self {
        debug_assert!(
            step_range.end <= step_timestamps.len(),
            "step_range {:?} exceeds step_timestamps len {}",
            step_range,
            step_timestamps.len(),
        );
        debug_assert!(
            step_range.start <= step_range.end,
            "step_range {step_range:?} is inverted",
        );
        debug_assert!(
            series_range.start <= series_range.end,
            "series_range {series_range:?} is inverted",
        );
        if let SchemaRef::Static(schema) = &series {
            debug_assert!(
                series_range.end <= schema.len(),
                "series_range {:?} exceeds series schema len {}",
                series_range,
                schema.len(),
            );
        }
        let expected = step_range.len() * series_range.len();
        debug_assert_eq!(
            values.len(),
            expected,
            "values.len()={} but step_count*series_count={}",
            values.len(),
            expected,
        );
        debug_assert_eq!(
            validity.len(),
            expected,
            "validity.len()={} but step_count*series_count={}",
            validity.len(),
            expected,
        );

        Self {
            step_timestamps,
            step_range,
            series,
            series_range,
            values,
            validity,
        }
    }

    /// Number of steps covered by this batch.
    #[inline]
    pub fn step_count(&self) -> usize {
        self.step_range.len()
    }

    /// Number of series covered by this batch.
    #[inline]
    pub fn series_count(&self) -> usize {
        self.series_range.len()
    }

    /// Total number of `(step, series)` cells = `step_count * series_count`.
    #[inline]
    pub fn len(&self) -> usize {
        self.step_count() * self.series_count()
    }

    /// `true` when the batch has zero cells (either zero steps or zero
    /// series).
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.step_count() == 0 || self.series_count() == 0
    }

    /// Timestamps for the steps covered by this batch.
    #[inline]
    pub fn step_timestamps_slice(&self) -> &[i64] {
        &self.step_timestamps[self.step_range.clone()]
    }

    /// Linear index into [`Self::values`] / [`Self::validity`] for the
    /// cell at `(step_offset, series_offset)`, where offsets are
    /// 0-indexed within this batch's ranges (not the global schema).
    ///
    /// Row-major by step: `step_offset * series_count + series_offset`.
    #[inline]
    pub fn cell_index(&self, step_offset: usize, series_offset: usize) -> usize {
        debug_assert!(step_offset < self.step_count());
        debug_assert!(series_offset < self.series_count());
        step_offset * self.series_count() + series_offset
    }

    /// Read the value at `(step_offset, series_offset)`. Returns `None`
    /// when the cell is absent (validity bit clear).
    #[inline]
    pub fn get(&self, step_offset: usize, series_offset: usize) -> Option<f64> {
        let idx = self.cell_index(step_offset, series_offset);
        if self.validity.get(idx) {
            Some(self.values[idx])
        } else {
            None
        }
    }
}

/// Per-query series roster. Plan-time constructed, read-only during
/// execution. Indexed by dense `series_idx: u32` so operator state can
/// be dense-array-keyed rather than `HashMap<Labels, …>`.
///
/// Subsequent units will extend this struct with group maps, binary
/// matching tables, and bucket-membership metadata; the minimum shape
/// here is what unit 1.3 needs.
#[derive(Debug, Clone)]
pub struct SeriesSchema {
    /// Label set for each series. `labels[i]` corresponds to
    /// `series_idx = i as u32`.
    labels: Arc<[Labels]>,
    /// Stable cross-bucket fingerprint for each series. Same dense
    /// indexing as [`Self::labels`].
    fingerprints: Arc<[u128]>,
}

impl SeriesSchema {
    /// Build a schema from parallel label / fingerprint vectors.
    ///
    /// Panics if the two slices have mismatched lengths — this is a
    /// plan-time programmer error, not a runtime condition.
    pub fn new(labels: Arc<[Labels]>, fingerprints: Arc<[u128]>) -> Self {
        assert_eq!(
            labels.len(),
            fingerprints.len(),
            "labels and fingerprints must have identical length",
        );
        Self {
            labels,
            fingerprints,
        }
    }

    /// Empty schema — used as a placeholder and by tests.
    pub fn empty() -> Self {
        Self {
            labels: Arc::from(Vec::<Labels>::new()),
            fingerprints: Arc::from(Vec::<u128>::new()),
        }
    }

    /// Number of series in the roster. Also the exclusive upper bound
    /// for any legal `series_idx` / `series_range.end`.
    #[inline]
    pub fn len(&self) -> usize {
        self.labels.len()
    }

    /// `true` when the roster has zero series.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.labels.is_empty()
    }

    /// Labels for a given `series_idx`.
    #[inline]
    pub fn labels(&self, series_idx: u32) -> &Labels {
        &self.labels[series_idx as usize]
    }

    /// All labels, dense-indexed.
    #[inline]
    pub fn labels_slice(&self) -> &[Labels] {
        &self.labels
    }

    /// Cross-bucket fingerprint for a given `series_idx`.
    #[inline]
    pub fn fingerprint(&self, series_idx: u32) -> u128 {
        self.fingerprints[series_idx as usize]
    }

    /// All fingerprints, dense-indexed.
    #[inline]
    pub fn fingerprints_slice(&self) -> &[u128] {
        &self.fingerprints
    }
}

/// Handle to an operator's output series schema.
///
/// Every operator exposes a schema before `next()` is called. In v1 this
/// is [`SchemaRef::Static`] for every operator except `count_values`,
/// whose output labelset is derived from runtime sample values; it
/// publishes [`SchemaRef::Deferred`] and only binds to a `Static` schema
/// after draining its child. Downstream operators treat `Deferred` as
/// "schema known after my child's first batch."
///
/// See RFC 0007 §"Core Data Model".
#[derive(Debug, Clone)]
pub enum SchemaRef {
    /// Statically-known schema, available at plan time.
    Static(Arc<SeriesSchema>),
    /// Schema is value-dependent and will be resolved at runtime by the
    /// producing operator (i.e., `count_values`).
    Deferred,
}

impl SchemaRef {
    /// Static helper for tests and callers that want an empty schema.
    pub fn empty_static() -> Self {
        SchemaRef::Static(Arc::new(SeriesSchema::empty()))
    }

    /// `Some(&schema)` when the schema is statically known, `None` when
    /// [`SchemaRef::Deferred`].
    pub fn as_static(&self) -> Option<&Arc<SeriesSchema>> {
        match self {
            SchemaRef::Static(schema) => Some(schema),
            SchemaRef::Deferred => None,
        }
    }

    /// `true` if this is a [`SchemaRef::Deferred`] handle.
    pub fn is_deferred(&self) -> bool {
        matches!(self, SchemaRef::Deferred)
    }
}

/// Fixed-length bitset backed by `Vec<u64>`.
///
/// Hand-rolled deliberately: the RFC forbids adding workspace
/// dependencies for this unit, and the required surface area
/// (`with_len`, `get`, `set`, `clear`, `len`) is trivial. All-zero
/// default means "all cells absent," matching the intended use for
/// `StepBatch::validity`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BitSet {
    bits: Vec<u64>,
    len: usize,
}

impl BitSet {
    /// Create a bitset of the given length with all bits cleared
    /// (i.e., all cells marked absent).
    pub fn with_len(len: usize) -> Self {
        let word_count = len.div_ceil(64);
        Self {
            bits: vec![0u64; word_count],
            len,
        }
    }

    /// Create a bitset of the given length with all bits set
    /// (i.e., all cells marked present).
    pub fn all_set(len: usize) -> Self {
        let word_count = len.div_ceil(64);
        let mut bits = vec![u64::MAX; word_count];
        if !len.is_multiple_of(64)
            && let Some(last) = bits.last_mut()
        {
            *last = (1u64 << (len % 64)) - 1;
        }
        Self { bits, len }
    }

    /// Number of tracked bits (equal to `step_count * series_count` for
    /// a `StepBatch`).
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// `true` when zero bits are tracked.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Read the bit at `idx`. Panics if `idx >= len`.
    #[inline]
    pub fn get(&self, idx: usize) -> bool {
        assert!(
            idx < self.len,
            "index {} out of bounds for bitset of len {}",
            idx,
            self.len
        );
        let (word, bit) = (idx / 64, idx % 64);
        (self.bits[word] >> bit) & 1 == 1
    }

    /// Set the bit at `idx`. Panics if `idx >= len`.
    #[inline]
    pub fn set(&mut self, idx: usize) {
        assert!(
            idx < self.len,
            "index {} out of bounds for bitset of len {}",
            idx,
            self.len
        );
        let (word, bit) = (idx / 64, idx % 64);
        self.bits[word] |= 1u64 << bit;
    }

    /// Clear the bit at `idx`. Panics if `idx >= len`.
    #[inline]
    pub fn clear(&mut self, idx: usize) {
        assert!(
            idx < self.len,
            "index {} out of bounds for bitset of len {}",
            idx,
            self.len
        );
        let (word, bit) = (idx / 64, idx % 64);
        self.bits[word] &= !(1u64 << bit);
    }

    /// Count of set bits. Linear in `len / 64`; callers relying on this
    /// repeatedly should cache it.
    pub fn count_ones(&self) -> usize {
        self.bits.iter().map(|w| w.count_ones() as usize).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Label, Labels};

    fn mk_labels(name: &str) -> Labels {
        Labels::new(vec![Label {
            name: "__name__".to_string(),
            value: name.to_string(),
        }])
    }

    fn mk_schema(n: usize) -> Arc<SeriesSchema> {
        let labels: Vec<Labels> = (0..n).map(|i| mk_labels(&format!("m{i}"))).collect();
        let fps: Vec<u128> = (0..n as u128).collect();
        Arc::new(SeriesSchema::new(Arc::from(labels), Arc::from(fps)))
    }

    #[test]
    fn should_report_len_as_step_count_times_series_count() {
        // given: a 3-step × 4-series batch
        let steps: Arc<[i64]> = Arc::from(vec![0i64, 1, 2, 3, 4]);
        let schema = mk_schema(10);
        let values = vec![0.0; 3 * 4];
        let validity = BitSet::with_len(3 * 4);

        // when
        let batch = StepBatch::new(
            steps,
            1..4,
            SchemaRef::Static(schema),
            2..6,
            values,
            validity,
        );

        // then
        assert_eq!(batch.step_count(), 3);
        assert_eq!(batch.series_count(), 4);
        assert_eq!(batch.len(), 12);
        assert!(!batch.is_empty());
    }

    #[test]
    fn should_index_cells_row_major_by_step() {
        // given: values laid out row-major by step (step-major)
        // 2 steps × 3 series: [s0_step0, s1_step0, s2_step0, s0_step1, s1_step1, s2_step1]
        let steps: Arc<[i64]> = Arc::from(vec![10i64, 20]);
        let schema = mk_schema(3);
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0];
        let validity = BitSet::all_set(6);
        let batch = StepBatch::new(
            steps,
            0..2,
            SchemaRef::Static(schema),
            0..3,
            values,
            validity,
        );

        // when / then: row-major by step
        assert_eq!(batch.cell_index(0, 0), 0);
        assert_eq!(batch.cell_index(0, 2), 2);
        assert_eq!(batch.cell_index(1, 0), 3);
        assert_eq!(batch.cell_index(1, 2), 5);
        assert_eq!(batch.get(0, 0), Some(1.0));
        assert_eq!(batch.get(0, 2), Some(3.0));
        assert_eq!(batch.get(1, 0), Some(4.0));
        assert_eq!(batch.get(1, 2), Some(6.0));
    }

    #[test]
    fn should_return_none_for_invalid_cells() {
        // given: 1 step × 2 series with only series 1 valid
        let steps: Arc<[i64]> = Arc::from(vec![0i64]);
        let schema = mk_schema(2);
        let values = vec![42.0, 99.0];
        let mut validity = BitSet::with_len(2);
        validity.set(1);
        let batch = StepBatch::new(
            steps,
            0..1,
            SchemaRef::Static(schema),
            0..2,
            values,
            validity,
        );

        // when / then
        assert_eq!(batch.get(0, 0), None);
        assert_eq!(batch.get(0, 1), Some(99.0));
    }

    #[test]
    fn should_slice_step_timestamps_by_step_range() {
        // given: 5 step timestamps, batch covers the middle three
        let steps: Arc<[i64]> = Arc::from(vec![10i64, 20, 30, 40, 50]);
        let schema = mk_schema(1);
        let values = vec![0.0; 3];
        let validity = BitSet::with_len(3);
        let batch = StepBatch::new(
            steps,
            1..4,
            SchemaRef::Static(schema),
            0..1,
            values,
            validity,
        );

        // when
        let slice = batch.step_timestamps_slice();

        // then
        assert_eq!(slice, &[20, 30, 40]);
    }

    #[test]
    fn should_report_is_empty_for_zero_steps_or_zero_series() {
        // given: a schema of size 3
        let steps: Arc<[i64]> = Arc::from(vec![0i64, 1]);
        let schema = mk_schema(3);

        // when: zero step range
        let batch = StepBatch::new(
            steps.clone(),
            1..1,
            SchemaRef::Static(schema.clone()),
            0..2,
            vec![],
            BitSet::with_len(0),
        );
        // then
        assert!(batch.is_empty());

        // when: zero series range
        let batch = StepBatch::new(
            steps,
            0..2,
            SchemaRef::Static(schema),
            1..1,
            vec![],
            BitSet::with_len(0),
        );
        // then
        assert!(batch.is_empty());
    }

    #[test]
    #[should_panic(expected = "step_range")]
    fn should_panic_when_step_range_exceeds_timestamps() {
        // given: 2 timestamps
        let steps: Arc<[i64]> = Arc::from(vec![0i64, 1]);
        let schema = mk_schema(1);

        // when: step_range extends past the end — only trips in debug builds
        let _ = StepBatch::new(
            steps,
            0..3,
            SchemaRef::Static(schema),
            0..1,
            vec![0.0; 3],
            BitSet::with_len(3),
        );
        // then: debug_assert panics
    }

    #[test]
    fn should_expose_static_schema_and_none_for_deferred() {
        // given
        let schema = mk_schema(2);
        let s = SchemaRef::Static(schema.clone());
        let d = SchemaRef::Deferred;

        // when / then
        assert!(!s.is_deferred());
        assert!(d.is_deferred());
        assert!(s.as_static().is_some());
        assert!(d.as_static().is_none());
    }

    #[test]
    fn should_construct_empty_schema_with_zero_len() {
        // given / when
        let schema = SeriesSchema::empty();

        // then
        assert_eq!(schema.len(), 0);
        assert!(schema.is_empty());
    }

    #[test]
    #[should_panic(expected = "labels and fingerprints must have identical length")]
    fn should_panic_on_mismatched_labels_and_fingerprints() {
        // given: 2 labels, 1 fingerprint
        let labels: Arc<[Labels]> = Arc::from(vec![mk_labels("a"), mk_labels("b")]);
        let fps: Arc<[u128]> = Arc::from(vec![1u128]);

        // when
        let _ = SeriesSchema::new(labels, fps);
        // then: panics
    }

    #[test]
    fn should_index_schema_labels_and_fingerprints_by_series_idx() {
        // given
        let schema = mk_schema(3);

        // when / then
        assert_eq!(schema.labels(0).metric_name(), "m0");
        assert_eq!(schema.labels(2).metric_name(), "m2");
        assert_eq!(schema.fingerprint(0), 0);
        assert_eq!(schema.fingerprint(2), 2);
        assert_eq!(schema.labels_slice().len(), 3);
        assert_eq!(schema.fingerprints_slice().len(), 3);
    }

    #[test]
    fn should_start_bitset_with_all_bits_clear() {
        // given
        let bs = BitSet::with_len(130);

        // when / then
        assert_eq!(bs.len(), 130);
        assert!(!bs.is_empty());
        assert_eq!(bs.count_ones(), 0);
        for i in 0..130 {
            assert!(!bs.get(i));
        }
    }

    #[test]
    fn should_set_and_clear_individual_bits() {
        // given: spans 3 u64 words to exercise word crossings
        let mut bs = BitSet::with_len(200);

        // when
        bs.set(0);
        bs.set(63);
        bs.set(64);
        bs.set(127);
        bs.set(199);

        // then
        assert!(bs.get(0));
        assert!(bs.get(63));
        assert!(bs.get(64));
        assert!(bs.get(127));
        assert!(bs.get(199));
        assert!(!bs.get(1));
        assert!(!bs.get(62));
        assert!(!bs.get(128));
        assert_eq!(bs.count_ones(), 5);

        // when
        bs.clear(64);
        // then
        assert!(!bs.get(64));
        assert_eq!(bs.count_ones(), 4);
    }

    #[test]
    fn should_initialize_all_set_bitset_with_every_bit_set() {
        // given: non-word-aligned length
        let bs = BitSet::all_set(130);

        // when / then
        for i in 0..130 {
            assert!(bs.get(i), "bit {i} should be set");
        }
        assert_eq!(bs.count_ones(), 130);
    }

    #[test]
    fn should_initialize_all_set_with_word_aligned_length() {
        // given: exactly 128 bits (two full words)
        let bs = BitSet::all_set(128);

        // when / then
        assert_eq!(bs.count_ones(), 128);
        for i in 0..128 {
            assert!(bs.get(i));
        }
    }

    #[test]
    #[should_panic(expected = "out of bounds")]
    fn should_panic_when_bitset_get_is_out_of_bounds() {
        // given
        let bs = BitSet::with_len(10);

        // when
        let _ = bs.get(10);
        // then: panics
    }

    #[test]
    fn should_report_empty_bitset_as_empty() {
        // given / when
        let bs = BitSet::with_len(0);

        // then
        assert_eq!(bs.len(), 0);
        assert!(bs.is_empty());
        assert_eq!(bs.count_ones(), 0);
    }
}
