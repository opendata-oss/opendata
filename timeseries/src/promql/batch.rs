//! The columnar data types that flow between operators.
//!
//! A query plan is a tree of operators; [`StepBatch`] is the one shape that
//! moves along every edge. Every supporting type here exists so batches can
//! carry just enough metadata to be produced, consumed, and re-assembled
//! without per-cell lookups.
//!
//! - [`StepBatch`]: the wire shape. A rectangle of `f64` values over
//!   `series × step` with a parallel validity bitset.
//! - [`SeriesSchema`]: the per-query series roster (the full list of series
//!   a query will touch), dense-indexed by `series_idx: u32` so operator
//!   state can use arrays instead of label hashmaps.
//! - [`SchemaRef`]: a handle to an operator's output schema, callable before
//!   the operator has produced any batches. [`SchemaRef::Deferred`] is the
//!   one escape hatch for `count_values`, whose output labels depend on
//!   sample values.
//! - [`BitSet`]: the `Vec<u64>` bitset behind every batch's validity column.

use std::ops::Range;
use std::sync::Arc;

use crate::model::Labels;

/// The wire shape of the engine — operators produce and consume
/// `StepBatch`es, and nothing else flows between them.
///
/// Each batch covers a rectangle of the query's output: a contiguous slice of
/// the series roster (`series_range`) across a contiguous slice of output
/// steps (`step_range`). `values` holds the numbers in a flat `Vec<f64>`,
/// laid out step-by-step — all series for step 0, then all series for step 1,
/// and so on (row-major by step). `validity` tracks which cells actually
/// carry a sample; a cleared bit means "no sample in the lookback window,"
/// which is distinct from `NaN` (a real value) and from `STALE_NAN` (an
/// explicit staleness marker preserved from the source).
///
/// A query usually emits many batches: leaf operators tile the
/// `series × step` grid into sub-rectangles so a single batch's allocation
/// stays bounded, and downstream operators either pass the tiling through
/// or repackage it (see [`RechunkOp`](super::operators::rechunk::RechunkOp)).
///
/// For [`SchemaRef::Deferred`] outputs, `series_range` is only meaningful
/// after the deferred child has produced its first batch. Shape invariants
/// are checked in debug builds by [`StepBatch::new`].
#[derive(Debug, Clone)]
pub struct StepBatch {
    /// Absolute evaluation timestamps (ms). `Arc`-shared across every batch in a query.
    pub step_timestamps: Arc<[i64]>,
    pub step_range: Range<usize>,

    pub series: SchemaRef,
    pub series_range: Range<usize>,

    /// Row-major by step: cell `(t, s)` lives at `values[t * series_count + s]`.
    pub values: Vec<f64>,
    /// Bit `i` set iff `values[i]` is a real sample. Cleared means "no sample in
    /// lookback window" — distinct from `NaN` (real value) and `STALE_NAN` (explicit
    /// marker preserved by the source).
    pub validity: BitSet,

    /// Per-cell source-sample timestamps. Populated only by [`VectorSelectorOp`],
    /// dropped by any operator that derives a new value, matching Prometheus'
    /// `timestamp()` semantics. [`InstantFnKind::Timestamp`] consults this column
    /// before falling back to the step timestamp.
    ///
    /// [`VectorSelectorOp`]: super::operators::vector_selector::VectorSelectorOp
    /// [`InstantFnKind::Timestamp`]: super::operators::instant_fn::InstantFnKind::Timestamp
    pub source_timestamps: Option<Arc<[i64]>>,
}

impl StepBatch {
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
            source_timestamps: None,
        }
    }

    /// Attach per-cell source-sample timestamps. Only
    /// [`super::operators::vector_selector::VectorSelectorOp`] should call this.
    pub fn with_source_timestamps(mut self, source_timestamps: Arc<[i64]>) -> Self {
        debug_assert_eq!(
            source_timestamps.len(),
            self.values.len(),
            "source_timestamps.len()={} but values.len()={}",
            source_timestamps.len(),
            self.values.len(),
        );
        self.source_timestamps = Some(source_timestamps);
        self
    }

    #[inline]
    pub fn step_count(&self) -> usize {
        self.step_range.len()
    }

    #[inline]
    pub fn series_count(&self) -> usize {
        self.series_range.len()
    }

    /// Total number of `(step, series)` cells.
    #[inline]
    pub fn len(&self) -> usize {
        self.step_count() * self.series_count()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.step_count() == 0 || self.series_count() == 0
    }

    #[inline]
    pub fn step_timestamps_slice(&self) -> &[i64] {
        &self.step_timestamps[self.step_range.clone()]
    }

    /// Linear index into `values` / `validity` for the cell at
    /// `(step_offset, series_offset)`, 0-indexed within this batch's ranges.
    #[inline]
    pub fn cell_index(&self, step_offset: usize, series_offset: usize) -> usize {
        debug_assert!(step_offset < self.step_count());
        debug_assert!(series_offset < self.series_count());
        step_offset * self.series_count() + series_offset
    }

    /// `None` when the cell's validity bit is clear.
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

/// The full list of series a query will touch, built once by the physical
/// planner and shared (via `Arc`) across every batch for that query.
///
/// Because the roster is fixed at plan time, a series can be identified by
/// a dense `series_idx: u32` — an index into this schema — so operator state
/// (group maps, match tables, per-series accumulators) uses arrays rather
/// than `HashMap<Labels, _>`. Fingerprints are kept alongside labels so
/// comparisons across buckets don't have to re-hash labelsets.
#[derive(Debug, Clone)]
pub struct SeriesSchema {
    labels: Arc<[Labels]>,
    /// Stable cross-bucket fingerprint for each series.
    fingerprints: Arc<[u128]>,
}

impl SeriesSchema {
    /// Panics on mismatched lengths — a plan-time programmer error.
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

    pub fn empty() -> Self {
        Self {
            labels: Arc::from(Vec::<Labels>::new()),
            fingerprints: Arc::from(Vec::<u128>::new()),
        }
    }

    /// Exclusive upper bound for any legal `series_idx`.
    #[inline]
    pub fn len(&self) -> usize {
        self.labels.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.labels.is_empty()
    }

    #[inline]
    pub fn labels(&self, series_idx: u32) -> &Labels {
        &self.labels[series_idx as usize]
    }

    #[inline]
    pub fn labels_slice(&self) -> &[Labels] {
        &self.labels
    }

    #[inline]
    pub fn fingerprint(&self, series_idx: u32) -> u128 {
        self.fingerprints[series_idx as usize]
    }

    #[inline]
    pub fn fingerprints_slice(&self) -> &[u128] {
        &self.fingerprints
    }
}

/// Handle to an operator's output series schema, callable before the
/// operator has produced any batches.
///
/// Almost every operator can name its output series at plan time — it either
/// forwards the child's schema or derives a new one from plan-time data —
/// and so returns [`SchemaRef::Static`]. The one exception is `count_values`,
/// whose output labels include each distinct observed sample value and so
/// cannot be known until the child has been drained. It publishes
/// [`SchemaRef::Deferred`] from [`super::operator::Operator::schema`] and
/// stamps a concrete `Static` schema onto each emitted batch. Downstream
/// operators read `Deferred` as "this schema is known once my child has
/// produced its first batch."
#[derive(Debug, Clone)]
pub enum SchemaRef {
    Static(Arc<SeriesSchema>),
    Deferred,
}

impl SchemaRef {
    pub fn empty_static() -> Self {
        SchemaRef::Static(Arc::new(SeriesSchema::empty()))
    }

    pub fn as_static(&self) -> Option<&Arc<SeriesSchema>> {
        match self {
            SchemaRef::Static(schema) => Some(schema),
            SchemaRef::Deferred => None,
        }
    }

    pub fn is_deferred(&self) -> bool {
        matches!(self, SchemaRef::Deferred)
    }
}

/// Fixed-length bitset backed by `Vec<u64>`, used as the validity column of
/// every [`StepBatch`]. Default (all zero) means "all cells absent."
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BitSet {
    bits: Vec<u64>,
    len: usize,
}

impl BitSet {
    /// All bits cleared.
    pub fn with_len(len: usize) -> Self {
        let word_count = len.div_ceil(64);
        Self {
            bits: vec![0u64; word_count],
            len,
        }
    }

    /// All bits set.
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

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Panics if `idx >= len`.
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

    /// Panics if `idx >= len`.
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

    /// Panics if `idx >= len`.
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

    /// Linear in `len / 64`.
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
