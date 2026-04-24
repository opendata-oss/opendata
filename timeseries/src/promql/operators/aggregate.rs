//! `AggregateOp` implements PromQL's aggregation operators: `sum by (…)`,
//! `avg`, `min`, `max`, `count`, `stddev`, `stdvar`, `group`, `topk`,
//! `bottomk`, and `quantile`. One operator type handles all of them; the
//! specific reduction is selected by an [`AggregateKind`] enum.
//! (`count_values` has its own operator because its output labels depend on
//! sample values, not just label matchers.)
//!
//! PromQL's `by (…)` / `without (…)` decides which input series end up in
//! the same output group. That mapping is fixed by the query's labels and
//! doesn't depend on sample data, so the planner computes it once up
//! front — the operator just receives a [`GroupMap`]
//! (input series index → output group index) and reads from it. For
//! `sum by (pod) (http_requests_total)`, the planner maps each input
//! series to a group index keyed on its `pod` label value before any
//! samples are fetched. No grouping logic at runtime.
//!
//! # Streaming kinds
//!
//! `Sum`, `Avg`, `Min`, `Max`, `Count`, `Stddev`, `Stdvar`, `Group` apply a
//! per-cell single-pass reducer. The child may tile its output arbitrarily
//! (any `(step_range, series_range)` slice), so the operator buffers a
//! `(step × group)` accumulator grid and only emits once the child signals
//! end-of-stream. Grid footprint is
//! `O(step_count × output_groups × sizeof(Accumulator))`.
//!
//! # Breaker kinds — `Topk`, `Bottomk`, `Quantile`
//!
//! These buffer the whole step before emitting (K-selection and quantile
//! interpolation need every input for the step, so they can't stream
//! cell-by-cell like the reducer kinds — hence "breaker").
//!
//! | kind         | output schema  | per-step scratch        |
//! |--------------|----------------|-------------------------|
//! | `Topk(k)`    | input series   | `k × group_count` heap  |
//! | `Bottomk(k)` | input series   | `k × group_count` heap  |
//! | `Quantile(q)`| group series   | per-group sort buffer   |
//!
//! `topk` / `bottomk` filter (preserve input series, flip validity on
//! unselected cells); `quantile` reduces (one cell per group). The planner
//! picks the output schema; the operator debug-asserts its size matches the
//! variant.
//!
//! Tie-break for `topk` / `bottomk`: equal values go to the lower
//! input-series index (deterministic). NaN inputs rank worst. `k < 1`
//! selects nothing; `k` past input width selects every valid cell.
//!
//! `quantile`: `q < 0` ⇒ `-inf`, `q > 1` ⇒ `+inf`, `q == NaN` ⇒ `NaN`.
//!
//! # Group map
//!
//! `input_to_group: Vec<Option<u32>>` — `None` drops an input from all
//! aggregations. `by ()` ⇒ all inputs map to group 0. `without ()` ⇒ one
//! group per input.
//!
//! # Validity
//!
//! Every kind emits a valid output cell iff at least one valid input
//! contributed to its group. `Group` always emits `1.0`; `Count` emits the
//! count (always ≥ 1 when valid).
//!
//! # Memory
//!
//! Streaming-kind accumulator grid is allocated once at construction and
//! fails with [`QueryError::MemoryLimit`] if it doesn't fit. Breaker
//! scratch is per-step, drained between steps. Output batches allocate
//! per-poll.
//!
//! Output schema is always [`SchemaRef::Static`].

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;
use std::task::{Context, Poll};

use super::super::batch::{BitSet, SchemaRef, SeriesSchema, StepBatch};
use super::super::memory::{MemoryReservation, QueryError};
use super::super::operator::{Operator, OperatorSchema};

// ---------------------------------------------------------------------------
// AggregateKind — function selection as data
// ---------------------------------------------------------------------------

/// Discriminant selecting the per-group reducer.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AggregateKind {
    /// Kahan-compensated.
    Sum,
    /// Overflow-resistant running mean.
    Avg,
    /// NaN-safe (NaN ignored when a real value exists).
    Min,
    /// NaN-safe (NaN ignored when a real value exists).
    Max,
    /// Integer count emitted as `f64`.
    Count,
    /// Welford single-pass.
    Stddev,
    /// Welford single-pass.
    Stdvar,
    /// Always `1.0` when any input contributed.
    Group,
    /// `topk(k, v)`. Output schema is the **input series**; unselected cells
    /// get `validity = 0`. See module docs for tie-break / k semantics.
    Topk(i64),
    /// `bottomk(k, v)` — smallest-K counterpart to [`Self::Topk`].
    Bottomk(i64),
    /// `quantile(q, v)` — per-group, per-step q-th quantile with linear
    /// interpolation between ranks. Shares the streaming output-schema
    /// shape (one cell per group). See `rollup_fns::quantile` in
    /// `operators::rollup` for the canonical reducer; this operator
    /// calls into the same implementation.
    Quantile(f64),
}

impl AggregateKind {
    /// True for variants that buffer the whole step before emitting
    /// (pipeline breakers).
    #[inline]
    fn is_breaker(self) -> bool {
        matches!(self, Self::Topk(_) | Self::Bottomk(_) | Self::Quantile(_))
    }

    /// True for variants whose output-series count equals the input
    /// series count (filter-shaped). False for reducer-shaped variants
    /// (output-series count == group_count).
    #[inline]
    fn output_is_inputs(self) -> bool {
        matches!(self, Self::Topk(_) | Self::Bottomk(_))
    }
}

// ---------------------------------------------------------------------------
// GroupMap — the planner-built series-index → group-index mapping
// ---------------------------------------------------------------------------

/// Input-series index → output-group index, precomputed by the planner
/// from the query's `by` / `without` clause. The operator reads this array
/// to decide which accumulator absorbs each cell; it never recomputes
/// grouping itself.
///
/// Example: for `sum by (pod) (http_requests_total)`, the planner walks the
/// resolved series list, bucketises by `pod` label value, and records a
/// group index (or `None` to drop the input) for each input series here.
///
/// Invariants (planner-guaranteed):
/// - `input_to_group.len()` equals the input operator's series count.
/// - Every `Some(g)` satisfies `g < group_count`.
#[derive(Debug, Clone)]
pub struct GroupMap {
    /// `None` drops an input from aggregation.
    pub input_to_group: Vec<Option<u32>>,
    pub group_count: usize,
}

impl GroupMap {
    pub fn new(input_to_group: Vec<Option<u32>>, group_count: usize) -> Self {
        debug_assert!(input_to_group.iter().all(|slot| match slot {
            Some(g) => (*g as usize) < group_count,
            None => true,
        }));
        Self {
            input_to_group,
            group_count,
        }
    }

    #[inline]
    pub fn input_series_count(&self) -> usize {
        self.input_to_group.len()
    }
}

// ---------------------------------------------------------------------------
// Per-group accumulator
// ---------------------------------------------------------------------------

/// Single-pass per-group accumulator. Three overlapping lanes (Kahan sum,
/// NaN-safe min/max, Welford) are all maintained; the reducer reads its own.
#[derive(Debug, Clone, Copy)]
struct Accumulator {
    /// Count of valid inputs that contributed. Also the Welford `n`.
    count: u64,
    /// Kahan sum and its compensation term.
    sum: f64,
    c_sum: f64,
    /// Min / max running extremum — `any_real` tracks whether we've
    /// absorbed a non-NaN value. When `any_real == false` and
    /// `count > 0`, every contribution so far has been NaN; the
    /// extremum stays NaN and the output is NaN (Prometheus semantics).
    min: f64,
    max: f64,
    /// True once at least one **non-NaN** valid value has been absorbed.
    /// Used by Min/Max to decide "first real initialises".
    any_real: bool,
    /// Welford accumulators (Kahan-compensated on both mean and M2).
    mean: f64,
    c_mean: f64,
    m2: f64,
    c_m2: f64,
}

impl Accumulator {
    #[inline]
    fn new() -> Self {
        Self {
            count: 0,
            sum: 0.0,
            c_sum: 0.0,
            min: f64::NAN,
            max: f64::NAN,
            any_real: false,
            mean: 0.0,
            c_mean: 0.0,
            m2: 0.0,
            c_m2: 0.0,
        }
    }

    #[inline]
    fn reset(&mut self) {
        *self = Self::new();
    }

    /// Absorb one valid input cell.
    #[inline]
    fn absorb(&mut self, v: f64) {
        self.count = self.count.saturating_add(1);

        // Kahan sum lane.
        let (new_sum, new_c) = kahan_inc(v, self.sum, self.c_sum);
        self.sum = new_sum;
        self.c_sum = new_c;

        // Min/Max lane — NaN-safe: first real value initialises, NaN
        // inputs are ignored once any real value is present. When every
        // contribution is NaN, `min`/`max` stay NaN.
        if !v.is_nan() {
            if !self.any_real {
                self.min = v;
                self.max = v;
                self.any_real = true;
            } else {
                if v < self.min {
                    self.min = v;
                }
                if v > self.max {
                    self.max = v;
                }
            }
        }

        // Welford lane with Kahan compensation on both mean and M2.
        let n = self.count as f64;
        let delta = v - (self.mean + self.c_mean);
        let (new_mean, new_c_mean) = kahan_inc(delta / n, self.mean, self.c_mean);
        self.mean = new_mean;
        self.c_mean = new_c_mean;
        let new_delta = v - (self.mean + self.c_mean);
        let (new_m2, new_c_m2) = kahan_inc(delta * new_delta, self.m2, self.c_m2);
        self.m2 = new_m2;
        self.c_m2 = new_c_m2;
    }

    #[inline]
    fn sum_value(&self) -> f64 {
        if self.sum.is_infinite() {
            self.sum
        } else {
            self.sum + self.c_sum
        }
    }

    #[inline]
    fn avg_value(&self) -> f64 {
        // Mean via Welford is numerically robust. For groups where the
        // Kahan sum hasn't overflowed, sum/count and mean agree to
        // within the compensation term; we emit the Welford mean for
        // the overflow-resistance the task spec mandates.
        self.mean + self.c_mean
    }

    #[inline]
    fn variance_value(&self) -> f64 {
        // Population variance: M2 / n.
        let n = self.count as f64;
        (self.m2 + self.c_m2) / n
    }
}

/// Kahan–Neumaier compensated summation step. Matches the implementation
/// in [`super::rollup`] bit-for-bit so sum / avg across streaming and
/// range aggregates round identically.
#[inline(never)]
fn kahan_inc(inc: f64, sum: f64, c: f64) -> (f64, f64) {
    let t = sum + inc;
    let new_c = if t.is_infinite() {
        0.0
    } else if sum.abs() >= inc.abs() {
        c + ((sum - t) + inc)
    } else {
        c + ((inc - t) + sum)
    };
    (t, new_c)
}

// ---------------------------------------------------------------------------
// Memory-accounted output buffers
// ---------------------------------------------------------------------------

#[inline]
fn out_bytes(cells: usize) -> usize {
    let values = cells.saturating_mul(std::mem::size_of::<f64>());
    let validity = cells
        .div_ceil(64)
        .saturating_mul(std::mem::size_of::<u64>());
    values.saturating_add(validity)
}

#[inline]
fn accum_bytes(step_count: usize, group_count: usize) -> usize {
    step_count
        .saturating_mul(group_count)
        .saturating_mul(std::mem::size_of::<Accumulator>())
}

/// Upper-bound bytes for the per-group min-heap scratch used by
/// topk/bottomk. Each of the `group_count` groups holds at most `k`
/// entries; when `k` exceeds the input series count the loop caps the
/// actual length, but we reserve on the naïve product as a conservative
/// upper bound. `k <= 0` ⇒ zero bytes (no selection happens).
#[inline]
fn heap_scratch_bytes(k: i64, group_count: usize, input_series: usize) -> usize {
    if k <= 0 {
        return 0;
    }
    let k = (k as usize).min(input_series);
    group_count
        .saturating_mul(k)
        .saturating_mul(std::mem::size_of::<KHeapEntry>())
}

/// Upper-bound bytes for the per-group sort buffer used by `quantile`.
/// Sum of per-group capacities is at most `input_series_count` (each
/// input belongs to at most one group), so reserve `input_series ×
/// sizeof(f64)` + the `Vec<Vec<f64>>` outer skeleton.
#[inline]
fn sort_scratch_bytes(group_count: usize, input_series: usize) -> usize {
    let values = input_series.saturating_mul(std::mem::size_of::<f64>());
    let outer = group_count.saturating_mul(std::mem::size_of::<Vec<f64>>());
    values.saturating_add(outer)
}

/// Bytes for the full `(step × input_series)` breaker grid (values +
/// validity bit). Reserved up front so an undersize reservation surfaces
/// `QueryError::MemoryLimit` at construction rather than deep in the
/// hot path.
#[inline]
fn breaker_grid_bytes(cells: usize) -> usize {
    let values = cells.saturating_mul(std::mem::size_of::<f64>());
    let validity = cells
        .div_ceil(64)
        .saturating_mul(std::mem::size_of::<u64>());
    values.saturating_add(validity)
}

// ---------------------------------------------------------------------------
// Topk / Bottomk heap entry
// ---------------------------------------------------------------------------
//
// Mirrors `evaluator.rs::KHeapEntry` (lines 664-694): a max-heap whose
// ordering treats "greater" as "more likely to be evicted". For topk,
// `SmallestFirst` = min-heap on value (so `peek()` is the smallest
// currently-selected). For bottomk, `LargestFirst` = max-heap on value.
// Ties break by **larger index first** so the lower index wins selection
// (first-seen preference — matches legacy).

#[derive(Clone, Copy, Debug)]
enum KOrder {
    /// For `topk`: peek() returns the smallest value in the K winners.
    SmallestFirst,
    /// For `bottomk`: peek() returns the largest value in the K winners.
    LargestFirst,
}

/// NaN-aware comparison mirroring `evaluator.rs:652-662`.
///
/// Returns the [`Ordering`] used inside the `BinaryHeap`: `Greater`
/// means "more evictable" (pops first). The heap's `peek()` therefore
/// returns the worst currently-kept candidate, and a new candidate
/// replaces it iff the new cmp vs peek is `Less` ("strictly better").
///
/// NaN always ranks "worse" than any real value — a NaN entry will be
/// the first to be evicted when a real candidate arrives.
///
/// | order              | operation   | "worse" = more evictable |
/// |--------------------|-------------|--------------------------|
/// | `SmallestFirst`    | topk (keep K largest)  | smaller values  |
/// | `LargestFirst`     | bottomk (keep K smallest) | larger values |
#[inline]
fn k_cmp(a: f64, b: f64, order: KOrder) -> Ordering {
    match (a.is_nan(), b.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => match order {
            // topk: smaller value ⇒ "worse" ⇒ ordered greater.
            KOrder::SmallestFirst => b.partial_cmp(&a).unwrap_or(Ordering::Equal),
            // bottomk: larger value ⇒ "worse" ⇒ ordered greater.
            KOrder::LargestFirst => a.partial_cmp(&b).unwrap_or(Ordering::Equal),
        },
    }
}

#[derive(Clone, Copy, Debug)]
struct KHeapEntry {
    value: f64,
    /// Input-series index in the full child roster. Used for deterministic
    /// tie-breaks and group-map lookup under tiled input batches.
    global_series_idx: u32,
    /// Input-series offset within the current batch's `series_range`.
    /// Filter-shaped outputs (`topk` / `bottomk`) write back into this local
    /// slice, not the full input roster.
    local_series_idx: u32,
    order: KOrder,
}

impl PartialEq for KHeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.global_series_idx == other.global_series_idx
            && self.value.to_bits() == other.value.to_bits()
    }
}
impl Eq for KHeapEntry {}
impl PartialOrd for KHeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for KHeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // `BinaryHeap` is a max-heap. "Greater" = more evictable.
        // Equal values: larger index is more evictable so the smaller
        // index stays in the winners (first-seen preference; matches
        // `evaluator.rs:687-694`).
        k_cmp(self.value, other.value, self.order)
            .then_with(|| self.global_series_idx.cmp(&other.global_series_idx))
    }
}

struct OutBuffers {
    reservation: MemoryReservation,
    bytes: usize,
    values: Vec<f64>,
    validity: BitSet,
}

impl OutBuffers {
    fn allocate(reservation: &MemoryReservation, cells: usize) -> Result<Self, QueryError> {
        let bytes = out_bytes(cells);
        reservation.try_grow(bytes)?;
        Ok(Self {
            reservation: reservation.clone(),
            bytes,
            values: vec![0.0; cells],
            validity: BitSet::with_len(cells),
        })
    }

    fn finish(mut self) -> (Vec<f64>, BitSet) {
        let values = std::mem::take(&mut self.values);
        let validity = std::mem::replace(&mut self.validity, BitSet::with_len(0));
        self.reservation.release(self.bytes);
        self.bytes = 0;
        (values, validity)
    }
}

impl Drop for OutBuffers {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.reservation.release(self.bytes);
        }
    }
}

// ---------------------------------------------------------------------------
// AggregateOp — the operator
// ---------------------------------------------------------------------------

/// Implements PromQL's aggregation operators (`sum by (…)`, `topk`,
/// `quantile`, ...). One operator type, with the specific reduction
/// selected by an [`AggregateKind`] enum.
///
/// See module docs for supported kinds, group-map semantics, validity
/// rules, and memory accounting.
pub struct AggregateOp<C: Operator> {
    child: C,
    param_child: Option<Box<dyn Operator + Send>>,
    kind: AggregateKind,
    group_map: GroupMap,
    output_schema: Arc<SeriesSchema>,
    reservation: MemoryReservation,
    schema: OperatorSchema,
    /// Optional per-step `k` values for dynamic `topk` / `bottomk`
    /// parameters. Present only when lowering supplied a scalar child.
    param_values: Option<Vec<i64>>,
    /// Bytes reserved for `param_values`; released on `Drop`.
    param_bytes: usize,
    /// `true` once the optional scalar parameter child has been fully
    /// drained into [`Self::param_values`].
    param_loaded: bool,
    /// Per-step-per-group accumulator grid used by the streaming kinds.
    ///
    /// Length = `step_count × group_count`, indexed row-major:
    /// `accums[global_step * group_count + group]`. Allocated once at
    /// construction and absorbed-into across every input batch, regardless
    /// of how the child tiles its emission (step tiles × series tiles).
    /// Empty for breaker kinds.
    ///
    /// Streaming aggregate is a step-bounded breaker — it must see every
    /// series tile for a given step before it can emit, because
    /// aggregations are associative across input rows but not decomposable
    /// into "partial tile sums" without a downstream merge. Buffering the
    /// whole grid keeps the operator correct under arbitrary batch
    /// ordering (step/series tiles interleaved, or split by
    /// `Concurrent`/`Coalesce`).
    accums: Vec<Accumulator>,
    /// Step timestamps captured from the first child batch. Reused for the
    /// single output batch this operator emits on EOS. Streaming kinds
    /// only; breaker kinds echo the child batch's timestamps directly.
    streaming_step_timestamps: Option<Arc<[i64]>>,
    /// `true` once the streaming path has emitted its single output batch.
    /// Streaming kinds only.
    streaming_emitted: bool,
    /// Per-group min-heap scratch for `Topk`/`Bottomk`; one heap per
    /// group, drained between steps. Empty for other kinds.
    heaps: Vec<BinaryHeap<KHeapEntry>>,
    /// Per-group sort buffer for `Quantile`; one Vec per group, drained
    /// between steps. Empty for other kinds.
    sort_bufs: Vec<Vec<f64>>,
    /// Bytes reserved for the per-kind scratch (accums | heaps |
    /// sort_bufs); released on `Drop`.
    scratch_bytes: usize,
    /// Full input grid `step_count × input_series_count` used by the
    /// breaker kinds to absorb every child batch before running the
    /// selection / quantile per step. `values` is row-major
    /// (`values[step * input_series + series]`); `validity` runs
    /// parallel. Allocated alongside the per-kind heap/sort buffers so
    /// breakers remain correct under arbitrary input tiling. Empty for
    /// streaming kinds.
    breaker_values: Vec<f64>,
    breaker_validity: BitSet,
    /// Bytes reserved for [`Self::breaker_values`] / `breaker_validity`;
    /// released on `Drop`. Tracked separately from [`Self::scratch_bytes`]
    /// so construction can fail-fast on an undersize reservation.
    breaker_grid_bytes: usize,
    /// Step timestamps captured from the first child batch observed on
    /// the breaker path. Reused for the output batches emitted on EOS.
    /// Breaker kinds only.
    breaker_step_timestamps: Option<Arc<[i64]>>,
    /// `true` once the breaker path has emitted its final output batch.
    breaker_emitted: bool,
    /// `true` once the streaming path has absorbed at least one child
    /// batch. Tracked on `self` (not as a `next()`-local) so the flag
    /// survives `Poll::Pending` re-entries — under a `Concurrent`
    /// producer, `next()` is entered once per child batch, so a local
    /// flag would reset between absorb and EOS and skip `finalise`.
    streaming_saw_any_batch: bool,
    /// Analogous to [`Self::streaming_saw_any_batch`] for breaker kinds.
    breaker_saw_any_batch: bool,
    done: bool,
    errored: bool,
}

impl<C: Operator> AggregateOp<C> {
    #[inline]
    fn debug_assert_batch_within_input_roster(&self, input: &StepBatch) {
        debug_assert!(
            input.series_range.end <= self.group_map.input_series_count(),
            "child series_range {:?} exceeds group map input count {}",
            input.series_range,
            self.group_map.input_series_count(),
        );
    }

    /// Construct a streaming aggregate.
    ///
    /// * `child` — upstream operator.
    /// * `kind` — plan-time-selected reducer.
    /// * `group_map` — planner-built input-series → output-group
    ///   mapping; `group_map.input_series_count()` must match the
    ///   child's series count.
    /// * `output_schema` — planner-built output series roster; must
    ///   have `group_map.group_count` entries.
    /// * `reservation` — per-query reservation; charged for the per-
    ///   group scratch and every output batch.
    pub fn new(
        child: C,
        kind: AggregateKind,
        group_map: GroupMap,
        output_schema: Arc<SeriesSchema>,
        reservation: MemoryReservation,
    ) -> Result<Self, QueryError> {
        Self::new_with_param(child, None, kind, group_map, output_schema, reservation)
    }

    pub fn new_with_param(
        child: C,
        param_child: Option<Box<dyn Operator + Send>>,
        kind: AggregateKind,
        group_map: GroupMap,
        output_schema: Arc<SeriesSchema>,
        reservation: MemoryReservation,
    ) -> Result<Self, QueryError> {
        // Output schema shape depends on the variant. Planner is the
        // authority (see module docs §"Breakers / output schema
        // asymmetry"); these debug asserts catch planner bugs in tests.
        if kind.output_is_inputs() {
            debug_assert_eq!(
                group_map.input_series_count(),
                output_schema.len(),
                "topk/bottomk output schema must equal input series count",
            );
        } else {
            debug_assert_eq!(
                group_map.group_count,
                output_schema.len(),
                "streaming / quantile output schema must equal group_count",
            );
        }

        let grid = child.schema().step_grid;
        let schema = OperatorSchema::new(SchemaRef::Static(output_schema.clone()), grid);
        debug_assert!(
            param_child.is_none()
                || matches!(kind, AggregateKind::Topk(_) | AggregateKind::Bottomk(_)),
            "dynamic aggregate params are only supported for topk/bottomk",
        );
        if let Some(param_child) = &param_child {
            debug_assert_eq!(
                param_child.schema().step_grid,
                grid,
                "scalar aggregate param must share the main child step grid",
            );
        }

        let (param_values, param_bytes) = if param_child.is_some() {
            let bytes = grid.step_count.saturating_mul(std::mem::size_of::<i64>());
            reservation.try_grow(bytes)?;
            (Some(vec![0; grid.step_count]), bytes)
        } else {
            (None, 0)
        };

        // Breaker kinds need a full-grid (step × input_series) buffer
        // so `topk` / `bottomk` / `quantile` select globally against the
        // full input at each step rather than per-tile. Allocated
        // alongside the per-kind scratch so construction fails fast on
        // undersize reservations.
        let (breaker_values, breaker_validity, breaker_grid_bytes) = if kind.is_breaker() {
            let cells = grid
                .step_count
                .saturating_mul(group_map.input_series_count());
            let bytes = breaker_grid_bytes(cells);
            reservation.try_grow(bytes)?;
            (vec![f64::NAN; cells], BitSet::with_len(cells), bytes)
        } else {
            (Vec::new(), BitSet::with_len(0), 0)
        };

        // Per-kind scratch allocation. Reserve up front for the hot
        // path; release on `Drop`. Breakers skip the accumulator column
        // entirely (cheaper than paying for unused lanes).
        let (accums, heaps, sort_bufs, bytes) = match kind {
            AggregateKind::Topk(k) | AggregateKind::Bottomk(k) => {
                let heap_k = if param_child.is_some() {
                    group_map.input_series_count() as i64
                } else {
                    k
                };
                let bytes = heap_scratch_bytes(
                    heap_k,
                    group_map.group_count,
                    group_map.input_series_count(),
                );
                reservation.try_grow(bytes)?;
                // One heap per group — capacity is clamped to effective
                // K (≤ input_series_count) to keep the allocation tight.
                let cap = if heap_k <= 0 {
                    0
                } else {
                    (heap_k as usize).min(group_map.input_series_count())
                };
                let heaps = (0..group_map.group_count)
                    .map(|_| BinaryHeap::with_capacity(cap))
                    .collect();
                (Vec::new(), heaps, Vec::new(), bytes)
            }
            AggregateKind::Quantile(_) => {
                let bytes =
                    sort_scratch_bytes(group_map.group_count, group_map.input_series_count());
                reservation.try_grow(bytes)?;
                let sort_bufs: Vec<Vec<f64>> =
                    (0..group_map.group_count).map(|_| Vec::new()).collect();
                (Vec::new(), Vec::new(), sort_bufs, bytes)
            }
            _ => {
                // Streaming kinds buffer a (step × group) accumulator grid
                // so they remain correct under arbitrary input tiling
                // (series tiles × step tiles).
                let bytes = accum_bytes(grid.step_count, group_map.group_count);
                reservation.try_grow(bytes)?;
                let cells = grid.step_count.saturating_mul(group_map.group_count);
                let accums = vec![Accumulator::new(); cells];
                (accums, Vec::new(), Vec::new(), bytes)
            }
        };

        Ok(Self {
            child,
            param_child,
            kind,
            group_map,
            output_schema,
            reservation,
            schema,
            param_values,
            param_bytes,
            param_loaded: false,
            accums,
            streaming_step_timestamps: None,
            streaming_emitted: false,
            heaps,
            sort_bufs,
            scratch_bytes: bytes,
            breaker_values,
            breaker_validity,
            breaker_grid_bytes,
            breaker_step_timestamps: None,
            breaker_emitted: false,
            streaming_saw_any_batch: false,
            breaker_saw_any_batch: false,
            done: false,
            errored: false,
        })
    }

    fn load_param_values(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), QueryError>> {
        if self.param_loaded {
            return Poll::Ready(Ok(()));
        }
        let Some(param_child) = self.param_child.as_mut() else {
            self.param_loaded = true;
            return Poll::Ready(Ok(()));
        };

        loop {
            match param_child.next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => {
                    self.param_child = None;
                    self.param_loaded = true;
                    return Poll::Ready(Ok(()));
                }
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Err(err)),
                Poll::Ready(Some(Ok(batch))) => {
                    debug_assert_eq!(
                        batch.series_count(),
                        1,
                        "aggregate scalar param must produce exactly one series",
                    );
                    for step_off in 0..batch.step_count() {
                        let global_step = batch.step_range.start + step_off;
                        let cell = batch.cell_index(step_off, 0);
                        let value = if batch.validity.get(cell) {
                            batch.values[cell] as i64
                        } else {
                            0
                        };
                        if let Some(param_values) = self.param_values.as_mut() {
                            param_values[global_step] = value;
                        }
                    }
                }
            }
        }
    }

    #[inline]
    fn k_for_step(&self, step_idx: usize, input_len: usize, static_k: i64) -> usize {
        let k_param = self
            .param_values
            .as_ref()
            .map(|values| values[step_idx])
            .unwrap_or(static_k);
        coerce_k_size(k_param, input_len)
    }

    /// Absorb one child batch into the streaming-kind per-(step, group)
    /// accumulator grid. Idempotent across arbitrary batch ordering:
    /// step tiles × series tiles × `Concurrent`/`Coalesce` interleaving
    /// all funnel into the same buffered grid, and the final aggregate is
    /// associative on insertion order (modulo floating-point rounding,
    /// which the Kahan lane caps).
    fn absorb_batch_streaming(&mut self, input: &StepBatch) {
        debug_assert!(!self.kind.is_breaker());
        self.debug_assert_batch_within_input_roster(input);

        // Capture the step-timestamp `Arc<[i64]>` from the first non-empty
        // batch we see. Every child batch shares the same outer grid, so
        // any single batch's slice is authoritative for its own step span;
        // we only need one source of truth because the output batch covers
        // the entire grid (`0..step_count`). If the child never emits any
        // batches we synthesise a fresh timestamps array in
        // `finalise_streaming` from the plan-time grid.
        if self.streaming_step_timestamps.is_none()
            && input.step_range.start == 0
            && input.step_count() == self.schema.step_grid.step_count
        {
            self.streaming_step_timestamps = Some(input.step_timestamps.clone());
        }

        let step_count_in = input.step_count();
        let in_series_count = input.series_count();
        let group_count = self.group_map.group_count;

        // Row-major: for each step in the input batch, offset to the
        // global step index and absorb every valid cell into
        // `accums[global_step * group_count + group]`.
        for step_off in 0..step_count_in {
            let global_step = input.step_range.start + step_off;
            let step_base = step_off * in_series_count;
            let accum_base = global_step * group_count;
            for in_series in 0..in_series_count {
                let cell = step_base + in_series;
                if !input.validity.get(cell) {
                    continue;
                }
                let global_series = input.series_range.start + in_series;
                let group = match self.group_map.input_to_group[global_series] {
                    Some(g) => g as usize,
                    None => continue,
                };
                let v = input.values[cell];
                self.accums[accum_base + group].absorb(v);
            }
        }
    }

    /// Produce the single output batch covering the full outer grid for
    /// streaming kinds. Called once, on child-EOS, after every batch has
    /// been absorbed via [`Self::absorb_batch_streaming`].
    fn finalise_streaming(&mut self) -> Result<StepBatch, QueryError> {
        debug_assert!(!self.kind.is_breaker());
        let grid = self.schema.step_grid;
        let step_count = grid.step_count;
        let group_count = self.group_map.group_count;
        let out_cells = step_count.saturating_mul(group_count);

        let mut out = OutBuffers::allocate(&self.reservation, out_cells)?;

        for step in 0..step_count {
            let accum_base = step * group_count;
            let out_base = step * group_count;
            for g in 0..group_count {
                let accum = &self.accums[accum_base + g];
                if accum.count == 0 {
                    continue;
                }
                let value = match self.kind {
                    AggregateKind::Sum => accum.sum_value(),
                    AggregateKind::Avg => accum.avg_value(),
                    AggregateKind::Min => accum.min,
                    AggregateKind::Max => accum.max,
                    AggregateKind::Count => accum.count as f64,
                    AggregateKind::Stddev => accum.variance_value().sqrt(),
                    AggregateKind::Stdvar => accum.variance_value(),
                    AggregateKind::Group => 1.0,
                    // Unreachable: breakers are routed through
                    // `reduce_batch_breaker` and never visit this path.
                    AggregateKind::Topk(_)
                    | AggregateKind::Bottomk(_)
                    | AggregateKind::Quantile(_) => {
                        unreachable!("breaker kind routed to streaming finaliser")
                    }
                };
                let idx = out_base + g;
                out.values[idx] = value;
                out.validity.set(idx);
            }
        }

        let step_timestamps = self.streaming_step_timestamps.take().unwrap_or_else(|| {
            Arc::from(
                (0..step_count)
                    .map(|i| grid.start_ms + (i as i64) * grid.step_ms)
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            )
        });

        let (values, validity) = out.finish();
        Ok(StepBatch::new(
            step_timestamps,
            0..step_count,
            SchemaRef::Static(self.output_schema.clone()),
            0..group_count,
            values,
            validity,
        ))
    }

    /// Absorb one child batch into the breaker full-grid buffer.
    /// Row-major by step: `breaker_values[global_step * input_series +
    /// global_series]`. Idempotent across arbitrary batch ordering —
    /// step tiles × series tiles × Concurrent/Coalesce interleaving all
    /// funnel into the same grid.
    fn absorb_batch_breaker(&mut self, input: &StepBatch) {
        debug_assert!(self.kind.is_breaker());
        self.debug_assert_batch_within_input_roster(input);

        // Capture the step-timestamp `Arc<[i64]>` once (first non-empty
        // batch is authoritative; the outer grid is shared).
        if self.breaker_step_timestamps.is_none() {
            self.breaker_step_timestamps = Some(input.step_timestamps.clone());
        }

        let step_count_in = input.step_count();
        let in_series_count = input.series_count();
        let input_series_total = self.group_map.input_series_count();
        for step_off in 0..step_count_in {
            let global_step = input.step_range.start + step_off;
            let step_base = step_off * in_series_count;
            let grid_base = global_step * input_series_total;
            for in_series in 0..in_series_count {
                let cell = step_base + in_series;
                if !input.validity.get(cell) {
                    continue;
                }
                let global_series = input.series_range.start + in_series;
                if global_series >= input_series_total {
                    continue;
                }
                let grid_cell = grid_base + global_series;
                self.breaker_values[grid_cell] = input.values[cell];
                self.breaker_validity.set(grid_cell);
            }
        }
    }

    /// Finalise breaker output from the buffered full grid. Routes to
    /// the kind-specific `finalise_topk_or_bottomk` / `finalise_quantile`
    /// helpers, each of which runs its per-step selection / quantile
    /// against the complete `(step_count × input_series_count)` grid.
    fn finalise_breaker(&mut self) -> Result<StepBatch, QueryError> {
        match self.kind {
            AggregateKind::Topk(k) => self.finalise_topk_or_bottomk(k, KOrder::SmallestFirst),
            AggregateKind::Bottomk(k) => self.finalise_topk_or_bottomk(k, KOrder::LargestFirst),
            AggregateKind::Quantile(q) => self.finalise_quantile(q),
            _ => unreachable!("non-breaker kind routed to finalise_breaker"),
        }
    }

    /// Global-scope topk / bottomk: for each step, push every valid
    /// `(group, series, value)` triple into the per-group heap, then
    /// emit the survivors' input cells with validity=1. Output shape =
    /// `(step_count × input_series_count)` filter batch.
    fn finalise_topk_or_bottomk(&mut self, k: i64, order: KOrder) -> Result<StepBatch, QueryError> {
        let grid = self.schema.step_grid;
        let step_count = grid.step_count;
        let in_series_count = self.group_map.input_series_count();
        let out_cells = step_count * in_series_count;

        let mut out = OutBuffers::allocate(&self.reservation, out_cells)?;

        for global_step in 0..step_count {
            let k_usize = self.k_for_step(global_step, in_series_count, k);
            for heap in &mut self.heaps {
                heap.clear();
            }
            if k_usize == 0 {
                continue;
            }
            let grid_base = global_step * in_series_count;
            for global_series in 0..in_series_count {
                let cell = grid_base + global_series;
                if !self.breaker_validity.get(cell) {
                    continue;
                }
                let group = match self.group_map.input_to_group[global_series] {
                    Some(g) => g as usize,
                    None => continue,
                };
                let v = self.breaker_values[cell];
                let entry = KHeapEntry {
                    value: v,
                    global_series_idx: global_series as u32,
                    local_series_idx: global_series as u32,
                    order,
                };
                let heap = &mut self.heaps[group];
                if heap.len() < k_usize {
                    heap.push(entry);
                } else if let Some(worst) = heap.peek()
                    && entry.cmp(worst) == Ordering::Less
                {
                    heap.pop();
                    heap.push(entry);
                }
            }
            let out_base = global_step * in_series_count;
            for heap in &mut self.heaps {
                for entry in heap.drain() {
                    let idx = out_base + entry.local_series_idx as usize;
                    out.values[idx] = entry.value;
                    out.validity.set(idx);
                }
            }
        }

        let step_timestamps = self.breaker_step_timestamps.take().unwrap_or_else(|| {
            Arc::from(
                (0..step_count)
                    .map(|i| grid.start_ms + (i as i64) * grid.step_ms)
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            )
        });
        let (values, validity) = out.finish();
        Ok(StepBatch::new(
            step_timestamps,
            0..step_count,
            SchemaRef::Static(self.output_schema.clone()),
            0..in_series_count,
            values,
            validity,
        ))
    }

    /// Global-scope quantile: for each step, collect every valid
    /// `(group, value)` pair into the per-group sort buffer, then emit
    /// one reducer-shape cell per group with the q-th quantile.
    /// Output shape = `(step_count × group_count)`.
    fn finalise_quantile(&mut self, q: f64) -> Result<StepBatch, QueryError> {
        let grid = self.schema.step_grid;
        let step_count = grid.step_count;
        let in_series_count = self.group_map.input_series_count();
        let group_count = self.group_map.group_count;
        let out_cells = step_count * group_count;

        let mut out = OutBuffers::allocate(&self.reservation, out_cells)?;

        for global_step in 0..step_count {
            for buf in &mut self.sort_bufs {
                buf.clear();
            }
            let grid_base = global_step * in_series_count;
            for global_series in 0..in_series_count {
                let cell = grid_base + global_series;
                if !self.breaker_validity.get(cell) {
                    continue;
                }
                let group = match self.group_map.input_to_group[global_series] {
                    Some(g) => g as usize,
                    None => continue,
                };
                self.sort_bufs[group].push(self.breaker_values[cell]);
            }
            let out_base = global_step * group_count;
            for (g, buf) in self.sort_bufs.iter_mut().enumerate() {
                if buf.is_empty() {
                    continue;
                }
                let idx = out_base + g;
                out.values[idx] = quantile_linear_interp(q, buf);
                out.validity.set(idx);
            }
        }

        let step_timestamps = self.breaker_step_timestamps.take().unwrap_or_else(|| {
            Arc::from(
                (0..step_count)
                    .map(|i| grid.start_ms + (i as i64) * grid.step_ms)
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            )
        });
        let (values, validity) = out.finish();
        Ok(StepBatch::new(
            step_timestamps,
            0..step_count,
            SchemaRef::Static(self.output_schema.clone()),
            0..group_count,
            values,
            validity,
        ))
    }

    /// Buffered per-step reduction for `Topk` / `Bottomk` / `Quantile`.
    ///
    /// Memory: the per-step scratch (heap or sort buffer) was reserved
    /// up front in `new`. The per-batch output column is charged here
    /// via `OutBuffers`.
    #[allow(dead_code)]
    fn reduce_batch_breaker(&mut self, input: &StepBatch) -> Result<StepBatch, QueryError> {
        let step_count = input.step_count();
        self.debug_assert_batch_within_input_roster(input);

        match self.kind {
            AggregateKind::Topk(k) => self.reduce_topk_or_bottomk(input, k, KOrder::SmallestFirst),
            AggregateKind::Bottomk(k) => {
                self.reduce_topk_or_bottomk(input, k, KOrder::LargestFirst)
            }
            AggregateKind::Quantile(q) => self.reduce_quantile(input, q),
            _ => {
                debug_assert!(false, "non-breaker kind routed to breaker reducer");
                // Unreachable: the top-level `next()` dispatch branches
                // on `kind.is_breaker()` and the streaming kinds never
                // reach this method. If a new variant is added without
                // updating the dispatch, surface a planner bug loudly
                // in release builds rather than silently misrouting.
                let _ = step_count;
                unreachable!("non-breaker kind routed to breaker reducer")
            }
        }
    }

    /// `topk(k)` / `bottomk(k)` per-step selection.
    ///
    /// Output schema is the **input series** — cells not in the top-K
    /// (bottom-K) of their group get `validity = 0`. Selected cells
    /// carry the input value through unchanged.
    fn reduce_topk_or_bottomk(
        &mut self,
        input: &StepBatch,
        k: i64,
        order: KOrder,
    ) -> Result<StepBatch, QueryError> {
        let step_count = input.step_count();
        let in_series_count = input.series_count();
        // Output column width equals input column width (filter-shape).
        let out_cells = step_count * in_series_count;

        let mut out = OutBuffers::allocate(&self.reservation, out_cells)?;

        for step_off in 0..step_count {
            let k_usize = self.k_for_step(input.step_range.start + step_off, in_series_count, k);
            // Reset heaps for this step. `clear()` retains capacity.
            for heap in &mut self.heaps {
                heap.clear();
            }
            if k_usize == 0 {
                continue;
            }

            let step_base = step_off * in_series_count;
            for in_series in 0..in_series_count {
                let cell = step_base + in_series;
                if !input.validity.get(cell) {
                    continue;
                }
                let global_series = input.series_range.start + in_series;
                let group = match self.group_map.input_to_group[global_series] {
                    Some(g) => g as usize,
                    None => continue,
                };
                let v = input.values[cell];
                let entry = KHeapEntry {
                    value: v,
                    global_series_idx: global_series as u32,
                    local_series_idx: in_series as u32,
                    order,
                };
                let heap = &mut self.heaps[group];
                if heap.len() < k_usize {
                    heap.push(entry);
                } else if let Some(worst) = heap.peek() {
                    // Only displace when the candidate is strictly
                    // better than the current worst (stable-ish —
                    // equal values keep first-seen preference via the
                    // index tie-break already baked into `KHeapEntry`).
                    if entry.cmp(worst) == Ordering::Less {
                        heap.pop();
                        heap.push(entry);
                    }
                }
            }

            // Emit the survivors' cells with validity=1; all other
            // cells for this step stay at validity=0.
            let out_base = step_off * in_series_count;
            for heap in &mut self.heaps {
                for entry in heap.drain() {
                    let idx = out_base + entry.local_series_idx as usize;
                    out.values[idx] = entry.value;
                    out.validity.set(idx);
                }
            }
        }

        let (values, validity) = out.finish();
        Ok(StepBatch::new(
            input.step_timestamps.clone(),
            input.step_range.clone(),
            SchemaRef::Static(self.output_schema.clone()),
            input.series_range.clone(),
            values,
            validity,
        ))
    }

    /// `quantile(q)` per-step reduction — one cell per group per step,
    /// linear interpolation between ranks.
    fn reduce_quantile(&mut self, input: &StepBatch, q: f64) -> Result<StepBatch, QueryError> {
        let step_count = input.step_count();
        let in_series_count = input.series_count();
        let group_count = self.group_map.group_count;
        let out_cells = step_count * group_count;

        let mut out = OutBuffers::allocate(&self.reservation, out_cells)?;

        for step_off in 0..step_count {
            for buf in &mut self.sort_bufs {
                buf.clear();
            }

            let step_base = step_off * in_series_count;
            for in_series in 0..in_series_count {
                let cell = step_base + in_series;
                if !input.validity.get(cell) {
                    continue;
                }
                let global_series = input.series_range.start + in_series;
                let group = match self.group_map.input_to_group[global_series] {
                    Some(g) => g as usize,
                    None => continue,
                };
                self.sort_bufs[group].push(input.values[cell]);
            }

            let out_base = step_off * group_count;
            for (g, buf) in self.sort_bufs.iter_mut().enumerate() {
                if buf.is_empty() {
                    continue;
                }
                let idx = out_base + g;
                out.values[idx] = quantile_linear_interp(q, buf);
                out.validity.set(idx);
            }
        }

        let (values, validity) = out.finish();
        Ok(StepBatch::new(
            input.step_timestamps.clone(),
            input.step_range.clone(),
            SchemaRef::Static(self.output_schema.clone()),
            0..group_count,
            values,
            validity,
        ))
    }
}

/// Linear-interpolation quantile matching
/// `super::rollup::rollup_fns::quantile` (Prometheus
/// `quantile_over_time`).
///
/// Sort is in-place on the caller's buffer to avoid a second
/// allocation. Out-of-range `q` matches the rollup citation:
/// `q < 0 ⇒ -inf`, `q > 1 ⇒ +inf`, `NaN ⇒ NaN`.
fn quantile_linear_interp(q: f64, buf: &mut [f64]) -> f64 {
    if buf.is_empty() {
        return f64::NAN;
    }
    if q.is_nan() {
        return f64::NAN;
    }
    if q < 0.0 {
        return f64::NEG_INFINITY;
    }
    if q > 1.0 {
        return f64::INFINITY;
    }
    buf.sort_by(|a, b| a.total_cmp(b));
    let n = buf.len();
    if n == 1 {
        return buf[0];
    }
    let rank = q * (n - 1) as f64;
    let lo = rank.floor() as usize;
    let hi = rank.ceil() as usize;
    if lo == hi {
        return buf[lo];
    }
    let weight = rank - lo as f64;
    buf[lo] * (1.0 - weight) + buf[hi] * weight
}

impl<C: Operator> Operator for AggregateOp<C> {
    fn schema(&self) -> &OperatorSchema {
        &self.schema
    }

    fn next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<StepBatch, QueryError>>> {
        if self.done || self.errored {
            return Poll::Ready(None);
        }
        match self.load_param_values(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(err)) => {
                self.errored = true;
                return Poll::Ready(Some(Err(err)));
            }
            Poll::Ready(Ok(())) => {}
        }

        if self.kind.is_breaker() {
            // Breaker kinds: drain every child batch into the full
            // `(step × input_series)` grid, then on EOS run the per-step
            // selection / quantile globally. Correct under arbitrary
            // child tile ordering (step tiles × series tiles, Concurrent
            // / Coalesce reordering).
            if self.breaker_emitted {
                self.done = true;
                return Poll::Ready(None);
            }
            loop {
                match self.child.next(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(None) => {
                        self.breaker_emitted = true;
                        if !self.breaker_saw_any_batch {
                            self.done = true;
                            return Poll::Ready(None);
                        }
                        return match self.finalise_breaker() {
                            Ok(batch) => Poll::Ready(Some(Ok(batch))),
                            Err(err) => {
                                self.errored = true;
                                Poll::Ready(Some(Err(err)))
                            }
                        };
                    }
                    Poll::Ready(Some(Err(err))) => {
                        self.errored = true;
                        return Poll::Ready(Some(Err(err)));
                    }
                    Poll::Ready(Some(Ok(input))) => {
                        self.breaker_saw_any_batch = true;
                        self.absorb_batch_breaker(&input);
                    }
                }
            }
        } else {
            // Streaming kinds: drain every child batch into the
            // per-(step, group) accumulator grid, then emit a single
            // output batch covering the full grid on EOS. This is
            // correct under arbitrary tile ordering (the child may
            // interleave step tiles × series tiles).
            if self.streaming_emitted {
                self.done = true;
                return Poll::Ready(None);
            }
            loop {
                match self.child.next(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(None) => {
                        self.streaming_emitted = true;
                        // If the child produced zero batches, there is
                        // nothing to emit — matches Prometheus behaviour
                        // when a selector has no series and preserves
                        // parity with the v1 engine (no empty grid
                        // trailing after an empty selector).
                        if !self.streaming_saw_any_batch {
                            self.done = true;
                            return Poll::Ready(None);
                        }
                        return match self.finalise_streaming() {
                            Ok(batch) => Poll::Ready(Some(Ok(batch))),
                            Err(err) => {
                                self.errored = true;
                                Poll::Ready(Some(Err(err)))
                            }
                        };
                    }
                    Poll::Ready(Some(Err(err))) => {
                        self.errored = true;
                        return Poll::Ready(Some(Err(err)));
                    }
                    Poll::Ready(Some(Ok(input))) => {
                        self.streaming_saw_any_batch = true;
                        self.absorb_batch_streaming(&input);
                    }
                }
            }
        }
    }
}

impl<C: Operator> Drop for AggregateOp<C> {
    fn drop(&mut self) {
        if self.param_bytes > 0 {
            self.reservation.release(self.param_bytes);
            self.param_bytes = 0;
        }
        if self.scratch_bytes > 0 {
            self.reservation.release(self.scratch_bytes);
            self.scratch_bytes = 0;
        }
        if self.breaker_grid_bytes > 0 {
            self.reservation.release(self.breaker_grid_bytes);
            self.breaker_grid_bytes = 0;
        }
    }
}

#[inline]
fn coerce_k_size(k_param: i64, input_len: usize) -> usize {
    let max_k = input_len as i64;
    let coerced = k_param.min(max_k);
    if coerced < 1 { 0 } else { coerced as usize }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Label, Labels};
    use crate::promql::batch::{SchemaRef, SeriesSchema};
    use crate::promql::operator::StepGrid;
    use std::sync::Arc;
    use std::task::{RawWaker, RawWakerVTable, Waker};

    // ---- waker + mock child -------------------------------------------------

    fn noop_waker() -> Waker {
        const VTABLE: RawWakerVTable = RawWakerVTable::new(
            |_| RawWaker::new(std::ptr::null(), &VTABLE),
            |_| {},
            |_| {},
            |_| {},
        );
        unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
    }

    struct MockOp {
        schema: OperatorSchema,
        queue: Vec<Result<StepBatch, QueryError>>,
    }

    impl MockOp {
        fn with_queue(
            schema: Arc<SeriesSchema>,
            grid: StepGrid,
            queue: Vec<Result<StepBatch, QueryError>>,
        ) -> Self {
            Self {
                schema: OperatorSchema::new(SchemaRef::Static(schema), grid),
                queue,
            }
        }

        fn new(schema: Arc<SeriesSchema>, grid: StepGrid, batches: Vec<StepBatch>) -> Self {
            Self::with_queue(schema, grid, batches.into_iter().map(Ok).collect())
        }
    }

    impl Operator for MockOp {
        fn schema(&self) -> &OperatorSchema {
            &self.schema
        }
        fn next(&mut self, _cx: &mut Context<'_>) -> Poll<Option<Result<StepBatch, QueryError>>> {
            if self.queue.is_empty() {
                Poll::Ready(None)
            } else {
                Poll::Ready(Some(self.queue.remove(0)))
            }
        }
    }

    // ---- fixtures -----------------------------------------------------------

    fn mk_schema(prefix: &str, n: usize) -> Arc<SeriesSchema> {
        let labels: Vec<Labels> = (0..n)
            .map(|i| {
                Labels::new(vec![
                    Label {
                        name: "__name__".to_string(),
                        value: prefix.to_string(),
                    },
                    Label {
                        name: "i".to_string(),
                        value: i.to_string(),
                    },
                ])
            })
            .collect();
        let fps: Vec<u128> = (0..n as u128).collect();
        Arc::new(SeriesSchema::new(Arc::from(labels), Arc::from(fps)))
    }

    fn mk_grid(step_count: usize) -> StepGrid {
        StepGrid {
            start_ms: 0,
            end_ms: 10 * (step_count.max(1) as i64 - 1),
            step_ms: 10,
            step_count,
        }
    }

    fn mk_batch(
        schema: Arc<SeriesSchema>,
        step_count: usize,
        series_count: usize,
        values: Vec<f64>,
        validity: Vec<bool>,
    ) -> StepBatch {
        mk_batch_with_series_range(schema, step_count, 0..series_count, values, validity)
    }

    fn mk_batch_with_series_range(
        schema: Arc<SeriesSchema>,
        step_count: usize,
        series_range: std::ops::Range<usize>,
        values: Vec<f64>,
        validity: Vec<bool>,
    ) -> StepBatch {
        let series_count = series_range.len();
        assert_eq!(values.len(), step_count * series_count);
        assert_eq!(validity.len(), step_count * series_count);
        let ts: Arc<[i64]> =
            Arc::from((0..step_count).map(|i| (i as i64) * 10).collect::<Vec<_>>());
        let mut bits = BitSet::with_len(step_count * series_count);
        for (i, &b) in validity.iter().enumerate() {
            if b {
                bits.set(i);
            }
        }
        StepBatch::new(
            ts,
            0..step_count,
            SchemaRef::Static(schema),
            series_range,
            values,
            bits,
        )
    }

    fn drive<C: Operator>(op: &mut AggregateOp<C>) -> Vec<Result<StepBatch, QueryError>> {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut out = Vec::new();
        loop {
            match op.next(&mut cx) {
                Poll::Ready(None) => return out,
                Poll::Ready(Some(r)) => out.push(r),
                Poll::Pending => panic!("unexpected Pending from sync mock"),
            }
        }
    }

    fn approx_eq(a: f64, b: f64, eps: f64) -> bool {
        if a.is_nan() && b.is_nan() {
            return true;
        }
        if a.is_infinite() || b.is_infinite() {
            return a == b;
        }
        (a - b).abs() <= eps * (1.0 + a.abs().max(b.abs()))
    }

    // ========================================================================
    // required tests
    // ========================================================================

    #[test]
    fn should_sum_across_grouped_series_per_step() {
        // given: 4 input series, 2 steps, grouped 2+2 into 2 output groups.
        //   step 0: values [1,2,3,4]  group 0 = s0+s1 = 3; group 1 = s2+s3 = 7
        //   step 1: values [10,20,30,40] group 0 = 30; group 1 = 70
        let in_schema = mk_schema("in", 4);
        let out_schema = mk_schema("out", 2);
        let grid = mk_grid(2);
        let values = vec![1.0, 2.0, 3.0, 4.0, 10.0, 20.0, 30.0, 40.0];
        let valid = vec![true; 8];
        let batch = mk_batch(in_schema.clone(), 2, 4, values, valid);

        let child = MockOp::new(in_schema, grid, vec![batch]);
        let gmap = GroupMap::new(vec![Some(0), Some(0), Some(1), Some(1)], 2);

        // when
        let mut op = AggregateOp::new(
            child,
            AggregateKind::Sum,
            gmap,
            out_schema,
            MemoryReservation::new(1 << 20),
        )
        .expect("operator constructs");
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        // then
        assert_eq!(outs.len(), 1);
        let b = &outs[0];
        assert_eq!(b.step_count(), 2);
        assert_eq!(b.series_count(), 2);
        assert_eq!(b.get(0, 0), Some(3.0));
        assert_eq!(b.get(0, 1), Some(7.0));
        assert_eq!(b.get(1, 0), Some(30.0));
        assert_eq!(b.get(1, 1), Some(70.0));
    }

    #[test]
    fn should_compute_avg_ignoring_invalid_cells() {
        // given: 3 input series in one group. Cell validity pattern:
        //   step 0: [v,v,_]  values 2,4,99 → mean = (2+4)/2 = 3
        //   step 1: [_,_,_]  validity all 0 → output absent
        //   step 2: [v,v,v]  values 5,10,15 → mean = 10
        let in_schema = mk_schema("in", 3);
        let out_schema = mk_schema("out", 1);
        let grid = mk_grid(3);
        let values = vec![2.0, 4.0, 99.0, 0.0, 0.0, 0.0, 5.0, 10.0, 15.0];
        let valid = vec![true, true, false, false, false, false, true, true, true];
        let batch = mk_batch(in_schema.clone(), 3, 3, values, valid);

        let child = MockOp::new(in_schema, grid, vec![batch]);
        let gmap = GroupMap::new(vec![Some(0), Some(0), Some(0)], 1);

        // when
        let mut op = AggregateOp::new(
            child,
            AggregateKind::Avg,
            gmap,
            out_schema,
            MemoryReservation::new(1 << 20),
        )
        .unwrap();
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        // then
        let b = &outs[0];
        assert!(approx_eq(b.get(0, 0).unwrap(), 3.0, 1e-12));
        assert_eq!(b.get(1, 0), None);
        assert!(approx_eq(b.get(2, 0).unwrap(), 10.0, 1e-12));
    }

    #[test]
    fn should_compute_min_and_max_ignoring_nan() {
        // given: 4 series, one group.
        //   step 0: NaN, 5, 3, NaN → min=3, max=5
        //   step 1: NaN, NaN, NaN, NaN → all NaN. The group still has
        //       "count>0 valid contributions" because validity bits are
        //       set; output min/max = NaN (Prometheus: preserves NaN when
        //       nothing else is seen).
        let in_schema = mk_schema("in", 4);
        let out_schema = mk_schema("out", 1);
        let grid = mk_grid(2);
        let values = vec![
            f64::NAN,
            5.0,
            3.0,
            f64::NAN,
            f64::NAN,
            f64::NAN,
            f64::NAN,
            f64::NAN,
        ];
        let valid = vec![true; 8];
        let batch = mk_batch(in_schema.clone(), 2, 4, values, valid);

        let child = MockOp::new(in_schema, grid, vec![batch]);
        let gmap = GroupMap::new(vec![Some(0); 4], 1);

        let mut op_min = AggregateOp::new(
            child,
            AggregateKind::Min,
            gmap.clone(),
            out_schema.clone(),
            MemoryReservation::new(1 << 20),
        )
        .unwrap();
        let outs: Vec<StepBatch> = drive(&mut op_min).into_iter().map(|r| r.unwrap()).collect();
        let b = &outs[0];
        assert_eq!(b.get(0, 0), Some(3.0));
        // step 1 had all NaN contributions — output is NaN with validity=1
        assert!(b.validity.get(b.cell_index(1, 0)));
        assert!(b.values[b.cell_index(1, 0)].is_nan());

        // max
        let in_schema = mk_schema("in", 4);
        let grid2 = mk_grid(2);
        let values = vec![
            f64::NAN,
            5.0,
            3.0,
            f64::NAN,
            f64::NAN,
            f64::NAN,
            f64::NAN,
            f64::NAN,
        ];
        let valid = vec![true; 8];
        let batch2 = mk_batch(in_schema.clone(), 2, 4, values, valid);
        let child2 = MockOp::new(in_schema, grid2, vec![batch2]);
        let mut op_max = AggregateOp::new(
            child2,
            AggregateKind::Max,
            gmap,
            out_schema,
            MemoryReservation::new(1 << 20),
        )
        .unwrap();
        let outs: Vec<StepBatch> = drive(&mut op_max).into_iter().map(|r| r.unwrap()).collect();
        assert_eq!(outs[0].get(0, 0), Some(5.0));
    }

    #[test]
    fn should_count_valid_cells_per_group() {
        // given: 3 series, 2 groups (s0,s1 → g0; s2 → g1), 2 steps
        //   step 0: [v,v,v] → counts g0=2, g1=1
        //   step 1: [_,v,_] → counts g0=1, g1 absent (validity=0)
        let in_schema = mk_schema("in", 3);
        let out_schema = mk_schema("out", 2);
        let grid = mk_grid(2);
        let values = vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0];
        let valid = vec![true, true, true, false, true, false];
        let batch = mk_batch(in_schema.clone(), 2, 3, values, valid);

        let child = MockOp::new(in_schema, grid, vec![batch]);
        let gmap = GroupMap::new(vec![Some(0), Some(0), Some(1)], 2);

        let mut op = AggregateOp::new(
            child,
            AggregateKind::Count,
            gmap,
            out_schema,
            MemoryReservation::new(1 << 20),
        )
        .unwrap();
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
        let b = &outs[0];
        assert_eq!(b.get(0, 0), Some(2.0));
        assert_eq!(b.get(0, 1), Some(1.0));
        assert_eq!(b.get(1, 0), Some(1.0));
        assert_eq!(b.get(1, 1), None);
    }

    #[test]
    fn should_compute_stddev_and_stdvar_with_welford() {
        // given: 8 series, one group, values 2,4,4,4,5,5,7,9
        //   population variance = 4, stddev = 2.
        let in_schema = mk_schema("in", 8);
        let out_schema = mk_schema("out", 1);
        let grid = mk_grid(1);
        let values = vec![2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
        let valid = vec![true; 8];
        let batch = mk_batch(in_schema.clone(), 1, 8, values, valid);

        let child = MockOp::new(in_schema, grid, vec![batch]);
        let gmap = GroupMap::new(vec![Some(0); 8], 1);

        let mut op_var = AggregateOp::new(
            child,
            AggregateKind::Stdvar,
            gmap.clone(),
            out_schema.clone(),
            MemoryReservation::new(1 << 20),
        )
        .unwrap();
        let outs: Vec<StepBatch> = drive(&mut op_var).into_iter().map(|r| r.unwrap()).collect();
        assert!(approx_eq(outs[0].get(0, 0).unwrap(), 4.0, 1e-12));

        // and: stddev
        let in_schema = mk_schema("in", 8);
        let grid2 = mk_grid(1);
        let values = vec![2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
        let valid = vec![true; 8];
        let batch = mk_batch(in_schema.clone(), 1, 8, values, valid);
        let child2 = MockOp::new(in_schema, grid2, vec![batch]);
        let mut op_stddev = AggregateOp::new(
            child2,
            AggregateKind::Stddev,
            gmap,
            out_schema,
            MemoryReservation::new(1 << 20),
        )
        .unwrap();
        let outs: Vec<StepBatch> = drive(&mut op_stddev)
            .into_iter()
            .map(|r| r.unwrap())
            .collect();
        assert!(approx_eq(outs[0].get(0, 0).unwrap(), 2.0, 1e-12));
    }

    #[test]
    fn should_emit_group_one_when_any_input_contributed() {
        // given: 3 series, 2 groups (s0→g0, s1,s2→g1). Step 0 valid only
        // in s0 and s2 → g0 has 1 contribution, g1 has 1 contribution,
        // both emit 1.0.
        let in_schema = mk_schema("in", 3);
        let out_schema = mk_schema("out", 2);
        let grid = mk_grid(1);
        let values = vec![42.0, 0.0, 7.0];
        let valid = vec![true, false, true];
        let batch = mk_batch(in_schema.clone(), 1, 3, values, valid);

        let child = MockOp::new(in_schema, grid, vec![batch]);
        let gmap = GroupMap::new(vec![Some(0), Some(1), Some(1)], 2);

        let mut op = AggregateOp::new(
            child,
            AggregateKind::Group,
            gmap,
            out_schema,
            MemoryReservation::new(1 << 20),
        )
        .unwrap();
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
        assert_eq!(outs[0].get(0, 0), Some(1.0));
        assert_eq!(outs[0].get(0, 1), Some(1.0));
    }

    #[test]
    fn should_set_validity_zero_when_group_empty() {
        // given: 2 series → 1 group; step has all inputs invalid.
        let out_schema = mk_schema("out", 1);

        for kind in [
            AggregateKind::Sum,
            AggregateKind::Avg,
            AggregateKind::Min,
            AggregateKind::Max,
            AggregateKind::Count,
            AggregateKind::Stddev,
            AggregateKind::Stdvar,
            AggregateKind::Group,
        ] {
            // Rebuild everything since operator + child are consumed.
            let in_schema = mk_schema("in", 2);
            let grid = mk_grid(1);
            let batch = mk_batch(
                in_schema.clone(),
                1,
                2,
                vec![99.0, 99.0],
                vec![false, false],
            );
            let child = MockOp::new(in_schema, grid, vec![batch]);
            let gmap = GroupMap::new(vec![Some(0), Some(0)], 1);

            let mut op = AggregateOp::new(
                child,
                kind,
                gmap,
                out_schema.clone(),
                MemoryReservation::new(1 << 20),
            )
            .unwrap();
            let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
            assert_eq!(outs[0].get(0, 0), None, "kind={kind:?}");
        }
    }

    #[test]
    fn should_handle_by_empty_single_group() {
        // given: 5 series → 1 group (all map to 0). `sum by ()` semantics.
        let in_schema = mk_schema("in", 5);
        let out_schema = mk_schema("out", 1);
        let grid = mk_grid(1);
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let valid = vec![true; 5];
        let batch = mk_batch(in_schema.clone(), 1, 5, values, valid);

        let child = MockOp::new(in_schema, grid, vec![batch]);
        let gmap = GroupMap::new(vec![Some(0); 5], 1);

        let mut op = AggregateOp::new(
            child,
            AggregateKind::Sum,
            gmap,
            out_schema,
            MemoryReservation::new(1 << 20),
        )
        .unwrap();
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
        assert_eq!(outs[0].series_count(), 1);
        assert_eq!(outs[0].get(0, 0), Some(15.0));
    }

    #[test]
    fn should_handle_without_degenerate_each_series_its_own_group() {
        // given: `sum without ()` — every input series is its own group.
        let in_schema = mk_schema("in", 3);
        let out_schema = mk_schema("out", 3);
        let grid = mk_grid(1);
        let values = vec![7.0, 11.0, 13.0];
        let valid = vec![true; 3];
        let batch = mk_batch(in_schema.clone(), 1, 3, values, valid);

        let child = MockOp::new(in_schema, grid, vec![batch]);
        let gmap = GroupMap::new(vec![Some(0), Some(1), Some(2)], 3);

        let mut op = AggregateOp::new(
            child,
            AggregateKind::Sum,
            gmap,
            out_schema,
            MemoryReservation::new(1 << 20),
        )
        .unwrap();
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
        let b = &outs[0];
        assert_eq!(b.series_count(), 3);
        assert_eq!(b.get(0, 0), Some(7.0));
        assert_eq!(b.get(0, 1), Some(11.0));
        assert_eq!(b.get(0, 2), Some(13.0));
    }

    #[test]
    fn should_span_multiple_step_tile_batches() {
        // given: child emits two step-tile batches covering disjoint step
        // ranges of the same outer grid. Batch A = step 0 (values 1, 2),
        // batch B = step 1 (values 10, 20). Both tile into the same
        // output group.
        //
        // After the tile-boundary fix, streaming aggregate is a
        // step-bounded breaker — it buffers the whole grid and emits a
        // single output batch covering every step. Expected output: one
        // batch, step 0 sum = 3, step 1 sum = 30.
        let in_schema = mk_schema("in", 2);
        let out_schema = mk_schema("out", 1);
        let grid = mk_grid(2);
        let ts: Arc<[i64]> = Arc::from(vec![0i64, 10].into_boxed_slice());

        // Build the two tiles with disjoint step_range — the mk_batch
        // helper always uses `0..step_count`, so assemble the batch
        // directly to get `step_range = 1..2` for batch B.
        let mut bits_a = BitSet::with_len(2);
        bits_a.set(0);
        bits_a.set(1);
        let batch_a = StepBatch::new(
            ts.clone(),
            0..1,
            SchemaRef::Static(in_schema.clone()),
            0..2,
            vec![1.0, 2.0],
            bits_a,
        );
        let mut bits_b = BitSet::with_len(2);
        bits_b.set(0);
        bits_b.set(1);
        let batch_b = StepBatch::new(
            ts,
            1..2,
            SchemaRef::Static(in_schema.clone()),
            0..2,
            vec![10.0, 20.0],
            bits_b,
        );

        let child = MockOp::new(in_schema, grid, vec![batch_a, batch_b]);
        let gmap = GroupMap::new(vec![Some(0), Some(0)], 1);

        let mut op = AggregateOp::new(
            child,
            AggregateKind::Sum,
            gmap,
            out_schema,
            MemoryReservation::new(1 << 20),
        )
        .unwrap();
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
        assert_eq!(outs.len(), 1);
        let b = &outs[0];
        assert_eq!(b.step_count(), 2);
        assert_eq!(b.get(0, 0), Some(3.0));
        assert_eq!(b.get(1, 0), Some(30.0));
    }

    #[test]
    fn should_use_absolute_series_indices_for_streaming_batches() {
        // given: the child publishes only the tail slice of a 4-series roster.
        // The group map still indexes the full roster, so the operator must
        // offset by `input.series_range.start` before looking up groups.
        let in_schema = mk_schema("in", 4);
        let out_schema = mk_schema("out", 2);
        let grid = mk_grid(1);
        let batch = mk_batch_with_series_range(
            in_schema.clone(),
            1,
            2..4,
            vec![3.0, 5.0],
            vec![true, true],
        );
        let child = MockOp::new(in_schema, grid, vec![batch]);
        let gmap = GroupMap::new(vec![Some(0), Some(0), Some(1), Some(1)], 2);

        // when
        let mut op = AggregateOp::new(
            child,
            AggregateKind::Sum,
            gmap,
            out_schema,
            MemoryReservation::new(1 << 20),
        )
        .unwrap();
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        // then
        assert_eq!(outs[0].get(0, 0), None);
        assert_eq!(outs[0].get(0, 1), Some(8.0));
    }

    #[test]
    fn should_respect_memory_reservation() {
        // given: a tiny reservation that the accumulator alone doesn't
        // fit into.
        let in_schema = mk_schema("in", 4);
        let out_schema = mk_schema("out", 4);
        let grid = mk_grid(1);
        let batch = mk_batch(in_schema.clone(), 1, 4, vec![1.0; 4], vec![true; 4]);
        let child = MockOp::new(in_schema, grid, vec![batch]);
        let gmap = GroupMap::new(vec![Some(0), Some(1), Some(2), Some(3)], 4);

        // Cap too small even for the per-group scratch.
        let result = AggregateOp::new(
            child,
            AggregateKind::Sum,
            gmap,
            out_schema,
            MemoryReservation::new(1),
        );
        match result {
            Err(QueryError::MemoryLimit { .. }) => {}
            _ => panic!("expected MemoryLimit on constructor"),
        }
    }

    #[test]
    fn should_return_static_schema() {
        // given: a minimal aggregate.
        let in_schema = mk_schema("in", 2);
        let out_schema = mk_schema("out", 1);
        let grid = mk_grid(1);
        let batch = mk_batch(in_schema.clone(), 1, 2, vec![1.0, 2.0], vec![true, true]);
        let child = MockOp::new(in_schema, grid, vec![batch]);
        let gmap = GroupMap::new(vec![Some(0), Some(0)], 1);

        let op = AggregateOp::new(
            child,
            AggregateKind::Sum,
            gmap,
            out_schema,
            MemoryReservation::new(1 << 20),
        )
        .unwrap();

        // when/then
        assert!(!op.schema().series.is_deferred());
        assert!(op.schema().series.as_static().is_some());
        assert_eq!(
            op.schema().series.as_static().unwrap().len(),
            1,
            "output schema must equal group_count",
        );
    }

    #[test]
    fn should_propagate_error_from_upstream() {
        let in_schema = mk_schema("in", 1);
        let out_schema = mk_schema("out", 1);
        let grid = mk_grid(1);
        let child = MockOp::with_queue(
            in_schema,
            grid,
            vec![Err(QueryError::MemoryLimit {
                requested: 10,
                cap: 0,
                already_reserved: 0,
            })],
        );
        let gmap = GroupMap::new(vec![Some(0)], 1);
        let mut op = AggregateOp::new(
            child,
            AggregateKind::Sum,
            gmap,
            out_schema,
            MemoryReservation::new(1 << 20),
        )
        .unwrap();
        let results = drive(&mut op);
        assert_eq!(results.len(), 1);
        assert!(matches!(results[0], Err(QueryError::MemoryLimit { .. })));
    }

    #[test]
    fn should_handle_child_end_of_stream() {
        // given: child emits no batches.
        let in_schema = mk_schema("in", 1);
        let out_schema = mk_schema("out", 1);
        let grid = mk_grid(1);
        let child = MockOp::new(in_schema, grid, vec![]);
        let gmap = GroupMap::new(vec![Some(0)], 1);
        let mut op = AggregateOp::new(
            child,
            AggregateKind::Sum,
            gmap,
            out_schema,
            MemoryReservation::new(1 << 20),
        )
        .unwrap();
        let results = drive(&mut op);
        assert!(results.is_empty());
    }

    /// Scripted child action for the [`PendingMockOp`] reproducer: either
    /// yield a batch on this poll, return `Poll::Pending` once (mirroring a
    /// `Concurrent` exchange with an empty channel), or signal EOS.
    enum Action {
        Batch(StepBatch),
        Pending,
        Eos,
    }

    /// Mock child that interleaves `Poll::Pending` with batches and EOS.
    /// Each `Pending` response self-wakes via the context waker so the
    /// [`drive_with_pending`] driver re-polls immediately, emulating the
    /// pattern produced by `ConcurrentOp` under non-trivial schedules
    /// (see trace in RFC 0008 stress harness).
    struct PendingMockOp {
        schema: OperatorSchema,
        actions: std::collections::VecDeque<Action>,
    }

    impl PendingMockOp {
        fn new(schema: Arc<SeriesSchema>, grid: StepGrid, actions: Vec<Action>) -> Self {
            Self {
                schema: OperatorSchema::new(SchemaRef::Static(schema), grid),
                actions: actions.into(),
            }
        }
    }

    impl Operator for PendingMockOp {
        fn schema(&self) -> &OperatorSchema {
            &self.schema
        }
        fn next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<StepBatch, QueryError>>> {
            match self.actions.pop_front() {
                Some(Action::Batch(b)) => Poll::Ready(Some(Ok(b))),
                Some(Action::Pending) => {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                Some(Action::Eos) | None => Poll::Ready(None),
            }
        }
    }

    /// Drive an operator that may return `Pending`, polling again on each
    /// `Pending` until `Ready(None)`.
    fn drive_with_pending<C: Operator>(
        op: &mut AggregateOp<C>,
    ) -> Vec<Result<StepBatch, QueryError>> {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut out = Vec::new();
        loop {
            match op.next(&mut cx) {
                Poll::Ready(None) => return out,
                Poll::Ready(Some(r)) => out.push(r),
                Poll::Pending => continue,
            }
        }
    }

    #[test]
    fn should_finalise_streaming_aggregate_when_child_interleaves_pending() {
        // given: 2 input series grouped into 1 group over 1 step.
        //   Child emits: [Batch(s0=3), Pending, Batch(s1=4), Pending, Eos]
        //   Each Pending forces AggregateOp::next to return Pending and
        //   be re-entered — under the bug, the local `saw_any_batch`
        //   resets on every re-entry, so the final `Eos` poll is entered
        //   with `saw_any_batch=false` and `finalise_streaming` is
        //   skipped, yielding an empty stream even though the grid has
        //   accumulated 3+4=7.
        let in_schema = mk_schema("in", 2);
        let out_schema = mk_schema("out", 1);
        let grid = mk_grid(1);
        let b1 = mk_batch_with_series_range(in_schema.clone(), 1, 0..1, vec![3.0], vec![true]);
        let b2 = mk_batch_with_series_range(in_schema.clone(), 1, 1..2, vec![4.0], vec![true]);
        let child = PendingMockOp::new(
            in_schema,
            grid,
            vec![
                Action::Batch(b1),
                Action::Pending,
                Action::Batch(b2),
                Action::Pending,
                Action::Eos,
            ],
        );
        let gmap = GroupMap::new(vec![Some(0), Some(0)], 1);

        // when
        let mut op = AggregateOp::new(
            child,
            AggregateKind::Sum,
            gmap,
            out_schema,
            MemoryReservation::new(1 << 20),
        )
        .unwrap();
        let outs: Vec<StepBatch> = drive_with_pending(&mut op)
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        // then: one batch with the grouped sum s0+s1 = 7.
        assert_eq!(outs.len(), 1, "aggregate emitted empty stream");
        assert_eq!(outs[0].get(0, 0), Some(7.0));
    }

    #[test]
    fn should_finalise_breaker_aggregate_when_child_interleaves_pending() {
        // given: the same scripted Pending interleaving but for a
        // breaker-kind aggregate (topk). The bug at `saw_any_batch` in
        // the breaker branch mirrors the streaming path.
        let in_schema = mk_schema("in", 2);
        let grid = mk_grid(1);
        let b1 = mk_batch_with_series_range(in_schema.clone(), 1, 0..1, vec![3.0], vec![true]);
        let b2 = mk_batch_with_series_range(in_schema.clone(), 1, 1..2, vec![4.0], vec![true]);
        let child = PendingMockOp::new(
            in_schema.clone(),
            grid,
            vec![
                Action::Batch(b1),
                Action::Pending,
                Action::Batch(b2),
                Action::Pending,
                Action::Eos,
            ],
        );
        // one group containing both series — topk(1) picks the larger.
        let gmap = GroupMap::new(vec![Some(0), Some(0)], 1);

        // when
        let mut op = AggregateOp::new(
            child,
            AggregateKind::Topk(1),
            gmap,
            in_schema,
            MemoryReservation::new(1 << 20),
        )
        .unwrap();
        let outs: Vec<StepBatch> = drive_with_pending(&mut op)
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        // then: one batch with the top-1 series (s1 = 4.0).
        assert_eq!(outs.len(), 1, "breaker aggregate emitted empty stream");
        // topk(1) over [3.0, 4.0] → 4.0 survives at its original slot.
        let observed: Vec<Option<f64>> = (0..outs[0].series_count())
            .map(|s| outs[0].get(0, s))
            .collect();
        assert!(
            observed.contains(&Some(4.0)),
            "expected topk winner 4.0 in output, got {:?}",
            observed
        );
    }

    #[test]
    fn should_drop_inputs_with_none_group_assignment() {
        // given: 3 series, 2 groups; s1 has `None` assignment (dropped).
        //   step 0: [1, 99, 2] → g0 sums s0=1, g1 sums s2=2 (s1 dropped)
        let in_schema = mk_schema("in", 3);
        let out_schema = mk_schema("out", 2);
        let grid = mk_grid(1);
        let values = vec![1.0, 99.0, 2.0];
        let valid = vec![true; 3];
        let batch = mk_batch(in_schema.clone(), 1, 3, values, valid);

        let child = MockOp::new(in_schema, grid, vec![batch]);
        let gmap = GroupMap::new(vec![Some(0), None, Some(1)], 2);

        let mut op = AggregateOp::new(
            child,
            AggregateKind::Sum,
            gmap,
            out_schema,
            MemoryReservation::new(1 << 20),
        )
        .unwrap();
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
        assert_eq!(outs[0].get(0, 0), Some(1.0));
        assert_eq!(outs[0].get(0, 1), Some(2.0));
    }

    #[test]
    fn should_aggregate_across_multiple_series_tile_batches_for_same_step_range() {
        // given: the child emits two batches covering the SAME step range
        // (0..2) but disjoint series tiles (0..3 then 3..5), mirroring the
        // (step_tile × series_tile) emission pattern of `VectorSelectorOp`
        // when the resolved roster exceeds the default series tile width.
        // Every cell has value 1.0 — the correct total sum per step is 5.0.
        let in_schema = mk_schema("in", 5);
        let out_schema = mk_schema("out", 1);
        let grid = mk_grid(2);

        let batch_tile_a =
            mk_batch_with_series_range(in_schema.clone(), 2, 0..3, vec![1.0; 6], vec![true; 6]);
        let batch_tile_b =
            mk_batch_with_series_range(in_schema.clone(), 2, 3..5, vec![1.0; 4], vec![true; 4]);

        let child = MockOp::new(in_schema, grid, vec![batch_tile_a, batch_tile_b]);
        let gmap = GroupMap::new(vec![Some(0); 5], 1);

        // when
        let mut op = AggregateOp::new(
            child,
            AggregateKind::Sum,
            gmap,
            out_schema,
            MemoryReservation::new(1 << 20),
        )
        .expect("operator constructs");
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        // then: the aggregate must produce one logical answer per step — a
        // single cell of 5.0 per step (one batch) or, if it emits multiple
        // batches for the same step range, the values across those batches
        // must *not* be partial per-tile sums that a downstream consumer
        // would render as duplicate/arbitrary timestamps. Assert both the
        // single-batch shape and the correct per-step total.
        assert_eq!(
            outs.len(),
            1,
            "expected a single output batch covering steps 0..2, got {}",
            outs.len(),
        );
        let b = &outs[0];
        assert_eq!(b.step_count(), 2);
        assert_eq!(b.series_count(), 1);
        assert_eq!(
            b.get(0, 0),
            Some(5.0),
            "step 0 sum must be 5.0 (3 + 2 tiles)"
        );
        assert_eq!(
            b.get(1, 0),
            Some(5.0),
            "step 1 sum must be 5.0 (3 + 2 tiles)"
        );
    }

    #[test]
    fn should_aggregate_many_series_through_default_tile_shape() {
        // given: end-to-end shape reproducing the production symptom —
        // 1500 input series across 3 groups, 4 steps, with the child
        // emitting one batch per (step_tile × series_tile) using the
        // default `series_chunk = 512`. Every valid cell contributes 1.0;
        // correct `sum by (group)` per step is therefore 500.
        const SERIES: usize = 1500;
        const GROUPS: usize = 3;
        const STEPS: usize = 4;
        const SERIES_CHUNK: usize = 512;
        // Split steps into arbitrary step-tiles too (2 + 2) to exercise
        // the (step_tile × series_tile) cross product.
        const STEP_TILES: &[std::ops::Range<usize>] = &[0..2, 2..4];

        let in_schema = mk_schema("in", SERIES);
        let out_schema = mk_schema("out", GROUPS);
        let grid = mk_grid(STEPS);
        let ts: Arc<[i64]> = Arc::from(
            (0..STEPS)
                .map(|i| (i as i64) * 10)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        );

        // Round-robin groups (s0→g0, s1→g1, s2→g2, s3→g0, ...). With
        // SERIES=1500 each group gets exactly 500 series.
        let group_assignments: Vec<Option<u32>> =
            (0..SERIES).map(|s| Some((s % GROUPS) as u32)).collect();

        // Assemble the batches: for every step tile × series tile, emit
        // one batch with values=1.0 and validity=1. Series tiling uses
        // `SERIES_CHUNK` matching `VectorSelectorOp`'s default.
        let mut batches: Vec<StepBatch> = Vec::new();
        for step_tile in STEP_TILES {
            let step_count_tile = step_tile.len();
            let mut series_start = 0usize;
            while series_start < SERIES {
                let series_end = (series_start + SERIES_CHUNK).min(SERIES);
                let series_count_tile = series_end - series_start;
                let cells = step_count_tile * series_count_tile;
                let mut bits = BitSet::with_len(cells);
                for i in 0..cells {
                    bits.set(i);
                }
                batches.push(StepBatch::new(
                    ts.clone(),
                    step_tile.clone(),
                    SchemaRef::Static(in_schema.clone()),
                    series_start..series_end,
                    vec![1.0; cells],
                    bits,
                ));
                series_start = series_end;
            }
        }

        let child = MockOp::new(in_schema, grid, batches);
        let gmap = GroupMap::new(group_assignments, GROUPS);

        // when
        let mut op = AggregateOp::new(
            child,
            AggregateKind::Sum,
            gmap,
            out_schema,
            MemoryReservation::new(1 << 20),
        )
        .expect("operator constructs");
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        // then: one output batch, 4 steps × 3 groups, each cell = 500.
        assert_eq!(outs.len(), 1, "expected one aggregated output batch");
        let b = &outs[0];
        assert_eq!(b.step_count(), STEPS);
        assert_eq!(b.series_count(), GROUPS);
        for step in 0..STEPS {
            for group in 0..GROUPS {
                assert_eq!(
                    b.get(step, group),
                    Some(500.0),
                    "step={step} group={group} expected 500",
                );
            }
        }
    }

    // ========================================================================
    // 3c.1 breaker tests — topk / bottomk / quantile
    // ========================================================================

    #[test]
    fn should_select_top_k_values_per_group() {
        // given: 5 input series in one group. Values 1..=5. topk(2) → two
        // largest values (4, 5) selected; other cells validity=0.
        let in_schema = mk_schema("in", 5);
        // Output schema for topk = input schema shape (5 series).
        let out_schema = in_schema.clone();
        let grid = mk_grid(1);
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let valid = vec![true; 5];
        let batch = mk_batch(in_schema.clone(), 1, 5, values, valid);

        let child = MockOp::new(in_schema, grid, vec![batch]);
        let gmap = GroupMap::new(vec![Some(0); 5], 1);

        // when
        let mut op = AggregateOp::new(
            child,
            AggregateKind::Topk(2),
            gmap,
            out_schema,
            MemoryReservation::new(1 << 20),
        )
        .expect("operator constructs");
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        // then: output preserves the 5-series shape; only s3 and s4 valid.
        assert_eq!(outs.len(), 1);
        let b = &outs[0];
        assert_eq!(b.series_count(), 5);
        assert_eq!(b.get(0, 0), None);
        assert_eq!(b.get(0, 1), None);
        assert_eq!(b.get(0, 2), None);
        assert_eq!(b.get(0, 3), Some(4.0));
        assert_eq!(b.get(0, 4), Some(5.0));
    }

    #[test]
    fn should_select_bottom_k_values_per_group() {
        // given: 5 input series in one group. Values 1..=5. bottomk(2) →
        // two smallest (1, 2); other cells validity=0.
        let in_schema = mk_schema("in", 5);
        let out_schema = in_schema.clone();
        let grid = mk_grid(1);
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let valid = vec![true; 5];
        let batch = mk_batch(in_schema.clone(), 1, 5, values, valid);

        let child = MockOp::new(in_schema, grid, vec![batch]);
        let gmap = GroupMap::new(vec![Some(0); 5], 1);

        let mut op = AggregateOp::new(
            child,
            AggregateKind::Bottomk(2),
            gmap,
            out_schema,
            MemoryReservation::new(1 << 20),
        )
        .unwrap();
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        let b = &outs[0];
        assert_eq!(b.get(0, 0), Some(1.0));
        assert_eq!(b.get(0, 1), Some(2.0));
        assert_eq!(b.get(0, 2), None);
        assert_eq!(b.get(0, 3), None);
        assert_eq!(b.get(0, 4), None);
    }

    #[test]
    fn should_select_all_when_k_ge_group_size() {
        // given: 3 series in one group; topk(10) → every valid input
        // selected (K ≥ group size).
        let in_schema = mk_schema("in", 3);
        let out_schema = in_schema.clone();
        let grid = mk_grid(1);
        let values = vec![7.0, 3.0, 9.0];
        let valid = vec![true; 3];
        let batch = mk_batch(in_schema.clone(), 1, 3, values, valid);

        let child = MockOp::new(in_schema, grid, vec![batch]);
        let gmap = GroupMap::new(vec![Some(0); 3], 1);

        let mut op = AggregateOp::new(
            child,
            AggregateKind::Topk(10),
            gmap,
            out_schema,
            MemoryReservation::new(1 << 20),
        )
        .unwrap();
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        let b = &outs[0];
        assert_eq!(b.get(0, 0), Some(7.0));
        assert_eq!(b.get(0, 1), Some(3.0));
        assert_eq!(b.get(0, 2), Some(9.0));
    }

    #[test]
    fn should_select_nothing_when_k_zero() {
        // given: 3 valid series; topk(0) ⇒ empty selection; every output
        // cell has validity=0.
        let in_schema = mk_schema("in", 3);
        let out_schema = in_schema.clone();
        let grid = mk_grid(1);
        let values = vec![7.0, 3.0, 9.0];
        let valid = vec![true; 3];
        let batch = mk_batch(in_schema.clone(), 1, 3, values, valid);

        let child = MockOp::new(in_schema, grid, vec![batch]);
        let gmap = GroupMap::new(vec![Some(0); 3], 1);

        let mut op = AggregateOp::new(
            child,
            AggregateKind::Topk(0),
            gmap,
            out_schema,
            MemoryReservation::new(1 << 20),
        )
        .unwrap();
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        let b = &outs[0];
        assert_eq!(b.series_count(), 3);
        assert_eq!(b.get(0, 0), None);
        assert_eq!(b.get(0, 1), None);
        assert_eq!(b.get(0, 2), None);
    }

    #[test]
    fn should_handle_negative_k_as_empty() {
        // given: Negative K ⇒ no output cells selected (matches
        // `evaluator.rs::coerce_k_size`).
        let in_schema = mk_schema("in", 3);
        let out_schema = in_schema.clone();
        let grid = mk_grid(1);
        let values = vec![7.0, 3.0, 9.0];
        let valid = vec![true; 3];
        let batch = mk_batch(in_schema.clone(), 1, 3, values, valid);

        let child = MockOp::new(in_schema, grid, vec![batch]);
        let gmap = GroupMap::new(vec![Some(0); 3], 1);

        let mut op = AggregateOp::new(
            child,
            AggregateKind::Bottomk(-5),
            gmap,
            out_schema,
            MemoryReservation::new(1 << 20),
        )
        .unwrap();
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        let b = &outs[0];
        assert_eq!(b.get(0, 0), None);
        assert_eq!(b.get(0, 1), None);
        assert_eq!(b.get(0, 2), None);
    }

    #[test]
    fn should_ignore_nan_inputs_in_topk() {
        // given: 5 valid inputs; s0 and s4 are NaN. topk(2) on real
        // values picks the two largest reals (s2=3, s3=4). NaNs rank
        // "worst" per the engine's `compare_k_values` and are only
        // selected when K exceeds the non-NaN count.
        let in_schema = mk_schema("in", 5);
        let out_schema = in_schema.clone();
        let grid = mk_grid(1);
        let values = vec![f64::NAN, 2.0, 3.0, 4.0, f64::NAN];
        let valid = vec![true; 5];
        let batch = mk_batch(in_schema.clone(), 1, 5, values, valid);

        let child = MockOp::new(in_schema, grid, vec![batch]);
        let gmap = GroupMap::new(vec![Some(0); 5], 1);

        let mut op = AggregateOp::new(
            child,
            AggregateKind::Topk(2),
            gmap,
            out_schema,
            MemoryReservation::new(1 << 20),
        )
        .unwrap();
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        let b = &outs[0];
        assert_eq!(b.get(0, 0), None);
        assert_eq!(b.get(0, 1), None);
        assert_eq!(b.get(0, 2), Some(3.0));
        assert_eq!(b.get(0, 3), Some(4.0));
        assert_eq!(b.get(0, 4), None);
    }

    #[test]
    fn should_tiebreak_topk_deterministically_by_series_index() {
        // given: 4 series all valued 5.0; topk(2) must pick 2. The
        // engine's tie-break (`evaluator.rs:687-694`) breaks ties by
        // lower index wins — so s0 and s1 are the survivors.
        let in_schema = mk_schema("in", 4);
        let out_schema = in_schema.clone();
        let grid = mk_grid(1);
        let values = vec![5.0; 4];
        let valid = vec![true; 4];
        let batch = mk_batch(in_schema.clone(), 1, 4, values, valid);

        let child = MockOp::new(in_schema, grid, vec![batch]);
        let gmap = GroupMap::new(vec![Some(0); 4], 1);

        let mut op = AggregateOp::new(
            child,
            AggregateKind::Topk(2),
            gmap,
            out_schema,
            MemoryReservation::new(1 << 20),
        )
        .unwrap();
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        let b = &outs[0];
        assert_eq!(b.get(0, 0), Some(5.0));
        assert_eq!(b.get(0, 1), Some(5.0));
        assert_eq!(b.get(0, 2), None);
        assert_eq!(b.get(0, 3), None);
    }

    #[test]
    fn should_use_absolute_series_indices_for_topk_batches() {
        // given: a filter-shaped topk over the tail slice of a 6-series roster.
        // The global lower-index tie-break and output `series_range` must both
        // be based on the child's absolute roster position, not batch-local 0..n.
        // The breaker kinds emit a single filter-shape output covering
        // the full input roster, so the selected cells land at their
        // global indices (series 3 and 4) and the leading 0..3 cells
        // are all invalid.
        let in_schema = mk_schema("in", 6);
        let out_schema = in_schema.clone();
        let grid = mk_grid(1);
        let batch = mk_batch_with_series_range(
            in_schema.clone(),
            1,
            3..6,
            vec![5.0, 5.0, 1.0],
            vec![true, true, true],
        );
        let child = MockOp::new(in_schema, grid, vec![batch]);
        let gmap = GroupMap::new(vec![Some(0); 6], 1);

        // when
        let mut op = AggregateOp::new(
            child,
            AggregateKind::Topk(2),
            gmap,
            out_schema,
            MemoryReservation::new(1 << 20),
        )
        .unwrap();
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        // then
        assert_eq!(outs.len(), 1);
        let b = &outs[0];
        assert_eq!(b.series_range, 0..6);
        // Leading slots (0..3) were never emitted by the child — invalid.
        assert_eq!(b.get(0, 0), None);
        assert_eq!(b.get(0, 1), None);
        assert_eq!(b.get(0, 2), None);
        // Global series 3 and 4 tie at value 5.0 and are selected
        // (lower-index tie-break keeps the first two); series 5 has
        // value 1.0 and drops out.
        assert_eq!(b.get(0, 3), Some(5.0));
        assert_eq!(b.get(0, 4), Some(5.0));
        assert_eq!(b.get(0, 5), None);
    }

    #[test]
    fn should_compute_quantile_with_linear_interpolation() {
        // given: 5 input series in one group, values 1..=5.
        //   q=0.5 → median = 3.0
        //   q=0.75 → between 4 and 5 → rank = 0.75 * 4 = 3.0 → exactly 4.0
        //   q=0.25 → rank 1.0 → exactly 2.0
        for (q, expected) in [(0.5_f64, 3.0_f64), (0.75, 4.0), (0.25, 2.0)] {
            let in_schema = mk_schema("in", 5);
            let out_schema = mk_schema("out", 1);
            let grid = mk_grid(1);
            let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
            let valid = vec![true; 5];
            let batch = mk_batch(in_schema.clone(), 1, 5, values, valid);

            let child = MockOp::new(in_schema, grid, vec![batch]);
            let gmap = GroupMap::new(vec![Some(0); 5], 1);

            let mut op = AggregateOp::new(
                child,
                AggregateKind::Quantile(q),
                gmap,
                out_schema,
                MemoryReservation::new(1 << 20),
            )
            .unwrap();
            let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
            let b = &outs[0];
            assert!(
                approx_eq(b.get(0, 0).unwrap(), expected, 1e-12),
                "q={q}: got {:?} expected {expected}",
                b.get(0, 0),
            );
        }
    }

    #[test]
    fn should_handle_quantile_out_of_range() {
        // given: matches rollup_fns::quantile — q<0 → -inf, q>1 → +inf.
        for (q, expected) in [(-0.5_f64, f64::NEG_INFINITY), (1.5, f64::INFINITY)] {
            let in_schema = mk_schema("in", 3);
            let out_schema = mk_schema("out", 1);
            let grid = mk_grid(1);
            let values = vec![1.0, 2.0, 3.0];
            let valid = vec![true; 3];
            let batch = mk_batch(in_schema.clone(), 1, 3, values, valid);

            let child = MockOp::new(in_schema, grid, vec![batch]);
            let gmap = GroupMap::new(vec![Some(0); 3], 1);

            let mut op = AggregateOp::new(
                child,
                AggregateKind::Quantile(q),
                gmap,
                out_schema,
                MemoryReservation::new(1 << 20),
            )
            .unwrap();
            let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();
            let b = &outs[0];
            let got = b.get(0, 0).expect("valid when group non-empty");
            assert_eq!(got, expected, "q={q}");
        }
    }

    #[test]
    fn should_compute_quantile_per_group() {
        // given: 6 series, 2 groups (s0..s2 → g0; s3..s5 → g1).
        //   group 0 values: 1, 2, 3 → q=0.5 median = 2.0
        //   group 1 values: 10, 20, 30 → q=0.5 median = 20.0
        let in_schema = mk_schema("in", 6);
        let out_schema = mk_schema("out", 2);
        let grid = mk_grid(1);
        let values = vec![1.0, 2.0, 3.0, 10.0, 20.0, 30.0];
        let valid = vec![true; 6];
        let batch = mk_batch(in_schema.clone(), 1, 6, values, valid);

        let child = MockOp::new(in_schema, grid, vec![batch]);
        let gmap = GroupMap::new(
            vec![Some(0), Some(0), Some(0), Some(1), Some(1), Some(1)],
            2,
        );

        let mut op = AggregateOp::new(
            child,
            AggregateKind::Quantile(0.5),
            gmap,
            out_schema,
            MemoryReservation::new(1 << 20),
        )
        .unwrap();
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        let b = &outs[0];
        assert!(approx_eq(b.get(0, 0).unwrap(), 2.0, 1e-12));
        assert!(approx_eq(b.get(0, 1).unwrap(), 20.0, 1e-12));
    }

    #[test]
    fn should_use_absolute_series_indices_for_quantile_batches() {
        // given: only the tail slice of a 5-series roster is present in this
        // batch; quantile must still bucket values by their absolute series ids.
        let in_schema = mk_schema("in", 5);
        let out_schema = mk_schema("out", 2);
        let grid = mk_grid(1);
        let batch = mk_batch_with_series_range(
            in_schema.clone(),
            1,
            2..5,
            vec![7.0, 11.0, 13.0],
            vec![true, true, true],
        );
        let child = MockOp::new(in_schema, grid, vec![batch]);
        let gmap = GroupMap::new(vec![Some(0), Some(0), Some(0), Some(1), Some(1)], 2);

        // when
        let mut op = AggregateOp::new(
            child,
            AggregateKind::Quantile(0.5),
            gmap,
            out_schema,
            MemoryReservation::new(1 << 20),
        )
        .unwrap();
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        // then
        let b = &outs[0];
        assert_eq!(b.get(0, 0), Some(7.0));
        assert_eq!(b.get(0, 1), Some(12.0));
    }

    #[test]
    fn should_respect_memory_reservation_for_topk_heap() {
        // given: 4 input series, K=2, but reservation only holds 1 byte
        // — nowhere near the per-group heap scratch.
        let in_schema = mk_schema("in", 4);
        let out_schema = in_schema.clone();
        let grid = mk_grid(1);
        let batch = mk_batch(in_schema.clone(), 1, 4, vec![1.0; 4], vec![true; 4]);
        let child = MockOp::new(in_schema, grid, vec![batch]);
        let gmap = GroupMap::new(vec![Some(0); 4], 1);

        let result = AggregateOp::new(
            child,
            AggregateKind::Topk(2),
            gmap,
            out_schema,
            MemoryReservation::new(1),
        );
        match result {
            Err(QueryError::MemoryLimit { .. }) => {}
            _ => panic!("expected MemoryLimit on constructor"),
        }
    }

    #[test]
    fn should_preserve_input_schema_for_topk_bottomk() {
        // given: the operator's published schema for topk/bottomk is the
        // input shape (filter-semantics), not group_count.
        let in_schema = mk_schema("in", 5);
        let out_schema = in_schema.clone();
        let grid = mk_grid(1);
        let batch = mk_batch(in_schema.clone(), 1, 5, vec![1.0; 5], vec![true; 5]);
        let child = MockOp::new(in_schema, grid, vec![batch]);
        // 5 inputs into 1 group; topk preserves 5 output series.
        let gmap = GroupMap::new(vec![Some(0); 5], 1);

        let op = AggregateOp::new(
            child,
            AggregateKind::Topk(3),
            gmap,
            out_schema.clone(),
            MemoryReservation::new(1 << 20),
        )
        .unwrap();

        // when/then
        let static_schema = op
            .schema()
            .series
            .as_static()
            .expect("topk publishes a static schema");
        assert_eq!(static_schema.len(), 5);
    }

    #[test]
    fn should_use_groups_schema_for_quantile() {
        // given: quantile is a reducer — output series = group_count.
        let in_schema = mk_schema("in", 6);
        let out_schema = mk_schema("out", 2);
        let grid = mk_grid(1);
        let batch = mk_batch(in_schema.clone(), 1, 6, vec![1.0; 6], vec![true; 6]);
        let child = MockOp::new(in_schema, grid, vec![batch]);
        let gmap = GroupMap::new(
            vec![Some(0), Some(0), Some(0), Some(1), Some(1), Some(1)],
            2,
        );

        let op = AggregateOp::new(
            child,
            AggregateKind::Quantile(0.5),
            gmap,
            out_schema,
            MemoryReservation::new(1 << 20),
        )
        .unwrap();

        let static_schema = op
            .schema()
            .series
            .as_static()
            .expect("quantile publishes a static schema");
        assert_eq!(static_schema.len(), 2);
    }

    // ========================================================================
    // 6.3.9 — breaker tile-boundary stress (topk / bottomk / quantile)
    // ========================================================================

    #[test]
    fn should_select_topk_globally_across_multiple_series_tile_batches() {
        // given: 6 input series in one group with values 1..=6, emitted
        // as two series-tile batches (0..3 and 3..6) covering the same
        // step range — same shape `VectorSelectorOp` produces when the
        // roster exceeds the default 512-series tile. topk(3) must
        // pick the three globally-largest (values 4, 5, 6), not the
        // per-tile top-3.
        let in_schema = mk_schema("in", 6);
        let out_schema = in_schema.clone();
        let grid = mk_grid(1);
        let batch_a = mk_batch_with_series_range(
            in_schema.clone(),
            1,
            0..3,
            vec![1.0, 2.0, 3.0],
            vec![true, true, true],
        );
        let batch_b = mk_batch_with_series_range(
            in_schema.clone(),
            1,
            3..6,
            vec![4.0, 5.0, 6.0],
            vec![true, true, true],
        );
        let child = MockOp::new(in_schema, grid, vec![batch_a, batch_b]);
        let gmap = GroupMap::new(vec![Some(0); 6], 1);

        let mut op = AggregateOp::new(
            child,
            AggregateKind::Topk(3),
            gmap,
            out_schema,
            MemoryReservation::new(1 << 20),
        )
        .expect("operator constructs");
        let outs: Vec<Result<StepBatch, QueryError>> = drive(&mut op);

        // then: reassemble the filter-shape output into a global series ×
        // step matrix and assert only global series 3, 4, 5 are selected.
        // Multiple output batches are allowed (one per input tile was the
        // old shape); the invariant checked is cell-level.
        let mut valid: [Option<f64>; 6] = [None; 6];
        for r in outs {
            let b = r.unwrap();
            for s in 0..b.series_count() {
                let gs = b.series_range.start + s;
                if b.validity.get(s) {
                    valid[gs] = Some(b.values[s]);
                }
            }
        }
        assert_eq!(valid[0], None, "series 0 not in top-3 globally");
        assert_eq!(valid[1], None, "series 1 not in top-3 globally");
        assert_eq!(valid[2], None, "series 2 not in top-3 globally");
        assert_eq!(valid[3], Some(4.0));
        assert_eq!(valid[4], Some(5.0));
        assert_eq!(valid[5], Some(6.0));
    }

    #[test]
    fn should_select_bottomk_globally_across_multiple_series_tile_batches() {
        // given: same shape as the topk test; bottomk(3) must pick the
        // three globally-smallest values (1, 2, 3 — all in tile A).
        let in_schema = mk_schema("in", 6);
        let out_schema = in_schema.clone();
        let grid = mk_grid(1);
        let batch_a = mk_batch_with_series_range(
            in_schema.clone(),
            1,
            0..3,
            vec![1.0, 2.0, 3.0],
            vec![true, true, true],
        );
        let batch_b = mk_batch_with_series_range(
            in_schema.clone(),
            1,
            3..6,
            vec![4.0, 5.0, 6.0],
            vec![true, true, true],
        );
        let child = MockOp::new(in_schema, grid, vec![batch_a, batch_b]);
        let gmap = GroupMap::new(vec![Some(0); 6], 1);

        let mut op = AggregateOp::new(
            child,
            AggregateKind::Bottomk(3),
            gmap,
            out_schema,
            MemoryReservation::new(1 << 20),
        )
        .unwrap();
        let outs: Vec<Result<StepBatch, QueryError>> = drive(&mut op);

        let mut valid: [Option<f64>; 6] = [None; 6];
        for r in outs {
            let b = r.unwrap();
            for s in 0..b.series_count() {
                let gs = b.series_range.start + s;
                if b.validity.get(s) {
                    valid[gs] = Some(b.values[s]);
                }
            }
        }
        assert_eq!(valid[0], Some(1.0));
        assert_eq!(valid[1], Some(2.0));
        assert_eq!(valid[2], Some(3.0));
        assert_eq!(valid[3], None);
        assert_eq!(valid[4], None);
        assert_eq!(valid[5], None);
    }

    #[test]
    fn should_compute_quantile_globally_across_multiple_series_tile_batches() {
        // given: 6 input series in one group, values 1..=6 split across
        // two tiles. median (q=0.5) over 1..=6 = 3.5; a per-tile median
        // path would compute 2.0 (tile A) and 5.0 (tile B) and emit two
        // cells for the one group — wrong both in value and in
        // duplicate-coverage.
        let in_schema = mk_schema("in", 6);
        let out_schema = mk_schema("out", 1);
        let grid = mk_grid(1);
        let batch_a = mk_batch_with_series_range(
            in_schema.clone(),
            1,
            0..3,
            vec![1.0, 2.0, 3.0],
            vec![true, true, true],
        );
        let batch_b = mk_batch_with_series_range(
            in_schema.clone(),
            1,
            3..6,
            vec![4.0, 5.0, 6.0],
            vec![true, true, true],
        );
        let child = MockOp::new(in_schema, grid, vec![batch_a, batch_b]);
        let gmap = GroupMap::new(vec![Some(0); 6], 1);

        let mut op = AggregateOp::new(
            child,
            AggregateKind::Quantile(0.5),
            gmap,
            out_schema,
            MemoryReservation::new(1 << 20),
        )
        .unwrap();
        let outs: Vec<Result<StepBatch, QueryError>> = drive(&mut op);

        // then: exactly one output batch / one cell carrying the global
        // median 3.5. Reducer-shape output (one cell per group per step).
        let batches: Vec<StepBatch> = outs.into_iter().map(|r| r.unwrap()).collect();
        assert_eq!(
            batches.len(),
            1,
            "expected one output batch covering one group-cell, got {}",
            batches.len(),
        );
        let b = &batches[0];
        assert_eq!(b.series_count(), 1);
        assert_eq!(b.step_count(), 1);
        assert_eq!(b.get(0, 0), Some(3.5));
    }
}
