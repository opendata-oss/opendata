//! `Aggregate` operator — units 3b.4 (streaming) and 3c.1 (breakers).
//!
//! Reduces an upstream [`Operator`]'s step batches by collapsing input
//! series into output groups. The group map — the input-series-index →
//! output-group-index lookup — is a **plan-time artifact** (RFC 0007
//! §"Core Data Model" / §"Execution Model"): the planner builds it from
//! the query's `by (labels)` / `without (labels)` modifiers and the
//! input series schema, and hands it to the operator as a constructor
//! argument. This operator performs no grouping at runtime.
//!
//! # Aggregations covered
//!
//! ## Streaming (3b.4)
//!
//! Named "streaming" for the per-cell single-pass reducer. Correctness
//! under arbitrary child tiling (`step_tile × series_tile` emission from
//! `VectorSelectorOp`, plus `Concurrent` / `Coalesce` interleaving)
//! requires buffering every input cell into a `(step × group)`
//! accumulator grid and emitting a single output batch on child-EOS.
//! The per-cell reducer stays cheap; the grid's footprint is
//! `O(step_count × output_groups × sizeof(Accumulator))`. See §5
//! Decisions Log for why an earlier per-batch emit path was incorrect.
//!
//! | kind     | reducer                                     | min-valid |
//! |----------|---------------------------------------------|-----------|
//! | `Sum`    | Kahan–Neumaier compensated sum              | 1         |
//! | `Avg`    | compensated incremental mean (overflow-safe)| 1         |
//! | `Min`    | NaN-safe min (first real initialises)       | 1         |
//! | `Max`    | NaN-safe max (first real initialises)       | 1         |
//! | `Count`  | integer count of valid input cells          | 1         |
//! | `Stddev` | Welford single-pass, Kahan-compensated      | 1         |
//! | `Stdvar` | Welford single-pass, Kahan-compensated      | 1         |
//! | `Group`  | `1.0` iff any input contributed             | 1         |
//!
//! ## Breakers (3c.1) — `topk`, `bottomk`, `quantile`
//!
//! These buffer a whole step before emitting: you cannot decide whether
//! a given `(step, series)` cell belongs in the top K until every input
//! for that step has been seen. The breaker paths run inside the same
//! [`AggregateOp`] — the variant on [`AggregateKind`] selects the code
//! path — and share the same [`GroupMap`] shape as the streaming ops.
//!
//! | kind         | output schema     | per-step memory         |
//! |--------------|-------------------|-------------------------|
//! | `Topk(k)`    | **input** series  | `k × group_count` heap  |
//! | `Bottomk(k)` | **input** series  | `k × group_count` heap  |
//! | `Quantile(q)`| group series      | per-group sort buffer   |
//!
//! ### Output schema asymmetry (planner contract)
//!
//! `topk` / `bottomk` act as **filters**: they pick the K largest /
//! smallest values per group per step and preserve the **input** series
//! as output, flipping validity=0 for cells that didn't make the cut.
//! `quantile` is a **reducer**: it emits one output cell per group per
//! step, same shape as the streaming kinds.
//!
//! The planner decides the output schema and hands the correct one in
//! to [`AggregateOp::new`] (see §5 Decisions Log 3c.1). The operator
//! debug-asserts the schema size matches the variant's expectation:
//!
//! - streaming / `Quantile`: `output_schema.len() == group_count`
//! - `Topk` / `Bottomk`: `output_schema.len() == input_series_count`
//!
//! ### Tie-break rule (topk / bottomk)
//!
//! Matches the existing engine (`evaluator.rs:649-693`
//! `compare_k_values` + `KHeapEntry::cmp`): equal-valued cells are
//! ordered by **lower input-series index wins** (deterministic,
//! first-seen preference). NaN inputs rank "worst" — they are
//! selected only when K ≥ (valid non-NaN count in group) and some
//! NaN-bearing cells remain; matches legacy behavior.
//!
//! ### K semantics (topk / bottomk)
//!
//! K is an `i64`. Per `evaluator.rs:2249-2253` `coerce_k_size`:
//! K < 1 ⇒ no cells selected. K ≥ input width ⇒ every valid input cell
//! is eligible for selection. Literal `k` values are coerced at plan
//! time; scalar expression params (e.g. `topk(scalar(foo), bar)`) are
//! drained once onto the query step grid and coerced per step.
//!
//! ### q semantics (quantile)
//!
//! Matches `rollup::rollup_fns::quantile` linear-interpolation rule
//! (cited Prometheus `quantile_over_time`): `q < 0` ⇒ `-inf`, `q > 1`
//! ⇒ `+inf`, `q == NaN` ⇒ `NaN`. Validity=1 for the group whenever
//! at least one valid input contributed.
//!
//! # Group map semantics
//!
//! The constructor takes a [`GroupMap`] with an `input_to_group:
//! Vec<Option<u32>>` of length equal to the input series count. Entry `i`
//! is the output group index for input series `i`, or `None` to drop
//! that input from all aggregations (edge case; the planner only emits
//! `None` if it ever needs to exclude a series without trimming the
//! input schema — see §5 Decisions Log for discussion).
//!
//! - `by ()` ⇒ all input series map to group 0, `group_count == 1`.
//! - `without ()` ⇒ each input series gets its own group, `group_count
//!   == input_series_count`.
//! - The output [`SeriesSchema`] (`group_count` entries) is built by the
//!   planner from the selected grouping labels and passed in via
//!   [`AggregateOp::new`].
//!
//! # Validity rules
//!
//! - `Sum` / `Avg` / `Min` / `Max` / `Stddev` / `Stdvar` — output cell is
//!   valid iff at least one valid input contributed to its group.
//! - `Count` — output cell is valid iff at least one valid input
//!   contributed (value is the count, always ≥ 1 in that case). A group
//!   with zero valid inputs emits `validity = 0` (matches the existing
//!   engine — `count()` over no samples drops the group, and within an
//!   existing group with no valid cells the result is absent).
//! - `Group` — output cell is valid iff at least one input contributed;
//!   value is always `1.0`.
//!
//! # Memory accounting
//!
//! - **Scratch accumulators** — `step_count × group_count` accumulators
//!   for streaming kinds, allocated once at operator construction; bytes
//!   are charged to the reservation and released on `Drop`. Construction
//!   fails with [`QueryError::MemoryLimit`] if the grid does not fit
//!   the query reservation.
//! - **Breaker scratch** — `Topk` / `Bottomk` reserve a per-group heap;
//!   `Quantile` reserves a per-group sort buffer. Both are per-step
//!   (drained between steps) because breaker paths still emit per-batch.
//! - **Per-batch output** — `Vec<f64>` + `BitSet` for the output batch,
//!   charged on each poll and transferred to the consumer via
//!   [`StepBatch::new`]. The input batch's reservation belongs to the
//!   child operator.
//!
//! # Schema contract
//!
//! [`SchemaRef::Static`] — the output schema is pre-computed by the
//! planner.

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
///
/// A `Copy` enum so dispatch is a single match per step. Streaming
/// variants (3b.4) are single-pass per cell; breaker variants (3c.1:
/// `Topk` / `Bottomk` / `Quantile`) buffer a whole step before emitting
/// and dispatch to a separate code path inside [`AggregateOp`]. Keeping
/// them all in one enum (vs. one-enum-per-operator) lets the planner
/// lower every aggregation to a single `AggregateKind`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AggregateKind {
    /// Kahan-compensated sum over valid input cells.
    Sum,
    /// Overflow-resistant running mean.
    Avg,
    /// NaN-safe minimum — NaN is ignored when a real value is available.
    Min,
    /// NaN-safe maximum — NaN is ignored when a real value is available.
    Max,
    /// Integer count of valid input cells, emitted as `f64`.
    Count,
    /// Population standard deviation (Welford single-pass).
    Stddev,
    /// Population variance (Welford single-pass).
    Stdvar,
    /// Always emits `1.0` when at least one input contributed.
    Group,
    /// `topk(k, v)` — select the K largest values per group per step.
    ///
    /// Output schema is the **input series** (see §"Breakers" in the
    /// module docs). Cells not selected get `validity = 0`. `k < 1`
    /// selects nothing. Matches the existing engine's
    /// `select_k_from_group` with `KAggregationOrder::Top`.
    Topk(i64),
    /// `bottomk(k, v)` — smallest-K counterpart to [`Self::Topk`].
    Bottomk(i64),
    /// `quantile(q, v)` — per-group, per-step q-th quantile with linear
    /// interpolation between ranks. Shares the streaming output-schema
    /// shape (one cell per group). See [`rollup_fns::quantile`] in
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

/// Plan-time mapping from input series index to output group index.
///
/// Built by the planner from the input series' labels and the query's
/// `by` / `without` modifier. The operator treats this as an opaque
/// lookup — it does not interpret labels or recompute grouping at
/// runtime (RFC §"Execution Model").
///
/// # Encoding choice
///
/// `input_to_group` uses `Vec<Option<u32>>` rather than a sentinel value
/// (e.g. `u32::MAX`). Rationale: the `None` case is rare (the planner
/// almost always emits `Some(_)` for every input; a `None` happens only
/// when the planner explicitly wants to drop an input from the
/// aggregation). `Option<u32>` makes the intent explicit at the cost of
/// 4 bytes of padding per entry — negligible compared to the per-batch
/// values/validity column this operator already pays for. See §5
/// Decisions Log.
///
/// # Invariants (planner-guaranteed)
///
/// - `input_to_group.len()` equals the input operator's series count.
/// - Every `Some(g)` value satisfies `g < group_count`.
#[derive(Debug, Clone)]
pub struct GroupMap {
    /// For each input-series index, the output group index (or `None`
    /// to drop from aggregation).
    pub input_to_group: Vec<Option<u32>>,
    /// Number of output groups. The output [`SeriesSchema`] has exactly
    /// this many rows.
    pub group_count: usize,
}

impl GroupMap {
    /// Build a group map. Panics in debug if any `Some(g)` exceeds
    /// `group_count` (planner contract).
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

    /// Number of input series this map covers.
    #[inline]
    pub fn input_series_count(&self) -> usize {
        self.input_to_group.len()
    }
}

// ---------------------------------------------------------------------------
// Per-group accumulator
// ---------------------------------------------------------------------------

/// Single-pass accumulator for one group, used by every kind above.
///
/// Fields fall into three orthogonal lanes:
/// - Sum/Avg: Kahan-compensated `sum` / `c`.
/// - Min/Max: `extremum` tracks the running value; the `any_valid` flag
///   distinguishes "no valid input yet" from "first valid was NaN".
/// - Welford (Stddev/Stdvar): `mean` / `c_mean` / `m2` / `c_m2` /
///   `count` evolve together on every update.
///
/// All fields are always updated regardless of kind (the update cost is
/// trivially overlapped with the per-cell work). The reducer reads only
/// the lane it needs.
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

/// Streaming aggregation operator.
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
    /// See §5 Decisions Log: streaming aggregate is a step-bounded breaker
    /// — it must see every series tile for a given step before it can
    /// emit, because aggregations are associative across input rows but
    /// not decomposable into "partial tile sums" without a downstream
    /// merge. Buffering the whole grid keeps the operator correct under
    /// arbitrary batch ordering (step/series tiles interleaved, or split
    /// by `Concurrent`/`Coalesce`).
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
                // (series tiles × step tiles). See §5 Decisions Log.
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

    /// Buffered per-step reduction for `Topk` / `Bottomk` / `Quantile`.
    ///
    /// Memory: the per-step scratch (heap or sort buffer) was reserved
    /// up front in `new`. The per-batch output column is charged here
    /// via `OutBuffers`.
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
            // Breaker kinds reduce batch-by-batch (existing per-step
            // buffering inside each batch is sufficient for their current
            // call sites — see §5 Decisions Log for the scope of this
            // fix).
            match self.child.next(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(None) => {
                    self.done = true;
                    Poll::Ready(None)
                }
                Poll::Ready(Some(Err(err))) => {
                    self.errored = true;
                    Poll::Ready(Some(Err(err)))
                }
                Poll::Ready(Some(Ok(input))) => match self.reduce_batch_breaker(&input) {
                    Ok(batch) => Poll::Ready(Some(Ok(batch))),
                    Err(err) => {
                        self.errored = true;
                        Poll::Ready(Some(Err(err)))
                    }
                },
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
            let mut saw_any_batch = false;
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
                        if !saw_any_batch {
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
                        saw_any_batch = true;
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
    use crate::promql::v2::batch::{SchemaRef, SeriesSchema};
    use crate::promql::v2::operator::StepGrid;
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
        let b = &outs[0];
        assert_eq!(b.series_range, 3..6);
        assert_eq!(b.get(0, 0), Some(5.0));
        assert_eq!(b.get(0, 1), Some(5.0));
        assert_eq!(b.get(0, 2), None);
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
        let mut valid = vec![None; 6];
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

        let mut valid = vec![None; 6];
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
