//! `BinaryOp` implements every PromQL binary expression — arithmetic
//! (`+`, `-`, `*`, `/`, `%`, `^`, `atan2`), comparisons (`==`, `<`, ...),
//! and the set operators (`and`, `or`, `unless`). One operator type drives
//! all of them cell-by-cell from two child streams.
//!
//! The interesting work is "which LHS series pairs with which RHS series?"
//! — PromQL's vector-matching rules (`on(...)`, `ignoring(...)`,
//! `group_left`, `group_right`) decide that from labels alone, so the
//! planner computes it once as a [`MatchTable`] (input index → paired
//! index) and hands it to the operator. The operator never recomputes the
//! match.
//!
//! Shapes:
//!
//! - Vector/vector: output schema follows the "many" side of the match
//!   ([`MatchTable::OneToOne`] / `GroupLeft` → LHS; `GroupRight` → RHS).
//!   Unmatched cells emit `validity = 0`.
//! - Vector/scalar: the scalar side is a single-series degenerate batch
//!   (one series, empty labels) broadcast across every vector series.
//!   Output schema = vector side.
//! - Scalar/scalar: both sides are degenerate; single-series output.
//!
//! Comparison ops filter by default: false predicates emit `validity = 0`,
//! true predicates pass through with the **LHS value**. The `bool`
//! modifier switches to `1.0` / `0.0` output.
//!
//! Set ops (`and`, `or`, `unless`) are label-structural, vector/vector only.
//!
//! `/` and `%` follow IEEE 754 (v1's evaluator incorrectly coerced
//! division-by-zero to NaN; engine matches `promqltest` goldens).

use std::sync::Arc;
use std::task::{Context, Poll};

use crate::model::Labels;

use super::super::batch::{BitSet, SchemaRef, SeriesSchema, StepBatch};
use super::super::memory::{MemoryReservation, QueryError};
use super::super::operator::{Operator, OperatorSchema, StepGrid};

// ---------------------------------------------------------------------------
// BinaryOpKind — one operator, function selection as data
// ---------------------------------------------------------------------------

/// Per-cell binary function plus the comparison `bool` modifier.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BinaryOpKind {
    // --- arithmetic ---
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
    Atan2,

    // --- comparisons ---
    Eq { bool_modifier: bool },
    Ne { bool_modifier: bool },
    Gt { bool_modifier: bool },
    Lt { bool_modifier: bool },
    Gte { bool_modifier: bool },
    Lte { bool_modifier: bool },

    // --- set ops (vector/vector only) ---
    And,
    Or,
    Unless,
}

impl BinaryOpKind {
    /// Picks the hot loop (arith / cmp / set) without re-matching per cell.
    #[inline]
    fn class(self) -> OpClass {
        match self {
            Self::Add | Self::Sub | Self::Mul | Self::Div | Self::Mod | Self::Pow | Self::Atan2 => {
                OpClass::Arith
            }
            Self::Eq { .. }
            | Self::Ne { .. }
            | Self::Gt { .. }
            | Self::Lt { .. }
            | Self::Gte { .. }
            | Self::Lte { .. } => OpClass::Cmp,
            Self::And | Self::Or | Self::Unless => OpClass::Set,
        }
    }

    #[inline]
    fn bool_modifier(self) -> bool {
        matches!(
            self,
            Self::Eq {
                bool_modifier: true
            } | Self::Ne {
                bool_modifier: true
            } | Self::Gt {
                bool_modifier: true
            } | Self::Lt {
                bool_modifier: true
            } | Self::Gte {
                bool_modifier: true
            } | Self::Lte {
                bool_modifier: true
            }
        )
    }

    #[inline]
    fn apply_arith(self, left: f64, right: f64) -> f64 {
        match self {
            Self::Add => left + right,
            Self::Sub => left - right,
            Self::Mul => left * right,
            // IEEE 754 — matches Prometheus (and `promqltest` goldens at
            // operators.test:108-118). See module docs re: legacy divergence.
            Self::Div => left / right,
            Self::Mod => left % right,
            Self::Pow => left.powf(right),
            Self::Atan2 => left.atan2(right),
            _ => unreachable!("apply_arith called on non-arith kind"),
        }
    }

    /// Evaluate a comparison predicate. NaN in either operand → false
    /// (IEEE 754; matches Prometheus).
    #[inline]
    fn apply_cmp(self, left: f64, right: f64) -> bool {
        match self {
            Self::Eq { .. } => left == right,
            Self::Ne { .. } => left != right,
            Self::Gt { .. } => left > right,
            Self::Lt { .. } => left < right,
            Self::Gte { .. } => left >= right,
            Self::Lte { .. } => left <= right,
            _ => unreachable!("apply_cmp called on non-cmp kind"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OpClass {
    Arith,
    Cmp,
    Set,
}

// ---------------------------------------------------------------------------
// MatchTable — plan-time series matching
// ---------------------------------------------------------------------------

/// Pre-computed series matching between LHS and RHS vectors.
///
/// Built by the planner from the input series schemas and the
/// PromQL `on` / `ignoring` / `group_left` / `group_right` modifiers. The
/// operator treats this as an opaque mapping — it does not recompute
/// matching at runtime.
///
/// # Variants
///
/// - [`MatchTable::OneToOne`] — the default vector-vector case. `map[i]`
///   is the RHS series index paired with LHS series `i`, or `None` when
///   there is no match. Output schema follows the LHS.
/// - [`MatchTable::GroupLeft`] — the LHS is the "many" side. `map[i]` is
///   the *single* RHS series index matched to LHS series `i` (the "one"
///   side is unique per group, per PromQL's matching rules). Output
///   schema follows the LHS. The planner is responsible for honouring the
///   `group_left(<labels>)` label-copy semantics when it *builds the
///   output schema* — this operator does not touch labels.
/// - [`MatchTable::GroupRight`] — the RHS is the "many" side. `map[j]` is
///   the single LHS series index matched to RHS series `j`. Output
///   schema follows the RHS.
///
/// # What the planner guarantees
///
/// - Indices in `map` are `< child.schema().series.len()` for the
///   corresponding side.
/// - The output schema (passed to [`BinaryOp::new_vector_vector`]) has
///   length equal to the "many" side and is parallel-indexed with `map`
///   (so `map[i]` is the RHS index for the `i`th output series when
///   `OneToOne` / `GroupLeft`, and the LHS index for the `i`th output
///   series when `GroupRight`).
#[derive(Debug, Clone)]
pub enum MatchTable {
    /// One-to-one matching. `map[i]` is the RHS series index for LHS `i`,
    /// or `None`. Output schema = LHS schema.
    OneToOne(Vec<Option<u32>>),
    /// `group_left`: LHS is "many". `map[i]` is the RHS series index for
    /// LHS `i`, or `None`. Output schema = LHS schema.
    GroupLeft(Vec<Option<u32>>),
    /// `group_right`: RHS is "many". `map[j]` is the LHS series index for
    /// RHS `j`, or `None`. Output schema = RHS schema.
    GroupRight(Vec<Option<u32>>),
}

impl MatchTable {
    /// Number of rows in the table — matches the output series count.
    pub fn len(&self) -> usize {
        match self {
            Self::OneToOne(m) | Self::GroupLeft(m) | Self::GroupRight(m) => m.len(),
        }
    }

    /// `true` when the table has zero entries (empty output).
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// ---------------------------------------------------------------------------
// BinaryShape — tags the runtime axis of broadcasting
// ---------------------------------------------------------------------------

/// Shape of the binary operation. Determined at plan time from the
/// children's types.
///
/// Vector/scalar and scalar/vector are distinct so the planner can
/// preserve operand order (non-commutative ops — `Sub`, `Div`, `Pow`,
/// `Atan2`, asymmetric comparisons — care).
#[derive(Debug, Clone)]
pub enum BinaryShape {
    /// Vector op Vector with a pre-built match table and output schema.
    VectorVector {
        match_table: MatchTable,
        /// Output series schema — built by the planner to match the
        /// table's output side (LHS for `OneToOne`/`GroupLeft`, RHS for
        /// `GroupRight`).
        output_schema: Arc<SeriesSchema>,
    },
    /// Vector op Scalar (scalar broadcast on the RHS). Output schema =
    /// LHS vector schema.
    VectorScalar,
    /// Scalar op Vector (scalar broadcast on the LHS). Output schema =
    /// RHS vector schema.
    ScalarVector,
    /// Scalar op Scalar. Output schema = a single-series `Static` schema
    /// built at construction time.
    ScalarScalar,
}

// ---------------------------------------------------------------------------
// ConstScalarOp — scalar-as-operator helper
// ---------------------------------------------------------------------------

/// Wraps a plan-time scalar literal (like `42` in `x + 42`) as an
/// [`Operator`] so [`BinaryOp`] can treat scalar and vector children
/// uniformly — a scalar is simply "a child that produces 1-series
/// batches." Emits one [`StepBatch`] covering the full step grid with the
/// constant replicated, then end-of-stream.
pub struct ConstScalarOp {
    schema: OperatorSchema,
    step_timestamps: Arc<[i64]>,
    value: f64,
    reservation: MemoryReservation,
    yielded: bool,
}

impl ConstScalarOp {
    /// Construct a scalar operator producing `value` for every step in
    /// `grid`.
    pub fn new(value: f64, grid: StepGrid, reservation: MemoryReservation) -> Self {
        let step_count = grid.step_count;
        let step_timestamps: Arc<[i64]> = if step_count == 0 {
            Arc::from(Vec::<i64>::new())
        } else {
            let mut v = Vec::with_capacity(step_count);
            for i in 0..step_count {
                v.push(grid.start_ms + (i as i64) * grid.step_ms);
            }
            Arc::from(v)
        };
        // Scalar output is a single, unnamed series.
        let labels: Arc<[Labels]> = Arc::from(vec![Labels::new(vec![])]);
        let fps: Arc<[u128]> = Arc::from(vec![0u128]);
        let schema_series = Arc::new(SeriesSchema::new(labels, fps));
        let schema = OperatorSchema::new(SchemaRef::Static(schema_series), grid);
        Self {
            schema,
            step_timestamps,
            value,
            reservation,
            yielded: false,
        }
    }
}

impl Operator for ConstScalarOp {
    fn schema(&self) -> &OperatorSchema {
        &self.schema
    }

    fn next(&mut self, _cx: &mut Context<'_>) -> Poll<Option<Result<StepBatch, QueryError>>> {
        if self.yielded {
            return Poll::Ready(None);
        }
        let step_count = self.schema.step_grid.step_count;
        if step_count == 0 {
            self.yielded = true;
            return Poll::Ready(None);
        }
        // Allocate values column + validity bitset through the reservation.
        let bytes = out_bytes(step_count);
        if let Err(err) = self.reservation.try_grow(bytes) {
            self.yielded = true;
            return Poll::Ready(Some(Err(err)));
        }
        let values = vec![self.value; step_count];
        let validity = BitSet::all_set(step_count);
        self.reservation.release(bytes);
        self.yielded = true;
        Poll::Ready(Some(Ok(StepBatch::new(
            self.step_timestamps.clone(),
            0..step_count,
            self.schema.series.clone(),
            0..1,
            values,
            validity,
        ))))
    }
}

// ---------------------------------------------------------------------------
// Output buffer helpers
// ---------------------------------------------------------------------------

#[inline]
fn out_bytes(cells: usize) -> usize {
    let values = cells.saturating_mul(std::mem::size_of::<f64>());
    let validity = cells
        .div_ceil(64)
        .saturating_mul(std::mem::size_of::<u64>());
    values.saturating_add(validity)
}

/// RAII output-buffer pair charged to a [`MemoryReservation`].
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
            values: vec![f64::NAN; cells],
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
// BufferedSide — drained full-grid scratch for the vector/vector path
// ---------------------------------------------------------------------------

/// Dense `(step_count × total_series_count)` buffer for one side of a
/// vector/vector binary operation. Populated by absorbing every input
/// batch from the child into the cell grid, regardless of the child's
/// tile shape (step × series). Used for cross-tile matching — see
/// [`BinaryOp::apply_vv`].
///
/// The grid is row-major by step: `values[step * total_series + series]`
/// (matching `StepBatch`'s layout). `validity` runs parallel.
///
/// Memory is reserved up front from the shared reservation and released
/// on drop; an undersize reservation surfaces `QueryError::MemoryLimit`
/// at construction.
struct BufferedSide {
    reservation: MemoryReservation,
    bytes: usize,
    step_count: usize,
    total_series: usize,
    values: Vec<f64>,
    validity: BitSet,
    step_timestamps: Arc<[i64]>,
}

impl BufferedSide {
    fn allocate(
        reservation: &MemoryReservation,
        step_count: usize,
        total_series: usize,
        step_timestamps: Arc<[i64]>,
    ) -> Result<Self, QueryError> {
        let cells = step_count.saturating_mul(total_series);
        let bytes = out_bytes(cells);
        reservation.try_grow(bytes)?;
        Ok(Self {
            reservation: reservation.clone(),
            bytes,
            step_count,
            total_series,
            values: vec![f64::NAN; cells],
            validity: BitSet::with_len(cells),
            step_timestamps,
        })
    }

    /// Absorb one child batch into the grid using the batch's global
    /// `(step_range, series_range)` as its destination; idempotent across
    /// arbitrary child tiling (step × series) and interleaved emission.
    fn absorb(&mut self, batch: &StepBatch) {
        let step_count_in = batch.step_count();
        let series_count_in = batch.series_count();
        for step_off in 0..step_count_in {
            let global_step = batch.step_range.start + step_off;
            if global_step >= self.step_count {
                continue;
            }
            let in_base = step_off * series_count_in;
            let out_base = global_step * self.total_series;
            for s in 0..series_count_in {
                let in_cell = in_base + s;
                if !batch.validity.get(in_cell) {
                    continue;
                }
                let global_series = batch.series_range.start + s;
                if global_series >= self.total_series {
                    continue;
                }
                let out_cell = out_base + global_series;
                self.values[out_cell] = batch.values[in_cell];
                self.validity.set(out_cell);
            }
        }
    }

    /// Fetch `(step, global_series)` as `Option<f64>`: `Some(v)` iff the
    /// cell was written by at least one absorbed batch.
    #[inline]
    fn get(&self, step: usize, global_series: usize) -> Option<f64> {
        if step >= self.step_count || global_series >= self.total_series {
            return None;
        }
        let cell = step * self.total_series + global_series;
        if self.validity.get(cell) {
            Some(self.values[cell])
        } else {
            None
        }
    }
}

impl Drop for BufferedSide {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.reservation.release(self.bytes);
            self.bytes = 0;
        }
    }
}

// ---------------------------------------------------------------------------
// BinaryOp — the operator
// ---------------------------------------------------------------------------

/// Implements every PromQL binary expression — arithmetic, comparisons,
/// and set operators — by pulling one batch at a time from two upstream
/// children and applying a per-cell reducer.
///
/// See module docs for shape semantics, validity policy, and the `bool`
/// comparison-modifier handling.
pub struct BinaryOp<L: Operator, R: Operator> {
    lhs: L,
    rhs: R,
    kind: BinaryOpKind,
    shape: BinaryShape,
    reservation: MemoryReservation,
    schema: OperatorSchema,
    /// Total series count of each side, captured at construction from
    /// each child's static schema. Used to size the vector/vector buffer
    /// grid; unused for scalar-involving shapes.
    lhs_total_series: usize,
    rhs_total_series: usize,
    /// Buffered drain state for the vector/vector shape. Allocated lazily
    /// on the first `next()` call (so construction doesn't reserve memory
    /// for scalar shapes), absorbed into across child polls, and
    /// consumed-exactly-once when both children hit EOS. `None` for
    /// non-vector/vector shapes.
    vv_lhs: Option<BufferedSide>,
    vv_rhs: Option<BufferedSide>,
    vv_lhs_done: bool,
    vv_rhs_done: bool,
    vv_any_batch: bool,
    vv_emitted: bool,
    done: bool,
    errored: bool,
}

impl<L: Operator, R: Operator> BinaryOp<L, R> {
    /// Construct a vector/vector binary op.
    ///
    /// The planner passes in a pre-computed `match_table` and the
    /// `output_schema` built alongside it. The step grid is taken from
    /// the LHS child (the planner guarantees both children share the same
    /// grid; mismatch is treated as a programmer error).
    pub fn new_vector_vector(
        lhs: L,
        rhs: R,
        kind: BinaryOpKind,
        match_table: MatchTable,
        output_schema: Arc<SeriesSchema>,
        reservation: MemoryReservation,
    ) -> Self {
        let grid = lhs.schema().step_grid;
        debug_assert_eq!(
            grid,
            rhs.schema().step_grid,
            "BinaryOp children must share a step grid",
        );
        let lhs_total_series = lhs
            .schema()
            .series
            .as_static()
            .map(|s| s.len())
            .unwrap_or(0);
        let rhs_total_series = rhs
            .schema()
            .series
            .as_static()
            .map(|s| s.len())
            .unwrap_or(0);
        let schema = OperatorSchema::new(SchemaRef::Static(output_schema.clone()), grid);
        Self {
            lhs,
            rhs,
            kind,
            shape: BinaryShape::VectorVector {
                match_table,
                output_schema,
            },
            reservation,
            schema,
            lhs_total_series,
            rhs_total_series,
            vv_lhs: None,
            vv_rhs: None,
            vv_lhs_done: false,
            vv_rhs_done: false,
            vv_any_batch: false,
            vv_emitted: false,
            done: false,
            errored: false,
        }
    }

    /// Construct a vector/scalar binary op. The LHS is the vector side;
    /// the RHS is a scalar-producing child ([`ConstScalarOp`] or a
    /// reduction that yields a 1-series batch).
    pub fn new_vector_scalar(
        lhs: L,
        rhs: R,
        kind: BinaryOpKind,
        reservation: MemoryReservation,
    ) -> Self {
        let grid = lhs.schema().step_grid;
        debug_assert_eq!(
            grid,
            rhs.schema().step_grid,
            "BinaryOp children must share a step grid",
        );
        let schema = lhs.schema().clone();
        Self {
            lhs,
            rhs,
            kind,
            shape: BinaryShape::VectorScalar,
            reservation,
            schema,
            lhs_total_series: 0,
            rhs_total_series: 0,
            vv_lhs: None,
            vv_rhs: None,
            vv_lhs_done: false,
            vv_rhs_done: false,
            vv_any_batch: false,
            vv_emitted: false,
            done: false,
            errored: false,
        }
    }

    /// Construct a scalar/vector binary op. The RHS is the vector side.
    pub fn new_scalar_vector(
        lhs: L,
        rhs: R,
        kind: BinaryOpKind,
        reservation: MemoryReservation,
    ) -> Self {
        let grid = lhs.schema().step_grid;
        debug_assert_eq!(
            grid,
            rhs.schema().step_grid,
            "BinaryOp children must share a step grid",
        );
        let schema = rhs.schema().clone();
        Self {
            lhs,
            rhs,
            kind,
            shape: BinaryShape::ScalarVector,
            reservation,
            schema,
            lhs_total_series: 0,
            rhs_total_series: 0,
            vv_lhs: None,
            vv_rhs: None,
            vv_lhs_done: false,
            vv_rhs_done: false,
            vv_any_batch: false,
            vv_emitted: false,
            done: false,
            errored: false,
        }
    }

    /// Construct a scalar/scalar binary op. Output is a single-series
    /// batch per step range.
    pub fn new_scalar_scalar(
        lhs: L,
        rhs: R,
        kind: BinaryOpKind,
        reservation: MemoryReservation,
    ) -> Self {
        let grid = lhs.schema().step_grid;
        debug_assert_eq!(
            grid,
            rhs.schema().step_grid,
            "BinaryOp children must share a step grid",
        );
        let labels: Arc<[Labels]> = Arc::from(vec![Labels::new(vec![])]);
        let fps: Arc<[u128]> = Arc::from(vec![0u128]);
        let out_schema = Arc::new(SeriesSchema::new(labels, fps));
        let schema = OperatorSchema::new(SchemaRef::Static(out_schema), grid);
        Self {
            lhs,
            rhs,
            kind,
            shape: BinaryShape::ScalarScalar,
            reservation,
            schema,
            lhs_total_series: 0,
            rhs_total_series: 0,
            vv_lhs: None,
            vv_rhs: None,
            vv_lhs_done: false,
            vv_rhs_done: false,
            vv_any_batch: false,
            vv_emitted: false,
            done: false,
            errored: false,
        }
    }

    fn pull_both(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<(StepBatch, StepBatch)>, QueryError>> {
        // Poll LHS first, then RHS. If either is Pending, return Pending.
        // If both reach EOS simultaneously, return Ok(None). Mixed EOS is
        // a planner bug (children share a step grid) — we error cleanly.
        let l = match self.lhs.next(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(None) => None,
            Poll::Ready(Some(Err(e))) => return Poll::Ready(Err(e)),
            Poll::Ready(Some(Ok(b))) => Some(b),
        };
        let r = match self.rhs.next(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(None) => None,
            Poll::Ready(Some(Err(e))) => return Poll::Ready(Err(e)),
            Poll::Ready(Some(Ok(b))) => Some(b),
        };
        match (l, r) {
            (None, None) => Poll::Ready(Ok(None)),
            (Some(lhs), Some(rhs)) => {
                if lhs.step_range != rhs.step_range {
                    return Poll::Ready(Err(QueryError::Internal(format!(
                        "Binary: children emitted misaligned step ranges: lhs={:?} rhs={:?}",
                        lhs.step_range, rhs.step_range
                    ))));
                }
                Poll::Ready(Ok(Some((lhs, rhs))))
            }
            _ => Poll::Ready(Err(QueryError::Internal(
                "Binary: children exhausted asynchronously (step grid mismatch)".to_string(),
            ))),
        }
    }

    fn apply(&self, lhs: StepBatch, rhs: StepBatch) -> Result<StepBatch, QueryError> {
        match &self.shape {
            BinaryShape::VectorVector { .. } => {
                // Handled via the buffered vv path in `next()`; this arm
                // is unreachable because `pull_both` is only invoked for
                // scalar-involving shapes.
                Err(QueryError::Internal(
                    "Binary: vector/vector routed through per-batch apply".to_string(),
                ))
            }
            BinaryShape::VectorScalar => self.apply_vs(lhs, rhs, /* scalar_on_right = */ true),
            BinaryShape::ScalarVector => {
                self.apply_vs(rhs, lhs, /* scalar_on_right = */ false)
            }
            BinaryShape::ScalarScalar => self.apply_ss(lhs, rhs),
        }
    }

    /// Drain both children into the vector/vector buffer grids, returning
    /// `Poll::Ready(Ok(true))` once both sides have reached EOS. Allocates
    /// the grids on first call. Returns `Poll::Pending` when either side
    /// would block.
    fn drain_vv(&mut self, cx: &mut Context<'_>) -> Poll<Result<bool, QueryError>> {
        if self.vv_lhs.is_none() {
            let grid = self.schema.step_grid;
            let step_timestamps: Arc<[i64]> = Arc::from(
                (0..grid.step_count)
                    .map(|i| grid.start_ms + (i as i64) * grid.step_ms)
                    .collect::<Vec<_>>(),
            );
            match BufferedSide::allocate(
                &self.reservation,
                grid.step_count,
                self.lhs_total_series,
                step_timestamps.clone(),
            ) {
                Ok(side) => self.vv_lhs = Some(side),
                Err(err) => return Poll::Ready(Err(err)),
            }
            match BufferedSide::allocate(
                &self.reservation,
                grid.step_count,
                self.rhs_total_series,
                step_timestamps,
            ) {
                Ok(side) => self.vv_rhs = Some(side),
                Err(err) => return Poll::Ready(Err(err)),
            }
        }

        // Poll each side independently until EOS. The loop below may
        // surface Pending from either child; the caller's waker is
        // registered by the child's poll, so we'll be re-invoked.
        loop {
            let mut progressed = false;
            if !self.vv_lhs_done {
                match self.lhs.next(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(None) => {
                        self.vv_lhs_done = true;
                        progressed = true;
                    }
                    Poll::Ready(Some(Err(err))) => return Poll::Ready(Err(err)),
                    Poll::Ready(Some(Ok(batch))) => {
                        if let Some(side) = self.vv_lhs.as_mut() {
                            side.absorb(&batch);
                        }
                        self.vv_any_batch = true;
                        progressed = true;
                    }
                }
            }
            if !self.vv_rhs_done {
                match self.rhs.next(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(None) => {
                        self.vv_rhs_done = true;
                        progressed = true;
                    }
                    Poll::Ready(Some(Err(err))) => return Poll::Ready(Err(err)),
                    Poll::Ready(Some(Ok(batch))) => {
                        if let Some(side) = self.vv_rhs.as_mut() {
                            side.absorb(&batch);
                        }
                        self.vv_any_batch = true;
                        progressed = true;
                    }
                }
            }
            if self.vv_lhs_done && self.vv_rhs_done {
                return Poll::Ready(Ok(true));
            }
            if !progressed {
                return Poll::Ready(Ok(false));
            }
        }
    }

    /// Vector/vector hot loop. `match_table` selects the per-output
    /// (lhs_idx, rhs_idx) pair; unmatched rows emit `validity = 0` cells.
    ///
    /// Buffers full lhs/rhs grids before emitting. Children may tile by
    /// series (see `VectorSelectorOp` `series_chunk` default of 512) and
    /// `match_table` indices are global over the children's full series
    /// rosters — a cross-tile match (lhs in tile A, its paired rhs in
    /// tile B) requires both sides fully resolved before the hot loop.
    /// A prior per-batch path emitted one partial output per paired tile
    /// with `series_range = 0..out_series` and garbage cell values for any
    /// row that fell outside the paired tile, causing downstream consumers
    /// to see duplicate-coverage batches for the same step range.
    fn apply_vv(
        &self,
        lhs: BufferedSide,
        rhs: BufferedSide,
        match_table: &MatchTable,
        output_schema: &Arc<SeriesSchema>,
    ) -> Result<StepBatch, QueryError> {
        let step_count = lhs.step_count;
        let out_series_count = match_table.len();
        let cell_count = step_count * out_series_count;
        let mut out = OutBuffers::allocate(&self.reservation, cell_count)?;
        let class = self.kind.class();
        let bool_mod = self.kind.bool_modifier();

        // Resolve the (lhs_idx, rhs_idx) lookup per output row once, then
        // loop over steps on the inside. For GroupRight, the map index is
        // RHS-indexed and the "many" side is RHS; for OneToOne and
        // GroupLeft it's LHS-indexed.
        let (scan_is_lhs, map) = match match_table {
            MatchTable::OneToOne(m) | MatchTable::GroupLeft(m) => (true, m),
            MatchTable::GroupRight(m) => (false, m),
        };

        for (out_row, mapped) in map.iter().enumerate() {
            // For OneToOne / GroupLeft the scan-side is LHS and
            // `out_row` IS the lhs *global* series index. For
            // GroupRight the scan-side is RHS and `out_row` IS the rhs
            // global series index. `map[out_row]` may be None (no match
            // found at planning time).
            let (lhs_idx, rhs_idx) = if scan_is_lhs {
                let lhs_idx = out_row;
                let rhs_idx = mapped.map(|g| g as usize);
                (Some(lhs_idx), rhs_idx)
            } else {
                let rhs_idx = out_row;
                let lhs_idx = mapped.map(|g| g as usize);
                (lhs_idx, Some(rhs_idx))
            };

            for step_off in 0..step_count {
                let out_idx = step_off * out_series_count + out_row;
                let l_cell = lhs_idx.and_then(|idx| lhs.get(step_off, idx));
                let r_cell = rhs_idx.and_then(|idx| rhs.get(step_off, idx));

                self.write_cell(
                    class,
                    bool_mod,
                    l_cell,
                    r_cell,
                    &mut out.values,
                    &mut out.validity,
                    out_idx,
                );
            }
        }

        let (values, validity) = out.finish();
        Ok(StepBatch::new(
            lhs.step_timestamps.clone(),
            0..step_count,
            SchemaRef::Static(output_schema.clone()),
            0..out_series_count,
            values,
            validity,
        ))
    }

    /// Vector/scalar hot loop. `vec_batch` is the vector side; `scalar_batch`
    /// has exactly 1 series. The scalar is broadcast across every vector
    /// series. `scalar_on_right == true` when the op is `vector OP scalar`
    /// (preserves operand order for non-commutative ops).
    fn apply_vs(
        &self,
        vec_batch: StepBatch,
        scalar_batch: StepBatch,
        scalar_on_right: bool,
    ) -> Result<StepBatch, QueryError> {
        debug_assert_eq!(
            scalar_batch.series_count(),
            1,
            "scalar side must have exactly 1 series",
        );
        let step_count = vec_batch.step_count();
        let series_count = vec_batch.series_count();
        let cell_count = step_count * series_count;
        let mut out = OutBuffers::allocate(&self.reservation, cell_count)?;
        let class = self.kind.class();
        let bool_mod = self.kind.bool_modifier();

        for step_off in 0..step_count {
            let scalar = cell_of(&scalar_batch, step_off, 0);
            for series_off in 0..series_count {
                let v = cell_of(&vec_batch, step_off, series_off);
                let out_idx = step_off * series_count + series_off;
                let (l_cell, r_cell) = if scalar_on_right {
                    (v, scalar)
                } else {
                    (scalar, v)
                };
                self.write_cell(
                    class,
                    bool_mod,
                    l_cell,
                    r_cell,
                    &mut out.values,
                    &mut out.validity,
                    out_idx,
                );
            }
        }

        let (values, validity) = out.finish();
        Ok(StepBatch::new(
            vec_batch.step_timestamps.clone(),
            vec_batch.step_range.clone(),
            vec_batch.series.clone(),
            vec_batch.series_range.clone(),
            values,
            validity,
        ))
    }

    /// Scalar/scalar hot loop. Both inputs have 1 series. Produces one
    /// output series (1 cell per step).
    fn apply_ss(&self, lhs: StepBatch, rhs: StepBatch) -> Result<StepBatch, QueryError> {
        debug_assert_eq!(lhs.series_count(), 1);
        debug_assert_eq!(rhs.series_count(), 1);
        let step_count = lhs.step_count();
        let mut out = OutBuffers::allocate(&self.reservation, step_count)?;
        let class = self.kind.class();
        let bool_mod = self.kind.bool_modifier();

        for step_off in 0..step_count {
            let l = cell_of(&lhs, step_off, 0);
            let r = cell_of(&rhs, step_off, 0);
            self.write_cell(
                class,
                bool_mod,
                l,
                r,
                &mut out.values,
                &mut out.validity,
                step_off,
            );
        }

        let (values, validity) = out.finish();
        // Output schema lives on the operator's `schema`; we just need a
        // SchemaRef here. Since scalar/scalar's schema is Static and
        // single-series, clone it from the operator schema.
        let series = self.schema.series.clone();
        Ok(StepBatch::new(
            lhs.step_timestamps.clone(),
            lhs.step_range.clone(),
            series,
            0..1,
            values,
            validity,
        ))
    }

    /// Write one output cell given the class and the two (optional) input
    /// cells. `None` means "input absence / unmatched" (validity = 0 on
    /// that side).
    ///
    /// The argument count is deliberate: each of these is on the per-cell
    /// hot loop and bundling them into a struct would add an indirection
    /// in the inner loop without improving readability.
    #[allow(clippy::too_many_arguments)]
    #[inline]
    fn write_cell(
        &self,
        class: OpClass,
        bool_mod: bool,
        l_cell: Option<f64>,
        r_cell: Option<f64>,
        out_values: &mut [f64],
        out_validity: &mut BitSet,
        out_idx: usize,
    ) {
        match class {
            OpClass::Arith => {
                if let (Some(lv), Some(rv)) = (l_cell, r_cell) {
                    out_values[out_idx] = self.kind.apply_arith(lv, rv);
                    out_validity.set(out_idx);
                }
                // else: validity stays clear (NaN-filled); matches 3a.1
                // convention.
            }
            OpClass::Cmp => {
                if let (Some(lv), Some(rv)) = (l_cell, r_cell) {
                    let predicate = self.kind.apply_cmp(lv, rv);
                    if bool_mod {
                        // bool modifier: always emit 0/1.
                        out_values[out_idx] = if predicate { 1.0 } else { 0.0 };
                        out_validity.set(out_idx);
                    } else if predicate {
                        // Filter mode: emit LHS value.
                        out_values[out_idx] = lv;
                        out_validity.set(out_idx);
                    }
                    // false + no bool → validity clear (filtered out).
                }
            }
            OpClass::Set => {
                let l_present = l_cell.is_some();
                let r_present = r_cell.is_some();
                let (emit_v, emit_valid) = match self.kind {
                    BinaryOpKind::And => {
                        if l_present && r_present {
                            (l_cell.unwrap_or(f64::NAN), true)
                        } else {
                            (f64::NAN, false)
                        }
                    }
                    BinaryOpKind::Or => {
                        if l_present {
                            (l_cell.unwrap_or(f64::NAN), true)
                        } else if r_present {
                            (r_cell.unwrap_or(f64::NAN), true)
                        } else {
                            (f64::NAN, false)
                        }
                    }
                    BinaryOpKind::Unless => {
                        if l_present && !r_present {
                            (l_cell.unwrap_or(f64::NAN), true)
                        } else {
                            (f64::NAN, false)
                        }
                    }
                    _ => unreachable!("OpClass::Set kinds handled above"),
                };
                if emit_valid {
                    out_values[out_idx] = emit_v;
                    out_validity.set(out_idx);
                }
            }
        }
    }
}

/// Read a cell as `Option<f64>`: `Some(value)` iff validity bit set.
#[inline]
fn cell_of(batch: &StepBatch, step_off: usize, series_off: usize) -> Option<f64> {
    let idx = step_off * batch.series_count() + series_off;
    if batch.validity.get(idx) {
        Some(batch.values[idx])
    } else {
        None
    }
}

impl<L: Operator, R: Operator> Operator for BinaryOp<L, R> {
    fn schema(&self) -> &OperatorSchema {
        &self.schema
    }

    fn next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<StepBatch, QueryError>>> {
        if self.done || self.errored {
            return Poll::Ready(None);
        }

        // Vector/vector is a pipeline-breaker: drain both children into
        // the full-grid buffers so cross-tile matches (lhs series in tile
        // A paired with rhs series in tile B) can be resolved against
        // a consistent snapshot.
        if matches!(self.shape, BinaryShape::VectorVector { .. }) {
            if self.vv_emitted {
                self.done = true;
                return Poll::Ready(None);
            }
            match self.drain_vv(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(err)) => {
                    self.errored = true;
                    return Poll::Ready(Some(Err(err)));
                }
                Poll::Ready(Ok(false)) => {
                    // No progress in the last round and at least one side
                    // is still live — should only be reachable via a bug
                    // in child Pending signalling. Guard with an internal
                    // error rather than spin.
                    self.errored = true;
                    return Poll::Ready(Some(Err(QueryError::Internal(
                        "Binary: vv drain stalled with live children".to_string(),
                    ))));
                }
                Poll::Ready(Ok(true)) => {}
            }

            self.vv_emitted = true;
            if !self.vv_any_batch {
                // No input from either side — emit no batch (matches the
                // previous streaming behaviour on empty children and keeps
                // `reshape_range` from seeing a zero-valued full grid).
                self.done = true;
                self.vv_lhs = None;
                self.vv_rhs = None;
                return Poll::Ready(None);
            }

            let (match_table, output_schema) = match &self.shape {
                BinaryShape::VectorVector {
                    match_table,
                    output_schema,
                } => (match_table.clone(), output_schema.clone()),
                _ => unreachable!(),
            };
            let lhs_side = self.vv_lhs.take();
            let rhs_side = self.vv_rhs.take();
            let (lhs_side, rhs_side) = match (lhs_side, rhs_side) {
                (Some(l), Some(r)) => (l, r),
                _ => {
                    self.errored = true;
                    return Poll::Ready(Some(Err(QueryError::Internal(
                        "Binary: vv buffers missing on drain completion".to_string(),
                    ))));
                }
            };
            return match self.apply_vv(lhs_side, rhs_side, &match_table, &output_schema) {
                Ok(batch) => Poll::Ready(Some(Ok(batch))),
                Err(err) => {
                    self.errored = true;
                    Poll::Ready(Some(Err(err)))
                }
            };
        }

        match self.pull_both(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => {
                self.errored = true;
                Poll::Ready(Some(Err(e)))
            }
            Poll::Ready(Ok(None)) => {
                self.done = true;
                Poll::Ready(None)
            }
            Poll::Ready(Ok(Some((lhs, rhs)))) => match self.apply(lhs, rhs) {
                Ok(batch) => Poll::Ready(Some(Ok(batch))),
                Err(e) => {
                    self.errored = true;
                    Poll::Ready(Some(Err(e)))
                }
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Label, Labels};
    use std::sync::Arc;
    use std::task::{RawWaker, RawWakerVTable, Waker};

    // ---- helpers ------------------------------------------------------------

    fn noop_waker() -> Waker {
        const VTABLE: RawWakerVTable = RawWakerVTable::new(
            |_| RawWaker::new(std::ptr::null(), &VTABLE),
            |_| {},
            |_| {},
            |_| {},
        );
        unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
    }

    fn mk_labels(prefix: &str, i: usize) -> Labels {
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
    }

    fn mk_schema(prefix: &str, n: usize) -> Arc<SeriesSchema> {
        let labels: Vec<Labels> = (0..n).map(|i| mk_labels(prefix, i)).collect();
        let fps: Vec<u128> = (0..n as u128).collect();
        Arc::new(SeriesSchema::new(Arc::from(labels), Arc::from(fps)))
    }

    fn mk_schema_single() -> Arc<SeriesSchema> {
        let labels: Arc<[Labels]> = Arc::from(vec![Labels::new(vec![])]);
        let fps: Arc<[u128]> = Arc::from(vec![0u128]);
        Arc::new(SeriesSchema::new(labels, fps))
    }

    fn mk_grid(step_count: usize) -> StepGrid {
        StepGrid {
            start_ms: 1_000,
            end_ms: 1_000 + ((step_count as i64 - 1).max(0)) * 1_000,
            step_ms: 1_000,
            step_count,
        }
    }

    /// Build a StepBatch from flat row-major values + parallel validity.
    fn mk_batch(
        schema: Arc<SeriesSchema>,
        step_count: usize,
        series_count: usize,
        values: Vec<f64>,
        validity: Vec<bool>,
    ) -> StepBatch {
        assert_eq!(values.len(), step_count * series_count);
        assert_eq!(validity.len(), values.len());
        let ts: Arc<[i64]> = Arc::from(
            (0..step_count)
                .map(|i| 1_000 + (i as i64) * 1_000)
                .collect::<Vec<_>>(),
        );
        let mut bits = BitSet::with_len(validity.len());
        for (i, &b) in validity.iter().enumerate() {
            if b {
                bits.set(i);
            }
        }
        StepBatch::new(
            ts,
            0..step_count,
            SchemaRef::Static(schema),
            0..series_count,
            values,
            bits,
        )
    }

    /// Mock operator yielding a scripted queue of batches.
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

    fn drive<L: Operator, R: Operator>(
        op: &mut BinaryOp<L, R>,
    ) -> Vec<Result<StepBatch, QueryError>> {
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

    // ========================================================================
    // required tests
    // ========================================================================

    #[test]
    fn should_apply_add_vector_vector_one_to_one() {
        // given: two vectors of 2 series, step_count=2, matched 0↔1, 1↔0
        let lschema = mk_schema("l", 2);
        let rschema = mk_schema("r", 2);
        let grid = mk_grid(2);
        let lhs_batch = mk_batch(
            lschema.clone(),
            2,
            2,
            vec![1.0, 2.0, 3.0, 4.0],
            vec![true; 4],
        );
        let rhs_batch = mk_batch(
            rschema.clone(),
            2,
            2,
            vec![10.0, 20.0, 30.0, 40.0],
            vec![true; 4],
        );
        let match_table = MatchTable::OneToOne(vec![Some(1), Some(0)]);

        // when
        let lhs = MockOp::new(lschema.clone(), grid, vec![lhs_batch]);
        let rhs = MockOp::new(rschema, grid, vec![rhs_batch]);
        let mut op = BinaryOp::new_vector_vector(
            lhs,
            rhs,
            BinaryOpKind::Add,
            match_table,
            lschema,
            MemoryReservation::new(1 << 20),
        );
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        // then: out row 0 = lhs[0] + rhs[1]; out row 1 = lhs[1] + rhs[0]
        assert_eq!(outs.len(), 1);
        let b = &outs[0];
        assert_eq!(b.series_count(), 2);
        assert_eq!(b.get(0, 0), Some(1.0 + 20.0)); // step 0, out_row 0 = lhs[0,0]+rhs[0,1]
        assert_eq!(b.get(0, 1), Some(2.0 + 10.0));
        assert_eq!(b.get(1, 0), Some(3.0 + 40.0));
        assert_eq!(b.get(1, 1), Some(4.0 + 30.0));
    }

    #[test]
    fn should_apply_sub_vector_scalar() {
        // given: vector of 2 series × 2 steps, scalar=100
        let vschema = mk_schema("v", 2);
        let grid = mk_grid(2);
        let vec_batch = mk_batch(
            vschema.clone(),
            2,
            2,
            vec![1.0, 2.0, 3.0, 4.0],
            vec![true; 4],
        );
        let scalar_batch = mk_batch(mk_schema_single(), 2, 1, vec![100.0, 100.0], vec![true; 2]);

        // when: v - 100
        let lhs = MockOp::new(vschema, grid, vec![vec_batch]);
        let rhs = MockOp::new(mk_schema_single(), grid, vec![scalar_batch]);
        let mut op = BinaryOp::new_vector_scalar(
            lhs,
            rhs,
            BinaryOpKind::Sub,
            MemoryReservation::new(1 << 20),
        );
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        // then
        let b = &outs[0];
        assert_eq!(b.series_count(), 2);
        assert_eq!(b.get(0, 0), Some(-99.0));
        assert_eq!(b.get(0, 1), Some(-98.0));
        assert_eq!(b.get(1, 0), Some(-97.0));
        assert_eq!(b.get(1, 1), Some(-96.0));
    }

    #[test]
    fn should_apply_div_scalar_scalar() {
        // given: two scalars, 3 steps
        let grid = mk_grid(3);
        let lhs_batch = mk_batch(mk_schema_single(), 3, 1, vec![6.0, 9.0, 1.0], vec![true; 3]);
        let rhs_batch = mk_batch(mk_schema_single(), 3, 1, vec![2.0, 3.0, 0.0], vec![true; 3]);

        // when: scalar/scalar, 1/0 → +Inf per IEEE 754 (Prometheus; see
        // module docs — legacy engine would emit NaN).
        let lhs = MockOp::new(mk_schema_single(), grid, vec![lhs_batch]);
        let rhs = MockOp::new(mk_schema_single(), grid, vec![rhs_batch]);
        let mut op = BinaryOp::new_scalar_scalar(
            lhs,
            rhs,
            BinaryOpKind::Div,
            MemoryReservation::new(1 << 20),
        );
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        // then
        let b = &outs[0];
        assert_eq!(b.series_count(), 1);
        assert_eq!(b.step_count(), 3);
        assert_eq!(b.get(0, 0), Some(3.0));
        assert_eq!(b.get(1, 0), Some(3.0));
        let inf = b.get(2, 0).unwrap();
        assert!(inf.is_infinite() && inf > 0.0);
    }

    #[test]
    fn should_set_validity_zero_when_rhs_missing_in_one_to_one() {
        // given: lhs[0] has no match (None), lhs[1]↔rhs[0]
        let lschema = mk_schema("l", 2);
        let rschema = mk_schema("r", 1);
        let grid = mk_grid(1);
        let lhs_batch = mk_batch(lschema.clone(), 1, 2, vec![1.0, 2.0], vec![true, true]);
        let rhs_batch = mk_batch(rschema.clone(), 1, 1, vec![10.0], vec![true]);
        let match_table = MatchTable::OneToOne(vec![None, Some(0)]);

        // when
        let lhs = MockOp::new(lschema.clone(), grid, vec![lhs_batch]);
        let rhs = MockOp::new(rschema, grid, vec![rhs_batch]);
        let mut op = BinaryOp::new_vector_vector(
            lhs,
            rhs,
            BinaryOpKind::Add,
            match_table,
            lschema,
            MemoryReservation::new(1 << 20),
        );
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        // then: out_row 0 invalid; out_row 1 = 2 + 10
        let b = &outs[0];
        assert_eq!(b.get(0, 0), None);
        assert_eq!(b.get(0, 1), Some(12.0));
    }

    #[test]
    fn should_apply_comparison_without_bool_modifier_skipping_false_cells() {
        // given: v > 10 with values [5, 15, 20]
        let vschema = mk_schema("v", 3);
        let grid = mk_grid(1);
        let vec_batch = mk_batch(vschema.clone(), 1, 3, vec![5.0, 15.0, 20.0], vec![true; 3]);
        let scalar_batch = mk_batch(mk_schema_single(), 1, 1, vec![10.0], vec![true]);

        // when
        let lhs = MockOp::new(vschema, grid, vec![vec_batch]);
        let rhs = MockOp::new(mk_schema_single(), grid, vec![scalar_batch]);
        let mut op = BinaryOp::new_vector_scalar(
            lhs,
            rhs,
            BinaryOpKind::Gt {
                bool_modifier: false,
            },
            MemoryReservation::new(1 << 20),
        );
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        // then: 5>10 false → dropped; 15 / 20 passed through with LHS value
        let b = &outs[0];
        assert_eq!(b.get(0, 0), None);
        assert_eq!(b.get(0, 1), Some(15.0));
        assert_eq!(b.get(0, 2), Some(20.0));
    }

    #[test]
    fn should_apply_comparison_with_bool_modifier_producing_0_1() {
        // given: v > bool 10 with values [5, 15, 20]
        let vschema = mk_schema("v", 3);
        let grid = mk_grid(1);
        let vec_batch = mk_batch(vschema.clone(), 1, 3, vec![5.0, 15.0, 20.0], vec![true; 3]);
        let scalar_batch = mk_batch(mk_schema_single(), 1, 1, vec![10.0], vec![true]);

        // when
        let lhs = MockOp::new(vschema, grid, vec![vec_batch]);
        let rhs = MockOp::new(mk_schema_single(), grid, vec![scalar_batch]);
        let mut op = BinaryOp::new_vector_scalar(
            lhs,
            rhs,
            BinaryOpKind::Gt {
                bool_modifier: true,
            },
            MemoryReservation::new(1 << 20),
        );
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        // then: all three matched, with 0.0/1.0
        let b = &outs[0];
        assert_eq!(b.get(0, 0), Some(0.0));
        assert_eq!(b.get(0, 1), Some(1.0));
        assert_eq!(b.get(0, 2), Some(1.0));
    }

    #[test]
    fn should_handle_pow_mod_atan2() {
        // given: two scalars, step_count=3
        let grid = mk_grid(3);
        let l = mk_batch(mk_schema_single(), 3, 1, vec![2.0, 7.0, 1.0], vec![true; 3]);
        let r = mk_batch(mk_schema_single(), 3, 1, vec![3.0, 4.0, 0.0], vec![true; 3]);

        let cases: [(BinaryOpKind, [f64; 3]); 3] = [
            (BinaryOpKind::Pow, [8.0, 2401.0, 1.0]),
            (BinaryOpKind::Mod, [2.0 % 3.0, 7.0 % 4.0, 1.0_f64 % 0.0_f64]),
            (
                BinaryOpKind::Atan2,
                [2.0f64.atan2(3.0), 7.0f64.atan2(4.0), 1.0f64.atan2(0.0)],
            ),
        ];
        for (kind, expect) in cases {
            let lhs = MockOp::new(mk_schema_single(), grid, vec![l.clone()]);
            let rhs = MockOp::new(mk_schema_single(), grid, vec![r.clone()]);
            let mut op =
                BinaryOp::new_scalar_scalar(lhs, rhs, kind, MemoryReservation::new(1 << 20));
            let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|rr| rr.unwrap()).collect();
            let b = &outs[0];
            for (step, e) in expect.iter().enumerate() {
                let got = b.get(step, 0).unwrap();
                if e.is_nan() {
                    assert!(got.is_nan(), "{kind:?} step {step}: expected NaN got {got}");
                } else {
                    assert!(
                        (got - e).abs() < 1e-9 || got == *e,
                        "{kind:?} step {step}: expected {e} got {got}"
                    );
                }
            }
        }
    }

    #[test]
    fn should_apply_and_returning_lhs_when_rhs_present() {
        // given: 2 lhs series, 2 rhs series, 1↔1 match
        let lschema = mk_schema("l", 2);
        let rschema = mk_schema("r", 2);
        let grid = mk_grid(1);
        // lhs valid on both; rhs has series 1 valid, series 0 invalid.
        let lhs_batch = mk_batch(lschema.clone(), 1, 2, vec![1.0, 2.0], vec![true, true]);
        let rhs_batch = mk_batch(rschema.clone(), 1, 2, vec![99.0, 99.0], vec![false, true]);
        let match_table = MatchTable::OneToOne(vec![Some(0), Some(1)]);

        // when: lhs and rhs
        let lhs = MockOp::new(lschema.clone(), grid, vec![lhs_batch]);
        let rhs = MockOp::new(rschema, grid, vec![rhs_batch]);
        let mut op = BinaryOp::new_vector_vector(
            lhs,
            rhs,
            BinaryOpKind::And,
            match_table,
            lschema,
            MemoryReservation::new(1 << 20),
        );
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        // then: row 0 → rhs invalid so drop; row 1 → both valid, emit LHS.
        let b = &outs[0];
        assert_eq!(b.get(0, 0), None);
        assert_eq!(b.get(0, 1), Some(2.0));
    }

    #[test]
    fn should_apply_or_falling_back_to_rhs_when_lhs_missing() {
        let lschema = mk_schema("l", 2);
        let rschema = mk_schema("r", 2);
        let grid = mk_grid(1);
        // lhs: series 0 invalid, series 1 valid; rhs: series 0 valid, series 1 invalid.
        let lhs_batch = mk_batch(lschema.clone(), 1, 2, vec![1.0, 2.0], vec![false, true]);
        let rhs_batch = mk_batch(rschema.clone(), 1, 2, vec![10.0, 20.0], vec![true, false]);
        let match_table = MatchTable::OneToOne(vec![Some(0), Some(1)]);

        let lhs = MockOp::new(lschema.clone(), grid, vec![lhs_batch]);
        let rhs = MockOp::new(rschema, grid, vec![rhs_batch]);
        let mut op = BinaryOp::new_vector_vector(
            lhs,
            rhs,
            BinaryOpKind::Or,
            match_table,
            lschema,
            MemoryReservation::new(1 << 20),
        );
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        // then: row 0 lhs missing, rhs present → emit rhs (10); row 1 lhs present → emit lhs (2)
        let b = &outs[0];
        assert_eq!(b.get(0, 0), Some(10.0));
        assert_eq!(b.get(0, 1), Some(2.0));
    }

    #[test]
    fn should_apply_unless_dropping_lhs_when_rhs_present() {
        let lschema = mk_schema("l", 2);
        let rschema = mk_schema("r", 2);
        let grid = mk_grid(1);
        let lhs_batch = mk_batch(lschema.clone(), 1, 2, vec![1.0, 2.0], vec![true, true]);
        // rhs: series 0 present → drop lhs[0]; series 1 missing → keep lhs[1]
        let rhs_batch = mk_batch(rschema.clone(), 1, 2, vec![99.0, 99.0], vec![true, false]);
        let match_table = MatchTable::OneToOne(vec![Some(0), Some(1)]);

        let lhs = MockOp::new(lschema.clone(), grid, vec![lhs_batch]);
        let rhs = MockOp::new(rschema, grid, vec![rhs_batch]);
        let mut op = BinaryOp::new_vector_vector(
            lhs,
            rhs,
            BinaryOpKind::Unless,
            match_table,
            lschema,
            MemoryReservation::new(1 << 20),
        );
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        let b = &outs[0];
        assert_eq!(b.get(0, 0), None);
        assert_eq!(b.get(0, 1), Some(2.0));
    }

    #[test]
    fn should_propagate_error_from_upstream() {
        // given: LHS yields an error; op short-circuits.
        let lschema = mk_schema("l", 1);
        let rschema = mk_schema("r", 1);
        let grid = mk_grid(1);
        let rhs_batch = mk_batch(rschema.clone(), 1, 1, vec![3.0], vec![true]);
        let lhs = MockOp::with_queue(
            lschema.clone(),
            grid,
            vec![Err(QueryError::MemoryLimit {
                requested: 8,
                cap: 0,
                already_reserved: 0,
            })],
        );
        let rhs = MockOp::new(rschema, grid, vec![rhs_batch]);
        let mut op = BinaryOp::new_vector_vector(
            lhs,
            rhs,
            BinaryOpKind::Add,
            MatchTable::OneToOne(vec![Some(0)]),
            lschema,
            MemoryReservation::new(1 << 20),
        );

        // when
        let outs = drive(&mut op);

        // then
        assert_eq!(outs.len(), 1);
        assert!(matches!(outs[0], Err(QueryError::MemoryLimit { .. })));
        // After error, subsequent polls return None.
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        match op.next(&mut cx) {
            Poll::Ready(None) => {}
            other => panic!("expected None after error, got {other:?}"),
        }
    }

    #[test]
    fn should_respect_memory_reservation() {
        // given: reservation of 0 bytes.
        let lschema = mk_schema("l", 1);
        let rschema = mk_schema("r", 1);
        let grid = mk_grid(1);
        let lhs_batch = mk_batch(lschema.clone(), 1, 1, vec![1.0], vec![true]);
        let rhs_batch = mk_batch(rschema.clone(), 1, 1, vec![2.0], vec![true]);
        let lhs = MockOp::new(lschema.clone(), grid, vec![lhs_batch]);
        let rhs = MockOp::new(rschema, grid, vec![rhs_batch]);
        let mut op = BinaryOp::new_vector_vector(
            lhs,
            rhs,
            BinaryOpKind::Add,
            MatchTable::OneToOne(vec![Some(0)]),
            lschema,
            MemoryReservation::new(1),
        );

        // when
        let outs = drive(&mut op);

        // then
        assert_eq!(outs.len(), 1);
        assert!(matches!(outs[0], Err(QueryError::MemoryLimit { .. })));
    }

    #[test]
    fn should_return_static_schema_matching_match_table_output() {
        // given: vector/vector binop with OneToOne
        let lschema = mk_schema("l", 3);
        let rschema = mk_schema("r", 3);
        let grid = mk_grid(2);
        let lhs = MockOp::new(lschema.clone(), grid, vec![]);
        let rhs = MockOp::new(rschema, grid, vec![]);
        let op = BinaryOp::new_vector_vector(
            lhs,
            rhs,
            BinaryOpKind::Add,
            MatchTable::OneToOne(vec![Some(0), Some(1), Some(2)]),
            lschema.clone(),
            MemoryReservation::new(1 << 20),
        );

        // when / then
        assert!(!op.schema().series.is_deferred());
        assert_eq!(op.schema().series.as_static().unwrap().len(), 3);
        assert_eq!(op.schema().step_grid.step_count, 2);
    }

    #[test]
    fn should_handle_child_end_of_stream() {
        // given: both children yield None from the start
        let lschema = mk_schema("l", 1);
        let rschema = mk_schema("r", 1);
        let grid = mk_grid(1);
        let lhs = MockOp::new(lschema.clone(), grid, vec![]);
        let rhs = MockOp::new(rschema, grid, vec![]);
        let mut op = BinaryOp::new_vector_vector(
            lhs,
            rhs,
            BinaryOpKind::Add,
            MatchTable::OneToOne(vec![Some(0)]),
            lschema,
            MemoryReservation::new(1 << 20),
        );

        // when
        let outs = drive(&mut op);

        // then: no results, no errors — just end-of-stream.
        assert!(outs.is_empty());
    }

    #[test]
    fn should_stitch_aligned_step_ranges_from_children() {
        // given: two children each emitting a [0..2) then [2..4) batch.
        // Vector/vector is a pipeline-breaker: children are drained into
        // a full-grid buffer and a single output batch covering 0..4 is
        // emitted on EOS (correct for cross-tile matches; see module
        // docs).
        let lschema = mk_schema("l", 1);
        let rschema = mk_schema("r", 1);
        let grid = StepGrid {
            start_ms: 1_000,
            end_ms: 4_000,
            step_ms: 1_000,
            step_count: 4,
        };
        let ts: Arc<[i64]> = Arc::from(vec![1_000, 2_000, 3_000, 4_000]);

        let mk = |start: usize, end: usize, values: Vec<f64>, schema: Arc<SeriesSchema>| {
            let mut v = BitSet::with_len(values.len());
            for i in 0..values.len() {
                v.set(i);
            }
            StepBatch::new(
                ts.clone(),
                start..end,
                SchemaRef::Static(schema),
                0..1,
                values,
                v,
            )
        };

        let lhs_batches = vec![
            mk(0, 2, vec![1.0, 2.0], lschema.clone()),
            mk(2, 4, vec![3.0, 4.0], lschema.clone()),
        ];
        let rhs_batches = vec![
            mk(0, 2, vec![10.0, 20.0], rschema.clone()),
            mk(2, 4, vec![30.0, 40.0], rschema.clone()),
        ];

        // when
        let lhs = MockOp::new(lschema.clone(), grid, lhs_batches);
        let rhs = MockOp::new(rschema, grid, rhs_batches);
        let mut op = BinaryOp::new_vector_vector(
            lhs,
            rhs,
            BinaryOpKind::Add,
            MatchTable::OneToOne(vec![Some(0)]),
            lschema,
            MemoryReservation::new(1 << 20),
        );
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        // then: one output batch covering the full grid with correct
        // per-(step, series) sums.
        assert_eq!(outs.len(), 1);
        assert_eq!(outs[0].step_range, 0..4);
        assert_eq!(outs[0].get(0, 0), Some(11.0));
        assert_eq!(outs[0].get(1, 0), Some(22.0));
        assert_eq!(outs[0].get(2, 0), Some(33.0));
        assert_eq!(outs[0].get(3, 0), Some(44.0));
    }

    #[test]
    fn should_apply_group_left_with_shared_rhs() {
        // given: LHS has 3 "many" series all matching the same single RHS
        // series (e.g. `http_requests + on(env) group_left single_rhs`).
        let lschema = mk_schema("l", 3);
        let rschema = mk_schema("r", 1);
        let grid = mk_grid(1);
        let lhs_batch = mk_batch(
            lschema.clone(),
            1,
            3,
            vec![1.0, 2.0, 3.0],
            vec![true, true, true],
        );
        let rhs_batch = mk_batch(rschema.clone(), 1, 1, vec![100.0], vec![true]);
        let match_table = MatchTable::GroupLeft(vec![Some(0), Some(0), Some(0)]);

        // when
        let lhs = MockOp::new(lschema.clone(), grid, vec![lhs_batch]);
        let rhs = MockOp::new(rschema, grid, vec![rhs_batch]);
        let mut op = BinaryOp::new_vector_vector(
            lhs,
            rhs,
            BinaryOpKind::Add,
            match_table,
            lschema,
            MemoryReservation::new(1 << 20),
        );
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        // then: all three lhs series were paired with rhs[0].
        let b = &outs[0];
        assert_eq!(b.series_count(), 3);
        assert_eq!(b.get(0, 0), Some(101.0));
        assert_eq!(b.get(0, 1), Some(102.0));
        assert_eq!(b.get(0, 2), Some(103.0));
    }

    #[test]
    fn should_apply_group_right_with_shared_lhs() {
        // given: RHS has 2 "many" series, both matching LHS[0].
        let lschema = mk_schema("l", 1);
        let rschema = mk_schema("r", 2);
        let grid = mk_grid(1);
        let lhs_batch = mk_batch(lschema.clone(), 1, 1, vec![5.0], vec![true]);
        let rhs_batch = mk_batch(rschema.clone(), 1, 2, vec![10.0, 20.0], vec![true; 2]);
        let match_table = MatchTable::GroupRight(vec![Some(0), Some(0)]);

        // when
        let lhs = MockOp::new(lschema, grid, vec![lhs_batch]);
        let rhs = MockOp::new(rschema.clone(), grid, vec![rhs_batch]);
        let mut op = BinaryOp::new_vector_vector(
            lhs,
            rhs,
            BinaryOpKind::Mul,
            match_table,
            rschema,
            MemoryReservation::new(1 << 20),
        );
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        // then: output schema is RHS-shaped; each output = lhs * rhs[j]
        let b = &outs[0];
        assert_eq!(b.series_count(), 2);
        assert_eq!(b.get(0, 0), Some(50.0));
        assert_eq!(b.get(0, 1), Some(100.0));
    }

    // ========================================================================
    // tile-boundary stress (RFC 0007 6.3.9)
    // ========================================================================

    /// Build a batch over an arbitrary `series_range` slice of a `roster`
    /// schema. Values are row-major; `values[step * series_count + s]`
    /// holds the sample for `(step, series_range.start + s)`.
    fn mk_tile_batch(
        roster: Arc<SeriesSchema>,
        step_count: usize,
        series_range: std::ops::Range<usize>,
        values: Vec<f64>,
        validity: Vec<bool>,
    ) -> StepBatch {
        let series_count = series_range.len();
        assert_eq!(values.len(), step_count * series_count);
        assert_eq!(validity.len(), values.len());
        let ts: Arc<[i64]> = Arc::from(
            (0..step_count)
                .map(|i| 1_000 + (i as i64) * 1_000)
                .collect::<Vec<_>>(),
        );
        let mut bits = BitSet::with_len(validity.len());
        for (i, &b) in validity.iter().enumerate() {
            if b {
                bits.set(i);
            }
        }
        StepBatch::new(
            ts,
            0..step_count,
            SchemaRef::Static(roster),
            series_range,
            values,
            bits,
        )
    }

    #[test]
    fn should_apply_vector_vector_binary_across_multi_series_tile_batches() {
        // given: vector/vector over 1024 matched series (identity match:
        // lhs[i] ↔ rhs[i]) with both sides tiled into 2 × 512-series
        // batches — this is exactly the shape the default
        // `VectorSelectorOp` emits for rosters >512 series. Values are
        // chosen so the correct per-cell answer is
        // `value[step, series] = 1000 * step + series` (lhs) + same
        // shifted to `10_000 * step + series` (rhs); add-combined sum per
        // cell is therefore `11_000 * step + 2 * series`.
        const SERIES: usize = 1024;
        const TILE: usize = 512;
        const STEP_COUNT: usize = 2;

        let lschema = mk_schema("m", SERIES);
        let rschema = mk_schema("m", SERIES);
        let grid = mk_grid(STEP_COUNT);

        let build_tile = |prefix_scale: f64, series_range: std::ops::Range<usize>| -> Vec<f64> {
            let mut v = Vec::with_capacity(STEP_COUNT * series_range.len());
            for step in 0..STEP_COUNT {
                for s in series_range.clone() {
                    v.push(prefix_scale * step as f64 + s as f64);
                }
            }
            v
        };

        let lhs_a = mk_tile_batch(
            lschema.clone(),
            STEP_COUNT,
            0..TILE,
            build_tile(1_000.0, 0..TILE),
            vec![true; STEP_COUNT * TILE],
        );
        let lhs_b = mk_tile_batch(
            lschema.clone(),
            STEP_COUNT,
            TILE..SERIES,
            build_tile(1_000.0, TILE..SERIES),
            vec![true; STEP_COUNT * TILE],
        );
        let rhs_a = mk_tile_batch(
            rschema.clone(),
            STEP_COUNT,
            0..TILE,
            build_tile(10_000.0, 0..TILE),
            vec![true; STEP_COUNT * TILE],
        );
        let rhs_b = mk_tile_batch(
            rschema.clone(),
            STEP_COUNT,
            TILE..SERIES,
            build_tile(10_000.0, TILE..SERIES),
            vec![true; STEP_COUNT * TILE],
        );

        // OneToOne identity match: output row i pulls lhs[i] + rhs[i].
        let match_table: Vec<Option<u32>> = (0..SERIES as u32).map(Some).collect();
        let match_table = MatchTable::OneToOne(match_table);

        let lhs = MockOp::new(lschema.clone(), grid, vec![lhs_a, lhs_b]);
        let rhs = MockOp::new(rschema, grid, vec![rhs_a, rhs_b]);
        let mut op = BinaryOp::new_vector_vector(
            lhs,
            rhs,
            BinaryOpKind::Add,
            match_table,
            lschema,
            MemoryReservation::new(1 << 20),
        );
        let outs: Vec<StepBatch> = drive(&mut op).into_iter().map(|r| r.unwrap()).collect();

        // then: reassemble a global (step, series) cell matrix from
        // whatever batch shapes the op produced, then verify every cell
        // equals the single-tile reference `11_000 * step + 2 * series`.
        // Also verify no duplicate coverage: each global cell must be
        // written exactly once.
        let mut covered = vec![vec![false; SERIES]; STEP_COUNT];
        let mut computed = vec![vec![f64::NAN; SERIES]; STEP_COUNT];
        for batch in &outs {
            let sc = batch.series_count();
            for step_off in 0..batch.step_count() {
                let gs = batch.step_range.start + step_off;
                for so in 0..sc {
                    let cell = step_off * sc + so;
                    if !batch.validity.get(cell) {
                        continue;
                    }
                    let global_series = batch.series_range.start + so;
                    assert!(
                        !covered[gs][global_series],
                        "cell (step={gs}, series={global_series}) covered twice",
                    );
                    covered[gs][global_series] = true;
                    computed[gs][global_series] = batch.values[cell];
                }
            }
        }
        for step in 0..STEP_COUNT {
            for series in 0..SERIES {
                let expected = 11_000.0 * step as f64 + 2.0 * series as f64;
                assert!(
                    covered[step][series],
                    "missing cell (step={step}, series={series})",
                );
                assert!(
                    (computed[step][series] - expected).abs() < 1e-9,
                    "cell (step={step}, series={series}): expected {expected}, got {}",
                    computed[step][series],
                );
            }
        }
    }
}
