//! `Binary` operator — unit 3b.3.
//!
//! Pointwise binary operation between two upstream [`Operator`]s producing
//! [`StepBatch`]es. Handles the three PromQL shapes: vector/vector (with
//! pre-computed series matching), vector/scalar (scalar broadcast), and
//! scalar/scalar (single-series degenerate).
//!
//! # Series matching is plan-time
//!
//! RFC 0007 §"Core Data Model": *"Group maps and series matching are
//! computed at plan time and reused for every batch — both are invariant
//! across steps."* This operator therefore does **not** compute matching
//! at runtime. The planner passes in a [`MatchTable`] indexing the LHS
//! series set to the RHS series set (or vice versa for `group_right`),
//! along with the output [`SeriesSchema`]. Unmatched cells emit with
//! `validity = 0`.
//!
//! # Shapes
//!
//! ## Vector/vector
//!
//! Both children produce `StepBatch`es over the plan-time-aligned step
//! grid. For each step in the batch range and each output-series slot,
//! the operator reads the corresponding LHS and RHS cells (via the
//! match table), applies [`BinaryOpKind::apply`], and writes one output
//! cell. The output series schema is:
//! - LHS's for [`MatchTable::OneToOne`] and [`MatchTable::GroupLeft`]
//!   ("many on the left").
//! - RHS's for [`MatchTable::GroupRight`] ("many on the right").
//!
//! ## Vector/scalar
//!
//! A scalar in v2 is a degenerate [`StepBatch`] with exactly one series.
//! [`ConstScalarOp`] emits such a batch for every step range. Use
//! [`BinaryShape::VectorScalar`] or [`BinaryShape::ScalarVector`] — the
//! operator broadcasts the scalar cell across all vector series.
//! Output schema = the vector side's schema.
//!
//! ## Scalar/scalar
//!
//! Both children are degenerate (1-series) batches. The operator produces
//! a single-series batch per step range. Output schema = a single-series
//! `SchemaRef::Static`.
//!
//! # Comparison ops and the `bool` modifier
//!
//! PromQL comparisons (`==`, `!=`, `<`, `>`, `<=`, `>=`) are filters by
//! default: cells whose predicate evaluates to false are dropped from the
//! result (emitted with `validity = 0`), and cells whose predicate is true
//! pass through with the **LHS value** (not `1.0`). With the `bool`
//! modifier, comparisons produce `1.0`/`0.0` for every matched cell
//! (validity stays with the input). See [`BinaryOpKind`] for the per-op
//! `bool` flag.
//!
//! This matches the legacy engine at `timeseries/src/promql/evaluator.rs:
//! 1898-1923, 2082-2094` bit-for-bit.
//!
//! # Set operators
//!
//! `and`, `or`, `unless` are label-structural — they operate on series
//! membership, not values. Per step:
//! - `A and B` — keep LHS cell iff matched RHS cell exists AND is valid.
//! - `A or B` — keep LHS cell if valid; otherwise fall back to RHS.
//! - `A unless B` — keep LHS cell iff matched RHS cell is missing or
//!   invalid.
//!
//! The existing engine (`evaluator.rs`) does **not** implement set ops;
//! this is a strict superset.
//!
//! # Division by zero
//!
//! The operator uses **IEEE 754 semantics** for `/` and `%`: `x / 0 = ±Inf
//! (or NaN for 0/0)`. The legacy engine at `evaluator.rs:2159-2165`
//! forces all division-by-zero to NaN — a bug. The `promqltest` goldens
//! (`operators.test:108-118`) expect IEEE 754; v2 matches goldens.
//! Divergence from legacy documented in §5 Decisions Log.
//!
//! # Memory accounting
//!
//! Only the output values column (`Vec<f64>`) and validity bitset
//! (`BitSet`) allocations are charged through [`MemoryReservation::try_grow`].
//! The input batches' reservations belong to the child operators.

use std::sync::Arc;
use std::task::{Context, Poll};

use crate::model::Labels;

use super::super::batch::{BitSet, SchemaRef, SeriesSchema, StepBatch};
use super::super::memory::{MemoryReservation, QueryError};
use super::super::operator::{Operator, OperatorSchema, StepGrid};

// ---------------------------------------------------------------------------
// BinaryOpKind — one operator, function selection as data
// ---------------------------------------------------------------------------

/// Discriminant for the per-cell binary function plus comparison-op
/// behaviour flags.
///
/// Comparison variants carry a `bool` field reflecting the PromQL
/// `bool` modifier:
/// - `bool = false` (the PromQL default for `==`, `!=`, `<`, `>`, `<=`,
///   `>=`): comparison acts as a filter. Cells with a false predicate are
///   dropped (`validity = 0`); cells with a true predicate pass through
///   with the LHS value.
/// - `bool = true`: comparison produces `1.0` / `0.0` per matched cell;
///   validity is preserved.
///
/// Set operators (`And`, `Or`, `Unless`) operate on series membership and
/// are only defined for vector/vector.
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
    /// Classify the kind. Callers use this to pick the hot loop (arithmetic
    /// vs comparison vs set) without re-matching per cell.
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

    /// `true` if this is a comparison op with the `bool` modifier set.
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

    /// Apply the scalar function. For comparisons, returns
    /// `CmpResult::True` / `CmpResult::False`; callers consult
    /// [`Self::bool_modifier`] to decide output-value and validity.
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
/// Built by the Phase 4 planner from the input series schemas and the
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

/// Degenerate [`Operator`] that emits a single 1-series `StepBatch` cover-
/// ing the full step grid, with the given constant value replicated.
///
/// Used by the Phase 4 planner (and by this module's tests) to wrap a
/// plan-time scalar as a child of [`BinaryOp`]. Keeps the binary operator
/// uniform across shapes: a scalar is "a child that produces 1-series
/// batches."
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
// BinaryOp — the operator
// ---------------------------------------------------------------------------

/// Pull-based binary operator across two upstream children.
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
            BinaryShape::VectorVector {
                match_table,
                output_schema,
            } => self.apply_vv(lhs, rhs, match_table, output_schema),
            BinaryShape::VectorScalar => self.apply_vs(lhs, rhs, /* scalar_on_right = */ true),
            BinaryShape::ScalarVector => {
                self.apply_vs(rhs, lhs, /* scalar_on_right = */ false)
            }
            BinaryShape::ScalarScalar => self.apply_ss(lhs, rhs),
        }
    }

    /// Vector/vector hot loop. `match_table` selects the per-output
    /// (lhs_idx, rhs_idx) pair; unmatched rows emit `validity = 0` cells.
    fn apply_vv(
        &self,
        lhs: StepBatch,
        rhs: StepBatch,
        match_table: &MatchTable,
        output_schema: &Arc<SeriesSchema>,
    ) -> Result<StepBatch, QueryError> {
        let step_count = lhs.step_count();
        let out_series_count = match_table.len();
        let cell_count = step_count * out_series_count;
        let mut out = OutBuffers::allocate(&self.reservation, cell_count)?;
        let lhs_series_count = lhs.series_count();
        let rhs_series_count = rhs.series_count();
        let class = self.kind.class();
        let bool_mod = self.kind.bool_modifier();

        // Resolve the (lhs_off, rhs_off) lookup per output row once, then
        // loop over steps on the inside. For GroupRight, the map index is
        // RHS-indexed and the "many" side is RHS; for OneToOne and
        // GroupLeft it's LHS-indexed.
        let (scan_is_lhs, map) = match match_table {
            MatchTable::OneToOne(m) | MatchTable::GroupLeft(m) => (true, m),
            MatchTable::GroupRight(m) => (false, m),
        };

        for (out_row, mapped) in map.iter().enumerate() {
            // Resolve the lhs / rhs batch-local offsets for this output
            // row. `map[out_row]` may be None (unmatched).
            let (lhs_off, rhs_off) = if scan_is_lhs {
                // map is LHS-indexed; out_row IS lhs_off (for OneToOne /
                // GroupLeft the output series set == LHS series set).
                let lhs_off = out_row;
                if lhs_off >= lhs_series_count {
                    continue; // batch doesn't cover this series
                }
                match mapped {
                    Some(rhs_idx) => {
                        let rhs_off = *rhs_idx as usize;
                        if rhs_off >= rhs_series_count {
                            continue;
                        }
                        (Some(lhs_off), Some(rhs_off))
                    }
                    None => (Some(lhs_off), None),
                }
            } else {
                // GroupRight: map is RHS-indexed; out_row IS rhs_off.
                let rhs_off = out_row;
                if rhs_off >= rhs_series_count {
                    continue;
                }
                match mapped {
                    Some(lhs_idx) => {
                        let lhs_off = *lhs_idx as usize;
                        if lhs_off >= lhs_series_count {
                            continue;
                        }
                        (Some(lhs_off), Some(rhs_off))
                    }
                    None => (None, Some(rhs_off)),
                }
            };

            for step_off in 0..step_count {
                let out_idx = step_off * out_series_count + out_row;

                // Read lhs/rhs cells for this (step, row). None means
                // the batch doesn't have that side in this row.
                let l_cell = lhs_off.and_then(|off| cell_of(&lhs, step_off, off));
                let r_cell = rhs_off.and_then(|off| cell_of(&rhs, step_off, off));

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
            lhs.step_range.clone(),
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
        // given: two children each emitting a [0..2) then [2..4) batch
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

        // then: two output batches covering the same step ranges.
        assert_eq!(outs.len(), 2);
        assert_eq!(outs[0].step_range, 0..2);
        assert_eq!(outs[0].get(0, 0), Some(11.0));
        assert_eq!(outs[0].get(1, 0), Some(22.0));
        assert_eq!(outs[1].step_range, 2..4);
        assert_eq!(outs[1].get(0, 0), Some(33.0));
        assert_eq!(outs[1].get(1, 0), Some(44.0));
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
}
