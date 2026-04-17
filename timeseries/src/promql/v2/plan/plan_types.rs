//! Logical-plan IR (unit 4.1).
//!
//! The logical plan is a tree that mirrors the PromQL AST but is (a) easier
//! to rewrite in the rule-based optimizer (unit 4.2) and (b) easier to
//! translate into a physical operator tree (unit 4.3).
//!
//! # Contract
//!
//! - The logical plan is **pure data** — no `Arc<SeriesSchema>`, no
//!   references to a `SeriesSource`, no compiled `GroupMap` / `MatchTable`.
//!   Those are physical-plan artifacts (unit 4.3) computed after series
//!   resolution.
//! - Each variant carries exactly the information the physical planner
//!   needs to pick the right operator and its constructor args.
//! - Operator-kind enums ([`InstantFnKind`], [`RollupKind`],
//!   [`AggregateKind`], [`BinaryOpKind`]) are re-used from
//!   [`crate::promql::v2::operators`] so the logical plan does not
//!   re-define them.

use std::sync::Arc;

use crate::promql::v2::operators::aggregate::AggregateKind;
use crate::promql::v2::operators::binary::BinaryOpKind;
use crate::promql::v2::operators::instant_fn::InstantFnKind;
use crate::promql::v2::operators::label_manip::LabelManipKind;
use crate::promql::v2::operators::rollup::RollupKind;
use promql_parser::parser;

// ---------------------------------------------------------------------------
// Supporting types
// ---------------------------------------------------------------------------

/// Millisecond-precision PromQL `offset` modifier.
///
/// Mirrors `promql_parser::parser::Offset`'s `Pos(Duration)` / `Neg(Duration)`
/// shape but stores a plain `i64` so the logical plan is cheaply `Copy`-able
/// and independent of `std::time::Duration` units. Evaluator semantics
/// (`Offset::Pos` subtracts from the step timestamp; `Offset::Neg` adds)
/// match the operator conventions established in units 3a.1 / 3a.2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Offset {
    /// `offset 5m` — shifts the evaluation point backward by the duration.
    Pos(i64),
    /// `offset -5m` — shifts the evaluation point forward by the duration.
    Neg(i64),
}

impl Offset {
    /// Millisecond magnitude of the offset, signed: positive values pull
    /// earlier, negative values pull later (mirrors the evaluator's use).
    #[inline]
    pub fn signed_ms(self) -> i64 {
        match self {
            Self::Pos(ms) => ms,
            Self::Neg(ms) => -ms,
        }
    }
}

impl From<&parser::Offset> for Offset {
    fn from(parser_offset: &parser::Offset) -> Self {
        match parser_offset {
            parser::Offset::Pos(dur) => Self::Pos(dur.as_millis() as i64),
            parser::Offset::Neg(dur) => Self::Neg(dur.as_millis() as i64),
        }
    }
}

/// Plan-time `@` modifier. Mirrors `promql_parser::parser::AtModifier` but
/// materialises `@ <float>` into an absolute millisecond timestamp up front.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AtModifier {
    /// `@ start()` — pins to the query range's start.
    Start,
    /// `@ end()` — pins to the query range's end.
    End,
    /// `@ <float>` — absolute pin in UTC milliseconds.
    Value(i64),
}

impl TryFrom<&parser::AtModifier> for AtModifier {
    type Error = &'static str;

    fn try_from(at: &parser::AtModifier) -> Result<Self, Self::Error> {
        match at {
            parser::AtModifier::Start => Ok(Self::Start),
            parser::AtModifier::End => Ok(Self::End),
            parser::AtModifier::At(system_time) => system_time
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|_| "@ modifier earlier than UNIX_EPOCH not supported")
                .map(|d| Self::Value(d.as_millis() as i64)),
        }
    }
}

/// Plan-time representation of PromQL vector-matching. Carries the raw
/// label lists from the parser; the physical planner (unit 4.3) compiles
/// this into a runtime `MatchTable` once input schemas are known.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinaryMatching {
    /// Matching axis: `on` or `ignoring`.
    pub axis: MatchingAxis,
    /// Labels that form the matching key (on `axis`).
    pub labels: Arc<[String]>,
    /// Cardinality of the match (`OneToOne`, `group_left`, `group_right`,
    /// or `ManyToMany` for set operators).
    pub cardinality: Cardinality,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchingAxis {
    /// `on(l1, l2, ...)` — match only on the listed labels.
    On,
    /// `ignoring(l1, l2, ...)` — match on every label except the listed ones.
    Ignoring,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cardinality {
    /// `a op b` — default. No side is the "many" side.
    OneToOne,
    /// `a op group_left(labels) b` — LHS is "many", RHS is "one". The
    /// `include` labels are carried through from the "one" side.
    GroupLeft { include: Arc<[String]> },
    /// `a op group_right(labels) b` — RHS is "many", LHS is "one".
    GroupRight { include: Arc<[String]> },
    /// Set-operator cardinality (`and` / `or` / `unless`).
    ManyToMany,
}

/// Lowered `by` / `without` modifier for aggregations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggregateGrouping {
    /// `by (l1, l2, ...)` — output groups on the listed labels.
    By(Arc<[String]>),
    /// `without (l1, l2, ...)` — output groups on every label except the
    /// listed ones. Empty list ⇒ each input series gets its own group.
    Without(Arc<[String]>),
}

impl AggregateGrouping {
    /// The empty grouping: `by ()` — one global group.
    pub fn by_empty() -> Self {
        Self::By(Arc::from(Vec::<String>::new()))
    }

    /// Construct from the parser's optional `LabelModifier`.
    pub fn from_label_modifier(modifier: Option<&parser::LabelModifier>) -> Self {
        match modifier {
            None => Self::by_empty(),
            Some(parser::LabelModifier::Include(labels)) => Self::By(labels_to_arc(labels)),
            Some(parser::LabelModifier::Exclude(labels)) => Self::Without(labels_to_arc(labels)),
        }
    }
}

fn labels_to_arc(labels: &promql_parser::label::Labels) -> Arc<[String]> {
    // `promql_parser::label::Labels` is a `HashSet<String>` under the hood;
    // materialise into a stable-ordered `Arc<[String]>` for plan equality.
    let mut v: Vec<String> = labels.labels.to_vec();
    v.sort();
    Arc::from(v)
}

// ---------------------------------------------------------------------------
// LogicalPlan
// ---------------------------------------------------------------------------

/// One node per operator group produced by Phase 3, plus `Scalar` for
/// compile-time constants. The tree mirrors the PromQL AST one-to-one after
/// lowering (no optimizer rewrites applied yet — that is unit 4.2).
#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlan {
    // --- leaves -----------------------------------------------------------
    /// `foo{label="value"}` — consumed by `VectorSelector` operator.
    /// `lookback_ms` is threaded through from the `LoweringContext` so the
    /// physical planner can parameterise the operator without re-reading
    /// query state.
    VectorSelector {
        selector: parser::VectorSelector,
        offset: Offset,
        at: Option<AtModifier>,
        /// Per-plan lookback; `None` means "use the engine default".
        lookback_ms: Option<i64>,
    },

    /// `foo[5m]` — consumed by `MatrixSelector` operator inside a `Rollup`
    /// (or a subquery). Never a valid logical-plan root; the `Rollup` /
    /// `Subquery` parent wraps it.
    MatrixSelector {
        selector: parser::VectorSelector,
        range_ms: i64,
        offset: Offset,
        at: Option<AtModifier>,
    },

    /// Literal scalar constant (`42`, `1.5e-3`). `NumberLiteral` and `Scalar`
    /// are collapsed into a single variant per RFC — there is no semantic
    /// distinction at plan time.
    Scalar(f64),

    /// Scalar leaf whose value is the current evaluation step timestamp in
    /// seconds. Used for `time()` and as the default argument carrier for the
    /// zero-arg calendar functions.
    Time,

    /// Scalar coercion: `scalar(v)` collapses an instant-vector child to one
    /// scalar value per step (`exactly one sample => that value`, otherwise
    /// `NaN`).
    Scalarize { child: Box<LogicalPlan> },

    /// Vector coercion: `vector(s)` re-interprets a scalar-producing child as
    /// a single anonymous instant-vector series. This is a logical typing
    /// wrapper; the physical planner may lower it to the child op unchanged.
    Vectorize { child: Box<LogicalPlan> },

    // --- stateless middle -------------------------------------------------
    /// Pointwise instant-vector function (`abs`, `ln`, `clamp`, ...).
    InstantFn {
        kind: InstantFnKind,
        child: Box<LogicalPlan>,
    },

    /// Label-rewriting instant-vector functions (`label_replace`,
    /// `label_join`).
    LabelManip {
        kind: LabelManipKind,
        child: Box<LogicalPlan>,
    },

    /// Unified range-function driver (`rate`, `increase`, `*_over_time`).
    /// `child` must be a `MatrixSelector` or `Subquery` — lowering enforces.
    Rollup {
        kind: RollupKind,
        child: Box<LogicalPlan>,
    },

    /// Vector/vector or vector/scalar or scalar/scalar binop. Matching is
    /// present for vector/vector; `None` for scalar-involving shapes.
    Binary {
        op: BinaryOpKind,
        lhs: Box<LogicalPlan>,
        rhs: Box<LogicalPlan>,
        matching: Option<BinaryMatching>,
    },

    /// Streaming or breaker aggregate (`sum`, `avg`, `topk`, `quantile`,
    /// ...). `count_values` is a separate variant ([`Self::CountValues`]).
    Aggregate {
        kind: AggregateKind,
        child: Box<LogicalPlan>,
        /// Optional scalar parameter expression for `topk` / `bottomk`
        /// when the argument is not a foldable numeric literal.
        param: Option<Box<LogicalPlan>>,
        grouping: AggregateGrouping,
    },

    // --- breakers ---------------------------------------------------------
    /// `expr[range:step]` — re-grids the child onto an inner step.
    Subquery {
        child: Box<LogicalPlan>,
        range_ms: i64,
        step_ms: i64,
        offset: Offset,
        at: Option<AtModifier>,
    },

    /// Breaker that reshapes upstream batches to different tile dimensions.
    /// Inserted by the physical planner (unit 4.5) when operators disagree
    /// on the preferred axis; present here so the IR is complete across
    /// rewrites.
    Rechunk {
        child: Box<LogicalPlan>,
        target_step_chunk: usize,
        target_series_chunk: usize,
    },

    /// `count_values("label", expr)` — deferred-schema aggregation.
    CountValues {
        label: String,
        child: Box<LogicalPlan>,
        grouping: AggregateGrouping,
    },

    // --- exchange ---------------------------------------------------------
    /// Producer/consumer decoupling via a bounded mpsc. Inserted by the
    /// planner (unit 4.5); represented here so the IR is complete.
    Concurrent {
        child: Box<LogicalPlan>,
        channel_bound: usize,
    },

    /// Fan-in over parallel child streams sharing a schema.
    Coalesce { children: Vec<LogicalPlan> },
}

impl LogicalPlan {
    /// `true` iff this node is a leaf (no logical children).
    pub fn is_leaf(&self) -> bool {
        matches!(
            self,
            Self::VectorSelector { .. }
                | Self::MatrixSelector { .. }
                | Self::Scalar(_)
                | Self::Time
        )
    }

    /// `true` iff this node produces a range (matrix) value. Useful for the
    /// physical planner's type-checking pass.
    pub fn produces_matrix(&self) -> bool {
        matches!(self, Self::MatrixSelector { .. } | Self::Subquery { .. })
    }

    /// `true` iff this node produces a scalar shape (one anonymous value per
    /// step rather than a vector/matrix).
    pub fn produces_scalar(&self) -> bool {
        match self {
            Self::Scalar(_) | Self::Time => true,
            Self::Scalarize { .. } => true,
            Self::Vectorize { .. } => false,
            Self::VectorSelector { .. }
            | Self::MatrixSelector { .. }
            | Self::Rollup { .. }
            | Self::LabelManip { .. }
            | Self::Aggregate { .. }
            | Self::Subquery { .. }
            | Self::Rechunk { .. }
            | Self::CountValues { .. }
            | Self::Concurrent { .. }
            | Self::Coalesce { .. } => false,
            Self::InstantFn { child, .. } => child.produces_scalar(),
            Self::Binary { lhs, rhs, .. } => lhs.produces_scalar() && rhs.produces_scalar(),
        }
    }
}
