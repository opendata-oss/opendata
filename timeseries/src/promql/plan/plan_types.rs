//! The logical-plan IR — pure data, one enum variant per PromQL operator
//! family, and the single rewrite surface the optimiser walks.
//!
//! Kept deliberately free of physical-planner artifacts: no
//! `Arc<SeriesSchema>`, no source handles, no compiled group maps or
//! match tables. Each variant carries only the data the physical planner
//! will need to *pick* an operator; resolution and compilation happen
//! later. Operator-kind enums ([`InstantFnKind`], [`RollupKind`],
//! [`AggregateKind`], [`BinaryOpKind`]) are re-used from
//! [`crate::promql::operators`].

use std::sync::Arc;

use crate::promql::operators::aggregate::AggregateKind;
use crate::promql::operators::binary::BinaryOpKind;
use crate::promql::operators::instant_fn::InstantFnKind;
use crate::promql::operators::label_manip::LabelManipKind;
use crate::promql::operators::rollup::RollupKind;
use promql_parser::parser;

// ---------------------------------------------------------------------------
// Supporting types
// ---------------------------------------------------------------------------

/// PromQL `offset` in ms. `Pos` shifts the eval point backward, `Neg` forward.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Offset {
    Pos(i64),
    Neg(i64),
}

impl Offset {
    /// Signed magnitude in ms: positive pulls earlier, negative pulls later.
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

/// Plan-time `@` modifier. `@ <float>` is materialised to absolute ms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AtModifier {
    Start,
    End,
    /// Absolute UTC ms.
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

/// How the two sides of a vector/vector binary op match — the plan-time
/// encoding of PromQL's `on(...)` / `ignoring(...)` / `group_left(...)`
/// / `group_right(...)` clauses. The physical planner compiles this into
/// a runtime
/// [`MatchTable`](crate::promql::operators::binary::MatchTable) once
/// both input schemas are known.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinaryMatching {
    pub axis: MatchingAxis,
    pub labels: Arc<[String]>,
    pub cardinality: Cardinality,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchingAxis {
    On,
    Ignoring,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cardinality {
    OneToOne,
    GroupLeft {
        include: Arc<[String]>,
    },
    GroupRight {
        include: Arc<[String]>,
    },
    /// Set-operator cardinality (`and` / `or` / `unless`).
    ManyToMany,
}

/// The `by (...)` / `without (...)` clause on a PromQL aggregation,
/// preserved verbatim from the AST. The physical planner consumes this
/// to build the
/// [`GroupMap`](crate::promql::operators::aggregate::GroupMap) once
/// the child's series schema is known.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggregateGrouping {
    By(Arc<[String]>),
    /// Empty list ⇒ each input series gets its own group.
    Without(Arc<[String]>),
}

impl AggregateGrouping {
    /// `by ()` — one global group.
    pub fn by_empty() -> Self {
        Self::By(Arc::from(Vec::<String>::new()))
    }

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

/// The engine's logical-plan IR — a tree of PromQL operators as plain
/// data, the single rewrite surface the optimiser walks, and the
/// post-lowering mirror of the PromQL AST (one variant per operator
/// family, plus `Scalar` / `Time` for scalar leaves).
///
/// The exchange-shaped variants (`Rechunk`, `Concurrent`, `Coalesce`) are
/// physical concerns in spirit, but they live here because
/// [`PhysicalPlan`](super::physical::PhysicalPlan) is a `dyn Operator`
/// tree with no rewrite surface of its own. Optimizer rules must treat
/// them as structurally transparent. Revisit when adding scatter-gather:
/// the natural fix is a second enum IR between `LogicalPlan` and the
/// operator tree.
#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlan {
    // --- leaves -----------------------------------------------------------
    /// `foo{...}`. `lookback_ms = None` means "use engine default".
    VectorSelector {
        selector: parser::VectorSelector,
        offset: Offset,
        at: Option<AtModifier>,
        lookback_ms: Option<i64>,
    },

    /// `foo[5m]` — never a valid logical-plan root; `Rollup` / `Subquery` wraps it.
    MatrixSelector {
        selector: parser::VectorSelector,
        range_ms: i64,
        offset: Offset,
        at: Option<AtModifier>,
    },

    /// Literal scalar (`42`, `1.5e-3`). Subsumes parser's `NumberLiteral`.
    Scalar(f64),

    /// `time()` — current step timestamp in seconds. Also the default
    /// argument for zero-arg calendar functions.
    Time,

    /// `scalar(v)`: collapses an instant-vector child to one scalar per step
    /// (`exactly one sample => that value`, else `NaN`).
    Scalarize { child: Box<LogicalPlan> },

    /// `vector(s)`: scalar-producing child → single anonymous series. The
    /// physical planner may lower this to the child unchanged.
    Vectorize { child: Box<LogicalPlan> },

    // --- stateless middle -------------------------------------------------
    /// Pointwise (`abs`, `ln`, `clamp`, ...).
    InstantFn {
        kind: InstantFnKind,
        child: Box<LogicalPlan>,
    },

    /// `label_replace`, `label_join`.
    LabelManip {
        kind: LabelManipKind,
        child: Box<LogicalPlan>,
    },

    /// `rate`, `increase`, `*_over_time`. Lowering enforces that `child` is
    /// `MatrixSelector` or `Subquery`.
    Rollup {
        kind: RollupKind,
        child: Box<LogicalPlan>,
    },

    /// `matching` is present for vector/vector; `None` for scalar-involving shapes.
    Binary {
        op: BinaryOpKind,
        lhs: Box<LogicalPlan>,
        rhs: Box<LogicalPlan>,
        matching: Option<BinaryMatching>,
    },

    /// `sum`, `avg`, `topk`, `quantile`, .... `count_values` is separate.
    Aggregate {
        kind: AggregateKind,
        child: Box<LogicalPlan>,
        /// Scalar parameter for `topk` / `bottomk` when not a foldable literal.
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

    /// Tile-shape breaker, inserted by the physical planner when operators
    /// disagree on the preferred axis. See enum-level docs for why it lives here.
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
    /// Producer/consumer decoupling via a bounded mpsc.
    Concurrent {
        child: Box<LogicalPlan>,
        channel_bound: usize,
    },

    /// Fan-in over parallel child streams sharing a schema.
    Coalesce { children: Vec<LogicalPlan> },
}

impl LogicalPlan {
    pub fn is_leaf(&self) -> bool {
        matches!(
            self,
            Self::VectorSelector { .. }
                | Self::MatrixSelector { .. }
                | Self::Scalar(_)
                | Self::Time
        )
    }

    pub fn produces_matrix(&self) -> bool {
        matches!(self, Self::MatrixSelector { .. } | Self::Subquery { .. })
    }

    /// One anonymous value per step (vs. a vector/matrix).
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
