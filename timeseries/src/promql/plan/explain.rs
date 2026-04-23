//! The `/explain` endpoint: renders a plan as a JSON-serialisable
//! [`PlanNode`] tree, at three stages — unoptimised logical, optimised
//! logical, and physical.
//!
//! [`describe_physical`] mirrors
//! `super::physical::build_node`'s per-variant dispatch but
//! instantiates no operators and performs no I/O, so HTTP handlers can
//! render it as a dry run. The physical describer duplicates
//! `build_node`'s dispatch;
//! `should_describe_physical_agrees_with_build_node` guards against
//! drift.
//!
//! [`pretty_print`] renders the same tree in a DataFusion-style indented
//! text format.

use serde::{Deserialize, Serialize};

use super::lowering::LoweringContext;
use super::parallelism::Parallelism;
use super::plan_types::{
    AggregateGrouping, AtModifier, BinaryMatching, Cardinality, LogicalPlan, MatchingAxis, Offset,
};

/// Bump on a breaking change to node names or arg shapes.
pub const SCHEMA_VERSION: u32 = 1;

/// `op` names the operator; `args` holds variant-specific fields; `children`
/// are sub-plans in execution order.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PlanNode {
    pub op: String,
    #[serde(skip_serializing_if = "serde_json::Map::is_empty", default)]
    pub args: serde_json::Map<String, serde_json::Value>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub children: Vec<PlanNode>,
}

impl PlanNode {
    fn new(op: &str) -> Self {
        Self {
            op: op.to_string(),
            args: serde_json::Map::new(),
            children: Vec::new(),
        }
    }

    fn with_arg(mut self, key: &str, value: impl Into<serde_json::Value>) -> Self {
        self.args.insert(key.to_string(), value.into());
        self
    }

    fn with_children(mut self, children: Vec<PlanNode>) -> Self {
        self.children = children;
        self
    }
}

/// Three-stage dry-run plan: unoptimised logical, optimised logical,
/// and a pure description of the physical tree the planner would build.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExplainResult {
    pub schema_version: u32,
    pub logical_unoptimized: PlanNode,
    pub logical_optimized: PlanNode,
    pub physical: PlanNode,
}

// ---------------------------------------------------------------------------
// describe_logical — one PlanNode per LogicalPlan variant
// ---------------------------------------------------------------------------

/// Walk a [`LogicalPlan`] and produce a serialisable [`PlanNode`] tree.
pub fn describe_logical(plan: &LogicalPlan) -> PlanNode {
    match plan {
        LogicalPlan::VectorSelector {
            selector,
            offset,
            at,
            lookback_ms,
        } => {
            let mut n = PlanNode::new("VectorSelector").with_arg("matcher", selector.to_string());
            add_offset_at(&mut n, *offset, *at);
            if let Some(lb) = lookback_ms {
                n = n.with_arg("lookbackMs", *lb);
            }
            n
        }
        LogicalPlan::MatrixSelector {
            selector,
            range_ms,
            offset,
            at,
        } => {
            let mut n = PlanNode::new("MatrixSelector")
                .with_arg("matcher", selector.to_string())
                .with_arg("rangeMs", *range_ms);
            add_offset_at(&mut n, *offset, *at);
            n
        }
        LogicalPlan::Scalar(v) => PlanNode::new("Scalar").with_arg("value", *v),
        LogicalPlan::Time => PlanNode::new("Time"),
        LogicalPlan::Scalarize { child } => {
            PlanNode::new("Scalarize").with_children(vec![describe_logical(child)])
        }
        LogicalPlan::Vectorize { child } => {
            PlanNode::new("Vectorize").with_children(vec![describe_logical(child)])
        }
        LogicalPlan::InstantFn { kind, child } => PlanNode::new("InstantFn")
            .with_arg("kind", format!("{kind:?}"))
            .with_children(vec![describe_logical(child)]),
        LogicalPlan::LabelManip { kind, child } => PlanNode::new("LabelManip")
            .with_arg("kind", format!("{kind:?}"))
            .with_children(vec![describe_logical(child)]),
        LogicalPlan::Rollup { kind, child } => PlanNode::new("Rollup")
            .with_arg("kind", format!("{kind:?}"))
            .with_children(vec![describe_logical(child)]),
        LogicalPlan::Binary {
            op,
            lhs,
            rhs,
            matching,
        } => {
            let mut n = PlanNode::new("Binary").with_arg("op", format!("{op:?}"));
            if let Some(m) = matching {
                n = n.with_arg("matching", matching_to_value(m));
            }
            n.with_children(vec![describe_logical(lhs), describe_logical(rhs)])
        }
        LogicalPlan::Aggregate {
            kind,
            child,
            param,
            grouping,
        } => {
            let mut children = vec![describe_logical(child)];
            if let Some(p) = param {
                children.push(describe_logical(p));
            }
            PlanNode::new("Aggregate")
                .with_arg("kind", format!("{kind:?}"))
                .with_arg("grouping", grouping_to_value(grouping))
                .with_children(children)
        }
        LogicalPlan::Subquery {
            child,
            range_ms,
            step_ms,
            offset,
            at,
        } => {
            let mut n = PlanNode::new("Subquery")
                .with_arg("rangeMs", *range_ms)
                .with_arg("stepMs", *step_ms);
            add_offset_at(&mut n, *offset, *at);
            n.with_children(vec![describe_logical(child)])
        }
        LogicalPlan::Rechunk {
            child,
            target_step_chunk,
            target_series_chunk,
        } => PlanNode::new("Rechunk")
            .with_arg("targetStepChunk", *target_step_chunk as u64)
            .with_arg("targetSeriesChunk", *target_series_chunk as u64)
            .with_children(vec![describe_logical(child)]),
        LogicalPlan::CountValues {
            label,
            child,
            grouping,
        } => PlanNode::new("CountValues")
            .with_arg("label", label.clone())
            .with_arg("grouping", grouping_to_value(grouping))
            .with_children(vec![describe_logical(child)]),
        LogicalPlan::Concurrent {
            child,
            channel_bound,
        } => PlanNode::new("Concurrent")
            .with_arg("channelBound", *channel_bound as u64)
            .with_children(vec![describe_logical(child)]),
        LogicalPlan::Coalesce { children } => {
            PlanNode::new("Coalesce").with_children(children.iter().map(describe_logical).collect())
        }
    }
}

// ---------------------------------------------------------------------------
// describe_physical — mirror of build_node without I/O
// ---------------------------------------------------------------------------

/// Describe the physical tree that `super::physical::build_node`
/// would build for `plan`. No operators, no source, no reservation.
///
/// Decisions that depend on runtime state (e.g. `ConcurrentOp`
/// wrapping, which needs the resolved series count) are rendered as a
/// *rule* in the node's `args` rather than a concrete yes/no — the
/// paired drift test (`should_describe_physical_agrees_with_build_node`)
/// cross-checks the rule against the real plan's counts.
pub fn describe_physical(plan: &LogicalPlan, ctx: &LoweringContext) -> PlanNode {
    describe_physical_inner(plan, ctx, /*under_rollup=*/ false)
}

fn describe_physical_inner(
    plan: &LogicalPlan,
    ctx: &LoweringContext,
    under_rollup: bool,
) -> PlanNode {
    match plan {
        LogicalPlan::Scalar(v) => PlanNode::new("ConstScalarOp").with_arg("value", *v),
        LogicalPlan::Time => PlanNode::new("TimeScalarOp"),
        LogicalPlan::Scalarize { child } => PlanNode::new("ScalarizeOp")
            .with_children(vec![describe_physical_inner(child, ctx, false)]),
        LogicalPlan::Vectorize { child } => describe_physical_inner(child, ctx, false),
        LogicalPlan::VectorSelector {
            selector,
            offset,
            at,
            lookback_ms,
        } => {
            let lookback = lookback_ms.unwrap_or(ctx.lookback_delta_ms);
            let mut n = PlanNode::new("VectorSelectorOp")
                .with_arg("matcher", selector.to_string())
                .with_arg("lookbackMs", lookback);
            add_offset_at(&mut n, *offset, *at);
            maybe_wrap_concurrent_describe(n, &ctx.parallelism)
        }
        LogicalPlan::MatrixSelector {
            selector,
            range_ms,
            offset,
            at,
        } => {
            let mut n = PlanNode::new("MatrixSelectorOp")
                .with_arg("matcher", selector.to_string())
                .with_arg("rangeMs", *range_ms);
            add_offset_at(&mut n, *offset, *at);
            if !under_rollup {
                n = n.with_arg(
                    "error",
                    "MatrixSelector requires a Rollup or Subquery parent",
                );
            }
            n
        }
        LogicalPlan::InstantFn { kind, child } => PlanNode::new("InstantFnOp")
            .with_arg("kind", format!("{kind:?}"))
            .with_children(vec![describe_physical_inner(child, ctx, false)]),
        LogicalPlan::LabelManip { kind, child } => PlanNode::new("LabelManipOp")
            .with_arg("kind", format!("{kind:?}"))
            .with_children(vec![describe_physical_inner(child, ctx, false)]),
        LogicalPlan::Rollup { kind, child } => {
            let rollup = PlanNode::new("RollupOp").with_arg("kind", format!("{kind:?}"));
            match child.as_ref() {
                LogicalPlan::MatrixSelector { .. } => {
                    let matrix = describe_physical_inner(child, ctx, /*under_rollup=*/ true);
                    let wrapped = rollup.with_children(vec![matrix]);
                    maybe_wrap_concurrent_describe(wrapped, &ctx.parallelism)
                }
                LogicalPlan::Subquery {
                    child: inner,
                    range_ms,
                    step_ms,
                    offset,
                    at,
                } => {
                    let mut sub = PlanNode::new("SubqueryOp")
                        .with_arg("rangeMs", *range_ms)
                        .with_arg("stepMs", *step_ms);
                    add_offset_at(&mut sub, *offset, *at);
                    sub = sub.with_children(vec![describe_physical_inner(inner, ctx, false)]);
                    rollup.with_children(vec![sub])
                }
                other => rollup
                    .with_arg(
                        "error",
                        format!(
                            "Rollup child must be MatrixSelector or Subquery, got {}",
                            describe_logical(other).op
                        ),
                    )
                    .with_children(vec![describe_physical_inner(other, ctx, false)]),
            }
        }
        LogicalPlan::Binary {
            op,
            lhs,
            rhs,
            matching,
        } => {
            let lhs_scalar = lhs.produces_scalar();
            let rhs_scalar = rhs.produces_scalar();
            let shape = match (lhs_scalar, rhs_scalar) {
                (true, true) => "scalar_scalar",
                (true, false) => "scalar_vector",
                (false, true) => "vector_scalar",
                (false, false) => "vector_vector",
            };
            let mut n = PlanNode::new("BinaryOp")
                .with_arg("op", format!("{op:?}"))
                .with_arg("shape", shape);
            if let Some(m) = matching {
                n = n.with_arg("matching", matching_to_value(m));
            }
            n.with_children(vec![
                describe_physical_inner(lhs, ctx, false),
                describe_physical_inner(rhs, ctx, false),
            ])
        }
        LogicalPlan::Aggregate {
            kind,
            child,
            param,
            grouping,
        } => {
            let mut children = vec![describe_physical_inner(child, ctx, false)];
            if let Some(p) = param {
                children.push(describe_physical_inner(p, ctx, false));
            }
            PlanNode::new("AggregateOp")
                .with_arg("kind", format!("{kind:?}"))
                .with_arg("grouping", grouping_to_value(grouping))
                .with_children(children)
        }
        LogicalPlan::Subquery { .. } => PlanNode::new("SubqueryOp").with_arg(
            "error",
            "bare Subquery without a Rollup parent is unsupported in v1",
        ),
        LogicalPlan::Rechunk { child, .. } => PlanNode::new("Rechunk")
            .with_arg(
                "error",
                "Rechunk is not lowered by the physical planner in v1",
            )
            .with_children(vec![describe_physical_inner(child, ctx, false)]),
        LogicalPlan::CountValues {
            label,
            child,
            grouping,
        } => PlanNode::new("CountValuesOp")
            .with_arg("label", label.clone())
            .with_arg("grouping", grouping_to_value(grouping))
            .with_children(vec![describe_physical_inner(child, ctx, false)]),
        LogicalPlan::Concurrent { child, .. } => PlanNode::new("Concurrent")
            .with_arg(
                "error",
                "Concurrent is inserted by the physical planner; not a valid input",
            )
            .with_children(vec![describe_physical_inner(child, ctx, false)]),
        LogicalPlan::Coalesce { children } => PlanNode::new("Coalesce")
            .with_arg(
                "error",
                "Coalesce is inserted by the physical planner; not a valid input",
            )
            .with_children(
                children
                    .iter()
                    .map(|c| describe_physical_inner(c, ctx, false))
                    .collect(),
            ),
    }
}

/// Wrap `inner` in a `ConcurrentOp` describer node when the parallelism
/// gate would trip at runtime. Emits the *rule* rather than the
/// decision (we do not have the resolved series count at describe
/// time).
fn maybe_wrap_concurrent_describe(inner: PlanNode, parallelism: &Parallelism) -> PlanNode {
    if parallelism.concurrent_threshold_series == u64::MAX {
        return inner;
    }
    PlanNode::new("ConcurrentOp")
        .with_arg(
            "concurrentGate",
            format!(
                "series_count >= {}",
                parallelism.concurrent_threshold_series
            ),
        )
        .with_arg("channelBound", parallelism.channel_bound as u64)
        .with_children(vec![inner])
}

// ---------------------------------------------------------------------------
// pretty_print — DataFusion-style indented tree
// ---------------------------------------------------------------------------

/// Render `node` as an indented tree — each child two spaces deeper.
/// Args are rendered in brackets as `key=value` pairs, sorted by key
/// for deterministic output.
pub fn pretty_print(node: &PlanNode) -> String {
    let mut out = String::new();
    pretty_print_into(node, 0, &mut out);
    out
}

fn pretty_print_into(node: &PlanNode, depth: usize, out: &mut String) {
    for _ in 0..depth {
        out.push_str("  ");
    }
    out.push_str(&node.op);
    if !node.args.is_empty() {
        out.push_str(" [");
        let mut keys: Vec<&String> = node.args.keys().collect();
        keys.sort();
        for (i, k) in keys.iter().enumerate() {
            if i > 0 {
                out.push_str(", ");
            }
            out.push_str(k);
            out.push('=');
            out.push_str(&render_value(&node.args[k.as_str()]));
        }
        out.push(']');
    }
    out.push('\n');
    for child in &node.children {
        pretty_print_into(child, depth + 1, out);
    }
}

fn render_value(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Null => "null".to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        other => other.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn add_offset_at(node: &mut PlanNode, offset: Offset, at: Option<AtModifier>) {
    let signed = offset.signed_ms();
    if signed != 0 {
        node.args.insert("offsetMs".to_string(), signed.into());
    }
    if let Some(a) = at {
        node.args.insert("at".to_string(), at_to_value(a));
    }
}

fn at_to_value(at: AtModifier) -> serde_json::Value {
    match at {
        AtModifier::Start => serde_json::Value::String("start()".to_string()),
        AtModifier::End => serde_json::Value::String("end()".to_string()),
        AtModifier::Value(ms) => {
            let mut m = serde_json::Map::new();
            m.insert("valueMs".to_string(), ms.into());
            serde_json::Value::Object(m)
        }
    }
}

fn grouping_to_value(g: &AggregateGrouping) -> serde_json::Value {
    let (axis, labels) = match g {
        AggregateGrouping::By(labels) => ("by", labels),
        AggregateGrouping::Without(labels) => ("without", labels),
    };
    let mut m = serde_json::Map::new();
    m.insert("axis".to_string(), axis.into());
    m.insert(
        "labels".to_string(),
        labels.iter().cloned().collect::<Vec<_>>().into(),
    );
    serde_json::Value::Object(m)
}

fn matching_to_value(m: &BinaryMatching) -> serde_json::Value {
    let axis = match m.axis {
        MatchingAxis::On => "on",
        MatchingAxis::Ignoring => "ignoring",
    };
    let mut out = serde_json::Map::new();
    out.insert("axis".to_string(), axis.into());
    out.insert(
        "labels".to_string(),
        m.labels.iter().cloned().collect::<Vec<_>>().into(),
    );
    match &m.cardinality {
        Cardinality::OneToOne => {
            out.insert("cardinality".to_string(), "one_to_one".into());
        }
        Cardinality::GroupLeft { include } => {
            out.insert("cardinality".to_string(), "group_left".into());
            out.insert(
                "include".to_string(),
                include.iter().cloned().collect::<Vec<_>>().into(),
            );
        }
        Cardinality::GroupRight { include } => {
            out.insert("cardinality".to_string(), "group_right".into());
            out.insert(
                "include".to_string(),
                include.iter().cloned().collect::<Vec<_>>().into(),
            );
        }
        Cardinality::ManyToMany => {
            out.insert("cardinality".to_string(), "many_to_many".into());
        }
    }
    serde_json::Value::Object(out)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::promql::plan::{LoweringContext, lower};
    use promql_parser::parser;

    fn lower_query(q: &str, ctx: &LoweringContext) -> LogicalPlan {
        let expr = parser::parse(q).expect("parse");
        lower(&expr, ctx).expect("lower")
    }

    fn instant_ctx() -> LoweringContext {
        LoweringContext::for_instant(1_000_000, 300_000)
    }

    #[test]
    fn should_describe_vector_selector() {
        // given: a bare vector selector with a label matcher
        let plan = lower_query("foo{env=\"prod\"}", &instant_ctx());

        // when: we describe the logical plan
        let node = describe_logical(&plan);

        // then: the node names the variant, carries the matcher string and lookback
        assert_eq!(node.op, "VectorSelector");
        assert_eq!(node.args["matcher"].as_str().unwrap(), "foo{env=\"prod\"}");
        assert_eq!(node.args["lookbackMs"].as_i64().unwrap(), 300_000);
        assert!(node.children.is_empty());
    }

    #[test]
    fn should_describe_matrix_selector_under_rollup() {
        // given: rate(foo[5m]) — Rollup wrapping a MatrixSelector
        let plan = lower_query("rate(foo[5m])", &instant_ctx());

        // when: we describe the logical plan
        let node = describe_logical(&plan);

        // then: the root is Rollup with kind=Rate and a MatrixSelector child
        assert_eq!(node.op, "Rollup");
        assert_eq!(node.args["kind"].as_str().unwrap(), "Rate");
        assert_eq!(node.children.len(), 1);
        assert_eq!(node.children[0].op, "MatrixSelector");
        assert_eq!(node.children[0].args["rangeMs"].as_i64().unwrap(), 300_000);
    }

    #[test]
    fn should_describe_scalar_literal() {
        // given: a bare numeric literal
        let plan = lower_query("42", &instant_ctx());

        // when: we describe the logical plan
        let node = describe_logical(&plan);

        // then: the value is preserved verbatim
        assert_eq!(node.op, "Scalar");
        assert_eq!(node.args["value"].as_f64().unwrap(), 42.0);
    }

    #[test]
    fn should_describe_aggregate_sum_by_job() {
        // given: sum by (job)(foo) — Aggregate with By grouping
        let plan = lower_query("sum by (job)(foo)", &instant_ctx());

        // when: we describe the logical plan
        let node = describe_logical(&plan);

        // then: the aggregate carries kind and grouping, child is VectorSelector
        assert_eq!(node.op, "Aggregate");
        assert_eq!(node.args["kind"].as_str().unwrap(), "Sum");
        let grouping = &node.args["grouping"];
        assert_eq!(grouping["axis"].as_str().unwrap(), "by");
        assert_eq!(
            grouping["labels"].as_array().unwrap()[0].as_str().unwrap(),
            "job"
        );
        assert_eq!(node.children[0].op, "VectorSelector");
    }

    #[test]
    fn should_describe_aggregate_topk_with_folded_param() {
        // given: topk(5, foo) — the numeric literal param is folded into the
        // AggregateKind::Topk(k) variant by lowering, so no separate child.
        let plan = lower_query("topk(5, foo)", &instant_ctx());

        // when: we describe the logical plan
        let node = describe_logical(&plan);

        // then: kind encodes the literal k, and only the vector child remains
        assert_eq!(node.op, "Aggregate");
        assert_eq!(node.args["kind"].as_str().unwrap(), "Topk(5)");
        assert_eq!(node.children.len(), 1);
        assert_eq!(node.children[0].op, "VectorSelector");
    }

    #[test]
    fn should_describe_binary_vector_vector() {
        // given: a / on(instance) group_left b
        let plan = lower_query("a / on(instance) group_left b", &instant_ctx());

        // when: we describe the logical plan
        let node = describe_logical(&plan);

        // then: matching captures axis, labels, and cardinality=group_left
        assert_eq!(node.op, "Binary");
        let matching = &node.args["matching"];
        assert_eq!(matching["axis"].as_str().unwrap(), "on");
        assert_eq!(matching["cardinality"].as_str().unwrap(), "group_left");
        assert_eq!(
            matching["labels"].as_array().unwrap()[0].as_str().unwrap(),
            "instance"
        );
    }

    #[test]
    fn should_describe_binary_scalar_vector() {
        // given: 2 * foo (scalar on the left, vector on the right)
        let plan = lower_query("2 * foo", &instant_ctx());

        // when: we describe the physical plan
        let node = describe_physical(&plan, &instant_ctx());

        // then: the BinaryOp shape is scalar_vector (post-ConcurrentOp wrap on the RHS)
        assert_eq!(node.op, "BinaryOp");
        assert_eq!(node.args["shape"].as_str().unwrap(), "scalar_vector");
    }

    #[test]
    fn should_describe_instant_fn_abs() {
        // given: abs(foo)
        let plan = lower_query("abs(foo)", &instant_ctx());

        // when: we describe the logical plan
        let node = describe_logical(&plan);

        // then: the InstantFn carries kind=Abs and a VectorSelector child
        assert_eq!(node.op, "InstantFn");
        assert_eq!(node.args["kind"].as_str().unwrap(), "Abs");
        assert_eq!(node.children[0].op, "VectorSelector");
    }

    #[test]
    fn should_describe_subquery_under_rollup() {
        // given: avg_over_time(rate(foo[1m])[5m:1m])
        let plan = lower_query("avg_over_time(rate(foo[1m])[5m:1m])", &instant_ctx());

        // when: we describe the physical plan
        let node = describe_physical(&plan, &instant_ctx());

        // then: the Rollup physical node contains a SubqueryOp child
        assert_eq!(node.op, "RollupOp");
        let sub = node
            .children
            .iter()
            .find(|c| c.op == "SubqueryOp")
            .expect("SubqueryOp under RollupOp");
        assert_eq!(sub.args["rangeMs"].as_i64().unwrap(), 300_000);
        assert_eq!(sub.args["stepMs"].as_i64().unwrap(), 60_000);
    }

    #[test]
    fn should_describe_label_replace() {
        // given: label_replace rewrites a label — a LabelManip node
        let plan = lower_query(
            "label_replace(foo, \"dst\", \"$1\", \"src\", \"(.*)\")",
            &instant_ctx(),
        );

        // when: we describe the logical plan
        let node = describe_logical(&plan);

        // then: the node is LabelManip with kind=Replace carrying regex args
        assert_eq!(node.op, "LabelManip");
        let kind = node.args["kind"].as_str().unwrap();
        assert!(kind.starts_with("Replace"), "unexpected kind: {kind}");
        assert!(kind.contains("dst_label: \"dst\""));
    }

    #[test]
    fn should_describe_count_values() {
        // given: count_values("x", foo) with default (empty) grouping
        let plan = lower_query("count_values(\"x\", foo)", &instant_ctx());

        // when: we describe the logical plan
        let node = describe_logical(&plan);

        // then: CountValues carries the output label and grouping
        assert_eq!(node.op, "CountValues");
        assert_eq!(node.args["label"].as_str().unwrap(), "x");
    }

    #[test]
    fn should_describe_time_function() {
        // given: time() lowers to the Time leaf
        let plan = lower_query("time()", &instant_ctx());

        // when: we describe the logical plan
        let node = describe_logical(&plan);

        // then: the node is a leaf named Time with no args
        assert_eq!(node.op, "Time");
        assert!(node.args.is_empty());
        assert!(node.children.is_empty());
    }

    #[test]
    fn should_describe_scalarize_wrapper() {
        // given: scalar(foo) — wraps the child in a Scalarize
        let plan = lower_query("scalar(foo)", &instant_ctx());

        // when: we describe the logical plan
        let node = describe_logical(&plan);

        // then: Scalarize parent with a VectorSelector child
        assert_eq!(node.op, "Scalarize");
        assert_eq!(node.children[0].op, "VectorSelector");
    }

    #[test]
    fn should_describe_vectorize_wrapper() {
        // given: vector(42) — wraps a scalar as a single-series vector
        let plan = lower_query("vector(42)", &instant_ctx());

        // when: we describe the logical plan
        let node = describe_logical(&plan);

        // then: Vectorize parent with a Scalar child
        assert_eq!(node.op, "Vectorize");
        assert_eq!(node.children[0].op, "Scalar");
    }

    #[test]
    fn should_describe_physical_vectorize_as_passthrough() {
        // given: vector(42) — physical planner's Vectorize is a no-op shim
        let plan = lower_query("vector(42)", &instant_ctx());

        // when: we describe the physical plan
        let node = describe_physical(&plan, &instant_ctx());

        // then: only the child is visible — no Vectorize wrapper
        assert_eq!(node.op, "ConstScalarOp");
    }

    #[test]
    fn should_describe_physical_vector_selector_wrapped_in_concurrent() {
        // given: a plain vector selector with default parallelism
        let plan = lower_query("foo", &instant_ctx());

        // when: we describe the physical plan
        let node = describe_physical(&plan, &instant_ctx());

        // then: ConcurrentOp wraps the VectorSelectorOp with the gating rule
        assert_eq!(node.op, "ConcurrentOp");
        assert!(
            node.args["concurrentGate"]
                .as_str()
                .unwrap()
                .contains("series_count >=")
        );
        assert_eq!(node.children[0].op, "VectorSelectorOp");
    }

    #[test]
    fn should_describe_physical_skips_concurrent_when_gate_disabled() {
        // given: parallelism with threshold = u64::MAX (gate disabled)
        let mut ctx = instant_ctx();
        ctx.parallelism.concurrent_threshold_series = u64::MAX;
        let plan = lower_query("foo", &ctx);

        // when: we describe the physical plan
        let node = describe_physical(&plan, &ctx);

        // then: no ConcurrentOp wrap
        assert_eq!(node.op, "VectorSelectorOp");
    }

    #[test]
    fn should_describe_physical_rollup_over_matrix() {
        // given: rate(foo[5m]) — RollupOp wrapping MatrixSelectorOp + ConcurrentOp
        let plan = lower_query("rate(foo[5m])", &instant_ctx());

        // when: we describe the physical plan
        let node = describe_physical(&plan, &instant_ctx());

        // then: ConcurrentOp → RollupOp → MatrixSelectorOp
        assert_eq!(node.op, "ConcurrentOp");
        assert_eq!(node.children[0].op, "RollupOp");
        assert_eq!(node.children[0].children[0].op, "MatrixSelectorOp");
    }

    #[test]
    fn should_pretty_print_with_indentation() {
        // given: a Rollup tree with nested nodes + args
        let node = PlanNode::new("AggregateOp")
            .with_arg("kind", "Sum")
            .with_children(vec![
                PlanNode::new("VectorSelectorOp").with_arg("matcher", "foo"),
            ]);

        // when: we pretty-print
        let out = pretty_print(&node);

        // then: children indent two spaces; args sorted in brackets
        assert_eq!(
            out,
            "AggregateOp [kind=Sum]\n  VectorSelectorOp [matcher=foo]\n"
        );
    }

    // ---- golden pretty-print tests ---------------------------------------
    //
    // These tests snapshot the full text rendering across the three
    // stages for representative queries. If the planner shape changes,
    // the diff here is the first place you see it.

    fn golden(query: &str, ctx: &LoweringContext) -> String {
        let expr = parser::parse(query).expect("parse");
        let unoptimized = lower(&expr, ctx).expect("lower");
        let optimized = crate::promql::plan::optimize(unoptimized.clone());
        let u_node = describe_logical(&unoptimized);
        let o_node = describe_logical(&optimized);
        let p_node = describe_physical(&optimized, ctx);
        let mut out = String::new();
        out.push_str("=== logical (unoptimized) ===\n");
        out.push_str(&pretty_print(&u_node));
        out.push_str("=== logical (optimized) ===\n");
        out.push_str(&pretty_print(&o_node));
        out.push_str("=== physical ===\n");
        out.push_str(&pretty_print(&p_node));
        out
    }

    #[test]
    fn should_golden_print_bare_selector() {
        // given: a plain vector selector
        let out = golden("foo", &instant_ctx());

        // when/then: three stages including the ConcurrentOp wrap around the leaf
        let expected = "\
=== logical (unoptimized) ===
VectorSelector [lookbackMs=300000, matcher=foo]
=== logical (optimized) ===
VectorSelector [lookbackMs=300000, matcher=foo]
=== physical ===
ConcurrentOp [channelBound=4, concurrentGate=series_count >= 64]
  VectorSelectorOp [lookbackMs=300000, matcher=foo]
";
        assert_eq!(out, expected);
    }

    #[test]
    fn should_golden_print_rate_matrix() {
        // given: rate(foo[5m]) — Rollup over MatrixSelector
        let out = golden("rate(foo[5m])", &instant_ctx());

        // when/then: Rollup logical, ConcurrentOp-wrapped RollupOp physical
        let expected = "\
=== logical (unoptimized) ===
Rollup [kind=Rate]
  MatrixSelector [matcher=foo, rangeMs=300000]
=== logical (optimized) ===
Rollup [kind=Rate]
  MatrixSelector [matcher=foo, rangeMs=300000]
=== physical ===
ConcurrentOp [channelBound=4, concurrentGate=series_count >= 64]
  RollupOp [kind=Rate]
    MatrixSelectorOp [matcher=foo, rangeMs=300000]
";
        assert_eq!(out, expected);
    }

    #[test]
    fn should_golden_print_sum_by_aggregate() {
        // given: sum by (job)(foo)
        let out = golden("sum by (job)(foo)", &instant_ctx());

        // when/then: AggregateOp parent, ConcurrentOp+VectorSelectorOp leaf
        let expected = "\
=== logical (unoptimized) ===
Aggregate [grouping={\"axis\":\"by\",\"labels\":[\"job\"]}, kind=Sum]
  VectorSelector [lookbackMs=300000, matcher=foo]
=== logical (optimized) ===
Aggregate [grouping={\"axis\":\"by\",\"labels\":[\"job\"]}, kind=Sum]
  VectorSelector [lookbackMs=300000, matcher=foo]
=== physical ===
AggregateOp [grouping={\"axis\":\"by\",\"labels\":[\"job\"]}, kind=Sum]
  ConcurrentOp [channelBound=4, concurrentGate=series_count >= 64]
    VectorSelectorOp [lookbackMs=300000, matcher=foo]
";
        assert_eq!(out, expected);
    }

    #[test]
    fn should_golden_print_binary_group_left() {
        // given: a / on(instance) group_left b
        let out = golden("a / on(instance) group_left b", &instant_ctx());

        // when/then: Binary(Div) with vector_vector shape and matching info
        let expected = "\
=== logical (unoptimized) ===
Binary [matching={\"axis\":\"on\",\"cardinality\":\"group_left\",\"include\":[],\"labels\":[\"instance\"]}, op=Div]
  VectorSelector [lookbackMs=300000, matcher=a]
  VectorSelector [lookbackMs=300000, matcher=b]
=== logical (optimized) ===
Binary [matching={\"axis\":\"on\",\"cardinality\":\"group_left\",\"include\":[],\"labels\":[\"instance\"]}, op=Div]
  VectorSelector [lookbackMs=300000, matcher=a]
  VectorSelector [lookbackMs=300000, matcher=b]
=== physical ===
BinaryOp [matching={\"axis\":\"on\",\"cardinality\":\"group_left\",\"include\":[],\"labels\":[\"instance\"]}, op=Div, shape=vector_vector]
  ConcurrentOp [channelBound=4, concurrentGate=series_count >= 64]
    VectorSelectorOp [lookbackMs=300000, matcher=a]
  ConcurrentOp [channelBound=4, concurrentGate=series_count >= 64]
    VectorSelectorOp [lookbackMs=300000, matcher=b]
";
        assert_eq!(out, expected);
    }

    #[test]
    fn should_golden_print_subquery_avg_over_time() {
        // given: avg_over_time(rate(foo[1m])[5m:1m]) — subquery under a rollup
        let out = golden("avg_over_time(rate(foo[1m])[5m:1m])", &instant_ctx());

        // when/then: RollupOp(AvgOverTime) → SubqueryOp → RollupOp(Rate) →
        // MatrixSelectorOp (wrapped at the matrix leaf)
        let expected = "\
=== logical (unoptimized) ===
Rollup [kind=AvgOverTime]
  Subquery [rangeMs=300000, stepMs=60000]
    Rollup [kind=Rate]
      MatrixSelector [matcher=foo, rangeMs=60000]
=== logical (optimized) ===
Rollup [kind=AvgOverTime]
  Subquery [rangeMs=300000, stepMs=60000]
    Rollup [kind=Rate]
      MatrixSelector [matcher=foo, rangeMs=60000]
=== physical ===
RollupOp [kind=AvgOverTime]
  SubqueryOp [rangeMs=300000, stepMs=60000]
    ConcurrentOp [channelBound=4, concurrentGate=series_count >= 64]
      RollupOp [kind=Rate]
        MatrixSelectorOp [matcher=foo, rangeMs=60000]
";
        assert_eq!(out, expected);
    }

    #[test]
    fn should_golden_print_topk_via_aggregate() {
        // given: topk(3, rate(foo[1m])) — the literal param folds into AggregateKind::Topk(3)
        let out = golden("topk(3, rate(foo[1m]))", &instant_ctx());

        // when/then: single-child Aggregate with Topk(3) kind
        let expected = "\
=== logical (unoptimized) ===
Aggregate [grouping={\"axis\":\"by\",\"labels\":[]}, kind=Topk(3)]
  Rollup [kind=Rate]
    MatrixSelector [matcher=foo, rangeMs=60000]
=== logical (optimized) ===
Aggregate [grouping={\"axis\":\"by\",\"labels\":[]}, kind=Topk(3)]
  Rollup [kind=Rate]
    MatrixSelector [matcher=foo, rangeMs=60000]
=== physical ===
AggregateOp [grouping={\"axis\":\"by\",\"labels\":[]}, kind=Topk(3)]
  ConcurrentOp [channelBound=4, concurrentGate=series_count >= 64]
    RollupOp [kind=Rate]
      MatrixSelectorOp [matcher=foo, rangeMs=60000]
";
        assert_eq!(out, expected);
    }

    #[test]
    fn should_emit_schema_version_one() {
        // given: an ExplainResult with trivial nodes
        let result = ExplainResult {
            schema_version: SCHEMA_VERSION,
            logical_unoptimized: PlanNode::new("Scalar"),
            logical_optimized: PlanNode::new("Scalar"),
            physical: PlanNode::new("ConstScalarOp"),
        };

        // when: we round-trip through JSON
        let json = serde_json::to_value(&result).unwrap();

        // then: schemaVersion is 1 and camelCase keys are preserved
        assert_eq!(json["schemaVersion"].as_u64().unwrap(), 1);
        assert!(json.get("logicalUnoptimized").is_some());
        assert!(json.get("logicalOptimized").is_some());
        assert!(json.get("physical").is_some());
    }
}
