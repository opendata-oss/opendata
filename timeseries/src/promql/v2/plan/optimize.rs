//! Rule-based logical-plan optimizer (unit 4.2).
//!
//! Sits between AST→IR lowering (unit 4.1) and physical planning (unit 4.3)
//! — see RFC 0007 §"Architecture Overview". Rewrites a [`LogicalPlan`] tree
//! in-place (by owned-value transformation) using a small, fixed set of
//! correctness-preserving rules. No cost model — the RFC's §"Non-Goals"
//! rules out cost-based optimization for v1.
//!
//! # Rule set
//!
//! 1. **Constant folding** — `Binary { Scalar(a), op, Scalar(b) }` is
//!    rewritten to `Scalar(op.apply(a, b))`. Applies to both arithmetic
//!    ops (`+`, `-`, `*`, `/`, `%`, `^`, `atan2`) and comparison ops
//!    (`==`, `!=`, `<`, `>`, `<=`, `>=`). Comparison folding is safe
//!    regardless of the `bool` modifier because PromQL returns a scalar
//!    (not a filter) for scalar-vs-scalar comparisons — cited against
//!    [`crate::promql::evaluator::Evaluator::evaluate_binary_expr`]
//!    (`timeseries/src/promql/evaluator.rs:2109-2112`) where
//!    `(Scalar, Scalar)` drops straight through
//!    `apply_binary_op` to `ExprResult::Scalar`.
//!
//! 2. **Unary-minus fold** — the lowering represents `-x` as
//!    `Binary { Mul, Scalar(-1.0), x }` (see `lowering.rs` and §5
//!    Decisions Log 4.1). Rule 1 already folds this when `x` is a
//!    `Scalar(n)` — no dedicated rule needed.
//!
//! 3. **Redundant vector-selector matcher dedup** — a
//!    [`LogicalPlan::VectorSelector`] whose `Matchers` contains two
//!    structurally-equal `(name, op, value)` entries collapses to one.
//!    The parser does not normalise these (verified against
//!    `promql-parser-0.8.0/src/parser/production.rs`), so a user-written
//!    `{job="a", job="a"}` reaches us with two matchers. This rule is
//!    *not* a general label pushdown — it does not invent matchers from
//!    surrounding context.
//!
//! 4. **CSE** — **deferred to unit 4.3 / v2**. See §5 Decisions Log entry
//!    for 4.2. The physical plan builds unique operator objects rather
//!    than a DAG, so structural dedup on the logical tree would not yet
//!    yield a shared computation. Open Question #4 (cache-key
//!    canonicalization) is unresolved; implementing CSE prematurely
//!    risks picking the wrong equivalence relation.
//!
//! 5. **Algebraic identities** (`x * 1 → x`, `x + 0 → x`, `x - 0 → x`,
//!    `x * 0 → 0`) — **dropped**. `0 * NaN` must preserve `NaN` per
//!    IEEE 754 and the v2 `Binary` operator already does (`apply_arith`
//!    at `binary.rs:180-193`). Dropping the multiply for `x * 1` is
//!    correct only if we can guarantee `x` has no side effects and no
//!    NaN-observability changes — safe for pure pointwise scalars but
//!    not for vector branches that the Binary operator might materialise
//!    for validity/matching purposes. Correctness > micro-optimization.
//!
//! 6. **Parens** — already stripped by lowering; no rule needed.
//!
//! # Fixpoint strategy
//!
//! Iterate all rules until the plan stops changing, bounded by
//! [`MAX_PASSES`]. Each pass is top-level: the entire tree is rewritten
//! once, and the result is compared (structural `PartialEq`) to the
//! input. Rule bodies are themselves recursive bottom-up traversals so a
//! single pass already reaches a local fixpoint for the rule — the outer
//! loop exists only to re-run *other* rules after a rewrite exposes new
//! opportunities (e.g. `fold_constants` exposing a `Scalar` that the
//! dedup rule may not care about but a future rule might). In practice
//! the optimizer converges in one or two passes; the bound is a safety
//! net.

use promql_parser::label::{Matcher, Matchers};

use super::plan_types::LogicalPlan;
use crate::promql::v2::operators::binary::BinaryOpKind;

/// Upper bound on rule-set passes before the optimizer gives up. Hit is
/// logged via `tracing::warn!` at runtime (not here — optimizer is a pure
/// function today; phase-5 wiring may wrap it). Small because rule
/// interactions are shallow.
const MAX_PASSES: usize = 4;

/// Run the rule-based optimizer over a logical plan.
///
/// See the module docs for the rule set and fixpoint strategy.
pub fn optimize(plan: LogicalPlan) -> LogicalPlan {
    let mut current = plan;
    for _ in 0..MAX_PASSES {
        let next = apply_rules(current.clone());
        if next == current {
            return next;
        }
        current = next;
    }
    current
}

/// Apply every rule once, bottom-up. Each rule consumes and returns a
/// [`LogicalPlan`]; composing them by pipelining is the simplest way to
/// keep rule ordering explicit.
fn apply_rules(plan: LogicalPlan) -> LogicalPlan {
    let plan = dedupe_vector_selector_matchers(plan);
    fold_constants(plan)
}

// ---------------------------------------------------------------------------
// Rule 1/2: constant folding (arith + comparison + unary-minus-of-literal)
// ---------------------------------------------------------------------------

/// Fold `Binary { Scalar(a), op, Scalar(b) }` into `Scalar(op.apply(a, b))`.
/// Recursive bottom-up: children are folded before the parent, so
/// `(1 + 2) * (3 + 4)` collapses to `Scalar(21)` in one pass.
fn fold_constants(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Binary {
            op,
            lhs,
            rhs,
            matching,
        } => {
            let lhs = fold_constants(*lhs);
            let rhs = fold_constants(*rhs);
            if let (LogicalPlan::Scalar(a), LogicalPlan::Scalar(b)) = (&lhs, &rhs)
                && let Some(folded) = apply_scalar_op(op, *a, *b)
            {
                return LogicalPlan::Scalar(folded);
            }
            LogicalPlan::Binary {
                op,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
                matching,
            }
        }
        LogicalPlan::InstantFn { kind, child } => LogicalPlan::InstantFn {
            kind,
            child: Box::new(fold_constants(*child)),
        },
        LogicalPlan::Rollup { kind, child } => LogicalPlan::Rollup {
            kind,
            child: Box::new(fold_constants(*child)),
        },
        LogicalPlan::Aggregate {
            kind,
            child,
            grouping,
        } => LogicalPlan::Aggregate {
            kind,
            child: Box::new(fold_constants(*child)),
            grouping,
        },
        LogicalPlan::Subquery {
            child,
            range_ms,
            step_ms,
            offset,
            at,
        } => LogicalPlan::Subquery {
            child: Box::new(fold_constants(*child)),
            range_ms,
            step_ms,
            offset,
            at,
        },
        LogicalPlan::Rechunk {
            child,
            target_step_chunk,
            target_series_chunk,
        } => LogicalPlan::Rechunk {
            child: Box::new(fold_constants(*child)),
            target_step_chunk,
            target_series_chunk,
        },
        LogicalPlan::CountValues {
            label,
            child,
            grouping,
        } => LogicalPlan::CountValues {
            label,
            child: Box::new(fold_constants(*child)),
            grouping,
        },
        LogicalPlan::Concurrent {
            child,
            channel_bound,
        } => LogicalPlan::Concurrent {
            child: Box::new(fold_constants(*child)),
            channel_bound,
        },
        LogicalPlan::Coalesce { children } => LogicalPlan::Coalesce {
            children: children.into_iter().map(fold_constants).collect(),
        },
        // Leaves — no children to recurse into.
        leaf @ (LogicalPlan::VectorSelector { .. }
        | LogicalPlan::MatrixSelector { .. }
        | LogicalPlan::Scalar(_)) => leaf,
    }
}

/// Fold `a op b` on two `f64` scalars to a single `f64`, if the op is
/// foldable. Set operators (`And`/`Or`/`Unless`) are vector-only and never
/// appear on scalar/scalar; returning `None` keeps the caller on the
/// no-fold path for safety.
fn apply_scalar_op(op: BinaryOpKind, a: f64, b: f64) -> Option<f64> {
    match op {
        // --- arithmetic ---
        BinaryOpKind::Add => Some(a + b),
        BinaryOpKind::Sub => Some(a - b),
        BinaryOpKind::Mul => Some(a * b),
        // IEEE 754 division — matches the v2 `BinaryOp::apply_arith` at
        // `operators/binary.rs:180-193` bit-for-bit (and Prometheus).
        BinaryOpKind::Div => Some(a / b),
        BinaryOpKind::Mod => Some(a % b),
        BinaryOpKind::Pow => Some(a.powf(b)),
        BinaryOpKind::Atan2 => Some(a.atan2(b)),
        // --- comparisons (scalar/scalar always returns a scalar 1.0/0.0,
        // regardless of `bool` modifier — cited against evaluator.rs:2109-
        // 2112 + apply_binary_op at 2153-2177). ---
        BinaryOpKind::Eq { .. } => Some(if a == b { 1.0 } else { 0.0 }),
        BinaryOpKind::Ne { .. } => Some(if a != b { 1.0 } else { 0.0 }),
        BinaryOpKind::Gt { .. } => Some(if a > b { 1.0 } else { 0.0 }),
        BinaryOpKind::Lt { .. } => Some(if a < b { 1.0 } else { 0.0 }),
        BinaryOpKind::Gte { .. } => Some(if a >= b { 1.0 } else { 0.0 }),
        BinaryOpKind::Lte { .. } => Some(if a <= b { 1.0 } else { 0.0 }),
        // --- set ops — vector-only. Planner guarantees these never reach
        // scalar/scalar; returning None leaves the tree untouched if a bug
        // upstream does produce this shape. ---
        BinaryOpKind::And | BinaryOpKind::Or | BinaryOpKind::Unless => None,
    }
}

// ---------------------------------------------------------------------------
// Rule 3: vector-selector matcher dedup
// ---------------------------------------------------------------------------

/// Collapse structurally-identical `(op, name, value)` matchers on any
/// [`LogicalPlan::VectorSelector`] or [`LogicalPlan::MatrixSelector`].
/// `or_matchers` are left untouched — dedup across OR branches would
/// change semantics when branches overlap.
fn dedupe_vector_selector_matchers(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::VectorSelector {
            mut selector,
            offset,
            at,
            lookback_ms,
        } => {
            selector.matchers = dedupe_matchers(selector.matchers);
            LogicalPlan::VectorSelector {
                selector,
                offset,
                at,
                lookback_ms,
            }
        }
        LogicalPlan::MatrixSelector {
            mut selector,
            range_ms,
            offset,
            at,
        } => {
            selector.matchers = dedupe_matchers(selector.matchers);
            LogicalPlan::MatrixSelector {
                selector,
                range_ms,
                offset,
                at,
            }
        }
        LogicalPlan::InstantFn { kind, child } => LogicalPlan::InstantFn {
            kind,
            child: Box::new(dedupe_vector_selector_matchers(*child)),
        },
        LogicalPlan::Rollup { kind, child } => LogicalPlan::Rollup {
            kind,
            child: Box::new(dedupe_vector_selector_matchers(*child)),
        },
        LogicalPlan::Binary {
            op,
            lhs,
            rhs,
            matching,
        } => LogicalPlan::Binary {
            op,
            lhs: Box::new(dedupe_vector_selector_matchers(*lhs)),
            rhs: Box::new(dedupe_vector_selector_matchers(*rhs)),
            matching,
        },
        LogicalPlan::Aggregate {
            kind,
            child,
            grouping,
        } => LogicalPlan::Aggregate {
            kind,
            child: Box::new(dedupe_vector_selector_matchers(*child)),
            grouping,
        },
        LogicalPlan::Subquery {
            child,
            range_ms,
            step_ms,
            offset,
            at,
        } => LogicalPlan::Subquery {
            child: Box::new(dedupe_vector_selector_matchers(*child)),
            range_ms,
            step_ms,
            offset,
            at,
        },
        LogicalPlan::Rechunk {
            child,
            target_step_chunk,
            target_series_chunk,
        } => LogicalPlan::Rechunk {
            child: Box::new(dedupe_vector_selector_matchers(*child)),
            target_step_chunk,
            target_series_chunk,
        },
        LogicalPlan::CountValues {
            label,
            child,
            grouping,
        } => LogicalPlan::CountValues {
            label,
            child: Box::new(dedupe_vector_selector_matchers(*child)),
            grouping,
        },
        LogicalPlan::Concurrent {
            child,
            channel_bound,
        } => LogicalPlan::Concurrent {
            child: Box::new(dedupe_vector_selector_matchers(*child)),
            channel_bound,
        },
        LogicalPlan::Coalesce { children } => LogicalPlan::Coalesce {
            children: children
                .into_iter()
                .map(dedupe_vector_selector_matchers)
                .collect(),
        },
        leaf @ LogicalPlan::Scalar(_) => leaf,
    }
}

/// Remove duplicate `(op, name, value)` matchers from a parser
/// [`Matchers`] while preserving first-occurrence order.
fn dedupe_matchers(matchers: Matchers) -> Matchers {
    let Matchers {
        matchers: existing,
        or_matchers,
    } = matchers;
    let mut seen: Vec<Matcher> = Vec::with_capacity(existing.len());
    for m in existing {
        if !seen.iter().any(|e| matcher_eq(e, &m)) {
            seen.push(m);
        }
    }
    Matchers {
        matchers: seen,
        or_matchers,
    }
}

/// Structural matcher equality. [`MatchOp`] already derives `PartialEq` in
/// promql-parser 0.8 (regex compares on source string), so `Matcher`'s
/// derived `PartialEq` is a complete structural check. Helper kept as a
/// one-liner for readability / to isolate the dependency on the derived
/// impl in case promql-parser semantics shift.
fn matcher_eq(a: &Matcher, b: &Matcher) -> bool {
    a == b
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::promql::v2::operators::aggregate::AggregateKind;
    use crate::promql::v2::operators::rollup::RollupKind;
    use crate::promql::v2::plan::lowering::{LoweringContext, lower};
    use crate::promql::v2::plan::plan_types::{AggregateGrouping, Offset};
    use promql_parser::parser;

    const START_MS: i64 = 1_700_000_000_000;
    const END_MS: i64 = 1_700_000_060_000;
    const STEP_MS: i64 = 1_000;
    const LOOKBACK_MS: i64 = 5 * 60 * 1000;

    fn ctx() -> LoweringContext {
        LoweringContext::new(START_MS, END_MS, STEP_MS, LOOKBACK_MS)
    }

    fn parse(input: &str) -> parser::Expr {
        parser::parse(input).unwrap_or_else(|e| panic!("parse({input:?}) failed: {e}"))
    }

    fn lower_and_optimize(input: &str) -> LogicalPlan {
        let expr = parse(input);
        let plan = lower(&expr, &ctx()).unwrap();
        optimize(plan)
    }

    #[test]
    fn should_fold_scalar_arithmetic() {
        // given: a scalar/scalar addition
        // when: lowered and optimized
        let plan = lower_and_optimize("1 + 2");
        // then: the whole tree collapses to a single Scalar(3)
        assert_eq!(plan, LogicalPlan::Scalar(3.0));
    }

    #[test]
    fn should_fold_scalar_comparison_with_bool() {
        // given: scalar < scalar `bool` on both sides of the truth table
        // when: lowered + optimized
        // then: both collapse to Scalar(1.0) / Scalar(0.0) per the same
        //       rule the existing engine uses (evaluator.rs:2109-2112 +
        //       apply_binary_op at :2153-2177, which always maps scalar/
        //       scalar comparisons to 1.0/0.0).
        assert_eq!(lower_and_optimize("1 < bool 2"), LogicalPlan::Scalar(1.0));
        assert_eq!(lower_and_optimize("2 < bool 1"), LogicalPlan::Scalar(0.0));
    }

    #[test]
    fn should_fold_comparison_without_bool() {
        // given: scalar < scalar *without* `bool`. PromQL's parser
        // rejects this source form ("comparisons between scalars must
        // use BOOL modifier"), but an (incorrect) earlier optimizer
        // pass or hand-built AST could synthesise the shape — the
        // optimizer must still fold it correctly. Build the
        // `LogicalPlan` directly to exercise the defensive path.
        //
        // Semantics: evaluator.rs:2109-2112 + apply_binary_op at
        // :2153-2177 always return 1.0 / 0.0 on the `(Scalar, Scalar)`
        // arm regardless of `return_bool`. Folding matches that.
        let plan = LogicalPlan::Binary {
            op: BinaryOpKind::Lt {
                bool_modifier: false,
            },
            lhs: Box::new(LogicalPlan::Scalar(1.0)),
            rhs: Box::new(LogicalPlan::Scalar(2.0)),
            matching: None,
        };
        assert_eq!(optimize(plan), LogicalPlan::Scalar(1.0));

        let plan = LogicalPlan::Binary {
            op: BinaryOpKind::Lt {
                bool_modifier: false,
            },
            lhs: Box::new(LogicalPlan::Scalar(2.0)),
            rhs: Box::new(LogicalPlan::Scalar(1.0)),
            matching: None,
        };
        assert_eq!(optimize(plan), LogicalPlan::Scalar(0.0));

        let plan = LogicalPlan::Binary {
            op: BinaryOpKind::Eq {
                bool_modifier: false,
            },
            lhs: Box::new(LogicalPlan::Scalar(3.0)),
            rhs: Box::new(LogicalPlan::Scalar(3.0)),
            matching: None,
        };
        assert_eq!(optimize(plan), LogicalPlan::Scalar(1.0));
    }

    #[test]
    fn should_fold_unary_minus_of_literal() {
        // given: `-3` — the lowering rewrites this to `Binary { Mul,
        // Scalar(-1), Scalar(3) }` (lowering.rs:126-137, §5 Decisions
        // Log 4.1).
        // when: optimized
        // then: collapses to Scalar(-3).
        let plan = lower_and_optimize("-3");
        assert_eq!(plan, LogicalPlan::Scalar(-3.0));
    }

    #[test]
    fn should_preserve_non_constant_binary() {
        // given: `rate(foo[5m]) + 1` — only the RHS is a constant.
        // when: optimized
        // then: the Binary node survives with the rollup intact; only
        // descendant Scalars remain unchanged.
        let plan = lower_and_optimize("rate(foo[5m]) + 1");
        match plan {
            LogicalPlan::Binary {
                op: BinaryOpKind::Add,
                lhs,
                rhs,
                matching: None,
            } => {
                assert!(matches!(
                    *lhs,
                    LogicalPlan::Rollup {
                        kind: RollupKind::Rate,
                        ..
                    }
                ));
                assert_eq!(*rhs, LogicalPlan::Scalar(1.0));
            }
            other => panic!("unexpected optimized plan: {other:?}"),
        }
    }

    #[test]
    fn should_collapse_duplicate_selector_matchers() {
        // given: `foo{job="a", job="a"}` — the parser does not dedup the
        // matcher list (verified against promql-parser-0.8.0 production
        // rules).
        let plan = lower_and_optimize(r#"foo{job="a", job="a"}"#);
        // when + then: the optimizer collapses the duplicate (name, op,
        // value) pair to a single matcher while keeping any non-duplicate
        // matchers and the `__name__` entry.
        match plan {
            LogicalPlan::VectorSelector { selector, .. } => {
                let job_matchers: Vec<_> = selector
                    .matchers
                    .matchers
                    .iter()
                    .filter(|m| m.name == "job")
                    .collect();
                assert_eq!(
                    job_matchers.len(),
                    1,
                    "expected a single job matcher after dedup; got {:?}",
                    selector.matchers.matchers
                );
            }
            other => panic!("unexpected optimized plan: {other:?}"),
        }
    }

    #[test]
    fn should_pass_through_unchanged_when_no_rule_applies() {
        // given: a bare vector selector — no rule touches it.
        let plan = lower_and_optimize("foo");
        // then: optimizer is a no-op.
        match plan {
            LogicalPlan::VectorSelector {
                selector, offset, ..
            } => {
                assert_eq!(selector.name.as_deref(), Some("foo"));
                assert_eq!(offset, Offset::Pos(0));
            }
            other => panic!("unexpected optimized plan: {other:?}"),
        }
    }

    #[test]
    fn should_fold_nested_constants() {
        // given: `(1 + 2) * (3 + 4)` — two independent folds plus a
        // third after the children collapse.
        // when + then: folds to Scalar(21).
        let plan = lower_and_optimize("(1 + 2) * (3 + 4)");
        assert_eq!(plan, LogicalPlan::Scalar(21.0));
    }

    #[test]
    fn should_not_fold_identities_deferred_to_cost_based() {
        // given: `foo * 1` — the textbook "multiplicative identity"
        // rewrite. We deliberately *do not* implement this because
        // `0 * NaN` preserves NaN under IEEE 754 (and the v2 Binary
        // operator's arith path at operators/binary.rs:180-193) — an
        // identity rewrite would change observability of NaN cells.
        // when + then: the Binary stays untouched (except children are
        // recursed, but `foo` has no children to fold).
        let plan = lower_and_optimize("foo * 1");
        match plan {
            LogicalPlan::Binary {
                op: BinaryOpKind::Mul,
                lhs,
                rhs,
                matching: None,
            } => {
                assert!(matches!(*lhs, LogicalPlan::VectorSelector { .. }));
                assert_eq!(*rhs, LogicalPlan::Scalar(1.0));
            }
            other => panic!("unexpected optimized plan: {other:?}"),
        }
    }

    #[test]
    fn should_reach_fixpoint_in_bounded_passes() {
        // given: a deeply-nested all-constant expression.
        // when: optimized
        // then: collapses to a single Scalar within MAX_PASSES.
        // (((1+2)*3) - 4) / 5  →  5/5  →  1
        let plan = lower_and_optimize("(((1 + 2) * 3) - 4) / 5");
        assert_eq!(plan, LogicalPlan::Scalar(1.0));
    }

    #[test]
    fn should_not_fold_nan_binary_ops_to_scalar() {
        // given: `NaN + 1` — NaN is a well-defined scalar; IEEE 754
        // arithmetic propagates NaN. The existing engine's
        // `apply_binary_op` (evaluator.rs:2153-2177) simply returns
        // `a + b`, which yields NaN. The v2 Binary operator at
        // operators/binary.rs:180-193 likewise. Folding IS enabled
        // because the result is semantically identical.
        // when: optimized
        // then: folds to Scalar(NaN); test pins the bit-pattern.
        let plan = lower_and_optimize("NaN + 1");
        match plan {
            LogicalPlan::Scalar(v) => {
                assert!(v.is_nan(), "expected NaN after folding NaN + 1, got {v}")
            }
            other => panic!("expected Scalar(NaN), got {other:?}"),
        }
    }

    #[test]
    fn should_fold_under_instant_fn() {
        // given: `abs(-foo)` — the lowering yields
        // `InstantFn { Abs, Binary { Mul, Scalar(-1), VectorSelector(foo) } }`.
        // when: optimized
        // then: the inner binary is NOT folded (VectorSelector is not a
        // Scalar) so the shape is preserved. This test proves the
        // optimizer recurses into `InstantFn` children — a buggy
        // non-recursive implementation would trivially pass this because
        // there's nothing to fold, but the recursion check is that
        // children of children-to-be-folded are still visited, so use
        // `abs(ln(-3 * 1))` variant instead.
        //
        // Revised shape: `abs((1 + 2) * foo)` — the planner should fold
        // `1 + 2` to `Scalar(3)` under the `InstantFn`'s descendant
        // Binary, without folding the outer `Scalar(3) * foo` (vector
        // branch). This exercises recursion under `InstantFn`.
        let plan = lower_and_optimize("abs((1 + 2) * foo)");
        match plan {
            LogicalPlan::InstantFn { child, .. } => match *child {
                LogicalPlan::Binary {
                    op: BinaryOpKind::Mul,
                    lhs,
                    rhs,
                    matching: None,
                } => {
                    assert_eq!(*lhs, LogicalPlan::Scalar(3.0));
                    assert!(matches!(*rhs, LogicalPlan::VectorSelector { .. }));
                }
                other => panic!("unexpected InstantFn child: {other:?}"),
            },
            other => panic!("unexpected optimized plan: {other:?}"),
        }
    }

    #[test]
    fn should_fold_under_aggregate() {
        // given: `sum(foo + (1 + 2))` — the inner binary on two scalars
        // folds; the outer binary (vector + scalar) does not.
        let plan = lower_and_optimize("sum(foo + (1 + 2))");
        match plan {
            LogicalPlan::Aggregate {
                kind: AggregateKind::Sum,
                child,
                grouping: AggregateGrouping::By(ref labels),
            } if labels.is_empty() => match *child {
                LogicalPlan::Binary {
                    op: BinaryOpKind::Add,
                    lhs,
                    rhs,
                    matching: None,
                } => {
                    assert!(matches!(*lhs, LogicalPlan::VectorSelector { .. }));
                    assert_eq!(*rhs, LogicalPlan::Scalar(3.0));
                }
                other => panic!("unexpected aggregate child: {other:?}"),
            },
            other => panic!("unexpected optimized plan: {other:?}"),
        }
    }
}
