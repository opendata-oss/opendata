# RFC 0005: Generalized PromQL Function Signatures

**Status**: Draft

**Authors**:
- [Samson Hailu](https://github.com/samsond)

## Summary

This RFC proposes a generalized PromQL function system for `opendata-timeseries`.
Today, function execution is effectively constrained to single-argument calls and
special-cased function categories. This prevents support for many valid PromQL
functions that require multiple arguments, optional arguments, variadic
arguments, string arguments, and scalar return values.

The proposal introduces a typed function argument model and a unified function
execution interface that can evaluate functions across argument shapes while
preserving PromQL semantics.

## Motivation

OpenData-timeseries aims to be Prometheus-compatible. Limiting function handling
to single-argument forms blocks common production queries such as:

- `round(v, to_nearest)`
- `clamp(v, min, max)`
- `label_replace(v, dst, replacement, src, regex)`
- `histogram_quantile(phi, b)`
- `time()`, `pi()` (zero-arg scalar-returning functions)

Current implementation constraints are visible in the PromQL evaluator and
function registry:

- Function calls are rejected unless they have exactly one argument.
- Function dispatch is split by ad hoc instant/range categories rather than
  typed signatures.
- Scalar-return semantics are not modeled consistently through function
  execution.

Without generalization, PromQL support growth becomes brittle and repetitive.

## Goals

- Support function signatures with multiple arguments.
- Support mixed argument types (`vector`, `matrix`, `scalar`, `string`).
- Support optional arguments and variadic arguments.
- Support zero-argument calls.
- Support scalar and vector return types.
- Align runtime function handling with Prometheus parser/type metadata.
- Keep migration incremental so existing single-argument functions continue to
  work during refactor.
- Add a clear test strategy for function signature correctness and behavior.

## Non-Goals

- Full parity for every PromQL function in one PR.
- Histogram-native behavior parity for every function in this RFC.
- Replacing the existing PromQL parser crate.
- Introducing new non-PromQL function syntax.

## Design

### Current State

Function handling currently uses function traits that accept a single argument
form and category-based registries, which is too narrow for PromQL's signature
diversity.

Module responsibilities today:

- `functions.rs`: function trait definitions, function registry, and
  implementation bodies.
- `evaluator.rs`: function-call orchestration (arity gate, argument evaluation,
  dispatch, and result wrapping).

After this RFC:

- `functions.rs`: generalized function interfaces, function implementations, and
  registry wiring.
- `evaluator.rs`: signature resolution/validation, argument materialization,
  special call-path handling, and unified dispatch.

Both modules must change together. Updating `functions.rs` alone cannot unlock
multi-argument function support without evaluator-side call-path changes.

### Prometheus Reference Model

Prometheus separates function concerns into parser-time signatures and
evaluation-time handlers:

- In `promql/parser`, each function has metadata: `ArgTypes`, `Variadic`,
  `ReturnType`, `Experimental`.
- Signature examples include `round(vector, scalar)` with optional second
  argument.
- Signature examples include `label_replace(vector, string, string, string,
  string)`.
- Signature examples include `label_join(vector, string, string, string, ...)`
  with a variadic string tail.
- Signature examples include `time()` and `pi()` as zero-argument
  scalar-returning functions.
- In `promql`, runtime functions are registered in `FunctionCalls` and receive
  evaluated args plus original AST args.
- For string arguments, handlers rely on original AST args instead of evaluated
  value slots.
- Prometheus engine has explicit special call-path handling for
  `label_replace`, `label_join`, and `info` to preserve series/label semantics.

This gives Prometheus a single model that supports fixed-arity, optional,
variadic, mixed-type, and scalar-returning functions.

References:

- Prometheus parser package docs (function signatures and metadata):
  <https://pkg.go.dev/github.com/prometheus/prometheus/promql/parser>
- Prometheus promql package docs (`FunctionCalls`, `FunctionCall`):
  <https://pkg.go.dev/github.com/prometheus/prometheus/promql>
- Prometheus function docs (default/variadic argument behavior):
  <https://prometheus.io/docs/prometheus/latest/querying/functions>

### Alignment with Rust `promql-parser` (0.6.x)

The parser used in this crate (`promql-parser = "0.6"`) is mostly aligned with
Prometheus at signature/type-check level, with two key representation
differences:

- Rust parser `Function` exposes `name`, `arg_types`, `variadic: bool`,
  `return_type` (no integer variadic count field).
- Rust parser `Function` does not expose Prometheus `Experimental` metadata.
- Arity/type-checking is implemented in parser AST validation.
- Non-variadic functions require exact arity.
- Variadic functions allow at least `len(arg_types) - 1`.
- For `label_join`, `sort_by_label`, and `sort_by_label_desc`, max arity is
  unbounded.
- Type-checking for variadic overflow reuses the final declared arg type.

Implication for this RFC:

- AST metadata on `Call.func` is reliable for baseline type expectations.
- Rust parser `variadic: bool` is not rich enough to represent Prometheus
  `Variadic int` semantics exactly.
- Rust parser metadata cannot carry Prometheus `Experimental` function state.
- Evaluator must not encode arity behavior through scattered function-name
  exceptions.
- For this phase, use parser metadata as the runtime signature source and defer
  Prometheus-exact variadic cardinality and experimental metadata parity to an
  upstream `promql-parser` contribution.

### Proposed Function Argument Model

Introduce typed function arguments:

```rust
pub(crate) enum FunctionArgValue {
    Scalar(f64),
    InstantVector(Vec<EvalSample>),
    RangeVector(Vec<EvalSamples>),
}

pub(crate) struct FunctionCallContext<'a> {
    pub eval_timestamp_ms: i64,
    pub raw_args: &'a [Box<Expr>],
}
```

Evolve the existing `PromQLFunction` trait (same name, new unified signature):

```rust
pub(crate) trait PromQLFunction {
    fn apply(
        &self,
        evaluated_args: Vec<Option<FunctionArgValue>>,
        ctx: &FunctionCallContext<'_>,
    ) -> EvalResult<ExprResult>;
}
```

Notes:

- Return type becomes `ExprResult` (not always instant vector).
- Keep one `PromQLFunction` trait: dispatch is runtime by function name, so
  splitting traits by return shape would still require a single dynamic
  dispatch boundary while adding registry complexity.
- Type safety is enforced by parser metadata and evaluator return-shape checks
  (`Call.func.return_type` against `ExprResult`).
- String arguments are read from `ctx.raw_args`; corresponding
  `evaluated_args[idx]` is `None` (Prometheus-compatible shape).
- Existing one-arg implementations are ported directly to the evolved
  `PromQLFunction` interface in the same change; no compatibility adapter layer
  is kept.

### Scalar Boundary Semantics

Prometheus function internals represent scalar results as vector samples. This
engine intentionally keeps scalar results as `ExprResult::Scalar` at the
function boundary.

Boundary rule:

- Function handlers may use internal scalar/vector helper representations as
  needed, but final outputs for scalar-returning signatures must surface as
  `ExprResult::Scalar`.

### Signature Source of Truth

For this RFC phase, parser metadata on `Call.func` is the runtime signature
source:

- `arg_types`: argument type expectations.
- `return_type`: return-type expectations.
- `variadic`: current parser-supported variadic/arity behavior.

Evaluator still performs runtime validation as a safety layer, because tests and
internal AST construction paths can bypass normal parser entry points.

### Variadic and Optional Argument Semantics

For this phase, adopt parser-supported semantics:

- Non-variadic: exact arity.
- Variadic: minimum arity is `len(arg_types) - 1`.
- Maximum arity follows parser behavior for supported functions (currently
  unbounded only where parser handles it explicitly).
- Variadic overflow type-check uses the final declared arg type.
- Default argument materialization remains inside function implementations,
  matching Prometheus behavior.
- Prometheus-exact `Variadic int` and `Experimental` metadata parity is deferred
  to upstream `promql-parser` improvements.

### Evaluator Responsibilities

At a high level, evaluator-side function handling is responsible for:

- Validating call arity and argument types using parser metadata, with runtime
  guards for non-parser call paths.
- Preserving special handling for series/label-oriented functions
  (`label_replace`, `label_join`, and `info` when parser support exists).
- Passing both raw AST arguments and evaluated argument values into function
  execution.
- Enforcing result-shape expectations at the boundary (`ExprResult` consistency
  with the function signature).

### Registry Design

Replace split registries with one registry keyed by function name:

```rust
pub(crate) struct FunctionRegistry {
    functions: HashMap<String, Box<dyn PromQLFunction>>,
}
```

### Initial Function Rollout (Post-Refactor)

After core refactor lands, implement high-impact functions in this order:

1. `round(v, to_nearest=1)` (fix optional scalar arg support)
2. `clamp`, `clamp_min`, `clamp_max`
3. `time()`, `pi()` scalar returns
4. `scalar(v)` returning scalar `ExprResult`
5. `label_replace`, `label_join` (string and variadic strings)

### Testing Strategy

Add and/or update tests in:

- `timeseries/src/promql/evaluator.rs`
- `timeseries/src/promql/functions.rs`
- `timeseries/src/promql/promqltest/testdata/functions.test`
- `timeseries/src/promql/promqltest/testdata/aggregators.test`

Test categories:

- Arity validation (exact, optional, variadic, zero-arg)
- Type validation (scalar/vector/matrix/string mismatch paths)
- Scalar-return behavior through query and range-query pipelines
- Multi-arg function correctness against Prometheus behavior
- String-argument functions (`label_replace`, `label_join`) using raw AST args
  and evaluated-value slots

## Alternatives Considered

### Keep Current Split Function Traits

Retaining separate single-arg instant and range function traits would keep
adding one-off logic in evaluator and not scale to mixed-type signatures.

### Copy Prometheus Runtime Signature Exactly

Prometheus uses an evaluation signature optimized for its engine internals.
We considered matching it exactly. We chose a typed enum argument list because
it is easier to reason about in Rust and maps naturally to current evaluator
results.

### Parser-Only Validation

Relying exclusively on parser validation was considered. We still keep runtime
checks for robustness in tests and internal call paths that construct AST nodes
directly.

## Open Questions

- Should optional/defaulted arguments be materialized in evaluator before
  dispatch, or inside function handlers?
- Once `promql-parser` exposes experimental metadata, should experimental
  functions be feature-gated in the function registry?
- Do we want to support all string-argument functions in one phase, or stage
  `label_replace` before `label_join`?

## Updates

| Date       | Description |
|------------|-------------|
| 2026-03-04 | Initial draft |
