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
preserving PromQL semantics. In the current incremental design, expression
shape (`scalar`, `vector`, `matrix`) remains an evaluator concern, while
function implementations continue to operate on `EvalSample`-based values.

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

### Decisions

- Use parser metadata on `Call.func` (`arg_types`, `return_type`, `variadic`,
  `experimental`) as the signature source of truth.
- Use one runtime-dispatched `PromQLFunction` interface at the function
  boundary.
- Keep `ExprResult` as the evaluator boundary for expression shape.
- Keep `PromQLFunction` implementations returning `Vec<EvalSample>` in the
  current incremental design.
- Preserve explicit special handling for series/label-oriented functions
  (`label_replace`, `label_join`, and `info` when supported).

### Current State

Function handling currently uses function traits that accept a single argument
form and category-based registries, which is too narrow for PromQL's signature
diversity.

Module responsibilities today:

- `functions.rs`: function trait definitions, function registry, and
  implementation bodies.
- `evaluator.rs`: function-call orchestration (argument evaluation, unsupported
  argument rejection, dispatch, and result wrapping).

After this RFC:

- `functions.rs`: generalized function interfaces, function implementations, and
  registry wiring.
- `evaluator.rs`: signature-aware argument materialization, special call-path
  handling, and unified dispatch.

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

### Alignment with Rust `promql-parser` (0.8.x)

The parser used in this crate (`promql-parser = "0.8"`) now carries
Prometheus-style function signature metadata directly on `Call.func`:

- `Function` exposes `name`, `arg_types`, `variadic: i32`, `return_type`, and
  `experimental`.
- Arity/type-checking is implemented in parser AST validation.
- Variadic cardinality follows Prometheus semantics:
  `0` exact, `>0` bounded optional, `<0` unbounded.
- Type-checking for variadic overflow reuses the final declared arg type.

Implication for this RFC:

- `Call.func` metadata is the runtime signature source for both type
  expectations and arity semantics.
- `experimental` metadata is available for registry policy decisions.

### Proposed Function Argument Model

Use one unified function-call interface at the function boundary.

- `PromQLArg` is the function-input channel for `scalar`, `instant vector`,
  and `range vector` arguments.
- `FunctionCallContext` carries evaluation timestamp and raw AST arguments for
  handlers that must inspect literals directly.
- `PromQLFunction` remains the single runtime-dispatched function interface.
- In the current implementation, function handlers return `Vec<EvalSample>`.
- `ExprResult` remains the expression-output channel owned by evaluator.

```rust
pub(crate) enum PromQLArg {
    Scalar(f64),
    InstantVector(Vec<EvalSample>),
    RangeVector(Vec<EvalSamples>),
}

pub(crate) struct FunctionCallContext<'a> {
    pub eval_timestamp_ms: i64,
    pub raw_args: &'a [Box<Expr>],
}
```

Notes:

- Keep one `PromQLFunction` trait: dispatch is runtime by function name, so
  splitting traits by return shape would still require a single dynamic
  dispatch boundary while adding registry complexity.
- Type safety for parsed queries is enforced by parser metadata and parser AST
  validation.
- String-argument handling uses `raw_args` with `None` in evaluated argument
  slots.
- Function implementations stay simpler if they operate on sample vectors and
  let evaluator own final expression-shape conversion.

### Scalar Boundary Semantics

Prometheus function internals represent scalar results as vector samples. This
engine keeps scalar results as `ExprResult::Scalar` at the evaluator boundary.

Boundary rule:

- Scalar-returning function handlers may encode their result as a single-sample
  `Vec<EvalSample>`.
- Evaluator unwraps that result to `ExprResult::Scalar` using parser
  `return_type` metadata.
- This keeps function bodies simple while preserving correct PromQL expression
  semantics.

### Signature Source of Truth

Parser metadata on `Call.func` is the runtime signature source:

- `arg_types`: argument type expectations.
- `return_type`: return-type expectations.
- `variadic`: Prometheus-style variadic cardinality (`0`, `>0`, `<0`).
- `experimental`: function experimental status.

### Variadic and Optional Argument Semantics

Use parser-supported semantics:

- `variadic == 0`: exact arity `len(arg_types)`.
- `variadic > 0`: minimum arity `len(arg_types) - 1`; maximum arity is
  `min + variadic`.
- `variadic < 0`: minimum arity `len(arg_types) - 1`; maximum arity unbounded.
- Variadic overflow type-check uses the final declared arg type.
- Default argument materialization remains inside function implementations,
  matching Prometheus behavior.
- Support for unbounded variadic string-tail behavior (for example full
  `label_join(...src_labels)` handling) still depends on string-argument
  function implementation in this engine.

### Evaluator Responsibilities

At a high level, evaluator-side function handling is responsible for:

- Materializing supported argument values for dispatch.
- Preserving special handling for series/label-oriented functions
  (`label_replace`, `label_join`, and `info` when parser support exists).
- Passing both raw AST arguments and evaluated argument values into function
  execution.
- Converting function outputs into the declared expression shape using parser
  metadata (`ExprResult::Scalar` vs `ExprResult::InstantVector`).

### Registry Design

Replace split registries with one registry keyed by function name:

```rust
pub(crate) struct FunctionRegistry {
    functions: HashMap<String, Box<dyn PromQLFunction>>,
}
```

### Testing Strategy

Coverage should include:

- Arity validation (exact, optional, variadic, zero-arg)
- Type validation (scalar/vector/matrix/string mismatch paths)
- Scalar-return behavior through query and range-query pipelines
- Multi-argument function correctness against Prometheus behavior
- String-argument functions using raw AST args and evaluated-value slots

## Alternatives Considered

### Keep Current Split Function Traits

Retaining separate single-arg instant and range function traits would keep
adding one-off logic in evaluator and not scale to mixed-type signatures.

### Copy Prometheus Runtime Signature Exactly

Prometheus uses an evaluation signature optimized for its engine internals.
We considered matching it exactly. We chose a typed enum argument list because
it is easier to reason about in Rust and maps naturally to current evaluator
results.

## Open Questions

- Should optional/defaulted arguments be materialized in evaluator before
  dispatch, or inside function handlers?
- Should experimental functions be feature-gated in the function registry?
- Should string-argument functions be delivered in one step or staged?

## Updates

| Date       | Description |
|------------|-------------|
| 2026-03-04 | Initial draft |
| 2026-03-05 | Clarified Rust `promql-parser` alignment: parser metadata is runtime signature source; Prometheus-exact variadic cardinality and `Experimental` metadata remain upstream follow-ups. |
| 2026-03-05 | Added upstream tracking link for richer parser function metadata: <https://github.com/GreptimeTeam/promql-parser/issues/129>. |
| 2026-03-05 | Refocused design sections on target architecture and removed rollout sequencing details from core RFC sections. |
| 2026-03-05 | Simplified target argument model by removing duplicate `FunctionArgValue`; use one function-input channel plus `raw_args` with `None` slots for string literals. |
| 2026-03-05 | Clarified current variadic scope: bounded optional-arity support is in scope; unbounded variadic tails (for example `label_join` extra labels) are deferred. |
| 2026-03-06 | Updated alignment for `promql-parser` `v0.8.x` (`variadic: i32`, `experimental: bool`) and switched RFC variadic semantics to Prometheus-style cardinality. |
| 2026-03-06 | Added explicit architecture decisions section and reduced implementation-detail sections to keep RFC scope high-level. |
| 2026-03-09 | Clarified input/output separation for function execution: `PromQLArg` for function inputs, `ExprResult` for expression outputs. |
| 2026-03-12 | Updated the RFC to match the incremental implementation: `PromQLFunction` remains `Vec<EvalSample>`-based, while evaluator keeps ownership of `ExprResult` conversion and scalar unwrapping. |
