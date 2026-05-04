# RFC 0006: Native Histogram Support in PromQL

**Status**: Draft

**Authors**:
- [Samson Hailu](https://github.com/samsond)

## Summary

This RFC recommends approving a narrow Phase 1 of native histogram support in the PromQL engine: carry native histogram payloads through the evaluator/function boundary, support `histogram_count`, `histogram_sum`, and `histogram_avg`, and validate that slice with unit tests and `promqltest`.

This RFC does **not** propose end-to-end native histogram support in the same change. Storage, ingestion, HTTP response serialization, broad range-function behavior, and full histogram-related annotation parity remain later phases.

The intent is pragmatic: start with the smallest slice that is both useful and testable, prove the architecture at the evaluator boundary, and make the follow-on work explicit rather than letting scope expand implicitly.

## Motivation

OpenData's current PromQL evaluator and storage are float-only. Prometheus treats native histograms as first-class sample types with dedicated functions and HTTP API behaviors. This RFC adopts the smallest slice that proves the evaluator architecture before committing to storage and API changes: carry native histogram payloads through the evaluator/function boundary, support three projection functions, and validate with unit tests and promqltest. Prometheus already defines the reference semantics (native histogram spec, `histogram_count`/`histogram_sum`/`histogram_avg`, HTTP `"histogram"` fields), so OpenData avoids reinventing behavior. Phase 1 is narrow to reduce risk; storage, ingestion, HTTP serialization, and broader histogram semantics remain later phases. See [native histogram spec](https://prometheus.io/docs/specs/native_histograms/), [PromQL functions](https://prometheus.io/docs/prometheus/latest/querying/functions/), and [promtool unit testing](https://prometheus.io/docs/prometheus/latest/configuration/unit_testing_rules/).

## Goals

- Add a native histogram payload to the PromQL evaluator.
- Support the smallest useful set of native histogram-aware functions: `histogram_count`, `histogram_sum`, and `histogram_avg`.
- Keep existing float semantics unchanged for non-histogram queries.
- Add a native histogram test path that proves the feature works at the current evaluator/function boundary.
- Define a migration path from this minimal slice to broader native histogram support later.

## Non-Goals

- Classic histogram support or reconstruction.
- Parity with Prometheus for histogram-aware functions beyond `histogram_count`, `histogram_sum`, and `histogram_avg` in one change (e.g., `rate`, `delta`, `increase`, aggregation behavior on histograms).
- Full histogram-related annotation parity in the first implementation.
- Finalizing a storage encoding for native histogram samples in the prototype slice.

## Design

### Native Histogram Data Shapes

Phase 1 introduces a minimal native histogram payload sufficient for projection functions (`histogram_count`, `histogram_sum`, `histogram_avg`). The structure aligns with Prometheus's evaluator/runtime `FloatHistogram` model for exponential-bucket native histograms.

**Minimal Phase 1 Shape (Runtime Representation)**:

```rust
// Phase 1: Minimal payload for instant functions
pub struct NativeHistogram {
    /// Schema parameter that determines bucket boundaries (-4..=8).
    /// Larger schema = smaller buckets (higher resolution).
    pub schema: i8,
    
    /// Total count of all observations across all buckets.
    /// For float histograms, may contain fractional values.
    pub count: f64,
    
    /// Sum of all observed values. Used for calculating average.
    pub sum: f64,
    
    /// Zero threshold: observations within [-zero_threshold, +zero_threshold]
    /// are placed in the zero bucket rather than positive/negative buckets.
    pub zero_threshold: f64,
    
    /// Count of observations that fall within the zero threshold.
    pub zero_count: f64,
    
    /// Positive bucket spans and counts (see Span definition below).
    pub positive_spans: Vec<Span>,
    pub positive_buckets: Vec<f64>,  // Phase 1: float counts; future: i64 deltas for integer histograms
    
    /// Negative bucket spans and counts. Negative buckets store absolute value
    /// observations; bucket boundaries are expressed as absolute values.
    pub negative_spans: Vec<Span>,
    pub negative_buckets: Vec<f64>,  // Phase 1: float counts; future: i64 deltas for integer histograms
}

/// Span represents a range of consecutive non-empty buckets.
/// Used to compactly encode sparse bucket layouts.
pub struct Span {
    /// For the first span: the starting bucket index (can be negative).
    /// For subsequent spans: the gap to the previous span (must be positive).
    pub offset: i32,
    
    /// Number of consecutive buckets in this span.
    pub length: u32,
}
```

Bucket boundaries are computed from `schema` and bucket index per Prometheus spec; the `Span` structure encodes which buckets are non-empty, avoiding storage cost of empty buckets.

**Phase 1 Constraints**:

- Both `positive_buckets` and `negative_buckets` store absolute counts (f64), not deltas.
- No `counter_reset_hint` or reset tracking in Phase 1.
- Buckets are sparse; empty buckets are not stored.
- Integer histogram variant (with i64 delta encoding) is deferred to Phase 2+ storage design.

### Decision and Scope

**Approved for Phase 1**:
- Native histogram payload support at the evaluator/function boundary
- `histogram_count`, `histogram_sum`, and `histogram_avg` functions
- Native histogram test coverage via promqltest

**Explicitly Deferred**:
- End-to-end ingestion and storage encoding for histogram samples
- Prometheus HTTP `"histogram"`/`"histograms"` responses
- Range semantics (`rate`, `delta`, aggregations on histograms)
- Full quantile/fraction and annotation parity

OpenData follows Prometheus semantics: `histogram_count`, `histogram_sum`, and `histogram_avg` extract float values from histogram samples and ignore float samples. Quantile/fraction annotation behavior is deferred after core numeric semantics are correct.

### Implementation Path

The implementation stays incremental: Phase 1 adds native histogram payloads to `EvalSample`, implements the three projection functions, and validates via promqltest. Later phases extend storage, range semantics, API serialization, and quantile/fraction.

### Cross-RFC Impact

This RFC must align with three existing contracts:

- **RFC 0001 (Storage)**: Phase 1 does not require storage changes. Phase 4 will need a dedicated `RecordType::Histogram` record family alongside float `TimeSeries`, with dedicated merge logic. Histogram encoding must preserve schema, count, sum, zero-bucket info, and sparse bucket populations.

- **RFC 0003 (Public Query Types)**: Phase 1 uses internal `EvalSample` types only. Phase 3 requires extending `InstantSample`, `RangeSample`, and `QueryValue` to carry `enum QuerySampleValue { Float(f64), Histogram(...) }`, and extending HTTP response serializers to emit `"histogram"`/`"histograms"` fields per Prometheus spec.

- **RFC 0005 (PromQL Functions)**: Histogram-aware functions remain `(vector) -> vector` signatures; histogram behavior is runtime, not parser-level. No new parser `ValueType` required. Function dispatch and histogram-specific semantics are evaluator concerns.

### Runtime Sample Model

OpenData follows Prometheus: distinguish float and histogram samples at runtime via the sample itself, not parser types. Each `EvalSample` represents **either** a float sample (with `value: f64` and `histogram: None`) **or** a histogram sample (with `histogram: Some(NativeHistogram)` and `value` semantically ignored), never both. Histogram-aware functions inspect the optional payload, while float-only functions ignore histograms. This aligns with RFC 0005 (functions remain `(vector) -> vector` signatures; no new parser `ValueType`). Parser types stay unchanged: `scalar`, `vector`, `matrix`, `string`.

### Phase Roadmap

| Phase | Focus | Key Changes |
|-------|-------|-------------|
| 1 (RFC 0006) | Evaluator + instant functions | `EvalSample` with `Option<NativeHistogram>`; `histogram_count`, `histogram_sum`, `histogram_avg`; promqltest native histogram literals |
| 2 | Range semantics | Range functions (`rate`, `delta`, `sum_over_time`, etc.) with histogram behavior |
| 3 | Public API & HTTP | RFC 0003 extension: `QuerySampleValue` enum; response serializers emit `"histogram"`/`"histograms"` |
| 4 | Storage & ingestion | RFC 0001 amendment: `RecordType::Histogram`; histogram value codec; merge and reader updates |
| 5 | Quantile/fraction | `quantile.rs` module; `histogram_quantile`, `histogram_fraction`; annotation parity |

### Out of Scope

Phase 1 intentionally omits:
- End-to-end ingestion and persistent storage of histogram samples
- Selection aggregations (`topk`, `bottomk`) with histogram-aware semantics
- Reduction aggregations and binary operators assuming float arithmetic
- HTTP response serialization of histogram payloads
- Full quantile/fraction and annotation parity
- `histogram_quantile` and `histogram_fraction` (Phase 5)
- Mixed float/histogram range handling and warnings (Phase 2+)

These paths remain float-oriented today and are explicitly deferred, preventing accidental overclaiming of histogram support.

### Validation

Phase 1 is validated when:
- Native histogram literals parse in `promqltest`
- `histogram_count`, `histogram_sum`, and `histogram_avg` return expected float outputs
- Float samples are properly ignored per Prometheus semantics
- Validation is via unit tests and promqltest, without requiring storage/scrape/HTTP support

### Module Organization (Phase 2+)

Phase 1 keeps `NativeHistogram` in `evaluator.rs`. As the shape grows, introduce `timeseries/src/promql/float_histogram.rs` for histogram data structure helpers (bucket iteration, merging, schema alignment) and keep `quantile.rs` for algorithm concerns (percentile math, interpolation). Evaluator operations produce fractional bucket counts; conversion between storage (integer deltas, Phase 4) and runtime (float absolute) happens at read boundaries, matching Prometheus design.

## Alternatives

### Treat Native Histograms as Encoded Floats

Prometheus treats native histograms as a distinct sample type with dedicated PromQL and HTTP API behavior. Encoding them as synthetic floats would make correct function behavior and mixed-sample handling much harder.

### Delay All Histogram Work Until Full Storage Support Exists

That would block useful evaluator and function work that can be validated independently. A narrow prototype is valuable precisely because it reduces the risk of the later storage decision.

## Open Questions

- Should Phase 1 include only instant functions, or also `rate` for counter histograms?
- Should the full runtime histogram payload move to a dedicated `promql/float_histogram.rs` module immediately, or after Phase 1?
- What is the preferred storage encoding for the `RecordType::Histogram` record family (Phase 4)?
- Should RFC 0001 be amended before any end-to-end ingestion work begins?
- Do we need a separate gauge-histogram metric kind, or is the current histogram metadata model sufficient with native details in the payload/storage layer?
- Should response `warnings` / `infos` be added when introducing histogram query results, or in a separate change?

## Updates

| Date       | Description |
|------------|-------------|
| 2026-03-17 | Initial draft |
| 2026-03-21 | Added `NativeHistogram` and `Span` struct definitions |
| 2026-03-23 | added RFC 0001/0003/0005 cross-references.|
