# RFC 0003: Common Benchmark Framework

**Status**: Draft

**Authors**:
- [Jason Gustafson](https://github.com/hachikuji)

## Summary

This RFC proposes a common benchmark framework for opendata that standardizes
provisioning and configuration aspects while remaining flexible enough to
accommodate the diverse needs of each database system (TSDB, Log, Vector, etc.). The
framework introduces minimal abstractions, allowing each system to plug in its
own workload generators and measurement concerns. Cross-cutting capabilities
like telemetry collection are handled by the framework, and benchmark results
can be periodically checked into the repository for regression analysis.

## Motivation

As opendata grows to encompass multiple database systems (TSDB, Log, Vector)
built on a shared storage substrate, we need a consistent way to measure and
track performance. Each system has distinct performance characteristics worth
measuring, but they share common cross-cutting concerns: provisioning (storage
setup, data loading, cleanup), data collection (metrics, telemetry, structured
output), and interpretation of results (regression detection, historical
tracking). A survey of existing crates did not reveal obvious candidates—SlateDB
rolled its own bencher for similar reasons. A common framework that handles
these shared concerns while remaining flexible enough to accommodate
system-specific workloads reduces duplicated effort and lowers the barrier to
adding meaningful benchmarks.

## Goals

- **Standardize provisioning and configuration**: Provide common abstractions
  for setting up storage backends, initializing databases, and managing
  benchmark lifecycle (setup, run, teardown).

- **Minimal framework abstractions**: Define only the essential traits and types
  needed to plug in system-specific workloads. Avoid over-engineering or
  imposing rigid structure on what benchmarks must look like.

- **Support common benchmark patterns**: Enable pure ingest benchmarks that
  measure write throughput, and query benchmarks that may involve a loading
  phase followed by read operations. However, do not require benchmarks to fit
  either pattern—allow flexibility for system-specific needs.

- **Telemetry collection as a cross-cutting concern**: The framework should
  handle metrics collection (throughput, latency percentiles, resource usage)
  so individual benchmarks don't need to implement this repeatedly.

- **Machine-readable output**: Produce JSON or similar structured output that
  can be checked into the repository and consumed by external tools.

- **CI integration readiness**: Design for periodic execution in CI, with
  results stored in a format amenable to automated regression analysis.

- **Enable AI-assisted regression analysis**: Structure results so that an AI
  agent can periodically inspect them, identify regressions, and correlate
  changes with specific commits.

## Non-Goals

- **Built-in regression analysis**: The framework produces data; analysis is
  handled externally (by AI agents or other tools). We don't build statistical
  analysis or alerting into the framework itself.

- **Replacing Criterion for micro-benchmarks**: Criterion remains appropriate
  for CPU-bound micro-benchmarks (like the existing `varint` benchmarks). This
  framework targets higher-level integration benchmarks involving I/O, storage,
  and realistic workloads.

- **Real-time dashboarding**: Visualization and dashboards are external
  concerns. The framework focuses on execution and data collection.

- **Production load testing**: This framework is for controlled benchmarks, not
  for load testing production systems or chaos engineering.

## Design

*To be detailed in subsequent revisions. Initial design considerations include:*

- CLI-based benchmark runner with subcommands per system
- TOML-based configuration with CLI overrides
- Trait-based workload abstraction
- Pluggable storage backend provisioning
- Structured JSON output format
- Integration with existing Prometheus metrics where applicable

## Alternatives

*To be detailed in subsequent revisions.*

## Open Questions

1. Should the benchmark framework live as a separate crate in the workspace, or
   as a module within `common`?

2. What specific telemetry should be collected by default? Throughput and
   latency percentiles seem essential—what else?

3. Where should we store periodic bench results? 

4. Should we adopt or integrate with external tools like
   [Bencher](https://bencher.dev/) for tracking, or keep results self-contained?

5. What is the minimum viable interface a system must implement to plug into
   the framework?

## Updates

| Date       | Description   |
|------------|---------------|
| 2026-01-16 | Initial draft |
