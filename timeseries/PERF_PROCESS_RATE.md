# Process Rate Perf Notes

Benchmark target:

- Query: `sum by (applicationId) (diagnoser_latency_expected_node_commit_rate)`
- Range: `6h`
- Step: `30s`
- Harness: `heracles/perftest/opendata`
- Reader config: `FoyerHybrid` block cache with `8 GiB` memory and `20 GiB` disk

Warm-cache median latency by optimization:

- Baseline on `main` with `merge_batch`: `1213.0ms`
- `timeseries: cache shared labels and precomputed series fingerprints`: `678.0ms`
- `timeseries: reuse cached sample vectors for instant lookups`: about `661ms` in the warm sanity runs
- `timeseries: fast-path reduction aggregates over vector selectors`: `325.1ms`
- `timeseries: materialize cached forward and inverted query views`: `322.0ms`
- `timeseries: avoid set churn for simple reader selectors`: `303.0ms`

Interpretation:

- The shared-label plus fingerprint caching and the aggregate fast path are the clear high-impact general wins.
- The cached forward/inverted view materialization is measurable but small.
- The selector vectorization is intentionally isolated in its own commit so it can be reverted easily if the team decides the extra code is not worth a roughly `6%` improvement on this benchmark.
