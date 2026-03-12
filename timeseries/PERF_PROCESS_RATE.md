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
- `timeseries: use an integer hasher for fingerprint-keyed evaluator state`: `270.4ms`

Interpretation:

- The shared-label plus fingerprint caching and the aggregate fast path are the clear high-impact general wins.
- The cached forward/inverted view materialization is measurable but small.
- The selector vectorization is intentionally isolated in its own commit so it can be reverted easily if the team decides the extra code is not worth a roughly `6%` improvement on this benchmark.
- The integer-hasher change is a general improvement for fingerprint-keyed maps/sets. After it landed, the remaining profile shifted away from `HashSet::insert` and more clearly toward allocator churn plus residual SlateDB point-read work.

Cross-query validation for the forward-index entry cache:

- Baseline commit for comparison: `5a06d36`
- Candidate optimization: cache and reuse requested `(SeriesId, SeriesSpec)` entries so evaluator loops stop re-hashing and re-looking-up each series in `CachedForwardIndex`

Warm-cache medians with the same `6h` / `30s` harness:

- `sum by (applicationId) (diagnoser_latency_expected_node_commit_rate)`: `270.4ms -> 273.7ms`
- `max by (applicationid, host_name) (diagnoser_saturation_blocked_ratio)`: `809.3ms -> 761.2ms`
- `sum by (topic) ((max by (topic, partition) (responsive_kafka_streams_source_offset_end{applicationid="topic-replicator-buffer-dedicated"})) - (max by (topic, partition) (responsive_kafka_streams_source_offset_end{applicationid="topic-replicator-buffer-dedicated"} offset 2d))) / 2`: `491.9ms -> 379.0ms`

Takeaway:

- The forward-index entry cache is not a universal latency reduction for every query shape.
- It is still worth keeping because it improves the two higher-cardinality queries that were dominated by `CachedForwardIndex::get_spec`, while leaving the already-optimized `process_rate` query effectively flat.
