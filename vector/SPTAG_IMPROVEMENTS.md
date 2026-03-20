# SPTAG Centroid Graph Improvements

## Major functional changes

- Added a new `SptagCentroidGraph` implementation of `CentroidGraph`.
- Wired `SptagCentroidGraph` in as the default centroid graph implementation.
- Kept `UsearchCentroidGraph` available for direct comparison and benchmarking.
- Preserved the `CentroidGraph` trait surface so the new implementation remains a drop-in replacement.

## SPTAG / SPFresh fidelity improvements

- Replaced the initial graph-only approach with a closer SPFresh-style head index:
  - BKT tree for search seeding
  - relative-neighborhood graph for traversal
  - `RefineNode`-style local repair on insert
  - tombstone deletes with periodic rebuild
- Added a flatter internal representation intended to support future external save/load.
- Added BKTree balance-factor state so tree construction behavior is stable across rebuilds.
- Replaced the simplified cluster partitioning with a more SPFresh-like balanced k-means build:
  - lambda-factor selection
  - iterative center refinement
  - exemplar snapping back to real points
  - shuffled build order per tree
- Added collapsed leaf handling and center-to-leaf backlinks so the BKTree can model SPFresh-style sample-backed leaves more closely.

## Search-path improvements

- Added thread-local reusable search workspace to avoid per-query allocation of visited state and heaps.
- Added a no-exclusion fast path so common searches avoid extra exclusion bookkeeping.
- Avoided extra include/exclude merge work in the dirty-graph wrapper when no pending centroid mutations exist.
- Removed unnecessary `sqrt` work from internal L2 comparisons.
- Replaced iterator-heavy distance loops with unrolled scalar L2 and dot-product kernels.
- Avoided cloning the final result heap by consuming it directly at the end of search.

## Storage-layout improvements

- Replaced per-node neighbor `Vec<usize>` storage with one flat fixed-width adjacency buffer.
- Replaced per-node centroid `Vec<f32>` storage with one contiguous flat vector buffer.
- Added row helpers for both adjacency and vector access so search/build code runs over contiguous storage.
- Kept the graph/tree/vector layout flat enough to be a realistic basis for later serialization.

## Build / maintenance improvements

- Improved tree rebuild behavior so BKTree rebuilds no longer force unnecessary graph rebuilds.
- Kept insert repair incremental instead of falling back to full rebuild on every mutation.
- Kept delete handling tombstone-based with threshold-triggered compaction and rebuild.

## Benchmarking improvements

- Added a direct side-by-side centroid benchmark at `vector/bench/src/centroid_graph_compare.rs`.
- Wired the new benchmark into `vector-bench`.
- Documented how to run the isolated centroid benchmark in `vector/bench/README.md`.

## Performance progression observed during this work

- Initial graph-only SPTAG version was roughly:
  - `sift100k_recall`: about `46s`
- First BKT + graph rewrite improved to roughly:
  - `sift100k_recall`: about `24s`
- Distance-loop and search-path tuning improved the real workload further to roughly:
  - `sift100k_recall`: about `19.8s`
- Current latest measured real workload result:
  - `cargo test --release -p opendata-vector --test sift1m sift100k_recall -- --nocapture`
  - `hnsw_recall@10 = 0.9500`
  - test time `18.78s`

## Current isolated centroid benchmark state

- Command:
  - `cargo run -p vector-bench --release -- --benchmark centroid_graph_compare --config /tmp/vector-bench-centroid-graph.toml`
- Latest measured result on `5000` centroids, `256` dimensions, `50` queries, `k=10`:
  - `usearch`: build `0.778s`, query `0.0061s`
  - `sptag`: build `3.586s`, query `0.026s`
  - build ratio: `4.61x`
  - query ratio: `4.24x`

## What did not help enough

- A more aggressive sample-to-center graph aliasing experiment regressed both runtime and recall, so it was backed out.
- Replacing heap-based result reservoirs with a vector-backed fixed-capacity reservoir regressed query time, so it was backed out.

## Current conclusion

- The implementation is now materially closer to SPFresh/SPTAG than the starting point.
- The biggest wins came from:
  - moving to BKT + graph
  - balanced BKTree construction
  - thread-local search workspace
  - flat adjacency storage
  - flat vector storage
  - cheaper distance kernels
- The remaining performance gap versus `usearch` appears to be mostly in frontier/search-traversal behavior rather than raw storage layout.
