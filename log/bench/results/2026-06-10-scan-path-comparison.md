# Scan-path comparison: range (A) vs scan_prefix (B) vs range+inferred-filters (C)

Results from the 2026-06-10 measurement campaign for slatedb#1778
(https://github.com/slatedb/slatedb/issues/1778, draft PR
https://github.com/slatedb/slatedb/pull/1783). Raw per-cell bench output
(all percentiles) is in `2026-06-10-raw/`; this file is the curated view.

## Variants

- **A — range:** bounded range scan `key|cursor .. key|end`; slatedb prunes
  SSTs/blocks via manifest range metadata + block index, consults no bloom
  filters. What opendata main and stock slatedb do today.
- **B — prefix:** `scan_prefix(segment|key)` + client-side sequence
  filtering. Filters consulted; reads each segment's key-slice from entry 0.
  Jason's `worktree-logdb-prefix-bloom` approach.
- **C — range + inferred filters:** same call as A; slatedb infers the prefix
  from the bounds (`BytesRange::infer_prefix`) and consults filters. PR #1783.

## Setup

- Bench: `log/bench/src/scan.rs` (`-b scan`), configs in `log/bench/configs/`.
  Both paths measured **interleaved over the same store** (even sample slots
  range, odd prefix) — cross-cell comparison is invalid, LSM shape varies
  run-to-run (see README "scan" section).
- Machine: local macOS, local-FS object store (`/tmp`), cold block cache
  (close+reopen after prefill), sequential scans, per-scan GET/byte deltas.
- slatedb settings: `slatedb-small-sst.toml` (1 MiB L0 SSTs, l0_max_ssts=128).
- Data shapes: shallow = 20k keys × 20 entries × 256 B; deep = 2k keys ×
  1000 entries × 256 B. PRNG arrivals (seed 42). Every scan asserts exact
  expected entries — results are correctness-validated.
- Latencies are local-FS; on S3 the byte/GET gaps widen into request and
  transfer time.

## 1. A vs B baseline (raw: 01) — opendata fc6ef8b, slatedb crates.io 0.13.0

GETs/scan mean, p50 latency. Lag = entries behind the key's tail at cursor.

| regime | lag | A gets | A p50 | B gets | B p50 |
|---|---|---|---|---|---|
| shallow, unsealed | 5 | 99.4 | 2.38 ms | 20.2 | 537 µs |
| shallow, unsealed | full | 99.0 | 2.42 ms | 20.5 | 578 µs |
| deep, unsealed | 5 | 155.2 | 2.80 ms | 141.9 | 3.72 ms |
| deep, unsealed | full | 155.9 | 3.94 ms | 141.5 | 3.82 ms |
| deep, sealed+settled | 5 | 7.9 | 399 µs | 7.1 | 488 µs |
| deep, sealed+settled | full | 12.0 | 3.31 ms | 11.4 | 3.40 ms |

## 2. C vs B, same tree (raw: 02) — opendata 5bde528, slatedb main+inference

The range column IS variant C here. Sealed cells from this run are INVALID
(slatedb-main compactor stall left the tree unconsolidated; see handoff) —
valid sealed C numbers are in §3.

| regime | lag | C gets | C p50 | B gets | B p50 |
|---|---|---|---|---|---|
| shallow, unsealed | 5 | 20.2 | 625 µs | 20.3 | 655 µs |
| shallow, unsealed | full | 20.2 | 682 µs | 20.3 | 674 µs |
| deep, unsealed | 5 | 153.8 | 3.17 ms | 151.5 | 4.32 ms |
| deep, unsealed | full | 153.9 | 4.36 ms | 151.4 | 4.31 ms |

Headline (C vs A, B as cross-run control — B identical 20.3/20.3 across
runs): shallow regime **99.4 → 20.2 GETs (−80 %), 2.38 ms → 625 µs**.

## 3. C vs B sealed regime (raw: 03) — slatedb `range-scan-prefix-filter-0.13`

| lag | C gets | C p50 | B gets | B p50 |
|---|---|---|---|---|
| 5 | 7.67 | 418 µs | 7.54 | 539 µs |
| full | 13.0 | 3.47 ms | 12.3 | 3.46 ms |

Matches the §1 baseline shape; C keeps A's latency at the tail.

## 4. Seal-interval sweep (raw: 04 gets-only, 05 with bytes) — 0.13 branch

Deep shape, lag=5, `seal_tail=true` (sentinel write seals+consolidates the
data tail), sweep of `seal_interval_ms` ⇒ per-segment per-key slice depth.
p50 values from run 05:

| seal interval | C bytes/scan | B bytes/scan | B÷C | C p50 | B p50 | gets (both) |
|---|---|---|---|---|---|---|
| 500 ms | 7.9 KB | 7.9 KB | 1× | 99 µs | 102 µs | ~1.5 |
| 1 s | 7.9 KB | 55.1 KB | 7× | 207 µs | 662 µs | ~1.5 |
| 2 s | 6.2 KB | 47.3 KB | 7.7× | 193 µs | 616 µs | ~1.5 |
| 4 s | 3.9 KB | **286.8 KB** | **73×** | 248 µs | **1.76 ms** | ~1.6 |

B reads the cursor segment's whole per-key slice from entry 0; at 4 s the
slice is the full 1000-entry history (262 KB raw ≈ the measured 287 KB). C
reads ~one block regardless. GET counts do NOT show this: dense contiguous
slices coalesce into single large ranged reads — **bytes/scan is the
discriminating metric in consolidated regimes**. The 500 ms row is parity
because that run's sentinel-sealed tail segment held only a sliver of
history (seal-boundary timing varies; run 04's 500 ms cell showed 612 µs).

## Cost model (what the numbers obey)

- Per-scan cost ≈ #sources (L0 SSTs + sorted runs) in the segments the
  cursor covers, ~1 probe each — minus filter-negative skips.
- Filter skip probability per source ≈ `e^(−records_per_source / num_keys)`:
  shallow 20k keys ⇒ ~83 % of 1 MiB L0s skippable (the −80 %); deep 2k keys
  ⇒ ~15 % (the −9 %); consolidated runs contain every key ⇒ 0 %.
- In consolidated segments the residual differences are bytes/decode, not
  requests (§4).
- Verdict: **C ≈ max(A,B) everywhere and strictly better than both where
  they diverge** — B's filter skips at high cardinality, A's cursor-bounded
  reads over deep consolidated histories. RFC 0006 target workloads
  (100k–1M keys, catch-up tail reads) sit in both winning regimes at once.
