# RFC 0007: Efficient Tail Scans

**Status**: Draft

**Authors**:
- Jason Gustafson <12502538+hachikuji@users.noreply.github.com>

## Summary

This RFC keeps tail scans cheap as the log grows, so that following the end of
the log costs roughly `O(data since the last poll)` rather than `O(backlog)`. It
combines two cooperating mechanisms:

1. A **resume watermark** — `LogIterator::next_sequence()` reports the exclusive
   global sequence a scan observed, so a follower resumes past the range it has
   already seen instead of re-scanning from its original start. Activity on any
   key advances the watermark for all keys.
2. **Sequence-aware SST filtering** — per-SST filter metadata lets a scan with an
   advanced cursor skip entire tables, so a higher watermark actually prunes I/O.
   Two filter policies cover the tree: a sequence-range policy that prunes
   by sequence (table-level on recent data, per-key once compacted) and a prefix
   bloom that prunes by key.

The watermark decides *how far* a follower can skip ahead; the filtering makes a
skipped-ahead cursor *cheap*. Neither is sufficient alone.

## Motivation

Benchmarking found idle tail polling is expensive: a follower querying
`scan(key, N..)` pays a full range scan every poll even when the key has no new
data, and that cost grows with the log as the empty `[N, tip)` range widens.

The scan already knows more than it returns. If `scan(key, N..)` observes the log
through global sequence `M` and finds nothing past `N`, then `key` provably has no
records in `[N, M)` — durable, because the log is append-only (no backfill below
the tip). Surfacing `M` lets the caller resume at `M`.

But advancing the cursor only helps if a higher cursor reads less. The key layout
sorts `segment_id` high and `relative_seq` low, so a per-key scan prunes whole
**segments** (a different `segment_id` is a disjoint key range) but, *within* a
segment, every SST spanning the key is probed regardless of the cursor — sequence
has no pruning power over SST selection. So the watermark must be paired with SST
filtering that can prune on sequence (Part 2).

## Goals

- Expose, per scan, the exclusive global sequence through which the scan observed
  the log, as a lossless resume cursor.
- Let activity on hot keys advance the resume point for idle keys (one watermark,
  shared via prefix-consistent visibility).
- Prune SSTs a scan cannot need — by key membership and by sequence range — so an
  advanced cursor reduces I/O rather than just narrowing the logical range.
- Keep the resume contract mechanism-independent, so frontier sources and filters
  can improve without changing callers.

## Non-Goals

- Erasing work for a key that has genuinely advanced. The watermark narrows scan
  width and the filters drop whole tables; together they shrink — but do not
  eliminate — the cost of catching a key up.
- Server-side per-key cursor storage. Cursors are carried by the caller; the
  server holds at most a single shared watermark.
- Event-driven wakeup instead of polling. The watermark can later double as a
  wakeup source, but that is out of scope here.

## Design — Part 1: the resume watermark

### The value

```rust
impl LogIterator {
    /// Exclusive upper bound on the global sequence this scan observed —
    /// the next sequence a reader should fetch to resume losslessly.
    ///
    /// `next_sequence() == N` means: under this scan's read visibility, the
    /// scan observed `key` completely through `seq < N`. Resuming
    /// `scan(key, next_sequence()..)` omits nothing this scan would return.
    pub fn next_sequence(&self) -> Sequence;
}
```

Exclusive, matching `LogDb::durable_sequence` and `WrittenView.next_sequence`.
Resume is `cursor = cursor.max(it.next_sequence())` — no off-by-one.

### Always a sound resume cursor

- **While iterating, or stopped early by a `ScanOptions` limit:**
  `last_yielded_seq + 1` (or the scan's `start` if nothing was yielded). The scan
  only covered as far as it consumed, so it cannot claim the frontier.
- **After `next()` returns `None` (full drain):** lifted to the global frontier
  this scan observed — which advances even for keys with no records, because the
  frontier is fed by activity on other keys.

The value is never larger than what the scan actually covered for `key`, so
advancing to it can never skip a record, drained or not.

### Frontier source by reader type

The contract is one value; only the frontier source differs:

- **In-process reader:** the frontier is exact and free — it is the read-view
  frontier the subscriber already maintains (`durable_sequence` in Remote mode,
  the written epoch in Memory mode). Lift `next_sequence` to it at drain.
- **Standalone reader:** has no writer watermark, so it estimates the frontier
  as the maximum of two sound lower bounds:
  - a **seal floor** — the start of the active segment (`SegmentCache::sealed_frontier`),
    below which everything is in sealed, fully-durable segments. Available at
    open from metadata, advances only at seal boundaries.
  - an **SST bound** — the maximum global sequence decoded from the key bounds
    (`SsTableInfo.last_entry`) of the durable SSTs (L0 and compacted), read via
    `StorageRead::sst_key_bounds()` **on the scan reader itself**. Any record's
    sequence in an SST exceeds every sequence in older SSTs, so this advances the
    frontier at *flush* granularity — finer than the seal floor, off the scan path,
    and independent of which keys are queried. Because the bounds come from the
    same reader — hence the same monotonic snapshot — that the scans use, the
    frontier can never outrun what a scan observes. (Sourcing it from a *separate*
    `DbReader` would not: that reader can poll a newer manifest and lift the
    frontier past records the scan reader has yet to see.) A bound key may be a
    non-`LogEntry` record or sit outside the active segment, so the decode checks
    the record type and segment and skips it when it cannot decode a `LogEntry`
    sequence.

This is a **conservative lower bound**, not the exact tip, on two counts: the
bound is an SST's lexicographically-largest *key*, not its largest *sequence*
(sequence is the lowest-order key term); and listing/metadata records sort after
log entries, so an SST topped by one of those contributes nothing. Both make the
frontier under-estimate — which is sound (a lower bound), at the cost of an
occasional redundant scan. The split matches reality: the writer knows its tip; a
standalone reader estimates it from durable metadata — sealed boundaries and SST
key bounds — and the estimate also reflects only data already flushed to SSTs
(records still in the WAL are not counted). A tighter, filter-derived source
(Future work) closes the per-SST gap by carrying each SST's true max sequence.

### Why the frontier is sound

Both frontier sources rest on the log's visibility being **prefix-consistent**: a
record at `M` being visible implies every `seq < M` is visible in the same snapshot.
For the in-process reader, LogDb expresses durability as a single monotonic scalar
(`storage.subscribe_durable` → `ViewTracker::advance` → `next_sequence`) and advances
the read view *before* publishing the watermark (`log.rs` `try_advance_durable`; the
Memory-mode ordering is the 0006/0005-era read-visibility fix), so an observer of the
frontier never outruns the snapshot.

For the standalone reader this reduces to SlateDB's manifest being a consistent
point-in-time snapshot under a single sequence-ordered writer — a SlateDB invariant,
named here as a dependency rather than proven in this crate. The SST bound is then
sound by the same property: a bound is the sequence of a record in some SST, so
everything below it is flushed and present in that snapshot. The maximum over the
decodable SST bounds is therefore itself a valid completeness frontier.

## Design — Part 2: sequence-aware SST filtering

Per Motivation, advancing the cursor prunes only whole segments; within a segment,
sequence is the lowest-order key term and cannot select SSTs. SST-level filtering
supplies the missing pruning.

### Mechanism

SlateDB's pluggable `FilterPolicy` provides the two hooks we need: a
`FilterBuilder::add_entry(&RowEntry)` that accumulates per-SST metadata as the SST
is written (and rebuilt on compaction output), and `Filter::might_match(&FilterQuery)`
that consults it at read time. A query carries a caller-supplied `FilterContext`
(an inline 64-byte payload, enough for a pair of `u64`s), so a scan can parametrize
the filter with its cursor. A policy is identified by `name()`, stored per-SST, so
a reader only applies a filter it can decode.

We register **two policies**, complementary by LSM level:

- **Sequence-range policy.** Every SST stores a table-level min/max sequence (~16
  bytes). Because every scan is segment-scoped (see Evaluation path) and the reader
  holds the segment's `start_seq`, the policy stores sequences **relative** to their
  segment — exactly what the key encodes — and the scan relativizes its cursor to
  `N_rel = N − start_seq` before passing it as context. The filter returns *no match*
  when `max < N_rel`. This is exact in **L0**, where single-writer, time-ordered
  flushes have disjoint, monotonic ranges — a caught-up cursor skips all but the
  newest L0(s).

  A single range is coarse on **compacted** SSTs: key-range compaction merges many
  keys across a wide window, so the overall `max` stays high even when the scanned
  key's records are all old. To recover precision, the builder additionally stores
  a per-key map `{key-hash → [min, max]}`, but only when it fits a byte budget `B`
  — otherwise it is omitted (all-or-nothing). Overflow is benign on high-key
  **L0s** (the table range already suffices there, and a caught-up cursor sits
  above them). It is *not* benign on a high-cardinality **compacted** SST: there
  the table `max` can stay high because of *other* keys, so an idle key's stale
  data stays unskippable — exactly the case the per-key map exists for. So
  all-or-nothing trades precision for predictable size, and on dense compacted
  SSTs that trade can cost an idle key a redundant scan until the partial-coverage
  strategy below (retain the lowest-`max` keys) is in play. Keys are stored as
  hashes, keeping the map compact for arbitrary key sizes; a collision unions two
  ranges, costing selectivity but never soundness. (Lookup is specified under
  Serialization.)

  The encoding is chosen per-SST at build time and rebuilt on compaction, so as a
  table's resident-key count falls under `B` it flips from table-level to per-key
  automatically — precision improves monotonically with compaction, under a hard
  per-SST size ceiling. Per-key ranges remain SST-wide: a sparse, long-lived key
  spanning the whole table is not narrowed (block-level ranges would be a secondary
  index, out of scope). For the dense compacted SST that overflows `B`, the
  mitigation is **partial coverage**: spend the budget on the lowest-`max` keys —
  whose data is oldest, hence most likely skippable by a caught-up cursor — and
  fall back to the table `max` for the rest. That keeps the size ceiling while
  preserving the per-key ranges that matter most.

  Storing relative sequences keeps the builder **stateless** — it reads `relative_seq`
  straight off the key, with no `segment_id → start_seq` resolution. This stays sound
  even when a compacted SST mixes segments: the per-key map keys on
  `hash(segment_id ‖ user_key)`, so each entry's range shares one segment base and is
  internally consistent, while the table-level range mixes bases but still bounds any
  single-segment query from above (a matching entry has `relative_seq ≤ table_max`, so
  it is never skipped; other segments only inflate the bound — lost selectivity, never
  a false negative). An earlier design stored *global* sequence and resolved
  `segment_id → start_seq` at build time; relative storage drops the resolver and all
  build-time state.

- **Prefix bloom policy.** The builder hashes the **log-key** prefix of each entry
  — the `subsystem | version | segment_id | record_type | user_key` prefix up to
  the user key — and at read time rules out any SST that cannot contain the
  scanned key. This is a *distinct* extractor from the SlateDB **segment
  extractor** used for segment routing, which keys on `segment_id` alone: a bloom
  over `segment_id` would prune nothing useful (every active-segment SST shares
  the id), whereas blooming on `(segment_id, …, user_key)` is what gives per-key
  pruning. It is strongest at the **compacted levels**: as data sorts, each SST
  covers a narrower key range, so fewer distinct logs per SST and a higher skip
  rate. It is weak in L0, where each flush spans many keys.

### Composition

The policies layer rather than split by level: the prefix bloom drops SSTs that
cannot contain the key, then the sequence policy drops survivors whose data for
the key is all below the cursor — and the watermark from Part 1 keeps that cursor
high. Together they collapse a tail follower's per-poll cost to the newest L0(s)
plus the one compacted SST that both contains its key and holds a record at or
past the cursor, independent of how large the log or active segment has grown.

### Sequence-filter serialization

The policy owns its filter blob; SlateDB stores it tagged with the policy `name()`
and returns it to `decode` on read, so this layout is private to the policy.
Sequences use the project's `var_u64` varint (`common::serde::varint`); fixed
64-bit values use big-endian `u64`, matching existing record encodings.

```
blob := version  : u8
        flags    : u8                    # bit 0: per-key map present
        table_min       : var_u64
        table_max_delta : var_u64        # table_max = table_min + delta
        if flags.per_key:
            count : var_u64
            entry × count, sorted by hash (binary-searched at query):
                hash      : u64          # 64-bit FNV-1a + fmix64 of the
                                         #   (segment, user_key) prefix; need not
                                         #   match the bloom hash (private to this
                                         #   filter); collisions union ranges
                min       : var_u64
                max_delta : var_u64
```

All stored sequences are **relative** to the entry's segment (see above). Each range
is stored as `min + (max − min)` so the varints stay small — the delta is bounded by
the records the SST holds, and relative sequences keep `min` small too.

The cursor reaches the filter through `FilterContext::Inline`: bytes `0..8` hold the
segment-relative cursor `N_rel = N − start_seq` as a big-endian `u64`, the remainder
reserved (zero). A missing context (or an unrecognized variant) means "cannot prune",
so the filter matches. `decode` parses the blob into
`{ table: (min, max), per_key: Option<[(hash, min, max)]> }`. The resume predicate
uses only `max` (`might_match` reads `N_rel`, looks up `hash(K)` in `per_key` when
present — binary search, absent → `table.max` — else `table.max`, and returns
`max ≥ N_rel`); `min` is carried for a future bounded-scan upper-bound prune and is
otherwise unused.

Compatibility: an incompatible format change bumps the policy `name`, so a reader
that cannot match the policy skips the filter entirely — no pruning, still sound.
The `version` byte covers compatible evolution; an unknown `version` is likewise
treated as match-everything.

### Evaluation path

SlateDB evaluates SST filters on `get` and `scan_prefix`, but **not** on plain
range `scan`. A per-key scan is naturally a prefix scan — all sequences for a key
share the prefix `…|segment_id|record_type|key`, with `seq` as the suffix — so it
runs as `scan_prefix` over that prefix, with the segment-relativized cursor supplied
as filter context (threaded through `StorageRead::scan_prefix_iter`). Pruning then
drops whole SSTs the cursor has outrun. A surviving SST is still read from the start
of the key's history; letting the scan *begin* at the cursor within the prefix needs
an upstream SlateDB change to accept a **sub-range** on `scan_prefix`, which would
avoid re-reading already-seen records — an optimization, not a correctness gap, and
out of scope here.

## Wire surface

The scan response gains one field, `next_sequence`; `follow=true` feeds it back as
the next poll's start. There is no existing global-sequence field on the read API
or wire today, so this is purely additive.

## Future work: a tighter frontier from the filter

The standalone reader's SST bound is the sequence of the bound *key*, not the SST's
maximum sequence, so within the newest SST it can fall short of the true tip — a
caught-up follower may re-scan that SST once before finding it empty. The
sequence-range policy's per-SST `max` is that maximum; since it is stored relative
to the segment, the global frontier is `start_seq + table_max` for an SST in the
active segment. Sourcing the frontier from that would close this part of the gap
(the newest SST's `max` is its exact tip), eliminating the redundant scan — while
changing only how `LogReadView.frontier` is computed, not the `next_sequence`
contract.

This is a tightening, not an unblock: the flush-granular frontier above already works
today with no upstream change. The tighter version needs a way to *read* a stored
filter value off an SST, off the scan path — `FilterPolicy` today exposes only
`might_match` (a per-query predicate during a scan). The cleanest fit is a public
filter-read on the existing `FilterPolicy` framework (expose the policy's payload, or
the decoded filter plus a downcast), paired with the `DbStatus` manifest subscription
to harvest the newest SST's `max` on refresh. SlateDB's own surfaces don't substitute:
`WriteHandle::seqnum` and the manifest live in SlateDB's internal seqnum space, not
LogDb's global sequence, and the manifest's per-SST max *key* is useless for max
sequence under the key-major layout.
