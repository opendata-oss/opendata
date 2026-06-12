# RFC 0007: Resumable Scans via `next_sequence`

**Status**: Draft

**Authors**:
- Jason Gustafson <12502538+hachikuji@users.noreply.github.com>

## Summary

This RFC adds a single value, `LogIterator::next_sequence()`, that reports the
exclusive global-sequence upper bound a scan observed. Callers resume the next
scan from that value instead of from their original start, so idle tail polling
no longer re-scans the empty `[start, tip)` range on every poll. Activity on any
key advances the bound for *all* keys, keeping repeated scans confined to the hot
portion of the log even for keys that are themselves idle.

## Motivation

Benchmarking found that idle tail polling is expensive: a follower querying
`scan(key, N..)` pays a full range scan every poll even when the key has no new
data, and that cost grows with the log as the empty `[N, tip)` range widens.

The scan already knows more than it returns. If `scan(key, N..)` observes the log
through global sequence `M` and finds nothing past `N`, then `key` provably has no
records in `[N, M)` — a durable fact, because the log is append-only
(no backfill below the tip). Surfacing `M` lets the caller resume at `M`, turning
each idle poll from `O(backlog)` into `O(delta since last poll)`.

## Goals

- Expose, per scan, the exclusive global sequence through which the scan observed
  the log, usable as a lossless resume cursor.
- Let activity on hot keys advance the resume point for idle keys (one watermark,
  shared across keys via the log's prefix-consistent visibility).
- Keep the contract mechanism-independent so the implementation can tighten the
  bound later without changing callers.

## Non-Goals

- Eliminating scans. The bound narrows scan *width*; it does not let a reader
  return "empty" without a scan. (A separate zero-work short-circuit — e.g. off
  the SlateDB manifest version — is possible later and does not change this API.)
- Server-side per-key cursor storage. Cursors are carried by the caller; the
  server holds at most a single shared watermark.
- Eventfully waking followers instead of polling (the watermark can later double
  as a wakeup source, but that is out of scope here).

## Design

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
  - a **witnessed watermark** — `observed`, one past the highest sequence any
    scan on the read view has read (`fetch_max` on yield). A scan of one key
    lifts it for all keys, so it advances *within* the active segment between
    seals. The estimate can be tightened further later (manifest extent, a
    durable tip key) without changing the contract.

This split matches reality: the writer knows its tip; a standalone reader can only
estimate it — from sealed boundaries plus what its scans actually read.

### Why the cross-key lift is sound

A scan of one key may advance an idle key's resume point because the log's
visibility is **prefix-consistent**: a record at `M` being visible implies every
`seq < M` is visible in the same snapshot. LogDb expresses durability as a single
monotonic scalar (`storage.subscribe_durable` → `ViewTracker::advance` →
`next_sequence`), and advances the read view *before* publishing the watermark
(`log.rs` `try_advance_durable`; the Memory-mode ordering is the 0006/0005-era
read-visibility fix), so an observer of the watermark never outruns the snapshot.

For the standalone reader this reduces to SlateDB's manifest being a consistent
point-in-time snapshot under a single sequence-ordered writer — a SlateDB
invariant, named here as a dependency rather than proven in this crate.

The witnessed watermark adds one requirement: a scan must read the snapshot and
`observed` together (under the read lock at open). The reader's snapshot only
moves forward, so anything in `observed` was witnessed in a snapshot no newer
than the current one; by prefix-consistency the current snapshot is therefore
complete through `observed`, and an empty idle scan genuinely has nothing below
it. `scan_with_options` already runs under that lock, so the ordering is free.

## Wire surface

The scan response gains one field, `next_sequence`; `follow=true` feeds it back as
the next poll's start. There is no existing global-sequence field on the read API
or wire today, so this is purely additive.

## Future work: SST-level pruning via filter policies

`next_sequence` advances a reader's cursor, but advancing it *within a segment*
does not by itself reduce scan cost. The key layout sorts `segment_id` high and
`relative_seq` low, so a per-key range scan over `[…|key|N, …|key|end)` still
overlaps every active-segment SST that spans `key` — sequence has no pruning
power over SST selection. The cursor prunes only at **segment** granularity (a
different `segment_id` is a disjoint key range). So a tail follower still probes
every active-segment L0 on each poll; the only lever today is keeping that L0
count small via seal/compaction tuning.

SlateDB 0.13's pluggable `FilterPolicy` recovers finer pruning: a
`FilterBuilder::add_entry(&RowEntry)` accumulates per-SST metadata at build time,
and `Filter::might_match(&FilterQuery)` consults it at read time with
caller-supplied `FilterContext`. Two policies, complementary by LSM level, are
planned:

- **Global-sequence-range policy.** Each SST records its min/max global
  sequence; a scan passes its cursor `N` as context and skips any SST whose
  `max < N`. Most effective in **L0**, where SSTs are single-writer,
  time-ordered flushes with disjoint, monotonic sequence ranges. Paired with the
  cursor advancement in this RFC, it keeps a caught-up follower's reads focused
  on the most recent L0s. It is weak at compacted levels — key-range compaction
  yields wide, overlapping sequence ranges — but a caught-up cursor sits above
  all compacted data, so those SSTs are skipped wholesale anyway.
- **Prefix bloom policy** (in progress on branch `worktree-logdb-prefix-bloom`).
  Skips an SST that cannot contain the scanned key. Becomes *increasingly*
  effective as data is compacted and sorted: each compacted SST covers a
  narrower key range, so fewer distinct logs per SST and a higher skip rate. It
  is weak in L0, where each flush spans many keys.

The two are strong exactly where the other is weak: the sequence-range policy
prunes recent L0s, the prefix bloom prunes the sorted lower levels, and the
cursor advancement in this RFC keeps the sequence policy fed. Together they make
the within-segment cursor advance actually reduce SST probes.

For SST filters to apply, a per-key scan runs as `scan_prefix` (plain range
`scan` does not evaluate filters) over the key's prefix, with the cursor `N`
supplied as filter context.

The global-sequence-range policy also doubles as the standalone reader's
**frontier source**, superseding both the seal floor and the witnessed
watermark. The maximum `max` across the SSTs in the reader's snapshot is its
durable tip: by prefix-consistency, if any SST holds global sequence `M` then
everything below `M` is durable and present, so the reader is complete through
it. It is tighter than the seal floor (it includes the active segment's flushed
data) and, unlike the witnessed watermark, independent of which keys are
queried. The newest L0's `max` is the tip, so the reader harvests it on refresh
rather than on the scan path. This only changes how `LogReadView.frontier` is
computed — the `next_sequence` contract is unchanged.
