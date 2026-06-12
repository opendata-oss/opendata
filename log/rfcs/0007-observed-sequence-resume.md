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
- **Standalone reader:** the frontier is estimated from the maximum global
  sequence witnessed in the blocks the scan read (`witnessed_max + 1`). This is a
  sound lower bound; it can be tightened later (manifest extent, a durable tip
  key) without changing the contract.

This split matches reality: the writer knows its tip; a standalone reader can only
estimate it from what it reads.

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

## Wire surface

The scan response gains one field, `next_sequence`; `follow=true` feeds it back as
the next poll's start. There is no existing global-sequence field on the read API
or wire today, so this is purely additive.
