# RFC 0007: Global Scan

**Status**: Draft

**Authors**:
- [Jason Gustafson](https://github.com/hachikuji)

## Summary

LogDb lets a writer fan out across an enormous number of keys, each its own
ordered stream. Reading the data back is currently only possible one key at a
time. This RFC explores a **global scan**: a single API that reads entries
across *all* keys within a sequence range, without the caller enumerating keys
first.

This is a brain dump more than a specification. The interesting part of this
problem is not the API surface — it's that the natural way to ask the question
("give me everything from sequence X onward") is hard to answer efficiently
given how LogDb lays data out on disk, and harder still if you want to *follow*
the log as it grows. The bulk of this document explains why, walks through the
ideas we considered, and lands on an approach rooted in **safe checkpoint
advancement** over the SlateDB manifest, with the **WAL reader** as the
mechanism for tailing new data.

## Motivation

LogDb's core bet is that you can use keys liberally — millions of them, created
on the fly as access patterns evolve — because each key is an independent log.
That works well for writes and for targeted per-key reads.

It works poorly for any consumer that wants *all* the data: a CDC pipeline, a
full export, a reindexing job, a backup, an analytics sink. Today such a
consumer has to `list_keys` and then `scan` each key individually. With a large
key space that is enormously costly: one (or more) storage operation per key,
no amortization, and a key-listing step that itself grows with the cardinality
of the log. The whole point of using many keys turns into a tax at read time.

A global scan removes that tax. The consumer says "read the log from here" and
gets a single stream of entries across every key, paying roughly in proportion
to the *data* it reads rather than the *number of keys* that data is spread
across.

## Goals

- Read entries across all keys in a sequence range as a single stream.
- Cost proportional to data scanned, not to key cardinality.
- Support resuming from a durable position (checkpoint/cursor).
- Eventually, support *following* the log — surfacing new entries as they land.

## Non-Goals

- Strict global-sequence ordering across the entire result (see Open Questions —
  we may settle for key-grouped order during backfill).
- Secondary indexes or filtered scans beyond a sequence range / key prefix.
- Exactly-once delivery semantics on top of the scan (a consumer concern).

## Background: how data is laid out

Two facts about LogDb storage drive everything below.

**1. Within a segment, entries are ordered by key, then sequence — not by
sequence.** A log entry's storage key is

```
| subsystem | version | segment_id (u32 BE) | record_type (0x10) | terminated_key | relative_seq |
```

so a single segment's entries sort as `(user_key, relative_seq)`. This is
deliberate: it gives per-key scans their locality. It also means that a global,
sequence-ordered view is *not* something the layout hands you for free.

**2. Sequence numbers are assigned monotonically at append time.** Segments are
contiguous, non-overlapping sequence ranges (segment owns
`[start_seq, next.start_seq)`). The absolute sequence of any entry is
recoverable from its key (segment `start_seq` + `relative_seq`).

## Why position tracking is difficult

A global scan needs a **resumable position** — a cursor a consumer can persist
and resume from. The trouble is that the dimension the caller wants to track
(sequence) is not the dimension the data is sorted by (key).

Mapping a sequence *range* to a *segment* range is easy. The problem is locating
entries *within* a segment. Because a segment is sorted by key, the entries for
a given sequence window are scattered across every key in the segment; there is
no contiguous byte range to seek to, and a sequence number alone does not tell
you where to start reading.

This splits cleanly along one line — whether the segment is **sealed**:

- **Sealed segments are easy.** Once a segment seals, its contents are frozen.
  The order is fully determined, so a single prefix scan over the segment's
  `0x10` records yields every entry in `(key, seq)` order, and the cursor is
  simply "the last storage key I read." Resumption is exact and cheap.

- **The active (unsealed) segment is the hard case.** Its total order is not
  determined until the last write lands. Concretely, a cursor based on "last
  key read" is unstable: a *new key* can be written that sorts *behind* the
  cursor, and a position-based cursor would skip it forever. The active segment
  grows in two directions — new keys appear anywhere in the key order, and
  existing keys get higher-sequence entries appended — and a single positional
  cursor cannot track both.

The deeper framing: **position-based isolation of "already read" data breaks
whenever the underlying order can change beneath you.** That is exactly the
active segment, and — as we'll see — it's also what happens when you advance a
checkpoint across compaction.

## Ideas considered

This is the path we actually walked, including the dead ends, because each one
explains why the next was needed.

### 1. Scan by sequence directly

The obvious idea — treat the sequence range as a scan range — fails immediately:
the layout is key-major within a segment, so there is no sequence-ordered byte
range to scan. Rejected at the first hurdle, but it frames the whole problem.

### 2. Sealed-only / defer the active segment

Restrict the global scan to sealed segments; the active segment becomes visible
only once it seals. This is fully correct and trivially resumable (positional
cursor on immutable data). The cost is tail latency equal to the seal interval —
a consumer can't see fresh data until the segment rolls. Viable as a floor, but
not "following the log."

### 3. Per-segment key cursor

Generalize the sealed-segment cursor: `(segment_id, last_storage_key)`, with the
sequence range acting as an inclusion filter rather than an ordering key. This
nails sealed segments. It does **not** solve the active segment, for the
inserts-behind-the-cursor reason above.

### 4. Snapshot watermark + value isolation

Lean on the monotonic-sequence invariant. At any instant, if `W` is the current
max sequence, the set `{ seq ≤ W }` is **frozen and complete** in every segment,
including the active one — no later append can land a sequence `≤ W`. So:

- isolate "already read" data by **value** (the log sequence in the key), not by
  position: emit only `seq > W`, advance `W` to the max seen once a pass
  completes;
- get a consistent pass by pinning a snapshot so nothing inserts behind you
  mid-pass.

This is *correct* and needs no new storage structures. Its weakness is cost: the
active segment isn't sequence-ordered, so advancing `W` means **re-scanning the
whole active segment and filtering** — O(active segment) per advance. Bounded
(the active segment seals), but wasteful for a tailing consumer.

### 5. Checkpoint pinning — and where it dead-ends

A SlateDB **checkpoint** is a persistent reference to a manifest version; it is
the durable cousin of a snapshot and prevents GC of whatever that manifest
references. The simple idea: attach a checkpoint, read all segments, then
re-establish the checkpoint on the latest manifest and repeat.

This gives a consistent, GC-safe view *for a single point in time*. But it has a
catch that felt fatal at first: a checkpoint lets you **exclude** new entries
while iterating a fixed point, but once you **advance** the checkpoint it gives
you no way to **isolate** the data you already read. The newer entries you want
are mixed, by key, into the sorted runs alongside everything you've already
seen. Position can't separate them.

The escape is the same value-isolation trick as (4): the log sequence in the key
*does* separate new from old, because slate sequence and log sequence are
co-monotonic — anything written after the old checkpoint has a strictly higher
log sequence. So advancing a checkpoint is *correct*. But on its own it inherits
(4)'s cost: to find the new entries smeared through the sorted runs, you re-read
and filter. Checkpoints buy consistency and GC-safety; they don't, by
themselves, buy cheap deltas.

### 6. The WAL reader as a change feed

The recurring wish in (4) and (5) is "give me the delta in write order, cheaply."
SlateDB's WAL is exactly that: a write-ordered log of changes that exists
*before* data is merged into key-sorted runs. SlateDB exposes it publicly via
`WalReader` — list WAL files by ascending id, iterate each as `RowEntry`s in the
order they were written (i.e. sequence order), with each row carrying its
sequence. It is explicitly intended for CDC.

This sidesteps the sorted-run problem entirely: tailing reads each WAL file
exactly once, in order, with no re-scan and no interleaving with already-read
data. The log sequence is still recovered from the entry key; the WAL's job is
purely "hand me changes in order." The costs move to WAL retention (files are
GC'd after compaction, so a follower needs enough retention or a checkpoint to
pin them) and to WAL being enabled at all.

### 7. Direction: safe checkpoint advancement + WAL reader

The pieces compose into one model, rooted in checkpoints walking the manifest,
with the WAL as the tail accelerant:

- **Anchor.** Hold our own checkpoint at manifest `M`. It pins everything `M`
  references — sorted runs, L0 SSTs, and WAL files back to `replay_after_wal_id`
  — so GC cannot remove data we haven't read. Crucially, *we* control when the
  checkpoint advances, gating it on consumption.

- **Backfill.** Read the tree pinned by `M` once, emitting entries in the
  requested sequence range. (Order during backfill is key-grouped per segment —
  see Open Questions.)

- **Tail by manifest diff.** Poll for a newer manifest `M'`. The manifest names
  exactly what is new: the WAL id range `M.next_wal_sst_id .. M'.next_wal_sst_id`
  and the new entries in `l0()`. We read only those — **O(delta), not O(active
  segment).** Already-read data lives in files we never reopen.

- **Ignore reorganizations.** Compaction churns the lower levels but introduces
  no new logical entries; it only reshuffles data we've already emitted. The
  tailer consumes only the **ingestion frontier** (new WALs / new L0s) and skips
  compaction outputs, using the sequence watermark to tell frontier from reorg.

- **Advance.** Only after the delta between `M` and `M'` is consumed do we
  replace the checkpoint with one on `M'`, releasing `M`'s now-unneeded files.
  This is precisely the loop `DbReader` already runs internally
  (`should_reestablish_checkpoint` → `replace_checkpoint` →
  `reestablish_checkpoint`), but with advancement gated on *our* progress rather
  than a poll timer.

The backfill→tail handoff is stitched by the sequence watermark: the tail drops
any `seq ≤` the backfill's max, so nothing is double-emitted.

**WAL-disabled fallback.** If the WAL is off, the tail source becomes the new L0
SSTs from the manifest diff, read via `SstReader` (`open_with_handle` +
`read_block`). This works but is lower-level than `WalReader` — there is no
turnkey per-L0 iterator — and yields per-L0 key-sorted batches rather than
strict sequence order. The `common::storage::sst_blocks` count path is the
existing precedent to extend from counting to iterating.

| | WAL enabled | WAL disabled |
|---|---|---|
| Tail source | `WalReader`, new WAL ids in order | new L0 SSTs via `SstReader::read_block` |
| Tail order | strict sequence | per-L0 key-sorted batches |
| Latency | WAL flush interval | memtable flush interval |
| Effort | turnkey | block-walk to build |

### 8. Ergonomics: the minimal SlateDB hooks we'd want

Approach (7) gets us most of the way, but it leans on low-level pieces and a few
awkward corners. SlateDB is never going to ship a CDC package shaped exactly for
LogDb, and we shouldn't ask it to. The useful question is narrower: what is the
*minimal* set of hooks that would make "track new data, GC-safely, in order"
integrate with checkpointing without friction? A survey of where today's API
fights us, roughly in order of leverage:

- **Pin a checkpoint at a known manifest version — and get the diff on advance.**
  Today `Db::create_checkpoint(scope, options)` only pins "now" (`All` /
  `Durable`) or forks from an existing checkpoint (`CheckpointOptions::source`).
  There is no "checkpoint manifest version `N`." So our advance is necessarily
  "pin the latest, then read back which version we landed on"
  (`CheckpointCreateResult::manifest_id`) rather than "pin the version I just
  diffed." It works, but it means the version we pin and the version we reasoned
  about can differ. The hook we actually want is an **atomic
  advance-and-diff**: move the cursor's checkpoint to the latest manifest and
  return `(old_manifest, new_manifest)` (or directly the set of new WAL ids / new
  L0 SSTs / retired files). That single primitive removes both the
  version-skew problem and the hand-rolled diffing below.

- **Reader-owned checkpoint lifecycle.** `create_checkpoint` lives on `Db` — the
  writer. A standalone `LogDbReader` (possibly a different process) wants to
  establish and advance *its own* GC-safe cursor against the manifest store.
  `DbReader` already does exactly this internally (`replace_checkpoint`,
  `reestablish_checkpoint`) and the `admin` module can manipulate checkpoints at
  the manifest-store level, but neither is a clean "reader holds a cursor"
  surface. Exposing the reader-side checkpoint cursor would let us gate
  advancement on *our* consumption — the core safety property — without a writer
  handle.

- **A public SST-level row iterator.** `WalReader` is pleasant: `WalFile::iterator()`
  hands back `RowEntry`s. There is no equivalent for L0 SSTs or sorted runs — the
  public surface stops at `SstReader::read_block` / `index`, so the WAL-disabled
  fallback means reassembling iteration block-by-block. SlateDB already has an
  internal `SstIterator` (the WAL reader and checkpoint tests use it); exposing a
  ranged row iterator over an `SsTableView` / `SortedRun` would make reading the
  ingestion frontier from L0 as clean as reading it from the WAL, and would make
  the backfill read of the pinned tree first-class too.

- **WAL retention tied to the cursor (CDC retention).** Our ordered tail assumes
  the WAL files we need are still present. They survive only because our
  checkpoint pins back to `replay_after_wal_id` — but a WAL that gets compacted
  into L0 before we read it forces a fall back to the (unordered, lower-level) L0
  path. A hook that retains the WAL from a cursor-held id forward, decoupled from
  compaction, would let the tail *always* be served in order from the WAL and
  make the L0 fallback a true cold-start/backfill path rather than a steady-state
  concern.

- **A manifest-diff helper.** We currently compare `next_wal_sst_id`, `l0()`, and
  `compacted` by hand; the manifest code itself flags the L0-list ordering
  assumption as brittle. Even without the atomic advance-and-diff above, a small
  "what changed between these two manifests" helper would remove a class of
  foot-guns.

None of these is strictly required — (7) is implementable on today's API. But the
first two (atomic advance-and-diff, reader-owned cursor) are high leverage: they
turn the whole loop into "advance the cursor, here's the new data, GC is safe,"
which is the ergonomic target. The SST iterator and WAL-retention hooks mostly
matter for making the WAL-disabled and slow-consumer cases first-class rather
than fallbacks. We should scope which of these we attempt as upstream SlateDB
contributions versus work around in LogDb.

## Alternatives

- **Secondary sequence-ordered index.** Write a second record per entry keyed by
  `(segment, relative_seq)` so the log is physically readable in sequence order.
  Makes both ordering and resumption trivial and positional, at the cost of
  doubling write amplification (or maintaining an in-memory tail the writer
  owns, invisible to an out-of-process reader). Held in reserve if the
  checkpoint/WAL approach proves too costly for following.
- **In-memory write-ordered tail in the writer.** A co-located consumer could
  follow the active segment from the writer's append-ordered memory and fall
  back to storage for sealed segments. Cheap and ordered, but does not serve a
  standalone `LogDbReader` in another process.

## Open Questions

- **Is following in scope for v1, or is v1 one-shot (backfill to a checkpoint and
  stop)?** One-shot removes all the cross-advance machinery; following is the
  reason the hard parts exist.
- **WAL-required for v1, or must WAL-disabled work from day one?** Decides whether
  we build the L0 block-walk now or defer it.
- **Our own checkpoint vs. piggybacking the `DbReader`'s.** Our own gives
  advancement-gated-on-consumption (safe); piggybacking is less code but the
  reader advances on its own schedule and could GC ahead of us.
- **Output ordering.** Backfill is naturally key-grouped per segment; the WAL
  tail is strict sequence order. Is "key-grouped while catching up, sequence
  order once live" acceptable, or do we need one order throughout? Strict
  sequence order during backfill reintroduces a per-segment k-way merge.
- **API shape.** Likely a method on the `LogRead` trait (e.g. `scan_all(seq_range)`)
  returning a new iterator type, with a serializable cursor for resumption.

## Updates

| Date       | Description |
|------------|-------------|
| 2026-06-04 | Initial draft |
