# RFC 0007: Time Index

**Status**: Draft

**Authors**:
- [Jason Gustafson](https://github.com/hachikuji)

## Summary

This RFC adds a time dimension to LogDb queries. Today a log is addressable only by
its global sequence number; this RFC introduces a per-segment mapping from sequence to
timestamp so that a caller can query a single key over a wall-clock time range —
`[t0, t1]` or "the last *duration*" — instead of having to know sequence numbers.

The design leans entirely on timestamps that the underlying store (SlateDB) already
assigns and persists. LogDb does not invent a clock or store its own per-record
timestamp; it adopts SlateDB's `create_ts`, learns it from the write result, and uses
it to drive a coarse, in-memory time→sequence index and a reactive segment-rolling
loop. Because SlateDB's clock is monotonic and durable across restarts, the design
avoids the recovery scans and ordering hazards that a hand-rolled time index would
otherwise require.

## Motivation

LogDb is designed for a high cardinality of separate logs — one per user, device,
agent, session, or other entity. Workloads of this shape look very different from the
messaging *pipelines* that traditional log systems are built around. A pipeline
consumer reads a topic end to end and carries its own cursor; an opaque offset is a
perfectly good address because the consumer only ever resumes from where it left off.

The workloads LogDb serves are more database-like, closer to a time-series store: a
caller issues an ad-hoc query against one entity's history, holding no prior cursor.
"Show me this session's activity around 14:30," "this device's readings over the last
hour," "this account's events yesterday." For these queries the global sequence number
is the wrong coordinate — it is an internal counter that means nothing to a caller who
has not been following the log. Users will inevitably need to map the sequence
dimension of the log into something meaningful, and the natural, universal coordinate
for a log is wall-clock time.

The rest of the system already treats time as first class on the write and lifecycle
side. RFC 0005 expires data by age — we reason about "older than T" to *drop* records —
yet there is no way to *read* by that same axis. RFC 0006 makes the gap explicit: its
query benchmark uses the global sequence as a *proxy* for time and notes that the proxy
"goes away" once a time index lands. This RFC lands it.

A time-addressed read needs two things from the design: a timestamp on every record, and
a cheap way to translate a time bound into the global sequence the storage engine already
understands. Both fall out naturally — record timestamps turn out to be monotonic in
sequence, so the translation is a binary search over a coarse index — as the rest of this
RFC develops.

## Goals

- **Per-key time seek.** Map a `(key, time)` to the sequence of that key's first record at
  or after `time`, so a caller can start an existing `scan` at a wall-clock position and
  read forward — for a window or a trailing duration.
- **Exact lower bound, caller-driven end.** The seek returns the key's first record with
  `create_ts ≥ time` exactly — no index-granularity rounding leaks to the caller — and the
  caller terminates the read at its upper bound by reading `create_ts` on returned entries.
- **No new clock, no duplicate timestamp.** Reuse SlateDB's per-record `create_ts` as
  the authoritative record time rather than minting or storing a parallel timestamp.
- **Restart-safe by construction.** Preserve timestamp monotonicity across writer
  restarts without a recovery scan of prior records.
- **Cheap index.** A coarse, in-memory, binary-searchable map with a configurable step,
  built as a side effect of the normal write path.

## Non-Goals

- **Caller-supplied timestamps.** Record time is the store-assigned `create_ts`. Letting
  callers stamp their own event time is a plausible future extension but is not part of
  this RFC.

## Design

### Record time is SlateDB's `create_ts`

SlateDB stamps every stored row with a `create_ts` (milliseconds since epoch) and a
sequence number, and returns the `create_ts` assigned to a write synchronously in the
write result. LogDb already issues one SlateDB write batch per append but currently
keeps only the sequence number from the result and discards the timestamp.

This RFC adopts `create_ts` as the authoritative record time. Two consequences follow
from how SlateDB assigns it:

- **One timestamp per batch.** SlateDB reads its clock once per write batch and applies
  that tick to every row in the batch. Since a LogDb append is one batch, every record
  in an append shares a single `create_ts`. This is the right grain — a log's records
  are naturally grouped by the append that produced them — and it is why this RFC has no
  sub-batch time resolution.
- **Learned, not provided.** There is no API to set `create_ts` on a write; it always
  comes from SlateDB's clock. So LogDb *learns* each batch's timestamp from the write
  result rather than choosing it. No read-after-write is needed — the value is returned
  by the write call itself, even when the write is not awaited for durability.

Plumbing: the storage layer's write result is extended to carry `create_ts` alongside
the sequence number, and the writer propagates it.

### Monotonicity and restart safety

The index translation and the segment time bounds both rely on `create_ts` being
monotonic in sequence. SlateDB provides this:

- Within a run, its clock enforces forward-only ticks, and LogDb writes batches
  serially, so a later batch never has a smaller `create_ts` than an earlier one.
- Across a restart, the clock is re-initialized from a durable tick persisted in the
  manifest and continues forward from there. Any record written after a restart has a
  `create_ts` no smaller than every record durable before it.

This durability is what makes the design tractable. A hand-rolled time index would have
to recover the maximum timestamp of the previous segment on startup — and because
records are stored key-major (`user_key | relative_seq`, RFC 0001), "the maximum
timestamp" is not a cheap lookup; it implies a near-full segment scan. With SlateDB's
durable monotonic clock, the segment-boundary invariant we need — *a new segment's
records are always later than every record in prior segments* — holds **by
construction**, with no recovery scan and no high-water bookkeeping.

### Reactive segment rolling

Because `create_ts` is learned from the write result rather than known beforehand,
segment rolling becomes a post-write reaction rather than a pre-write decision. The
append loop is:

```
allocate sequences → write batch into the current segment → read create_ts from result
  → maybe append a time-index record  (if create_ts − last_index_ts ≥ index_step)
  → maybe seal and roll               (if create_ts − segment_first_ts ≥ seal_interval;
                                        the new segment opens on the next append)
```

No wall clock is read at segment-assignment time: a new segment's only pre-write input is
its `start_seq` (an identity, not a timestamp), and its time span is purely *observed*
post-write. The pre-write start-*time* requirement that the boundary invariant would
otherwise impose is gone — the durable clock (above) already supplies the ordering.

This composes with size-based sealing (the RFC 0006 prerequisite): size sealing reacts
to accumulated record bytes, time sealing reacts to the returned `create_ts`; both are
post-write reactions and a segment seals when either trips.

Because rolling is purely write-driven, an idle instance never seals (no write, no roll).
The first pass accepts this — segments are global, so an idle instance is one taking no
writes at all, which the target workload (RFC 0006) rarely is — and leaves an optional
wall-clock idle-seal timer to a later iteration.

**The straggler tradeoff.** Because the roll decision happens *after* the batch is
written, the batch that trips the seal interval lands in the *old* segment. After a long
gap between writes, a lone "straggling" write therefore sits at the tail of the prior
segment, widening that segment's observed time span. This is acceptable, and the reason
is structural: query correctness does not depend on segment time alignment at all, so a
widened span only loosens segment pruning efficiency and retention granularity, never
correctness. Avoiding the straggler would require detecting the gap *before* writing — a
pre-write clock read — which reintroduces exactly the recovery and ordering hurdles the
reactive model removes. The straggler is the price of those removals, not a separate
defect.

### The time index

The index is **two-level**, both levels derived purely from observed `create_ts`:

1. **Coarse, eager — per-segment lower bound.** Each segment's `base_ts` lives in its
   `SegmentMeta` (see Segment metadata) and is already resident in the in-memory segment
   cache; a segment's time range runs up to the *next* segment's `base_ts`. Because
   segments are time-ordered, a time `t` lands in its owning segment directly from the
   cache, with no extra I/O — the time analogue of the existing sequence-based covering
   (RFC 0002).
2. **Fine, lazy — per-segment index records.** Each segment stores a coarse, sorted set
   of `(relative_seq → create_ts)` samples — one small record per sample — appended as the
   write loop crosses each `index_step`. For the owning segment the samples are loaded and
   binary-searched to the greatest sample at or before `t` — a sequence floor, rounded down
   so it is never past a valid record. The mapping is monotonic, so binary search suffices.

The split matches the workload (RFC 0006): the coarse bounds are tiny and always in
memory, while the fine samples load only for the segment a seek resolves in, so a
recency-biased query stream keeps hot segments' samples cached and leaves cold segments'
on disk until needed.

Properties of the fine level:

- **A soft accelerator.** The samples only narrow where to look; the record timestamps
  they point at are the durable source of truth. If a segment's most recent samples are
  lost (e.g. a crash before they were persisted), a query near that segment's tail rounds
  to the previous sample and scans a slightly wider sequence range. Correctness is
  unaffected; only pruning degrades. The index never owes a synchronous write or a
  mandatory rebuild.
- **A user-tunable step.** `index_step` is a knob trading index size against query
  precision. The step is expressed in time, and the first pass makes no effort to cap
  index size beyond it: a segment's byte size is already bounded by the seal threshold, so
  its sample count is inherently bounded, and a denser tracker (downsampling, hybrid
  triggers) is left to later iterations.

### Index record schema

The fine-level samples are stored as **one small record per sample**, in the segment's
*own* keyspace so they are routed to the same SlateDB segment as the segment's data and
therefore compact and expire with it (RFC 0005) — no separate lifecycle to manage. This
first pass keeps the index plain: append-only puts, no merge operator. A denser
merge-accumulated layout is a deliberate future optimization (see Alternatives).

**Record type and key.** A new `RecordType::TimeIndex` (tag byte `0x50`) joins the
existing `LogEntry`/`SeqBlock`/`SegmentMeta`/`ListingEntry` tags (RFC 0001). The sample's
`relative_seq` is the key suffix, so samples sort by sequence (= time order) and a
segment's whole index is one contiguous prefix scan — the same shape as `ListingEntry`:

```text
| subsystem (u8) | version (u8) | segment_id (u32 BE) | record_type (u8=0x50) | relative_seq (var_u64) |
```

`relative_seq` is relative to the segment's `start_seq`, reusing the `LogEntryKey`
convention (RFC 0001), so it stays small and varint-compact.

**Value.** The sample's timestamp as a delta:

```text
| ts_delta (var_u64) |
```

`ts_delta` is `create_ts − base_ts`, where `base_ts` is the segment's first observed
`create_ts`, stored in `SegmentMeta`. Both `relative_seq` and `ts_delta` are monotonic
and small, and SlateDB block compression collapses the dense, near-uniform columns
further.

**Write cadence.** Records are written lazily: since `create_ts` is only known after a
write, the sample crossed by one append rides the *next* append's batch — no separate
flush, no rebuild.

**Loading.** A segment's index is a single contiguous prefix scan over its `0x50`
records, decoded into a sorted in-memory array and cached on first touch.

### Query API

The index is exposed as a single seek primitive; everything else reuses the existing
`scan(key, seq_range)`:

```rust
// on the reader; async + fallible (it may load the key's boundary block)
fn find_sequence(&self, key: &Key, time: Timestamp) -> Result<Option<Sequence>>
```

`find_sequence` returns the smallest `Sequence` at which `key` has a record with
`create_ts ≥ time`, or `None` if the key has no record at or after `time`. A time-window
read composes from it:

```
start = find_sequence(key, t0)?      // exact lower bound; None ⇒ nothing to read
scan(key, start..)                   // read forward, stop at the first create_ts > t1
```

The lower bound is exact and the caller terminates at the upper bound, so no
index-granularity rounding is visible to the caller, and there is no separate tail
concept — a trailing duration is just `find_sequence(key, now − d)`.

It resolves in the two index levels, then refines per key:

1. **Coarse** — locate the segment whose `[base_ts, next base_ts)` contains `time`.
2. **Fine** — binary-search that segment's index records to the sequence floor at or before
   `time` (rounded down, so never past a valid record).
3. **Refine** — seek `key` from that floor and advance over its records, skipping any with
   `create_ts < time`; return the first with `create_ts ≥ time`. This per-key skip is what
   makes the bound exact and spares the caller from filtering the leading edge — and it is
   cheap: one key's records within a single `index_step` window, contiguous in storage and
   left warm in cache for the follow-up `scan`.

`create_ts` is surfaced on returned scan entries — it is the caller's upper-bound stop
condition, and a time-oriented consumer wants `(time, value)` back regardless.

### Segment metadata

`SegmentMeta` keeps `start_seq` as the segment's identity, and its `start_time_ms` becomes
`base_ts` — the segment's first observed `create_ts`, learned post-write (consistent with
the reactive model) rather than chosen before the first write. `base_ts` does double duty:
the delta base the fine index records' `ts_delta` values decode against, and the segment's
lower time bound. It is immutable, written once.

A segment needs no stored *upper* bound. Segments are time-ordered and non-overlapping —
the durable clock guarantees the next segment's first record is later than all of this
one's — so segment *i* owns `[base_ts_i, base_ts_{i+1})`: its upper bound is simply the
next segment's `base_ts`, with the active segment unbounded above. This is exactly how
retention (RFC 0005) already infers a segment's end time from the next segment's start, so
`base_ts` is a drop-in for the old `start_time_ms`, sourced from `create_ts` instead of a
pre-write wall clock.

## Alternatives

- **A LogDb-owned monotonic clock and a stored per-record timestamp.** Rejected:
  SlateDB already assigns a durable, monotonic `create_ts` per row and hands it back on
  write. Minting our own clock and persisting a parallel timestamp duplicates that work
  and — worse — reintroduces the cross-restart recovery problem (scanning to recover the
  previous segment's max time) that the durable clock eliminates.
- **Reusing SlateDB's sequence↔timestamp tracker / admin API directly.** Rejected as a
  building block: it operates on SlateDB's internal sequence numbers, not LogDb's global
  `Sequence`, so it cannot translate a time bound into the sequence range our per-key
  scan needs. Retained as the reference design for the index's sampling and rounding.
- **Pre-write rolling on a wall clock (today's model).** Rejected: deciding the roll
  before the write requires reading a clock at assignment time and recovering a
  high-water timestamp on restart. The reactive model trades those away for the
  straggler, which costs nothing on correctness.
- **A dense, per-record time index.** Rejected as unnecessary: a coarse index gets the
  seek close, and the short per-key forward skip lands it exactly on `create_ts ≥ time`, so
  a dense index would only add cost for no gain in precision.
- **A merge-accumulated per-segment blob instead of per-sample records (deferred).**
  Folding a segment's samples into a *single* record grown by SlateDB `merge` operands
  drops the repeated per-sample key framing and packs the timestamps into a
  harder-compressing column. It needs a merge operator and pinned-down read/compaction
  semantics, and at coarse granularity the size win is modest (a segment's index is
  KB-scale either way) — so it is deferred; revisit if index storage or load cost matters.
- **Pruning by per-segment min/max time alone, with no fine-level index.** Rejected as
  insufficient on its own: it selects candidate segments but cannot narrow *within* a
  segment, so it degrades to scanning the key across each candidate's full sequence
  range. The two-level design keeps the min/max as the coarse selector (cheap, always
  resident) and adds the fine per-segment index for the within-segment translation.

## Open Questions

- **Default `index_step`.** The step is a user knob (above); what default balances query
  precision against index size, and should it also admit a record-count trigger ("emit
  every `index_step` ms *or* every N records") to bound sample spacing under bursty
  writes? Refinements like downsampling are deferred.

## Updates

| Date       | Description |
|------------|-------------|
| 2026-06-01 | Initial draft |
