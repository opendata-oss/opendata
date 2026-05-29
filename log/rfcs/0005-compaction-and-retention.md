# RFC 0005: Compaction and Retention

**Status**: Draft

**Authors**:
- Jason Gustafson <12502538+hachikuji@users.noreply.github.com>

## Summary

This RFC defines OpenData-Log's compaction strategy on top of SlateDB, and
the retention semantics that ride on top of it. Retention physically
removes data by issuing drain compactions; the same custom
`CompactionScheduler` handles drains alongside active- and sealed-segment
policies. Treating them as one design avoids fragmenting policy across
components.

The strategy is state-aware over a segment's lifecycle: while active,
compact aggressively to relieve L0 backpressure but defer
read-amplification work; on seal, run a one-shot compaction that produces
the segment's steady-state layout; on expiry, drain. The retention protocol
is grounded in the durable system segment: `SegmentMeta` records are the
source of truth for live segments, the writer deletes them on expiry, and
the compactor garbage-collects any SlateDB segment in the manifest without
a corresponding `SegmentMeta`. The initial release supports time-based
retention only.

## Motivation

OpenData-Log needs a compaction strategy and a retention story. RFC 0002
introduced logical segments and made each segment a unit of compaction
via a SlateDB prefix extractor (RFC 0024 in SlateDB terms), and called
out segment-based deletion as future work. Both problems are unresolved.

They share the same machinery. SlateDB executes retention via
`CompactionSpec::drain_segment` — a kind of compaction — so the
component that picks compactions is also the one that issues drains.
Folding them into a single `CompactionScheduler` keeps the protocol
uniform; splitting would force two code paths to coordinate inside
SlateDB's compactor.

LogDb's workload also has properties that change what compaction is
*for*:

- **Append-only.** No overwrites or tombstones, so the primary benefit
  of merging sorted runs — reclaiming space from superseded versions —
  is zero. Within a segment, sorted runs produced sequentially from L0
  batches are sequence-disjoint per key, so a scan touching multiple SRs
  reads disjoint chunks rather than merge-deduplicating.
- **Bounded, immutable sealed segments.** Each segment's size is capped
  by `seal_interval`; once sealed, its layout only changes through
  compaction.
- **Tip-dominated reads.** Consumers read at or near the head of the
  log; the hot path is served from memory (memtable, WAL, in-process
  caches), not compacted SSTs.

In a general-purpose key-value store, compaction is the primary lever
for read-amp control. In OpenData-Log, ingest backpressure is the only
thing it *must* address, and read-amp can be deferred to seal time. That
permits a much simpler strategy than size-tiered, with predictable
bounds on total compaction work.

## Goals

- Define a state-aware compaction strategy with two explicit goals:
  aggressively relieve write backpressure; judiciously reduce read
  amplification only when there's substantial benefit.
- Define a time-based retention policy, layered on the compaction
  strategy via an "orphaned" segment state.
- Specify the writer/compactor protocol grounded in `SegmentMeta` as the
  source of truth: writer deletes on expiry, compactor reaps orphans.
- Wire all of this into `LogDbBuilder` so the compaction strategy runs
  unconditionally, and users opt into retention by setting a single
  `retention` option.

## Non-Goals

- **Size-based retention**: deferred. Time-based covers the common case;
  size-based requires plumbing for segment size accounting and conflicts
  with time-based ordering when both are set. A follow-up RFC.
- **Per-key retention**: deferred. The current model is uniform across the
  log. Per-key retention would require either an explicit policy registry
  or value-level TTLs; either way it's a larger surface area.
- **Reader leases / pinned checkpoints**: drains are best-effort with
  respect to long-running reads (see [Reader safety](#reader-safety)).
  Tighter coordination via SlateDB checkpoints is left for a future RFC.
- **A public API to trigger drains manually**: drains are scheduled
  internally based on the configured retention policy.

## Design

This design has two closely related parts:

1. **Retention enforcement.** `SegmentMeta` is the source of truth for
   which user segments are live. The writer expires segments by deleting
   `SegmentMeta`; the compactor later reaps any manifest segment that no
   longer has corresponding metadata.
2. **Compaction policy.** LogDb uses different compaction policies over a
   segment's lifecycle: active segments compact only for backpressure,
   sealed segments get one final consolidation pass for steady-state
   reads, and expired/orphaned segments are drained.

The retention protocol explains **when** a segment stops being live. The
compaction policy explains **what** storage work happens while a segment
is active, after it is sealed, and after it expires.

### SegmentMeta as the source of truth

The retention protocol is built around a single invariant: **a user
segment is "live" iff a `SegmentMeta` record exists for it in the system
segment.** This mirrors SlateDB's own relationship to its manifest one
layer up — the SlateDB compactor reaps any SST in object storage that
isn't referenced by the manifest; LogDb's compactor reaps any SlateDB
segment in the manifest that doesn't have a corresponding `SegmentMeta`.

The protocol is therefore:

1. **Writer expires a segment by deleting its `SegmentMeta` record.**
   This marks the segment as expired and eligible for reclamation.
   Readers continue to see it until they advance to a newer manifest /
   snapshot state that no longer includes the deleted `SegmentMeta`.
   Long-lived readers may therefore observe expired segments for some
   time after the delete.
2. **Compactor reaps the orphaned SlateDB segment.** It maintains a
   refreshed view of the live set and, on each `propose()` tick, drains
   any segment in the manifest but not in the live set.

The system segment is the channel — durable rather than in-memory.
Benefits:

- **No process-local hand-off.** A standalone compactor needs nothing
  more than storage read access; crash recovery is implicit (whatever
  `SegmentMeta` survives is the live set).
- **Lazy refresh is fine.** The `SegmentMeta` set is tiny; staleness only
  delays drain, never corrupts state.

### Overview

```text
                  ┌──────────────────────────────────────────┐
                  │            LogDb (writer side)           │
                  │                                          │
   appends ──▶    │  LogWriter                               │
                  │      │                                   │
                  │      ├─ writes entries + SegmentMeta     │
                  │      │  + ListingEntry to storage        │
                  │      │                                   │
                  │      └─ RetentionTask (periodic tick)    │
                  │           for each sealed segment with   │
                  │           end_time_ms < now() - retention:│
                  │             delete its SegmentMeta       │
                  └──────────────────────────────────────────┘
                                       │
                                       ▼  (system segment storage:
                                          SegmentMeta records are
                                          the live set)
                                       │
                  ┌──────────────────────────────────────────┐
                  │   SlateDB compactor (separate runtime)   │
                  │                                          │
                  │  LogCompactionScheduler                  │
                  │      │                                   │
                  │      ├─ tracks the current live segment  │
                  │      │   set (sourced from the writer    │
                  │      │   in-process; from storage in     │
                  │      │   a future standalone compactor)  │
                  │      │                                   │
                  │      └─ propose():                       │
                  │           active: L0→fresh SR on         │
                  │             backpressure                 │
                  │           sealed: one-shot final         │
                  │             compaction to single SR      │
                  │           orphaned: drain (segment in    │
                  │             manifest but not in live set)│
                  └──────────────────────────────────────────┘
```

Two components: a writer-side `RetentionTask` that decides when segments
have expired and asks the single writer task to delete their
`SegmentMeta` records, and a `LogCompactionScheduler` inside SlateDB's
compactor that proposes active/sealed compactions plus drains for any
segment in the manifest but not in the live set. How the scheduler
learns the current live set depends on whether the compactor is
in-process with the writer or runs standalone — see
[Live set propagation](#live-set-propagation).

### Configuration

The compaction strategy runs unconditionally. Retention is configured
via `RetentionConfig`; compactor-side knobs live in
`LogCompactionOptions`.

```rust
struct Config {
    storage: StorageConfig,
    segmentation: SegmentConfig,
    retention: RetentionConfig,
    compaction: LogCompactionOptions,
    // ...
}

struct RetentionConfig {
    /// Segment retention duration. Segments whose end time is older than
    /// `now() - retention` are eligible for deletion. `None` disables
    /// retention (segments live forever).
    ///
    /// Default: `None`.
    pub retention: Option<Duration>,

    /// How often the writer's RetentionTask re-evaluates segment expiry
    /// and deletes any SegmentMeta records that are past the retention
    /// bound. Smaller values reduce the lag between expiry and the
    /// segment becoming invisible to readers.
    ///
    /// Default: 60 seconds.
    pub check_interval: Duration,
}

struct LogCompactionOptions {
    /// L0 SST count at which the active-segment policy proposes an L0
    /// compaction. Sized as a fraction of SlateDB's `l0_max_ssts` so
    /// the scheduler triggers before backpressure hits.
    pub min_l0_per_compaction: usize,

    /// Maximum number of L0 SSTs to roll into one fresh SR per
    /// active-segment compaction.
    pub max_l0_per_compaction: usize,

    /// When set, skip the sealed-segment final consolidation and keep
    /// only L0 relief (see [Sealed segment](#sealed-segment)). Trades higher read
    /// amplification for lower write amplification. Default: `false`.
    pub l0_only: bool,
}
```

In the embedded case, the scheduler subscribes to the writer's
`WrittenView` watch channel and derives the live segment range from
the two segment-id watermarks already published there, so no polling
cadence is configured here. A future standalone-compactor RFC will
introduce a polling interval for the read side it owns.

#### Granularity and precision

Retention's unit of measure is the segment: a segment is expired or
not, with no sub-segment granularity. The segment's "end time" for
retention purposes is its successor's `start_time_ms` — the atomic
moment of seal. Before a successor exists, the segment has no end
time and is ineligible for retention; the active segment is always
in this state by definition.

Two consequences:

- **Precision is bounded by segment lifespan.** A record ingested
  early in a segment lives as long as the entire segment plus the
  configured retention. Effective data lifetime falls in
  `[retention, segment_lifespan + retention)`.
- **Low-traffic size-sealed segments can stay open indefinitely.**
  This RFC only specifies time-based segmentation, where
  `seal_interval` bounds lifespan.

#### Validation

`LogDbBuilder::build` rejects configurations that don't respect the
above granularity constraint:

1. **`retention` requires `seal_interval`.** If
   `retention.retention.is_some()` but
   `segmentation.seal_interval.is_none()`, no segment ever rolls,
   so retention can never fire. Error: *"retention requires
   segmentation.seal_interval to be set."*
2. **`retention >= seal_interval`.** A retention smaller than the
   segment interval produces misleading data lifetime: a record
   ingested at the start of a segment survives until
   `seal_interval + retention`, not `retention`. Setting
   `retention = 1m` with `seal_interval = 1h` does not give the
   user 1-minute retention. Error: *"retention must be at least as
   large as segmentation.seal_interval."*

With both set and consistent, the worst-case lifetime of any record
is `seal_interval + retention`.

### Compaction strategy

#### Two goals: backpressure and read amplification

OpenData-Log's compaction strategy serves two goals, with different
urgency:

1. **Backpressure relief (must).** SlateDB stalls writes once L0 reaches
   `l0_max_ssts`. Compaction must keep L0 under that cap; this is
   non-negotiable.
2. **Read amplification reduction (should).** Scans that touch many SSTs
   can waste I/O, but for log workloads hot reads are tip reads served
   from memory, so the marginal value of aggressive active-segment
   compaction is low.

The policy is therefore: be aggressive about (1), judicious about (2).

#### Three segment states

The compaction strategy is state-aware:

| State    | Definition                                       | Compaction goal                                           |
|----------|--------------------------------------------------|-----------------------------------------------------------|
| Active   | The segment with the largest id; receives writes | Keep L0 below backpressure; don't pay for read-amp        |
| Sealed   | Any segment with id < the active segment's id    | One-shot final compaction down to a single SR             |
| Expired  | A sealed segment older than the retention bound  | Drain (no merge; SSTs and SRs are physically removed)     |

The system segment (id 0) doesn't fit any of the three user states. It
holds `SeqBlock` and `SegmentMeta` records, receives writes on segment
rolls and retention deletes, and is never drained. Its own L0 still
needs occasional cleanup, though, or it will eventually trip
backpressure and block the writer from creating new `SegmentMeta`
records. The policy is intentionally simple: compact the system
segment when its L0 crosses a small threshold, consolidating it into a
single SR. The records are tiny, so this work is effectively free; the
exact threshold is an implementation/configuration detail.

**Sealing is implicit.** There is no `is_sealed` marker stored anywhere;
"sealed" is computed as `id < max(SegmentMeta.id)`. A segment is sealed
exactly when a `SegmentMeta` with a higher id exists. The transition is
atomic: when `SegmentCache::assign_segment` decides to roll, it appends
the new `SegmentMeta` record to the same SlateDB batch as the first
entries for the new segment, so the predecessor goes from active to
sealed in a single durable write. The successor's `start_time_ms` is
therefore the canonical seal moment of its predecessor.

Compaction work is bounded by ~2× ingest under sustained load: a byte
flushed to L0 is rewritten at most once during the active phase
(L0-relief into a fresh SR) and once at seal (final compaction). Light
loads that never reach `min_l0_per_compaction` do only the seal-time
rewrite. Either way, there's no per-tier write amplification.

#### Active segment

**Backpressure-only**: when L0 SST count reaches `min_l0_per_compaction`
(a configurable fraction of SlateDB's `l0_max_ssts`), merge some L0 SSTs
into a fresh SR. Otherwise do nothing — no SR-to-SR merging, no read-amp
work.

SR count grows linearly with active-segment age. We accept this because
tip reads are served from memory and sealed-segment compaction repairs
read-amp on roll. A follow-up can revisit this if deep-history
active-segment reads become expensive.

#### Sealed segment

When a new segment is created, the previous segment is sealed and never
receives further writes. The desired steady-state layout for a sealed
segment is a single SR, so scans avoid per-SR overhead and the segment
incurs no further compaction churn.

The scheduler proposes a one-shot final compaction per sealed segment
that isn't yet in single-SR steady state:

```rust
// Pseudocode for sealed-segment final compaction.
for (prefix, tree) in sealed_trees(manifest) {
    if tree.l0.is_empty() && tree.compacted.len() <= 1 {
        continue; // Already consolidated.
    }
    // Merge every L0 + every SR into the lowest-id SR among them.
    let sources = tree.l0_sources().chain(tree.sr_sources()).collect();
    let dst = tree
        .compacted
        .iter()
        .map(|sr| sr.id)
        .min()
        .unwrap_or_else(next_fresh_sr_id);
    propose(CompactionSpec::for_segment(prefix, sources, dst));
}
```

Because no new writes land in a sealed segment, this runs once in the
success case; if it fails or is preempted, the scheduler re-proposes it.

With `l0_only` set, this final consolidation is suppressed: sealed
segments keep whatever SRs they accumulated and are scanned as multiple
SRs, trading higher read amplification for one fewer rewrite per record.
L0 relief still runs on sealed segments (an L0 count pinned at
`l0_max_ssts` would otherwise permanently fail the commit gate for any
memtable touching the segment), so L0 stays bounded under either setting.

#### Orphaned (expired) segment

A segment is "orphaned" when it appears in the SlateDB manifest but has
no corresponding `SegmentMeta`. Under normal operation, this happens
because the writer's `RetentionTask` deleted the `SegmentMeta` on
expiry. The scheduler proposes
`CompactionSpec::drain_segment(prefix, all_sources)` for each.

`drain_segment` retires a segment without producing replacement SRs.
Drain works regardless of the segment's internal layout; it does not
require prior consolidation.

### LogCompactionScheduler

The scheduler classifies each segment in the manifest and proposes the
compaction (if any) called for by its state. It does not wrap
`SizeTieredCompactionScheduler`; LogDb wants a different policy.

The scheduler maintains a cached view of the live user-segment set
derived from `SegmentMeta`. The cache may be stale; staleness may delay
drain, but must not create false drains.

On each `propose()` tick, for every segment in the manifest:

- **System segment (id 0)**: consolidate when L0 ≥ 2 (per [Three
  segment states](#three-segment-states)).
- **Orphaned** (in manifest but not in the live set): emit
  `drain_segment`. Skipped entirely while the live set is `None`,
  otherwise an empty cache at startup would orphan every segment.
- **Active** (largest segment id that's both in the manifest and in
  the live set): emit an L0 compaction when L0 ≥ `min_l0_per_compaction`.
- **Sealed** (any other segment in the live set): emit a one-shot
  final compaction unless it's already in single-SR steady state
  (L0 = 0 and at most one SR).

One manifest/live-set relationship is an explicit invariant check:

- **Normal orphan:** a manifest segment with id less than the largest
  live id may be missing from the live set. This is the expected
  retention case: the writer deleted its `SegmentMeta`, and the
  compactor should drain it.
- **Invariant failure:** if the manifest contains any user segment with
  id greater than the largest live id, the newest writer-owned segment
  has lost its `SegmentMeta`. Under the writer protocol this should
  never happen: writers create `SegmentMeta` atomically with the first
  records in a segment, and retention only deletes sealed segments. The
  scheduler treats this as fatal (`panic!` in the embedded case, or a
  fatal process error in a standalone compactor), rather than
  reinterpreting an older segment as active.

Before the live set is initialized, the scheduler falls back to the
largest manifest segment id; the worst case is one missed L0 compaction
tick.

Standard hygiene applies throughout: skip any segment with an
already-active compaction or drain in `CompactorStateView` to avoid
duplicate proposals.

#### Live set propagation

The scheduler needs an up-to-date view of the live segment set to
classify manifest segments correctly. How that view reaches the
scheduler depends on the deployment shape:

- **Embedded with the writer (initial release).** The writer is the
  authoritative source for the live set — it owns the `SegmentMeta`
  records — and it runs in the same process as the compactor. It
  notifies the in-process scheduler whenever the live set changes (on
  segment roll and on retention deletes). No storage scan is needed.
- **Standalone compactor (future).** A standalone process can't observe
  the writer's in-memory state, so the scheduler maintains the live set
  by periodically scanning `SegmentMeta` records from storage. The scan
  is cheap and a coarse cadence is sufficient because staleness only
  delays drain.

Either way, until the scheduler has observed a live set at least once,
it leaves it uninitialized and orphan drains stay suppressed.

### RetentionTask

The writer-side `RetentionTask` is the only producer of retention
decisions. It is intentionally **decision-only**: it computes which
segments have expired, but the actual delete of `SegmentMeta` flows
through LogDb's single writer task so retention reuses the same
snapshot / `WrittenView` publication path as appends and seals.

Each tick: snapshot the `SegmentCache`; for every sealed segment
(`id < active_id`), use the next segment's `start_time_ms` as the end
time; if `end_time_ms < now() - retention`, enqueue a writer command to
delete that segment's `SegmentMeta`.

This loop is retried opportunistically: a failed delete attempt simply
leaves the segment live until a later tick succeeds.

A few properties worth being explicit about:

- The active segment has no end time and is never eligible (the
  `windows(2)` iteration excludes it as "current").
- Expiry is anchored to **end** time, not start time. A 1h segment with
  24h retention isn't drainable until ~24h after it was sealed.
- Deleting `SegmentMeta` marks the segment expired. Because the delete
  flows through the single writer task, the local LogDb instance
  publishes a new snapshot just like it does for appends and seals. Even
  so, other readers may continue to observe the segment until their own
  manifest / snapshot view catches up. Retention does not provide a hard
  visibility cutoff.
- `ListingEntry` records share the routing prefix of the segment they
  describe, so they live in the segment's SlateDB segment (not the
  system segment) and are drained automatically. The only system-segment
  record requiring explicit cleanup is `SegmentMeta`.

### Builder wiring

`LogDbBuilder::build` wires in the scheduler and, when retention is
configured, starts the writer-side `RetentionTask`.

The exact startup sequencing is an implementation detail, but two
requirements are part of the design:

- The scheduler must not act on an uninitialized live-set view.
- Orphan drains remain suppressed until the scheduler has observed the
  live set at least once (see [Live set propagation](#live-set-propagation)).

For a future standalone compactor, the scheduler's read side would be
backed by a `DbReader` opened by the standalone process and a polling
task that refreshes the live set from `SegmentMeta`; the retention
protocol itself does not change.

If `retention` is `None`, no `RetentionTask` runs, no `SegmentMeta`
records get deleted, and the scheduler proposes no drains. Active and
sealed policies still run.

### Reader safety

Drains are best-effort with respect to in-flight reads. Readers may
continue to see an expired segment until they advance to a newer
manifest / snapshot state that no longer includes the deleted
`SegmentMeta`. Even after drain is proposed and applied, physical
reclamation is still gated by SlateDB's snapshot rules.

The important distinction is:

- **Logical expiry** happens when the writer deletes `SegmentMeta`.
- **Reader disappearance** happens later, when a reader advances to a
  newer view that no longer includes that metadata.
- **Physical reclamation** happens later still, when the compactor drains
  the orphaned segment and no snapshot still pins its files.

A future RFC may add reader leases via SlateDB checkpoints for
long-running consumers. That is compatible with this design because
retention's primary purpose is to bound dataset growth, not to provide
an instantaneous visibility cutoff.

### Implications for clone and projection

This layout composes naturally with full-database clone: the system
segment is just part of the SlateDB keyspace, so a standard clone
preserves `SeqBlock`, `SegmentMeta`, and all user segments without any
LogDb-specific handling.

Partial projection is more nuanced. Any projected LogDb must include a
rewritten system segment, because `SegmentMeta` defines the projected
segment set, and a writable projection may also need a fresh `SeqBlock`.
That metadata is tiny, so the extra work is in planning the projection,
not in copying metadata.

Two projection shapes are plausible:

- **Sequence-oriented projection** keeps some contiguous portion of the
  log, such as a head or tail segment range.
- **Key-oriented projection** keeps some logical key range. Because user
  data is segmented, this would be planned by deriving a projected range
  within each selected segment prefix.

The current SlateDB clone/projection surface is narrower than that: it
accepts a simple range. But the underlying manifest/view model is rich
enough to support finer-grained LogDb projection in a future SlateDB RFC
and implementation. The main added API surface would be a projection
form that can derive per-segment ranges from a logical LogDb request.

### Metrics

The implementation exposes the following Prometheus metrics:

| Metric                                       | Type    | Description                                                     |
|----------------------------------------------|---------|-----------------------------------------------------------------|
| `logdb_segments_live_total`                  | gauge   | Live user segments (segments with a `SegmentMeta` record)       |
| `logdb_segments_orphaned_total`              | gauge   | Segments in the manifest awaiting drain (no `SegmentMeta`)      |
| `logdb_segment_oldest_age_seconds`           | gauge   | Age of the oldest live segment's start time                     |
| `logdb_retention_meta_deletes_total`         | counter | `SegmentMeta` records deleted by the `RetentionTask`            |
| `logdb_retention_meta_delete_errors_total`   | counter | Failed `SegmentMeta` deletes by the `RetentionTask`             |

## Alternatives

### Compactor enforces retention directly

An alternative design is to let the compactor read `SegmentMeta`,
compute expiry itself, and drain expired segments without any writer-side
delete step.

Rejected because it weakens the consistency story around metadata and
visibility. In the adopted design, the writer is the authority for when
a segment stops being live: it deletes `SegmentMeta`, updates its normal
snapshot / `WrittenView` path, and readers eventually stop observing the
segment from the same metadata source that drives retention. The
compactor then performs pure garbage collection on already-expired
segments.

If the compactor instead decides expiry itself, liveliness is split
across two places: `SegmentMeta` still says the segment is live, while
the compactor independently decides that it is expired. That weakens the
relationship between system metadata and the readable segment set, and
it pushes policy configuration and clock-based decisions into the
compactor rather than keeping them with the writer.

## Updates

| Date       | Description |
|------------|-------------|
| 2026-05-20 | Initial draft. SegmentMeta-as-source-of-truth protocol: writer deletes SegmentMeta on expiry, compactor garbage-collects orphaned SlateDB segments. |
| 2026-05-22 | Implementation: rename `l0_high_water` → `min_l0_per_compaction`; drop `live_set_refresh_interval` (deferred to standalone-compactor RFC); restate live-set propagation as a shape-dependent concern (in-process notification for embedded, storage polling for future standalone). |
| 2026-05-29 | Add `l0_only` option: suppress sealed-segment final consolidation, keeping only L0 relief, to trade read amplification for lower write amplification. |
