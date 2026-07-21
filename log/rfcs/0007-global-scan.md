# RFC 0007: Global and Range Scan

**Status**: Draft

**Authors**:
- [Jason Gustafson](https://github.com/hachikuji)

## Summary

LogDb optimizes for per-key reads: entries sort by
`(segment, key, relative_seq)`. That layout makes targeted reads cheap, but it
is expensive for CDC, export, backup, and indexing jobs that need many keys.

This RFC proposes a resumable global or key-range scan API. The cursor is a
**manifest coordinate**: it pins a SlateDB checkpoint and records the source
being consumed inside that checkpoint, including the LogDb segment, SlateDB
WAL/L0/SR source, and last storage-key or WAL row position. After a checkpoint
is consumed, following mode advances through manifest history and reads only the
new WAL files, or new L0 SSTs when WAL is disabled.

The design mostly fits SlateDB's current building blocks, but production-quality
checkpoint handoff and slow-consumer retention need cleaner SlateDB APIs. Those
gaps are listed as prerequisites.

## Motivation

LogDb encourages many independent keyed streams. Today a broad consumer must
list keys and scan them one at a time, so cost scales with key cardinality.
Global and range scans shift the work to the storage sources that contain log
entries, enabling:

- CDC consumers that replicate all LogDb records into another system.
- Full export and backup workflows.
- Reindexing jobs for secondary indexes or search systems.
- Analytics and audit consumers that need complete record coverage.
- Prefix or key-range scans without key enumeration.
- Keyspace sharding, where multiple consumers own disjoint key ranges and each
  advances an independent cursor.

## Goals

- Read LogDb entries across all keys, or across a key scope, within a sequence
  range.
- Resume from a durable cursor without missing records.
- Make scan cost proportional to data scanned, not key cardinality.
- Allow independent consumers to split a scan by key prefix or key range.
- Support a following mode that continues to surface newly durable records.
- Keep garbage collection safe by tying retained SlateDB data to scanner-owned
  checkpoints.
- Identify SlateDB API gaps needed for a clean implementation.

## Non-Goals

- Exactly-once delivery into downstream systems. The API provides a durable
  cursor; downstream commit semantics remain the consumer's responsibility.
- Strict global sequence ordering during historical backfill. The scan returns
  each record's global sequence, but does not order different keys inside the
  same LogDb segment.
- Reading non-durable writer memory from an out-of-process reader. Following is
  defined over durable WAL/L0 state visible through SlateDB manifests.
- Adding a sequence-ordered secondary index.
- Defining retention policy. This RFC relies on SlateDB checkpoints to keep
  scan sources alive while they are needed.

## Background

### LogDb storage order

Log entries use the segmented key layout introduced by RFC 0002:

```text
| subsystem | version | segment_id (u32 BE) | record_type=0x10 |
| terminated_user_key | relative_seq (var_u64) |
```

The segment id is a LogDb logical segment id. The SlateDB segment extractor
routes all records with the same six-byte prefix
`[subsystem, version, segment_id]` into the same SlateDB named segment, so each
LogDb segment has its own LSM tree in the SlateDB manifest.

Within a LogDb segment, entries sort by `user_key` and then `relative_seq`. They
do not sort by global sequence. This is why per-key scans are efficient and why
global sequence scans are not a simple byte-range scan.

### SlateDB manifest order

A SlateDB manifest version describes the durable sources for each tree:

- the L0 SST views in each tree,
- the compacted sorted runs in each tree,
- the WAL id range needed for recovery,
- the last L0 sequence persisted into the manifest, and
- active checkpoints.

A checkpoint pins one manifest version and the files it needs. That lets a
scanner decide when a manifest has been consumed before allowing GC to release
its sources.

### SlateDB WAL order

SlateDB 0.13 stores WALs as specialized SST files:

- entries are appended in insertion order, which is expected to be SlateDB
  sequence order;
- the data blocks preserve that insertion order;
- the index stores the first sequence number for each block, not the first key;
- the SST info stores the first sequence number and has no normal last-key
  bound.

`WalReader` therefore gives an ordered change stream by WAL id and row order,
but not a key-ordered table that can be pruned by user-key range.

### Active-order problem

A "last storage key read" cursor is safe only inside immutable data. In an
active key-sorted view, a new write can sort behind the cursor. The scanner
therefore consumes immutable sources named by manifest versions and discovers
new data by following manifest progress.

## Design

### Public API

The scan API belongs on `LogRead` so both `LogDb` and `LogDbReader` can expose
it.

```rust
pub enum LogScanScope {
    AllKeys,
    KeyPrefix(Bytes),
    KeyRange(BytesRange),
}

pub enum LogScanMode {
    Backfill,
    Follow,
}

pub struct LogScanOptions {
    pub scope: LogScanScope,
    pub mode: LogScanMode,
    pub cursor: Option<LogScanCursor>,
}

pub struct LogScanCursor(Bytes);

pub struct LogScanIterator { ... }

impl LogScanIterator {
    pub async fn next(&mut self) -> Result<Option<LogEntry>>;

    /// Cursor for all entries returned so far. Consumers persist this only
    /// after they have durably processed the corresponding entries.
    pub fn cursor(&self) -> LogScanCursor;
}

#[async_trait]
pub trait LogRead {
    async fn scan_log(
        &self,
        seq_range: impl RangeBounds<Sequence> + Send,
        options: LogScanOptions,
    ) -> Result<LogScanIterator>;
}
```

Prefix and key-range scopes are user-key scopes, not raw SlateDB storage-key
bounds. The implementation maps them into per-segment storage ranges.

`LogScanCursor` is opaque bytes. Consumers persist the exact bytes returned by
the iterator and pass them back on resume. LogDb owns the encoding, versioning,
and validation metadata; users should not inspect, construct, or modify cursor
bytes.

### Cursor model

Internally, the decoded cursor is a manifest coordinate:

```rust
struct DecodedLogScanCursor {
    version: u8,
    scope: LogScanScope,
    seq_range: SequenceRange,
    high_watermark: Option<Sequence>,
    checkpoints: CheckpointCursor,
    phase: CursorPhase,
}

struct CheckpointRef {
    checkpoint_id: Uuid,
    manifest_id: u64,
}

struct CheckpointCursor {
    current: CheckpointRef,
    candidate: Option<CheckpointRef>,
}

enum CursorPhase {
    Backfill(BackfillPosition),
    Tail(TailPosition),
    CaughtUp,
}
```

`current` is the consumed checkpoint. `candidate` pins the next manifest during
handoff while the scanner consumes the delta from `current.manifest_id` to
`candidate.manifest_id`.

The source position depends on the kind of source:

```rust
struct BackfillPosition {
    source: SourcePosition,
}

enum SourcePosition {
    Wal {
        wal_id: u64,
        row_offset: u64,
    },
    L0 {
        log_segment_id: SegmentId,
        sst_view_id: Ulid,
        sst_id: Ulid,
        last_key: Option<Bytes>,
    },
    SortedRun {
        log_segment_id: SegmentId,
        run_id: u32,
        sst_view_id: Ulid,
        sst_id: Ulid,
        last_key: Option<Bytes>,
    },
}

struct TailPosition {
    from_manifest_id: u64,
    to_manifest_id: u64,
    source: SourcePosition,
}
```

For SST-backed sources, `last_key` is the raw SlateDB storage key last consumed
from that source. For WAL sources, the cursor stores `wal_id` and `row_offset`.
`high_watermark` is the largest acknowledged LogDb sequence and is used only for
duplicate suppression during recovery paths.

### Establishing the initial checkpoint

When a scan starts without a cursor, it creates a scanner-owned SlateDB
checkpoint, records the checkpoint id and manifest id, loads that manifest, and
starts in `Backfill`.

For a writer-local `LogDb`, `CheckpointScope::All` may be used to force
in-memory state into durable WAL/L0 before creating the checkpoint. For a
standalone `LogDbReader`, the scanner cannot flush writer memory, so the scan
starts from the durable state already visible in the manifest and WAL.

### Backfill

Backfill reads LogDb entries visible through the checkpointed manifest and
matching the scope and sequence range.

The SST-backed portion is emitted per LogDb segment in ascending segment id:

1. Build the visible source set for that segment from L0 SST views and
   compacted sorted runs.
2. Read rows from those sources, merging or buffering as needed to preserve the
   ordering contract.
3. Apply scope and sequence filtering while decoding `LogEntryKey`.
4. Advance the source coordinate after records are yielded and acknowledged.

If WAL is enabled, the checkpointed view may include WAL files not yet flushed
into L0:

```text
(manifest.replay_after_wal_id, manifest.next_wal_sst_id)
```

The implementation may need to merge or buffer WAL and SST rows to preserve the
public ordering contract.

When all sources in the checkpointed manifest are consumed, the cursor enters
`CaughtUp` for that manifest. In `Backfill` mode, the iterator ends and the
scanner may delete its owned checkpoint. In `Follow` mode, the scanner starts
the checkpoint handoff loop.

### Following manifest progress

Following mode advances from one consumed manifest to a later manifest that
introduces new durable data. The scanner distinguishes:

- **Ingestion frontier changes** introduce new logical records: new WAL files
  when WAL is enabled, or new L0 SST views when WAL is disabled.
- **Reorganization changes** only move existing records: L0 compaction, sorted
  run compaction, and segment drains.

The tailer consumes only ingestion frontier changes.

#### WAL-enabled delta

If WAL is enabled, the delta from manifest `M` to manifest `N` is the WAL id
range:

```text
M.next_wal_sst_id .. N.next_wal_sst_id
```

The scanner reads each WAL file once through `WalReader`, in ascending WAL id
and row order. This is O(delta) and preserves write order for the live tail.

For prefix or range scopes, WAL-backed following is correct but not range-local:
each range consumer reads the same WAL files and filters different rows.

Compaction may later flush those WALs into L0, but that does not create new
logical records. If a needed WAL file is gone, the scanner falls back to a
value-filtered scan of the target manifest or fails with `CursorTooOld`.

#### WAL-disabled delta

If WAL is disabled, new durable data first appears as L0 SST views. The scanner
follows manifests in order and consumes newly added L0 views.

For adjacent manifests, an L0 delta is:

```text
new_l0_views = N.segment(prefix).l0 - M.segment(prefix).l0
```

using `SsTableView.id` as the stable identity.

This is the best case for key-range sharding: each consumer maps its scope into
per-segment storage ranges and opens only overlapping L0 views and blocks.

If the scanner misses the manifest that introduced an L0 and the L0 has been
compacted and GC'd, it falls back to `sequence > high_watermark` on the target
checkpoint or reports `CursorTooOld`.

### Key-range partitioning

The same cursor model works for all keys and for disjoint key ranges:

```text
consumer 0: LogScanScope::KeyRange(a..f)
consumer 1: LogScanScope::KeyRange(f..m)
consumer 2: LogScanScope::KeyRange(m..)
```

Each consumer owns a checkpoint cursor and advances independently. The cursor
includes `scope`, so a cursor for one range cannot resume another range.

SST-backed sources are efficient because entries are key-sorted inside each
LogDb segment. The implementation derives a storage range for
`record_type=LogEntry` plus the user scope, then opens overlapping L0/SR views
and blocks.

WAL-backed sources are filtering operations because WAL SSTs are
sequence/insertion ordered and indexed by sequence, not key. Options:

- accept duplicate WAL reads across range consumers in v1;
- run one WAL follower that demultiplexes rows to range-owned workers;
- disable WAL for range-sharded following so the tail source is key-sorted L0;
- add a new key-range-aware WAL/CDC index or per-range tail structure.

The last option is outside this RFC unless WAL-enabled range sharding becomes a
hard requirement.

### Checkpoint handoff

Checkpoint handoff must pin both the consumed manifest and the target manifest
until consumer progress is durable.

For a scanner at consumed manifest `M`:

1. Poll or list manifest versions after `M`.
2. Choose the next manifest `N` that contains an ingestion frontier change.
3. Create a candidate checkpoint for `N`.
4. Persist a cursor containing both `M` and `N`.
5. Consume the delta from `M` to `N`.
6. After the consumer durably persists the cursor at the end of the delta,
   promote `N` to `current` and delete the old checkpoint for `M`.

This avoids deleting the old checkpoint too early and avoids reading unpinned
WAL/L0 sources.

An atomic SlateDB "advance checkpoint and return diff" operation could collapse
this to one checkpoint. Until then, handoff uses a candidate checkpoint.

### Manifest selection

The scanner should not blindly jump to the latest manifest. With WAL enabled,
that is safe only when all WAL ids in
`M.next_wal_sst_id .. latest.next_wal_sst_id` are retained. With WAL disabled,
the scanner should process manifest history in order so it can see the first
manifest that introduced each L0 view.

The scanner therefore uses manifest history as part of the cursor protocol:

```text
current manifest M
  -> next retained ingest manifest N
  -> consume frontier delta
  -> promote N
```

If required history is missing, the scanner returns `CursorTooOld` unless
fallback rescan is enabled.

### Filtering

Every source reader applies the same filters:

1. Key must decode as `RecordType::LogEntry`.
2. LogDb segment id must be a user segment, not the system segment.
3. User key must match `LogScanScope`.
4. Log sequence must be within the requested sequence range.
5. Log sequence must be greater than `high_watermark` when duplicate
   suppression is required.

Metadata, listing, sequence block, tombstone, and merge rows are skipped. A
non-value log entry row is corruption or an unsupported future format.

### Ordering

The v1 API guarantees complete coverage, stable resume, per-entry sequence
metadata, segment order, and per-key order. It does not define the relative
order of different keys inside the same segment.

The ordering contract is:

| Scope | Guarantee |
|-------|-----------|
| Across segments | Entries from an earlier LogDb segment are emitted before entries from a later LogDb segment. |
| Same key | Entries for the same user key are emitted in ascending LogDb sequence order. |
| Different keys in same segment | No ordering guarantee. The implementation may use storage-key order, WAL order, batching order, or an internal merge order. |

This applies to global, prefix, and key-range scans. WAL-backed scans may read
in WAL id and row-offset order, but that source order is not part of the API.

A future `require_sequence_order` option could define inter-key order, likely
using WAL-backed following or a bounded k-way merge/sort for backfill.

### Failure and resume

The scanner is at-least-once at the API boundary. Consumers persist a cursor
only after durably processing the corresponding records; otherwise records after
the last persisted cursor may be re-emitted.

On resume:

1. Validate that all checkpoint ids referenced by the cursor still exist.
2. Load the referenced manifest versions.
3. Reconstruct the source list and find the stored source coordinate.
4. Resume from `last_key` for SST sources or `row_offset` for WAL sources.
5. Apply `high_watermark` duplicate suppression when needed.

If any checkpoint, manifest, WAL file, or SST view required by the cursor is
missing, the scanner returns `CursorTooOld` unless fallback rescan is enabled.

### Fallback rescan

When the exact delta source is unavailable, the scanner can rescan the target
checkpoint and filter by LogDb sequence:

```text
emit entries where sequence > high_watermark
```

This is correct because LogDb sequences are monotonic, but it is
O(target checkpoint size) instead of O(delta). It is a recovery path, not the
steady-state following mechanism.

## SlateDB Prerequisites

SlateDB already exposes manifests, WAL readers, SST readers, and checkpoint
administration. LogDb still needs cleaner hooks for the following cases.

### 1. Checkpoint at a known manifest version

The scanner needs to pin the manifest version it selected from history. Today,
checkpoint creation pins the current/latest view, which can produce version skew
between the manifest LogDb diffed and the manifest SlateDB pinned.

### 2. Reader-owned checkpoint cursor

`DbReader` has internal checkpoint replacement, but scan checkpoint advancement
must be gated on consumer progress.

The useful primitive is:

```rust
advance_checkpoint_if(
    checkpoint_id,
    expected_manifest_id,
    target_manifest_id,
) -> CheckpointAdvanceResult
```

returning the old/new manifests or the ingestion frontier diff.

### 3. Atomic advance-and-diff

The ideal primitive:

1. validates that the scanner's checkpoint is still at `M`,
2. advances it to the selected target manifest `N`,
3. returns the manifest diff or ingestion frontier, and
4. preserves enough state for safe resume if the client crashes.

This removes the LogDb two-checkpoint handoff.

### 4. Manifest diff helper

LogDb can diff manifests itself, but it should not depend on SlateDB L0 list
ordering or compaction details. A diff helper should report:

- new WAL id ranges,
- new L0 SST views by segment prefix,
- compaction-only changes to ignore, and
- whether required sources are still retained.

### 5. Public row iterator for SST views and sorted runs

`WalReader` exposes a row iterator, but the SST path stops at
`SstReader::index()` and `read_block()`. A public ranged row iterator over
`SsTableView` and `SortedRun`, respecting `visible_range`, would simplify
backfill and WAL-disabled following.

### 6. CDC WAL retention

WAL-backed following is O(delta) only while needed WAL files are retained. A
checkpoint pins the WAL range for its manifest, not future WAL files that the
scanner has not yet latched.

SlateDB should expose a CDC retention hook such as:

```rust
retain_wal_from(consumer_id, next_wal_id)
```

or checkpoint advancement should pin the WAL frontier before GC can remove it.
Without this, slow consumers must poll frequently, fall back to rescans, or fail
with `CursorTooOld`.

### 7. Seekable WAL reader or row offset support

`WalFileIterator` currently starts at the beginning of a WAL file. The cursor can
store a row offset and skip on resume, but a seekable iterator would make
resuming large WAL files cheaper.

### 8. Optional range-aware WAL tail

This is optional for correctness, but needed to avoid duplicated WAL reads in
WAL-enabled range sharding. Since WAL SSTs are sequence-indexed, a range-aware
tail needs a shared CDC demultiplexer, per-range WAL routing, or a secondary
range index. Per-block min/max keys are only a heuristic because WAL blocks are
not key-clustered.

## Implementation Plan

1. Add cursor types and serde.
2. Add helpers to build LogEntry storage ranges for an entire LogDb segment and
   optional key scope.
3. Add an SST row iterator wrapper over `SstReader::index` and
   `SstReader::read_block`, including `visible_range` clipping.
4. Add a WAL source reader that decodes LogDb entries and tracks `row_offset`.
5. Implement checkpointed backfill over a manifest coordinate.
6. Implement WAL-backed following from manifest deltas.
7. Implement WAL-disabled L0 following, guarded by retained manifest history.
8. Add key-range pruning for SST-backed sources and key-range filtering for
   WAL-backed sources.
9. Add fallback rescan using `high_watermark`.
10. Add integration tests for resume, checkpoint handoff, retention,
    compaction, WAL tailing, WAL-disabled L0 tailing, range-partitioned scans,
    and cursor-too-old failures.

## Alternatives

### Secondary sequence index

Write a second record for each log entry keyed by `(segment, relative_seq)`.
This makes sequence scans easy, but doubles write amplification and adds another
retained structure. It remains a fallback if checkpoint/WAL following is too
complex or strict global sequence order becomes mandatory.

### List keys and scan each key

This works today but scales with key cardinality.

### Repeated snapshot scan with sequence watermark

Repeatedly scan the active dataset and emit `sequence > high_watermark`. This
is correct but O(active dataset) per poll, so it is only a recovery fallback.

### Writer-owned in-memory feed

A co-located writer could publish append batches to local subscribers. This is
low latency, but it does not serve standalone readers or provide a durable
object-store cursor.

### Shared WAL demultiplexer

One process could read each WAL file once and route rows to range workers. This
avoids duplicate WAL I/O, but adds a service-level component and complicates
durable progress.

## Open Questions

- Should v1 require WAL for following and leave WAL-disabled L0 following as a
  follow-up?
- For range-sharded following, is duplicated WAL scanning acceptable when WAL is
  enabled, or do we require a shared demultiplexer / range-aware tail source?
- Should the public API expose one entry at a time, batches with a batch cursor,
  or both?
- Should strict global sequence ordering be an option for bounded backfills?
- Should `CursorTooOld` be the default when a delta source is missing, or should
  the scanner automatically perform the expensive fallback rescan?
- Which SlateDB prerequisites should be upstreamed before implementing the
  public LogDb API?

## Updates

| Date       | Description |
|------------|-------------|
| 2026-06-04 | Initial draft |
| 2026-06-05 | Convert sketch into a checkpoint-driven manifest cursor proposal with explicit SlateDB prerequisites. |
