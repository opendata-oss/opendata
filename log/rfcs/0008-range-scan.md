# RFC 0008: Global and Range Scan

**Status**: Draft

**Authors**:
- [Jason Gustafson](https://github.com/hachikuji)

## Summary

LogDb optimizes for per-key reads: entries sort by
`(segment, key, relative_seq)`. That layout makes targeted reads cheap, but it
is expensive for CDC, export, backup, and indexing jobs that need to follow
many keys.

This RFC proposes a resumable range-based scan API. Rather than directing reads
through SlateDb's usual query path, we propose to follow SSTs that are visible
from the manifest and read log entries directly. As new L0s are created, we can
directly read over a broad range of keys efficiently. A range scan covering all 
keys reduces to reading every L0 with no query overhead.

To make this possible, we need a cursor which tracks a consistent position
within SlateDb's manifest history. The cursor pins a SlateDB checkpoint and
records the source being consumed inside that checkpoint, including the LogDb
segment, SlateDB WAL/L0/SR source, and last storage-key or WAL row position.
With this, we can safely resume after failures or restarts.

This proposal explains the mechanics of the manifest-based cursor and the
semantics of the range scan API that it enables. While most of the functionality
we need from SlateDb already exists, there are a few gaps. For example, we
need a convenient API to read records directly from raw SSTs. This
already exists for WALs, but not for SSTs. This document identifies these
gaps, but leaves their design for separate work within SlateDb.

## Motivation

LogDb is designed to represent many independent key streams. The key in SlateDb
is structured as `(segment, log_key, relative_seq)`, which is optimized
for efficient reads from an individual stream. However, that efficiency does
not scale to reads from many keys. To read across a range of keys with a
bounded sequence range, we must read each key stream individually. There is no
efficient alternative within the standard SlateDb APIs given our key structure.
If we specify the full key range `(segment, log_key_start)` up to
`(segment, log_key_end)`, then we would read the full log of every key.

There are many use cases which require reads over key ranges. Pipeline
use cases may use LogDb as a durable way to ship the data from all logs to other systems.
An auditing use case would likely require viewing the log of every key.
Without the ability to read across key ranges, LogDb cannot serve these
workloads efficiently. We want to encourage large numbers of keys, but the
more keys in the system, the more inflexible the system becomes.

A range-based scan API would address this gap. It generalizes to reads over
all keys, and it enables parallel reading similar to Kafka's consumer groups. A set
of readers can divide the keyspace so that each reader would see a subrange of keys. The
ranges could be split or merged arbitrarily so that the number of readers can
scale with the system load. 

## Goals

- Scan LogDb entries across an arbitrary key range.
- Resume from a durable cursor without missing records.
- Make scan cost proportional to data scanned, not key cardinality.

## Non-Goals

- Exactly-once delivery into downstream systems. The API provides a durable
  cursor; downstream commit semantics remain the consumer's responsibility.
- Strict global sequence ordering during historical backfill. The scan returns
  each record's global sequence, but does not order different keys inside the
  same LogDb segment.

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
global sequence scans are not a simple byte-range scan. In fact, there is no
convenient way to find the entry corresponding to a given global sequence number. One
would need to do a range scan from every key until the sequence is found. This
is what makes the global sequence unsuitable as a cursor.

LogDb is an append-only system: log enetry keys are never overwritten. When a segment has
been sealed, no further writes are possible on that segment. Within a segment,
we do not need to merge keys across SSTs. Each L0 or SR contains a unique subrange
of the keys present within them. This is an important fact which our design will
depend on: we can read the log stream of each key (or range of keys) by scanning
from each SR and L0 in the order defined by the manifest.

### SlateDB manifest order

A SlateDB manifest version describes the durable sources for each segment tree:

- the WAL id range needed for recovery,
- the L0 SST views in each tree, and
- the compacted sorted runs in each tree.

This order reflects the precedence when SlateDb merges keys during query
execution. WALs contain the most recent writes and always have the highest
precedence. Next are the L0s which span the full key range, and lastly,
the SRs.

To read the entries for a LogDb key stream, a scan would begin at the oldest SR,
and then work its way "upward" through the L0s. The most recent entries would be
from the WALs (if enabled), which are replayed into a memtable. This is the
same path that SlateDb's own query path, but significantly, it is not necessary
to merge across layers. We know that the sequence range of each key must
follow the order of the SSTs in the manifest. This is the intuition that
our design will depend on.

## Design

At a high level, our design is based on the manifest ordering insight from above.
Rather than reading through SlateDb's default merged query path, the append-only
structure of LogDb records allows us to read L0 and SR SSTs directly without
conflict. The position in the manifest represents the specific L0/SR/WAL that
we last read. This position can be made durable with a SlateDb checkpoint
to ensure that the manifest and the referenced data is not garbage collected.
We can read all of the L0s and SRs from the checkpoint manifest

We can efficiently read the range of blocks from an L0 or SR that matches
our key range since they are sorted by key. It is not possible to read a range
efficiently from a WAL file because stored in write order. If we are scanning all keys,
then there is no issue, but when reading a subrange, the WAL entries must be
filtered which means increased read amplification.
It might be better to skip the WAL and await L0s in this case. This is a
central latency tradeoff in this design.

Additionally, it is important to understand how the LSM structure affects
the ordering of the records that are returned. For data in the WAL, the
records will be ordered precisely by the LogDb global sequence number.
This ordering is not preserved for data in L0 and the SRs. To read
efficiently from L0s and SRs, we have to take the order of the data
within the SST, which means we cannot preserve the global sequence order
outside of the WALs. We are guaranteed to return data from each key
in its correct order, but sequence ordering across keys is not practical.

The corollary of this is that reads across keys do not have a deterministic
ordering. As compaction restructures new SRs, the entries for each log key
are located closer together, which means the overall order across keys
in the SST changes. This is another central tradeoff which stems directly
from the structure of the LogDb key. Below we discuss the proposed API and
then explain the cursor mechanics and the reader semantics.

### Public API

The scan API belongs on `LogRead` so both `LogDb` and `LogDbReader` can expose
it.

```rust
pub struct LogScanCursor(Bytes);

impl LogScanCursor {
  /// Construct a cursor from an initial sequence range
  pub fn new(seq_range: impl RangeBounds<Sequence> + Send) -> Result<LogScanCursor> { ... }

  /// Construct a cursor from the serialized bytes
  pub fn from_bytes(bytes: Bytes) -> Result<LogScanCursor> { ... }
}

pub struct LogScanIterator { ... }

impl LogScanIterator {
    pub async fn next(&mut self) -> Result<Option<LogEntry>>;

    /// Cursor for all entries returned so far. Consumers persist this only
    /// after they have durably processed the corresponding entries.
    pub fn cursor(&self) -> LogScanCursor;
}

#[async_trait]
pub trait LogRead {
    async fn scan_range(
        &self,
        key_range: impl RangeBounds<Bytes> + Send,
        cursor: LogScanCursor,
    ) -> Result<LogScanIterator>;
}
```

`LogScanCursor` is opaque bytes. Readers persist the exact bytes returned by
the iterator and pass them back on resume. LogDb owns the encoding, versioning,
and validation metadata; users should not inspect, construct, or modify cursor
bytes.

### Cursor model

Internally, the decoded cursor is a manifest coordinate:

```rust
struct DecodedLogScanCursor {
    version: u8,
    seq_range: SequenceRange,
    checkpoint: CheckpointRef,
    position: ManifestPosition,
}

struct CheckpointRef {
    checkpoint_id: Uuid,
    manifest_id: u64,
}
```

The manifest position depends on the source:

```rust
enum ManifestPosition {
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
```

For SST-backed sources, `last_key` is the raw SlateDB storage key last consumed
from that source. For WAL sources, the cursor stores `wal_id` and `row_offset`.

### Establishing the initial checkpoint

When a scan starts, the database creates a reader checkpoint,
records the checkpoint id and manifest id, and loads that manifest.
The initial cursor is constructed with `LogScanCursor::new` which requires
a sequence range to indicate the scope of the query. 

The sequence range maps to segments through `SegmentMeta` records visible in the
checkpoint. Each segment covers a half-open global sequence range
`[segment.start_seq, next_segment.start_seq)`. For the active segment, the
effective end is the smaller of the requested upper bound and the scanner's
current durable frontier. For an unbounded following scan, that frontier advances
as subsequent manifests are consumed.

```text
SegmentMeta visible at checkpoint C

              segment 1           segment 2           segment 3
global seq    [0, 40)             [40, 90)            [90, 140)
              sealed              sealed              active at C

scan range                         [55, 120)
segments touched                    segment 2          segment 3
initial source                      oldest source in segment 2
```

Operationally, the scanner lists the segment metadata in `start_seq` order and
keeps the segments whose global sequence span intersects the requested sequence
range. Within each segment, the global range is relativized against
`segment.start_seq` before it is passed to storage-key construction or
sequence-aware SST filters. For the example above, segment 2 receives
`[15, 50)` and segment 3 receives `[0, 30)`.

For a writer-local `LogDb`, `CheckpointScope::All` may be used to force
in-memory state into durable WAL/L0 before creating the checkpoint. For a
standalone `LogDbReader`, the scanner cannot flush writer memory, so the scan
starts from the durable state already visible in the manifest and WAL.

### Scanning a segment LSM

Each segment contains its own LSM tree. The levels of the LSM partition the
global sequence range into disjoint subranges. That is, each level (WAL/L0/SR)
contains a disjoint subrange which is strictly greater than each lower level.
For example, the sequence range of an L0 SST is strictly higher than any prior
L0 and all SRs.

```text
One LogDb segment in manifest C

newest
  WAL        [118, 126)
  L0-4       [104, 118)
  L0-3       [ 91, 104)
  SR-2       [ 64,  91)
  SR-1       [ 32,  64)
  SR-0       [  0,  32)
oldest

scan range: [70, 112)
read:       SR-2, L0-3, L0-4
skip:       SR-0, SR-1, WAL
```

A scan begins with the oldest sorted run and proceeds through each subsequent level
of the LSM. We only need to scan the levels that intersect the sequence range that we are
scanning, but we do not know which levels these are ahead of time. In RFC 7, we defined
a filter policy which leverages sequence range metadata which is stored in each SST.
As we scan the each level, we can skip any SSTs which do not intersect the scan range.

### Scan Locality and Performance

The efficiency of a scan depends on the level that is being read, and on the
granularity of the scan (both key range and sequence range). Consider a scan
over the sorted runs. The more data that gets compacted into an SR,
the worse the efficiency will be when scanning a specific sequence range. Our
key structure does not let us easily pick out the keys contained in the sequence
range, so we must scan the logs for all keys. We can use the segment size as a
way to bound the worst-case behavior. Smaller segments implies fewer entries
for each log stream within that segment.

```text
Storage order inside one sorted run

key prefix a* scan range
|
v
+----------------+-------------------------------+
| user key       | relative sequences in key run |
+----------------+-------------------------------+
| a/0001         |  0   8  13 [64  71]  96       |
| a/0002         |  4  17  48 [67  72] 101       |
| a/0003         |  2  19  35 [63]     88        |
+----------------+-------------------------------+
| b/0001         |  1  12 [65] 90                |
+----------------+-------------------------------+

wanted sequence range: [60, 80)
```

The key prefix is contiguous, so the scanner can avoid `b/0001`. The sequence
range is not contiguous in storage, however. It appears as a local slice inside
each matching key run, which means a prefix scan over `a*` reads all entries for
the `a*` keys in that sorted run and filters by sequence while decoding.

A scan over L0 has the same issue, but its scope is more limited.
The problem instead is that L0s cover the entire keyspace. This means that reading
a fine-grained key range will likely involve read amplification. We may fetch
some blocks containing keys outside of the range. Tuning the size of L0 files and
the block size is necessary to control read amplification.


### Advancing the manifest frontier

The cursor points to a single manifest. We read from each segment in the scan range
contained in the current manifest before advancing the cursor to the next. If all segments
in the scan range are sealed and contained within that manifest, then the scan ends
and the checkpoint is dropped. Alternatively, if we find a sequence number referenced
in the current manifest which is larger than the upper bound of our sequence range, then
we know that the manifest itself contains the full sequence range and there is no
need to advance to the next manifest.

If the scan continues into the active log segment and we have not found the upper
bound within the current manifest, then we need to advance to the next manifest.
If that manifest does not exist, then we must await it. An iterator following
an unbounded scan range will automatically receive new data as it is discovered
by the reader's own manifest polling loop.

The manifest pointed to by the cursor is known as the frontier. The central challenge
in this RFC is how to safely advance the frontier without missing data. To get
a picture of the problem, imagine that an L0 that we had not read was merged into a
new SR with other L0s that we have read. We would have no easy way to find our position
within the newly created sorted run.

```text
Frontier M10

  sorted runs
    S0

  L0s
    L0-7  [already read]
    L0-8  [not read]

Cursor position: after L0-7, before L0-8


Later manifest M13

  sorted runs
    S0
    S1 = compact(L0-7, L0-8)

  L0s
    L0-9
```

The missing data from `L0-8` is now mixed into `S1` with data from `L0-7`.
Skipping `S1` would miss `L0-8`; reading `S1` from the beginning would duplicate
rows from `L0-7`. A cursor that only names the old source cannot seek to "the
part of `S1` that came from `L0-8`."

If we cannot determine which data is new, then we can scan all levels of the
segment that the cursor currently points to. We can retain the maximum log sequence
number that we have observed from the prior manifest across all keys in the range.
New log entries in the same range are guaranteed to have a larger sequence.

This is only necessary when the chain of L0s cannot be followed. If our L0 position
is still valid in a new manifest version, then we can continue following using
the new L0s. This suggests an alternative approach. Rather than skipping ahead
to the latest manifest, we could follow manifest versions sequentially. If we do so,
then we are ensured to see each L0 while it is still named in a manifest.

A checkpoint in SlateDb is a lock on a single manifest version and all of the data
that it refers to. We could ensure that no manifest versions are missed by extending
the lock to cover a range of manifests. This makes garbage collection more complicated
and prevents cleanup in cases where a reader has crashed for an extended amount of
time.

One alternative is to extend SlateDb's own bookkeeping so that we know which
SlateDb-based sequence numbers are contained in each level. This provides a simpler
way to find the data that is new. If an L0 that we have read is merged into a new
sorted run with an L0 that we have not read, then the sequence numbers will tell
us exactly which sorted run we need to resume from. We will have to filter the
data fetched from that sorted run, but this allows us to skip over older sorted
runs which have already been read. It gives us a precise method to find the next
level that we need regardless of where it is located.

```text
Same M13 with SlateDB write-sequence coverage
(`db_seq` is SlateDB's write sequence, not LogDb's global sequence)

Cursor frontier
  consumed db_seq <= 119

M13 sources
  S0                         db_seq [  0,  79]   skip
  S1 = compact(L0-7, L0-8)   db_seq [ 80, 139]   read + filter
       from L0-7             db_seq [ 80, 119]   duplicate
       from L0-8             db_seq [120, 139]   new
  L0-9                       db_seq [140, 159]   read
```

With this metadata, `S1` is known to overlap the unread SlateDB sequence range,
so the scanner reads it and emits only rows with `db_seq > 119`. Older sorted
runs such as `S0` can be skipped because their sequence coverage is entirely
below the cursor frontier.

#### WAL-enabled delta

If the WAL is enabled, the delta from manifest `M` to manifest `N` is the WAL id
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

### Filtering

Reading directly from SSTs means the potential to see records which are not
log entries. Every source reader applies the same filters:

1. Key must decode as `RecordType::LogEntry`.
2. LogDb segment id must be a user segment, not the system segment.
3. Log key must be wiithin the requested key range.
3. Log sequence must be within the requested sequence range.

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
missing, the scanner returns `ExpiredCursor`.

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
