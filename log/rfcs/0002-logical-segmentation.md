# RFC 0002: Logical Segmentation

**Status**: Draft

**Authors**:
- Jason Gustafson <12502538+hachikuji@users.noreply.github.com>

## Summary

This RFC introduces logical segmentation as a partitioning mechanism for log data. Segments are logical boundaries in the sequence space that can be triggered by wall-clock time or manual intervention. They enable efficient seeking within the log and provide a foundation for future range-based query APIs.

## Motivation

RFC 0001 defined a log entry key encoding as:

```
| version | type | key (TerminatedBytes) | sequence |
```

While this supports efficient scans for a specific key's entire log or a sequence number range, it lacks the ability to efficiently seek to a particular point based on criteria other than sequence number. Consider these use cases:

1. **Tail queries**: "Read the last day of data" requires scanning from the beginning to find relevant entries, as there's no mapping from time to sequence number.

2. **Time-bounded queries**: "Read entries from 2pm to 4pm yesterday" similarly requires a full scan.

3. **Data lifecycle**: Retention policies based on time need to identify which sequence ranges correspond to expired data.

4. **Prefix queries**: RFC 0001's key encoding supports prefix-based scans (e.g., all keys under `/sensors/*`), but there's no way to seek within a prefix without knowing the full key. Sequence numbers are global, so scanning `/sensors/*` from sequence 1000 requires knowing which specific keys have entries at or after that sequence. With segments, a prefix scan can seek directly to a segment boundary, skipping earlier data across all matching keys.

The timeseries implementation solves similar problems using time-based buckets. For logs, time isn't always the right partitioning dimension. A streaming system processing 1 million events per second has different needs than one processing 100 events per day. It should also be possible to align segments on size.

## Goals

- Enable efficient seeking within a log based on pluggable criteria (time, size, etc.)
- Provide a foundation for time-based retention policies and prefix queries

## Non-Goals

- Defining specific retention policies (left for future RFCs)
- API design for range and prefix queries (left for future RFCs)
- Cross-key queries or joins
- Segment compaction or merging (users control segment granularity via triggers)

## Design

### Segment Concept

A **segment** is a logical boundary in the log's sequence space. Each segment represents a contiguous range of sequence numbers and carries metadata about its contents. Segments enable readers to efficiently skip portions of the log that don't match their query criteria.

Key properties of segments:

1. **Sequential**: Segments are numbered starting from 0 and increment monotonically
2. **Non-overlapping**: Each sequence number belongs to exactly one segment
3. **Immutable once sealed**: A completed segment's boundaries and metadata don't change
4. **Sparse in sequence space**: Segment boundaries don't need to align with sequence allocation blocks
5. **Atomic batch placement**: Each write batch lands entirely within a single segment; batches never span segment boundaries

### Key Encoding with Segments

The log entry key format is extended to include the segment ID:

```
Log Entry:
  | version (u8) | type (u8) | segment_id (u64 BE) | key (TerminatedBytes) | sequence (u64 BE) |
```

This encoding ensures:

- All entries within a segment are contiguous in storage
- Within a segment, entries are grouped by key
- Within a key, entries are ordered by sequence number
- Prefix scans can be constrained to specific segments

### Segment Metadata

Each segment has associated metadata stored in a separate record:

```
SegmentMeta Record:
  Key:   | version (u8) | type (u8=0x03) | segment_id (u64 BE) |
  Value: | start_seq (u64 BE) | start_time_ms (i64 BE) |
```

The metadata tracks:
- **start_seq**: The first sequence number in this segment
- **start_time_ms**: Wall-clock time when the segment was created

End boundaries (end sequence, end time) are derived from the next segment's start values, or from the current log state for the active segment.

#### Metadata Lifecycle

When a new segment is created, a `SegmentMeta` record is written with `start_seq` and `start_time_ms`. This ensures the segment is immediately discoverable.

### Segment Triggers

Segments are "bumped" (a new segment is started) automatically based on configured triggers.

#### Time-Based Trigger

The built-in trigger starts a new segment after a configurable wall-clock duration. Example: With a 1-hour interval, a new segment starts every hour. This provides predictable time-based partitioning similar to timeseries buckets.

Time-based triggering is simple to implement because it only requires comparing the current wall-clock time against the segment's `start_time_ms`. No internal state tracking is needed beyond what's already stored in the segment metadata.

### Configuration

Segment configuration is part of the main `Config` struct:

```rust
struct Config {
    storage: StorageConfig,
    segmentation: SegmentConfig,
}

struct SegmentConfig {
    /// Interval for automatic segment sealing based on wall-clock time.
    /// If `None`, automatic sealing is disabled and all entries are written
    /// to segment 0.
    ///
    /// Default: `None` (disabled)
    seal_interval: Option<Duration>,
}
```

With the default configuration (`seal_interval: None`), the log writes to segment 0 indefinitely. Users who want time-based partitioning can enable it by setting an interval. This keeps the default behavior simple while allowing opt-in to automatic segment management.

## Alternatives

### Time in the Key (No Segments)

An alternative is to embed timestamp directly in the key:

```
| version | type | timestamp (i64) | key (TerminatedBytes) | sequence |
```

Rejected because:
- Forces time-based ordering as the primary dimension
- Doesn't support size-based or manual partitioning
- Complicates key-based access patterns (all times for a key are scattered)

### Fixed-Size Segments

Using fixed sequence ranges per segment (e.g., 1M sequences each):

```
segment_id = sequence / 1_000_000
```

Rejected because:
- No flexibility for different workloads
- Time-based queries still require scanning segment metadata
- Doesn't naturally align with time boundaries

## Open Questions

None at this time.

## Potential Future Work

### Size-Based Triggers

Size-based triggers (e.g., seal after N entries or N bytes) require tracking entry counts or byte sizes, which adds complexity. Once the `count` API from RFC 0001 is implemented, size-based triggers could leverage it.

### Segment-Based Deletion

Segments provide a natural unit for data lifecycle management. APIs for deleting entire segments would enable efficient retention policies. Rather than scanning and deleting individual entries, retention can be enforced by dropping segments older than a threshold.

### Public Segment Sealing API

A future enhancement could expose manual segment sealing to applications:

```rust
impl Log {
    /// Seals the current segment and starts a new one.
    ///
    /// # Arguments
    /// * `user_meta` - Optional user-defined metadata to attach to the sealed segment
    ///
    /// # Returns
    /// The sealed segment ID
    async fn seal_segment(&self, user_meta: Option<Bytes>) -> Result<u64, Error>;

    /// Returns an iterator over all segments in the log.
    ///
    /// Segments are returned in order from oldest (segment 0) to newest.
    /// Each segment includes its metadata (start sequence, start time, user metadata).
    fn segments(&self) -> SegmentIterator;
}
```

This would enable use cases such as:
- Creating checkpoint boundaries aligned with application logic
- Attaching metadata accumulated during writes (checksums, record counts, correlation IDs)
- Aligning segments with external events (e.g., end of business day)

## Updates

| Date       | Description |
|------------|-------------|
| 2026-01-07 | Initial draft |
