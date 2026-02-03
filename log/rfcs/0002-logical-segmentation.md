# RFC 0002: Logical Segmentation

**Status**: Draft

**Authors**:
- Jason Gustafson <12502538+hachikuji@users.noreply.github.com>

## Summary

This RFC introduces logical segmentation as a partitioning mechanism for log data. Segments are logical boundaries in the sequence space that can be triggered by wall-clock time or manual intervention. They enable efficient seeking within the log and provide a foundation for future range-based query APIs.

## Motivation

RFC 0001 defined a log entry key encoding that supports efficient scans for a specific key's entire history or a sequence number range. However, for operations which naturally span multiple keys, it would be simpler and more efficient to operate on a higher-level unit which spans the entire keyspace of the log.

This proposal introduces the notion of a *log segment* to address this gap. A log segment corresponds to the entries spanning a range of sequence numbers across the full keyspace. This is the same notion as the time bucket that we use in the OpenData-Timeseries design, but generalized to allow for segments marked by other factors than time. The segment provides a natural anchor point for cross-key operations and metadata maintenance. In principle, this makes it possible to manage metadata tied to that operation at the level of the segment, rather than level of keys. For example:

**Range queries** naturally span multiple keys. For example, a prefix scan over `/sensors/*` needs to seek to a particular point in time across all matching keys. Without segments, this requires either scanning from the beginning or maintaining a separate index mapping time to sequence numbers for every key.

**Retention** is simpler to reason about at the segment level. Rather than tracking expiration per key, we can drop entire segments when they age out. This also avoids coherence issues when maintaining metadata about keys. For example, a key listing API (see future work) would tie listing records to segments—when a key is no longer used, it naturally falls out of scope when its segments are removed.

## Goals

- Enable efficient seeking within a log based on pluggable criteria (time, size, etc.)
- Provide a foundation for pluggable retention policies

## Non-Goals

- Defining specific retention policies
- API design for range and prefix queries
- Cross-key queries or joins

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
  | version (u8) | type (u8) | segment_id (u32 BE) | key (TerminatedBytes) | relative_seq (varint u64) |
```

The `segment_id` is a 32-bit identifier, allowing up to ~4 billion segments. Future versions can expand this to 64 bits if needed.

The `relative_seq` is the entry's sequence number relative to the segment's `start_seq` (i.e., it resets to 0 at the start of each segment). Using variable-length encoding keeps keys compact since most relative offsets within a segment are small. See [`common::serde::varint`](../../common/src/serde/varint.rs) for the encoding specification.

This encoding ensures:

- All entries within a segment are contiguous in storage
- Within a segment, entries are grouped by key
- Within a key, entries are ordered by sequence number
- Prefix scans can be constrained to specific segments

### Segment Metadata

Each segment has associated metadata stored in a separate record:

```
SegmentMeta Record:
  Key:   | version (u8) | type (u8=0x03) | segment_id (u32 BE) |
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

### Key Listing API

Currently the log provides no simple way to inspect which keys are present—users would have to scan the full log. A key listing API would introduce listing records tied to each segment. When a key first appears in a segment, a listing record is written. To enumerate all keys, users iterate the listing records across segments. When segments are deleted, their listing records are removed as well, so keys that are no longer present naturally fall out of scope.

### Public Segment Sealing API

A future enhancement could expose manual segment sealing to applications:

```rust
impl LogDb {
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
- Creating segment boundaries aligned with application logic
- Attaching metadata accumulated during writes (checksums, record counts, correlation IDs)
- Aligning segments with external events (e.g., end of business day)

## Updates

| Date       | Description |
|------------|-------------|
| 2026-01-07 | Initial draft |
| 2026-01-12 | Added link to varint implementation |
