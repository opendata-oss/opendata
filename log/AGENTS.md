# Log Database

A Kafka-like append-only log system built on SlateDB. Each key is its own independent log stream.

## Architecture

See `rfcs/0001-storage.md` for the complete storage design.

### Key Concepts

- **Per-Key Logs**: Each user key is logically its own log stream
- **Global Sequence**: Single counter incremented after every append (monotonic but not contiguous per key)
- **TerminatedBytes**: Variable-length key encoding with 0x00 delimiter for correct ordering
- **Append-Only**: No updates or deletes, only appends

### Key Encoding

```
SlateDB Key: | version (u8) | type (u8) | key (TerminatedBytes) | sequence (u64) |
SlateDB Value: | record value (bytes) |
```

- Type `0x01`: Log entry
- Type `0x02`: SeqBlock (sequence allocation)

### TerminatedBytes Encoding

Escapes special bytes for unambiguous key boundaries:
- `0x00` -> `0x01 0x01`
- `0x01` -> `0x01 0x02`
- `0xFF` -> `0x01 0x03`
- Terminator: `0x00`

This preserves lexicographic ordering and enables prefix-based range queries.

### Key Modules

- `src/log.rs` - Core `LogDb` implementation (append, scan)
- `src/reader.rs` - `LogDbReader` for read-only access
- `src/model.rs` - Public types (Record, LogEntry)
- `src/serde.rs` - Key/value encoding
- `src/sequence.rs` - Block-based sequence allocation

### APIs

```rust
// Write
log.append(records: Vec<Record>) -> Result<()>
log.append_with_options(records, WriteOptions) -> Result<()>

// Read
log.scan(key, seq_range) -> LogIterator
log.count(key, seq_range) -> Result<u64>  // requires SST enhancements
```

### Append-Only Scan Optimization

Since newer entries are always in higher LSM levels, scans can iterate level-by-level instead of concurrent merging. Tip-of-log queries avoid loading blocks from older levels.

## Cross-References

- Uses `common::SequenceAllocator` for sequence number generation
- TerminatedBytes implementation in `common/src/serde/terminated_bytes.rs`
- Similar pattern to timeseries/vector but simpler (no indexes, no buckets)
