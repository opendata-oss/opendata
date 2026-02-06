# RFC 0005: Write Coordination

**Status**: Draft

**Authors**:
- Jason Gustafson <12502538+hachikuji@users.noreply.github.com>

## Summary

This RFC proposes integrating the common write coordinator (RFC 0005 in common) with the Log
system. The coordinator replaces the current `RwLock<LogInner>` write path with an event-loop-based
model that batches writes, decouples flush I/O from the write path, and provides epoch-based
durability tracking. It also introduces a shared read component used by both `LogDb` and
`LogDbReader`.

## Motivation

The current write path in `LogDb::append_with_options` holds a `RwLock` for the entire
three-phase commit: build deltas, write to storage, and apply to in-memory caches. This serializes
writes against reads and couples write latency directly to storage I/O. Every append results in
a separate storage write, with no batching across concurrent callers.

The common write coordinator solves these problems generically. It has already been designed for
use by other OpenData systems (timeseries, vector) and provides batching, backpressure, and
non-blocking flushes out of the box.

## Goals

- Integrate the write coordinator with the Log write path
- Decouple flush I/O from the write event loop
- Batch multiple appends into fewer, larger storage writes
- Separate read-side and write-side state to eliminate lock contention
- Reuse a common read component between `LogDb` and `LogDbReader`

## Non-Goals

- Read-your-writes semantics (reads see flushed data only)
- Per-write durability control (callers use explicit `flush()` instead)
- Returning sequence numbers from `append()` (deferred to a future RFC)

## Design

### Delta

The caches are owned by the `LogContext`, which carries persistent state across deltas. The
`LogDelta` only holds what's accumulated during the current batch.

```rust
struct LogContext {
    sequence_allocator: SequenceAllocator,
    segment_cache: SegmentCache,
    listing_cache: ListingCache,
    clock: Arc<dyn Clock>,
}

struct LogDelta {
    context: LogContext,
    records: Vec<StorageRecord>,
    new_segments: Vec<LogSegment>,
}
```

The associated types map as follows:

| Type | Definition | Purpose |
|------|-----------|---------|
| `Write` | `Vec<Record>` | Batch of user records to append |
| `Context` | `LogContext` | Carries `SequenceAllocator`, `SegmentCache`, `ListingCache`, `Clock` across deltas |
| `Frozen` | `FrozenLogDelta` | Storage records to flush + new segment metadata for readers |

#### `init(context: LogContext)`

Takes ownership of the context from the coordinator. The delta holds the caches for the duration
of the batch, mutating them during `apply()`.

#### `apply(write: Vec<Record>)`

Runs the existing build phase against the caches owned via the context:

1. `context.sequence_allocator.build_delta(count)` — allocate sequence numbers
2. `context.segment_cache.build_delta(timestamp, start_seq)` — resolve or create segment
3. `context.listing_cache.build_delta(segment, keys)` — track new keys in segment
4. `LogEntryBuilder::build(segment, start_seq, records)` — serialize entries

All storage records are accumulated into `self.records`. New segments are tracked in
`self.new_segments` for reader cache updates. Each subsystem's delta is applied immediately
to the caches within `apply()`, since the coordinator guarantees sequential processing.

#### `freeze() -> (FrozenLogDelta, LogContext)`

Returns the accumulated storage records and new segment metadata as the frozen delta. The context
(with updated caches) is returned to the coordinator for the next delta.

```rust
struct FrozenLogDelta {
    records: Vec<StorageRecord>,
    new_segments: Vec<LogSegment>,
}
```

### Flusher

The `LogFlusher` writes the frozen delta's storage records to SlateDB with `await_durable: false`.

```rust
struct LogFlusher {
    storage: LogStorage,
}

#[async_trait]
impl Flusher<LogDelta> for LogFlusher {
    async fn flush(&self, event: &FlushEvent<LogDelta>) -> Result<Arc<dyn StorageRead>, String> {
        let options = StorageWriteOptions { await_durable: false };
        self.storage
            .put_with_options(event.delta.records.clone(), options)
            .await
            .map_err(|e| e.to_string())?;
        Ok(self.storage.snapshot())
    }
}
```

Using `await_durable: false` is consistent with the default behavior of `append()` today — data
is written to SlateDB's memtable and WAL, but the call does not block on object store persistence.
The `Durability::Flushed` watermark means data has been handed to SlateDB. In the future, once the
coordinator's `Durable` watermark is wired up to SlateDB's own flush notifications, callers that
need object store persistence can wait on `Durability::Durable`.

### Durability Model

`WriteOptions` and `append_with_options` are removed. All writes go through a single `append()`
method and are non-durable by default. Callers that need durability use an explicit `flush()` call.

| Current API | New API |
|---|---|
| `append(records)` | `append(records)` |
| `append_with_options(records, { await_durable: true })` | `append(records)` then `flush()` |
| `append_with_options(records, { await_durable: false })` | `append(records)` |
| `flush()` | `flush()` |

Internally, `append()` submits writes to the coordinator, which applies them to the in-memory
delta. The delta is flushed to SlateDB on timer/size thresholds or when `flush()` is called
explicitly. The flusher always writes with `await_durable: false` — data lands in SlateDB's
memtable and WAL, matching today's default `append()` behavior.

`Durability::Flushed` means data has been written to SlateDB (memtable + WAL).
`Durability::Durable` (object store persistence) is not yet implemented but can be added later
by wiring up SlateDB flush notifications to the coordinator's durable watermark.

This simplifies the public API — durability is an explicit action (`flush()`) rather than a
per-write flag. The `WriteOptions` struct is no longer needed.

### Read Path

#### Shared Read Component

The read implementations in `LogDb` and `LogDbReader` are nearly identical — both resolve segments
from a `SegmentCache` and scan entries from a `LogStorageRead`. The only difference is how the
segment cache is updated.

A shared inner component captures this:

```rust
struct LogReadInner {
    storage: LogStorageRead,
    segments: SegmentCache,
}
```

Both `LogDb` and `LogDbReader` hold an `Arc<RwLock<LogReadInner>>` and implement `LogRead`
identically against it. The `ListingCache` is not included — it is a write-path optimization
for deduplicating listing entry writes. Readers discover keys by scanning listing records
directly from storage.

#### Cache Updates

The two implementations differ only in how `LogReadInner.segments` is updated:

- **`LogDb`**: Subscribes to `FlushResult` from the coordinator. On each result, the frozen
  delta's `new_segments` are applied to the reader's `SegmentCache`. This is event-driven and
  stays in sync with flushed state.

- **`LogDbReader`**: Retains the existing timer-based poll that discovers new segments from
  storage. No coordinator access is needed.

#### Write/Read Cache Separation

The writer's `SegmentCache` (inside the `Delta`/`Context`) and the reader's `SegmentCache`
(inside `LogReadInner`) are independent. Both are initialized from the same storage state at
open time, but then diverge:

- **Writer cache**: Advances on every `apply()`. May contain segments not yet in storage.
- **Reader cache**: Advances on flush completion. Always reflects persisted state.

This eliminates the current lock contention where reads acquire the `RwLock<LogInner>` and
contend with the write path.

### Architecture

```
              ┌─────────┐ ┌─────────┐ ┌─────────┐
              │ append  │ │ append  │ │ append  │
              └─────────┘ └─────────┘ └─────────┘
                   │           │           │
                   └───────────┼───────────┘
                               ▼
                   ┌───────────────────────┐
                   │      Write Queue      │
                   └───────────────────────┘
                               │
                               ▼
  ┌Write Coordinator──────────────────────────────────────────┐
  │  ┌──────────────┐  ┌──────────────┐  ┌────────────────┐  │
  │  │  select! loop │  │   LogDelta   │  │   LogFlusher   │  │
  │  │               │  │              │  │                │  │
  │  │  • apply()    │  │  • SeqAlloc  │  │  put_with_opts │  │
  │  │  • freeze()   │  │  • SegCache  │  │  (await_durable│  │
  │  │  • flush cmd  │  │  • Listing   │  │   = false)     │  │
  │  │  • timer      │  │  • Records   │  │                │  │
  │  └──────────────┘  └──────────────┘  └───────┬────────┘  │
  └──────────────────────────────────────────────┼───────────┘
                                                 │
                          FlushResult            │
                    ┌────────────────────────────┘
                    ▼
  ┌─LogReadInner────────────────┐     ┌─LogReadInner────────────────┐
  │  LogDb (event-driven)       │     │  LogDbReader (timer-polled) │
  │  • SegmentCache             │     │  • SegmentCache             │
  │  • LogStorageRead           │     │  • LogStorageRead           │
  └─────────────────────────────┘     └─────────────────────────────┘
```

## Implementation Plan

The implementation is broken into incremental steps. Each step should compile and pass tests
before moving to the next.

### Step 1: Extract `LogReadInner`

Factor out the shared read component from `LogDb` and `LogDbReader`.

- Create `LogReadInner` holding `LogStorageRead` and `SegmentCache` behind `Arc<RwLock<_>>`
- Implement `LogRead` methods as free functions or methods on `LogReadInner` (scan, list_keys,
  list_segments all delegate to the segment cache + storage)
- Refactor `LogDb` to hold a `LogReadInner` for its `LogRead` impl, separate from `LogInner`
  (which still holds the write-side caches)
- Refactor `LogDbReader` to hold a `LogReadInner` for its `LogRead` impl
- The existing refresh task in `LogDbReader` updates `LogReadInner.segments`
- `LogDb` continues to update its own `LogReadInner.segments` from the write path's
  `apply_delta` for now (this will change in step 4)

**Validation**: All existing tests pass. No behavioral change.

### Step 2: Remove `WriteOptions`

Simplify the public API before wiring up the coordinator.

- Remove `WriteOptions` from `config.rs` and `lib.rs` exports
- Remove `append_with_options` from `LogDb`
- Change `append` to always use `await_durable: false` internally (this is already the default)
- Update `LogStorage::put_with_options` calls to hardcode `await_durable: false`
- Update the HTTP handler in `server/handlers.rs` if it references `WriteOptions`
- Update `AppendResult` — for now, keep returning `start_sequence` from `append()`. This will
  change when the coordinator is wired up.

**Validation**: All existing tests pass. Tests that used `append_with_options` switch to
`append` + `flush`.

### Step 3: Implement `LogDelta` and `LogFlusher`

Implement the coordinator traits for the log without changing the write path yet.

- Create a new module `log/src/delta.rs`
- Define `LogContext` holding `SequenceAllocator`, `SegmentCache`, `ListingCache`, `Arc<dyn Clock>`
- Define `LogDelta` implementing `Delta`:
  - `Write = Vec<Record>` (user records + timestamp bundled together)
  - `Context = LogContext`
  - `Frozen = FrozenLogDelta` (contains `Vec<StorageRecord>` and `Vec<LogSegment>`)
  - `init()` takes ownership of `LogContext`
  - `apply()` runs the build phase (sequence allocation, segment selection, listing, entry
    building) and immediately applies subsystem deltas to the caches. Accumulates storage
    records and new segments.
  - `estimate_size()` returns total size of accumulated `records`
  - `freeze()` returns `FrozenLogDelta` and hands back `LogContext` with updated caches
- Define `LogFlusher` implementing `Flusher<LogDelta>`:
  - Holds `LogStorage`
  - `flush()` calls `storage.put_with_options(records, { await_durable: false })`
    and returns `storage.snapshot()`
- The `Write` type needs to include the timestamp since `Delta::apply` doesn't have access to
  the clock otherwise. Define as:
  ```rust
  struct LogWrite {
      records: Vec<Record>,
      timestamp_ms: i64,
  }
  ```

**Validation**: Unit tests for `LogDelta` and `LogFlusher` in isolation. Test the full
apply/freeze/flush cycle using the coordinator's test utilities.

### Step 4: Wire up `LogDb` to the coordinator

Replace the `RwLock<LogInner>` write path with the coordinator.

- `LogDb` holds a `WriteCoordinatorHandle<LogDelta>` instead of `RwLock<LogInner>`
- `LogDb::open` initializes `LogContext` from storage (same as current `LogInner` init),
  creates the `WriteCoordinator` and `LogFlusher`, spawns `coordinator.run()`
- `LogDb::append` submits a `LogWrite` via `handle.write()`. For now, return type changes
  to drop `AppendResult` (or return a simplified version without `start_sequence`)
- `LogDb::flush` calls `handle.flush()` and waits on `Durability::Flushed`
- Spawn a background task that subscribes to `FlushResult` via `handle.subscribe()` and
  updates `LogReadInner.segments` with `new_segments` from each `FrozenLogDelta`
- Remove `LogInner` — the write-side caches now live in `LogContext` owned by the coordinator
- `LogDb::close` drops the coordinator handle (triggers graceful shutdown) and awaits the
  flush subscriber task

**Validation**: All read/write integration tests pass. Verify flush-based durability works
(append + flush + read). Verify segment cache updates propagate to the read path.

### Step 5: Clean up

- Remove dead code: `LogInner`, unused `build_delta`/`apply_delta` patterns if they were
  only called from the old write path
- Update `LogDb` doc comments to reflect the new write model
- Verify the `LogDbReader` timer-based refresh still works independently
- Run the full test suite including `reader_writer` integration tests

**Validation**: Full test suite passes. No dead code warnings.

### Open Questions

1. **Sequence number return path**: The current `append()` returns `AppendResult { start_sequence }`.
   The coordinator's `write()` returns a `WriteHandle` with only an epoch. Returning the sequence
   requires a side channel (e.g., a `oneshot` embedded in the `Write` type). This is deferred for
   now — callers that need the sequence can be adapted later.

2. **Error handling**: `Delta::apply()` returns `Result<(), String>`, losing the log's typed `Error`
   enum. Errors from the build phase (sequence allocation, segment creation) are stringified through
   `WriteError::ApplyError(String)`. This may warrant extending the coordinator's error type in the
   future.
