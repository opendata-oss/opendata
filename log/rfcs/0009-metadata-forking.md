# RFC 0009: Fast Forking

**Status**: Draft

**Authors**:
- [Andrei Racila](https://github.com/TheEnthralled)

## Summary

This RFC outlines a design for implementing fast forking for OpenData Log.

## Motivation

Fast forking of durable logs has many interesting potential applications.

- **Branching agent sessions and trajectories**. As AI agents become more powerful and prevalent, it becomes increasingly important to track the behavior and
  actions of these agents. AI agent sessions tend to spawn ephemeral sub-agents to complete large, complex tasks over
  increasingly longer timescales (O(hours)). In addition, popular RL schemes for LLM post-training,
  such as Group Relative Policy Optimization (GRPO), perform multiple parallel rollouts during each stage.
  It would be useful to easily record these rollouts to determine why certain ones were invalid or resulted in poor
  reward updates.

- **Ephemeral environments based off of production environments**. Popular hosted database solutions such as 
Neon/Planetscale branches, Dolt, and Snowflake zero-copy allow the creation of cheap copy-on-write copies of databases.
This feature would enable users to more easily develop similar self-hosted workflows for cheap testing and experimentation on arbitrary components.

- **Deterministic replay and what-if debugging**. It would be useful for CI/CD systems or debugging complex bugs that test the application of multiple dependent
  queries in certain orders and against certain stateful components such as databases.

## Goals

- Forking from an already present key stream should be fast and in line with the time it takes to create a new regular
  log stream using a new key (within 3% of p99 append latency for the warm path).

- Appending to forked logs should be just as fast as non-forked logs.

- Clients should be able to search entries across both fork and ancestor log streams. However, only the ancestor entries
  written before the fork was created and are retained should be viewable.

- Fork operations should not be noisy; operations on a fork stream should not interfere with
  operations on different streams, even ancestor streams.

- Extend the current record encoding scheme with new entry types without modifying existing metadata or user entry
  encodings to improve backwards compatibility.

## Out of Scope

- **Per-client visibility**: clients can specify which users can view and/or append to a specific log. Future RFC.
- **Merging of log streams back into parent sequences**: not planned.
- **Detaching and re-attaching of forks to arbitrary log streams**: not planned.
- **Full or partial snapshots**: future RFC.
- **Per-log retention policies**: future RFC.
- **Descendant listing** (enumerating the forks created from a key): deferred; see Alternatives.

## Design

### Terminology

- BE: big endian byte order.
- LE: little endian byte order.
- Segment: an LSM tree maintained by SlateDB. 
- SST: [SlateDB sorted string table](https://github.com/slatedb/slatedb/blob/main/schemas/sst.fbs). 
- Stream: The append-only sequence associated with a user key.
- Root: A stream that was not created by a call to `/fork`.
- Parent: The stream directly forked to create a child.
- Child/fork/branch: A stream created by a call to `/fork`.
- Ancestor/descendant: Transitive parent/child relationships.
- Fork sequence: The exclusive inheritance boundary.
- Logical history: The applicable ancestor intervals followed by the stream’s own entries.
- Fork family: A root and all its descendants.

### Forking

A fork is a log stream whose prefix of user entries (i.e., entries whose sequence numbers are less than the sequence
number at which `/fork` was called) reference ancestor streams.

Forks can use arbitrary keys just like regular logs as long as the key name is not in use by any other live stream.

Example: forking of log A at fork sequence 146.

```text
 __________       __________       __________                  __________
| A : val1 | --> | A : val2 | --> | A : val3 | ------------> | A : val4 | ...
 ----------       ----------       ----------    | /fork       ----------
  seq: 143         seq: 144         seq: 145     | seq: 146     seq: 148
                                                 |
                                                 |     __________
                                                 '--> | B : val1 | ...
                                                       ----------
                                                        seq: 147
```

As can be seen in the diagram above, a successful `/fork` call consumes a global sequence ID. This allows for a 
total ordering of fork creation times, i.e. we can have two forks of A named B and C that without this property 
could have the same sequence value associated with them even though they were created at different physical times.

A fork is a point-in-time divergence. It does not continuously inherit new writes subsequently appended to its parent.

Appends can still continue on the parent log stream. Fork creation will fail if the key for the new forked log is already
in use by any other log.

Forked logs can be created from logs that in turn are also forks.

```text
 __________       __________       __________                          __________
| A : val1 | --> | A : val2 | --> | A : val3 | --------------------> | A : val5 | ...
 ----------       ----------       ----------    | /fork              ----------
  seq: 143         seq: 144         seq: 145     | seq: 146            seq: 149
                                                 |
                                                 |     __________
                                                 '--> | B : val4 | --> ...
                                                       ----------
                                                        seq: 147
                                                            |
                                                            | /fork
                                                            | seq: 148
                                                            |     __________
                                                            '--> | C : val6 | --> ...
                                                                  ----------
                                                                   seq: 150
```

Keys used for forks have to be globally unique amongst all live segments. 

### API Surface Additions

#### Fork

```http
POST /api/v1/log/fork
Content-Type: application/protobuf+json
```

```json
{
  "parentKey": {"value": "cGFyZW50"},
  "childKey": {"value": "Y2hpbGQ="},  
  "forkSequence": 146,
  "awaitDurable": true
}
```

Semantics:
- `parentKey` will be checked to ensure that a stream keyed by it exists in a live segment.
- `childKey` is not associated with a `ListingEntry` in a live segment. 
- Fork and append commands are serialized through the existing single writer.
- `forkSequence` is an optional field indicating at which point the forked log should continue from the parent.
   If not set, it is the next global sequence at that point.
- Every parent entry assigned below `forkSequence` is inherited.
- Every parent entry assigned at or above `forkSequence` is excluded.
- `awaitDurable` mirrors `/append` and also applies to the whole atomic metadata batch.
- An append batch is entirely before or after a fork; the fork cannot split a batch.
- Metadata and every secondary-index record are written atomically.
- Child appends cannot become visible before the fork metadata is committed to the WAL.
- Successful `/fork` calls will also write a `ListingEntry` for the child key.

Retry behavior:

- If the same child already names the same fork, return the existing result, making retries idempotent.

`200` response:

```json
{
  "status": "success",
  "fork": {
    "sequence": 146,
    "timeMs": 1786550400000
  }
}
```

`400` response:

```json
{
  "status": "error",
  "message": "<string>"
}
```

This response will be returned under the following conditions
- `parentKey` == `childKey`
- No retained streams are keyed with `parentKey` at the point in time the request is made.
- `childKey` is already in use by another root or fork stream in a live segment.

#### Ancestors

Lists the logs from which the specified key inherited entries.

```http
GET /api/v1/log/ancestors?key=C&limit=100
```

`200` response:

```json
{
  "status": "success",
  "key": {"value": "Qw=="},
  "root": {"value": "QQ=="},
  "edges": [
    {
      "parent": {"value": "Qg=="},
      "child": {"value": "Qw=="},
      "forkSequence": 148,
      "forkTimeMs": 1786550500000
    },
    {
      "parent": {"value": "QQ=="},
      "child": {"value": "Qg=="},
      "forkSequence": 146,
      "forkTimeMs": 1786550400000
    }
  ]
}
```

Semantics:
- The query `key` must be in use.
- Root query, query for a key which is not a fork, or `limit=0` returns an empty edge list.

Edges are returned from the queried branch toward the root.

`400` response:

```json
{
  "status": "error",
  "message": "<string>"
}
```

This response will be returned under the following conditions
- The query key is not associated with any retained stream.
- The value of the `limit` query parameter has a non-integer type. 
- The value of the `limit` query parameter is a negative integer. 

### Existing API Operation Changes

A new `/scan` boolean query parameter, `include_ancestors`, will be added.

When a `/scan` is performed on a forked log with the `include_ancestors` parameter included, the returned
`values` entry list will also contain entries from ancestor logs.

A key `K`'s logical history is defined recursively:

`history(K) = K's own entries ∪ {entry ∈ history(parent) : entry.seq < K's fork sequence}`. If
`K` is a root key, `history(K) = K's own entries`. A scan will return all entries in
`history(K)` for which `start_seq <= entry.seq < end_seq` (`end_seq` is exclusive, matching the
existing `/scan` semantics). Returned `values` list entries will still be sorted by increasing
`sequence` value.

For example, based on the second diagram above, if we made the following `/scan` query:

```http
GET /api/v1/log/scan
    ?key=C
    &start_seq=142
    &end_seq=151
    &include_ancestors=true
```

it would return:

```json
{
  "status": "success",
  "key": {"value": "Qw=="},
  "values": [
    {
      "sequence": 143,
      "value": "dmFsMQ==",
      "sourceKey": {"value": "QQ=="}
    },
    {
      "sequence": 144,
      "value": "dmFsMg==",
      "sourceKey": {"value": "QQ=="}
    },
    {
      "sequence": 145,
      "value": "dmFsMw==",
      "sourceKey": {"value": "QQ=="}
    },
    {
      "sequence": 147,
      "value": "dmFsNA==",
      "sourceKey": {"value": "Qg=="}
    },
    {
      "sequence": 150,
      "value": "dmFsNg==",
      "sourceKey": {"value": "Qw=="}
    }
  ],
  "nextSequence": 151
}
```

The `sourceKey` field that is part of every entry in the `values` list is optional and only included when
a call to `/scan` is made with `include_ancestors=true`.

If we instead ran

```http
GET /api/v1/log/scan
    ?key=C
    &start_seq=142
    &end_seq=151
```

the following payload will be returned:

```json
{
  "status": "success",
  "key": {"value": "Qw=="},
  "values": [
    {
      "sequence": 150,
      "value": "dmFsNg=="
    }
  ],
  "nextSequence": 151
}
```

### Protobuf Schema

New message types:

```
message ForkEntry {
    uint64 sequence = 1;
    int64 timeMs = 2;
}

message ForkRequest {
   Key parentKey = 1;
   Key childKey = 2;
   bool awaitDurable = 3;
   optional uint64 forkSequence = 4;
}

message ForkResponse {
  string status = 1;
  ForkEntry fork = 2;
}

message Edge {
    Key parent = 1;
    Key child = 2;
    uint64 forkSequence = 3;
    int64 forkTimeMs = 4;
}

message AncestorsRequest {
    Key key = 1;
    optional uint32 limit = 2;
}

message AncestorsResponse {
   string status = 1;
   Key key = 2;
   Key root = 3;
   repeated Edge edges = 4;
}

```

To support the new version of `/scan` we will need to add another optional field to the 
`Value` Protobuf message type

```
message Value {
  uint64 sequence = 1;
  bytes value = 2;
  optional Key sourceKey = 3;
}
```

### New record types

#### ForkEntry

`ForkEntry` mirrors the structure of `ListingEntry` and likewise is also tied to the lifetime of each SlateDB segment
so that unused keys are not retained indefinitely. A new `ForkEntry` will be created and committed 
upon every successful call to `/fork`.

Its proposed structure is:

```text
Key:   | subsystem (u8) = 0x03 | version (u8) = 0x01 | segment_id (u32 BE) | record_type (u8) = 0x50 | key (TerminatedBytes) |

Value: | parent_key (TerminatedBytes) | fork_sequence (u64 BE) | fork_time_ms (i64 BE) |
```

These records are immutable and unique across all segments.  

### Key encoding changes

No changes to existing record types are currently planned in order to be backwards compatible with older versions 
of OpenData Log and remove the needs for migrating data. 

### Rust API 

#### Writers 

```rust 
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForkOutput {
    /// The global sequence consumed by the fork point.
    pub fork_sequence: Sequence,
}

impl LogDb {
    pub async fn try_fork(&self, key: Bytes, source_key: Bytes, fork_sequence: Option<Sequence>) -> AppendResult<ForkOutput> {
        ...
    }

    pub async fn fork_timeout(&self, key: Bytes, source_key: Bytes, fork_sequence: Option<Sequence>, timeout: Duration) -> AppendResult<ForkOutput> {
        ...
    }
}

```

#### Readers

`/ancestors` is a read only operation. The following addition to the `LogRead` trait is proposed 

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForkEdge {
    pub key: Bytes,
    pub source_key: Bytes,
    pub fork_sequence: Sequence,
    pub fork_time_ms: i64,
}

trait LogRead {
    // ... existing methods ...
    async fn list_ancestors(&self, key: Bytes) -> Result<Vec<ForkEdge>>;
}
```

The `ScanOptions` struct (`/log/src/config.rs`) has already been reserved for future use by 
`LogRead::scan` and `LogRead::scan_with_options`. Currently it is empty but we can add the following
field to it 

```rust
     
pub struct ScanOptions {
    /// When true, the scan also yields entries inherited from ancestor keys
    /// (each ancestor's entries below its fork point). Defaults to false.
    pub include_ancestors: bool
}
```

`count/inspect/list_keys` operations remain unchanged. 

### Metadata Lookup Optimization

To speed up fork related metadata lookups without needing to constantly consult SlateDB, 
an in-memory data structure will be actively maintained. Updates to the in-memory index will 
occur concurrently with durable metadata writes through SlateDB. 

Since there is no explicit deletion operation for `/fork` streams, we only have to worry about ensuring 
metadata updates for new forked nodes and new segments are fast.

#### Readers

The following in-memory data structure is proposed:

```rust
/// Cached fork metadata, keyed by the fork's key.
/// The segment ID records which segment the fork's `ForkEntry` was
/// written in, so the entry can be evicted when that segment expires.
fork_index: HashMap<Bytes, (SegmentId, ForkEdge)>
```

Because forks only hold parent pointers, no tree structure is required:

- `/ancestors` walks parent pointers: look up the queried key, then its parent, and so on until a key
  with no entry (a root) is reached.
- A successful `/fork` inserts one entry.
- When a segment expires, entries whose `ForkEntry` was written in that segment are removed.

Read-only nodes (`LogDbReader`) lazily build the map. If the requested key is not in the map, a scan of `ForkEntry` 
records across live segments will be performed. The map will be maintained as part of the existing refresh cycle. 
This is required because `ForkEntry` keys are segment-scoped, so a key's fork metadata may not be found with a single point lookup.

#### Writers

Writers will make updates to the reader's fork index but won't have to maintain a separate in-memory index for fork information, nor will
they have to consult the index before committing records. 

Calls to `/fork` require key checks to ensure 

1. The parent key is still in active use, i.e. a `ListingEntry(parentKey)` exists in a live segment. 
2. The child key is NOT in use. 

Because every stream creation writes a `ListingEntry` for the new key, and SlateDB's SST bloom filters
answer absent-key point lookups cheaply, these checks do not always require touching data in object storage itself. 

`append` operations don't have any need to consult fork metadata so writes will be fast regardless of what kind 
of stream they occur on. 

### Crash Recovery 

The fork index will behave like `ListingCache` currently behaves. 

`fork` commits `ForkEntry` records, `ListingEntry` records, and the consumed sequence in one atomic SlateDB write batch through the existing WAL.
Crash recovery is just SlateDB's WAL replay, unchanged from the crash-recovery mechanism currently used by OpenData Log.

Object storage remains the source of truth. No in-memory state requires recovery as indexes are caches over committed records.

## Alternatives

* Instead of creating new record types, `ListingEntry` can be repurposed to contain tree information in its value field. 
However, this would require re-defining the key structure as well as migrating older `ListingEntry` records 
of OpenData Log instances to the new format. 

* A descendants index (child-pointer edge records plus a `/descendants` listing endpoint) was considered
  but scrapped due to the implementation complexity it would introduce (separate records and indexes to maintain 
  fork key history in BFS order).

## Updates
| Date       | Description |
|------------|-------------|
| 2026-09-01 | Initial draft |
