# TimeSeries Database

A Prometheus-compatible time series database built on SlateDB.

## Architecture

See `rfcs/0001-tsdb-storage.md` for the complete storage design and `rfcs/0002-write-api.md` for the public API.

### Key Concepts

- **Time Buckets**: Data partitioned by time (hourly). Bucket encoded as u32 minutes since epoch.
- **Series ID**: u32 identifier scoped to a time bucket (not global)
- **Labels**: Key-value pairs identifying a series, including `__name__` for metric name

### Record Types (in `src/serde/`)

| Type | File | Description |
|------|------|-------------|
| `BucketList` | `bucket_list.rs` | Available time buckets |
| `SeriesDictionary` | `dictionary.rs` | Label fingerprint -> series_id mapping |
| `ForwardIndex` | `forward_index.rs` | series_id -> full label set |
| `InvertedIndex` | `inverted_index.rs` | (label, value) -> RoaringBitmap of series_ids |
| `TimeSeries` | `timeseries.rs` | Gorilla-compressed (timestamp, value) pairs |

### Key Modules

- `src/tsdb.rs` - Core database implementation
- `src/timeseries.rs` - Public `TimeSeries` API (write, flush)
- `src/serde/` - Record serialization (mirrors common patterns)
- `src/promql/` - PromQL query engine and HTTP server
- `src/index.rs` - Index operations
- `src/head.rs` - In-memory head block for recent data

### Query Path

1. Parse PromQL query
2. Use inverted index to find series matching label selectors
3. Use forward index to get full label sets for grouping
4. Load Gorilla-compressed data from TimeSeries records
5. Apply aggregations and return results

## Cross-References

- Serde patterns match `common/src/serde/` (key_prefix, terminated_bytes, encoding)
- Uses `common::Storage` trait for SlateDB access
- Similar RFC structure to `vector/rfcs/` - reference for consistency
