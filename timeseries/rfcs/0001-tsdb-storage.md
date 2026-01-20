# RFC 0001: TSDB Storage

**Status**: Draft

**Authors**:
- [Rohan Desai](https://github.com/rodesai)
- [Almog Gavra](https://github.com/agavra)
- [Jason Gustafson](https://github.com/hachikuji)

## Summary

This RFC defines the storage model for OpenData-timeseries, a time series
database built on [SlateDB](https://github.com/slatedb/slatedb). Data is
organized into time buckets with inverted indexes for efficient label-based
querying, a forward index for retreiving series information from a given series
id, and Gorilla-compressed storage for time series samples.

## Motivation

OpenData-timeseries stores time series data with Prometheus-compatible
semantics. The storage design must support:

1. **Efficient label-based queries** — Finding series by label selectors (e.g.,
  `{job="api", status="500"}`) requires inverted indexes mapping label/value pairs
  to series IDs.

2. **Time-bounded scans** — Queries typically target recent data. Organizing
  data into time buckets enables efficient range scans without reading historical
  data.

3. **High compression** — Time series data exhibits strong temporal locality.
  Gorilla compression achieves excellent compression ratios for timestamp/value
  pairs.

4. **Cardinality management** — Series IDs are scoped to time buckets, limiting
  cardinality growth and enabling efficient cleanup of old data.

## Goals

- Define the record key/value encoding scheme
- Define time bucket encoding and management
- Define common value encodings for strings and arrays
- Define metadata and indexing record types

## Non-Goals (left for future RFCs)

- Compaction/Retention policies
- Ingesting data in batches / merging
- Query execution and optimization

## Design

### Background on the query path

This RFC focuses on storage design rather than query execution, but understanding
the query path helps explain the storage model. A timeseries database has three
storage components:

- **Raw series storage**: Stores `(timestamp, value)` pairs keyed by series ID
- **Inverted index**: Maps label/value pairs to series IDs, enabling efficient
  label selector matching (e.g., `{job="api", status="500"}`)
- **Forward index**: Maps series IDs to their canonical label specifications
  (all labels as UTF-8 strings), used to resolve series discovered via the
  inverted index

To illustrate, here's how the query `sum by (instance) (metric{path="/query",
method="GET", status="200"})` is served:

1. Parse the query into a query plan
2. Use the inverted index to find series matching `{__name__="metric", path="/query", method="GET", status="200"}`
3. Use the forward index to resolve each series ID to its full label set (including `instance` for grouping)
4. Sum the values for matching series and return the result

### Record Layout

Record keys in SlateDB are built by concatenating big-endian binary tokens with
no delimiters. Each token is only unique within its prefix and lexicographical
ordering matches the numeric order of encoded values.

Keys are referenced using two forms:

1. `XXX_id` — a _scoped identifier_ unique within the scope of `XXX`
2. `XXX_path` — a _fully qualified path_ which is globally unique

Examples of this convention:

- `series_id`: a token identifying a time series which is unique within its scope
- `series_path`: the fully qualified path that includes the scope (e.g., `<time_bucket><series_id>`)

### Standard Key Prefix

All records use a standard, 2-byte prefix: a single `u8` for the record version
and another `u8` for the record tag. The record tag is encoded as two 4-bit
fields. The high 4 bits are the record type. The lower 4 bits depend on the
scope of the record. Globally scoped records set the lower 4 bits to 0x00 for
future use and bucket scoped records set the lower 4 bits to encode the
`TimeBucketSize` allowing different time granularities to coexist (in the initial
implementation, we will only support 1 hour time buckets).

```
record_tag byte layout (global-scoped):
┌────────────┬────────────┐
│  bits 7-4  │  bits 3-0  │
│ record type│  reserved  │
│   (1-15)   │     (0)    │
└────────────┴────────────┘

record_tag byte layout (bucket-scoped):
┌────────────┬────────────┐
│  bits 7-4  │  bits 3-0  │
│ record type│ bucket size│
│   (1-15)   │   (1-15)   │
└────────────┴────────────┘
```

### Time Bucket Encoding

Data in the system is divided into time buckets, which represent all of the data
received within a specific window of time. The time bucket is encoded into the
record key as a `u32` representing number of minutes since the UNIX epoch. The
byte ordering must be big-endian to be consistent with lexicographic ordering
from SlateDB.

OpenData-timeseries will eventually support windows of time at different granularities.
Recent data will likely be fine-grained buckets (every hour in our prototype).
As these buckets age in the system, they will be rolled up into more coarsely
defined buckets. New data may be stored by hour, for example, and then later
rolled up into days or weeks.

### Common Value Encodings

To keep the record schemas compact and consistent, we use shared encodings for
strings that are referenced throughout the value definitions:

- `Utf8`: `len: u16` (little-endian) followed by `len` bytes containing a UTF-8
  payload. When `len = 0`, the payload is empty but still considered present.
- `OptionalNonEmptyUtf8`: Same layout as `Utf8`, but a zero-length payload
  indicates that the field was not provided. Ingestion never emits empty strings
  for these fields, so `len > 0` implies a non-empty UTF-8 payload.
- `Array<T>`: `count: u16` (little-endian) followed by `count` serialized
  elements of type `T`, encoded back-to-back with no additional padding.
- `FixedElementArray<T>`: Serialized elements of type `T`, encoded back-to-back
  with no additional padding. The array is not preceded by a count since the
  number of elements can be computed by dividing the buffer length by the fixed
  element size. This array must only be used when the schema only contains a
  single array and nothing else.

**Note on Endianness**: Value schemas use little-endian encoding for multi-byte
integers. This differs from key schemas, which use big-endian to maintain
lexicographic ordering for range scans. Since values don't participate in
ordering comparisons, little-endian provides better performance on common
architectures (x86, ARM).

### Record Type Reference

|   ID    |        Name         |                        Description                        |
|---------|---------------------|-----------------------------------------------------------|
| `0x00`  | *(reserved)*        | Reserved for future use                                   |
| `0x01`  | `BucketList`        | Lists the available time buckets                          |
| `0x02`  | `SeriesDictionary`  | Stores the mapping of series to series IDs                |
| `0x03`  | `ForwardIndex`      | Stores the canonical label set for each series            |
| `0x04`  | `InvertedIndex`     | Maps label/value pairs to posting lists of series IDs     |
| `0x05`  | `TimeSeries`        | Holds the raw time-series payloads                        |

## Record Definitions & Schemas

### `BucketList` (`RecordType::BucketList` = `0x01`)

The buckets list is used to discover within each namespace the set of time
buckets that are available (i.e. have data). In addition to enumerating each
bucket, the listing also indicates its granularity. When data is rolled up into
coarser buckets as part of the compaction process, the old finer-grained buckets
will be replaced by the new coarse buckets in the listing.

**Key Layout:**

```
┌─────────┬─────────────┐
│ version | record_tag  │
│ 1 byte  │   8 bits    │
└─────────┴─────────────┘
```

- `version` (u8): Key format version (currently `0x01`)
- `record_tag` (u8): Record tag encoding the record type and bucket size
  - `bits 7-4` (u4): Record type (`0x01` for `BucketList`)
  - `bits 3-0` (u4): Bucket size (`0x00` reserved for future use)

**Value Schema:**

```
┌─────────────────────────────────────────────────────────┐
│                    BucketsListValue                     │
├─────────────────────────────────────────────────────────┤
│  FixedElementArray<(bucket_size: u8, time_bucket: u32)> │
└─────────────────────────────────────────────────────────┘
```

- `bucket_size` (u8): The size of the time bucket in hours
- `time_bucket` (u32): The number of minutes since the UNIX epoch

### `SeriesDictionary` (`RecordType::SeriesDictionary` = `0x02`)

The series dictionary maps label sets (label/value pairs) to series IDs.
This enables ingestion to resolve a series ID for a given label set, and allows
query execution to map series IDs back to their label sets when needed.

**Key Layout:**
```
┌─────────┬─────────────┬─────────────┬─────────────────────┐
│ version | record_tag  | time_bucket │ series_fingerprint  │
│ 1 byte  │   8 bits    │   4 bytes   │   16 bytes          │
└─────────┴─────────────┴─────────────┴─────────────────────┘
```

**Key Fields:**

- `version` (u8): Key format version (currently `0x01`)
- `record_tag` (u8): Record tag encoding the record type and bucket size
  - `bits 7-4` (u4): Record type (`0x02` for `SeriesDictionary`)
  - `bits 3-0` (u4): Bucket size in hours
- `time_bucket` (u32): The number of minutes since the UNIX epoch
- `series_fingerprint` (u128): The fingerprint of the label set, computed as a hash of the labels

The series fingerprint is computed using the labels of the series. The value
contains the series ID that corresponds to this fingerprint. Note that these IDs
are scoped to a single time bucket so the cardinality is measured in the number
of series in the bucket, not globally.

**Value Schema:**

```
┌──────────────────────────────────────────────────────────┐
│                   SeriesDictionaryValue                  │
├──────────────────────────────────────────────────────────┤
│  series_id: u32                                          │
└──────────────────────────────────────────────────────────┘
```

**Structure:**

- The value is a single `series_id` (u32) that has the specified fingerprint.
  Hash collisions are assumed not to occur (we should consider changing this to a
  FixedElementArray<SeriesId> to allow for hash collisions).

### `ForwardIndex` (`RecordType::ForwardIndex` = `0x03`)

The forward index is used during query time to map each series ID defined
through the index to its canonical specification, which includes all labels
defined as UTF-8 strings. This is used during query execution in order to map
each series discovered through the inverted index back to the labels which
define it so that they can be returned to the user.

**Key Layout:**
```
┌─────────┬─────────────┬─────────────┬──────────────┐
│ version | record_tag  | time_bucket │  series_id   │
│ 1 byte  │   8 bits    │   4 bytes   │   4 bytes    │
└─────────┴─────────────┴─────────────┴──────────────┘
```

**Key Fields:**

- `version` (u8): Key format version (currently `0x01`)
- `record_tag` (u8): Record tag encoding the record type and bucket size
  - `bits 7-4` (u4): Record type (`0x03` for `ForwardIndex`)
  - `bits 3-0` (u4): Bucket size in hours
- `time_bucket` (u32): The number of minutes since the UNIX epoch
- `series_id` (u32): The series ID, unique within the time bucket

**Value Schema:**

The forward index stores the canonical time-series specification keyed by
`(metric_id, series_id)` using compact references into the label and value
dictionaries.

```
┌──────────────────────────────────────────────────────────────────────────┐
│                          ForwardIndexValue                               │
├──────────────────────────────────────────────────────────────────────────┤
│  metric_unit:        OptionalNonEmptyUtf8                                │
│  metric_meta:        MetricMeta                                          │
│  label_count:        u16                                                 │
│  labels:             Array<LabelBinding>                                 │
│                                                                          │
│  MetricMeta                                                              │
│  ┌────────────────────────────────────────────────────────────────────┐  │
│  │  metric_type: u8                                                   │  │
│  │  flags:       u8                                                   │  │
│  └────────────────────────────────────────────────────────────────────┘  │
│                                                                          │
│  LabelBinding                                                            │
│  ┌────────────────────────────────────────────────────────────────────┐  │
│  │  name:  utf8                                                       │  │
│  │  value: utf8                                                       │  │
│  └────────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────┘
```

**Structure:**

- `metric_unit` (`OptionalNonEmptyUtf8`): Metric unit captured from the OTLP payload when present; a zero-length payload denotes `None`.
- `metric_meta` (`MetricMeta`): Encodes the series' metric type and auxiliary flags.
  - `metric_type` (u8): Enumeration matching `TimeSeriesSpec::metric_type` — `1=Gauge`, `2=Sum`, `3=Histogram`, `4=ExponentialHistogram`, `5=Summary`.
  - `flags` (u8): Bit-packed metadata; bits `0-1` store temporality (`0=Unspecified`, `1=Cumulative`, `2=Delta`), bit `2` is the `monotonic` flag (only meaningful when `metric_type=2`), remaining bits are reserved and must be zero.
- `label_count` (u16): Total number of label bindings.
- `labels` (`Array<LabelBinding>`): All label bindings encoded as `label_count` followed by that many `LabelBinding` entries.
  - `name` (`Utf8`): Label name.
  - `value` (`Utf8`): Label value.

All label groups are serialized in lexicographical order of the `name` and
`value` fields. Given the frequent repetition of label names and values, a
standard compression algorithm will significantly reduce the size of the record
even absent a dedicated dictionary.

### `InvertedIndex` (`RecordType::InvertedIndex` = `0x04`)

The inverted index maps each label and value to a list of the series which
have defined that label and value (also known as a posting list). For
example, suppose that series 713 identifies two labels: A=1 and B=2. The
label value `A=1` will have a posting list which then includes series 713,
and similarly for `B=2`.

**Key Layout:**
```
┌─────────┬──────────────┬─────────────┬──────────────────┬─────────────┐
│ version │  record_tag  │ time_bucket │      label       │   value     │
│ 1 byte  │   8 bits     │   4 bytes   │ terminated bytes │  raw utf8   │
└─────────┴──────────────┴─────────────┴──────────────────┴─────────────┘
```

**Key Fields:**
- `version` (u8): Key format version (currently `0x01`)
- `record_tag` (u8): Record tag encoding the record type and bucket size
  - `bits 7-4` (u4): Record type (`0x04` for `InvertedIndex`)
  - `bits 3-0` (u4): Bucket size in hours
- `time_bucket` (u32): The number of minutes since the UNIX epoch (big-endian)
- `label` (terminated bytes): Label name encoded using `terminated_bytes::serialize`,
  which escapes `0x00` and `0x01` bytes and appends a `0x00` terminator. This
  preserves lexicographical ordering and unambiguously separates the label from
  the value.
- `value` (raw UTF-8): Label value stored as raw UTF-8 bytes without a terminator.
  Since this is the last field in the key, the end of the key implicitly delimits
  the value.

**Encoding Rationale:**

Only the label (attribute) field uses terminated encoding because it must be
unambiguously separated from the value field. The value does not require a
terminator since it extends to the end of the key. This minimizes key size while
preserving the ability to do efficient prefix scans by label name.


Note that since the label names and values are stored lexicographically,
SlateDB's prefix encoding will be very effective without the use of a
dictionary.

**Value Schema:**

The value schema stores the posting list of series IDs.

```
┌──────────────────────────────────────────────────────────┐
│                   InvertedIndexValue                     │
├──────────────────────────────────────────────────────────┤
│  RoaringBitmap<series_id: u32>                           │
└──────────────────────────────────────────────────────────┘
```

**Structure:**

- The value is a RoaringBitmap encoding the set of `series_id` values (u32) that
  have the specified label/value pair for the specified metric.
- Series IDs within the bitmap are maintained in sorted order.
- RoaringBitmap provides efficient compression and set operations.

### `TimeSeries` (`RecordType::TimeSeries` = `0x05`)

The time series record type is used to store the actual values of a timestamp.
It uses the same key structure as the forward index except for the record type.

**Key Layout:**
```
┌─────────┬──────────────┬─────────────┬──────────────┐
│ version │  record_tag  │ time_bucket │  series_id   │
│ 1 byte  │   8 bits     │   4 bytes   │   4 bytes    │
└─────────┴──────────────┴─────────────┴──────────────┘
```

**Key Fields:**
- `version` (u8): Key format version (currently `0x01`)
- `record_tag` (u8): Record tag encoding the record type and bucket size
  - `bits 7-4` (u4): Record type (`0x05` for `TimeSeries`)
  - `bits 3-0` (u4): Bucket size in hours
- `time_bucket` (u32): The number of minutes since the UNIX epoch
- `series_id` (u32): The series ID, unique within the time bucket

**Value Schema:**

Time series data is stored as a compressed sequence of `(timestamp, value)`
tuples using the Gorilla compression algorithm, which is optimized for
time-series workloads with temporal locality.

```
┌──────────────────────────────────────────────────────┐
│                    TimeSeriesValue                   │
├──────────────────────────────────────────────────────┤
│  Gorilla-encoded stream of:                          │
│    timestamp: u64  (epoch milliseconds)              │
│    value:     f64  (IEEE 754 double-precision)       │
└──────────────────────────────────────────────────────┘
```

**Structure:**

- Each time series is stored as a Gorilla-compressed stream of data points
- `timestamp` (u64): Epoch milliseconds, delta-encoded and compressed
- `value` (f64): Double-precision floating-point value, XOR-compressed

**Metric Type Handling:**

Following the Prometheus approach, all OpenTelemetry metric values are normalized to `f64`:

- **Gauges/Counters**: Stored directly as `(timestamp, value)` pairs
- **Histograms**: Decomposed into multiple distinct series using the same naming and labeling scheme that Prometheus expects:
  - `metric_name_bucket{le="<upper>"}` for each bucket, where the stored value is the cumulative count up to `explicit_bounds[i]` and the final bucket uses `le="+Inf"`. Delta OpenTelemetry histograms are first accumulated so that the series remain monotonically increasing like native Prometheus histograms.
  - `metric_name_sum` stores the floating-point sum of all observations.
  - `metric_name_count` stores the running total number of observations.

The mappings mirror the OpenTelemetry Collector's [OTLP to Prometheus
translation](https://github.com/open-telemetry/opentelemetry-collector/blob/main/translator/prometheus/README.md#otlp-to-prometheus),
which ensures downstream tooling sees histogram data with the same semantics it
expects from Prometheus-native scrapes. This representation allows histograms to
be queried and aggregated using the same mechanisms as simple gauge and counter
metrics.

## Alternatives

### Dictionary Encoding for Labels

An alternative approach would use dictionary encoding to map label names and
values to integer IDs, reducing storage size for the inverted and forward
indexes. For example:

```
Label Dictionary: {"method": 1, "GET": 2, "status": 3, "200": 4}
Index Entry: [1, 2] -> [series_ids...]  // instead of "method=GET"
```

This was rejected for several reasons:

1. **Added complexity** — Dictionary encoding requires maintaining separate
  dictionary records, coordinating ID assignment during ingestion, and resolving
  IDs back to strings during query execution. This adds significant bookkeeping
  overhead.

2. **SlateDB prefix encoding** — Since label names and values are stored
  lexicographically in inverted index keys, SlateDB's built-in prefix encoding
  already provides effective compression. Keys with common prefixes (e.g.,
  `status=200`, `status=404`, `status=500`) share prefix bytes automatically.

3. **Industry validation** — [VictoriaMetrics stores labels as raw byte arrays](https://victoriametrics.com/blog/vmstorage-how-indexdb-works/) rather
  than dictionary-encoded IDs, demonstrating that this simpler approach scales
  well in production. Their IndexDB design prioritizes search efficiency over
  encoding compression.

4. **High-cardinality concerns** — Dictionary encoding can become a bottleneck
  with high-cardinality labels. The dictionary itself grows unboundedly and must
  be consulted for every lookup, whereas direct encoding has no such coordination
  overhead.

### Metric ID Prefix for Inverted Index

An alternative would prefix inverted index keys with a metric identifier:

```
Current:  | version | tag | bucket | label | value |
Alt:      | version | tag | bucket | metric_id | label | value |
```

This was rejected because treating the metric name as a first-class prefix would
limit query flexibility:

1. **Query planning flexibility** — By treating `__name__` (metric name) as a
  regular label, the query execution layer can use posting list statistics to
  determine whether filtering by metric name is selective. Some queries benefit
  from filtering by other labels first (e.g., `{cluster="prod"}` may be more
  useful than `{__name__="http_requests_total"}`).

2. **Cross-metric queries** — Queries like `{job="api", status="500"}` that span
  multiple metrics would require scanning all metric prefixes, whereas a flat
  structure allows direct lookup.

3. **Prometheus/VictoriaMetrics alignment** — [Neither VictoriaMetrics nor Prometheus treat the metric name as a first-class label](https://docs.victoriametrics.com/keyconcepts/). Following their lead ensures compatibility with existing query patterns and tooling expectations.

### Namespace Prefix

An alternative would add a namespace prefix to all record keys, enabling
multi-tenant storage within a single deployment:

```
| version | tag | namespace_id | bucket | ... |
```

This was rejected in favor of separate deployments per namespace:

1. **Simpler storage model** — Without namespaces, every record type has one
  fewer field to encode, decode, and filter on. This simplifies the codebase and
  reduces the surface area for bugs.

2. **Better isolation** — Separate deployments provide complete isolation between
  tenants: independent failure domains, no noisy-neighbor effects, and no risk of
  cross-tenant data leakage.

3. **Independent scaling** — Different namespaces often have vastly different
  cardinality and query patterns. Separate deployments allow right-sizing
  resources (memory, storage, CPU) for each workload without compromise.

4. **Independent retention** — Retention policies, compaction schedules, and
  rollup configurations can vary per deployment without complicating the storage
  layer with per-namespace settings.

## Updates

| Date       | Description |
|------------|-------------|
| 2025-12-17 | Initial draft |
| 2026-01-18 | Updated InvertedIndex key encoding: attribute uses terminated bytes, value uses raw UTF-8 (backward incompatible) |
