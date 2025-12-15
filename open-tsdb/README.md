# OpenTSDB

OpenTSDB is a time series database with prometheus-like APIs which uses SlateDB
as the underlying storage engine. 

## Record Layout

Record keys in SlateDB are built by concatenating big-endian binary tokens with
no delimiteres. Each token is only unique within its prefix and lexographical
ordering matches the numeric order of encoded values.

Keys are referenced using two forms:

1. `XXX_id` - a _scoped identifier_ unique within the scope of `XXX`
2. `XXX_path` - a _fully qualified path_ which is globally unique

Examples of this convention:

- `series_id`: a token identifying a time series which is unique within its scope
- `series_path`: the fully qualified path that includes the scope (e.g. `<namespace_id><metric_id><series_id>`)

### Standard Key Prefix

All records use a standard, 2-byte prefix: a single `u8` for the record version and
another `u8` for the record tag. The record tag is encoded as two 4-bit fields. The
high 4 bits are the record type. The lower 4 bits depend on the scope of the record.
Globally scoped records set the lower 4 bits to 0x00 for future use and bucket scoped
records set the lower 4 bits to encode the `TimeBucketSize` allowing different time
granularities to coexist.

The `TimeBucketSize` is encoded exponentially: a value of `n` represents `2^(n-1)` hours.
For example, `1` = 1 hour, `2` = 2 hours, `3` = 4 hours, `4` = 8 hours, etc. This allows
the system to efficiently represent a wide range of time bucket sizes (1 hour to 16,384 hours)
using only 4 bits. 

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
record key is a `u32` representing number of minutes since the UNIX epoch. The
byte ordering must be big-endian to be consistent with lexicographic ordering
from SlateDB.

OpenTSDB will eventually support windows of time at different granularities.
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
  indicates that the field was not provided. Ingestion never emits empty
  strings for these fields, so `len > 0` implies a non-empty UTF-8 payload.
- `Array<T>`: `count: u16` (little-endian) followed by `count` serialized
  elements of type `T`, encoded back-to-back with no additional padding.
- `SingleArray<T>`: Serialized elements of type `T`, encoded back-to-back 
  with no additional padding. The array is not preceded by a count since
  this array must only be used when the schema only contains a single
  array and nothing else.

**Note on Endianness**: Value schemas use little-endian encoding for multi-byte
integers. This differs from key schemas, which use big-endian to maintain
lexicographic ordering for range scans. Since values don't participate in
ordering comparisons, little-endian provides better performance on common
architectures (x86, ARM).

### Record Type Reference

| ID | Name | Description |
|----|------|-------------|
| `0x00` | | Reserved |
| `0x01` | `BucketList` | Lists the available time buckets |
| `0x02` | `SeriesDictionary` | Stores the mapping of series to series ids |
| `0x03` | `ForwardIndex` | Stores the canonical attributes set for each series |
| `0x04` | `InvertedIndex` | Maps attirbute/value pairs to posting lists of matching series ids |
| `0x05` | `TimeSeries` | Holds the raw time-series payloads |

## Record Definitions & Schemas

### `BucketList` (`RecordType::BucketList` = `0x01`)

The buckets list is used to discover within each namespace the set of time buckets
that are available (i.e. have data). In addition to enumerating each bucket, the
listing also indicates its granularity. When data is rolled up into coarser buckets
as part of the compaction process, the old finer-grained buckets will be replaced
by the new coarse buckets in the listing.

**Key Layout:**

```
┌─────────┬─────────────┐
│ version | record_tag  |
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
│  SingleArray<(bucket_size: u8, time_bucket: u32)>       │
└─────────────────────────────────────────────────────────┘
```

- `bucket_size` (u8): The size of the time bucket in hours
- `time_bucket` (u32): The number of minutes since the UNIX epoch

### `SeriesDictionary` (`RecordType::SeriesDictionary` = `0x02`)

The series dictionary maps label sets (attribute/value pairs) to series IDs. This
enables ingestion to resolve a series ID for a given label set, and allows query
execution to map series IDs back to their label sets when needed.

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

The series fingerprint is computed using the labels of the series, which means
there is a small chance that two series with different labels will have the same
fingerprint. Prometheus makes the same trade-off and accepts the risk of
collisions. The value contains the series ID(s) that correspond to this
fingerprint, typically a single series ID. Note that these IDs are scoped to a
single time bucket so the cardinality is measured in the number of series in the
bucket, not globally.

**Value Schema:**

```
┌──────────────────────────────────────────────────────────┐
│                   SeriesDictionaryValue                  │
├──────────────────────────────────────────────────────────┤
│  SingleArray<series_id: u32>                             │
└──────────────────────────────────────────────────────────┘
```

**Structure:**

- The value is a `SingleArray<u32>` encoding the list of `series_id` values that
  have the specified fingerprint. In the common case of no collisions, this array
  contains a single `series_id`. When collisions occur, multiple series IDs are
  stored, and the forward index must be consulted to disambiguate by comparing
  the actual label sets.

### `ForwardIndex` (`RecordType::ForwardIndex` = `0x03`)

The forward index is used during query time to map each series ID
defined through the index to its canonical specification, which includes all
attributes defined as UTF-8 strings. This is used during query execution in order
to map each series discovered through the inverted index back to the attributes
which define it so that they can be returned to the user.

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
`(metric_id, series_id)` using compact references into the attribute and value
dictionaries.

```
┌──────────────────────────────────────────────────────────────────────────┐
│                          ForwardIndexValue                               │
├──────────────────────────────────────────────────────────────────────────┤
│  metric_unit:        OptionalNonEmptyUtf8                                │
│  metric_meta:        MetricMeta                                          │
│  attr_count:         u16                                                 │
│  attrs:              Array<AttributeBinding>                             │
│                                                                          │
│  MetricMeta                                                              │
│  ┌────────────────────────────────────────────────────────────────────┐  │
│  │  metric_type: u8                                                   │  │
│  │  flags:       u8                                                   │  │
│  └────────────────────────────────────────────────────────────────────┘  │
│                                                                          │
│  AttributeBinding                                                        │
│  ┌────────────────────────────────────────────────────────────────────┐  │
│  │  attr:  utf8                                                       │  │
│  │  value: utf8                                                       │  │
│  └────────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────┘
```

**Structure:**

- `metric_unit` (`OptionalNonEmptyUtf8`): Metric unit captured from the OTLP payload when present; a zero-length payload denotes `None`.
- `metric_meta` (`MetricMeta`): Encodes the series' metric type and auxiliary flags.
  - `metric_type` (u8): Enumeration matching `TimeSeriesSpec::metric_type` — `1=Gauge`, `2=Sum`, `3=Histogram`, `4=ExponentialHistogram`, `5=Summary`.
  - `flags` (u8): Bit-packed metadata; bits `0-1` store temporality (`0=Unspecified`, `1=Cumulative`, `2=Delta`), bit `2` is the `monotonic` flag (only meaningful when `metric_type=2`), remaining bits are reserved and must be zero.
- `attr_count` (u16): Total number of attribute bindings.
- `attrs` (`Array<AttributeBinding>`): All attribute bindings encoded as `attr_count` followed by that many `AttributeBinding` entries.
  - `attr` (`Utf8`): Attribute name.
  - `value` (`Utf8`): Attribute value.

All attribute groups are serialized in lexicographical order of the `attr` and
`value` fields. Given the frequent repitition of attribute names and values, a
standard compression algorithm will significantly reduce the size of the record
even absent a dedicated dictionary.

NOTE: this is what VictoriaMetrics does (essentially) for its forward index and
is evidence that this is likely sufficient for our use case to reduce complexity.

### `InvertedIndex` (`RecordType::InvertedIndex` = `0x04`)

The inverted index maps each attribute and value to a list of the series which
have defined that attribute and value (also known as a posting list).
For example, suppose that series 713 identifies two attributes: A=1 and B=2.
The attribute value `A=1` will have a posting list which then includes series 713,
and similarly for `B=2`.

**Key Layout:**
```
┌─────────┬──────────────┬─────────────┬────────────┬─────────────┐
│ version │  record_tag  │ time_bucket │ attribute  │   value     │
│ 1 byte  │   8 bits     │   4 bytes   │   utf8     │   utf8      │
└─────────┴──────────────┴─────────────┴────────────┴─────────────┘
```

**Key Fields:**
- `version` (u8): Key format version (currently `0x01`)
- `record_tag` (u8): Record tag encoding the record type and bucket size
  - `bits 7-4` (u4): Record type (`0x04` for `InvertedIndex`)
  - `bits 3-0` (u4): Bucket size in hours
- `time_bucket` (u32): The number of minutes since the UNIX epoch
- `attribute` (`Utf8`): Attribute name.
- `value` (`Utf8`): Attribute value.

Note that since the attribute names and values are stored lexicographically,
SlateDB's prefix encoding will be very effective without the use of a dictionary.

NOTE: VictoriaMetrics also does not use a dictionary for its inverted index and
is evidence that this is likely sufficient for our use case to reduce complexity.

NOTE: Neither VictoraiMetrics nor Prometheus treat the metric name as a first-class
attribute, so we'll follow their lead here. This will allow the query execution layer
to use statistics on the posting list lenght to determine whether or not the metric name
is a good filter candidate.

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
  have the specified attribute/value pair for the specified metric.
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

Time series data is stored as a compressed sequence of `(timestamp, value)` tuples using
the Gorilla compression algorithm, which is optimized for time-series workloads with
temporal locality.

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
- **Histograms**: Decomposed into multiple distinct series using the same naming
  and labeling scheme that Prometheus expects:
  - `metric_name_bucket{le="<upper>"}` for each bucket, where the stored value is
    the cumulative count up to `explicit_bounds[i]` and the final bucket uses
    `le="+Inf"`. Delta OpenTelemetry histograms are first accumulated so that the
    series remain monotonically increasing like native Prometheus histograms.
  - `metric_name_sum` stores the floating-point sum of all observations.
  - `metric_name_count` stores the running total number of observations.

The mappings mirror the OpenTelemetry Collector's
[OTLP ↔︎ Prometheus translation](https://github.com/open-telemetry/opentelemetry-collector/blob/main/translator/prometheus/README.md#otlp-to-prometheus),
which ensures downstream tooling sees histogram data with the same semantics it
expects from Prometheus-native scrapes. This representation allows histograms
to be queried and aggregated using the same mechanisms as simple gauge and
counter metrics.