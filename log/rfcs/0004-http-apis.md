# RFC 0004: HTTP APIs

**Status**: Draft 

**Authors**:
- [Apurva Mehta](https://github.com/apurvam)

## Summary

This RFC introduce HTTP APIs for the OpenData-Log, enabling users to append and scan events using HTTP with JSON 
payloads.

## Motivation

A native HTTP API will make OpenData-Log maximally accessible to developers, enabling access to the log in any language
and programming framework. By being the most accessible protocol and by supporting a text payload format, the HTTP API
will improve the out-of-the-box developer experience. It does not preclude more performant binary APIs from being added
later.

## Goals

- Add a basic HTTP API with a JSON payload

## Non-Goals

- gRPC service endpoints. The protobuf schema defined in this RFC is designed to be compatible with a future gRPC API, which will be proposed in a separate RFC.
- Streaming APIs (e.g., pushing a stream of log entries, subscribing to the log as a stream). These are valuable but deferred to a future RFC.

## Design

The HTTP server will use HTTP/2 as the transport protocol.

### Message Format

The API supports both binary protobuf and ProtoJSON formats, with a single protobuf schema serving as the source of 
truth for all message definitions. 

**Content Types:**
- Binary protobuf: `Content-Type: application/x-protobuf`
- JSON: `Content-Type: application/json`

**Field Naming:**
- JSON field names use `lowerCamelCase` per the ProtoJSON specification
- URL query parameters remain `snake_case` (standard URL convention)

#### Proto Schema

```protobuf
syntax = "proto3";
package opendata.log.v1;

message AppendRequest {
  repeated Record records = 1;
  bool await_durable = 2;
}

message Record {
  bytes key = 1;
  bytes value = 2;
}

message AppendResponse {
  string status = 1;
  int32 records_appended = 2;
  uint64 start_sequence = 3;
  uint64 end_sequence = 4;
}

message ScanResponse {
  string status = 1;
  bytes key = 2;
  repeated Entry entries = 3;
}

message Entry {
  uint64 sequence = 1;
  bytes value = 2;
}

message SegmentsResponse {
  string status = 1;
  repeated Segment segments = 2;
}

message Segment {
  uint32 id = 1;
  uint64 start_seq = 2;
  int64 start_time_ms = 3;
}

message KeysResponse {
  string status = 1;
  repeated bytes keys = 2;
}

message CountResponse {
  string status = 1;
  uint64 count = 2;
}

message ErrorResponse {
  string status = 1;
  string message = 2;
}
```

#### Interoperability Example

The same logical message can be produced and consumed by both Rust clients (binary) and HTTP clients (JSON):

**Rust client writing binary protobuf:**
```rust
let request = AppendRequest {
    records: vec![Record { key: "my-key".into(), value: "my-value".into() }],
    await_durable: false,
};
let binary = request.encode_to_vec();
client.post("/api/v1/log/append")
    .header("Content-Type", "application/x-protobuf")
    .body(binary)
    .send();
```

**HTTP client writing JSON:**
```bash
curl -X POST http://localhost:8080/api/v1/log/append \
  -H "Content-Type: application/json" \
  -d '{
    "records": [{"key": "bXkta2V5", "value": "bXktdmFsdWU="}],
    "awaitDurable": false
  }'
```

Both produce the same logical message that can be read by either client type.

**Note:** Since `key` and `value` are `bytes` fields in the proto schema, JSON clients must use base64 encoding per the
[ProtoJSON specification](https://protobuf.dev/programming-guides/proto3/#json). The examples above use base64-encoded
versions of "my-key" (`bXkta2V5`) and "my-value" (`bXktdmFsdWU=`).


### Log HTTP Server API Summary

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/log/append` | POST | Append records to the log |
| `/api/v1/log/scan` | GET | Scan entries by key and sequence range |
| `/api/v1/log/keys` | GET | List distinct keys within a segment range |
| `/api/v1/log/segments` | GET | List segments overlapping a sequence range |
| `/api/v1/log/count` | GET | Count entries for a key |
| `/metrics` | GET | Prometheus metrics |

### HTTP Status Codes

| Status Code | Description |
|-------------|-------------|
| 200 | Success |
| 400 | Bad Request - Invalid input parameters (missing required params, malformed values) |
| 500 | Internal Server Error - Storage errors, encoding errors, or internal failures |

### Error Response Format

All error responses follow this format:

```json
{
  "status": "error",
  "message": "Missing required parameter: key"
}
```

### APIs 

#### Append
`POST /api/v1/log/append`

Append records to the log.

Request Body:

```json
{
  "records": [
    { "key": "bXkta2V5", "value": "bXktdmFsdWU=" }
  ],
  "awaitDurable": false
}
```

Keys and values are base64-encoded in JSON (ProtoJSON spec for `bytes` fields).

**Success Response (200):**

```json
{
  "status": "success",
  "recordsAppended": 1,
  "startSequence": 0
}
```

The `startSequence` is the sequence number assigned to the first record (inclusive).

**Error Responses:**
- `400` - Invalid request body or empty records array
- `500` - Storage write failure

#### Scan

`GET /api/v1/log/scan`

Scan entries for a specific key within a sequence range.

Query Parameters:

| Param | Type | Required | Description |
|-------|------|----------|-------------|
| key | string | yes | Key to scan |
| start_seq | u64 | no | Start sequence (inclusive), default: 0 |
| end_seq | u64 | no | End sequence (exclusive), default: u64::MAX |
| limit | usize | no | Max entries to return, default: 1000 |

**Success Response (200):**

```json
{
  "status": "success",
  "key": "bXkta2V5",
  "entries": [
    { "sequence": 0, "value": "bXktdmFsdWU=" }
  ]
}
```

Keys and values are base64-encoded in JSON.

**Error Responses:**
- `400` - Missing required `key` parameter or invalid sequence range
- `500` - Storage read failure

---

#### Segments

`GET /api/v1/log/segments`

List segments overlapping a sequence range. Use this to discover segment boundaries before calling /keys.

Query Parameters:

| Param | Type | Required | Description |
|-------|------|----------|-------------|
| start_seq | u64 | no | Start sequence (inclusive), default: 0 |
| end_seq | u64 | no | End sequence (exclusive), default: u64::MAX |
| limit | usize | no | Max segments to return, default: 1000 |

**Success Response (200):**

```json
{
  "status": "success",
  "segments": [
    { "id": 0, "startSeq": 0, "startTimeMs": 1705766400000 },
    { "id": 1, "startSeq": 100, "startTimeMs": 1705766460000 }
  ]
}
```

**Error Responses:**
- `400` - Invalid sequence range parameters
- `500` - Storage read failure

---

#### Keys

`GET /api/v1/log/keys`

List distinct keys within a segment range.

Query Parameters:

| Param | Type | Required | Description |
|-------|------|----------|-------------|
| start_segment | u32 | no | Start segment ID (inclusive), default: 0 |
| end_segment | u32 | no | End segment ID (exclusive), default: u32::MAX |
| limit | usize | no | Max keys to return, default: 1000 |

**Success Response (200):**

```json
{
  "status": "success",
  "keys": ["ZXZlbnRz", "b3JkZXJz"]
}
```

Keys are base64-encoded in JSON.

**Error Responses:**
- `400` - Invalid segment range parameters
- `500` - Storage read failure

---

#### Count

`GET /api/v1/log/count`

Count entries for a key within a sequence range.

Query Parameters:

| Param | Type | Required | Description |
|-------|------|----------|-------------|
| key | string | yes | Key to count |
| start_seq | u64 | no | Start sequence (inclusive), default: 0 |
| end_seq | u64 | no | End sequence (exclusive), default: u64::MAX |

**Success Response (200):**

```json
{ "status": "success", "count": 42 }
```

**Error Responses:**
- `400` - Missing required `key` parameter or invalid sequence range
- `500` - Storage read failure

## Alternatives

### Supporting plain JSON over HTTP

The current proposal adopts `ProtoJSON` as the the JSON format. This brings support for binary payloads. It also means
that keys and values are base64 encoded. The alternative was to allow plain JSON over HTTP without any protobuf schema.
The latter approach was rejected because we will to support typed binary payloads for the log, and moving from a
flexible JSON payload will make the transition to a properly typed payload with a fixed schema impossible.  
]
## Open Questions

   * The proposal as presented mimics the Rust API for OpenData-Log. Should we consider APIs that are more HTTP-native? 

## Updates

| Date       | Description |
|------------|-------------|
| 2026-01-22 | Initial draft |
