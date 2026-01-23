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

- Supporting binary keys and values via HTTP.
- Non-HTTP APIs over protocols like gRPC, etc.
- Streaming APIs (e.g., pushing a stream of log entries, subscribing to the log as a stream). These are valuable but deferred to a future RFC.

## Design

The HTTP server will use HTTP/2 as the transport protocol.

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
    { "key": "my-key", "value": "my-value" }
  ],
  "await_durable": false
}
```

**Success Response (200):**

```json
{
  "status": "success",
  "records_appended": 1,
  "start_sequence": 0,
}
```

The `start_sequence` is the sequence number assigned to the first record (inclusive).

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
  "key": "my-key",
  "entries": [
    { "sequence": 0, "value": "my-value" }
  ]
}
```

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
    { "id": 0, "start_seq": 0, "start_time_ms": 1705766400000 },
    { "id": 1, "start_seq": 100, "start_time_ms": 1705766460000 }
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
  "keys": [
    { "key": "events" },
    { "key": "orders" }
  ]
}
```

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

### Supporting binary protocols and payloads

We should support binary APIs and protocols in the future, but they can complement the HTTP API. Similarly support
for a non json payloads within the HTTP protocol can be added as a future extension. 


## Open Questions

   * The proposal as presented mimics the Rust API for OpenData-Log. Should we consider APIs that are more HTTP-native? 

## Updates

| Date       | Description |
|------------|-------------|
| 2026-01-22 | Initial draft |
