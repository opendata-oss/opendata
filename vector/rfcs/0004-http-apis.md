# RFC 0004: HTTP APIs

**Status**: Draft

**Authors**:
- [Rohan Desai](https://github.com/rodesai) 

## Summary

This RFC introduces HTTP APIs for OpenData Vector, enabling clients to write vectors, search for
nearest neighbors, and fetch vectors by ID over HTTP using either binary protobuf or JSON payloads.

## Motivation

- Makes OpenData Vector accessible from any language or runtime without requiring a rust client
- Allows multiple client processes to write to a single db
- Allows multiple client processes to read from a single db or reader while sharing a cache.

## Goals

- Add a basic HTTP API for vector write and read operations
- Support both binary protobuf and JSON using a single protobuf schema
- Keep the HTTP data model aligned with the Rust `Vector` API

## Non-Goals

- gRPC service endpoints. The protobuf schema in this RFC is intended to be reusable for a future
  gRPC API, but no gRPC transport is introduced here.
- Streaming APIs.

## Design

### Message Format

The API supports both binary protobuf and ProtoJSON-style JSON, with the protobuf schema acting as
the source of truth for message definitions.

**Content Types:**
- Binary protobuf: `Content-Type: application/protobuf`
- JSON: `Content-Type: application/protobuf+json`

**Response Negotiation:**
- Success responses use binary protobuf when `Accept: application/protobuf` is present
- Otherwise success responses default to JSON
- Error responses are JSON

**Field Naming:**
- JSON field names use `lowerCamelCase`
- Protobuf field names remain `snake_case`

#### Proto Schema

```protobuf
syntax = "proto3";
package opendata.vector.v1;

message Vector {
  string id = 1;
  map<string, AttributeValueMessage> attributes = 3;
}

message AttributeValueMessage {
  oneof value {
    string string_value = 1;
    int64 int64_value = 2;
    double float64_value = 3;
    bool bool_value = 4;
    VectorValue vector_value = 5;
  }
}

message VectorValue {
  repeated float values = 1;
}

message WriteRequest {
  repeated Vector upsert_vectors = 1;
}

message UpsertVectorsResponse {
  string status = 1;
  int32 vectors_upserted = 2;
}

message ComparisonFilter {
  string field = 1;
  AttributeValueMessage value = 2;
}

message InFilter {
  string field = 1;
  repeated AttributeValueMessage values = 2;
}

message FilterList {
  repeated FilterMessage filters = 1;
}

message FilterMessage {
  oneof kind {
    ComparisonFilter eq = 1;
    ComparisonFilter neq = 2;
    InFilter in = 3;
    FilterList and = 4;
    FilterList or = 5;
  }
}

message SearchRequest {
  repeated float vector = 1;
  uint32 k = 2;
  optional uint32 nprobe = 3;
  optional FilterMessage filter = 4;
  repeated string include_fields = 5;
}

message SearchResponse {
  string status = 1;
  repeated SearchResult results = 2;
}

message SearchResult {
  float score = 1;
  optional Vector vector = 2;
}

message GetVectorResponse {
  string status = 1;
  optional Vector vector = 2;
}

message ErrorResponse {
  string status = 1;
  string message = 2;
}
```

#### JSON Interoperability Note

The JSON form intentionally flattens attribute values instead of using canonical ProtoJSON wrapper
objects for `AttributeValueMessage`.

For example, the server emits:

```json
{
  "id": "doc-1",
  "attributes": {
    "vector": [1.0, 2.0],
    "category": "electronics",
    "department": "hw"
  }
}
```

rather than canonical protobuf JSON such as:

```json
{
  "id": "doc-1",
  "attributes": {
    "category": { "stringValue": "electronics" }
  }
}
```

For JSON write requests, the server resolves attribute value types using the collection metadata
schema from the configured vector DB when available.

For JSON search requests, the optional `filter` field uses a flattened operator form rather than
canonical ProtoJSON for the filter messages. The currently supported operators are `eq`, `neq`,
`in`, `and`, and `or`.

#### Interoperability Example

**Rust client writing binary protobuf:**

```rust
let request = WriteRequest {
    upsert_vectors: vec![Vector {
        id: "doc-1".to_string(),
        attributes: std::collections::HashMap::from([
            (
                "vector".to_string(),
                AttributeValueMessage {
                    value: Some(AttributeValueProto::VectorValue(VectorValue {
                        values: vec![1.0, 2.0, 3.0],
                    })),
                },
            ),
            (
                "category".to_string(),
                AttributeValueMessage {
                    value: Some(AttributeValueProto::StringValue("electronics".to_string())),
                },
            ),
        ]),
    }],
};
let binary = request.encode_to_vec();
client.post("/api/v1/vector/write")
    .header("Content-Type", "application/protobuf")
    .body(binary)
    .send();
```

**HTTP client writing JSON:**

```bash
curl -X POST http://localhost:8080/api/v1/vector/write \
  -H "Content-Type: application/protobuf+json" \
  -d '{
    "upsertVectors": [
      {
        "id": "doc-1",
        "attributes": {
          "vector": [1.0, 2.0, 3.0],
          "category": "electronics"
        }
      }
    ]
  }'
```

Both represent the same logical write request.

### Vector HTTP Server API Summary

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/vector/write` | POST | Write or upsert vectors |
| `/api/v1/vector/search` | POST | Search for nearest neighbors |
| `/api/v1/vector/vectors/{id}` | GET | Fetch a vector by external ID |
| `/-/healthy` | GET | Liveness probe |
| `/-/ready` | GET | Readiness probe |

### HTTP Status Codes

| Status Code | Description |
|-------------|-------------|
| 200 | Success |
| 400 | Bad Request - Invalid request body or query input |
| 404 | Not Found - Vector ID not found |
| 500 | Internal Server Error - Storage or internal failures |

### Error Response Format

All error responses use JSON:

```json
{
  "status": "error",
  "message": "vector is required"
}
```

## APIs

#### Write

`POST /api/v1/vector/write`

Write or upsert vectors.

Request Body:

```json
{
  "upsertVectors": [
    {
      "id": "doc-1",
      "attributes": {
        "vector": [1.0, 2.0, 3.0],
        "category": "electronics",
        "department": "hw"
      }
    }
  ]
}
```

**Success Response (200):**

```json
{
  "status": "success",
  "vectorsUpserted": 1
}
```

**Error Responses:**
- `400` - Invalid body, missing `id`, missing `vector`, or schema/type mismatch
- `500` - Storage write failure

#### Search

`POST /api/v1/vector/search`

Search for nearest neighbors to a query vector.

Request Body:

```json
{
  "vector": [1.0, 2.0, 3.0],
  "k": 10,
  "nprobe": 10,
  "filter": {
    "and": [
      {
        "eq": {
          "field": "category",
          "value": "electronics"
        }
      },
      {
        "eq": {
          "field": "department",
          "value": "hw"
        }
      }
    ]
  },
  "includeFields": ["category", "department"]
}
```

`nprobe`, `filter`, and `includeFields` are optional. If `includeFields` is absent, all fields
(including the vector embedding) are returned. If present, only the named fields are included in
each result's `attributes`. Use `"vector"` as a field name to include the embedding.

**Success Response (200):**

```json
{
  "status": "success",
  "results": [
    {
      "score": 0.0,
      "vector": {
        "id": "doc-1",
        "attributes": {
          "vector": [1.0, 2.0, 3.0],
          "category": "electronics",
          "department": "hw"
        }
      }
    }
  ]
}
```

For `L2`, lower scores are more similar. For `DotProduct`, higher scores are more similar.

**Error Responses:**
- `400` - Missing or empty `vector`, `k == 0`, malformed request body, or invalid filter
- `500` - Storage read or search failure

#### Get Vector

`GET /api/v1/vector/vectors/{id}`

Fetch a vector by external ID.

**Success Response (200):**

```json
{
  "status": "success",
  "vector": {
    "id": "doc-1",
    "attributes": {
      "vector": [1.0, 2.0, 3.0],
      "category": "electronics",
      "department": "hw"
    }
  }
}
```

**Error Responses:**
- `404` - No vector exists for the requested ID
- `400` - Invalid input
- `500` - Storage read or internal failure
