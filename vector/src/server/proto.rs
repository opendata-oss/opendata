//! Protobuf message definitions for the vector server HTTP API.
//!
//! These types support both binary protobuf encoding (application/protobuf)
//! and ProtoJSON encoding (application/protobuf+json) per RFC 0004-http-apis.

use std::collections::HashMap;

use prost::Message;
use serde::{Serialize, Serializer};

/// A vector with an ID and attributes, including the embedding vector under "vector".
#[derive(Clone, PartialEq, Message, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Vector {
    #[prost(string, tag = "1")]
    pub(crate) id: String,
    #[prost(map = "string, message", tag = "3")]
    #[serde(default)]
    pub(crate) attributes: HashMap<String, AttributeValueMessage>,
}

/// Wrapper message for attribute values in protobuf map fields.
#[derive(Clone, PartialEq, Message)]
pub(crate) struct AttributeValueMessage {
    #[prost(oneof = "AttributeValueProto", tags = "1, 2, 3, 4, 5")]
    pub(crate) value: Option<AttributeValueProto>,
}

impl AttributeValueMessage {
    pub(crate) fn new(value: AttributeValueProto) -> Self {
        Self { value: Some(value) }
    }
}

/// Protobuf oneof for attribute value types.
#[derive(Clone, PartialEq, prost::Oneof)]
#[allow(clippy::enum_variant_names)]
pub(crate) enum AttributeValueProto {
    #[prost(string, tag = "1")]
    StringValue(String),
    #[prost(int64, tag = "2")]
    Int64Value(i64),
    #[prost(double, tag = "3")]
    Float64Value(f64),
    #[prost(bool, tag = "4")]
    BoolValue(bool),
    #[prost(message, tag = "5")]
    VectorValue(VectorValueProto),
}

/// Wrapper for vector-valued attributes.
#[derive(Clone, PartialEq, Message)]
pub(crate) struct VectorValueProto {
    #[prost(float, repeated, tag = "1")]
    pub(crate) values: Vec<f32>,
}

impl Serialize for AttributeValueMessage {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self.value.as_ref() {
            Some(AttributeValueProto::StringValue(value)) => serializer.serialize_str(value),
            Some(AttributeValueProto::Int64Value(value)) => serializer.serialize_i64(*value),
            Some(AttributeValueProto::Float64Value(value)) => serializer.serialize_f64(*value),
            Some(AttributeValueProto::BoolValue(value)) => serializer.serialize_bool(*value),
            Some(AttributeValueProto::VectorValue(value)) => value.values.serialize(serializer),
            None => serializer.serialize_none(),
        }
    }
}

/// WriteRequest is the request body for POST /api/v1/vector/write.
#[derive(Clone, PartialEq, Message, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WriteRequest {
    #[prost(message, repeated, tag = "1")]
    pub(crate) upsert_vectors: Vec<Vector>,
}

/// UpsertVectorsResponse is the response for POST /api/v1/vector/write.
#[derive(Clone, PartialEq, Message, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct UpsertVectorsResponse {
    #[prost(string, tag = "1")]
    pub(crate) status: String,
    #[prost(int32, tag = "2")]
    pub(crate) vectors_upserted: i32,
}

impl UpsertVectorsResponse {
    pub(crate) fn success(vectors_upserted: i32) -> Self {
        Self {
            status: "success".to_string(),
            vectors_upserted,
        }
    }
}

#[derive(Clone, PartialEq, Message)]
pub(crate) struct ComparisonFilter {
    #[prost(string, tag = "1")]
    pub(crate) field: String,
    #[prost(message, optional, tag = "2")]
    pub(crate) value: Option<AttributeValueMessage>,
}

#[derive(Clone, PartialEq, Message)]
pub(crate) struct InFilter {
    #[prost(string, tag = "1")]
    pub(crate) field: String,
    #[prost(message, repeated, tag = "2")]
    pub(crate) values: Vec<AttributeValueMessage>,
}

#[derive(Clone, PartialEq, Message)]
pub(crate) struct FilterList {
    #[prost(message, repeated, tag = "1")]
    pub(crate) filters: Vec<FilterMessage>,
}

#[derive(Clone, PartialEq, Message)]
pub(crate) struct FilterMessage {
    #[prost(oneof = "FilterKind", tags = "1, 2, 3, 4, 5")]
    pub(crate) kind: Option<FilterKind>,
}

#[derive(Clone, PartialEq, prost::Oneof)]
pub(crate) enum FilterKind {
    #[prost(message, tag = "1")]
    Eq(ComparisonFilter),
    #[prost(message, tag = "2")]
    Neq(ComparisonFilter),
    #[prost(message, tag = "3")]
    In(InFilter),
    #[prost(message, tag = "4")]
    And(FilterList),
    #[prost(message, tag = "5")]
    Or(FilterList),
}

/// SearchRequest is the request body for POST /api/v1/vector/search.
#[derive(Clone, PartialEq, Message)]
pub(crate) struct SearchRequest {
    #[prost(float, repeated, tag = "1")]
    pub(crate) vector: Vec<f32>,
    #[prost(uint32, tag = "2")]
    pub(crate) k: u32,
    #[prost(uint32, optional, tag = "3")]
    pub(crate) nprobe: Option<u32>,
    #[prost(message, optional, tag = "4")]
    pub(crate) filter: Option<FilterMessage>,
    #[prost(string, repeated, tag = "5")]
    pub(crate) include_fields: Vec<String>,
}

/// SearchResponse is the response for POST /api/v1/vector/search.
#[derive(Clone, PartialEq, Message, Serialize)]
pub(crate) struct SearchResponse {
    #[prost(string, tag = "1")]
    pub(crate) status: String,
    #[prost(message, repeated, tag = "2")]
    pub(crate) results: Vec<SearchResultProto>,
}

impl SearchResponse {
    pub(crate) fn success(results: Vec<SearchResultProto>) -> Self {
        Self {
            status: "success".to_string(),
            results,
        }
    }
}

/// A single search result with score and the matched vector.
#[derive(Clone, PartialEq, Message, Serialize)]
pub(crate) struct SearchResultProto {
    #[prost(float, tag = "1")]
    pub(crate) score: f32,
    #[prost(message, optional, tag = "2")]
    pub(crate) vector: Option<Vector>,
}

/// GetVectorResponse is the response for GET /api/v1/vector/vectors/:id.
#[derive(Clone, PartialEq, Message, Serialize)]
pub(crate) struct GetVectorResponse {
    #[prost(string, tag = "1")]
    pub(crate) status: String,
    #[prost(message, optional, tag = "2")]
    pub(crate) vector: Option<Vector>,
}

impl GetVectorResponse {
    pub(crate) fn success(vector: Vector) -> Self {
        Self {
            status: "success".to_string(),
            vector: Some(vector),
        }
    }
}

/// ErrorResponse is returned for all error cases.
#[allow(dead_code)]
#[derive(Clone, PartialEq, Message)]
pub(crate) struct ErrorResponse {
    #[prost(string, tag = "1")]
    pub(crate) status: String,
    #[prost(string, tag = "2")]
    pub(crate) message: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_encode_and_decode_write_request() {
        // given
        let request = WriteRequest {
            upsert_vectors: vec![Vector {
                id: "doc-1".to_string(),
                attributes: HashMap::from([
                    (
                        "vector".to_string(),
                        AttributeValueMessage::new(AttributeValueProto::VectorValue(
                            VectorValueProto {
                                values: vec![1.0, 2.0, 3.0],
                            },
                        )),
                    ),
                    (
                        "category".to_string(),
                        AttributeValueMessage::new(AttributeValueProto::StringValue(
                            "test".to_string(),
                        )),
                    ),
                ]),
            }],
        };

        // when
        let encoded = request.encode_to_vec();
        let decoded = WriteRequest::decode(encoded.as_slice()).unwrap();

        // then
        assert_eq!(decoded.upsert_vectors.len(), 1);
        assert_eq!(decoded.upsert_vectors[0].id, "doc-1");
        assert_eq!(decoded.upsert_vectors[0].attributes.len(), 2);
        assert!(matches!(
            decoded.upsert_vectors[0].attributes.get("vector"),
            Some(AttributeValueMessage {
                value: Some(AttributeValueProto::VectorValue(VectorValueProto { values }))
            }) if values == &vec![1.0, 2.0, 3.0]
        ));
    }

    #[test]
    fn should_encode_and_decode_upsert_vectors_response() {
        // given
        let response = UpsertVectorsResponse::success(5);

        // when
        let encoded = response.encode_to_vec();
        let decoded = UpsertVectorsResponse::decode(encoded.as_slice()).unwrap();

        // then
        assert_eq!(decoded.status, "success");
        assert_eq!(decoded.vectors_upserted, 5);
    }

    #[test]
    fn should_encode_and_decode_search_request() {
        // given
        let request = SearchRequest {
            vector: vec![1.0, 2.0, 3.0],
            k: 10,
            nprobe: Some(20),
            filter: Some(FilterMessage {
                kind: Some(FilterKind::Eq(ComparisonFilter {
                    field: "category".to_string(),
                    value: Some(AttributeValueMessage::new(
                        AttributeValueProto::StringValue("electronics".to_string()),
                    )),
                })),
            }),
            include_fields: vec![],
        };

        // when
        let encoded = request.encode_to_vec();
        let decoded = SearchRequest::decode(encoded.as_slice()).unwrap();

        // then
        assert_eq!(decoded.vector, vec![1.0, 2.0, 3.0]);
        assert_eq!(decoded.k, 10);
        assert_eq!(decoded.nprobe, Some(20));
        assert!(matches!(
            decoded.filter.as_ref().and_then(|filter| filter.kind.as_ref()),
            Some(FilterKind::Eq(ComparisonFilter { field, value }))
                if field == "category"
                    && matches!(
                        value,
                        Some(AttributeValueMessage {
                            value: Some(AttributeValueProto::StringValue(value))
                        }) if value == "electronics"
                    )
        ));
    }

    #[test]
    fn should_encode_and_decode_search_response() {
        // given
        let response = SearchResponse::success(vec![SearchResultProto {
            score: 0.95,
            vector: Some(Vector {
                id: "doc-1".to_string(),
                attributes: HashMap::from([(
                    "vector".to_string(),
                    AttributeValueMessage::new(AttributeValueProto::VectorValue(
                        VectorValueProto {
                            values: vec![1.0, 2.0, 3.0],
                        },
                    )),
                )]),
            }),
        }]);

        // when
        let encoded = response.encode_to_vec();
        let decoded = SearchResponse::decode(encoded.as_slice()).unwrap();

        // then
        assert_eq!(decoded.status, "success");
        assert_eq!(decoded.results.len(), 1);
        assert_eq!(decoded.results[0].score, 0.95);
        assert_eq!(
            decoded.results[0]
                .vector
                .as_ref()
                .map(|vector| vector.id.as_str()),
            Some("doc-1")
        );
    }

    #[test]
    fn should_encode_and_decode_get_vector_response() {
        // given
        let response = GetVectorResponse::success(Vector {
            id: "doc-1".to_string(),
            attributes: HashMap::from([(
                "vector".to_string(),
                AttributeValueMessage::new(AttributeValueProto::VectorValue(VectorValueProto {
                    values: vec![1.0, 2.0, 3.0],
                })),
            )]),
        });

        // when
        let encoded = response.encode_to_vec();
        let decoded = GetVectorResponse::decode(encoded.as_slice()).unwrap();

        // then
        assert_eq!(decoded.status, "success");
        assert!(decoded.vector.is_some());
        assert_eq!(decoded.vector.as_ref().unwrap().id, "doc-1");
        assert_eq!(decoded.vector.as_ref().unwrap().attributes.len(), 1);
    }

    #[test]
    fn should_encode_and_decode_error_response() {
        // given
        let response = ErrorResponse {
            status: "error".to_string(),
            message: "Something went wrong".to_string(),
        };

        // when
        let encoded = response.encode_to_vec();
        let decoded = ErrorResponse::decode(encoded.as_slice()).unwrap();

        // then
        assert_eq!(decoded.status, "error");
        assert_eq!(decoded.message, "Something went wrong");
    }

    #[test]
    fn should_serialize_upsert_response_with_camel_case() {
        // given
        let response = UpsertVectorsResponse::success(3);

        // when
        let json = serde_json::to_string(&response).unwrap();

        // then
        assert!(json.contains(r#""vectorsUpserted":3"#));
    }

    #[test]
    fn should_serialize_search_result_with_attributes() {
        // given
        let response = SearchResponse::success(vec![SearchResultProto {
            score: 0.5,
            vector: Some(Vector {
                id: "doc-1".to_string(),
                attributes: HashMap::from([(
                    "category".to_string(),
                    AttributeValueMessage::new(AttributeValueProto::StringValue(
                        "electronics".to_string(),
                    )),
                )]),
            }),
        }]);

        // when
        let json: serde_json::Value = serde_json::to_value(&response).unwrap();

        // then
        assert_eq!(json["results"][0]["score"], 0.5);
        assert_eq!(json["results"][0]["vector"]["id"], "doc-1");
        assert_eq!(
            json["results"][0]["vector"]["attributes"]["category"],
            "electronics"
        );
    }

    #[test]
    fn should_serialize_vector_attributes_as_flat_object() {
        // given
        let vector = Vector {
            id: "doc-1".to_string(),
            attributes: HashMap::from([
                (
                    "vector".to_string(),
                    AttributeValueMessage::new(AttributeValueProto::VectorValue(
                        VectorValueProto {
                            values: vec![1.0, 2.0, 3.0],
                        },
                    )),
                ),
                (
                    "category".to_string(),
                    AttributeValueMessage::new(AttributeValueProto::StringValue(
                        "electronics".to_string(),
                    )),
                ),
            ]),
        };

        // when
        let json: serde_json::Value = serde_json::to_value(&vector).unwrap();

        // then
        assert_eq!(json["id"], "doc-1");
        assert_eq!(
            json["attributes"]["vector"],
            serde_json::json!([1.0, 2.0, 3.0])
        );
        assert_eq!(json["attributes"]["category"], "electronics");
    }
}
