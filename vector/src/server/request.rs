//! HTTP request types for the vector server.

use std::collections::HashMap;

use axum::http::{HeaderMap, header};
use prost::Message;
use serde::Deserialize;
use serde_json::Value;

use super::proto;
use super::response::is_binary_protobuf;
use crate::Error;
use crate::model::VECTOR_FIELD_NAME;
use crate::{Attribute, AttributeValue, FieldType, Filter, Query};

/// Check if the request body is protobuf based on Content-Type header.
fn is_protobuf_content(headers: &HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(is_binary_protobuf)
        .unwrap_or(false)
}

/// Unified upsert vectors request that can be parsed from either JSON or protobuf.
#[derive(Debug)]
pub struct WriteRequest {
    pub upsert_vectors: Vec<crate::Vector>,
}

impl WriteRequest {
    /// Parse an upsert request from the raw body based on Content-Type header.
    pub fn from_body(
        headers: &HeaderMap,
        body: &[u8],
        metadata_fields: &HashMap<String, FieldType>,
    ) -> Result<Self, Error> {
        if is_protobuf_content(headers) {
            Self::from_protobuf(body)
        } else {
            Self::from_json(body, metadata_fields)
        }
    }

    fn from_protobuf(body: &[u8]) -> Result<Self, Error> {
        let proto_request = proto::WriteRequest::decode(body)
            .map_err(|e| Error::InvalidInput(format!("Invalid protobuf: {}", e)))?;
        Self::from_proto_request(proto_request)
    }

    fn from_json(body: &[u8], metadata_fields: &HashMap<String, FieldType>) -> Result<Self, Error> {
        let json_request: JsonWriteRequest = serde_json::from_slice(body)
            .map_err(|e| Error::InvalidInput(format!("Invalid JSON: {}", e)))?;
        Self::from_json_request(json_request, metadata_fields)
    }

    fn from_proto_request(proto_request: proto::WriteRequest) -> Result<Self, Error> {
        let mut upsert_vectors = Vec::with_capacity(proto_request.upsert_vectors.len());
        for (i, doc) in proto_request.upsert_vectors.into_iter().enumerate() {
            if doc.id.is_empty() {
                return Err(Error::InvalidInput(format!(
                    "upsert_vectors[{}]: id is required",
                    i
                )));
            }

            upsert_vectors.push(proto_vector_to_vector(i, doc)?);
        }
        Ok(Self { upsert_vectors })
    }

    fn from_json_request(
        json_request: JsonWriteRequest,
        metadata_fields: &HashMap<String, FieldType>,
    ) -> Result<Self, Error> {
        let mut upsert_vectors = Vec::with_capacity(json_request.upsert_vectors.len());

        for (i, vector) in json_request.upsert_vectors.into_iter().enumerate() {
            if vector.id.is_empty() {
                return Err(Error::InvalidInput(format!(
                    "upsert_vectors[{}]: id is required",
                    i
                )));
            }

            let mut attributes = Vec::with_capacity(vector.attributes.len());
            let mut has_vector = false;

            for (name, value) in vector.attributes {
                let attribute_value = json_attribute_to_value(i, &name, value, metadata_fields)?;
                if name == VECTOR_FIELD_NAME {
                    has_vector = true;
                }
                attributes.push(Attribute {
                    name,
                    value: attribute_value,
                });
            }

            if !has_vector {
                return Err(Error::InvalidInput(format!(
                    "upsert_vectors[{}]: vector is required",
                    i
                )));
            }

            upsert_vectors.push(crate::Vector {
                id: vector.id,
                attributes,
            });
        }

        Ok(Self { upsert_vectors })
    }
}

/// Unified search request that can be parsed from either JSON or protobuf.
#[derive(Debug)]
pub struct SearchRequest {
    pub query: Query,
    pub nprobe: Option<usize>,
}

impl SearchRequest {
    /// Parse a search request from the raw body based on Content-Type header.
    pub fn from_body(
        headers: &HeaderMap,
        body: &[u8],
        metadata_fields: &HashMap<String, FieldType>,
    ) -> Result<Self, Error> {
        if is_protobuf_content(headers) {
            Self::from_protobuf(body)
        } else {
            Self::from_json(body, metadata_fields)
        }
    }

    fn from_protobuf(body: &[u8]) -> Result<Self, Error> {
        let proto_request = proto::SearchRequest::decode(body)
            .map_err(|e| Error::InvalidInput(format!("Invalid protobuf: {}", e)))?;
        Self::from_proto_request(proto_request)
    }

    fn from_json(body: &[u8], metadata_fields: &HashMap<String, FieldType>) -> Result<Self, Error> {
        let json_request: JsonSearchRequest = serde_json::from_slice(body)
            .map_err(|e| Error::InvalidInput(format!("Invalid JSON: {}", e)))?;
        Self::from_json_request(json_request, metadata_fields)
    }

    fn from_proto_request(proto_request: proto::SearchRequest) -> Result<Self, Error> {
        if proto_request.vector.is_empty() {
            return Err(Error::InvalidInput("vector is required".to_string()));
        }
        if proto_request.k == 0 {
            return Err(Error::InvalidInput("k must be greater than 0".to_string()));
        }
        let mut query = Query::new(proto_request.vector).with_limit(proto_request.k as usize);
        if let Some(filter) = proto_request.filter {
            query = query.with_filter(proto_filter_to_filter(filter)?);
        }
        if !proto_request.include_fields.is_empty() {
            query = query.with_fields(proto_request.include_fields);
        }
        Ok(Self {
            query,
            nprobe: proto_request.nprobe.map(|n| n as usize),
        })
    }

    fn from_json_request(
        json_request: JsonSearchRequest,
        metadata_fields: &HashMap<String, FieldType>,
    ) -> Result<Self, Error> {
        if json_request.vector.is_empty() {
            return Err(Error::InvalidInput("vector is required".to_string()));
        }
        if json_request.k == 0 {
            return Err(Error::InvalidInput("k must be greater than 0".to_string()));
        }

        let mut query = Query::new(json_request.vector).with_limit(json_request.k as usize);
        if let Some(filter) = json_request.filter {
            query = query.with_filter(json_filter_to_filter(filter, metadata_fields)?);
        }
        if let Some(fields) = json_request.include_fields {
            query = query.with_fields(fields);
        }

        Ok(Self {
            query,
            nprobe: json_request.nprobe.map(|n| n as usize),
        })
    }
}

fn proto_attribute_to_value(value: proto::AttributeValueProto) -> crate::AttributeValue {
    match value {
        proto::AttributeValueProto::StringValue(s) => crate::AttributeValue::String(s),
        proto::AttributeValueProto::Int64Value(v) => crate::AttributeValue::Int64(v),
        proto::AttributeValueProto::Float64Value(v) => crate::AttributeValue::Float64(v),
        proto::AttributeValueProto::BoolValue(v) => crate::AttributeValue::Bool(v),
        proto::AttributeValueProto::VectorValue(v) => crate::AttributeValue::Vector(v.values),
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonWriteRequest {
    upsert_vectors: Vec<JsonVector>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonSearchRequest {
    vector: Vec<f32>,
    k: u32,
    #[serde(default)]
    nprobe: Option<u32>,
    #[serde(default)]
    filter: Option<JsonFilter>,
    #[serde(default)]
    include_fields: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct JsonVector {
    id: String,
    attributes: HashMap<String, Value>,
}

#[derive(Debug, Deserialize)]
struct JsonFilter {
    #[serde(default)]
    eq: Option<JsonComparisonFilter>,
    #[serde(default)]
    neq: Option<JsonComparisonFilter>,
    #[serde(default)]
    r#in: Option<JsonInFilter>,
    #[serde(default)]
    and: Option<Vec<JsonFilter>>,
    #[serde(default)]
    or: Option<Vec<JsonFilter>>,
}

#[derive(Debug, Deserialize)]
struct JsonComparisonFilter {
    field: String,
    value: Value,
}

#[derive(Debug, Deserialize)]
struct JsonInFilter {
    field: String,
    values: Vec<Value>,
}

fn proto_vector_to_vector(index: usize, doc: proto::Vector) -> Result<crate::Vector, Error> {
    let mut attributes = Vec::with_capacity(doc.attributes.len());
    let mut has_vector = false;

    for (name, attr) in doc.attributes {
        let value = attr.value.ok_or_else(|| {
            Error::InvalidInput(format!(
                "upsert_vectors[{}]: attribute '{}' has no value",
                index, name
            ))
        })?;

        let value = proto_attribute_to_value(value);
        validate_vector_attribute(index, &name, &value)?;
        if name == VECTOR_FIELD_NAME {
            has_vector = true;
        }

        attributes.push(Attribute { name, value });
    }

    if !has_vector {
        return Err(Error::InvalidInput(format!(
            "upsert_vectors[{}]: vector is required",
            index
        )));
    }

    Ok(crate::Vector {
        id: doc.id,
        attributes,
    })
}

fn validate_vector_attribute(
    index: usize,
    name: &str,
    value: &AttributeValue,
) -> Result<(), Error> {
    if name != VECTOR_FIELD_NAME {
        return Ok(());
    }

    match value {
        AttributeValue::Vector(v) if !v.is_empty() => Ok(()),
        AttributeValue::Vector(_) => Err(Error::InvalidInput(format!(
            "upsert_vectors[{}]: vector is required",
            index
        ))),
        _ => Err(Error::InvalidInput(format!(
            "upsert_vectors[{}]: '{}' must be an array of numbers",
            index, VECTOR_FIELD_NAME
        ))),
    }
}

fn proto_filter_to_filter(filter: proto::FilterMessage) -> Result<Filter, Error> {
    match filter.kind {
        Some(proto::FilterKind::Eq(filter)) => Ok(Filter::eq(
            filter.field,
            proto_filter_value_to_attribute(filter.value)?,
        )),
        Some(proto::FilterKind::Neq(filter)) => Ok(Filter::neq(
            filter.field,
            proto_filter_value_to_attribute(filter.value)?,
        )),
        Some(proto::FilterKind::In(filter)) => {
            let mut values = Vec::with_capacity(filter.values.len());
            for value in filter.values {
                values.push(proto_filter_value_to_attribute(Some(value))?);
            }
            Ok(Filter::in_set(filter.field, values))
        }
        Some(proto::FilterKind::And(filter)) => {
            let mut filters = Vec::with_capacity(filter.filters.len());
            for inner in filter.filters {
                filters.push(proto_filter_to_filter(inner)?);
            }
            Ok(Filter::and(filters))
        }
        Some(proto::FilterKind::Or(filter)) => {
            let mut filters = Vec::with_capacity(filter.filters.len());
            for inner in filter.filters {
                filters.push(proto_filter_to_filter(inner)?);
            }
            Ok(Filter::or(filters))
        }
        None => Err(Error::InvalidInput("filter is required".to_string())),
    }
}

fn proto_filter_value_to_attribute(
    value: Option<proto::AttributeValueMessage>,
) -> Result<AttributeValue, Error> {
    let value = value
        .and_then(|message| message.value)
        .ok_or_else(|| Error::InvalidInput("filter value is required".to_string()))?;

    match value {
        proto::AttributeValueProto::StringValue(value) => Ok(AttributeValue::String(value)),
        proto::AttributeValueProto::Int64Value(value) => Ok(AttributeValue::Int64(value)),
        proto::AttributeValueProto::Float64Value(value) => Ok(AttributeValue::Float64(value)),
        proto::AttributeValueProto::BoolValue(value) => Ok(AttributeValue::Bool(value)),
        proto::AttributeValueProto::VectorValue(_) => Err(Error::InvalidInput(
            "filter values must be scalar".to_string(),
        )),
    }
}

fn json_filter_to_filter(
    filter: JsonFilter,
    metadata_fields: &HashMap<String, FieldType>,
) -> Result<Filter, Error> {
    let mut variants = 0;
    variants += usize::from(filter.eq.is_some());
    variants += usize::from(filter.neq.is_some());
    variants += usize::from(filter.r#in.is_some());
    variants += usize::from(filter.and.is_some());
    variants += usize::from(filter.or.is_some());

    if variants != 1 {
        return Err(Error::InvalidInput(
            "filter must contain exactly one operator".to_string(),
        ));
    }

    if let Some(filter) = filter.eq {
        return Ok(Filter::eq(
            filter.field.clone(),
            json_filter_value_to_attribute(&filter.field, filter.value, metadata_fields)?,
        ));
    }

    if let Some(filter) = filter.neq {
        return Ok(Filter::neq(
            filter.field.clone(),
            json_filter_value_to_attribute(&filter.field, filter.value, metadata_fields)?,
        ));
    }

    if let Some(filter) = filter.r#in {
        let mut values = Vec::with_capacity(filter.values.len());
        for value in filter.values {
            values.push(json_filter_value_to_attribute(
                &filter.field,
                value,
                metadata_fields,
            )?);
        }
        return Ok(Filter::in_set(filter.field, values));
    }

    if let Some(filters) = filter.and {
        let mut parsed = Vec::with_capacity(filters.len());
        for filter in filters {
            parsed.push(json_filter_to_filter(filter, metadata_fields)?);
        }
        return Ok(Filter::and(parsed));
    }

    if let Some(filters) = filter.or {
        let mut parsed = Vec::with_capacity(filters.len());
        for filter in filters {
            parsed.push(json_filter_to_filter(filter, metadata_fields)?);
        }
        return Ok(Filter::or(parsed));
    }

    Err(Error::InvalidInput("filter is required".to_string()))
}

fn json_attribute_to_value(
    index: usize,
    name: &str,
    value: Value,
    schema: &HashMap<String, FieldType>,
) -> Result<AttributeValue, Error> {
    if name == VECTOR_FIELD_NAME {
        return parse_vector_value(index, value);
    }

    match schema.get(name).copied() {
        Some(FieldType::String) => parse_string_value(index, name, value),
        Some(FieldType::Int64) => parse_int64_value(index, name, value),
        Some(FieldType::Float64) => parse_float64_value(index, name, value),
        Some(FieldType::Bool) => parse_bool_value(index, name, value),
        Some(FieldType::Vector) => parse_vector_value(index, value),
        None => infer_json_value(index, name, value),
    }
}

fn json_filter_value_to_attribute(
    field: &str,
    value: Value,
    schema: &HashMap<String, FieldType>,
) -> Result<AttributeValue, Error> {
    if field == VECTOR_FIELD_NAME {
        return Err(Error::InvalidInput(
            "filter field 'vector' is not supported".to_string(),
        ));
    }

    match schema.get(field).copied() {
        Some(FieldType::String) => value
            .as_str()
            .map(|value| AttributeValue::String(value.to_string()))
            .ok_or_else(|| Error::InvalidInput(format!("filter field '{}' must be string", field))),
        Some(FieldType::Int64) => value
            .as_i64()
            .map(AttributeValue::Int64)
            .ok_or_else(|| Error::InvalidInput(format!("filter field '{}' must be int64", field))),
        Some(FieldType::Float64) => value.as_f64().map(AttributeValue::Float64).ok_or_else(|| {
            Error::InvalidInput(format!("filter field '{}' must be float64", field))
        }),
        Some(FieldType::Bool) => value
            .as_bool()
            .map(AttributeValue::Bool)
            .ok_or_else(|| Error::InvalidInput(format!("filter field '{}' must be bool", field))),
        Some(FieldType::Vector) => Err(Error::InvalidInput(format!(
            "filter field '{}' must be scalar",
            field
        ))),
        None => infer_json_value_for_filter(field, value),
    }
}

fn infer_json_value_for_filter(field: &str, value: Value) -> Result<AttributeValue, Error> {
    match value {
        Value::String(value) => Ok(AttributeValue::String(value)),
        Value::Bool(value) => Ok(AttributeValue::Bool(value)),
        Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(AttributeValue::Int64(value))
            } else if let Some(value) = value.as_f64() {
                Ok(AttributeValue::Float64(value))
            } else {
                Err(Error::InvalidInput(format!(
                    "filter field '{}' must be a scalar value",
                    field
                )))
            }
        }
        _ => Err(Error::InvalidInput(format!(
            "filter field '{}' must be a scalar value",
            field
        ))),
    }
}

fn parse_vector_value(index: usize, value: Value) -> Result<AttributeValue, Error> {
    let items = value.as_array().ok_or_else(|| {
        Error::InvalidInput(format!(
            "upsert_vectors[{}]: '{}' must be an array of numbers",
            index, VECTOR_FIELD_NAME
        ))
    })?;

    let mut vector = Vec::with_capacity(items.len());
    for item in items {
        let number = item.as_f64().ok_or_else(|| {
            Error::InvalidInput(format!(
                "upsert_vectors[{}]: '{}' must be an array of numbers",
                index, VECTOR_FIELD_NAME
            ))
        })?;
        vector.push(number as f32);
    }

    if vector.is_empty() {
        return Err(Error::InvalidInput(format!(
            "upsert_vectors[{}]: vector is required",
            index
        )));
    }

    Ok(AttributeValue::Vector(vector))
}

fn parse_string_value(index: usize, name: &str, value: Value) -> Result<AttributeValue, Error> {
    value
        .as_str()
        .map(|value| AttributeValue::String(value.to_string()))
        .ok_or_else(|| type_error(index, name, "string"))
}

fn parse_int64_value(index: usize, name: &str, value: Value) -> Result<AttributeValue, Error> {
    value
        .as_i64()
        .map(AttributeValue::Int64)
        .ok_or_else(|| type_error(index, name, "int64"))
}

fn parse_float64_value(index: usize, name: &str, value: Value) -> Result<AttributeValue, Error> {
    value
        .as_f64()
        .map(AttributeValue::Float64)
        .ok_or_else(|| type_error(index, name, "float64"))
}

fn parse_bool_value(index: usize, name: &str, value: Value) -> Result<AttributeValue, Error> {
    value
        .as_bool()
        .map(AttributeValue::Bool)
        .ok_or_else(|| type_error(index, name, "bool"))
}

fn infer_json_value(index: usize, name: &str, value: Value) -> Result<AttributeValue, Error> {
    match value {
        Value::String(value) => Ok(AttributeValue::String(value)),
        Value::Bool(value) => Ok(AttributeValue::Bool(value)),
        Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(AttributeValue::Int64(value))
            } else if let Some(value) = value.as_f64() {
                Ok(AttributeValue::Float64(value))
            } else {
                Err(type_error(index, name, "number"))
            }
        }
        _ => Err(type_error(index, name, "scalar value")),
    }
}

fn type_error(index: usize, name: &str, expected: &str) -> Error {
    Error::InvalidInput(format!(
        "upsert_vectors[{}]: attribute '{}' must be {}",
        index, name, expected
    ))
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderValue;

    use super::*;

    fn protobuf_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/protobuf"),
        );
        headers
    }

    fn protojson_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/protobuf+json"),
        );
        headers
    }

    #[test]
    fn should_parse_upsert_request_from_json() {
        // given
        let json = br#"{
            "upsertVectors": [{
                "id": "doc-1",
                "attributes": {
                    "vector": [1.0, 2.0, 3.0],
                    "category": "test"
                }
            }]
        }"#;

        // when
        let request = WriteRequest::from_body(&protojson_headers(), json, &HashMap::new()).unwrap();

        // then
        assert_eq!(request.upsert_vectors.len(), 1);
        assert_eq!(request.upsert_vectors[0].id, "doc-1");
    }

    #[test]
    fn should_parse_upsert_request_from_protobuf() {
        // given
        let proto_request = proto::WriteRequest {
            upsert_vectors: vec![proto::Vector {
                id: "doc-1".to_string(),
                attributes: HashMap::from([(
                    "vector".to_string(),
                    proto::AttributeValueMessage::new(proto::AttributeValueProto::VectorValue(
                        proto::VectorValueProto {
                            values: vec![1.0, 2.0, 3.0],
                        },
                    )),
                )]),
            }],
        };
        let body = proto_request.encode_to_vec();

        // when
        let request = WriteRequest::from_body(&protobuf_headers(), &body, &HashMap::new()).unwrap();

        // then
        assert_eq!(request.upsert_vectors.len(), 1);
        assert_eq!(request.upsert_vectors[0].id, "doc-1");
        assert!(matches!(
            request.upsert_vectors[0].attribute(VECTOR_FIELD_NAME),
            Some(crate::AttributeValue::Vector(v)) if v == &vec![1.0, 2.0, 3.0]
        ));
    }

    #[test]
    fn should_return_error_for_missing_id() {
        // given
        let json = br#"{
            "upsertVectors": [{"id": "", "attributes": {"vector": [1.0, 2.0]}}]
        }"#;

        // when
        let result = WriteRequest::from_body(&protojson_headers(), json, &HashMap::new());

        // then
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("id is required"));
    }

    #[test]
    fn should_return_error_for_missing_vector() {
        // given
        let json = br#"{
            "upsertVectors": [{"id": "doc-1", "attributes": {"vector": []}}]
        }"#;

        // when
        let result = WriteRequest::from_body(&protojson_headers(), json, &HashMap::new());

        // then
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("vector is required")
        );
    }

    #[test]
    fn should_return_error_when_upsert_vector_attribute_is_missing() {
        // given
        let json = br#"{
            "upsertVectors": [{"id": "doc-1", "attributes": {"category": "test"}}]
        }"#;

        // when
        let result = WriteRequest::from_body(&protojson_headers(), json, &HashMap::new());

        // then
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("vector is required")
        );
    }

    #[test]
    fn should_return_error_when_upsert_vector_attribute_has_wrong_type() {
        // given
        let json = br#"{
            "upsertVectors": [{"id": "doc-1", "attributes": {"vector": "bad"}}]
        }"#;

        // when
        let result = WriteRequest::from_body(&protojson_headers(), json, &HashMap::new());

        // then
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("'vector' must be an array of numbers")
        );
    }

    #[test]
    fn should_parse_upsert_request_using_schema_types() {
        // given
        let json = br#"{
            "upsertVectors": [{
                "id": "doc-1",
                "attributes": {
                    "vector": [1.0, 2.0, 3.0],
                    "department": "hw",
                    "in_stock": true,
                    "price": 42.5,
                    "count": 7
                }
            }]
        }"#;
        let schema = HashMap::from([
            ("department".to_string(), FieldType::String),
            ("in_stock".to_string(), FieldType::Bool),
            ("price".to_string(), FieldType::Float64),
            ("count".to_string(), FieldType::Int64),
        ]);

        // when
        let request = WriteRequest::from_body(&protojson_headers(), json, &schema).unwrap();

        // then
        assert!(matches!(
            request.upsert_vectors[0].attribute("department"),
            Some(AttributeValue::String(value)) if value == "hw"
        ));
        assert!(matches!(
            request.upsert_vectors[0].attribute("in_stock"),
            Some(AttributeValue::Bool(true))
        ));
        assert!(matches!(
            request.upsert_vectors[0].attribute("price"),
            Some(AttributeValue::Float64(value)) if *value == 42.5
        ));
        assert!(matches!(
            request.upsert_vectors[0].attribute("count"),
            Some(AttributeValue::Int64(7))
        ));
    }

    #[test]
    fn should_parse_search_request_from_json() {
        // given
        let json = br#"{
            "vector": [1.0, 2.0, 3.0],
            "k": 10,
            "nprobe": 20,
            "filter": {
                "eq": {
                    "field": "category",
                    "value": "electronics"
                }
            }
        }"#;

        // when
        let request = SearchRequest::from_body(
            &protojson_headers(),
            json,
            &HashMap::from([("category".to_string(), FieldType::String)]),
        )
        .unwrap();

        // then
        assert_eq!(request.query.vector, vec![1.0, 2.0, 3.0]);
        assert_eq!(request.query.limit, 10);
        assert_eq!(request.nprobe, Some(20));
        assert_eq!(
            request.query.filter,
            Some(Filter::eq("category", "electronics"))
        );
    }

    #[test]
    fn should_parse_search_request_without_nprobe() {
        // given
        let json = br#"{"vector": [1.0, 2.0], "k": 5}"#;

        // when
        let request =
            SearchRequest::from_body(&protojson_headers(), json, &HashMap::new()).unwrap();

        // then
        assert_eq!(request.query.limit, 5);
        assert_eq!(request.nprobe, None);
    }

    #[test]
    fn should_return_error_for_empty_search_vector() {
        // given
        let json = br#"{"vector": [], "k": 10}"#;

        // when
        let result = SearchRequest::from_body(&protojson_headers(), json, &HashMap::new());

        // then
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("vector is required")
        );
    }

    #[test]
    fn should_return_error_for_zero_k() {
        // given
        let json = br#"{"vector": [1.0], "k": 0}"#;

        // when
        let result = SearchRequest::from_body(&protojson_headers(), json, &HashMap::new());

        // then
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("k must be greater than 0")
        );
    }

    #[test]
    fn should_return_error_for_invalid_json() {
        // given
        let body = b"not valid json";

        // when
        let result = WriteRequest::from_body(&protojson_headers(), body, &HashMap::new());

        // then
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid JSON"));
    }

    #[test]
    fn should_parse_search_request_from_protobuf_with_filter() {
        // given
        let body = proto::SearchRequest {
            vector: vec![1.0, 2.0, 3.0],
            k: 10,
            nprobe: Some(20),
            filter: Some(proto::FilterMessage {
                kind: Some(proto::FilterKind::Eq(proto::ComparisonFilter {
                    field: "category".to_string(),
                    value: Some(proto::AttributeValueMessage::new(
                        proto::AttributeValueProto::StringValue("electronics".to_string()),
                    )),
                })),
            }),
            include_fields: vec![],
        }
        .encode_to_vec();

        // when
        let request =
            SearchRequest::from_body(&protobuf_headers(), &body, &HashMap::new()).unwrap();

        // then
        assert_eq!(request.query.vector, vec![1.0, 2.0, 3.0]);
        assert_eq!(request.query.limit, 10);
        assert_eq!(
            request.query.filter,
            Some(Filter::eq("category", "electronics"))
        );
        assert_eq!(request.nprobe, Some(20));
    }

    #[test]
    fn should_return_error_for_non_scalar_json_filter_value() {
        // given
        let json = br#"{
            "vector": [1.0, 2.0],
            "k": 5,
            "filter": {
                "eq": {
                    "field": "category",
                    "value": ["electronics"]
                }
            }
        }"#;

        // when
        let result = SearchRequest::from_body(&protojson_headers(), json, &HashMap::new());

        // then
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("must be a scalar value")
        );
    }

    #[test]
    fn should_return_error_for_invalid_protobuf() {
        // given
        let body = &[0xFF, 0xFF, 0xFF];

        // when
        let result = WriteRequest::from_body(&protobuf_headers(), body, &HashMap::new());

        // then
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid protobuf"));
    }
}
