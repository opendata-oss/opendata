//! HTTP route handlers for the vector server.
//!
//! Per RFC 0004, handlers support both binary protobuf (`application/protobuf`)
//! and ProtoJSON (`application/protobuf+json`) formats.

use std::collections::HashMap;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};

use super::error::ApiError;
use super::proto::{
    AttributeValueMessage, AttributeValueProto, GetVectorResponse, SearchResponse,
    SearchResultProto, UpsertVectorsResponse, Vector, VectorValueProto,
};
use super::request::{SearchRequest, WriteRequest};
use super::response::{ApiResponse, ResponseFormat, to_api_response};
use crate::{FieldType, VectorDb, VectorDbRead};

/// Shared application state, generic over the database type.
pub(crate) struct AppState<T> {
    pub(crate) db: Arc<T>,
    pub(crate) metadata_fields: HashMap<String, FieldType>,
}

impl<T> Clone for AppState<T> {
    fn clone(&self) -> Self {
        Self {
            db: self.db.clone(),
            metadata_fields: self.metadata_fields.clone(),
        }
    }
}

/// Handle POST /api/v1/vector/write
pub(crate) async fn handle_write(
    State(state): State<AppState<VectorDb>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<ApiResponse, ApiError> {
    let format = ResponseFormat::from_headers(&headers);

    let request = WriteRequest::from_body(&headers, &body, &state.metadata_fields)?;
    let count = request.upsert_vectors.len();

    state.db.write(request.upsert_vectors).await?;

    let response = UpsertVectorsResponse::success(count as i32);
    Ok(to_api_response(response, format))
}

/// Handle POST /api/v1/vector/search
pub(crate) async fn handle_search<T: VectorDbRead + Send + Sync + 'static>(
    State(state): State<AppState<T>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<ApiResponse, ApiError> {
    let format = ResponseFormat::from_headers(&headers);

    let request = SearchRequest::from_body(&headers, &body, &state.metadata_fields)?;
    let results = state
        .db
        .search_with_options(&request.query, request.options)
        .await?;

    let result_protos: Vec<SearchResultProto> = results
        .into_iter()
        .map(|r| SearchResultProto {
            score: r.score,
            vector: Some(Vector {
                id: r.vector.id,
                attributes: attributes_to_proto(&r.vector.attributes),
            }),
        })
        .collect();

    let response = SearchResponse::success(result_protos);
    Ok(to_api_response(response, format))
}

/// Handle GET /api/v1/vector/vectors/:id
pub(crate) async fn handle_get_vector<T: VectorDbRead + Send + Sync + 'static>(
    State(state): State<AppState<T>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<ApiResponse, (StatusCode, ApiResponse)> {
    let format = ResponseFormat::from_headers(&headers);

    let record = state.db.get(&id).await.map_err(|e| {
        let api_err = ApiError::from(e);
        let status = match &api_err.0 {
            crate::Error::InvalidInput(_) => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let body = serde_json::json!({
            "status": "error",
            "message": api_err.0.to_string()
        });
        (status, ApiResponse::Json(axum::Json(body)))
    })?;

    match record {
        Some(vector) => {
            let result = Vector {
                id: vector.id,
                attributes: attributes_to_proto(&vector.attributes),
            };

            let response = GetVectorResponse::success(result);
            Ok(to_api_response(response, format))
        }
        None => {
            let body = serde_json::json!({
                "status": "error",
                "message": format!("Document '{}' not found", id)
            });
            Err((StatusCode::NOT_FOUND, ApiResponse::Json(axum::Json(body))))
        }
    }
}

/// Handle GET /-/healthy
pub(crate) async fn handle_healthy() -> (StatusCode, &'static str) {
    (StatusCode::OK, "OK")
}

/// Handle GET /-/ready
pub(crate) async fn handle_ready() -> (StatusCode, &'static str) {
    (StatusCode::OK, "OK")
}

fn attributes_to_proto(attrs: &[crate::Attribute]) -> HashMap<String, AttributeValueMessage> {
    attrs
        .iter()
        .map(|a| (a.name.clone(), attribute_value_to_proto(&a.value)))
        .collect()
}

fn attribute_value_to_proto(value: &crate::AttributeValue) -> AttributeValueMessage {
    let proto_value = match value {
        crate::AttributeValue::String(s) => AttributeValueProto::StringValue(s.clone()),
        crate::AttributeValue::Int64(v) => AttributeValueProto::Int64Value(*v),
        crate::AttributeValue::Float64(v) => AttributeValueProto::Float64Value(*v),
        crate::AttributeValue::Bool(v) => AttributeValueProto::BoolValue(*v),
        crate::AttributeValue::Vector(values) => {
            AttributeValueProto::VectorValue(VectorValueProto {
                values: values.clone(),
            })
        }
    };
    AttributeValueMessage::new(proto_value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Attribute, AttributeValue};

    #[tokio::test]
    async fn should_return_ok_for_healthy() {
        // given/when
        let (status, body) = handle_healthy().await;

        // then
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "OK");
    }

    #[tokio::test]
    async fn should_return_ok_for_ready() {
        // given/when
        let (status, body) = handle_ready().await;

        // then
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "OK");
    }

    #[test]
    fn should_convert_vector_attribute_to_proto() {
        // given
        let attrs = vec![Attribute::new(
            crate::model::VECTOR_FIELD_NAME,
            AttributeValue::Vector(vec![1.0, 2.0, 3.0]),
        )];

        // when
        let proto_attrs = attributes_to_proto(&attrs);

        // then
        assert_eq!(proto_attrs.len(), 1);
        assert!(proto_attrs.contains_key(crate::model::VECTOR_FIELD_NAME));
        assert!(matches!(
            proto_attrs
                .get(crate::model::VECTOR_FIELD_NAME)
                .and_then(|value| value.value.as_ref()),
            Some(AttributeValueProto::VectorValue(VectorValueProto { values }))
                if values == &vec![1.0, 2.0, 3.0]
        ));
    }

    #[test]
    fn should_convert_metadata_attribute_to_proto() {
        // given
        let attrs = vec![Attribute::new(
            "category",
            AttributeValue::String("books".to_string()),
        )];

        // when
        let proto_attrs = attributes_to_proto(&attrs);

        // then
        assert_eq!(proto_attrs.len(), 1);
        assert!(proto_attrs.contains_key("category"));
        assert!(matches!(
            proto_attrs.get("category").and_then(|value| value.value.as_ref()),
            Some(AttributeValueProto::StringValue(value)) if value == "books"
        ));
    }
}
