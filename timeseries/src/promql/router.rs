use async_trait::async_trait;

use super::request::{
    FederateRequest, LabelValuesRequest, LabelsRequest, MetadataRequest, QueryRangeRequest,
    QueryRequest, SeriesRequest,
};
use super::response::{
    FederateResponse, LabelValuesResponse, LabelsResponse, MetadataResponse, QueryRangeResponse,
    QueryResponse, SeriesResponse,
};

/// Trait for routing PromQL API requests to their corresponding handlers
#[async_trait]
pub trait PromqlRouter {
    /// Handle an instant query request (/api/v1/query)
    async fn query(&self, request: QueryRequest) -> QueryResponse;

    /// Handle a range query request (/api/v1/query_range)
    async fn query_range(&self, request: QueryRangeRequest) -> QueryRangeResponse;

    /// Handle a series listing request (/api/v1/series)
    async fn series(&self, request: SeriesRequest) -> SeriesResponse;

    /// Handle a label names request (/api/v1/labels)
    async fn labels(&self, request: LabelsRequest) -> LabelsResponse;

    /// Handle a label values request (/api/v1/label/{name}/values)
    async fn label_values(&self, request: LabelValuesRequest) -> LabelValuesResponse;

    /// Handle a metadata request (/api/v1/metadata)
    async fn metadata(&self, request: MetadataRequest) -> MetadataResponse;

    /// Handle a federation request (/federate)
    async fn federate(&self, request: FederateRequest) -> FederateResponse;
}
