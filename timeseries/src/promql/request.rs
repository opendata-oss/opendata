use serde::Deserialize;

// =============================================================================
// HTTP Query Parameter Types (serde deserializable)
// =============================================================================

/// Query parameters for /api/v1/query
#[derive(Debug, Deserialize)]
pub struct QueryParams {
    pub query: String,
    pub time: Option<String>,
    pub timeout: Option<String>,
}

/// Query parameters for /api/v1/query_range
#[derive(Debug, Deserialize)]
pub struct QueryRangeParams {
    pub query: String,
    pub start: String,
    pub end: String,
    pub step: String,
    pub timeout: Option<String>,
}

/// Query parameters for /api/v1/series
#[derive(Debug, Deserialize)]
pub struct SeriesParams {
    #[serde(rename = "match[]", default)]
    pub matches: Vec<String>,
    pub start: Option<String>,
    pub end: Option<String>,
    pub limit: Option<usize>,
}

/// Query parameters for /api/v1/labels
#[derive(Debug, Deserialize)]
pub struct LabelsParams {
    #[serde(rename = "match[]", default)]
    pub matches: Vec<String>,
    pub start: Option<String>,
    pub end: Option<String>,
    pub limit: Option<usize>,
}

/// Query parameters for /api/v1/label/{name}/values
#[derive(Debug, Deserialize)]
pub struct LabelValuesParams {
    #[serde(rename = "match[]", default)]
    pub matches: Vec<String>,
    pub start: Option<String>,
    pub end: Option<String>,
    pub limit: Option<usize>,
}

/// Query parameters for /api/v1/metadata
#[derive(Debug, Deserialize)]
pub struct MetadataParams {
    pub metric: Option<String>,
    pub limit: Option<usize>,
    pub limit_per_metric: Option<usize>,
}

/// Query parameters for /federate
#[derive(Debug, Deserialize)]
pub struct FederateParams {
    #[serde(rename = "match[]", default)]
    pub matches: Vec<String>,
}
