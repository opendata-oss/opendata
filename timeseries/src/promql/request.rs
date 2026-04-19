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
    /// Set `explain=true` (or `1`, or present-but-empty) to return a
    /// dry-run plan instead of executing the query. Thanos-compatible.
    pub explain: Option<String>,
    /// With `explain=true`, set `pretty=true` to receive a text/plain
    /// indented tree instead of the structured JSON response.
    pub pretty: Option<String>,
    /// Set `trace=true` to collect per-phase / per-operator timings and
    /// return them under a `trace` field in the response. Combines with
    /// the server's `tracing.enabled` config (logical OR).
    pub trace: Option<String>,
}

/// Query parameters for /api/v1/query_range
#[derive(Debug, Deserialize)]
pub struct QueryRangeParams {
    pub query: String,
    pub start: String,
    pub end: String,
    pub step: String,
    pub timeout: Option<String>,
    /// See [`QueryParams::explain`].
    pub explain: Option<String>,
    /// See [`QueryParams::pretty`].
    pub pretty: Option<String>,
    /// See [`QueryParams::trace`].
    pub trace: Option<String>,
}

/// `true` when a query-string flag like `explain` or `pretty` should be
/// read as "on". Accepts `"true"`, `"1"`, or present-but-empty (bare
/// `?explain` — matches Thanos behaviour).
pub fn is_flag_set(v: Option<&str>) -> bool {
    match v {
        None => false,
        Some(s) => {
            let t = s.trim();
            t.is_empty() || t.eq_ignore_ascii_case("true") || t == "1"
        }
    }
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
