use std::time::{Duration, SystemTime};

use serde::Deserialize;

use crate::util::{OpenTsdbError, parse_duration, parse_timestamp, parse_timestamp_to_seconds};

// =============================================================================
// Domain Request Types (parsed, validated)
// =============================================================================

/// Request for instant query (/api/v1/query)
#[derive(Debug, Clone)]
pub struct QueryRequest {
    pub query: String,
    pub time: Option<SystemTime>,
    pub timeout: Option<Duration>,
}

/// Request for range query (/api/v1/query_range)
#[derive(Debug, Clone)]
pub struct QueryRangeRequest {
    pub query: String,
    pub start: SystemTime,
    pub end: SystemTime,
    pub step: Duration,
    pub timeout: Option<Duration>,
}

/// Request for series listing (/api/v1/series)
#[derive(Debug, Clone)]
pub struct SeriesRequest {
    pub matches: Vec<String>,
    pub start: Option<i64>,
    pub end: Option<i64>,
    pub limit: Option<usize>,
}

/// Request for label names (/api/v1/labels)
#[derive(Debug, Clone)]
pub struct LabelsRequest {
    pub matches: Option<Vec<String>>,
    pub start: Option<i64>,
    pub end: Option<i64>,
    pub limit: Option<usize>,
}

/// Request for label values (/api/v1/label/{name}/values)
#[derive(Debug, Clone)]
pub struct LabelValuesRequest {
    pub label_name: String,
    pub matches: Option<Vec<String>>,
    pub start: Option<i64>,
    pub end: Option<i64>,
    pub limit: Option<usize>,
}

/// Request for metric metadata (/api/v1/metadata)
#[derive(Debug, Clone)]
pub struct MetadataRequest {
    pub metric: Option<String>,
    pub limit: Option<usize>,
}

/// Request for federation (/federate)
#[derive(Debug, Clone)]
pub struct FederateRequest {
    pub matches: Vec<String>,
}

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

impl TryFrom<QueryParams> for QueryRequest {
    type Error = OpenTsdbError;

    fn try_from(params: QueryParams) -> Result<Self, Self::Error> {
        Ok(QueryRequest {
            query: params.query,
            time: params.time.map(|s| parse_timestamp(&s)).transpose()?,
            timeout: params.timeout.map(|s| parse_duration(&s)).transpose()?,
        })
    }
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

impl TryFrom<QueryRangeParams> for QueryRangeRequest {
    type Error = OpenTsdbError;

    fn try_from(params: QueryRangeParams) -> Result<Self, Self::Error> {
        Ok(QueryRangeRequest {
            query: params.query,
            start: parse_timestamp(&params.start)?,
            end: parse_timestamp(&params.end)?,
            step: parse_duration(&params.step)?,
            timeout: params.timeout.map(|s| parse_duration(&s)).transpose()?,
        })
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

impl TryFrom<SeriesParams> for SeriesRequest {
    type Error = OpenTsdbError;

    fn try_from(params: SeriesParams) -> Result<Self, Self::Error> {
        Ok(SeriesRequest {
            matches: params.matches,
            start: params
                .start
                .map(|s| parse_timestamp_to_seconds(&s))
                .transpose()?,
            end: params
                .end
                .map(|s| parse_timestamp_to_seconds(&s))
                .transpose()?,
            limit: params.limit,
        })
    }
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

impl TryFrom<LabelsParams> for LabelsRequest {
    type Error = OpenTsdbError;

    fn try_from(params: LabelsParams) -> Result<Self, Self::Error> {
        Ok(LabelsRequest {
            matches: if params.matches.is_empty() {
                None
            } else {
                Some(params.matches)
            },
            start: params
                .start
                .map(|s| parse_timestamp_to_seconds(&s))
                .transpose()?,
            end: params
                .end
                .map(|s| parse_timestamp_to_seconds(&s))
                .transpose()?,
            limit: params.limit,
        })
    }
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

impl LabelValuesParams {
    /// Convert to LabelValuesRequest with the label name from the path
    pub fn into_request(self, label_name: String) -> Result<LabelValuesRequest, OpenTsdbError> {
        Ok(LabelValuesRequest {
            label_name,
            matches: if self.matches.is_empty() {
                None
            } else {
                Some(self.matches)
            },
            start: self
                .start
                .map(|s| parse_timestamp_to_seconds(&s))
                .transpose()?,
            end: self
                .end
                .map(|s| parse_timestamp_to_seconds(&s))
                .transpose()?,
            limit: self.limit,
        })
    }
}

/// Query parameters for /api/v1/metadata
#[derive(Debug, Deserialize)]
pub struct MetadataParams {
    pub metric: Option<String>,
    pub limit: Option<usize>,
}

impl From<MetadataParams> for MetadataRequest {
    fn from(params: MetadataParams) -> Self {
        MetadataRequest {
            metric: params.metric,
            limit: params.limit,
        }
    }
}

/// Query parameters for /federate
#[derive(Debug, Deserialize)]
pub struct FederateParams {
    #[serde(rename = "match[]", default)]
    pub matches: Vec<String>,
}

impl From<FederateParams> for FederateRequest {
    fn from(params: FederateParams) -> Self {
        FederateRequest {
            matches: params.matches,
        }
    }
}
