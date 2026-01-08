use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use async_trait::async_trait;
use common::clock::SystemClock;
use promql_parser::parser::{EvalStmt, Expr, VectorSelector};

use super::evaluator::Evaluator;
use super::parser::Parseable;
use super::request::{
    FederateRequest, LabelValuesRequest, LabelsRequest, MetadataRequest, QueryRangeRequest,
    QueryRequest, SeriesRequest,
};
use super::response::{
    ErrorResponse, FederateResponse, LabelValuesResponse, LabelsResponse, MatrixSeries,
    MetadataResponse, QueryRangeResponse, QueryRangeResult, QueryResponse, QueryResult,
    SeriesResponse, VectorSeries,
};
use super::router::PromqlRouter;
use super::selector::evaluate_selector_with_reader;
use crate::model::Label;
use crate::model::SeriesId;
use crate::query::QueryReader;
use crate::tsdb::Tsdb;

/// Parse a match[] selector string into a VectorSelector
fn parse_selector(selector: &str) -> Result<VectorSelector, String> {
    let expr = promql_parser::parser::parse(selector).map_err(|e| e.to_string())?;
    match expr {
        Expr::VectorSelector(vs) => Ok(vs),
        _ => Err("Expected a vector selector".to_string()),
    }
}

/// Get all series IDs matching any of the given selectors (UNION)
async fn get_matching_series<R: QueryReader>(
    reader: &R,
    matches: &[String],
) -> Result<HashSet<SeriesId>, String> {
    let mut all_series = HashSet::new();

    for selector_str in matches {
        let selector = parse_selector(selector_str)?;
        let series = evaluate_selector_with_reader(reader, &selector)
            .await
            .map_err(|e| e.to_string())?;
        all_series.extend(series);
    }

    Ok(all_series)
}

/// Convert attributes to a HashMap
fn labels_to_map(labels: &[Label]) -> HashMap<String, String> {
    labels
        .iter()
        .map(|label| (label.name.clone(), label.value.clone()))
        .collect()
}

#[async_trait]
impl PromqlRouter for Tsdb {
    async fn query(&self, request: QueryRequest) -> QueryResponse {
        // Parse the request to EvalStmt
        // Note: QueryRequest has a single `time` field for instant queries
        // The Parseable impl sets stmt.start == stmt.end == time
        let clock = Arc::new(SystemClock {});
        let stmt = match request.parse(clock) {
            Ok(stmt) => stmt,
            Err(e) => {
                let err = ErrorResponse::bad_data(e.to_string());
                return QueryResponse {
                    status: err.status,
                    data: None,
                    error: Some(err.error),
                    error_type: Some(err.error_type),
                };
            }
        };

        // Calculate time range for bucket discovery
        // For instant queries: query_time = stmt.start (which equals stmt.end)
        // We need buckets covering [query_time - lookback, query_time]
        let query_time = stmt.start;
        let query_time_secs = query_time
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let lookback_start_secs = query_time
            .checked_sub(stmt.lookback_delta)
            .unwrap_or(std::time::UNIX_EPOCH)
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        // Get query reader for the time range
        let reader = match self
            .query_reader(lookback_start_secs, query_time_secs)
            .await
        {
            Ok(reader) => reader,
            Err(e) => {
                let err = ErrorResponse::internal(e.to_string());
                return QueryResponse {
                    status: err.status,
                    data: None,
                    error: Some(err.error),
                    error_type: Some(err.error_type),
                };
            }
        };

        // Evaluate the query
        let evaluator = Evaluator::new(&reader);
        let samples = match evaluator.evaluate(stmt).await {
            Ok(samples) => samples,
            Err(e) => {
                let err = ErrorResponse::execution(e.to_string());
                return QueryResponse {
                    status: err.status,
                    data: None,
                    error: Some(err.error),
                    error_type: Some(err.error_type),
                };
            }
        };

        // Convert EvalSamples to VectorSeries format
        let result: Vec<VectorSeries> = samples
            .into_iter()
            .map(|sample| VectorSeries {
                metric: sample.labels,
                value: (
                    sample.timestamp_ms as f64 / 1000.0, // Convert ms to seconds
                    sample.value.to_string(),
                ),
            })
            .collect();

        // Return success response
        QueryResponse {
            status: "success".to_string(),
            data: Some(QueryResult {
                result_type: "vector".to_string(),
                result: serde_json::to_value(result).unwrap(),
            }),
            error: None,
            error_type: None,
        }
    }

    async fn query_range(&self, request: QueryRangeRequest) -> QueryRangeResponse {
        // Parse the request to get the expression
        let clock = Arc::new(SystemClock {});
        let stmt = match request.parse(clock) {
            Ok(stmt) => stmt,
            Err(e) => {
                let err = ErrorResponse::bad_data(e.to_string());
                return QueryRangeResponse {
                    status: err.status,
                    data: None,
                    error: Some(err.error),
                    error_type: Some(err.error_type),
                };
            }
        };

        // Calculate time range for bucket discovery
        // Need buckets covering [start - lookback, end]
        let start_secs = stmt
            .start
            .checked_sub(stmt.lookback_delta)
            .unwrap_or(UNIX_EPOCH)
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let end_secs = stmt.end.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;

        // Get query reader for the full time range
        let reader = match self.query_reader(start_secs, end_secs).await {
            Ok(reader) => reader,
            Err(e) => {
                let err = ErrorResponse::internal(e.to_string());
                return QueryRangeResponse {
                    status: err.status,
                    data: None,
                    error: Some(err.error),
                    error_type: Some(err.error_type),
                };
            }
        };

        // Evaluate at each step from start to end
        // Group results by metric labels -> Vec of (timestamp, value)
        // Use sorted Vec as key since HashMap doesn't implement Hash
        let mut series_map: HashMap<Vec<(String, String)>, Vec<(f64, String)>> = HashMap::new();

        let evaluator = Evaluator::new(&reader);
        let mut current_time = stmt.start;

        while current_time <= stmt.end {
            // Create instant query for this timestamp
            let instant_stmt = EvalStmt {
                expr: stmt.expr.clone(),
                start: current_time,
                end: current_time,
                interval: Duration::from_secs(0),
                lookback_delta: stmt.lookback_delta,
            };

            match evaluator.evaluate(instant_stmt).await {
                Ok(samples) => {
                    let timestamp_secs = current_time
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs_f64();

                    for sample in samples {
                        // Convert labels to sorted vec for use as key
                        let mut labels_key: Vec<(String, String)> =
                            sample.labels.into_iter().collect();
                        labels_key.sort();

                        let values = series_map.entry(labels_key).or_default();
                        values.push((timestamp_secs, sample.value.to_string()));
                    }
                }
                Err(e) => {
                    let err = ErrorResponse::execution(e.to_string());
                    return QueryRangeResponse {
                        status: err.status,
                        data: None,
                        error: Some(err.error),
                        error_type: Some(err.error_type),
                    };
                }
            }

            // Advance to next step
            current_time += stmt.interval;
        }

        // Convert to MatrixSeries format
        let result: Vec<MatrixSeries> = series_map
            .into_iter()
            .map(|(labels_vec, values)| {
                let metric: HashMap<String, String> = labels_vec.into_iter().collect();
                MatrixSeries { metric, values }
            })
            .collect();

        QueryRangeResponse {
            status: "success".to_string(),
            data: Some(QueryRangeResult {
                result_type: "matrix".to_string(),
                result,
            }),
            error: None,
            error_type: None,
        }
    }

    async fn series(&self, request: SeriesRequest) -> SeriesResponse {
        // Validate matches is non-empty
        if request.matches.is_empty() {
            let err = ErrorResponse::bad_data("at least one match[] required");
            return SeriesResponse {
                status: err.status,
                data: None,
                error: Some(err.error),
                error_type: Some(err.error_type),
            };
        }

        // Calculate time range (use defaults if not provided)
        let start_secs = request.start.unwrap_or(0);
        let end_secs = request.end.unwrap_or(i64::MAX);

        // Get query reader for time range
        let reader = match self.query_reader(start_secs, end_secs).await {
            Ok(reader) => reader,
            Err(e) => {
                let err = ErrorResponse::internal(e.to_string());
                return SeriesResponse {
                    status: err.status,
                    data: None,
                    error: Some(err.error),
                    error_type: Some(err.error_type),
                };
            }
        };

        // Get all matching series IDs (UNION)
        let series_ids = match get_matching_series(&reader, &request.matches).await {
            Ok(ids) => ids,
            Err(e) => {
                let err = ErrorResponse::bad_data(e);
                return SeriesResponse {
                    status: err.status,
                    data: None,
                    error: Some(err.error),
                    error_type: Some(err.error_type),
                };
            }
        };

        // Get forward index for all series
        let series_ids_vec: Vec<SeriesId> = series_ids.iter().copied().collect();
        let forward_index = match reader.forward_index(&series_ids_vec).await {
            Ok(index) => index,
            Err(e) => {
                let err = ErrorResponse::internal(e.to_string());
                return SeriesResponse {
                    status: err.status,
                    data: None,
                    error: Some(err.error),
                    error_type: Some(err.error_type),
                };
            }
        };

        // Extract label sets from each series
        let mut result: Vec<HashMap<String, String>> = series_ids_vec
            .iter()
            .filter_map(|id| forward_index.get_spec(id))
            .map(|spec| labels_to_map(&spec.labels))
            .collect();

        // Apply limit if specified
        if let Some(limit) = request.limit {
            result.truncate(limit);
        }

        SeriesResponse {
            status: "success".to_string(),
            data: Some(result),
            error: None,
            error_type: None,
        }
    }

    async fn labels(&self, request: LabelsRequest) -> LabelsResponse {
        // Calculate time range (use defaults if not provided)
        let start_secs = request.start.unwrap_or(0);
        let end_secs = request.end.unwrap_or(i64::MAX);

        // Get query reader for time range
        let reader = match self.query_reader(start_secs, end_secs).await {
            Ok(reader) => reader,
            Err(e) => {
                let err = ErrorResponse::internal(e.to_string());
                return LabelsResponse {
                    status: err.status,
                    data: None,
                    error: Some(err.error),
                    error_type: Some(err.error_type),
                };
            }
        };

        // Collect label names using hybrid approach:
        // - Filtered (match[]): use forward index (targeted I/O for matching series)
        // - Unfiltered: use inverted index (direct access to all label keys)
        let mut label_names: HashSet<String> = HashSet::new();

        match &request.matches {
            Some(matches) if !matches.is_empty() => {
                // Filtered: use forward index for targeted I/O
                let series_ids = match get_matching_series(&reader, matches).await {
                    Ok(ids) => ids,
                    Err(e) => {
                        let err = ErrorResponse::bad_data(e);
                        return LabelsResponse {
                            status: err.status,
                            data: None,
                            error: Some(err.error),
                            error_type: Some(err.error_type),
                        };
                    }
                };
                let series_ids_vec: Vec<SeriesId> = series_ids.iter().copied().collect();
                let forward_index = match reader.forward_index(&series_ids_vec).await {
                    Ok(index) => index,
                    Err(e) => {
                        let err = ErrorResponse::internal(e.to_string());
                        return LabelsResponse {
                            status: err.status,
                            data: None,
                            error: Some(err.error),
                            error_type: Some(err.error_type),
                        };
                    }
                };
                for (_id, spec) in forward_index.all_series() {
                    for attr in &spec.labels {
                        label_names.insert(attr.name.clone());
                    }
                }
            }
            _ => {
                // Unfiltered: use inverted index for direct key access
                let inverted_index = match reader.all_inverted_index().await {
                    Ok(index) => index,
                    Err(e) => {
                        let err = ErrorResponse::internal(e.to_string());
                        return LabelsResponse {
                            status: err.status,
                            data: None,
                            error: Some(err.error),
                            error_type: Some(err.error_type),
                        };
                    }
                };
                for attr in inverted_index.all_keys() {
                    label_names.insert(attr.name);
                }
            }
        };

        // Sort and apply limit
        let mut result: Vec<String> = label_names.into_iter().collect();
        result.sort();
        if let Some(limit) = request.limit {
            result.truncate(limit);
        }

        LabelsResponse {
            status: "success".to_string(),
            data: Some(result),
            error: None,
            error_type: None,
        }
    }

    async fn label_values(&self, request: LabelValuesRequest) -> LabelValuesResponse {
        // Calculate time range (use defaults if not provided)
        let start_secs = request.start.unwrap_or(0);
        let end_secs = request.end.unwrap_or(i64::MAX);

        // Get query reader for time range
        let reader = match self.query_reader(start_secs, end_secs).await {
            Ok(reader) => reader,
            Err(e) => {
                let err = ErrorResponse::internal(e.to_string());
                return LabelValuesResponse {
                    status: err.status,
                    data: None,
                    error: Some(err.error),
                    error_type: Some(err.error_type),
                };
            }
        };

        // Collect label values using hybrid approach:
        // - Filtered (match[]): use forward index (targeted I/O for matching series)
        // - Unfiltered: use inverted index (direct access to all label keys)
        let mut values: HashSet<String> = HashSet::new();

        match &request.matches {
            Some(matches) if !matches.is_empty() => {
                // Filtered: use forward index for targeted I/O
                let series_ids = match get_matching_series(&reader, matches).await {
                    Ok(ids) => ids,
                    Err(e) => {
                        let err = ErrorResponse::bad_data(e);
                        return LabelValuesResponse {
                            status: err.status,
                            data: None,
                            error: Some(err.error),
                            error_type: Some(err.error_type),
                        };
                    }
                };
                let series_ids_vec: Vec<SeriesId> = series_ids.iter().copied().collect();
                let forward_index = match reader.forward_index(&series_ids_vec).await {
                    Ok(index) => index,
                    Err(e) => {
                        let err = ErrorResponse::internal(e.to_string());
                        return LabelValuesResponse {
                            status: err.status,
                            data: None,
                            error: Some(err.error),
                            error_type: Some(err.error_type),
                        };
                    }
                };
                for (_id, spec) in forward_index.all_series() {
                    for attr in &spec.labels {
                        if attr.name == request.label_name {
                            values.insert(attr.value.clone());
                        }
                    }
                }
            }
            _ => {
                // Unfiltered: use optimized label_values that scans only keys for this label
                let label_values = match reader.label_values(&request.label_name).await {
                    Ok(vals) => vals,
                    Err(e) => {
                        let err = ErrorResponse::internal(e.to_string());
                        return LabelValuesResponse {
                            status: err.status,
                            data: None,
                            error: Some(err.error),
                            error_type: Some(err.error_type),
                        };
                    }
                };
                values.extend(label_values);
            }
        };

        // Sort and apply limit
        let mut result: Vec<String> = values.into_iter().collect();
        result.sort();
        if let Some(limit) = request.limit {
            result.truncate(limit);
        }

        LabelValuesResponse {
            status: "success".to_string(),
            data: Some(result),
            error: None,
            error_type: None,
        }
    }

    async fn metadata(&self, _request: MetadataRequest) -> MetadataResponse {
        todo!()
    }

    async fn federate(&self, _request: FederateRequest) -> FederateResponse {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::TimeBucket;
    use crate::model::{Label, MetricType, Sample, Series};
    use crate::storage::merge_operator::OpenTsdbMergeOperator;
    use common::Storage;
    use common::storage::in_memory::InMemoryStorage;
    use std::time::{Duration, UNIX_EPOCH};

    async fn create_test_storage() -> Arc<dyn Storage> {
        Arc::new(InMemoryStorage::with_merge_operator(Arc::new(
            OpenTsdbMergeOperator,
        )))
    }

    fn create_sample(
        metric_name: &str,
        label_pairs: Vec<(&str, &str)>,
        timestamp: i64,
        value: f64,
    ) -> Series {
        let labels: Vec<Label> = label_pairs
            .into_iter()
            .map(|(key, val)| Label {
                name: key.to_string(),
                value: val.to_string(),
            })
            .collect();
        let mut series = Series::new(
            metric_name,
            labels,
            vec![Sample {
                timestamp_ms: timestamp,
                value,
            }],
        );
        series.metric_type = Some(MetricType::Gauge);
        series
    }

    #[tokio::test]
    async fn should_return_success_response_for_valid_query() {
        // given
        let storage = create_test_storage().await;
        let tsdb = Tsdb::new(storage);

        // Ingest sample data
        // Bucket at minute 60 covers seconds 3600-7199
        let bucket = TimeBucket::hour(60);
        let mini = tsdb.get_or_create_for_ingest(bucket).await.unwrap();

        // Sample at 4000 seconds = 4000000 ms
        let sample = create_sample("http_requests", vec![("env", "prod")], 4_000_000, 42.0);
        mini.ingest(&sample).await.unwrap();
        tsdb.flush().await.unwrap();

        // Query time: 4100 seconds (within lookback of sample at 4000s)
        let query_time = UNIX_EPOCH + Duration::from_secs(4100);
        let request = QueryRequest {
            query: "http_requests".to_string(),
            time: Some(query_time),
            timeout: None,
        };

        // when
        let response = tsdb.query(request).await;

        // then
        assert_eq!(response.status, "success");
        assert!(response.error.is_none());
        assert!(response.data.is_some());

        let data = response.data.unwrap();
        assert_eq!(data.result_type, "vector");

        let results: Vec<VectorSeries> = serde_json::from_value(data.result).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].metric.get("__name__"),
            Some(&"http_requests".to_string())
        );
        assert_eq!(results[0].metric.get("env"), Some(&"prod".to_string()));
        assert_eq!(results[0].value.1, "42");
    }

    #[tokio::test]
    async fn should_return_error_for_invalid_query() {
        // given
        let storage = create_test_storage().await;
        let tsdb = Tsdb::new(storage);

        let request = QueryRequest {
            query: "invalid{".to_string(), // Invalid PromQL syntax
            time: None,
            timeout: None,
        };

        // when
        let response = tsdb.query(request).await;

        // then
        assert_eq!(response.status, "error");
        assert!(response.error.is_some());
        assert_eq!(response.error_type, Some("bad_data".to_string()));
        assert!(response.data.is_none());
    }

    #[tokio::test]
    async fn should_return_matrix_for_range_query() {
        // given
        let storage = create_test_storage().await;
        let tsdb = Tsdb::new(storage);

        // Ingest multiple samples at different timestamps
        // Bucket at minute 60 covers seconds 3600-7199
        let bucket = TimeBucket::hour(60);
        let mini = tsdb.get_or_create_for_ingest(bucket).await.unwrap();

        // Samples at 4000s, 4060s, 4120s (60s apart) - same series, multiple samples
        let series = Series::builder("http_requests")
            .label("env", "prod")
            .sample(4_000_000, 10.0)
            .sample(4_060_000, 20.0)
            .sample(4_120_000, 30.0)
            .build();
        mini.ingest(&series).await.unwrap();
        tsdb.flush().await.unwrap();

        // Query range: 4000s to 4120s with 60s step
        let request = QueryRangeRequest {
            query: "http_requests".to_string(),
            start: UNIX_EPOCH + Duration::from_secs(4000),
            end: UNIX_EPOCH + Duration::from_secs(4120),
            step: Duration::from_secs(60),
            timeout: None,
        };

        // when
        let response = tsdb.query_range(request).await;

        // then
        assert_eq!(response.status, "success");
        assert!(response.error.is_none());
        assert!(response.data.is_some());

        let data = response.data.unwrap();
        assert_eq!(data.result_type, "matrix");
        assert_eq!(data.result.len(), 1); // One series

        let series = &data.result[0];
        assert_eq!(
            series.metric.get("__name__"),
            Some(&"http_requests".to_string())
        );
        assert_eq!(series.metric.get("env"), Some(&"prod".to_string()));

        // Should have 3 values (one per step)
        assert_eq!(series.values.len(), 3);
        assert_eq!(series.values[0], (4000.0, "10".to_string()));
        assert_eq!(series.values[1], (4060.0, "20".to_string()));
        assert_eq!(series.values[2], (4120.0, "30".to_string()));
    }

    #[tokio::test]
    async fn should_return_series_for_valid_matcher() {
        // given
        let storage = create_test_storage().await;
        let tsdb = Tsdb::new(storage);

        let bucket = TimeBucket::hour(60);
        let mini = tsdb.get_or_create_for_ingest(bucket).await.unwrap();

        // Ingest two different series
        mini.ingest(&create_sample(
            "http_requests",
            vec![("env", "prod")],
            4_000_000,
            10.0,
        ))
        .await
        .unwrap();
        mini.ingest(&create_sample(
            "http_requests",
            vec![("env", "staging")],
            4_000_000,
            20.0,
        ))
        .await
        .unwrap();
        tsdb.flush().await.unwrap();

        let request = SeriesRequest {
            matches: vec!["http_requests".to_string()],
            start: Some(3600),
            end: Some(7200),
            limit: None,
        };

        // when
        let response = tsdb.series(request).await;

        // then
        assert_eq!(response.status, "success");
        assert!(response.data.is_some());

        let data = response.data.unwrap();
        assert_eq!(data.len(), 2);
    }

    #[tokio::test]
    async fn should_return_labels_for_matching_series() {
        // given
        let storage = create_test_storage().await;
        let tsdb = Tsdb::new(storage);

        let bucket = TimeBucket::hour(60);
        let mini = tsdb.get_or_create_for_ingest(bucket).await.unwrap();

        let sample = create_sample(
            "http_requests",
            vec![("env", "prod"), ("method", "GET")],
            4_000_000,
            10.0,
        );
        mini.ingest(&sample).await.unwrap();
        tsdb.flush().await.unwrap();

        let request = LabelsRequest {
            matches: Some(vec!["http_requests".to_string()]),
            start: Some(3600),
            end: Some(7200),
            limit: None,
        };

        // when
        let response = tsdb.labels(request).await;

        // then
        assert_eq!(response.status, "success");
        assert!(response.data.is_some());

        let data = response.data.unwrap();
        assert!(data.contains(&"__name__".to_string()));
        assert!(data.contains(&"env".to_string()));
        assert!(data.contains(&"method".to_string()));
    }

    #[tokio::test]
    async fn should_return_label_values_for_matching_series() {
        // given
        let storage = create_test_storage().await;
        let tsdb = Tsdb::new(storage);

        let bucket = TimeBucket::hour(60);
        let mini = tsdb.get_or_create_for_ingest(bucket).await.unwrap();

        mini.ingest(&create_sample(
            "http_requests",
            vec![("env", "prod")],
            4_000_000,
            10.0,
        ))
        .await
        .unwrap();
        mini.ingest(&create_sample(
            "http_requests",
            vec![("env", "staging")],
            4_000_000,
            20.0,
        ))
        .await
        .unwrap();
        tsdb.flush().await.unwrap();

        let request = LabelValuesRequest {
            label_name: "env".to_string(),
            matches: Some(vec!["http_requests".to_string()]),
            start: Some(3600),
            end: Some(7200),
            limit: None,
        };

        // when
        let response = tsdb.label_values(request).await;

        // then
        assert_eq!(response.status, "success");
        assert!(response.data.is_some());

        let data = response.data.unwrap();
        assert_eq!(data.len(), 2);
        assert!(data.contains(&"prod".to_string()));
        assert!(data.contains(&"staging".to_string()));
    }

    #[tokio::test]
    async fn should_return_error_when_series_has_no_matcher() {
        // given
        let storage = create_test_storage().await;
        let tsdb = Tsdb::new(storage);

        let request = SeriesRequest {
            matches: vec![],
            start: None,
            end: None,
            limit: None,
        };

        // when
        let response = tsdb.series(request).await;

        // then
        assert_eq!(response.status, "error");
        assert_eq!(response.error_type, Some("bad_data".to_string()));
    }

    #[tokio::test]
    async fn should_return_all_labels_when_no_matcher_provided() {
        // given
        let storage = create_test_storage().await;
        let tsdb = Tsdb::new(storage);

        let bucket = TimeBucket::hour(60);
        let mini = tsdb.get_or_create_for_ingest(bucket).await.unwrap();

        let sample = create_sample(
            "http_requests",
            vec![("env", "prod"), ("method", "GET")],
            4_000_000,
            10.0,
        );
        mini.ingest(&sample).await.unwrap();
        tsdb.flush().await.unwrap();

        let request = LabelsRequest {
            matches: None,
            start: Some(3600),
            end: Some(7200),
            limit: None,
        };

        // when
        let response = tsdb.labels(request).await;

        // then
        assert_eq!(response.status, "success");
        assert!(response.data.is_some());

        let data = response.data.unwrap();
        assert!(data.contains(&"__name__".to_string()));
        assert!(data.contains(&"env".to_string()));
        assert!(data.contains(&"method".to_string()));
    }

    #[tokio::test]
    async fn should_return_all_label_values_when_no_matcher_provided() {
        // given
        let storage = create_test_storage().await;
        let tsdb = Tsdb::new(storage);

        let bucket = TimeBucket::hour(60);
        let mini = tsdb.get_or_create_for_ingest(bucket).await.unwrap();

        mini.ingest(&create_sample(
            "http_requests",
            vec![("env", "prod")],
            4_000_000,
            10.0,
        ))
        .await
        .unwrap();
        mini.ingest(&create_sample(
            "http_requests",
            vec![("env", "staging")],
            4_000_000,
            20.0,
        ))
        .await
        .unwrap();
        tsdb.flush().await.unwrap();

        let request = LabelValuesRequest {
            label_name: "env".to_string(),
            matches: None,
            start: Some(3600),
            end: Some(7200),
            limit: None,
        };

        // when
        let response = tsdb.label_values(request).await;

        // then
        assert_eq!(response.status, "success");
        assert!(response.data.is_some());

        let data = response.data.unwrap();
        assert_eq!(data.len(), 2);
        assert!(data.contains(&"prod".to_string()));
        assert!(data.contains(&"staging".to_string()));
    }

    #[tokio::test]
    async fn should_filter_labels_by_match_correctly() {
        // given: two different metrics with different labels
        let storage = create_test_storage().await;
        let tsdb = Tsdb::new(storage);

        let bucket = TimeBucket::hour(60);
        let mini = tsdb.get_or_create_for_ingest(bucket).await.unwrap();

        // http_requests has env and method labels
        mini.ingest(&create_sample(
            "http_requests",
            vec![("env", "prod"), ("method", "GET")],
            4_000_000,
            10.0,
        ))
        .await
        .unwrap();
        // db_queries has env and table labels (different from http_requests)
        mini.ingest(&create_sample(
            "db_queries",
            vec![("env", "prod"), ("table", "users")],
            4_000_000,
            20.0,
        ))
        .await
        .unwrap();
        tsdb.flush().await.unwrap();

        // when: query labels with match[] filter for http_requests only
        let request = LabelsRequest {
            matches: Some(vec!["http_requests".to_string()]),
            start: Some(3600),
            end: Some(7200),
            limit: None,
        };
        let response = tsdb.labels(request).await;

        // then: should only return labels from http_requests, not db_queries
        assert_eq!(response.status, "success");
        let data = response.data.unwrap();
        assert!(data.contains(&"__name__".to_string()));
        assert!(data.contains(&"env".to_string()));
        assert!(data.contains(&"method".to_string()));
        // table label should NOT be present since it belongs to db_queries
        assert!(!data.contains(&"table".to_string()));
    }

    #[tokio::test]
    async fn should_filter_label_values_by_match_correctly() {
        // given: two different metrics with same label name but different values
        let storage = create_test_storage().await;
        let tsdb = Tsdb::new(storage);

        let bucket = TimeBucket::hour(60);
        let mini = tsdb.get_or_create_for_ingest(bucket).await.unwrap();

        // http_requests with env=prod
        mini.ingest(&create_sample(
            "http_requests",
            vec![("env", "prod")],
            4_000_000,
            10.0,
        ))
        .await
        .unwrap();
        // db_queries with env=staging (different metric, different env value)
        mini.ingest(&create_sample(
            "db_queries",
            vec![("env", "staging")],
            4_000_000,
            20.0,
        ))
        .await
        .unwrap();
        tsdb.flush().await.unwrap();

        // when: query label values for "env" with match[] filter for http_requests only
        let request = LabelValuesRequest {
            label_name: "env".to_string(),
            matches: Some(vec!["http_requests".to_string()]),
            start: Some(3600),
            end: Some(7200),
            limit: None,
        };
        let response = tsdb.label_values(request).await;

        // then: should only return env values from http_requests, not db_queries
        assert_eq!(response.status, "success");
        let data = response.data.unwrap();
        assert_eq!(data.len(), 1);
        assert!(data.contains(&"prod".to_string()));
        // staging should NOT be present since it belongs to db_queries
        assert!(!data.contains(&"staging".to_string()));
    }
}
