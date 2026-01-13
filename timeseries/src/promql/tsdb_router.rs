use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

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
use crate::index::InvertedIndexLookup;
use crate::model::Label;
use crate::model::SeriesId;
use crate::model::TimeBucket;
use crate::query::QueryReader;
use crate::tsdb::Tsdb;
use async_trait::async_trait;
use common::clock::SystemClock;
use promql_parser::parser::{EvalStmt, Expr, VectorSelector};

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
    bucket: TimeBucket,
    matches: &[String],
) -> Result<HashSet<SeriesId>, String> {
    let mut all_series = HashSet::new();

    for selector_str in matches {
        let selector = parse_selector(selector_str)?;
        let series = evaluate_selector_with_reader(reader, bucket, &selector)
            .await
            .map_err(|e| e.to_string())?;
        all_series.extend(series);
    }

    Ok(all_series)
}

/// Get all series across multiple buckets, with fingerprint-based deduplication
async fn get_matching_series_multi_bucket<R: QueryReader>(
    reader: &R,
    buckets: &[TimeBucket],
    matches: &[String],
) -> Result<HashMap<TimeBucket, HashSet<SeriesId>>, String> {
    let mut bucket_series_map = HashMap::new();

    for &bucket in buckets {
        let series = get_matching_series(reader, bucket, matches).await?;
        if !series.is_empty() {
            bucket_series_map.insert(bucket, series);
        }
    }

    Ok(bucket_series_map)
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

        // Get all buckets for multi-bucket support
        let buckets = match reader.list_buckets().await {
            Ok(buckets) => buckets,
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

        if buckets.is_empty() {
            // No buckets means no data, return empty result
            return SeriesResponse {
                status: "success".to_string(),
                data: Some(vec![]),
                error: None,
                error_type: None,
            };
        }

        let bucket_series_map =
            match get_matching_series_multi_bucket(&reader, &buckets, &request.matches).await {
                Ok(map) => map,
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

        let mut unique_series: HashMap<Vec<Label>, HashMap<String, String>> = HashMap::new();

        for (bucket, series_ids) in bucket_series_map {
            let series_ids_vec: Vec<SeriesId> = series_ids.iter().copied().collect();

            if series_ids_vec.is_empty() {
                continue;
            }

            let forward_index = match reader.forward_index(&bucket, &series_ids_vec).await {
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

            // Extract label sets and deduplicate by labels
            for id in &series_ids_vec {
                if let Some(spec) = forward_index.get_spec(id) {
                    // Create sorted labels for deduplication
                    let mut sorted_labels = spec.labels.clone();
                    sorted_labels
                        .sort_by(|a, b| a.name.cmp(&b.name).then_with(|| a.value.cmp(&b.value)));

                    if let std::collections::hash_map::Entry::Vacant(e) =
                        unique_series.entry(sorted_labels)
                    {
                        let labels_map = labels_to_map(&spec.labels);
                        e.insert(labels_map);
                    }
                }
            }
        }

        let mut result: Vec<HashMap<String, String>> = unique_series.into_values().collect();

        // Sort for consistent output (by metric name first, then other labels)
        result.sort_by(|a, b| {
            // Compare metric names, using empty string as default
            let a_name = a.get("__name__").map(|s| s.as_str()).unwrap_or("");
            let b_name = b.get("__name__").map(|s| s.as_str()).unwrap_or("");
            a_name.cmp(b_name).then_with(|| {
                // If metric names are equal, compare all labels
                let mut a_labels: Vec<_> = a.iter().collect();
                let mut b_labels: Vec<_> = b.iter().collect();
                a_labels.sort();
                b_labels.sort();
                a_labels.cmp(&b_labels)
            })
        });

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
        let start_secs = request.start.unwrap_or(0);
        let end_secs = request.end.unwrap_or(i64::MAX);

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

        let buckets = match reader.list_buckets().await {
            Ok(buckets) => buckets,
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

        if buckets.is_empty() {
            // No buckets means no data, return empty result
            return LabelsResponse {
                status: "success".to_string(),
                data: Some(vec![]),
                error: None,
                error_type: None,
            };
        }

        let mut label_names: HashSet<String> = HashSet::new();

        match &request.matches {
            Some(matches) if !matches.is_empty() => {
                let bucket_series_map =
                    match get_matching_series_multi_bucket(&reader, &buckets, matches).await {
                        Ok(map) => map,
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

                // Collect label names from all matching series across buckets
                for (bucket, series_ids) in bucket_series_map {
                    let series_ids_vec: Vec<SeriesId> = series_ids.iter().copied().collect();

                    if series_ids_vec.is_empty() {
                        continue;
                    }

                    let forward_index = match reader.forward_index(&bucket, &series_ids_vec).await {
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
            }
            _ => {
                // Unfiltered: use inverted index to pull all label kv pairs in bucket
                for bucket in buckets {
                    let inverted_index = match reader.all_inverted_index(&bucket).await {
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
        let start_secs = request.start.unwrap_or(0);
        let end_secs = request.end.unwrap_or(i64::MAX);

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

        let buckets = match reader.list_buckets().await {
            Ok(buckets) => buckets,
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

        if buckets.is_empty() {
            return LabelValuesResponse {
                status: "success".to_string(),
                data: Some(vec![]),
                error: None,
                error_type: None,
            };
        }

        let mut values: HashSet<String> = HashSet::new();

        match &request.matches {
            Some(matches) if !matches.is_empty() => {
                let bucket_series_map =
                    match get_matching_series_multi_bucket(&reader, &buckets, matches).await {
                        Ok(map) => map,
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

                // Collect label values from all matching series across buckets
                for (bucket, series_ids) in bucket_series_map {
                    let series_ids_vec: Vec<SeriesId> = series_ids.iter().copied().collect();

                    if series_ids_vec.is_empty() {
                        continue;
                    }

                    let forward_index = match reader.forward_index(&bucket, &series_ids_vec).await {
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
            }
            _ => {
                for bucket in buckets {
                    let label_values = match reader.label_values(&bucket, &request.label_name).await
                    {
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
            }
        };

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
        mini.ingest(&sample, 30).await.unwrap();
        tsdb.flush(30).await.unwrap();

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
        mini.ingest(&series, 30).await.unwrap();
        tsdb.flush(30).await.unwrap();

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
        mini.ingest(
            &create_sample("http_requests", vec![("env", "prod")], 4_000_000, 10.0),
            30,
        )
        .await
        .unwrap();
        mini.ingest(
            &create_sample("http_requests", vec![("env", "staging")], 4_000_000, 20.0),
            30,
        )
        .await
        .unwrap();
        tsdb.flush(30).await.unwrap();

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
        mini.ingest(&sample, 30).await.unwrap();
        tsdb.flush(30).await.unwrap();

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

        mini.ingest(
            &create_sample("http_requests", vec![("env", "prod")], 4_000_000, 10.0),
            30,
        )
        .await
        .unwrap();
        mini.ingest(
            &create_sample("http_requests", vec![("env", "staging")], 4_000_000, 20.0),
            30,
        )
        .await
        .unwrap();
        tsdb.flush(30).await.unwrap();

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
        mini.ingest(&sample, 30).await.unwrap();
        tsdb.flush(30).await.unwrap();

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

        mini.ingest(
            &create_sample("http_requests", vec![("env", "prod")], 4_000_000, 10.0),
            30,
        )
        .await
        .unwrap();
        mini.ingest(
            &create_sample("http_requests", vec![("env", "staging")], 4_000_000, 20.0),
            30,
        )
        .await
        .unwrap();
        tsdb.flush(30).await.unwrap();

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
        mini.ingest(
            &create_sample(
                "http_requests",
                vec![("env", "prod"), ("method", "GET")],
                4_000_000,
                10.0,
            ),
            30,
        )
        .await
        .unwrap();
        // db_queries has env and table labels (different from http_requests)
        mini.ingest(
            &create_sample(
                "db_queries",
                vec![("env", "prod"), ("table", "users")],
                4_000_000,
                20.0,
            ),
            30,
        )
        .await
        .unwrap();
        tsdb.flush(30).await.unwrap();

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
        mini.ingest(
            &create_sample("http_requests", vec![("env", "prod")], 4_000_000, 10.0),
            30,
        )
        .await
        .unwrap();
        // db_queries with env=staging (different metric, different env value)
        mini.ingest(
            &create_sample("db_queries", vec![("env", "staging")], 4_000_000, 20.0),
            30,
        )
        .await
        .unwrap();
        tsdb.flush(30).await.unwrap();

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

    fn labels_map(labels: &[(&str, &str)]) -> HashMap<String, String> {
        labels
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }

    #[tokio::test]
    async fn should_return_from_multiple_buckets_from_series_request() {
        // given: series spanning multiple time buckets
        let storage = create_test_storage().await;
        let tsdb = Tsdb::new(storage);
        // Bucket 1: hour 60 (covers seconds 3600-7199)
        let bucket1 = TimeBucket::hour(60);
        let mini1 = tsdb.get_or_create_for_ingest(bucket1).await.unwrap();
        // Bucket 2: hour 120 (covers seconds 7200-10799)
        let bucket2 = TimeBucket::hour(120);
        let mini2 = tsdb.get_or_create_for_ingest(bucket2).await.unwrap();
        mini1
            .ingest(
                &create_sample("http_requests", vec![("env", "prod")], 4_000_000, 10.0),
                30,
            )
            .await
            .unwrap();
        mini2
            .ingest(
                &create_sample("http_requests", vec![("env", "staging")], 8_000_000, 30.0),
                30,
            )
            .await
            .unwrap();

        tsdb.flush(30).await.unwrap();

        // when: query across both buckets
        let request = SeriesRequest {
            matches: vec!["http_requests".to_string()],
            start: Some(3600), // Covers bucket 1
            end: Some(10800),  // Covers bucket 2
            limit: None,
        };
        let response = tsdb.series(request).await;

        // then: should return deduplicated series from both buckets
        assert_eq!(response.status, "success");
        let data = response.data.unwrap();
        assert_eq!(
            data,
            vec![
                labels_map(&[("__name__", "http_requests"), ("env", "prod")]),
                labels_map(&[("__name__", "http_requests"), ("env", "staging")]),
            ]
        );
    }

    #[tokio::test]
    async fn should_deduplicate_identical_series_across_buckets_from_series_request() {
        // given: identical series appearing in multiple buckets
        let storage = create_test_storage().await;
        let tsdb = Tsdb::new(storage);
        // Bucket 1 and 2 with identical series
        let bucket1 = TimeBucket::hour(60);
        let bucket2 = TimeBucket::hour(120);
        let mini1 = tsdb.get_or_create_for_ingest(bucket1).await.unwrap();
        let mini2 = tsdb.get_or_create_for_ingest(bucket2).await.unwrap();
        // Same exact series in both buckets
        let series_labels = vec![("env", "prod"), ("service", "api")];
        mini1
            .ingest(
                &create_sample("memory_usage", series_labels.clone(), 4_000_000, 100.0),
                30,
            )
            .await
            .unwrap();
        mini2
            .ingest(
                &create_sample("memory_usage", series_labels, 8_000_000, 150.0),
                30,
            )
            .await
            .unwrap();
        tsdb.flush(30).await.unwrap();

        // when: query across both buckets
        let request = SeriesRequest {
            matches: vec!["memory_usage".to_string()],
            start: Some(3600),
            end: Some(10800),
            limit: None,
        };
        let response = tsdb.series(request).await;

        // then:
        assert_eq!(response.status, "success");
        let data = response.data.unwrap();
        assert_eq!(
            data,
            vec![labels_map(&[
                ("__name__", "memory_usage"),
                ("env", "prod"),
                ("service", "api")
            ]),]
        );
    }

    #[tokio::test]
    async fn should_handle_empty_buckets_from_series_request() {
        // given: some buckets have data, some don't
        let storage = create_test_storage().await;
        let tsdb = Tsdb::new(storage);
        // Only ingest into one bucket, leaving others empty
        let bucket1 = TimeBucket::hour(60);
        let mini1 = tsdb.get_or_create_for_ingest(bucket1).await.unwrap();
        mini1
            .ingest(
                &create_sample("cpu_usage", vec![("host", "server1")], 4_000_000, 75.0),
                30,
            )
            .await
            .unwrap();
        tsdb.flush(30).await.unwrap();

        // when: query across a range that includes empty buckets
        let request = SeriesRequest {
            matches: vec!["cpu_usage".to_string()],
            start: Some(3600), // Bucket 1 (has data)
            end: Some(14400),  // Spans multiple buckets (some empty)
            limit: None,
        };
        let response = tsdb.series(request).await;

        // then: should successfully return series from non-empty buckets
        assert_eq!(response.status, "success");
        let data = response.data.unwrap();
        assert_eq!(
            data,
            vec![labels_map(&[
                ("__name__", "cpu_usage"),
                ("host", "server1")
            ]),]
        );
    }

    #[tokio::test]
    async fn should_apply_limit_across_results_from_series_request() {
        // given: multiple series across multiple buckets
        let storage = create_test_storage().await;
        let tsdb = Tsdb::new(storage);
        let bucket1 = TimeBucket::hour(60);
        let bucket2 = TimeBucket::hour(120);
        let mini1 = tsdb.get_or_create_for_ingest(bucket1).await.unwrap();
        let mini2 = tsdb.get_or_create_for_ingest(bucket2).await.unwrap();
        // Create multiple series across buckets
        for i in 1..=5 {
            mini1
                .ingest(
                    &create_sample(
                        "requests",
                        vec![("service", &format!("svc{}", i))],
                        4_000_000,
                        i as f64 * 10.0,
                    ),
                    30,
                )
                .await
                .unwrap();
        }
        for i in 6..=10 {
            mini2
                .ingest(
                    &create_sample(
                        "requests",
                        vec![("service", &format!("svc{}", i))],
                        8_000_000,
                        i as f64 * 10.0,
                    ),
                    30,
                )
                .await
                .unwrap();
        }
        tsdb.flush(30).await.unwrap();

        // when: query with a limit
        let request = SeriesRequest {
            matches: vec!["requests".to_string()],
            start: Some(3600),
            end: Some(10800),
            limit: Some(3), // Limit to 3 series
        };
        let response = tsdb.series(request).await;

        // then: should return exactly 3 series (respecting the limit)
        assert_eq!(response.status, "success");
        let data = response.data.unwrap();
        assert_eq!(data.len(), 3); // Should be limited to 3 series
        // All should be requests series
        for series in &data {
            assert_eq!(series.get("__name__"), Some(&"requests".to_string()));
        }
    }

    #[tokio::test]
    async fn should_return_labels_from_multiple_buckets() {
        // given: labels spanning multiple time buckets
        let storage = create_test_storage().await;
        let tsdb = Tsdb::new(storage);
        let bucket1 = TimeBucket::hour(60);
        let mini1 = tsdb.get_or_create_for_ingest(bucket1).await.unwrap();
        let bucket2 = TimeBucket::hour(120);
        let mini2 = tsdb.get_or_create_for_ingest(bucket2).await.unwrap();
        mini1
            .ingest(
                &create_sample(
                    "http_requests",
                    vec![("env", "prod"), ("service", "web")],
                    4_000_000,
                    10.0,
                ),
                30,
            )
            .await
            .unwrap();
        mini2
            .ingest(
                &create_sample(
                    "db_queries",
                    vec![("env", "staging"), ("database", "postgres")],
                    8_000_000,
                    20.0,
                ),
                30,
            )
            .await
            .unwrap();

        tsdb.flush(30).await.unwrap();

        // when: query labels across both buckets (no match filter)
        let request = LabelsRequest {
            matches: None,
            start: Some(3600), // Covers bucket 1
            end: Some(10800),  // Covers bucket 2
            limit: None,
        };
        let response = tsdb.labels(request).await;

        // then: should return all label names from both buckets
        assert_eq!(response.status, "success");
        let data = response.data.unwrap();
        // Should contain labels from both buckets
        assert!(data.contains(&"__name__".to_string()));
        assert!(data.contains(&"env".to_string()));
        assert!(data.contains(&"service".to_string())); // From bucket 1
        assert!(data.contains(&"database".to_string())); // From bucket 2
    }

    #[tokio::test]
    async fn should_deduplicate_labels_across_buckets() {
        // given: same label names appearing in multiple buckets
        let storage = create_test_storage().await;
        let tsdb = Tsdb::new(storage);
        let bucket1 = TimeBucket::hour(60);
        let bucket2 = TimeBucket::hour(120);
        let mini1 = tsdb.get_or_create_for_ingest(bucket1).await.unwrap();
        let mini2 = tsdb.get_or_create_for_ingest(bucket2).await.unwrap();
        mini1
            .ingest(
                &create_sample("metric_a", vec![("env", "prod")], 4_000_000, 10.0),
                30,
            )
            .await
            .unwrap();
        mini2
            .ingest(
                &create_sample("metric_b", vec![("env", "staging")], 8_000_000, 20.0),
                30,
            )
            .await
            .unwrap();
        tsdb.flush(30).await.unwrap();

        // when: query labels across both buckets
        let request = LabelsRequest {
            matches: None,
            start: Some(3600),
            end: Some(10800),
            limit: None,
        };
        let response = tsdb.labels(request).await;

        // then: should return deduplicated label names
        assert_eq!(response.status, "success");
        let data = response.data.unwrap();
        // Should contain each label name only once despite appearing in multiple buckets
        assert!(data.contains(&"__name__".to_string()));
        assert!(data.contains(&"env".to_string()));
        assert_eq!(data.len(), 2);
    }

    #[tokio::test]
    async fn should_filter_labels_by_match_across_buckets() {
        // given: different metrics across multiple buckets
        let storage = create_test_storage().await;
        let tsdb = Tsdb::new(storage);
        let bucket1 = TimeBucket::hour(60);
        let bucket2 = TimeBucket::hour(120);
        let mini1 = tsdb.get_or_create_for_ingest(bucket1).await.unwrap();
        let mini2 = tsdb.get_or_create_for_ingest(bucket2).await.unwrap();
        mini1
            .ingest(
                &create_sample(
                    "http_requests",
                    vec![("env", "prod"), ("method", "GET")],
                    4_000_000,
                    10.0,
                ),
                30,
            )
            .await
            .unwrap();
        mini2
            .ingest(
                &create_sample(
                    "http_requests",
                    vec![("env", "staging"), ("path", "/api/v1")],
                    8_000_000,
                    20.0,
                ),
                30,
            )
            .await
            .unwrap();
        mini2
            .ingest(
                &create_sample(
                    "db_queries",
                    vec![("env", "prod"), ("table", "users")],
                    8_000_000,
                    30.0,
                ),
                30,
            )
            .await
            .unwrap();
        tsdb.flush(30).await.unwrap();

        // when: query labels with match[] filter for http_requests only
        let request = LabelsRequest {
            matches: Some(vec!["http_requests".to_string()]),
            start: Some(3600),
            end: Some(10800),
            limit: None,
        };
        let response = tsdb.labels(request).await;

        // then: should only return labels from http_requests across both buckets
        assert_eq!(response.status, "success");
        let data = response.data.unwrap();
        assert!(data.contains(&"__name__".to_string()));
        assert!(data.contains(&"env".to_string()));
        assert!(data.contains(&"method".to_string()));
        assert!(data.contains(&"path".to_string()));
        assert!(!data.contains(&"table".to_string()));
        assert_eq!(data.len(), 4);
    }

    #[tokio::test]
    async fn should_apply_limit_across_multi_bucket_labels() {
        // given: many different labels across multiple buckets
        let storage = create_test_storage().await;
        let tsdb = Tsdb::new(storage);
        let bucket1 = TimeBucket::hour(60);
        let bucket2 = TimeBucket::hour(120);
        let mini1 = tsdb.get_or_create_for_ingest(bucket1).await.unwrap();
        let mini2 = tsdb.get_or_create_for_ingest(bucket2).await.unwrap();
        mini1
            .ingest(
                &create_sample(
                    "metric1",
                    vec![
                        ("label_a", "value"),
                        ("label_b", "value"),
                        ("label_c", "value"),
                    ],
                    4_000_000,
                    10.0,
                ),
                30,
            )
            .await
            .unwrap();
        mini2
            .ingest(
                &create_sample(
                    "metric2",
                    vec![
                        ("label_d", "value"),
                        ("label_e", "value"),
                        ("label_f", "value"),
                    ],
                    8_000_000,
                    20.0,
                ),
                30,
            )
            .await
            .unwrap();
        tsdb.flush(30).await.unwrap();

        // when: query labels with a limit
        let request = LabelsRequest {
            matches: None,
            start: Some(3600),
            end: Some(10800),
            limit: Some(3),
        };
        let response = tsdb.labels(request).await;

        // then: should return exactly 3 labels (respecting the limit)
        assert_eq!(response.status, "success");
        let data = response.data.unwrap();
        assert_eq!(data.len(), 3); // Should be limited to 3 labels
    }

    #[tokio::test]
    async fn should_return_label_values_from_multiple_buckets() {
        // given: label values spanning multiple time buckets
        let storage = create_test_storage().await;
        let tsdb = Tsdb::new(storage);
        let bucket1 = TimeBucket::hour(60);
        let mini1 = tsdb.get_or_create_for_ingest(bucket1).await.unwrap();
        let bucket2 = TimeBucket::hour(120);
        let mini2 = tsdb.get_or_create_for_ingest(bucket2).await.unwrap();
        mini1
            .ingest(
                &create_sample("http_requests", vec![("env", "prod")], 4_000_000, 10.0),
                30,
            )
            .await
            .unwrap();
        mini2
            .ingest(
                &create_sample("http_requests", vec![("env", "staging")], 8_000_000, 20.0),
                30,
            )
            .await
            .unwrap();
        tsdb.flush(30).await.unwrap();

        // when: query env label values across both buckets (no match filter)
        let request = LabelValuesRequest {
            label_name: "env".to_string(),
            matches: None,
            start: Some(3600), // Covers bucket 1
            end: Some(10800),  // Covers bucket 2
            limit: None,
        };
        let response = tsdb.label_values(request).await;

        // then: should return all env values from both buckets
        assert_eq!(response.status, "success");
        let data = response.data.unwrap();
        assert_eq!(data.len(), 2);
        assert!(data.contains(&"prod".to_string()));
        assert!(data.contains(&"staging".to_string()));
    }

    #[tokio::test]
    async fn should_deduplicate_label_values_across_buckets() {
        // given: same label values appearing in multiple buckets
        let storage = create_test_storage().await;
        let tsdb = Tsdb::new(storage);
        let bucket1 = TimeBucket::hour(60);
        let bucket2 = TimeBucket::hour(120);
        let mini1 = tsdb.get_or_create_for_ingest(bucket1).await.unwrap();
        let mini2 = tsdb.get_or_create_for_ingest(bucket2).await.unwrap();
        mini1
            .ingest(
                &create_sample("metric_a", vec![("env", "prod")], 4_000_000, 10.0),
                30,
            )
            .await
            .unwrap();
        mini2
            .ingest(
                &create_sample("metric_b", vec![("env", "prod")], 8_000_000, 20.0),
                30,
            )
            .await
            .unwrap();
        tsdb.flush(30).await.unwrap();

        // when: query env label values across both buckets
        let request = LabelValuesRequest {
            label_name: "env".to_string(),
            matches: None,
            start: Some(3600),
            end: Some(10800),
            limit: None,
        };
        let response = tsdb.label_values(request).await;

        // then: should return deduplicated label values
        assert_eq!(response.status, "success");
        let data = response.data.unwrap();
        assert_eq!(data.len(), 1);
        assert!(data.contains(&"prod".to_string()));
    }

    #[tokio::test]
    async fn should_filter_label_values_by_match_across_buckets() {
        // given: different metrics across multiple buckets with same label name
        let storage = create_test_storage().await;
        let tsdb = Tsdb::new(storage);
        let bucket1 = TimeBucket::hour(60);
        let bucket2 = TimeBucket::hour(120);
        let mini1 = tsdb.get_or_create_for_ingest(bucket1).await.unwrap();
        let mini2 = tsdb.get_or_create_for_ingest(bucket2).await.unwrap();
        mini1
            .ingest(
                &create_sample("http_requests", vec![("env", "prod")], 4_000_000, 10.0),
                30,
            )
            .await
            .unwrap();
        mini2
            .ingest(
                &create_sample("http_requests", vec![("env", "staging")], 8_000_000, 20.0),
                30,
            )
            .await
            .unwrap();
        mini2
            .ingest(
                &create_sample("db_queries", vec![("env", "development")], 8_000_000, 30.0),
                30,
            )
            .await
            .unwrap();
        tsdb.flush(30).await.unwrap();

        // when: query env label values with match[] filter for http_requests only
        let request = LabelValuesRequest {
            label_name: "env".to_string(),
            matches: Some(vec!["http_requests".to_string()]),
            start: Some(3600),
            end: Some(10800),
            limit: None,
        };
        let response = tsdb.label_values(request).await;

        // then: should only return env values from http_requests across both buckets
        assert_eq!(response.status, "success");
        let data = response.data.unwrap();
        assert_eq!(data.len(), 2);
        assert!(data.contains(&"prod".to_string())); // From bucket 1
        assert!(data.contains(&"staging".to_string())); // From bucket 2
    }

    #[tokio::test]
    async fn should_handle_empty_buckets_in_multi_bucket_label_values() {
        // given: some buckets have data, some don't
        let storage = create_test_storage().await;
        let tsdb = Tsdb::new(storage);
        // Only ingest into one bucket, leaving others empty
        let bucket1 = TimeBucket::hour(60);
        let mini1 = tsdb.get_or_create_for_ingest(bucket1).await.unwrap();
        mini1
            .ingest(
                &create_sample("cpu_usage", vec![("host", "server1")], 4_000_000, 75.0),
                30,
            )
            .await
            .unwrap();
        tsdb.flush(30).await.unwrap();

        // when: query across a range that includes empty buckets
        let request = LabelValuesRequest {
            label_name: "host".to_string(),
            matches: None,
            start: Some(3600), // Bucket 1 (has data)
            end: Some(14400),  // Spans multiple buckets (some empty)
            limit: None,
        };
        let response = tsdb.label_values(request).await;

        // then: should successfully return label values from non-empty buckets
        assert_eq!(response.status, "success");
        let data = response.data.unwrap();
        assert_eq!(data.len(), 1);
        assert!(data.contains(&"server1".to_string()));
    }

    #[tokio::test]
    async fn should_apply_limit_across_multi_bucket_label_values() {
        // given: many different label values across multiple buckets
        let storage = create_test_storage().await;
        let tsdb = Tsdb::new(storage);
        let bucket1 = TimeBucket::hour(60);
        let bucket2 = TimeBucket::hour(120);
        let mini1 = tsdb.get_or_create_for_ingest(bucket1).await.unwrap();
        let mini2 = tsdb.get_or_create_for_ingest(bucket2).await.unwrap();
        for i in 1..=3 {
            mini1
                .ingest(
                    &create_sample(
                        "requests",
                        vec![("service", &format!("svc{}", i))],
                        4_000_000,
                        i as f64 * 10.0,
                    ),
                    30,
                )
                .await
                .unwrap();
        }
        for i in 4..=6 {
            mini2
                .ingest(
                    &create_sample(
                        "requests",
                        vec![("service", &format!("svc{}", i))],
                        8_000_000,
                        i as f64 * 10.0,
                    ),
                    30,
                )
                .await
                .unwrap();
        }
        tsdb.flush(30).await.unwrap();

        // when: query service label values with a limit
        let request = LabelValuesRequest {
            label_name: "service".to_string(),
            matches: None,
            start: Some(3600),
            end: Some(10800),
            limit: Some(3), // Limit to 3 values
        };
        let response = tsdb.label_values(request).await;

        // then: should return exactly 3 values (respecting the limit)
        assert_eq!(response.status, "success");
        let data = response.data.unwrap();
        assert_eq!(data.len(), 3); // Should be limited to 3 values
    }

    #[tokio::test]
    async fn should_handle_nonexistent_label_across_buckets() {
        // given: data across multiple buckets, but without the requested label
        let storage = create_test_storage().await;
        let tsdb = Tsdb::new(storage);
        let bucket1 = TimeBucket::hour(60);
        let bucket2 = TimeBucket::hour(120);
        let mini1 = tsdb.get_or_create_for_ingest(bucket1).await.unwrap();
        let mini2 = tsdb.get_or_create_for_ingest(bucket2).await.unwrap();
        mini1
            .ingest(
                &create_sample("metric_a", vec![("env", "prod")], 4_000_000, 10.0),
                30,
            )
            .await
            .unwrap();
        mini2
            .ingest(
                &create_sample("metric_b", vec![("service", "api")], 8_000_000, 20.0),
                30,
            )
            .await
            .unwrap();
        tsdb.flush(30).await.unwrap();

        // when: query for a label that doesn't exist in any bucket
        let request = LabelValuesRequest {
            label_name: "nonexistent".to_string(),
            matches: None,
            start: Some(3600),
            end: Some(10800),
            limit: None,
        };
        let response = tsdb.label_values(request).await;

        // then: should return empty result
        assert_eq!(response.status, "success");
        let data = response.data.unwrap();
        assert!(data.is_empty());
    }
}
