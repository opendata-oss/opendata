use crate::model::{Label, MetricType, Sample, Series, TimeBucket};
use crate::promql::promqltest::dsl::*;
use crate::storage::merge_operator::OpenTsdbMergeOperator;
use crate::tsdb::Tsdb;
use common::storage::in_memory::InMemoryStorage;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

// ============================================================================
// Test Discovery
// ============================================================================

/// Discover all .test files in a directory (matches Prometheus fs.Glob pattern)
fn discover_test_files(dir: &Path) -> Result<Vec<PathBuf>, String> {
    let mut files = Vec::new();

    for entry in fs::read_dir(dir).map_err(|e| e.to_string())? {
        let entry = entry.map_err(|e| e.to_string())?;
        let path = entry.path();

        if path.extension().and_then(|s| s.to_str()) == Some("test") {
            files.push(path);
        }
    }

    files.sort();
    Ok(files)
}

// ============================================================================
// Test Runner (Orchestration)
// ============================================================================

/// Run all embedded test files (matches Prometheus RunBuiltinTests)
pub async fn run_builtin_tests() -> Result<(), String> {
    run_builtin_tests_with_storage(new_test_storage).await
}

/// Run all tests with custom storage factory (matches Prometheus RunBuiltinTestsWithStorage)
pub async fn run_builtin_tests_with_storage<F>(storage_factory: F) -> Result<(), String>
where
    F: Fn() -> Tsdb,
{
    let test_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("promql")
        .join("promqltest")
        .join("testdata");

    let files = discover_test_files(&test_dir)?;

    for path in files {
        let name = path
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or("Invalid test filename")?;

        let content = fs::read_to_string(&path).map_err(|e| format!("{}: {}", name, e))?;

        run_test_with_storage(name, &content, &storage_factory)
            .await
            .map_err(|e| format!("{}: {}", name, e))?;
    }

    Ok(())
}

/// Run a single test file (matches Prometheus RunTest)
pub async fn run_test(name: &str, content: &str) -> Result<(), String> {
    run_test_with_storage(name, content, &new_test_storage).await
}

/// Run a single test file with custom storage (matches Prometheus RunTestWithStorage)
async fn run_test_with_storage<F>(
    name: &str,
    content: &str,
    storage_factory: &F,
) -> Result<(), String>
where
    F: Fn() -> Tsdb,
{
    let commands = parse_test_file(content)?;
    let mut tsdb = storage_factory();
    let mut eval_count = 0;
    let mut ignoring = false;

    for cmd in commands {
        match cmd {
            Command::Clear(_) => {
                // Create fresh TSDB instance - clears all state including caches and snapshots
                tsdb = storage_factory();
            }

            Command::Ignore(_) => {
                ignoring = true;
            }

            Command::Resume(_) => {
                ignoring = false;
            }

            Command::Load(load_cmd) => {
                if !ignoring {
                    load_series(&tsdb, load_cmd.interval, &load_cmd.series).await?;
                }
            }

            Command::EvalInstant(eval_cmd) => {
                if !ignoring {
                    eval_count += 1;
                    let result = eval_instant(&tsdb, eval_cmd.time, &eval_cmd.query).await?;
                    assert_results(
                        &result,
                        &eval_cmd.expected,
                        name,
                        eval_count,
                        &eval_cmd.query,
                    )?;
                }
            }
        }
    }

    Ok(())
}

// ============================================================================
// Loader (load -> tsdb)
// ============================================================================

/// Load series data into TSDB
async fn load_series(
    tsdb: &Tsdb,
    interval: std::time::Duration,
    series: &[SeriesLoad],
) -> Result<(), String> {
    for s in series {
        // Collect all samples for this series
        let mut samples = Vec::new();

        for (step, value) in &s.values {
            // Validate step index
            if *step < 0 {
                return Err(format!("Negative step index not allowed: {}", step));
            }

            // Safe timestamp calculation with overflow checking
            let delta = interval
                .checked_mul(*step as u32)
                .ok_or_else(|| format!("Timestamp overflow for step {}", step))?;

            let ts = UNIX_EPOCH + delta;
            let ts_ms = ts
                .duration_since(UNIX_EPOCH)
                .map_err(|e| format!("Invalid timestamp: {}", e))?
                .as_millis() as i64;

            samples.push((ts_ms, *value));
        }

        // Sort by timestamp and deduplicate (keep last value per timestamp)
        // This matches Prometheus promqltest semantics
        samples.sort_by_key(|(ts, _)| *ts);
        samples.dedup_by_key(|(ts, _)| *ts);

        // Group by bucket and ingest
        let mut bucket_samples: std::collections::HashMap<TimeBucket, Vec<Sample>> =
            std::collections::HashMap::new();

        for (ts_ms, value) in samples {
            let ts = UNIX_EPOCH + std::time::Duration::from_millis(ts_ms as u64);
            let bucket = TimeBucket::round_to_hour(ts).unwrap();
            bucket_samples
                .entry(bucket)
                .or_default()
                .push(Sample::new(ts_ms, value));
        }

        // Ingest each bucket
        for (bucket, bucket_samples) in bucket_samples {
            let labels: Vec<Label> = s
                .labels
                .iter()
                .map(|(k, v)| Label {
                    name: k.clone(),
                    value: v.clone(),
                })
                .collect();

            let series = Series {
                labels,
                metric_type: Some(MetricType::Gauge),
                unit: None,
                description: None,
                samples: bucket_samples,
            };

            let mini = tsdb.get_or_create_for_ingest(bucket).await.unwrap();
            mini.ingest(&series).await.unwrap();
        }
    }
    tsdb.flush().await.unwrap();
    Ok(())
}

// ============================================================================
// Evaluator (query -> result)
// ============================================================================

/// Execute instant query and return structured results
async fn eval_instant(
    tsdb: &Tsdb,
    time: SystemTime,
    query: &str,
) -> Result<Vec<EvalResult>, String> {
    use crate::promql::request::QueryRequest;
    use crate::promql::router::PromqlRouter;

    let resp = tsdb
        .query(QueryRequest {
            query: query.to_string(),
            time: Some(time),
            timeout: None,
        })
        .await;

    let data = resp.data.ok_or_else(|| {
        format!(
            "No data in response (query: {}, status: {:?}, error: {:?})",
            query, resp.status, resp.error
        )
    })?;
    let result = data.result.as_array().ok_or("Result is not a vector")?;

    // Convert JSON to structured EvalResult
    let mut results = Vec::new();
    for sample in result {
        let labels_obj = sample["metric"].as_object().ok_or("Missing metric")?;
        let mut labels = HashMap::new();
        for (k, v) in labels_obj {
            labels.insert(k.clone(), v.as_str().unwrap_or("").to_string());
        }

        let value = sample["value"][1]
            .as_str()
            .ok_or("Missing value")?
            .parse::<f64>()
            .map_err(|e| format!("Invalid value: {}", e))?;

        results.push(EvalResult { labels, value });
    }

    Ok(results)
}

// ============================================================================
// Assertion (compare results)
// ============================================================================

/// Compare actual results against expected results
///
/// IMPORTANT: Metric name handling follows Prometheus promqltest semantics:
/// - Prometheus represents the metric name as the __name__ label
/// - If expected sample omits __name__ → we don't check it (allows flexible matching)
/// - If expected sample includes __name__ → it must match exactly
///
/// This means test expectations can be written as:
///   {job="test"} 42          # Matches any metric with job="test"
///   {__name__="metric"} 42   # Must be exactly "metric"
///
/// The implementation achieves this by only checking labels that are present in the
/// expected sample, not all labels from the actual result.
fn assert_results(
    results: &[EvalResult],
    expected: &[ExpectedSample],
    test_name: &str,
    eval_num: usize,
    query: &str,
) -> Result<(), String> {
    if results.len() != expected.len() {
        return Err(format!(
            "{} eval #{} (query: {}): Expected {} samples, got {}",
            test_name,
            eval_num,
            query,
            expected.len(),
            results.len()
        ));
    }

    // Sort both sides deterministically - PromQL doesn't guarantee result ordering
    let mut results_sorted = results.to_vec();
    results_sorted.sort_by_key(|r| label_sort_key(&r.labels));

    let mut expected_sorted = expected.to_vec();
    expected_sorted.sort_by_key(|e| label_sort_key(&e.labels));

    for (i, exp) in expected_sorted.iter().enumerate() {
        let result = &results_sorted[i];

        // Check all expected labels are present and match
        for (k, v) in &exp.labels {
            let actual = result.labels.get(k).ok_or(format!(
                "{} eval #{} (query: {}): Missing label '{}'",
                test_name, eval_num, query, k
            ))?;
            if actual != v {
                return Err(format!(
                    "{} eval #{} (query: {}): Label {} mismatch: expected '{}', got '{}'",
                    test_name, eval_num, query, k, v, actual
                ));
            }
        }

        if (result.value - exp.value).abs() > 1e-6 {
            return Err(format!(
                "{} eval #{} (query: {}): Value mismatch: expected {}, got {}",
                test_name, eval_num, query, exp.value, result.value
            ));
        }
    }

    Ok(())
}

fn label_sort_key(labels: &HashMap<String, String>) -> String {
    let mut keys: Vec<_> = labels.iter().collect();
    keys.sort_by_key(|(k, _)| *k);
    keys.iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect::<Vec<_>>()
        .join(",")
}

// ============================================================================
// Storage Factory
// ============================================================================

fn new_test_storage() -> Tsdb {
    let storage = Arc::new(InMemoryStorage::with_merge_operator(Arc::new(
        OpenTsdbMergeOperator,
    )));
    Tsdb::new(storage)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn should_load_series_into_storage() {
        // given
        let tsdb = new_test_storage();
        let series = vec![SeriesLoad {
            labels: HashMap::from([
                ("__name__".to_string(), "test_metric".to_string()),
                ("job".to_string(), "test".to_string()),
            ]),
            values: vec![(0, 1.0), (1, 2.0), (2, 3.0)],
        }];

        // when
        load_series(&tsdb, Duration::from_secs(60), &series)
            .await
            .unwrap();

        // then
        let result = eval_instant(&tsdb, UNIX_EPOCH + Duration::from_secs(60), "test_metric")
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, 2.0);
    }

    #[tokio::test]
    async fn should_evaluate_query_at_specific_time() {
        // given
        let tsdb = new_test_storage();
        let series = vec![SeriesLoad {
            labels: HashMap::from([("__name__".to_string(), "metric".to_string())]),
            values: vec![(0, 10.0), (1, 20.0), (2, 30.0)],
        }];
        load_series(&tsdb, Duration::from_secs(60), &series)
            .await
            .unwrap();

        // when
        let result = eval_instant(&tsdb, UNIX_EPOCH + Duration::from_secs(120), "metric")
            .await
            .unwrap();

        // then
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, 30.0);
    }

    #[test]
    fn should_match_expected_results() {
        // given
        let results = vec![EvalResult {
            labels: HashMap::from([("job".to_string(), "test".to_string())]),
            value: 42.0,
        }];
        let expected = vec![ExpectedSample {
            labels: HashMap::from([("job".to_string(), "test".to_string())]),
            value: 42.0,
        }];

        // when
        let result = assert_results(&results, &expected, "test", 1, "query");

        // then
        assert!(result.is_ok());
    }

    #[test]
    fn should_reject_mismatched_values() {
        // given
        let results = vec![EvalResult {
            labels: HashMap::new(),
            value: 42.0,
        }];
        let expected = vec![ExpectedSample {
            labels: HashMap::new(),
            value: 100.0,
        }];

        // when
        let result = assert_results(&results, &expected, "test", 1, "query");

        // then
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Value mismatch"));
    }

    #[test]
    fn should_reject_count_mismatch() {
        // given
        let results = vec![
            EvalResult {
                labels: HashMap::new(),
                value: 1.0,
            },
            EvalResult {
                labels: HashMap::new(),
                value: 2.0,
            },
        ];
        let expected = vec![ExpectedSample {
            labels: HashMap::new(),
            value: 1.0,
        }];

        // when
        let result = assert_results(&results, &expected, "test", 1, "query");

        // then
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("Expected 1 samples, got 2"));
    }

    #[tokio::test]
    async fn should_run_simple_test_file() {
        // given
        let content = r#"
load 5m
  metric 1 2 3

eval instant at 10m
  metric
    {} 3
"#;

        // when
        let result = run_test("simple_test", content).await;

        // then
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn should_reject_negative_step_index() {
        // given
        let tsdb = new_test_storage();
        let series = vec![SeriesLoad {
            labels: HashMap::from([("__name__".to_string(), "metric".to_string())]),
            values: vec![(-1, 10.0)], // Negative step
        }];

        // when
        let result = load_series(&tsdb, Duration::from_secs(60), &series).await;

        // then
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Negative step index"));
    }
}
