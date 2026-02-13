use crate::promql::promqltest::assert::assert_results;
use crate::promql::promqltest::dsl::*;
use crate::promql::promqltest::evaluator::eval_instant;
use crate::promql::promqltest::loader::load_series;
use crate::storage::merge_operator::OpenTsdbMergeOperator;
use crate::tsdb::Tsdb;
use common::storage::in_memory::InMemoryStorage;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::UNIX_EPOCH;

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
async fn run_builtin_tests() -> Result<(), String> {
    run_builtin_tests_with_storage(new_test_storage).await
}

/// Run all tests with custom storage factory (matches Prometheus RunBuiltinTestsWithStorage)
async fn run_builtin_tests_with_storage<F>(storage_factory: F) -> Result<(), String>
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
