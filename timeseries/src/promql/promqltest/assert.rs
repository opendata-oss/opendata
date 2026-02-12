use crate::promql::promqltest::dsl::{EvalResult, ExpectedSample};
use std::collections::HashMap;

/// Compare actual results against expected results
///
/// IMPORTANT: Metric name handling follows Prometheus promqltest semantics:
/// - Prometheus represents the metric name as the __name__ label
/// - If expected sample omits __name__ → we don't check it (allows flexible matching)
/// - If expected sample includes __name__ → we verify it matches
///
/// This means test expectations can be written as:
///   {job="test"} 42          # Matches any metric with job="test"
///   {__name__="metric"} 42   # Must be exactly "metric"
///
/// The implementation achieves this by only checking labels that are present in the
/// expected sample, not all labels from the actual result.
pub(super) fn assert_results(
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

#[cfg(test)]
mod tests {
    use super::*;

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
        let result = assert_results(&results, &expected, "test", 1, "metric");

        // then
        assert!(result.is_ok());
    }

    #[test]
    fn should_reject_count_mismatch() {
        // given
        let results = vec![EvalResult {
            labels: HashMap::new(),
            value: 42.0,
        }];
        let expected = vec![];

        // when
        let result = assert_results(&results, &expected, "test", 1, "metric");

        // then
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Expected 0 samples, got 1"));
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
            value: 99.0,
        }];

        // when
        let result = assert_results(&results, &expected, "test", 1, "metric");

        // then
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Value mismatch"));
    }
}
