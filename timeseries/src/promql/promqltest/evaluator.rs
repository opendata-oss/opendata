use crate::model::{QueryOptions, QueryValue};
use crate::promql::promqltest::dsl::EvalResult;
use crate::tsdb::Tsdb;
use std::collections::HashMap;
use std::time::SystemTime;

/// Execute instant query and return structured results
pub(super) async fn eval_instant(
    tsdb: &Tsdb,
    time: SystemTime,
    query: &str,
) -> Result<Vec<EvalResult>, String> {
    let result = tsdb
        .eval_query(query, Some(time), &QueryOptions::default())
        .await
        .map_err(|e| e.to_string())?;

    match result {
        QueryValue::Vector(samples) => {
            let mut results = Vec::new();
            for sample in samples {
                let labels: HashMap<String, String> = sample.labels.into();
                results.push(EvalResult {
                    labels,
                    value: sample.value,
                });
            }
            Ok(results)
        }
        QueryValue::Scalar { value, .. } => Ok(vec![EvalResult {
            labels: HashMap::new(),
            value,
        }]),
    }
}
