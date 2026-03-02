use crate::model::{Labels, QueryOptions, QueryValue};
use crate::promql::promqltest::dsl::EvalResult;
use crate::tsdb::Tsdb;
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
            let results = samples
                .into_iter()
                .map(|sample| EvalResult {
                    labels: sample.labels,
                    value: sample.value,
                })
                .collect();
            Ok(results)
        }
        QueryValue::Scalar { value, .. } => Ok(vec![EvalResult {
            labels: Labels::empty(),
            value,
        }]),
        QueryValue::Matrix(range_samples) => {
            let results = range_samples
                .into_iter()
                .flat_map(|rs| {
                    rs.samples.into_iter().map(move |(_, val)| EvalResult {
                        labels: rs.labels.clone(),
                        value: val,
                    })
                })
                .collect();
            Ok(results)
        }
    }
}
