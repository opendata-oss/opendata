use crate::promql::promqltest::dsl::EvalResult;
use crate::promql::request::QueryRequest;
use crate::promql::router::PromqlRouter;
use crate::tsdb::Tsdb;
use std::collections::HashMap;
use std::time::SystemTime;

/// Execute instant query and return structured results
pub(super) async fn eval_instant(
    tsdb: &Tsdb,
    time: SystemTime,
    query: &str,
) -> Result<Vec<EvalResult>, String> {
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
