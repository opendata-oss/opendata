use crate::model::{QueryOptions, RangeSample};
use crate::tsdb::{Tsdb, TsdbReadEngine};
use std::time::SystemTime;

/// Execute instant query and return structured results
pub(super) async fn eval_instant(
    tsdb: &Tsdb,
    time: SystemTime,
    query: &str,
) -> Result<Vec<RangeSample>, String> {
    tsdb.eval_query(query, Some(time), &QueryOptions::default())
        .await
        .map_err(|e| e.to_string())
        .map(|qv| qv.into_matrix())
}
