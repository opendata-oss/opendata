use crate::model::{Label, MetricType, Sample, Series, TimeBucket};
use crate::promql::promqltest::dsl::SeriesLoad;
use crate::tsdb::Tsdb;
use std::time::UNIX_EPOCH;

/// Load series data into TSDB
pub(super) async fn load_series(
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
