use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common::storage::in_memory::InMemoryStorage;

use crate::model::{
    InstantSample, Label, Labels, MetricType, QueryOptions, RangeSample, Sample, Series,
    Temporality,
};
use crate::storage::merge_operator::OpenTsdbMergeOperator;
use crate::tsdb::Tsdb;

const BUCKET_MS: i64 = 60 * 60 * 1000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SeriesKind {
    Persistent,
    HourLocal { bucket_idx: usize },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ScenarioConfig {
    pub bucket_count: usize,
    pub persistent_series: usize,
    pub hour_local_series_per_bucket: usize,
    pub application_count: usize,
    pub base_start_ms: i64,
    pub scrape_interval_ms: i64,
    pub step_ms: i64,
    pub lookback_ms: i64,
    pub rate_window_ms: i64,
    pub unflushed_tail_buckets: usize,
}

impl ScenarioConfig {
    pub fn regression() -> Self {
        Self {
            bucket_count: 8,
            persistent_series: 1025,
            hour_local_series_per_bucket: 256,
            application_count: 16,
            base_start_ms: BUCKET_MS,
            scrape_interval_ms: 60 * 1000,
            step_ms: 60 * 1000,
            lookback_ms: 5 * 60 * 1000,
            rate_window_ms: 15 * 60 * 1000,
            unflushed_tail_buckets: 0,
        }
    }

    pub fn soak() -> Self {
        Self {
            bucket_count: 24,
            persistent_series: 1025,
            hour_local_series_per_bucket: 768,
            application_count: 16,
            base_start_ms: BUCKET_MS,
            scrape_interval_ms: 60 * 1000,
            step_ms: 60 * 1000,
            lookback_ms: 5 * 60 * 1000,
            rate_window_ms: 15 * 60 * 1000,
            unflushed_tail_buckets: 0,
        }
    }

    pub fn per_bucket_series(&self) -> usize {
        self.persistent_series + self.hour_local_series_per_bucket
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SeriesSpec {
    pub series_ordinal: usize,
    pub application_idx: usize,
    pub kind: SeriesKind,
    pub start_ms: i64,
    pub end_ms: i64,
    pub gauge_weight: u64,
    pub counter_inc_per_scrape: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct Scenario {
    pub config: ScenarioConfig,
    pub specs: Vec<SeriesSpec>,
    pub app_names: Vec<String>,
    pub query_start_ms: i64,
    pub query_end_ms: i64,
    pub step_timestamps: Vec<i64>,
    pub probe_timestamps: Vec<i64>,
}

#[derive(Debug, Clone)]
pub(crate) struct Oracle {
    pub count_by_app: Vec<Vec<u32>>,
    pub sum_by_app: Vec<Vec<u64>>,
    pub rate_by_app: Vec<Vec<Option<f64>>>,
}

pub(crate) fn build_scenario(config: ScenarioConfig) -> Scenario {
    let bucket_count = config.bucket_count;
    let last_bucket_end_ms = config.base_start_ms + bucket_count as i64 * BUCKET_MS;
    let last_sample_ms = last_bucket_end_ms - config.scrape_interval_ms;

    let app_names: Vec<String> = (0..config.application_count)
        .map(|idx| format!("app-{idx:02}"))
        .collect();

    let mut specs = Vec::with_capacity(
        config.persistent_series + bucket_count * config.hour_local_series_per_bucket,
    );

    for ordinal in 0..config.persistent_series {
        let application_idx = ordinal % config.application_count;
        specs.push(SeriesSpec {
            series_ordinal: ordinal,
            application_idx,
            kind: SeriesKind::Persistent,
            start_ms: config.base_start_ms,
            end_ms: last_sample_ms,
            gauge_weight: 10_000 + ordinal as u64,
            counter_inc_per_scrape: 60 + ((ordinal % 5) as u64 * 15),
        });
    }

    let mut ordinal = config.persistent_series;
    for bucket_idx in 0..bucket_count {
        let bucket_start_ms = config.base_start_ms + bucket_idx as i64 * BUCKET_MS;
        let bucket_end_ms = bucket_start_ms + BUCKET_MS;
        let bucket_last_sample_ms = bucket_end_ms - config.scrape_interval_ms;
        for local_idx in 0..config.hour_local_series_per_bucket {
            let application_idx = (ordinal + local_idx) % config.application_count;
            specs.push(SeriesSpec {
                series_ordinal: ordinal,
                application_idx,
                kind: SeriesKind::HourLocal { bucket_idx },
                start_ms: bucket_start_ms,
                end_ms: bucket_last_sample_ms,
                gauge_weight: 100 + bucket_idx as u64 * 1_000 + local_idx as u64,
                counter_inc_per_scrape: 60 + ((ordinal % 5) as u64 * 15),
            });
            ordinal += 1;
        }
    }

    let query_start_ms = config.base_start_ms;
    let query_end_ms = last_bucket_end_ms + config.scrape_interval_ms;
    let step_timestamps = (query_start_ms..=query_end_ms)
        .step_by(config.step_ms as usize)
        .collect();

    let probe_timestamps = probe_timestamps(&config);

    Scenario {
        config,
        specs,
        app_names,
        query_start_ms,
        query_end_ms,
        step_timestamps,
        probe_timestamps,
    }
}

pub(crate) fn build_oracle(scenario: &Scenario) -> Oracle {
    let app_count = scenario.app_names.len();
    let mut count_by_app = vec![vec![0u32; app_count]; scenario.step_timestamps.len()];
    let mut sum_by_app = vec![vec![0u64; app_count]; scenario.step_timestamps.len()];
    let mut rate_by_app = vec![vec![None; app_count]; scenario.step_timestamps.len()];

    for (step_idx, &timestamp_ms) in scenario.step_timestamps.iter().enumerate() {
        for spec in &scenario.specs {
            if let Some(last_scrape_ms) =
                latest_scrape_at_or_before(spec, timestamp_ms, scenario.config.scrape_interval_ms)
                && last_scrape_ms > timestamp_ms - scenario.config.lookback_ms
            {
                count_by_app[step_idx][spec.application_idx] += 1;
                sum_by_app[step_idx][spec.application_idx] += spec.gauge_weight;
            }

            if let Some(rate) = prom_extrapolated_rate(
                spec,
                timestamp_ms,
                scenario.config.rate_window_ms,
                scenario.config.scrape_interval_ms,
            ) {
                let slot = &mut rate_by_app[step_idx][spec.application_idx];
                *slot = Some(slot.unwrap_or(0.0) + rate);
            }
        }
    }

    Oracle {
        count_by_app,
        sum_by_app,
        rate_by_app,
    }
}

pub(crate) async fn create_tsdb_for_scenario(scenario: &Scenario) -> Tsdb {
    let tsdb = Tsdb::new(Arc::new(InMemoryStorage::with_merge_operator(Arc::new(
        OpenTsdbMergeOperator,
    ))));

    for bucket_idx in 0..scenario.config.bucket_count {
        let bucket_series = build_bucket_series(scenario, bucket_idx);
        tsdb.ingest_samples(bucket_series, None).await.unwrap();
        if bucket_idx + scenario.config.unflushed_tail_buckets < scenario.config.bucket_count {
            tsdb.flush().await.unwrap();
        }
    }

    tsdb
}

pub(crate) fn concurrency_profiles() -> [(&'static str, QueryOptions); 3] {
    [
        (
            "serial",
            QueryOptions {
                metadata_concurrency: 1,
                sample_concurrency: 1,
                ..Default::default()
            },
        ),
        ("default", QueryOptions::default()),
        (
            "parallel",
            QueryOptions {
                metadata_concurrency: 128,
                sample_concurrency: 512,
                ..Default::default()
            },
        ),
    ]
}

pub(crate) fn query_start_time(scenario: &Scenario) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(scenario.query_start_ms as u64)
}

pub(crate) fn query_end_time(scenario: &Scenario) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(scenario.query_end_ms as u64)
}

pub(crate) fn query_step(scenario: &Scenario) -> Duration {
    Duration::from_millis(scenario.config.step_ms as u64)
}

pub(crate) fn raw_selector_cardinality_at(scenario: &Scenario, timestamp_ms: i64) -> usize {
    scenario
        .specs
        .iter()
        .filter(|spec| {
            latest_scrape_at_or_before(spec, timestamp_ms, scenario.config.scrape_interval_ms)
                .is_some_and(|last_scrape_ms| {
                    last_scrape_ms > timestamp_ms - scenario.config.lookback_ms
                })
        })
        .count()
}

pub(crate) fn assert_raw_selector_cardinality(
    samples: &[InstantSample],
    scenario: &Scenario,
    timestamp_ms: i64,
) {
    let expected = raw_selector_cardinality_at(scenario, timestamp_ms);
    assert_eq!(
        samples.len(),
        expected,
        "raw selector cardinality mismatch at {}",
        timestamp_ms
    );
}

pub(crate) fn assert_grouped_count_vector(
    samples: &[InstantSample],
    scenario: &Scenario,
    oracle: &Oracle,
    timestamp_ms: i64,
) {
    let actual = normalize_vector_by_app(samples);
    let step_idx = step_index(scenario, timestamp_ms);
    assert_eq!(
        actual.len(),
        scenario.app_names.len(),
        "count vector group cardinality mismatch at {}",
        timestamp_ms
    );

    for (app_idx, app_name) in scenario.app_names.iter().enumerate() {
        let actual_value = actual
            .get(app_name)
            .copied()
            .unwrap_or_else(|| panic!("missing app {app_name} in count vector"));
        let expected = oracle.count_by_app[step_idx][app_idx] as f64;
        assert_eq!(
            actual_value, expected,
            "count mismatch for {app_name} at {}",
            timestamp_ms
        );
    }
}

pub(crate) fn assert_grouped_sum_vector(
    samples: &[InstantSample],
    scenario: &Scenario,
    oracle: &Oracle,
    timestamp_ms: i64,
) {
    let actual = normalize_vector_by_app(samples);
    let step_idx = step_index(scenario, timestamp_ms);
    assert_eq!(
        actual.len(),
        scenario.app_names.len(),
        "sum vector group cardinality mismatch at {}",
        timestamp_ms
    );

    for (app_idx, app_name) in scenario.app_names.iter().enumerate() {
        let actual_value = actual
            .get(app_name)
            .copied()
            .unwrap_or_else(|| panic!("missing app {app_name} in sum vector"));
        let expected = oracle.sum_by_app[step_idx][app_idx] as f64;
        assert_eq!(
            actual_value, expected,
            "sum mismatch for {app_name} at {}",
            timestamp_ms
        );
    }
}

pub(crate) fn assert_grouped_count_matrix(
    matrix: &[RangeSample],
    scenario: &Scenario,
    oracle: &Oracle,
) {
    let actual = normalize_matrix_by_app(matrix);
    assert_eq!(
        actual.len(),
        scenario.app_names.len(),
        "count matrix group cardinality mismatch",
    );

    for (app_idx, app_name) in scenario.app_names.iter().enumerate() {
        let actual_points = actual
            .get(app_name)
            .unwrap_or_else(|| panic!("missing app {app_name} in count matrix"));
        let expected_points: Vec<(i64, f64)> = scenario
            .step_timestamps
            .iter()
            .enumerate()
            .map(|(step_idx, &timestamp_ms)| {
                (timestamp_ms, oracle.count_by_app[step_idx][app_idx] as f64)
            })
            .collect();
        assert_eq!(
            actual_points, &expected_points,
            "count range mismatch for {app_name}",
        );
    }
}

pub(crate) fn assert_grouped_sum_matrix(
    matrix: &[RangeSample],
    scenario: &Scenario,
    oracle: &Oracle,
) {
    let actual = normalize_matrix_by_app(matrix);
    assert_eq!(
        actual.len(),
        scenario.app_names.len(),
        "sum matrix group cardinality mismatch",
    );

    for (app_idx, app_name) in scenario.app_names.iter().enumerate() {
        let actual_points = actual
            .get(app_name)
            .unwrap_or_else(|| panic!("missing app {app_name} in sum matrix"));
        let expected_points: Vec<(i64, f64)> = scenario
            .step_timestamps
            .iter()
            .enumerate()
            .map(|(step_idx, &timestamp_ms)| {
                (timestamp_ms, oracle.sum_by_app[step_idx][app_idx] as f64)
            })
            .collect();
        assert_eq!(
            actual_points, &expected_points,
            "sum range mismatch for {app_name}",
        );
    }
}

pub(crate) fn assert_grouped_rate_matrix(
    matrix: &[RangeSample],
    scenario: &Scenario,
    oracle: &Oracle,
) {
    let actual = normalize_matrix_by_app(matrix);
    assert_eq!(
        actual.len(),
        scenario.app_names.len(),
        "rate matrix group cardinality mismatch",
    );

    for (app_idx, app_name) in scenario.app_names.iter().enumerate() {
        let actual_points = actual
            .get(app_name)
            .unwrap_or_else(|| panic!("missing app {app_name} in rate matrix"));
        let expected_points: Vec<(i64, f64)> = scenario
            .step_timestamps
            .iter()
            .enumerate()
            .filter_map(|(step_idx, &timestamp_ms)| {
                oracle.rate_by_app[step_idx][app_idx].map(|value| (timestamp_ms, value))
            })
            .collect();
        let expected_timestamps: BTreeSet<i64> =
            expected_points.iter().map(|(ts, _)| *ts).collect();
        let filtered_actual: Vec<(i64, f64)> = actual_points
            .iter()
            .copied()
            .filter(|(timestamp_ms, _)| expected_timestamps.contains(timestamp_ms))
            .collect();

        assert_eq!(
            filtered_actual.len(),
            expected_points.len(),
            "rate point count mismatch for {app_name}",
        );

        for ((actual_ts, actual_value), (expected_ts, expected_value)) in
            filtered_actual.iter().zip(expected_points.iter())
        {
            assert_eq!(
                actual_ts, expected_ts,
                "rate timestamp mismatch for {app_name}",
            );
            let delta = (actual_value - expected_value).abs();
            assert!(
                delta < 1e-9,
                "rate mismatch for {app_name} at {}: actual={} expected={} delta={}",
                actual_ts,
                actual_value,
                expected_value,
                delta
            );
        }
    }
}

pub(crate) fn app_group_labels(app_name: &str) -> Labels {
    Labels::new(vec![Label::new("applicationid", app_name)])
}

fn probe_timestamps(config: &ScenarioConfig) -> Vec<i64> {
    let mut bucket_indices = BTreeSet::new();
    if config.bucket_count > 2 {
        bucket_indices.insert(1usize);
        bucket_indices.insert(config.bucket_count / 2);
        bucket_indices.insert(config.bucket_count - 2);
    }

    let mut timestamps = BTreeSet::new();
    for bucket_idx in bucket_indices {
        let bucket_end_ms = config.base_start_ms + (bucket_idx as i64 + 1) * BUCKET_MS;
        timestamps.insert(bucket_end_ms - config.scrape_interval_ms);
        timestamps.insert(bucket_end_ms);
        timestamps.insert(bucket_end_ms + config.scrape_interval_ms);
        timestamps.insert(bucket_end_ms + config.lookback_ms + config.scrape_interval_ms);
    }
    timestamps.into_iter().collect()
}

/// Mirrors the engine's `extrapolated_rate` (see `promql::operators::rollup`)
/// for a single synthetic series. The oracle must replicate extrapolation
/// because, at bucket boundaries, "dying" hour-local series still have
/// samples in the 15m rate window and Prometheus extrapolates over their
/// partial coverage.
fn prom_extrapolated_rate(
    spec: &SeriesSpec,
    timestamp_ms: i64,
    rate_window_ms: i64,
    scrape_interval_ms: i64,
) -> Option<f64> {
    let window_start_ms = timestamp_ms - rate_window_ms;
    let window_end_ms = timestamp_ms;

    // Samples the series has in the window (window_start_ms, window_end_ms].
    // Series samples are at `spec.start_ms + k*scrape_interval_ms`, for
    // k = 0.. while that timestamp <= spec.end_ms. Values are strictly
    // monotonic (no counter resets in this fixture).
    let effective_hi = window_end_ms.min(spec.end_ms);
    if effective_hi < spec.start_ms {
        return None;
    }
    let k_lo = if window_start_ms < spec.start_ms {
        0
    } else {
        (window_start_ms - spec.start_ms) / scrape_interval_ms + 1
    };
    let first_sample_ts = spec.start_ms + k_lo * scrape_interval_ms;
    if first_sample_ts > effective_hi {
        return None;
    }
    let k_hi = (effective_hi - spec.start_ms) / scrape_interval_ms;
    let n = (k_hi - k_lo + 1) as usize;
    if n < 2 {
        return None;
    }

    let first_t = first_sample_ts;
    let last_t = spec.start_ms + k_hi * scrape_interval_ms;
    let first_v = k_lo as f64 * spec.counter_inc_per_scrape as f64;
    let last_v = k_hi as f64 * spec.counter_inc_per_scrape as f64;

    let range_seconds = rate_window_ms as f64 / 1000.0;
    let time_diff_seconds = (last_t - first_t) as f64 / 1000.0;
    if time_diff_seconds <= 0.0 {
        return None;
    }

    let result = last_v - first_v;
    let avg_interval = time_diff_seconds / (n - 1) as f64;
    let extrapolation_threshold = avg_interval * 1.1;

    let mut duration_to_start = (first_t - window_start_ms) as f64 / 1000.0;
    let mut duration_to_end = (window_end_ms - last_t) as f64 / 1000.0;

    if duration_to_start >= extrapolation_threshold {
        duration_to_start = avg_interval / 2.0;
    }
    if result > 0.0 && first_v >= 0.0 {
        let duration_to_zero = first_v * (time_diff_seconds / result);
        if duration_to_zero < duration_to_start {
            duration_to_start = duration_to_zero;
        }
    }
    if duration_to_end >= extrapolation_threshold {
        duration_to_end = avg_interval / 2.0;
    }

    let factor = (time_diff_seconds + duration_to_start + duration_to_end) / time_diff_seconds;
    Some(result * factor / range_seconds)
}

fn latest_scrape_at_or_before(
    spec: &SeriesSpec,
    timestamp_ms: i64,
    scrape_interval_ms: i64,
) -> Option<i64> {
    if timestamp_ms < spec.start_ms {
        return None;
    }
    let capped = timestamp_ms.min(spec.end_ms);
    if capped < spec.start_ms {
        return None;
    }
    let steps = (capped - spec.start_ms) / scrape_interval_ms;
    Some(spec.start_ms + steps * scrape_interval_ms)
}

fn build_bucket_series(scenario: &Scenario, bucket_idx: usize) -> Vec<Series> {
    let bucket_start_ms = scenario.config.base_start_ms + bucket_idx as i64 * BUCKET_MS;
    let bucket_end_ms = bucket_start_ms + BUCKET_MS;
    let bucket_last_sample_ms = bucket_end_ms - scenario.config.scrape_interval_ms;

    let mut out = Vec::with_capacity(
        (scenario.config.persistent_series + scenario.config.hour_local_series_per_bucket) * 2,
    );
    for spec in &scenario.specs {
        let start_ms = spec.start_ms.max(bucket_start_ms);
        let end_ms = spec.end_ms.min(bucket_last_sample_ms);
        if start_ms > end_ms {
            continue;
        }

        let base_labels = labels_for_spec(scenario, spec);
        let gauge_samples =
            samples_for_bucket(start_ms, end_ms, scenario.config.scrape_interval_ms)
                .map(|timestamp_ms| Sample::new(timestamp_ms, spec.gauge_weight as f64))
                .collect();
        let counter_samples =
            samples_for_bucket(start_ms, end_ms, scenario.config.scrape_interval_ms)
                .map(|timestamp_ms| {
                    let scrape_idx =
                        (timestamp_ms - spec.start_ms) / scenario.config.scrape_interval_ms;
                    let value = scrape_idx as f64 * spec.counter_inc_per_scrape as f64;
                    Sample::new(timestamp_ms, value)
                })
                .collect();

        out.push(Series::new(
            "stress_gauge",
            base_labels.clone(),
            gauge_samples,
        ));
        let mut counter = Series::new("stress_counter_total", base_labels, counter_samples);
        counter.metric_type = Some(MetricType::Sum {
            monotonic: true,
            temporality: Temporality::Cumulative,
        });
        out.push(counter);
    }
    out
}

fn samples_for_bucket(
    start_ms: i64,
    end_ms: i64,
    scrape_interval_ms: i64,
) -> impl Iterator<Item = i64> {
    (start_ms..=end_ms).step_by(scrape_interval_ms as usize)
}

fn labels_for_spec(scenario: &Scenario, spec: &SeriesSpec) -> Vec<Label> {
    let mut labels = vec![
        Label::new(
            "applicationid",
            scenario.app_names[spec.application_idx].clone(),
        ),
        Label::new("series", format!("series-{:05}", spec.series_ordinal)),
    ];
    match spec.kind {
        SeriesKind::Persistent => {
            labels.push(Label::new("kind", "persistent"));
        }
        SeriesKind::HourLocal { bucket_idx } => {
            labels.push(Label::new("kind", "hour_local"));
            labels.push(Label::new("home_bucket", bucket_idx.to_string()));
        }
    }
    labels
}

fn normalize_vector_by_app(samples: &[InstantSample]) -> BTreeMap<String, f64> {
    let mut out = BTreeMap::new();
    for sample in samples {
        let app_name = sample
            .labels
            .get("applicationid")
            .unwrap_or_else(|| panic!("missing applicationid in {:?}", sample.labels))
            .to_string();
        out.insert(app_name, sample.value);
    }
    out
}

fn normalize_matrix_by_app(matrix: &[RangeSample]) -> BTreeMap<String, Vec<(i64, f64)>> {
    let mut out = BTreeMap::new();
    for series in matrix {
        let app_name = series
            .labels
            .get("applicationid")
            .unwrap_or_else(|| panic!("missing applicationid in {:?}", series.labels))
            .to_string();
        let mut samples = series.samples.clone();
        samples.sort_by_key(|(timestamp_ms, _)| *timestamp_ms);
        out.insert(app_name, samples);
    }
    out
}

fn step_index(scenario: &Scenario, timestamp_ms: i64) -> usize {
    scenario
        .step_timestamps
        .iter()
        .position(|&step_ts| step_ts == timestamp_ms)
        .unwrap_or_else(|| panic!("timestamp {} not in scenario step grid", timestamp_ms))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_build_regression_scenario_with_more_than_1024_series_per_bucket() {
        // given
        let scenario = build_scenario(ScenarioConfig::regression());
        let bucket_midpoint_ms = scenario.config.base_start_ms + BUCKET_MS / 2;

        // when
        let cardinality = raw_selector_cardinality_at(&scenario, bucket_midpoint_ms);

        // then
        assert_eq!(scenario.config.per_bucket_series(), 1281);
        assert_eq!(cardinality, 1281);
    }

    #[test]
    fn should_keep_hour_local_series_visible_until_lookback_expires() {
        // given
        let scenario = build_scenario(ScenarioConfig::regression());
        let oracle = build_oracle(&scenario);
        let bucket_idx = 1usize;
        let bucket_end_ms = scenario.config.base_start_ms + (bucket_idx as i64 + 1) * BUCKET_MS;

        // when
        let at_bucket_end = step_index(&scenario, bucket_end_ms);
        let after_expiry = step_index(
            &scenario,
            bucket_end_ms + scenario.config.lookback_ms + scenario.config.scrape_interval_ms,
        );

        // then
        let before = oracle.count_by_app[at_bucket_end]
            .iter()
            .copied()
            .sum::<u32>();
        let after = oracle.count_by_app[after_expiry]
            .iter()
            .copied()
            .sum::<u32>();
        assert!(
            before > after,
            "hour-local series should age out after lookback: before={} after={}",
            before,
            after
        );
        assert_eq!(
            after,
            scenario.config.persistent_series as u32
                + scenario.config.hour_local_series_per_bucket as u32,
            "after expiry only persistent + next bucket hour-local series should remain visible",
        );
    }

    #[test]
    fn should_mark_rate_only_on_safe_interior_steps() {
        // given
        let scenario = build_scenario(ScenarioConfig::regression());
        let oracle = build_oracle(&scenario);

        // when
        let first_step = &oracle.rate_by_app[0];
        let first_safe_step_idx =
            (scenario.config.rate_window_ms / scenario.config.step_ms) as usize;
        let first_safe_step = &oracle.rate_by_app[first_safe_step_idx];

        // then
        assert!(first_step.iter().all(|value| value.is_none()));
        assert!(first_safe_step.iter().all(|value| value.is_some()));
    }
}
