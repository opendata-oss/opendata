//! OpenTelemetry metrics to Series conversion.
//!
//! This module provides [`OtelConverter`], which converts an OTLP
//! `ExportMetricsServiceRequest` into `Vec<Series>` suitable for
//! [`TimeSeriesDb::write()`](crate::TimeSeriesDb::write).

use opentelemetry_proto::tonic::{
    collector::metrics::v1::ExportMetricsServiceRequest,
    common::v1::{KeyValue, any_value},
    metrics::v1::{AggregationTemporality, metric, number_data_point},
};

use crate::error::Error;
use crate::model::{Label, MetricType, Sample, Series, Temporality};

/// Configuration for [`OtelConverter`].
#[derive(Debug, Clone)]
pub struct OtelConfig {
    /// Include resource attributes as labels on every series. Default: `true`.
    pub include_resource_attrs: bool,
    /// Include scope attributes as labels on every series. Default: `true`.
    pub include_scope_attrs: bool,
}

impl Default for OtelConfig {
    fn default() -> Self {
        Self {
            include_resource_attrs: true,
            include_scope_attrs: true,
        }
    }
}

/// Converts OTLP `ExportMetricsServiceRequest` into `Vec<Series>`.
///
/// The converter walks the OTLP hierarchy (ResourceMetrics → ScopeMetrics →
/// Metric → data points) and decomposes each OTEL metric type into
/// Prometheus-compatible series following the
/// [OTLP Prometheus compatibility spec](https://opentelemetry.io/docs/specs/otel/compatibility/prometheus_and_openmetrics/).
pub struct OtelConverter {
    config: OtelConfig,
}

impl OtelConverter {
    /// Creates a new converter with the given configuration.
    pub fn new(config: OtelConfig) -> Self {
        Self { config }
    }

    /// Decompose an OTLP export request into Series.
    pub fn convert(&self, request: &ExportMetricsServiceRequest) -> Result<Vec<Series>, Error> {
        let mut result = Vec::new();
        for rm in &request.resource_metrics {
            self.convert_resource_metrics(rm, &mut result);
        }
        Ok(result)
    }

    /// Convert a single `ResourceMetrics` — extracts resource-level labels,
    /// then delegates each `ScopeMetrics` entry.
    fn convert_resource_metrics(
        &self,
        rm: &opentelemetry_proto::tonic::metrics::v1::ResourceMetrics,
        result: &mut Vec<Series>,
    ) {
        let resource_labels = if self.config.include_resource_attrs {
            rm.resource
                .as_ref()
                .map(|r| collect_labels(&r.attributes))
                .unwrap_or_default()
        } else {
            vec![]
        };

        for sm in &rm.scope_metrics {
            self.convert_scope_metrics(sm, &resource_labels, result);
        }
    }

    /// Convert a single `ScopeMetrics` — builds the base label set
    /// (resource + scope), then dispatches each metric by type.
    fn convert_scope_metrics(
        &self,
        sm: &opentelemetry_proto::tonic::metrics::v1::ScopeMetrics,
        resource_labels: &[Label],
        result: &mut Vec<Series>,
    ) {
        let mut base_labels = resource_labels.to_vec();

        if let Some(s) = sm.scope.as_ref() {
            if !s.name.is_empty() {
                base_labels.push(Label::new("otel_scope_name", &s.name));
            }
            if !s.version.is_empty() {
                base_labels.push(Label::new("otel_scope_version", &s.version));
            }
            if self.config.include_scope_attrs {
                base_labels.extend(collect_labels(&s.attributes));
            }
        }

        for metric in &sm.metrics {
            self.convert_metric(metric, &base_labels, result);
        }
    }

    /// Dispatch a single `Metric` to the appropriate type-specific converter.
    fn convert_metric(
        &self,
        metric: &opentelemetry_proto::tonic::metrics::v1::Metric,
        base_labels: &[Label],
        result: &mut Vec<Series>,
    ) {
        let name = &metric.name;
        let mut ctx = SeriesCollector {
            unit: &metric.unit,
            description: &metric.description,
            base_labels,
            result,
        };

        match &metric.data {
            Some(metric::Data::Gauge(g)) => {
                self.convert_gauge(name, &g.data_points, &mut ctx);
            }
            Some(metric::Data::Sum(s)) => {
                self.convert_sum(
                    name,
                    &s.data_points,
                    s.aggregation_temporality,
                    s.is_monotonic,
                    &mut ctx,
                );
            }
            Some(metric::Data::Histogram(h)) => {
                self.convert_histogram(name, &h.data_points, h.aggregation_temporality, &mut ctx);
            }
            Some(metric::Data::ExponentialHistogram(eh)) => {
                self.convert_exp_histogram(
                    name,
                    &eh.data_points,
                    eh.aggregation_temporality,
                    &mut ctx,
                );
            }
            Some(metric::Data::Summary(s)) => {
                self.convert_summary(name, &s.data_points, &mut ctx);
            }
            None => {}
        }
    }
}

/// Accumulates `Series` from a single OTLP metric, carrying the fields that are
/// constant across all data points (unit, description, base labels).
struct SeriesCollector<'a> {
    unit: &'a str,
    description: &'a str,
    base_labels: &'a [Label],
    result: &'a mut Vec<Series>,
}

impl SeriesCollector<'_> {
    fn push(
        &mut self,
        name: &str,
        metric_type: MetricType,
        point_labels: &[Label],
        extra_labels: &[Label],
        timestamp_ms: i64,
        value: f64,
    ) {
        let mut labels = Vec::with_capacity(
            1 + self.base_labels.len() + point_labels.len() + extra_labels.len(),
        );
        labels.push(Label::metric_name(name));
        labels.extend_from_slice(self.base_labels);
        labels.extend_from_slice(point_labels);
        labels.extend_from_slice(extra_labels);

        self.result.push(Series {
            labels,
            metric_type: Some(metric_type),
            unit: if self.unit.is_empty() {
                None
            } else {
                Some(self.unit.to_string())
            },
            description: if self.description.is_empty() {
                None
            } else {
                Some(self.description.to_string())
            },
            samples: vec![Sample::new(timestamp_ms, value)],
        });
    }
}

fn sanitize_name(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '.' | '-' => out.push('_'),
            _ if ch.is_ascii_alphanumeric() || ch == '_' => out.push(ch),
            _ => {} // strip
        }
    }
    if out.starts_with(|c: char| c.is_ascii_digit()) {
        out.insert(0, '_');
    }
    out
}

fn normalize_unit(unit: &str) -> Option<String> {
    if unit.is_empty() || unit == "1" {
        return None;
    }
    if unit.starts_with('{') && unit.ends_with('}') {
        return None;
    }
    let suffix = match unit {
        "s" => "seconds".to_string(),
        "ms" => "milliseconds".to_string(),
        "us" => "microseconds".to_string(),
        "ns" => "nanoseconds".to_string(),
        "By" => "bytes".to_string(),
        "KBy" => "kilobytes".to_string(),
        "MBy" => "megabytes".to_string(),
        "GBy" => "gigabytes".to_string(),
        "TBy" => "terabytes".to_string(),
        other => sanitize_name(other),
    };
    Some(suffix)
}

fn build_metric_name(name: &str, unit: &str, is_monotonic_counter: bool) -> String {
    let mut result = sanitize_name(name);

    if let Some(suffix) = normalize_unit(unit)
        && !result.ends_with(&format!("_{}", suffix))
    {
        result.push('_');
        result.push_str(&suffix);
    }

    if is_monotonic_counter && !result.ends_with("_total") {
        result.push_str("_total");
    }

    result
}

fn format_float(v: f64) -> String {
    if v.fract() == 0.0 && v.is_finite() {
        // Use i64 formatting when the value fits, otherwise fall back to f64
        // which avoids silent saturation for values beyond i64 range.
        let i = v as i64;
        if i as f64 == v {
            return format!("{i}");
        }
    }
    format!("{v}")
}

fn kv_to_label(kv: &KeyValue) -> Option<Label> {
    let key = sanitize_name(&kv.key);
    let value = kv.value.as_ref().and_then(|v| v.value.as_ref())?;
    let string_val = match value {
        any_value::Value::StringValue(s) => s.clone(),
        any_value::Value::IntValue(i) => i.to_string(),
        any_value::Value::DoubleValue(d) => d.to_string(),
        any_value::Value::BoolValue(b) => b.to_string(),
        _ => return None,
    };
    Some(Label::new(key, string_val))
}

fn collect_labels(kvs: &[KeyValue]) -> Vec<Label> {
    kvs.iter().filter_map(kv_to_label).collect()
}

fn to_temporality(t: i32) -> Temporality {
    if t == AggregationTemporality::Cumulative as i32 {
        Temporality::Cumulative
    } else if t == AggregationTemporality::Delta as i32 {
        Temporality::Delta
    } else {
        Temporality::Unspecified
    }
}

// Per-type conversion methods.
impl OtelConverter {
    fn convert_gauge(
        &self,
        name: &str,
        data_points: &[opentelemetry_proto::tonic::metrics::v1::NumberDataPoint],
        ctx: &mut SeriesCollector<'_>,
    ) {
        let metric_name = build_metric_name(name, ctx.unit, false);
        for dp in data_points {
            let value = match dp.value {
                Some(number_data_point::Value::AsDouble(v)) => v,
                Some(number_data_point::Value::AsInt(v)) => v as f64,
                None => continue,
            };
            let timestamp_ms = (dp.time_unix_nano / 1_000_000) as i64;
            let point_labels = collect_labels(&dp.attributes);
            ctx.push(
                &metric_name,
                MetricType::Gauge,
                &point_labels,
                &[],
                timestamp_ms,
                value,
            );
        }
    }

    fn convert_sum(
        &self,
        name: &str,
        data_points: &[opentelemetry_proto::tonic::metrics::v1::NumberDataPoint],
        temporality: i32,
        is_monotonic: bool,
        ctx: &mut SeriesCollector<'_>,
    ) {
        let temp = to_temporality(temporality);

        if temp == Temporality::Delta {
            tracing::warn!(metric = name, "dropping delta temporality sum");
            return;
        }

        let (metric_type, is_counter) = if is_monotonic {
            (
                MetricType::Sum {
                    monotonic: true,
                    temporality: temp,
                },
                true,
            )
        } else {
            (MetricType::Gauge, false)
        };

        let metric_name = build_metric_name(name, ctx.unit, is_counter);

        for dp in data_points {
            let value = match dp.value {
                Some(number_data_point::Value::AsDouble(v)) => v,
                Some(number_data_point::Value::AsInt(v)) => v as f64,
                None => continue,
            };
            let timestamp_ms = (dp.time_unix_nano / 1_000_000) as i64;
            let point_labels = collect_labels(&dp.attributes);
            ctx.push(
                &metric_name,
                metric_type,
                &point_labels,
                &[],
                timestamp_ms,
                value,
            );
        }
    }

    fn convert_histogram(
        &self,
        name: &str,
        data_points: &[opentelemetry_proto::tonic::metrics::v1::HistogramDataPoint],
        temporality: i32,
        ctx: &mut SeriesCollector<'_>,
    ) {
        let temp = to_temporality(temporality);
        let metric_type = MetricType::Histogram { temporality: temp };
        let base_name = build_metric_name(name, ctx.unit, false);
        let bucket_name = format!("{}_bucket", base_name);
        let sum_name = format!("{}_sum", base_name);
        let count_name = format!("{}_count", base_name);

        for dp in data_points {
            let timestamp_ms = (dp.time_unix_nano / 1_000_000) as i64;
            let point_labels = collect_labels(&dp.attributes);

            // Bucket series — cumulative counts
            let mut cumulative: u64 = 0;
            for (i, bound) in dp.explicit_bounds.iter().enumerate() {
                cumulative += dp.bucket_counts.get(i).copied().unwrap_or(0);
                let le_label = [Label::new("le", format_float(*bound))];
                ctx.push(
                    &bucket_name,
                    metric_type,
                    &point_labels,
                    &le_label,
                    timestamp_ms,
                    cumulative as f64,
                );
            }

            // +Inf bucket
            cumulative += dp
                .bucket_counts
                .get(dp.explicit_bounds.len())
                .copied()
                .unwrap_or(0);
            let inf_label = [Label::new("le", "+Inf")];
            ctx.push(
                &bucket_name,
                metric_type,
                &point_labels,
                &inf_label,
                timestamp_ms,
                cumulative as f64,
            );

            // _sum
            if let Some(sum) = dp.sum {
                ctx.push(
                    &sum_name,
                    metric_type,
                    &point_labels,
                    &[],
                    timestamp_ms,
                    sum,
                );
            }

            // _count
            ctx.push(
                &count_name,
                metric_type,
                &point_labels,
                &[],
                timestamp_ms,
                dp.count as f64,
            );
        }
    }

    /// Convert an OTLP ExponentialHistogram to classic Prometheus `_bucket`/`_sum`/`_count` series.
    ///
    /// Only positive buckets and `zero_count` are converted to `le`-style buckets. Negative
    /// buckets (representing negative measurement values) cannot be expressed as classic
    /// Prometheus buckets; the OTLP spec maps them to Prometheus Native Histograms instead.
    /// Negative observations are still reflected in `_count` and the `+Inf` bucket.
    fn convert_exp_histogram(
        &self,
        name: &str,
        data_points: &[opentelemetry_proto::tonic::metrics::v1::ExponentialHistogramDataPoint],
        temporality: i32,
        ctx: &mut SeriesCollector<'_>,
    ) {
        let temp = to_temporality(temporality);
        let metric_type = MetricType::Histogram { temporality: temp };
        let base_name = build_metric_name(name, ctx.unit, false);
        let bucket_name = format!("{}_bucket", base_name);
        let sum_name = format!("{}_sum", base_name);
        let count_name = format!("{}_count", base_name);

        for dp in data_points {
            let timestamp_ms = (dp.time_unix_nano / 1_000_000) as i64;
            let point_labels = collect_labels(&dp.attributes);

            // Convert exponential buckets to classic Prometheus le-style buckets.
            //
            // Negative buckets (for negative measurement values) are not converted.
            // The OTLP spec maps ExponentialHistograms to Prometheus Native Histograms
            // which can represent negative ranges natively; classic le-buckets cannot.
            // Negative bucket counts are still included in _count and +Inf.
            let base = 2_f64.powf(2_f64.powi(-dp.scale));

            let mut explicit_bounds = Vec::new();
            let mut cumulative_counts = Vec::new();

            // Start cumulative from zero_count so that the first positive bucket
            // includes observations in the zero range.
            let mut cumulative: u64 = dp.zero_count;

            // Positive buckets.
            if let Some(ref positive) = dp.positive {
                let offset = positive.offset;
                for (i, &count) in positive.bucket_counts.iter().enumerate() {
                    let boundary = base.powf((offset + i as i32 + 1) as f64);
                    cumulative += count;
                    explicit_bounds.push(boundary);
                    cumulative_counts.push(cumulative);
                }
            }

            // Emit bucket series.
            for (bound, cum_count) in explicit_bounds.iter().zip(cumulative_counts.iter()) {
                let le_label = [Label::new("le", format_float(*bound))];
                ctx.push(
                    &bucket_name,
                    metric_type,
                    &point_labels,
                    &le_label,
                    timestamp_ms,
                    *cum_count as f64,
                );
            }

            // +Inf bucket.
            let inf_label = [Label::new("le", "+Inf")];
            ctx.push(
                &bucket_name,
                metric_type,
                &point_labels,
                &inf_label,
                timestamp_ms,
                dp.count as f64,
            );

            // _sum
            if let Some(sum) = dp.sum {
                ctx.push(
                    &sum_name,
                    metric_type,
                    &point_labels,
                    &[],
                    timestamp_ms,
                    sum,
                );
            }

            // _count
            ctx.push(
                &count_name,
                metric_type,
                &point_labels,
                &[],
                timestamp_ms,
                dp.count as f64,
            );
        }
    }

    fn convert_summary(
        &self,
        name: &str,
        data_points: &[opentelemetry_proto::tonic::metrics::v1::SummaryDataPoint],
        ctx: &mut SeriesCollector<'_>,
    ) {
        let base_name = build_metric_name(name, ctx.unit, false);
        let sum_name = format!("{}_sum", base_name);
        let count_name = format!("{}_count", base_name);

        for dp in data_points {
            let timestamp_ms = (dp.time_unix_nano / 1_000_000) as i64;
            let point_labels = collect_labels(&dp.attributes);

            // Per-quantile series
            for q in &dp.quantile_values {
                let q_label = [Label::new("quantile", format_float(q.quantile))];
                ctx.push(
                    &base_name,
                    MetricType::Summary,
                    &point_labels,
                    &q_label,
                    timestamp_ms,
                    q.value,
                );
            }

            // _sum
            ctx.push(
                &sum_name,
                MetricType::Summary,
                &point_labels,
                &[],
                timestamp_ms,
                dp.sum,
            );

            // _count
            ctx.push(
                &count_name,
                MetricType::Summary,
                &point_labels,
                &[],
                timestamp_ms,
                dp.count as f64,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{MetricType, Temporality};
    use opentelemetry_proto::tonic::common::v1::InstrumentationScope;
    use opentelemetry_proto::tonic::{
        collector::metrics::v1::ExportMetricsServiceRequest,
        common::v1::{AnyValue, KeyValue, any_value},
        metrics::v1::{
            AggregationTemporality, ExponentialHistogram, ExponentialHistogramDataPoint, Gauge,
            Histogram, HistogramDataPoint, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
            Sum, Summary, SummaryDataPoint, exponential_histogram_data_point::Buckets, metric,
            number_data_point, summary_data_point,
        },
        resource::v1::Resource,
    };

    fn make_request(resource_metrics: Vec<ResourceMetrics>) -> ExportMetricsServiceRequest {
        ExportMetricsServiceRequest { resource_metrics }
    }

    fn make_resource_metrics(
        resource_attrs: Vec<KeyValue>,
        scope_metrics: Vec<ScopeMetrics>,
    ) -> ResourceMetrics {
        ResourceMetrics {
            resource: Some(Resource {
                attributes: resource_attrs,
                dropped_attributes_count: 0,
            }),
            scope_metrics,
            schema_url: String::new(),
        }
    }

    fn make_scope_metrics(
        scope_name: &str,
        scope_version: &str,
        metrics: Vec<Metric>,
    ) -> ScopeMetrics {
        ScopeMetrics {
            scope: Some(InstrumentationScope {
                name: scope_name.to_string(),
                version: scope_version.to_string(),
                attributes: vec![],
                dropped_attributes_count: 0,
            }),
            metrics,
            schema_url: String::new(),
        }
    }

    fn make_scope_metrics_with_attrs(
        scope_name: &str,
        scope_version: &str,
        scope_attrs: Vec<KeyValue>,
        metrics: Vec<Metric>,
    ) -> ScopeMetrics {
        ScopeMetrics {
            scope: Some(InstrumentationScope {
                name: scope_name.to_string(),
                version: scope_version.to_string(),
                attributes: scope_attrs,
                dropped_attributes_count: 0,
            }),
            metrics,
            schema_url: String::new(),
        }
    }

    fn make_gauge(
        name: &str,
        unit: &str,
        description: &str,
        data_points: Vec<NumberDataPoint>,
    ) -> Metric {
        Metric {
            name: name.to_string(),
            description: description.to_string(),
            unit: unit.to_string(),
            metadata: vec![],
            data: Some(metric::Data::Gauge(Gauge { data_points })),
        }
    }

    fn make_sum(
        name: &str,
        unit: &str,
        description: &str,
        data_points: Vec<NumberDataPoint>,
        temporality: i32,
        monotonic: bool,
    ) -> Metric {
        Metric {
            name: name.to_string(),
            description: description.to_string(),
            unit: unit.to_string(),
            metadata: vec![],
            data: Some(metric::Data::Sum(Sum {
                data_points,
                aggregation_temporality: temporality,
                is_monotonic: monotonic,
            })),
        }
    }

    fn make_histogram(
        name: &str,
        unit: &str,
        description: &str,
        data_points: Vec<HistogramDataPoint>,
        temporality: i32,
    ) -> Metric {
        Metric {
            name: name.to_string(),
            description: description.to_string(),
            unit: unit.to_string(),
            metadata: vec![],
            data: Some(metric::Data::Histogram(Histogram {
                data_points,
                aggregation_temporality: temporality,
            })),
        }
    }

    fn make_exp_histogram(
        name: &str,
        unit: &str,
        description: &str,
        data_points: Vec<ExponentialHistogramDataPoint>,
        temporality: i32,
    ) -> Metric {
        Metric {
            name: name.to_string(),
            description: description.to_string(),
            unit: unit.to_string(),
            metadata: vec![],
            data: Some(metric::Data::ExponentialHistogram(ExponentialHistogram {
                data_points,
                aggregation_temporality: temporality,
            })),
        }
    }

    fn make_summary(
        name: &str,
        unit: &str,
        description: &str,
        data_points: Vec<SummaryDataPoint>,
    ) -> Metric {
        Metric {
            name: name.to_string(),
            description: description.to_string(),
            unit: unit.to_string(),
            metadata: vec![],
            data: Some(metric::Data::Summary(Summary { data_points })),
        }
    }

    fn make_number_dp(
        value: number_data_point::Value,
        time_unix_nano: u64,
        attrs: Vec<KeyValue>,
    ) -> NumberDataPoint {
        NumberDataPoint {
            attributes: attrs,
            start_time_unix_nano: 0,
            time_unix_nano,
            exemplars: vec![],
            flags: 0,
            value: Some(value),
        }
    }

    fn make_histogram_dp(
        time_unix_nano: u64,
        count: u64,
        sum: f64,
        bucket_counts: Vec<u64>,
        explicit_bounds: Vec<f64>,
        attrs: Vec<KeyValue>,
    ) -> HistogramDataPoint {
        HistogramDataPoint {
            attributes: attrs,
            start_time_unix_nano: 0,
            time_unix_nano,
            count,
            sum: Some(sum),
            bucket_counts,
            explicit_bounds,
            exemplars: vec![],
            flags: 0,
            min: None,
            max: None,
        }
    }

    fn make_summary_dp(
        time_unix_nano: u64,
        count: u64,
        sum: f64,
        quantile_values: Vec<summary_data_point::ValueAtQuantile>,
        attrs: Vec<KeyValue>,
    ) -> SummaryDataPoint {
        SummaryDataPoint {
            attributes: attrs,
            start_time_unix_nano: 0,
            time_unix_nano,
            count,
            sum,
            quantile_values,
            flags: 0,
        }
    }

    fn kv(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(value.to_string())),
            }),
        }
    }

    /// Find all series whose `__name__` label contains `name_substr`.
    fn find_series<'a>(series: &'a [Series], name_substr: &str) -> Vec<&'a Series> {
        series
            .iter()
            .filter(|s| s.name().contains(name_substr))
            .collect()
    }

    /// Get the value of a label on a series.
    fn get_label<'a>(series: &'a Series, label_name: &str) -> Option<&'a str> {
        series
            .labels
            .iter()
            .find(|l| l.name == label_name)
            .map(|l| l.value.as_str())
    }

    fn build_default(request: &ExportMetricsServiceRequest) -> Vec<Series> {
        let builder = OtelConverter::new(OtelConfig::default());
        builder.convert(request).expect("convert should succeed")
    }

    fn ts_nanos(ms: u64) -> u64 {
        ms * 1_000_000
    }

    #[test]
    fn should_convert_gauge_to_single_series() {
        let request = make_request(vec![make_resource_metrics(
            vec![],
            vec![make_scope_metrics(
                "test",
                "1.0",
                vec![make_gauge(
                    "cpu_temperature",
                    "",
                    "CPU temperature",
                    vec![make_number_dp(
                        number_data_point::Value::AsDouble(72.5),
                        ts_nanos(1000),
                        vec![],
                    )],
                )],
            )],
        )]);

        let series = build_default(&request);
        let matched = find_series(&series, "cpu_temperature");
        assert_eq!(matched.len(), 1);
        assert_eq!(matched[0].metric_type, Some(MetricType::Gauge));
        assert_eq!(matched[0].samples.len(), 1);
        assert_eq!(matched[0].samples[0].value, 72.5);
        assert_eq!(matched[0].samples[0].timestamp_ms, 1000);
    }

    #[test]
    fn should_include_gauge_data_point_attributes_as_labels() {
        let request = make_request(vec![make_resource_metrics(
            vec![],
            vec![make_scope_metrics(
                "test",
                "1.0",
                vec![make_gauge(
                    "cpu_temperature",
                    "",
                    "",
                    vec![make_number_dp(
                        number_data_point::Value::AsDouble(72.5),
                        ts_nanos(1000),
                        vec![kv("host", "server1"), kv("region", "us-east")],
                    )],
                )],
            )],
        )]);

        let series = build_default(&request);
        let matched = find_series(&series, "cpu_temperature");
        assert_eq!(matched.len(), 1);
        assert_eq!(get_label(matched[0], "host"), Some("server1"));
        assert_eq!(get_label(matched[0], "region"), Some("us-east"));
    }

    #[test]
    fn should_handle_gauge_with_multiple_data_points() {
        // Each data point with distinct attributes becomes its own series.
        let request = make_request(vec![make_resource_metrics(
            vec![],
            vec![make_scope_metrics(
                "test",
                "1.0",
                vec![make_gauge(
                    "cpu_temperature",
                    "",
                    "",
                    vec![
                        make_number_dp(
                            number_data_point::Value::AsDouble(72.5),
                            ts_nanos(1000),
                            vec![kv("host", "server1")],
                        ),
                        make_number_dp(
                            number_data_point::Value::AsDouble(68.0),
                            ts_nanos(2000),
                            vec![kv("host", "server2")],
                        ),
                    ],
                )],
            )],
        )]);

        let series = build_default(&request);
        let matched = find_series(&series, "cpu_temperature");
        assert_eq!(matched.len(), 2);
    }

    #[test]
    fn should_handle_gauge_with_int_value() {
        let request = make_request(vec![make_resource_metrics(
            vec![],
            vec![make_scope_metrics(
                "test",
                "1.0",
                vec![make_gauge(
                    "active_connections",
                    "",
                    "",
                    vec![make_number_dp(
                        number_data_point::Value::AsInt(42),
                        ts_nanos(1000),
                        vec![],
                    )],
                )],
            )],
        )]);

        let series = build_default(&request);
        let matched = find_series(&series, "active_connections");
        assert_eq!(matched.len(), 1);
        assert_eq!(matched[0].samples[0].value, 42.0);
    }

    #[test]
    fn should_convert_monotonic_cumulative_sum_to_counter_with_total_suffix() {
        let request = make_request(vec![make_resource_metrics(
            vec![],
            vec![make_scope_metrics(
                "test",
                "1.0",
                vec![make_sum(
                    "http_requests",
                    "",
                    "Total HTTP requests",
                    vec![make_number_dp(
                        number_data_point::Value::AsDouble(100.0),
                        ts_nanos(1000),
                        vec![],
                    )],
                    AggregationTemporality::Cumulative as i32,
                    true,
                )],
            )],
        )]);

        let series = build_default(&request);
        let matched = find_series(&series, "http_requests_total");
        assert_eq!(matched.len(), 1);
        assert_eq!(
            matched[0].metric_type,
            Some(MetricType::Sum {
                monotonic: true,
                temporality: Temporality::Cumulative,
            })
        );
        assert_eq!(matched[0].samples[0].value, 100.0);
    }

    #[test]
    fn should_convert_non_monotonic_sum_to_gauge() {
        let request = make_request(vec![make_resource_metrics(
            vec![],
            vec![make_scope_metrics(
                "test",
                "1.0",
                vec![make_sum(
                    "queue_size",
                    "",
                    "Current queue size",
                    vec![make_number_dp(
                        number_data_point::Value::AsDouble(50.0),
                        ts_nanos(1000),
                        vec![],
                    )],
                    AggregationTemporality::Cumulative as i32,
                    false,
                )],
            )],
        )]);

        let series = build_default(&request);
        // Non-monotonic sum should NOT have _total suffix.
        let matched = find_series(&series, "queue_size");
        assert_eq!(matched.len(), 1);
        assert_eq!(matched[0].name(), "queue_size");
        assert_eq!(matched[0].metric_type, Some(MetricType::Gauge));
    }

    #[test]
    fn should_drop_delta_sum_with_warning() {
        let request = make_request(vec![make_resource_metrics(
            vec![],
            vec![make_scope_metrics(
                "test",
                "1.0",
                vec![make_sum(
                    "delta_counter",
                    "",
                    "",
                    vec![make_number_dp(
                        number_data_point::Value::AsDouble(10.0),
                        ts_nanos(1000),
                        vec![],
                    )],
                    AggregationTemporality::Delta as i32,
                    true,
                )],
            )],
        )]);

        let series = build_default(&request);
        assert!(
            find_series(&series, "delta_counter").is_empty(),
            "delta temporality sums should be dropped"
        );
    }

    #[test]
    fn should_decompose_histogram_into_bucket_sum_count() {
        let request = make_request(vec![make_resource_metrics(
            vec![],
            vec![make_scope_metrics(
                "test",
                "1.0",
                vec![make_histogram(
                    "http_request_duration",
                    "",
                    "",
                    vec![make_histogram_dp(
                        ts_nanos(1000),
                        10,                  // count
                        5.5,                 // sum
                        vec![2, 3, 5, 10],   // bucket_counts (last is +Inf)
                        vec![0.1, 0.5, 1.0], // explicit_bounds
                        vec![],
                    )],
                    AggregationTemporality::Cumulative as i32,
                )],
            )],
        )]);

        let series = build_default(&request);

        // Should have _bucket series (one per bound + Inf), _sum, and _count.
        let buckets = find_series(&series, "_bucket");
        assert!(!buckets.is_empty(), "should produce _bucket series");

        let sums = find_series(&series, "_sum");
        assert_eq!(sums.len(), 1, "should produce one _sum series");
        assert_eq!(sums[0].samples[0].value, 5.5);

        let counts = find_series(&series, "_count");
        assert_eq!(counts.len(), 1, "should produce one _count series");
        assert_eq!(counts[0].samples[0].value, 10.0);
    }

    #[test]
    fn should_include_le_label_on_histogram_buckets() {
        let request = make_request(vec![make_resource_metrics(
            vec![],
            vec![make_scope_metrics(
                "test",
                "1.0",
                vec![make_histogram(
                    "http_request_duration",
                    "",
                    "",
                    vec![make_histogram_dp(
                        ts_nanos(1000),
                        10,
                        5.5,
                        vec![2, 3, 5, 10],
                        vec![0.1, 0.5, 1.0],
                        vec![],
                    )],
                    AggregationTemporality::Cumulative as i32,
                )],
            )],
        )]);

        let series = build_default(&request);
        let buckets = find_series(&series, "_bucket");

        for bucket in &buckets {
            assert!(
                get_label(bucket, "le").is_some(),
                "each _bucket series should have an 'le' label"
            );
        }

        // Check specific le values exist.
        let le_values: Vec<&str> = buckets.iter().filter_map(|s| get_label(s, "le")).collect();
        assert!(le_values.contains(&"0.1"));
        assert!(le_values.contains(&"0.5"));
        assert!(le_values.contains(&"1"));
    }

    #[test]
    fn should_include_inf_bucket() {
        let request = make_request(vec![make_resource_metrics(
            vec![],
            vec![make_scope_metrics(
                "test",
                "1.0",
                vec![make_histogram(
                    "http_request_duration",
                    "",
                    "",
                    vec![make_histogram_dp(
                        ts_nanos(1000),
                        10,
                        5.5,
                        vec![2, 3, 5, 10],
                        vec![0.1, 0.5, 1.0],
                        vec![],
                    )],
                    AggregationTemporality::Cumulative as i32,
                )],
            )],
        )]);

        let series = build_default(&request);
        let buckets = find_series(&series, "_bucket");

        let le_values: Vec<&str> = buckets.iter().filter_map(|s| get_label(s, "le")).collect();
        assert!(
            le_values.contains(&"+Inf"),
            "+Inf bucket should always be present"
        );
    }

    #[test]
    fn should_set_histogram_series_metric_type_to_counter() {
        let request = make_request(vec![make_resource_metrics(
            vec![],
            vec![make_scope_metrics(
                "test",
                "1.0",
                vec![make_histogram(
                    "http_request_duration",
                    "",
                    "",
                    vec![make_histogram_dp(
                        ts_nanos(1000),
                        10,
                        5.5,
                        vec![2, 3, 5, 10],
                        vec![0.1, 0.5, 1.0],
                        vec![],
                    )],
                    AggregationTemporality::Cumulative as i32,
                )],
            )],
        )]);

        let series = build_default(&request);
        let all_histogram = find_series(&series, "http_request_duration");
        for s in &all_histogram {
            assert_eq!(
                s.metric_type,
                Some(MetricType::Histogram {
                    temporality: Temporality::Cumulative,
                })
            );
        }
    }

    #[test]
    fn should_convert_exponential_histogram_to_explicit_buckets() {
        // Exponential histogram with scale=0: base = 2^(2^0) = 2
        // Positive buckets: offset=0, counts=[1, 2, 3]
        // Boundaries: [2^0, 2^1, 2^2] = [1, 2, 4]
        let request = make_request(vec![make_resource_metrics(
            vec![],
            vec![make_scope_metrics(
                "test",
                "1.0",
                vec![make_exp_histogram(
                    "request_latency",
                    "",
                    "",
                    vec![ExponentialHistogramDataPoint {
                        attributes: vec![],
                        start_time_unix_nano: 0,
                        time_unix_nano: ts_nanos(1000),
                        count: 6,
                        sum: Some(15.0),
                        scale: 0,
                        zero_count: 0,
                        positive: Some(Buckets {
                            offset: 0,
                            bucket_counts: vec![1, 2, 3],
                        }),
                        negative: None,
                        flags: 0,
                        exemplars: vec![],
                        min: None,
                        max: None,
                        zero_threshold: 0.0,
                    }],
                    AggregationTemporality::Cumulative as i32,
                )],
            )],
        )]);

        let series = build_default(&request);

        // Should decompose into _bucket, _sum, _count just like a regular histogram.
        let buckets = find_series(&series, "_bucket");
        assert!(!buckets.is_empty(), "should produce _bucket series");

        let sums = find_series(&series, "_sum");
        assert_eq!(sums.len(), 1);
        assert_eq!(sums[0].samples[0].value, 15.0);

        let counts = find_series(&series, "_count");
        assert_eq!(counts.len(), 1);
        assert_eq!(counts[0].samples[0].value, 6.0);
    }

    #[test]
    fn should_include_zero_count_in_exp_histogram_buckets() {
        // Exponential histogram with scale=0, zero_count=5, positive=[1, 2, 3]
        // The zero bucket count should be included in the cumulative counts.
        // Expected cumulative: bucket at le=2 → 5+1=6, le=4 → 6+2=8, le=8 → 8+3=11 (wait, we
        // need to think about this more carefully)
        //
        // With scale=0: base = 2^(2^0) = 2
        // Positive bucket boundaries: base^(offset+i+1) with offset=0 → [2, 4, 8]
        // zero_count = 5 should appear in cumulative counts before positive buckets.
        // Cumulative: le=2 → 5+1=6, le=4 → 6+2=8, le=8 → 8+3=11
        // +Inf → dp.count = 11
        let request = make_request(vec![make_resource_metrics(
            vec![],
            vec![make_scope_metrics(
                "test",
                "1.0",
                vec![make_exp_histogram(
                    "request_size",
                    "",
                    "",
                    vec![ExponentialHistogramDataPoint {
                        attributes: vec![],
                        start_time_unix_nano: 0,
                        time_unix_nano: ts_nanos(1000),
                        count: 11,
                        sum: Some(30.0),
                        scale: 0,
                        zero_count: 5,
                        positive: Some(Buckets {
                            offset: 0,
                            bucket_counts: vec![1, 2, 3],
                        }),
                        negative: None,
                        flags: 0,
                        exemplars: vec![],
                        min: None,
                        max: None,
                        zero_threshold: 0.0,
                    }],
                    AggregationTemporality::Cumulative as i32,
                )],
            )],
        )]);

        let series = build_default(&request);
        let buckets = find_series(&series, "_bucket");

        // First positive bucket should include zero_count in its cumulative.
        let first_positive = buckets
            .iter()
            .find(|s| get_label(s, "le") == Some("2"))
            .expect("should have le=2 bucket");
        assert_eq!(
            first_positive.samples[0].value, 6.0,
            "first positive bucket should include zero_count (5) + bucket count (1) = 6"
        );

        // +Inf should equal dp.count
        let inf = buckets
            .iter()
            .find(|s| get_label(s, "le") == Some("+Inf"))
            .expect("should have +Inf bucket");
        assert_eq!(inf.samples[0].value, 11.0);
    }

    #[test]
    fn should_not_emit_negative_bucket_boundaries_in_exp_histogram() {
        // Negative buckets cannot be represented as classic Prometheus le-style buckets.
        // They should be silently skipped (counts still appear in +Inf via dp.count).
        let request = make_request(vec![make_resource_metrics(
            vec![],
            vec![make_scope_metrics(
                "test",
                "1.0",
                vec![make_exp_histogram(
                    "temperature_delta",
                    "",
                    "",
                    vec![ExponentialHistogramDataPoint {
                        attributes: vec![],
                        start_time_unix_nano: 0,
                        time_unix_nano: ts_nanos(1000),
                        count: 10,
                        sum: Some(-5.0),
                        scale: 0,
                        zero_count: 4,
                        positive: Some(Buckets {
                            offset: 0,
                            bucket_counts: vec![1],
                        }),
                        negative: Some(Buckets {
                            offset: 0,
                            bucket_counts: vec![2, 3],
                        }),
                        flags: 0,
                        exemplars: vec![],
                        min: None,
                        max: None,
                        zero_threshold: 0.0,
                    }],
                    AggregationTemporality::Cumulative as i32,
                )],
            )],
        )]);

        let series = build_default(&request);
        let buckets = find_series(&series, "_bucket");

        // No negative le values should be emitted.
        let negative_buckets: Vec<_> = buckets
            .iter()
            .filter(|s| {
                get_label(s, "le")
                    .and_then(|v| v.parse::<f64>().ok())
                    .is_some_and(|v| v < 0.0)
            })
            .collect();
        assert!(
            negative_buckets.is_empty(),
            "should not produce buckets with negative le values"
        );

        // +Inf still reflects total dp.count (including negative observations).
        let inf = buckets
            .iter()
            .find(|s| get_label(s, "le") == Some("+Inf"))
            .expect("should have +Inf bucket");
        assert_eq!(inf.samples[0].value, 10.0);
    }

    #[test]
    fn should_decompose_summary_into_quantile_sum_count() {
        let request = make_request(vec![make_resource_metrics(
            vec![],
            vec![make_scope_metrics(
                "test",
                "1.0",
                vec![make_summary(
                    "rpc_duration",
                    "",
                    "",
                    vec![make_summary_dp(
                        ts_nanos(1000),
                        100,
                        500.0,
                        vec![
                            summary_data_point::ValueAtQuantile {
                                quantile: 0.5,
                                value: 4.0,
                            },
                            summary_data_point::ValueAtQuantile {
                                quantile: 0.99,
                                value: 8.0,
                            },
                        ],
                        vec![],
                    )],
                )],
            )],
        )]);

        let series = build_default(&request);

        // Per-quantile series.
        let quantiles = find_series(&series, "rpc_duration")
            .into_iter()
            .filter(|s| get_label(s, "quantile").is_some())
            .collect::<Vec<_>>();
        assert_eq!(quantiles.len(), 2, "should produce one series per quantile");

        // _sum and _count.
        let sums = find_series(&series, "_sum");
        assert_eq!(sums.len(), 1);
        assert_eq!(sums[0].samples[0].value, 500.0);

        let counts = find_series(&series, "_count");
        assert_eq!(counts.len(), 1);
        assert_eq!(counts[0].samples[0].value, 100.0);
    }

    #[test]
    fn should_include_quantile_label() {
        let request = make_request(vec![make_resource_metrics(
            vec![],
            vec![make_scope_metrics(
                "test",
                "1.0",
                vec![make_summary(
                    "rpc_duration",
                    "",
                    "",
                    vec![make_summary_dp(
                        ts_nanos(1000),
                        100,
                        500.0,
                        vec![
                            summary_data_point::ValueAtQuantile {
                                quantile: 0.5,
                                value: 4.0,
                            },
                            summary_data_point::ValueAtQuantile {
                                quantile: 0.99,
                                value: 8.0,
                            },
                        ],
                        vec![],
                    )],
                )],
            )],
        )]);

        let series = build_default(&request);
        let quantile_series: Vec<_> = series
            .iter()
            .filter(|s| get_label(s, "quantile").is_some())
            .collect();

        let quantile_values: Vec<&str> = quantile_series
            .iter()
            .map(|s| get_label(s, "quantile").unwrap())
            .collect();
        assert!(quantile_values.contains(&"0.5"));
        assert!(quantile_values.contains(&"0.99"));
    }

    #[test]
    fn should_include_resource_attributes_as_labels() {
        let request = make_request(vec![make_resource_metrics(
            vec![kv("service.name", "my-svc"), kv("host.name", "node-1")],
            vec![make_scope_metrics(
                "test",
                "1.0",
                vec![make_gauge(
                    "cpu_temp",
                    "",
                    "",
                    vec![make_number_dp(
                        number_data_point::Value::AsDouble(70.0),
                        ts_nanos(1000),
                        vec![],
                    )],
                )],
            )],
        )]);

        let series = build_default(&request);
        let matched = find_series(&series, "cpu_temp");
        assert_eq!(matched.len(), 1);
        assert_eq!(get_label(matched[0], "service_name"), Some("my-svc"));
        assert_eq!(get_label(matched[0], "host_name"), Some("node-1"));
    }

    #[test]
    fn should_include_scope_labels() {
        let request = make_request(vec![make_resource_metrics(
            vec![],
            vec![make_scope_metrics(
                "my.library",
                "2.0.0",
                vec![make_gauge(
                    "cpu_temp",
                    "",
                    "",
                    vec![make_number_dp(
                        number_data_point::Value::AsDouble(70.0),
                        ts_nanos(1000),
                        vec![],
                    )],
                )],
            )],
        )]);

        let series = build_default(&request);
        let matched = find_series(&series, "cpu_temp");
        assert_eq!(matched.len(), 1);
        assert_eq!(get_label(matched[0], "otel_scope_name"), Some("my.library"));
        assert_eq!(get_label(matched[0], "otel_scope_version"), Some("2.0.0"));
    }

    #[test]
    fn should_include_scope_attributes_as_labels() {
        let request = make_request(vec![make_resource_metrics(
            vec![],
            vec![make_scope_metrics_with_attrs(
                "test",
                "1.0",
                vec![kv("scope.tag", "abc")],
                vec![make_gauge(
                    "cpu_temp",
                    "",
                    "",
                    vec![make_number_dp(
                        number_data_point::Value::AsDouble(70.0),
                        ts_nanos(1000),
                        vec![],
                    )],
                )],
            )],
        )]);

        let series = build_default(&request);
        let matched = find_series(&series, "cpu_temp");
        assert_eq!(matched.len(), 1);
        assert_eq!(get_label(matched[0], "scope_tag"), Some("abc"));
    }

    #[test]
    fn should_exclude_resource_attrs_when_config_disabled() {
        let request = make_request(vec![make_resource_metrics(
            vec![kv("service.name", "my-svc")],
            vec![make_scope_metrics(
                "test",
                "1.0",
                vec![make_gauge(
                    "cpu_temp",
                    "",
                    "",
                    vec![make_number_dp(
                        number_data_point::Value::AsDouble(70.0),
                        ts_nanos(1000),
                        vec![],
                    )],
                )],
            )],
        )]);

        let builder = OtelConverter::new(OtelConfig {
            include_resource_attrs: false,
            include_scope_attrs: true,
        });
        let series = builder.convert(&request).expect("convert should succeed");
        let matched = find_series(&series, "cpu_temp");
        assert_eq!(matched.len(), 1);
        assert_eq!(
            get_label(matched[0], "service_name"),
            None,
            "resource attrs should be excluded"
        );
    }

    #[test]
    fn should_exclude_scope_attrs_when_config_disabled() {
        let request = make_request(vec![make_resource_metrics(
            vec![],
            vec![make_scope_metrics_with_attrs(
                "test",
                "1.0",
                vec![kv("scope.tag", "abc")],
                vec![make_gauge(
                    "cpu_temp",
                    "",
                    "",
                    vec![make_number_dp(
                        number_data_point::Value::AsDouble(70.0),
                        ts_nanos(1000),
                        vec![],
                    )],
                )],
            )],
        )]);

        let builder = OtelConverter::new(OtelConfig {
            include_resource_attrs: true,
            include_scope_attrs: false,
        });
        let series = builder.convert(&request).expect("convert should succeed");
        let matched = find_series(&series, "cpu_temp");
        assert_eq!(matched.len(), 1);
        assert_eq!(
            get_label(matched[0], "scope_tag"),
            None,
            "scope attrs should be excluded"
        );
        // Scope name/version labels should still be present even when scope attrs are disabled,
        // as they are considered scope identity, not attributes.
        assert_eq!(get_label(matched[0], "otel_scope_name"), Some("test"));
    }

    #[test]
    fn should_omit_scope_labels_when_scope_is_empty() {
        let request = make_request(vec![make_resource_metrics(
            vec![],
            vec![make_scope_metrics(
                "",
                "",
                vec![make_gauge(
                    "cpu_temp",
                    "",
                    "",
                    vec![make_number_dp(
                        number_data_point::Value::AsDouble(70.0),
                        ts_nanos(1000),
                        vec![],
                    )],
                )],
            )],
        )]);

        let series = build_default(&request);
        let matched = find_series(&series, "cpu_temp");
        assert_eq!(matched.len(), 1);
        assert_eq!(
            get_label(matched[0], "otel_scope_name"),
            None,
            "empty scope name should not produce a label"
        );
        assert_eq!(
            get_label(matched[0], "otel_scope_version"),
            None,
            "empty scope version should not produce a label"
        );
    }

    #[test]
    fn should_append_unit_suffix_to_metric_name() {
        // Per OTEL spec: metric "http.request.duration" with unit "s" →
        // "http_request_duration_seconds"
        let request = make_request(vec![make_resource_metrics(
            vec![],
            vec![make_scope_metrics(
                "test",
                "1.0",
                vec![make_gauge(
                    "http.request.duration",
                    "s",
                    "",
                    vec![make_number_dp(
                        number_data_point::Value::AsDouble(0.5),
                        ts_nanos(1000),
                        vec![],
                    )],
                )],
            )],
        )]);

        let series = build_default(&request);
        let matched = find_series(&series, "http_request_duration_seconds");
        assert_eq!(
            matched.len(),
            1,
            "unit 's' should be expanded to '_seconds' suffix"
        );
    }

    #[test]
    fn should_normalize_unit_to_prometheus_convention() {
        // Curly-brace units like "{requests}" should produce no suffix.
        let request = make_request(vec![make_resource_metrics(
            vec![],
            vec![make_scope_metrics(
                "test",
                "1.0",
                vec![make_gauge(
                    "http_requests",
                    "{requests}",
                    "",
                    vec![make_number_dp(
                        number_data_point::Value::AsDouble(1.0),
                        ts_nanos(1000),
                        vec![],
                    )],
                )],
            )],
        )]);

        let series = build_default(&request);
        let matched = find_series(&series, "http_requests");
        assert_eq!(matched.len(), 1);
        // Name should NOT have a suffix from curly-brace units.
        assert_eq!(matched[0].name(), "http_requests");
    }

    #[test]
    fn should_handle_empty_request() {
        let request = make_request(vec![]);
        let series = build_default(&request);
        assert!(series.is_empty());
    }

    #[test]
    fn should_handle_metric_with_no_data_points() {
        let request = make_request(vec![make_resource_metrics(
            vec![],
            vec![make_scope_metrics(
                "test",
                "1.0",
                vec![make_gauge("empty_gauge", "", "", vec![])],
            )],
        )]);

        let series = build_default(&request);
        assert!(
            series.is_empty(),
            "metric with no data points should produce no series"
        );
    }

    #[test]
    fn should_handle_metric_with_no_data_field() {
        let request = make_request(vec![make_resource_metrics(
            vec![],
            vec![make_scope_metrics(
                "test",
                "1.0",
                vec![Metric {
                    name: "no_data".to_string(),
                    description: String::new(),
                    unit: String::new(),
                    metadata: vec![],
                    data: None,
                }],
            )],
        )]);

        let series = build_default(&request);
        assert!(series.is_empty(), "metric with data=None should be skipped");
    }

    #[test]
    fn should_propagate_description_and_unit_to_series_metadata() {
        let request = make_request(vec![make_resource_metrics(
            vec![],
            vec![make_scope_metrics(
                "test",
                "1.0",
                vec![make_gauge(
                    "cpu_temp",
                    "Cel",
                    "CPU temperature in Celsius",
                    vec![make_number_dp(
                        number_data_point::Value::AsDouble(70.0),
                        ts_nanos(1000),
                        vec![],
                    )],
                )],
            )],
        )]);

        let series = build_default(&request);
        let matched = find_series(&series, "cpu_temp");
        assert_eq!(matched.len(), 1);
        assert_eq!(
            matched[0].description.as_deref(),
            Some("CPU temperature in Celsius")
        );
        assert!(matched[0].unit.is_some(), "unit should be set on series");
    }

    #[test]
    fn should_sanitize_metric_name_for_prometheus() {
        // Dots should become underscores, leading digits should be prefixed.
        let request = make_request(vec![make_resource_metrics(
            vec![],
            vec![make_scope_metrics(
                "test",
                "1.0",
                vec![make_gauge(
                    "http.server.request.duration",
                    "",
                    "",
                    vec![make_number_dp(
                        number_data_point::Value::AsDouble(1.0),
                        ts_nanos(1000),
                        vec![],
                    )],
                )],
            )],
        )]);

        let series = build_default(&request);
        let matched = find_series(&series, "http_server_request_duration");
        assert_eq!(
            matched.len(),
            1,
            "dots in metric name should be replaced with underscores"
        );
    }

    #[test]
    fn format_float_should_handle_values_beyond_i64_range() {
        // 1e19 exceeds i64::MAX (~9.2e18). format_float should not saturate.
        assert_eq!(format_float(1e19), "10000000000000000000");
        assert_eq!(format_float(-1e19), "-10000000000000000000");
    }

    #[test]
    fn format_float_should_format_whole_numbers_without_decimal() {
        assert_eq!(format_float(1.0), "1");
        assert_eq!(format_float(100.0), "100");
        assert_eq!(format_float(0.0), "0");
        assert_eq!(format_float(-5.0), "-5");
    }

    #[test]
    fn format_float_should_preserve_fractional_values() {
        assert_eq!(format_float(0.5), "0.5");
        assert_eq!(format_float(0.99), "0.99");
    }
}
