//! OpenMetrics text format parser.
//!
//! Parses OpenMetrics exposition format into `Series` for ingestion.
//! See: <https://prometheus.io/docs/specs/om/open_metrics_spec/>

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::model::{Label, MetricType, Sample, Series, Temporality};
use crate::util::{Fingerprint, Result};

/// OpenMetrics metric types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum OpenMetricsType {
    Gauge,
    Counter,
    Histogram,
    Summary,
    StateSet,
    Info,
    GaugeHistogram,
    #[default]
    Unknown,
}

impl OpenMetricsType {
    fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "gauge" => Self::Gauge,
            "counter" => Self::Counter,
            "histogram" => Self::Histogram,
            "summary" => Self::Summary,
            "stateset" => Self::StateSet,
            "info" => Self::Info,
            "gaugehistogram" => Self::GaugeHistogram,
            _ => Self::Unknown,
        }
    }

    fn to_metric_type(self, suffix: &str) -> MetricType {
        match self {
            Self::Gauge | Self::StateSet | Self::Info | Self::Unknown => MetricType::Gauge,
            Self::Counter => MetricType::Sum {
                monotonic: true,
                temporality: Temporality::Cumulative,
            },
            Self::Histogram | Self::GaugeHistogram => {
                if suffix == "_bucket" || suffix == "_sum" || suffix == "_count" {
                    MetricType::Histogram {
                        temporality: Temporality::Cumulative,
                    }
                } else {
                    MetricType::Gauge
                }
            }
            Self::Summary => MetricType::Summary,
        }
    }
}

/// Tracks metadata for a metric family
#[derive(Debug, Default, Clone)]
struct MetricFamily {
    metric_type: OpenMetricsType,
    unit: Option<String>,
}

/// Parser state
struct Parser {
    families: HashMap<String, MetricFamily>,
    series_map: HashMap<u128, Series>,
    default_timestamp: i64,
}

impl Parser {
    fn new() -> Self {
        let default_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        Self {
            families: HashMap::new(),
            series_map: HashMap::new(),
            default_timestamp,
        }
    }

    fn parse(mut self, input: &str) -> Result<Vec<Series>> {
        let mut saw_eof = false;

        for line in input.lines() {
            let line = line.trim();

            if line.is_empty() {
                continue;
            }

            if line == "# EOF" {
                saw_eof = true;
                break;
            }

            if line.starts_with('#') {
                self.parse_metadata_line(line)?;
            } else {
                self.parse_sample_line(line)?;
            }
        }

        // Note: We don't require # EOF since Prometheus text format doesn't mandate it.
        // OpenMetrics requires it, but most exporters use Prometheus format.
        let _ = saw_eof;

        Ok(self.series_map.into_values().collect())
    }

    fn parse_metadata_line(&mut self, line: &str) -> Result<()> {
        let line = &line[1..].trim_start(); // Remove leading '#'

        if let Some(rest) = line.strip_prefix("TYPE ") {
            let parts: Vec<&str> = rest.splitn(2, ' ').collect();
            if parts.len() == 2 {
                let name = parts[0];
                let metric_type = OpenMetricsType::from_str(parts[1]);
                self.families
                    .entry(name.to_string())
                    .or_default()
                    .metric_type = metric_type;
            }
        } else if let Some(rest) = line.strip_prefix("UNIT ") {
            let parts: Vec<&str> = rest.splitn(2, ' ').collect();
            if parts.len() == 2 {
                let name = parts[0];
                let unit = parts[1];
                self.families.entry(name.to_string()).or_default().unit = Some(unit.to_string());
            }
        }
        // Ignore HELP and other comments

        Ok(())
    }

    fn parse_sample_line(&mut self, line: &str) -> Result<()> {
        // Parse: metric_name{labels} value [timestamp] [# exemplar]
        // or:    metric_name value [timestamp] [# exemplar]

        // Strip exemplar if present
        let line = if let Some(idx) = line.find(" # ") {
            &line[..idx]
        } else {
            line
        };

        let (metric_name, labels, rest) = self.parse_metric_name_and_labels(line)?;
        let (value, timestamp) = self.parse_value_and_timestamp(rest)?;

        // Determine base metric name and suffix
        let (base_name, suffix) = extract_base_name_and_suffix(&metric_name);

        // Look up metric family
        let family = self.families.get(base_name).cloned().unwrap_or_default();
        let metric_type = family.metric_type.to_metric_type(suffix);

        // Build labels with __name__ set to base metric name
        let mut series_labels = vec![Label {
            name: "__name__".to_string(),
            value: base_name.to_string(),
        }];

        // Add suffix as label if present
        if !suffix.is_empty() {
            series_labels.push(Label {
                name: "__suffix__".to_string(),
                value: suffix.to_string(),
            });
        }

        // Add parsed labels
        series_labels.extend(labels);

        let sample = Sample {
            timestamp_ms: timestamp,
            value,
        };

        // Sort labels for consistent fingerprint computation
        series_labels.sort_by(|a, b| a.name.cmp(&b.name));

        // Compute fingerprint and add sample to existing series or create new one
        let fingerprint = series_labels.fingerprint();
        self.series_map
            .entry(fingerprint)
            .and_modify(|series| series.samples.push(sample.clone()))
            .or_insert_with(|| Series {
                labels: series_labels,
                unit: family.unit.clone(),
                metric_type: Some(metric_type),
                description: None,
                samples: vec![sample],
            });

        Ok(())
    }

    fn parse_metric_name_and_labels<'a>(
        &self,
        line: &'a str,
    ) -> Result<(String, Vec<Label>, &'a str)> {
        let (name_end, labels_end) = if let Some(brace_start) = line.find('{') {
            let brace_end = find_closing_brace(line, brace_start)?;
            (brace_start, brace_end + 1)
        } else {
            let name_end = line.find(' ').unwrap_or(line.len());
            (name_end, name_end)
        };

        let metric_name = &line[..name_end];
        let labels = if name_end < labels_end {
            parse_labels(&line[name_end + 1..labels_end - 1])?
        } else {
            Vec::new()
        };

        let rest = line[labels_end..].trim_start();
        Ok((metric_name.to_string(), labels, rest))
    }

    fn parse_value_and_timestamp(&self, rest: &str) -> Result<(f64, i64)> {
        let parts: Vec<&str> = rest.split_whitespace().collect();

        if parts.is_empty() {
            return Err("Missing value in metric line".into());
        }

        let value = parse_float(parts[0])?;

        let timestamp = if parts.len() > 1 {
            // Prometheus text format uses milliseconds, OpenMetrics uses seconds.
            // Heuristic: if value < 1e12, it's likely seconds; otherwise milliseconds.
            // 1e12 ms = ~2001, so any reasonable timestamp in ms will be >= 1e12.
            let ts_raw = parse_float(parts[1])?;
            if ts_raw < 1e12 {
                // Likely seconds (OpenMetrics format)
                (ts_raw * 1000.0) as i64
            } else {
                // Likely milliseconds (Prometheus format)
                ts_raw as i64
            }
        } else {
            self.default_timestamp
        };

        Ok((value, timestamp))
    }
}

/// Find the closing brace, handling escaped characters in label values
fn find_closing_brace(s: &str, start: usize) -> Result<usize> {
    let bytes = s.as_bytes();
    let mut i = start + 1;
    let mut in_quotes = false;

    while i < bytes.len() {
        match bytes[i] {
            b'"' if i > 0 && bytes[i - 1] != b'\\' => in_quotes = !in_quotes,
            b'"' if i == start + 1 || bytes[i - 1] != b'\\' => in_quotes = !in_quotes,
            b'}' if !in_quotes => return Ok(i),
            _ => {}
        }
        i += 1;
    }

    Err("Unclosed brace in metric line".into())
}

/// Parse labels from the content between braces
fn parse_labels(s: &str) -> Result<Vec<Label>> {
    if s.is_empty() {
        return Ok(Vec::new());
    }

    let mut labels = Vec::new();
    let mut rest = s;

    while !rest.is_empty() {
        // Skip leading whitespace and commas
        rest = rest.trim_start_matches([',', ' ']);
        if rest.is_empty() {
            break;
        }

        // Find the '='
        let eq_pos = rest.find('=').ok_or("Invalid label format: missing '='")?;

        let key = rest[..eq_pos].trim();
        rest = &rest[eq_pos + 1..];

        // Expect opening quote
        if !rest.starts_with('"') {
            return Err("Expected '\"' after '=' in label".into());
        }
        rest = &rest[1..];

        // Find closing quote (handling escapes)
        let (value, remaining) = parse_quoted_string(rest)?;
        rest = remaining;

        labels.push(Label {
            name: key.to_string(),
            value,
        });
    }

    Ok(labels)
}

/// Parse a quoted string, handling escape sequences
fn parse_quoted_string(s: &str) -> Result<(String, &str)> {
    let mut result = String::new();
    let mut chars = s.char_indices();

    while let Some((i, c)) = chars.next() {
        match c {
            '"' => {
                return Ok((result, &s[i + 1..]));
            }
            '\\' => {
                if let Some((_, escaped)) = chars.next() {
                    match escaped {
                        'n' => result.push('\n'),
                        '"' => result.push('"'),
                        '\\' => result.push('\\'),
                        other => {
                            result.push('\\');
                            result.push(other);
                        }
                    }
                }
            }
            _ => result.push(c),
        }
    }

    Err("Unterminated quoted string".into())
}

/// Parse a float value, handling special values
fn parse_float(s: &str) -> Result<f64> {
    match s.to_lowercase().as_str() {
        "nan" => Ok(f64::NAN),
        "+inf" | "inf" => Ok(f64::INFINITY),
        "-inf" => Ok(f64::NEG_INFINITY),
        _ => s.parse::<f64>().map_err(|_| "Invalid float value".into()),
    }
}

/// Extract base metric name and suffix from a metric name
fn extract_base_name_and_suffix(name: &str) -> (&str, &str) {
    // Check for known suffixes in order of specificity
    let suffixes = [
        "_total", "_created", "_bucket", "_count", "_sum", "_info", "_gcount", "_gsum",
    ];

    for suffix in suffixes {
        if let Some(base) = name.strip_suffix(suffix) {
            return (base, suffix);
        }
    }

    (name, "")
}

/// Parse OpenMetrics text format into series for ingestion.
///
/// # Arguments
/// * `input` - OpenMetrics text format string
///
/// # Returns
/// A vector of `Series` ready for ingestion.
///
/// # Example
/// ```ignore
/// let input = r#"
/// # TYPE http_requests counter
/// http_requests_total{method="GET"} 1234 1700000000
/// # EOF
/// "#;
/// let samples = parse_openmetrics(input)?;
/// ```
pub(crate) fn parse_openmetrics(input: &str) -> Result<Vec<Series>> {
    Parser::new().parse(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    // ==================== METRIC TYPE PARSING ====================

    #[rstest]
    #[case::gauge("gauge", "test 1\n# EOF", MetricType::Gauge)]
    #[case::gauge_uppercase("GAUGE", "test 1\n# EOF", MetricType::Gauge)]
    #[case::gauge_mixed_case("GaUgE", "test 1\n# EOF", MetricType::Gauge)]
    #[case::counter("counter", "test_total 1\n# EOF", MetricType::Sum { monotonic: true, temporality: Temporality::Cumulative })]
    #[case::counter_uppercase("COUNTER", "test_total 1\n# EOF", MetricType::Sum { monotonic: true, temporality: Temporality::Cumulative })]
    #[case::summary("summary", "test 1\n# EOF", MetricType::Summary)]
    #[case::stateset("stateset", "test 1\n# EOF", MetricType::Gauge)]
    #[case::info("info", "test 1\n# EOF", MetricType::Gauge)]
    #[case::unknown("unknown", "test 1\n# EOF", MetricType::Gauge)]
    #[case::invalid_type("notarealtype", "test 1\n# EOF", MetricType::Gauge)]
    fn should_parse_metric_types(
        #[case] type_str: &str,
        #[case] body: &str,
        #[case] expected_type: MetricType,
    ) {
        // given
        let input = format!("# TYPE test {}\n{}", type_str, body);

        // when
        let samples = parse_openmetrics(&input).unwrap();

        // then
        assert_eq!(samples.len(), 1);
        assert!(
            std::mem::discriminant(&samples[0].metric_type.unwrap())
                == std::mem::discriminant(&expected_type),
            "Expected {:?}, got {:?}",
            expected_type,
            samples[0].metric_type
        );
    }

    #[rstest]
    #[case::histogram_bucket("histogram", "test_bucket{le=\"1\"} 10\n# EOF", "_bucket", MetricType::Histogram { temporality: Temporality::Cumulative })]
    #[case::histogram_sum("histogram", "test_sum 100\n# EOF", "_sum", MetricType::Histogram { temporality: Temporality::Cumulative })]
    #[case::histogram_count("histogram", "test_count 50\n# EOF", "_count", MetricType::Histogram { temporality: Temporality::Cumulative })]
    #[case::gaugehistogram_bucket("gaugehistogram", "test_bucket{le=\"1\"} 10\n# EOF", "_bucket", MetricType::Histogram { temporality: Temporality::Cumulative })]
    #[case::gaugehistogram_gsum(
        "gaugehistogram",
        "test_gsum 100\n# EOF",
        "_gsum",
        MetricType::Gauge
    )]
    #[case::gaugehistogram_gcount(
        "gaugehistogram",
        "test_gcount 50\n# EOF",
        "_gcount",
        MetricType::Gauge
    )]
    fn should_parse_histogram_suffixes(
        #[case] type_str: &str,
        #[case] body: &str,
        #[case] expected_suffix: &str,
        #[case] expected_type: MetricType,
    ) {
        // given
        let input = format!("# TYPE test {}\n{}", type_str, body);

        // when
        let samples = parse_openmetrics(&input).unwrap();

        // then
        assert_eq!(samples.len(), 1);
        let suffix = samples[0]
            .labels
            .iter()
            .find(|a| a.name == "__suffix__")
            .map(|a| a.value.as_str())
            .unwrap_or("");
        assert_eq!(suffix, expected_suffix);
        assert!(
            std::mem::discriminant(&samples[0].metric_type.unwrap())
                == std::mem::discriminant(&expected_type)
        );
    }

    // ==================== VALUE PARSING ====================

    #[rstest]
    #[case::integer("42", 42.0)]
    #[case::negative_integer("-42", -42.0)]
    #[case::float("1.234", 1.234)]
    #[case::negative_float("-5.678", -5.678)]
    #[case::zero("0", 0.0)]
    #[case::negative_zero("-0", 0.0)]
    #[case::small_decimal("0.001", 0.001)]
    #[case::large_number("1000000000", 1_000_000_000.0)]
    #[case::scientific_positive("1.5e10", 1.5e10)]
    #[case::scientific_negative("1.5e-10", 1.5e-10)]
    #[case::scientific_uppercase("1.5E10", 1.5e10)]
    fn should_parse_numeric_values(#[case] value_str: &str, #[case] expected: f64) {
        // given
        let input = format!("test {}\n# EOF", value_str);

        // when
        let samples = parse_openmetrics(&input).unwrap();

        // then
        assert_eq!(samples.len(), 1);
        assert!(
            (samples[0].samples[0].value - expected).abs() < f64::EPSILON,
            "Expected {}, got {}",
            expected,
            samples[0].samples[0].value
        );
    }

    #[rstest]
    #[case::nan_lowercase("nan")]
    #[case::nan_uppercase("NaN")]
    #[case::nan_allcaps("NAN")]
    fn should_parse_nan_values(#[case] value_str: &str) {
        // given
        let input = format!("test {}\n# EOF", value_str);

        // when
        let samples = parse_openmetrics(&input).unwrap();

        // then
        assert!(samples[0].samples[0].value.is_nan());
    }

    #[rstest]
    #[case::inf_plus("+Inf", f64::INFINITY)]
    #[case::inf_no_sign("Inf", f64::INFINITY)]
    #[case::inf_uppercase("+INF", f64::INFINITY)]
    #[case::inf_lowercase("+inf", f64::INFINITY)]
    #[case::neg_inf("-Inf", f64::NEG_INFINITY)]
    #[case::neg_inf_uppercase("-INF", f64::NEG_INFINITY)]
    #[case::neg_inf_lowercase("-inf", f64::NEG_INFINITY)]
    fn should_parse_infinity_values(#[case] value_str: &str, #[case] expected: f64) {
        // given
        let input = format!("test {}\n# EOF", value_str);

        // when
        let samples = parse_openmetrics(&input).unwrap();

        // then
        assert_eq!(samples[0].samples[0].value, expected);
    }

    // ==================== TIMESTAMP PARSING ====================

    #[rstest]
    // OpenMetrics format (seconds) - values < 1e12 are treated as seconds
    #[case::openmetrics_integer_seconds("1700000000", 1700000000000)]
    #[case::openmetrics_float_seconds("1700000000.5", 1700000000500)]
    #[case::openmetrics_float_seconds_millis("1700000000.123", 1700000000123)]
    #[case::zero_timestamp("0", 0)]
    // Prometheus format (milliseconds) - values >= 1e12 are treated as milliseconds
    #[case::prometheus_milliseconds("1700000000000", 1700000000000)]
    #[case::prometheus_milliseconds_recent("1702500000000", 1702500000000)]
    fn should_parse_timestamps(#[case] ts_str: &str, #[case] expected_ms: i64) {
        // given
        let input = format!("test 1 {}\n# EOF", ts_str);

        // when
        let samples = parse_openmetrics(&input).unwrap();

        // then
        assert_eq!(samples[0].samples[0].timestamp_ms, expected_ms);
    }

    #[test]
    fn should_use_current_time_when_timestamp_missing() {
        // given
        let input = "test 1\n# EOF";
        let before = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        // when
        let samples = parse_openmetrics(input).unwrap();

        // then
        let after = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        assert!(samples[0].samples[0].timestamp_ms >= before);
        assert!(samples[0].samples[0].timestamp_ms <= after);
    }

    // ==================== LABEL PARSING ====================

    #[rstest]
    #[case::no_labels("test 1\n# EOF", vec![])]
    #[case::empty_braces("test{} 1\n# EOF", vec![])]
    #[case::single_label("test{foo=\"bar\"} 1\n# EOF", vec![("foo", "bar")])]
    #[case::multiple_labels("test{a=\"1\",b=\"2\",c=\"3\"} 1\n# EOF", vec![("a", "1"), ("b", "2"), ("c", "3")])]
    #[case::label_with_spaces("test{ foo=\"bar\" } 1\n# EOF", vec![("foo", "bar")])]
    #[case::empty_label_value("test{foo=\"\"} 1\n# EOF", vec![("foo", "")])]
    #[case::label_with_numbers("test{code=\"200\"} 1\n# EOF", vec![("code", "200")])]
    #[case::label_with_underscore("test{my_label=\"value\"} 1\n# EOF", vec![("my_label", "value")])]
    fn should_parse_labels(#[case] input: &str, #[case] expected_labels: Vec<(&str, &str)>) {
        // when
        let samples = parse_openmetrics(input).unwrap();

        // then
        assert_eq!(samples.len(), 1);
        for (key, value) in expected_labels {
            let found = samples[0]
                .labels
                .iter()
                .find(|a| a.name == key)
                .map(|a| a.value.as_str());
            assert_eq!(found, Some(value), "Label {} not found or wrong value", key);
        }
    }

    #[rstest]
    #[case::escaped_quote("test{a=\"foo\\\"bar\"} 1\n# EOF", "a", "foo\"bar")]
    #[case::escaped_backslash("test{a=\"foo\\\\bar\"} 1\n# EOF", "a", "foo\\bar")]
    #[case::escaped_newline("test{a=\"foo\\nbar\"} 1\n# EOF", "a", "foo\nbar")]
    #[case::multiple_escapes("test{a=\"a\\\"b\\\\c\\nd\"} 1\n# EOF", "a", "a\"b\\c\nd")]
    #[case::escape_at_end("test{a=\"foo\\\"\"} 1\n# EOF", "a", "foo\"")]
    #[case::only_escape("test{a=\"\\\"\"} 1\n# EOF", "a", "\"")]
    fn should_parse_escaped_label_values(
        #[case] input: &str,
        #[case] label_name: &str,
        #[case] expected: &str,
    ) {
        // when
        let samples = parse_openmetrics(input).unwrap();

        // then
        let value = samples[0]
            .labels
            .iter()
            .find(|a| a.name == label_name)
            .map(|a| a.value.as_str())
            .unwrap();
        assert_eq!(value, expected);
    }

    // ==================== SUFFIX EXTRACTION ====================

    #[rstest]
    #[case::total_suffix("test_total", "test", "_total")]
    #[case::created_suffix("test_created", "test", "_created")]
    #[case::bucket_suffix("test_bucket", "test", "_bucket")]
    #[case::count_suffix("test_count", "test", "_count")]
    #[case::sum_suffix("test_sum", "test", "_sum")]
    #[case::info_suffix("test_info", "test", "_info")]
    #[case::gcount_suffix("test_gcount", "test", "_gcount")]
    #[case::gsum_suffix("test_gsum", "test", "_gsum")]
    #[case::no_suffix("test", "test", "")]
    #[case::underscore_in_name("my_test_metric", "my_test_metric", "")]
    #[case::total_in_middle("total_requests", "total_requests", "")]
    fn should_extract_suffix(
        #[case] metric_name: &str,
        #[case] expected_base: &str,
        #[case] expected_suffix: &str,
    ) {
        // when
        let (base, suffix) = extract_base_name_and_suffix(metric_name);

        // then
        assert_eq!(base, expected_base);
        assert_eq!(suffix, expected_suffix);
    }

    // ==================== UNIT PARSING ====================

    #[rstest]
    #[case::with_unit(
        "# TYPE test gauge\n# UNIT test seconds\ntest 1\n# EOF",
        Some("seconds")
    )]
    #[case::without_unit("# TYPE test gauge\ntest 1\n# EOF", None)]
    #[case::unit_before_type(
        "# UNIT test seconds\n# TYPE test gauge\ntest 1\n# EOF",
        Some("seconds")
    )]
    #[case::unit_with_underscore(
        "# TYPE test gauge\n# UNIT test milli_seconds\ntest 1\n# EOF",
        Some("milli_seconds")
    )]
    fn should_parse_unit(#[case] input: &str, #[case] expected_unit: Option<&str>) {
        // when
        let samples = parse_openmetrics(input).unwrap();

        // then
        assert_eq!(samples[0].unit.as_deref(), expected_unit);
    }

    // ==================== COMMENTS AND EMPTY LINES ====================

    #[rstest]
    #[case::empty_lines("test 1\n\n\n# EOF")]
    #[case::comment_line("# This is a comment\ntest 1\n# EOF")]
    #[case::help_line("# HELP test A test metric\ntest 1\n# EOF")]
    #[case::multiple_comments("# Comment 1\n# Comment 2\ntest 1\n# EOF")]
    #[case::whitespace_lines("  \n\t\ntest 1\n# EOF")]
    fn should_ignore_comments_and_empty_lines(#[case] input: &str) {
        // when
        let samples = parse_openmetrics(input).unwrap();

        // then
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].samples[0].value, 1.0);
    }

    // ==================== EXEMPLAR HANDLING ====================

    #[rstest]
    #[case::simple_exemplar("test 100 # {trace_id=\"abc\"} 1.0\n# EOF", 100.0)]
    #[case::exemplar_with_timestamp(
        "test 100 1700000000 # {trace_id=\"abc\"} 1.0 1700000001\n# EOF",
        100.0
    )]
    #[case::exemplar_multiple_labels(
        "test 100 # {trace_id=\"abc\",span_id=\"def\"} 1.0\n# EOF",
        100.0
    )]
    fn should_ignore_exemplars(#[case] input: &str, #[case] expected_value: f64) {
        // when
        let samples = parse_openmetrics(input).unwrap();

        // then
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].samples[0].value, expected_value);
    }

    // ==================== EOF HANDLING ====================

    #[rstest]
    #[case::eof_with_newline("test 1\n# EOF\n")]
    #[case::eof_without_newline("test 1\n# EOF")]
    #[case::eof_with_multiple_newlines("test 1\n# EOF\n\n\n")]
    fn should_accept_valid_eof(#[case] input: &str) {
        // when
        let result = parse_openmetrics(input);

        // then
        assert!(result.is_ok());
    }

    #[rstest]
    #[case::no_eof("test 1\n")]
    #[case::eof_lowercase("test 1\n# eof\n")]
    #[case::eof_no_space("test 1\n#EOF\n")]
    #[case::eof_extra_text("test 1\n# EOF extra\n")]
    fn should_accept_prometheus_format_without_strict_eof(#[case] input: &str) {
        // Prometheus text format doesn't require # EOF, so these should all parse
        // when
        let result = parse_openmetrics(input);

        // then
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 1);
    }

    // ==================== ERROR CASES ====================

    #[rstest]
    #[case::missing_value("test\n# EOF", "Missing value")]
    #[case::unclosed_brace("test{foo=\"bar\" 1\n# EOF", "Unclosed brace")]
    #[case::unterminated_string("test{foo=\"bar} 1\n# EOF", "Unclosed brace")]
    #[case::missing_equals("test{foo\"bar\"} 1\n# EOF", "missing '='")]
    #[case::missing_quote("test{foo=bar} 1\n# EOF", "Expected '\"'")]
    #[case::invalid_float("test abc\n# EOF", "Invalid float")]
    fn should_return_error_for_invalid_input(#[case] input: &str, #[case] expected_error: &str) {
        // when
        let result = parse_openmetrics(input);

        // then
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains(expected_error),
            "Expected error containing '{}', got '{}'",
            expected_error,
            err
        );
    }

    // ==================== MULTIPLE SAMPLES ====================

    #[test]
    fn should_parse_multiple_samples() {
        // given
        let input = r#"# TYPE requests counter
requests_total{method="GET"} 100
requests_total{method="POST"} 50
requests_total{method="PUT"} 25
# EOF
"#;

        // when
        let series_list = parse_openmetrics(input).unwrap();

        // then - 3 different series (different method labels)
        assert_eq!(series_list.len(), 3);

        // Find each series by method label and verify value
        let get_series = series_list
            .iter()
            .find(|s| {
                s.labels
                    .iter()
                    .any(|l| l.name == "method" && l.value == "GET")
            })
            .unwrap();
        assert_eq!(get_series.samples[0].value, 100.0);

        let post_series = series_list
            .iter()
            .find(|s| {
                s.labels
                    .iter()
                    .any(|l| l.name == "method" && l.value == "POST")
            })
            .unwrap();
        assert_eq!(post_series.samples[0].value, 50.0);

        let put_series = series_list
            .iter()
            .find(|s| {
                s.labels
                    .iter()
                    .any(|l| l.name == "method" && l.value == "PUT")
            })
            .unwrap();
        assert_eq!(put_series.samples[0].value, 25.0);
    }

    #[test]
    fn should_parse_multiple_metric_families() {
        // given
        let input = r#"# TYPE requests counter
requests_total 100
# TYPE latency gauge
latency 0.5
# TYPE errors counter
errors_total 5
# EOF
"#;

        // when
        let samples = parse_openmetrics(input).unwrap();

        // then
        assert_eq!(samples.len(), 3);

        let requests = samples
            .iter()
            .find(|s| {
                s.labels
                    .iter()
                    .any(|a| a.name == "__name__" && a.value == "requests")
            })
            .unwrap();
        assert!(matches!(
            requests.metric_type,
            Some(MetricType::Sum {
                monotonic: true,
                ..
            })
        ));

        let latency = samples
            .iter()
            .find(|s| {
                s.labels
                    .iter()
                    .any(|a| a.name == "__name__" && a.value == "latency")
            })
            .unwrap();
        assert!(matches!(latency.metric_type, Some(MetricType::Gauge)));
    }

    // ==================== HISTOGRAM FULL EXAMPLE ====================

    #[test]
    fn should_parse_complete_histogram() {
        // given
        let input = r#"# TYPE http_request_duration histogram
# UNIT http_request_duration seconds
http_request_duration_bucket{le="0.01"} 10
http_request_duration_bucket{le="0.05"} 50
http_request_duration_bucket{le="0.1"} 100
http_request_duration_bucket{le="0.5"} 200
http_request_duration_bucket{le="1"} 250
http_request_duration_bucket{le="+Inf"} 300
http_request_duration_sum 150.5
http_request_duration_count 300
# EOF
"#;

        // when
        let samples = parse_openmetrics(input).unwrap();

        // then
        assert_eq!(samples.len(), 8);

        // All should have the unit
        for sample in &samples {
            assert_eq!(sample.unit, Some("seconds".to_string()));
        }

        // 6 buckets
        let buckets: Vec<_> = samples
            .iter()
            .filter(|s| {
                s.labels
                    .iter()
                    .any(|a| a.name == "__suffix__" && a.value == "_bucket")
            })
            .collect();
        assert_eq!(buckets.len(), 6);

        // Check +Inf bucket
        let inf_bucket = buckets
            .iter()
            .find(|s| s.labels.iter().any(|a| a.name == "le" && a.value == "+Inf"))
            .unwrap();
        assert_eq!(inf_bucket.samples[0].value, 300.0);
    }

    // ==================== SUMMARY FULL EXAMPLE ====================

    #[test]
    fn should_parse_complete_summary() {
        // given
        let input = r#"# TYPE rpc_duration summary
rpc_duration{quantile="0.5"} 0.05
rpc_duration{quantile="0.9"} 0.08
rpc_duration{quantile="0.99"} 0.1
rpc_duration_sum 1000.5
rpc_duration_count 10000
# EOF
"#;

        // when
        let samples = parse_openmetrics(input).unwrap();

        // then
        assert_eq!(samples.len(), 5);

        // Quantile samples should have Summary type
        let quantiles: Vec<_> = samples
            .iter()
            .filter(|s| s.labels.iter().any(|a| a.name == "quantile"))
            .collect();
        assert_eq!(quantiles.len(), 3);
        for q in &quantiles {
            assert!(matches!(q.metric_type, Some(MetricType::Summary)));
        }
    }

    // ==================== METRIC NAME FORMATS ====================

    #[rstest]
    #[case::simple_name("foo 1\n# EOF", "foo")]
    #[case::with_underscore("foo_bar 1\n# EOF", "foo_bar")]
    #[case::with_colon("foo:bar 1\n# EOF", "foo:bar")]
    #[case::with_numbers("foo123 1\n# EOF", "foo123")]
    #[case::starts_with_underscore("_foo 1\n# EOF", "_foo")]
    #[case::complex_name(
        "http_request_duration_seconds 1\n# EOF",
        "http_request_duration_seconds"
    )]
    fn should_parse_metric_names(#[case] input: &str, #[case] expected_name: &str) {
        // when
        let samples = parse_openmetrics(input).unwrap();

        // then
        let name = samples[0]
            .labels
            .iter()
            .find(|a| a.name == "__name__")
            .map(|a| a.value.as_str())
            .unwrap();
        assert_eq!(name, expected_name);
    }

    // ==================== WHITESPACE HANDLING ====================

    #[rstest]
    #[case::leading_whitespace("  test 1\n# EOF")]
    #[case::trailing_whitespace("test 1  \n# EOF")]
    #[case::multiple_spaces_before_value("test   1\n# EOF")]
    fn should_handle_whitespace(#[case] input: &str) {
        // when
        let result = parse_openmetrics(input);

        // then
        assert!(result.is_ok());
        assert_eq!(result.unwrap()[0].samples[0].value, 1.0);
    }

    // ==================== PROMETHEUS FORMAT COMPATIBILITY ====================

    #[test]
    fn should_parse_counter_without_total_suffix() {
        // Prometheus text format doesn't require _total suffix for counters
        // given
        let input = "# TYPE http_requests counter\nhttp_requests 100\n";

        // when
        let samples = parse_openmetrics(input).unwrap();

        // then
        assert_eq!(samples.len(), 1);
        assert!(matches!(
            samples[0].metric_type,
            Some(MetricType::Sum {
                monotonic: true,
                ..
            })
        ));
        let name = samples[0]
            .labels
            .iter()
            .find(|a| a.name == "__name__")
            .map(|a| a.value.as_str())
            .unwrap();
        assert_eq!(name, "http_requests");
    }

    #[test]
    fn should_parse_counter_with_total_suffix() {
        // OpenMetrics requires _total suffix for counters
        // given
        let input = "# TYPE http_requests counter\nhttp_requests_total 100\n";

        // when
        let samples = parse_openmetrics(input).unwrap();

        // then
        assert_eq!(samples.len(), 1);
        assert!(matches!(
            samples[0].metric_type,
            Some(MetricType::Sum {
                monotonic: true,
                ..
            })
        ));
        // Base name should have _total stripped
        let name = samples[0]
            .labels
            .iter()
            .find(|a| a.name == "__name__")
            .map(|a| a.value.as_str())
            .unwrap();
        assert_eq!(name, "http_requests");
    }

    // ==================== EDGE CASES ====================

    #[test]
    fn should_parse_metric_without_type_declaration() {
        // given
        let input = "untyped_metric 42\n# EOF";

        // when
        let samples = parse_openmetrics(input).unwrap();

        // then
        assert_eq!(samples.len(), 1);
        assert!(matches!(samples[0].metric_type, Some(MetricType::Gauge)));
    }

    #[test]
    fn should_handle_type_declaration_for_unused_metric() {
        // given
        let input = "# TYPE unused counter\nother_metric 1\n# EOF";

        // when
        let samples = parse_openmetrics(input).unwrap();

        // then
        assert_eq!(samples.len(), 1);
        // other_metric has no type declaration, so defaults to gauge
        assert!(matches!(samples[0].metric_type, Some(MetricType::Gauge)));
    }

    #[test]
    fn should_parse_only_eof() {
        // given
        let input = "# EOF";

        // when
        let samples = parse_openmetrics(input).unwrap();

        // then
        assert!(samples.is_empty());
    }

    #[test]
    fn should_parse_label_with_brace_in_value() {
        // given - brace inside quoted string should not close labels
        let input = "test{msg=\"hello}world\"} 1\n# EOF";

        // when
        let samples = parse_openmetrics(input).unwrap();

        // then
        let msg = samples[0]
            .labels
            .iter()
            .find(|a| a.name == "msg")
            .map(|a| a.value.as_str())
            .unwrap();
        assert_eq!(msg, "hello}world");
    }
}
