use axum::{extract::State, http::StatusCode};
use bytes::Bytes;

#[cfg(feature = "otlp")]
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
#[cfg(feature = "otlp")]
use opentelemetry_proto::tonic::common::v1::any_value::Value as OtlpValue;
#[cfg(feature = "otlp")]
use prost::Message;

use crate::model::{Label, MetricType, Sample, Series};
use crate::promql::server::AppState;

#[cfg(feature = "otlp")]
fn decode_otlp_request(body: &[u8]) -> Result<ExportMetricsServiceRequest, prost::DecodeError> {
    ExportMetricsServiceRequest::decode(body)
}

/// Convert an OTLP AnyValue to a String representation.
#[cfg(feature = "otlp")]
fn otlp_value_to_string(value: &opentelemetry_proto::tonic::common::v1::AnyValue) -> String {
    match &value.value {
        Some(OtlpValue::StringValue(s)) => s.clone(),
        Some(OtlpValue::BoolValue(b)) => b.to_string(),
        Some(OtlpValue::IntValue(i)) => i.to_string(),
        Some(OtlpValue::DoubleValue(d)) => d.to_string(),
        Some(OtlpValue::BytesValue(b)) => format!("{:?}", b),
        Some(OtlpValue::ArrayValue(arr)) => format!("{:?}", arr),
        Some(OtlpValue::KvlistValue(kv)) => format!("{:?}", kv),
        None => String::new(),
    }
}

#[cfg(feature = "otlp")]
fn convert_otlp_request(req: ExportMetricsServiceRequest) -> Vec<Series> {
    let mut series_list = Vec::new();

    for resource_metrics in req.resource_metrics {
        for scope_metrics in resource_metrics.scope_metrics {
            for metric in scope_metrics.metrics {
                if let Some(opentelemetry_proto::tonic::metrics::v1::metric::Data::Gauge(gauge)) =
                    metric.data
                {
                    for datapoint in gauge.data_points {
                        // Extract timestamp (convert nanoseconds to milliseconds)
                        let timestamp_ms = (datapoint.time_unix_nano / 1_000_000) as i64;

                        // Extract value
                        let value = match datapoint.value {
                            Some(opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsDouble(v)) => v,
                            Some(opentelemetry_proto::tonic::metrics::v1::number_data_point::Value::AsInt(v)) => v as f64,
                            _ => continue,
                        };

                        // Convert datapoint attributes to labels
                        let mut labels: Vec<Label> = datapoint
                            .attributes
                            .iter()
                            .map(|kv| Label {
                                name: kv.key.clone(),
                                value: kv
                                    .value
                                    .as_ref()
                                    .map(otlp_value_to_string)
                                    .unwrap_or_default(),
                            })
                            .collect();

                        // Add metric name as label
                        labels.push(Label::metric_name(metric.name.clone()));

                        // Sort labels for consistent fingerprinting
                        labels.sort();

                        // Create sample
                        let sample = Sample::new(timestamp_ms, value);

                        // Create series
                        let series = Series {
                            labels,
                            metric_type: Some(MetricType::Gauge),
                            unit: if metric.unit.is_empty() {
                                None
                            } else {
                                Some(metric.unit.clone())
                            },
                            description: if metric.description.is_empty() {
                                None
                            } else {
                                Some(metric.description.clone())
                            },
                            samples: vec![sample],
                        };

                        series_list.push(series);
                    }
                }
            }
        }
    }

    series_list
}

pub async fn handle_otlp_metrics(State(state): State<AppState>, body: Bytes) -> StatusCode {
    #[cfg(feature = "otlp")]
    {
        let request = match decode_otlp_request(&body) {
            Ok(req) => req,
            Err(_) => return StatusCode::BAD_REQUEST,
        };

        let series = convert_otlp_request(request);

        if state
            .tsdb
            .ingest_samples(series, state.flush_interval_secs)
            .await
            .is_err()
        {
            return StatusCode::INTERNAL_SERVER_ERROR;
        }
    }

    StatusCode::OK
}
