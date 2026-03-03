//! Benchmark runner for PromQL queries using `TimeSeriesDbReader`.
//!
//! Opens a read-only reader (no fencing) against the configured storage
//! backend and runs a set of dashboard queries, outputting timing results
//! as CSV to stdout.
//!
//! # Usage
//!
//! ```bash
//! # Configure storage via OpenDataTimeSeries.yaml or env vars
//! cargo run --example query_bench --release -- [--config path/to/config.yaml]
//! ```

use std::time::{Duration, Instant, SystemTime};

use clap::Parser;
use timeseries::{ReaderConfig, TimeSeriesDbReader};

/// Dashboard queries to benchmark.
const QUERIES: &[(&str, &str)] = &[
    ("instant_simple", "up"),
    ("instant_rate", "rate(http_requests_total[5m])"),
    (
        "instant_sum_rate",
        "sum(rate(http_requests_total[5m])) by (status)",
    ),
    (
        "instant_histogram",
        "histogram_quantile(0.99, sum(rate(http_request_duration_seconds_bucket[5m])) by (le))",
    ),
    (
        "instant_topk",
        "topk(10, sum(rate(http_requests_total[5m])) by (handler))",
    ),
    ("instant_absent", "absent(nonexistent_metric)"),
    ("instant_scalar", "scalar(sum(up))"),
    ("instant_count", "count(up) by (job)"),
    (
        "instant_avg",
        "avg(rate(http_requests_total[5m])) by (method)",
    ),
    ("range_rate", "rate(http_requests_total[5m])"),
    (
        "range_sum_rate",
        "sum(rate(http_requests_total[5m])) by (status)",
    ),
    (
        "range_histogram",
        "histogram_quantile(0.99, sum(rate(http_request_duration_seconds_bucket[5m])) by (le))",
    ),
    (
        "range_topk",
        "topk(10, sum(rate(http_requests_total[5m])) by (handler))",
    ),
    ("range_count", "count(up) by (job)"),
    (
        "range_avg",
        "avg(rate(http_requests_total[5m])) by (method)",
    ),
    ("series_discovery", "{job=~\".+\"}"),
    ("label_names", ""),
    ("label_values", "__name__"),
];

#[derive(Parser)]
#[command(about = "Benchmark PromQL queries using TimeSeriesDbReader")]
struct Args {
    /// Path to ReaderConfig YAML file.
    #[arg(long, default_value = "OpenDataTimeSeries.yaml")]
    config: String,

    /// Query range duration in seconds (for range queries).
    #[arg(long, default_value_t = 3600)]
    range_secs: u64,

    /// Step interval in seconds (for range queries).
    #[arg(long, default_value_t = 15)]
    step_secs: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    let config_str = std::fs::read_to_string(&args.config)?;
    let config: ReaderConfig = serde_yaml::from_str(&config_str)?;

    let reader = TimeSeriesDbReader::open(config).await?;

    let now = SystemTime::now();
    let range_start = now - Duration::from_secs(args.range_secs);
    let step = Duration::from_secs(args.step_secs);

    println!("query_name,query_type,duration_ms,result_count");

    for (name, query) in QUERIES {
        let start = Instant::now();

        let (query_type, err, count): (&str, Option<String>, usize) =
            if name.starts_with("instant_") {
                let r = reader.query(query, Some(now)).await;
                let count = match &r {
                    Ok(timeseries::QueryValue::Vector(v)) => v.len(),
                    Ok(timeseries::QueryValue::Matrix(m)) => m.len(),
                    Ok(timeseries::QueryValue::Scalar { .. }) => 1,
                    Err(_) => 0,
                };
                ("instant", r.err().map(|e| e.to_string()), count)
            } else if name.starts_with("range_") {
                let r = reader.query_range(query, range_start..=now, step).await;
                (
                    "range",
                    r.as_ref().err().map(|e| e.to_string()),
                    r.as_ref().map_or(0, |v| v.len()),
                )
            } else if *name == "series_discovery" {
                let r = reader.series(&[query], range_start..=now).await;
                (
                    "series",
                    r.as_ref().err().map(|e| e.to_string()),
                    r.as_ref().map_or(0, |v| v.len()),
                )
            } else if *name == "label_names" {
                let r = reader.labels(None, range_start..=now).await;
                (
                    "labels",
                    r.as_ref().err().map(|e| e.to_string()),
                    r.as_ref().map_or(0, |v| v.len()),
                )
            } else if *name == "label_values" {
                let r = reader.label_values(query, None, range_start..=now).await;
                (
                    "label_values",
                    r.as_ref().err().map(|e| e.to_string()),
                    r.as_ref().map_or(0, |v| v.len()),
                )
            } else {
                continue;
            };

        let elapsed = start.elapsed();
        println!(
            "{},{},{:.3},{}",
            name,
            query_type,
            elapsed.as_secs_f64() * 1000.0,
            count
        );
        if let Some(e) = err {
            eprintln!("  error: {}", e);
        }
    }

    Ok(())
}
