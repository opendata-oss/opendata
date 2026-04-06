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
use common::StorageConfig;
use serde::{Deserialize, Deserializer};
use slatedb::config::DbReaderOptions;
use timeseries::TimeSeriesDbReader;

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
    /// Path to benchmark config YAML file.
    #[arg(long, default_value = "OpenDataTimeSeries.yaml")]
    config: String,

    /// Query range duration in seconds (for range queries).
    #[arg(long, default_value_t = 3600)]
    range_secs: u64,

    /// Step interval in seconds (for range queries).
    #[arg(long, default_value_t = 15)]
    step_secs: u64,
}

#[derive(Deserialize)]
struct BenchConfig {
    storage: StorageConfig,
    #[serde(
        default = "default_reader_options",
        deserialize_with = "deserialize_reader_options"
    )]
    reader: DbReaderOptions,
    #[serde(default = "default_cache_capacity")]
    cache_capacity: u64,
}

fn default_reader_options() -> DbReaderOptions {
    DbReaderOptions {
        skip_wal_replay: true,
        ..DbReaderOptions::default()
    }
}

fn default_cache_capacity() -> u64 {
    50
}

fn deserialize_reader_options<'de, D>(
    deserializer: D,
) -> std::result::Result<DbReaderOptions, D::Error>
where
    D: Deserializer<'de>,
{
    let overrides = serde_yaml::Value::deserialize(deserializer)?;
    let mut defaults =
        serde_yaml::to_value(default_reader_options()).map_err(serde::de::Error::custom)?;
    merge_yaml_value(&mut defaults, overrides);
    serde_yaml::from_value(defaults).map_err(serde::de::Error::custom)
}

fn merge_yaml_value(base: &mut serde_yaml::Value, overrides: serde_yaml::Value) {
    match (base, overrides) {
        (serde_yaml::Value::Mapping(base_map), serde_yaml::Value::Mapping(overrides_map)) => {
            for (key, value) in overrides_map {
                match base_map.get_mut(&key) {
                    Some(existing) => merge_yaml_value(existing, value),
                    None => {
                        base_map.insert(key, value);
                    }
                }
            }
        }
        (base_slot, override_value) => *base_slot = override_value,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    let config_str = std::fs::read_to_string(&args.config)?;
    let config: BenchConfig = serde_yaml::from_str(&config_str)?;

    let reader =
        TimeSeriesDbReader::open(config.storage, config.reader, config.cache_capacity).await?;

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
