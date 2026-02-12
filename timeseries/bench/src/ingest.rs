//! Ingest throughput benchmark for the tsdb database.

use std::sync::Arc;

use bencher::{Bench, Benchmark, Params, Summary};
use common::Storage;
use common::storage::in_memory::InMemoryStorage;
use timeseries::storage::merge_operator::OpenTsdbMergeOperator;
use timeseries::tsdb::Tsdb;
use timeseries::{Label, Sample, Series};

const MICROS_PER_SEC: f64 = 1_000_000.0;

/// Create a parameter set for the ingest benchmark.
fn make_params(num_series: usize, num_labels: usize, num_samples: usize) -> Params {
    let mut params = Params::new();
    params.insert("num_series", num_series.to_string());
    params.insert("num_labels", num_labels.to_string());
    params.insert("num_samples", num_samples.to_string());
    params
}

/// Benchmark for tsdb ingest throughput.
pub struct IngestBenchmark;

impl IngestBenchmark {
    pub fn new() -> Self {
        Self
    }
}

impl Default for IngestBenchmark {
    fn default() -> Self {
        Self::new()
    }
}

async fn create_test_storage() -> Arc<dyn Storage> {
    Arc::new(InMemoryStorage::with_merge_operator(Arc::new(
        OpenTsdbMergeOperator,
    )))
}

#[async_trait::async_trait]
impl Benchmark for IngestBenchmark {
    fn name(&self) -> &str {
        "ingest"
    }

    fn default_params(&self) -> Vec<Params> {
        vec![
            // Vary num_series
            make_params(10, 5, 100),
            make_params(100, 5, 100),
            make_params(1000, 5, 100),
            // Vary num_labels
            make_params(100, 2, 100),
            make_params(100, 10, 100),
            make_params(100, 20, 100),
            // Vary num_samples
            make_params(100, 5, 10),
            make_params(100, 5, 1000),
        ]
    }

    async fn run(&self, bench: Bench) -> anyhow::Result<()> {
        let num_series: usize = bench.spec().params().get_parse("num_series")?;
        let num_labels: usize = bench.spec().params().get_parse("num_labels")?;
        let num_samples: usize = bench.spec().params().get_parse("num_samples")?;

        // Live metrics - updated during the benchmark
        let sample_counter = bench.counter("sample_count");
        let series_counter = bench.counter("series_count");
        let batch_latency = bench.histogram("batch_latency_us");

        // Start the timed benchmark
        let runner = bench.start();

        let tsdb = Tsdb::new(create_test_storage().await);
        let mut series_written = 0;
        let mut iteration = 0;

        while runner.keep_running() {
            
            // Generate timeseries data
            let series: Vec<Series> = (0..num_series)
                .map(|i| {
                    let labels: Vec<Label> = (0..num_labels)
                        .map(|j| Label::new(format!("label_{}", i), format!("value_{}", j)))
                        .collect();

                    let base_timestamp = (iteration * num_series as u64 * num_samples as u64 * 1500)
                        + (i as u64 * num_samples as u64 * 1500);
                    let samples: Vec<Sample> = (0..num_samples)
                        .map(|j| Sample {
                            timestamp_ms: base_timestamp as i64 + (j as i64 * 1500),
                            value: 1.0,
                        })
                        .collect();

                    Series::new(format!("metric_{}", i), labels, samples)
                })

            .collect();


            let batch_start = std::time::Instant::now();
            //ingest
            tsdb.ingest_samples(series.clone(), 5).await?;
            let series_elapsed = batch_start.elapsed();

            // Update live metrics
            sample_counter.increment((num_samples * num_series) as u64);
            series_counter.increment(num_series as u64);
            batch_latency.record(series_elapsed.as_secs_f64() * MICROS_PER_SEC);

            series_written += num_series;
            iteration += 1;
        }

        let elapsed_secs = runner.elapsed().as_secs_f64();

        // Summary metrics - computed at the end
        let series_per_sec = series_written as f64 / elapsed_secs;
        let samples_per_sec = (series_written * num_samples) as f64 / elapsed_secs;

        bench
            .summarize(
                Summary::new()
                    .add("samples_per_sec", samples_per_sec)
                    .add("series_per_sec", series_per_sec)
                    .add("elapsed_ms", runner.elapsed().as_millis() as f64),
            )
            .await?;

        bench.close().await?;
        Ok(())
    }
}
