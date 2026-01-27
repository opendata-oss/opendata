//! Ingest throughput benchmark for the log database.

use std::time::Instant;

use bencher::{Bencher, Benchmark, Params, Summary};
use bytes::Bytes;
use log::{Config, Log, Record};

const MICROS_PER_SEC: f64 = 1_000_000.0;

/// Create a parameter set for the ingest benchmark.
fn make_params(
    batch_size: usize,
    value_size: usize,
    key_length: usize,
    num_keys: usize,
    total_records: usize,
) -> Params {
    let mut params = Params::new();
    params.insert("batch_size", batch_size.to_string());
    params.insert("value_size", value_size.to_string());
    params.insert("key_length", key_length.to_string());
    params.insert("num_keys", num_keys.to_string());
    params.insert("total_records", total_records.to_string());
    params
}

/// Benchmark for log ingest throughput.
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

#[async_trait::async_trait]
impl Benchmark for IngestBenchmark {
    fn name(&self) -> &str {
        "ingest"
    }

    fn default_params(&self) -> Vec<Params> {
        vec![
            // Vary batch size
            make_params(1, 256, 16, 10, 50_000),
            make_params(10, 256, 16, 10, 50_000),
            make_params(100, 256, 16, 10, 50_000),
            // Vary value size
            make_params(100, 64, 16, 10, 50_000),
            make_params(100, 512, 16, 10, 50_000),
            make_params(100, 1024, 16, 10, 50_000),
        ]
    }

    async fn run(&self, bencher: &Bencher, params: Vec<Params>) -> anyhow::Result<()> {
        for p in params {
            let batch_size: usize = p.get_parse("batch_size")?;
            let value_size: usize = p.get_parse("value_size")?;
            let key_length: usize = p.get_parse("key_length")?;
            let num_keys: usize = p.get_parse("num_keys")?;
            let total_records: usize = p.get_parse("total_records")?;

            let bench = bencher.bench(p);

            // Live metrics - updated during the benchmark
            let records_counter = bench.counter("records");
            let bytes_counter = bench.counter("bytes");
            let batch_latency = bench.histogram("batch_latency_us");

            // Initialize log with fresh in-memory storage
            let config = Config {
                storage: bencher.data().storage.clone(),
                ..Default::default()
            };
            let log = Log::open(config).await?;

            // Generate keys
            let keys: Vec<Bytes> = (0..num_keys)
                .map(|i| {
                    let key = format!("{:0>width$}", i, width = key_length);
                    Bytes::from(key)
                })
                .collect();

            // Generate value template
            let value = Bytes::from(vec![b'x'; value_size]);
            let record_size = key_length + value_size;

            // Run append loop
            let start = Instant::now();
            let mut records_written = 0;
            let mut key_idx = 0;

            while records_written < total_records {
                let batch_len = std::cmp::min(batch_size, total_records - records_written);
                let records: Vec<Record> = (0..batch_len)
                    .map(|_| {
                        let key = keys[key_idx % keys.len()].clone();
                        key_idx += 1;
                        Record {
                            key,
                            value: value.clone(),
                        }
                    })
                    .collect();

                let batch_start = Instant::now();
                log.append(records).await?;
                let batch_elapsed = batch_start.elapsed();

                // Update live metrics
                records_counter.increment(batch_len as u64);
                bytes_counter.increment((batch_len * record_size) as u64);
                batch_latency.record(batch_elapsed.as_secs_f64() * MICROS_PER_SEC);

                records_written += batch_len;
            }

            let elapsed = start.elapsed();
            let elapsed_secs = elapsed.as_secs_f64();

            // Summary metrics - computed at the end
            let ops_per_sec = total_records as f64 / elapsed_secs;
            let bytes_per_sec = (total_records * record_size) as f64 / elapsed_secs;

            bench
                .summarize(
                    Summary::new()
                        .add("throughput_ops", ops_per_sec)
                        .add("throughput_bytes", bytes_per_sec)
                        .add("elapsed_ms", elapsed.as_millis() as f64),
                )
                .await?;

            bench.close().await?;
        }
        Ok(())
    }
}
