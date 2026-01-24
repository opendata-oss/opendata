//! Ingest throughput benchmark for the log database.

use std::time::Instant;

use bencher::{Bencher, Benchmark, Label, Summary};
use bytes::Bytes;
use log::{Config, Log, Record};

const MICROS_PER_SEC: f64 = 1_000_000.0;

/// Parameters for the ingest benchmark.
#[derive(Debug, Clone)]
pub struct Params {
    /// Number of records per append call.
    pub batch_size: usize,
    /// Size of each record value in bytes.
    pub value_size: usize,
    /// Length of keys in bytes.
    pub key_length: usize,
    /// Number of distinct keys to write to.
    pub num_keys: usize,
    /// Total number of records to write.
    pub total_records: usize,
}

impl Params {
    fn new(
        batch_size: usize,
        value_size: usize,
        key_length: usize,
        num_keys: usize,
        total_records: usize,
    ) -> Self {
        Self {
            batch_size,
            value_size,
            key_length,
            num_keys,
            total_records,
        }
    }
}

impl From<&Params> for Vec<Label> {
    fn from(p: &Params) -> Vec<Label> {
        vec![
            Label::new("batch_size", p.batch_size.to_string()),
            Label::new("value_size", p.value_size.to_string()),
            Label::new("key_length", p.key_length.to_string()),
            Label::new("num_keys", p.num_keys.to_string()),
            Label::new("total_records", p.total_records.to_string()),
        ]
    }
}

/// Benchmark for log ingest throughput.
pub struct IngestBenchmark {
    /// Parameter combinations to sweep.
    params: Vec<Params>,
}

impl IngestBenchmark {
    pub fn new() -> Self {
        Self {
            params: vec![
                // Vary batch size
                Params::new(1, 256, 16, 10, 1000),
                Params::new(10, 256, 16, 10, 1000),
                Params::new(100, 256, 16, 10, 1000),
                // Vary value size
                Params::new(10, 64, 16, 10, 1000),
                Params::new(10, 1024, 16, 10, 1000),
            ],
        }
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

    async fn run(&self, bencher: &Bencher) -> anyhow::Result<()> {
        for params in &self.params {
            let bench = bencher.bench(params);

            // Live metrics - updated during the benchmark
            let records_counter = bench.counter("records");
            let bytes_counter = bench.counter("bytes");
            let batch_latency = bench.histogram("batch_latency_us");

            // Initialize log with fresh in-memory storage
            let config = Config {
                storage: bencher.data_config().storage.clone(),
                ..Default::default()
            };
            let log = Log::open(config).await?;

            // Generate keys
            let keys: Vec<Bytes> = (0..params.num_keys)
                .map(|i| {
                    let key = format!("{:0>width$}", i, width = params.key_length);
                    Bytes::from(key)
                })
                .collect();

            // Generate value template
            let value = Bytes::from(vec![b'x'; params.value_size]);
            let record_size = params.key_length + params.value_size;

            // Run append loop
            let start = Instant::now();
            let mut records_written = 0;
            let mut key_idx = 0;

            while records_written < params.total_records {
                let batch_size =
                    std::cmp::min(params.batch_size, params.total_records - records_written);
                let records: Vec<Record> = (0..batch_size)
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
                records_counter.increment(batch_size as u64);
                bytes_counter.increment((batch_size * record_size) as u64);
                batch_latency.record(batch_elapsed.as_secs_f64() * MICROS_PER_SEC);

                records_written += batch_size;
            }

            let elapsed = start.elapsed();
            let elapsed_secs = elapsed.as_secs_f64();

            // Summary metrics - computed at the end
            let ops_per_sec = params.total_records as f64 / elapsed_secs;
            let bytes_per_sec = (params.total_records * record_size) as f64 / elapsed_secs;

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
