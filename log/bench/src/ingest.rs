//! Ingest throughput benchmark for the log database.

use bencher::{Bench, Benchmark, Params, Summary};
use bytes::Bytes;
use log::{Config, LogDb, Record};

const MICROS_PER_SEC: f64 = 1_000_000.0;

/// Create a parameter set for the ingest benchmark.
fn make_params(batch_size: usize, value_size: usize, key_length: usize, num_keys: usize) -> Params {
    let mut params = Params::new();
    params.insert("batch_size", batch_size.to_string());
    params.insert("value_size", value_size.to_string());
    params.insert("key_length", key_length.to_string());
    params.insert("num_keys", num_keys.to_string());
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
            make_params(1, 256, 16, 10),
            make_params(10, 256, 16, 10),
            make_params(100, 256, 16, 10),
            // Vary value size
            make_params(100, 64, 16, 10),
            make_params(100, 512, 16, 10),
            make_params(100, 1024, 16, 10),
        ]
    }

    async fn run(&self, bench: Bench) -> anyhow::Result<()> {
        let batch_size: usize = bench.spec().params().get_parse("batch_size")?;
        let value_size: usize = bench.spec().params().get_parse("value_size")?;
        let key_length: usize = bench.spec().params().get_parse("key_length")?;
        let num_keys: usize = bench.spec().params().get_parse("num_keys")?;

        // Live metrics - updated during the benchmark
        let records_counter = bench.counter("records");
        let bytes_counter = bench.counter("bytes");
        let batch_latency = bench.histogram("batch_latency_us");

        // Initialize log with fresh in-memory storage
        let config = Config {
            storage: bench.spec().data().storage.clone(),
            ..Default::default()
        };
        let log = LogDb::open(config).await?;

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

        // Start the timed benchmark
        let runner = bench.start();

        // Run append loop until framework signals to stop
        let mut records_written = 0;
        let mut key_idx = 0;

        while runner.keep_running() {
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

            let batch_start = std::time::Instant::now();
            log.append(records).await?;
            let batch_elapsed = batch_start.elapsed();

            // Update live metrics
            records_counter.increment(batch_size as u64);
            bytes_counter.increment((batch_size * record_size) as u64);
            batch_latency.record(batch_elapsed.as_secs_f64() * MICROS_PER_SEC);

            records_written += batch_size;
        }
        log.flush().await?;

        let elapsed_secs = runner.elapsed().as_secs_f64();

        // Summary metrics - computed at the end
        let ops_per_sec = records_written as f64 / elapsed_secs;
        let bytes_per_sec = (records_written * record_size) as f64 / elapsed_secs;

        bench
            .summarize(
                Summary::new()
                    .add("throughput_ops", ops_per_sec)
                    .add("throughput_bytes", bytes_per_sec)
                    .add("elapsed_ms", runner.elapsed().as_millis() as f64),
            )
            .await?;

        // Close the log to release SlateDB fence
        log.close().await?;

        bench.close().await?;
        Ok(())
    }
}
