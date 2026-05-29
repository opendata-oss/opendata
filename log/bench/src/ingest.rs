//! Ingest throughput benchmark for the log database.

use bencher::{Bench, Benchmark, Params, Summary};
use bytes::Bytes;
use log::{Config, LogDb, Record};

use crate::slatedb_metrics::{self, SlatedbSnapshot};

const MICROS_PER_SEC: f64 = 1_000_000.0;

fn load_log_config(path: &str) -> anyhow::Result<Config> {
    let contents = std::fs::read_to_string(path)
        .map_err(|e| anyhow::anyhow!("failed to read {}: {}", path, e))?;
    serde_yaml::from_str(&contents).map_err(|e| anyhow::anyhow!("failed to parse {}: {}", path, e))
}

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

        // Build the LogDb config. When `log_config` is set, the YAML file is
        // the source of truth — its `storage` wins and `data.storage` is
        // ignored. Otherwise, fall back to grafting `data.storage` onto
        // default config.
        let config = match bench.spec().params().get("log_config") {
            Some(path) => load_log_config(path)?,
            None => Config {
                storage: bench.spec().data().storage.clone(),
                ..Default::default()
            },
        };
        let log = LogDb::open(config).await?;

        // Capture slatedb counters at the start so we can report per-run deltas
        // even when the global registry is shared across bench iterations.
        let metrics_handle = slatedb_metrics::handle();
        let slatedb_before = metrics_handle
            .as_ref()
            .map(SlatedbSnapshot::capture)
            .unwrap_or_default();

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
            log.try_append(records).await?;
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

        // Write-path timing breakdown: split the per-append wall-clock between
        // LogDb-internal work and the underlying storage (SlateDB).
        let stats = log.write_stats();
        let mut summary = Summary::new()
            .add("throughput_ops", ops_per_sec)
            .add("throughput_bytes", bytes_per_sec)
            .add("elapsed_ms", runner.elapsed().as_millis() as f64);
        if stats.append_count > 0 {
            let n = stats.append_count as f64;
            let append_avg_ns = stats.append_total_ns as f64 / n;
            let logdb_only_ns = stats
                .append_total_ns
                .saturating_sub(stats.storage_apply_ns)
                .saturating_sub(stats.storage_snapshot_ns) as f64
                / n;
            let slatedb_fraction = if stats.append_total_ns > 0 {
                (stats.storage_apply_ns + stats.storage_snapshot_ns) as f64
                    / stats.append_total_ns as f64
            } else {
                0.0
            };
            let apply_avg_ns = if stats.storage_apply_count > 0 {
                stats.storage_apply_ns as f64 / stats.storage_apply_count as f64
            } else {
                0.0
            };
            let snapshot_avg_ns = if stats.storage_snapshot_count > 0 {
                stats.storage_snapshot_ns as f64 / stats.storage_snapshot_count as f64
            } else {
                0.0
            };
            summary = summary
                .add("append_avg_ns", append_avg_ns)
                .add("logdb_only_avg_ns", logdb_only_ns)
                .add("slatedb_apply_avg_ns", apply_avg_ns)
                .add("slatedb_snapshot_avg_ns", snapshot_avg_ns)
                .add("slatedb_fraction", slatedb_fraction)
                // Per-phase breakdown of the LogDb-internal cost (each phase
                // runs once per handle_append, so divide by append_count).
                .add("seq_alloc_avg_ns", stats.seq_alloc_ns as f64 / n)
                .add("segment_assign_avg_ns", stats.segment_assign_ns as f64 / n)
                .add("listing_assign_avg_ns", stats.listing_assign_ns as f64 / n)
                .add("add_entries_avg_ns", stats.add_entries_ns as f64 / n)
                .add("ops_rebuild_avg_ns", stats.ops_rebuild_ns as f64 / n)
                .add(
                    "broadcast_other_avg_ns",
                    stats.broadcast_other_ns as f64 / n,
                );
        }

        // Surface a subset of SlateDB's internal counters as deltas for the
        // run. Useful for spotting backpressure, flush activity, and
        // compactor progress that aren't visible from the LogDb write path.
        if let Some(handle) = metrics_handle.as_ref() {
            let after = SlatedbSnapshot::capture(handle);
            let d = after.delta_since(&slatedb_before);
            summary = summary
                // DB write-path counters
                .add("slatedb_backpressure_count", d.backpressure_count as f64)
                .add("slatedb_write_ops", d.write_ops as f64)
                .add("slatedb_write_batch_count", d.write_batch_count as f64)
                .add(
                    "slatedb_immutable_memtable_flushes",
                    d.immutable_memtable_flushes as f64,
                )
                .add("slatedb_wal_buffer_flushes", d.wal_buffer_flushes as f64)
                .add(
                    "slatedb_wal_buffer_flush_requests",
                    d.wal_buffer_flush_requests as f64,
                )
                .add("slatedb_l0_flush_bytes", d.l0_flush_bytes as f64)
                .add(
                    "slatedb_l0_commit_gated_count",
                    d.l0_commit_gated_count as f64,
                )
                // DB gauges
                .add("slatedb_l0_sst_count", d.l0_sst_count)
                .add(
                    "slatedb_segment_max_l0_sst_count",
                    d.segment_max_l0_sst_count,
                )
                .add("slatedb_total_mem_size_bytes", d.total_mem_size_bytes)
                .add(
                    "slatedb_wal_buffer_estimated_bytes",
                    d.wal_buffer_estimated_bytes,
                )
                // Compactor progress
                .add(
                    "slatedb_compactor_bytes_compacted",
                    d.compactor_bytes_compacted as f64,
                )
                .add(
                    "slatedb_compactor_running_compactions",
                    d.compactor_running_compactions,
                )
                .add(
                    "slatedb_compactor_throughput_bps",
                    d.compactor_throughput_bps,
                )
                .add(
                    "slatedb_compactor_total_bytes_being_compacted",
                    d.compactor_total_bytes_being_compacted,
                )
                .add(
                    "slatedb_compactor_last_completion_ts_sec",
                    d.compactor_last_completion_ts_sec,
                )
                // Object-store PUT/GET latency (component=db; covers L0
                // flushes + compactor I/O — slatedb 0.13.0 doesn't split).
                .add("slatedb_put_count", d.put_count as f64)
                .add("slatedb_put_p50_ms", d.put_p50_ms)
                .add("slatedb_put_p99_ms", d.put_p99_ms)
                .add("slatedb_get_count", d.get_count as f64)
                .add("slatedb_get_p50_ms", d.get_p50_ms)
                .add("slatedb_get_p99_ms", d.get_p99_ms);
        }

        bench.summarize(summary).await?;

        // Close the log to release SlateDB fence
        log.close().await?;

        bench.close().await?;
        Ok(())
    }
}
