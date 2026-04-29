//! Binary entry point.
//!
//! Loads YAML config (with `INGESTOR__` env overrides), constructs the
//! object store, builds a `buffer::Consumer`, wires the OTLP logs
//! decoder + ClickHouse logs adapter + writer + ack controller into a
//! `BufferConsumerRuntime`, and runs until `SIGINT`/`SIGTERM`.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use clickhouse_ingestor::envelope::{ConfiguredEnvelope, PayloadEncoding, SignalType};
use clickhouse_ingestor::{
    AckFlushPolicy, BufferConsumerRuntime, ClickHouseWriter, IngestorConfig,
    OtlpLogsClickHouseAdapter, OtlpLogsDecoder, RuntimeOptions,
};
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(
    name = "clickhouse-ingestor",
    about = "OpenData Buffer to ClickHouse ingestor"
)]
struct Cli {
    /// Path to the ingestor YAML config file.
    #[arg(long, value_name = "PATH")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    let cfg = IngestorConfig::load(&cli.config)
        .with_context(|| format!("loading ingestor config from {}", cli.config.display()))?;

    info!(
        manifest = %cfg.buffer.manifest_path,
        endpoint = %cfg.clickhouse.endpoint,
        database = %cfg.clickhouse.database,
        table = %cfg.clickhouse.table,
        dry_run = cfg.runtime.dry_run,
        "configuration loaded",
    );

    let object_store = common::create_object_store(&cfg.buffer.object_store)
        .context("constructing object store")?;
    let consumer_config = buffer::ConsumerConfig {
        object_store: cfg.buffer.object_store.clone(),
        manifest_path: cfg.buffer.manifest_path.clone(),
        data_path_prefix: cfg.buffer.data_prefix.clone(),
        gc_interval: std::time::Duration::from_secs(5 * 60),
        gc_grace_period: std::time::Duration::from_secs(10 * 60),
    };
    let consumer =
        buffer::Consumer::with_object_store(consumer_config, Arc::clone(&object_store), None)
            .await
            .context("initializing buffer consumer")?;

    let writer = if cfg.runtime.dry_run {
        None
    } else {
        Some(ClickHouseWriter::new(cfg.writer_config()))
    };

    let adapter = OtlpLogsClickHouseAdapter::new(cfg.logs_adapter_config());
    let decoder = OtlpLogsDecoder::new();

    let runtime_options = RuntimeOptions {
        manifest_path: cfg.buffer.manifest_path.clone(),
        data_path_prefix: cfg.buffer.data_prefix.clone(),
        configured_envelope: ConfiguredEnvelope {
            version: 1,
            signal_type: SignalType::Logs,
            encoding: PayloadEncoding::OtlpProtobuf,
        },
        commit_group: cfg.commit_group_thresholds(),
        ack_flush_policy: cfg.ack_flush_policy(),
        dry_run: cfg.runtime.dry_run,
        poll_interval: std::time::Duration::from_millis(cfg.runtime.poll_interval_ms),
    };

    // Compile-check that the policies aren't accidentally constructed
    // wrong; not strictly necessary at runtime.
    let _: AckFlushPolicy = runtime_options.ack_flush_policy;

    let runtime = BufferConsumerRuntime::new(consumer, decoder, adapter, writer, runtime_options);

    let shutdown = CancellationToken::new();
    let signal_shutdown = shutdown.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        info!("received shutdown signal");
        signal_shutdown.cancel();
    });

    runtime.run(shutdown).await?;
    Ok(())
}
