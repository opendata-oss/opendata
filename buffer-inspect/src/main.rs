//! `buffer-inspect` — read-only inspector for OpenData Buffer manifests and
//! their referenced batch objects.
//!
//! Reads the manifest and any referenced batches via [`buffer::BufferReader`],
//! never invoking [`buffer::Consumer`]. Decodes per-entry metadata envelopes
//! and, when an envelope identifies an OTLP signal, summarizes the payload.
//! Unknown envelopes are reported verbatim and never crash the tool.
//!
//! Usage:
//!
//! ```text
//! buffer-inspect \
//!   --object-store-config object-store.yaml \
//!   --manifest-path ingest/otel/logs/manifest \
//!   --limit 10 \
//!   --records-per-entry 5
//! ```
//!
//! The `--object-store-config` file is a YAML representation of
//! [`common::ObjectStoreConfig`]; for example:
//!
//! ```yaml
//! type: Aws
//! region: us-west-2
//! bucket: responsive-prod-opendata-otel-logs-us-west-2
//! ```

mod inspect;

use std::path::PathBuf;
use std::process;
use std::sync::Arc;

use anyhow::{Context, Result};
use buffer::BufferReader;
use clap::Parser;
use common::create_object_store;

use crate::inspect::{InspectOptions, SignalDecodeMode, inspect_manifest};

#[derive(Parser, Debug)]
#[command(
    name = "buffer-inspect",
    about = "Read-only inspector for OpenData Buffer manifests"
)]
struct Cli {
    /// Path to a YAML file containing a `common::ObjectStoreConfig`.
    #[arg(long, value_name = "PATH")]
    object_store_config: PathBuf,

    /// Manifest object path inside the configured object store.
    #[arg(long, value_name = "PATH")]
    manifest_path: String,

    /// Optional starting sequence. When omitted, the tool inspects the
    /// most recent `--limit` entries on the manifest.
    #[arg(long, value_name = "SEQUENCE")]
    from_sequence: Option<u64>,

    /// Maximum number of manifest entries to inspect (default 10).
    #[arg(long, default_value_t = 10)]
    limit: usize,

    /// How to interpret per-entry payloads when summarizing.
    ///
    /// - `auto`: dispatch on each entry's metadata envelope (default).
    /// - `metrics`: assume OTLP metrics regardless of envelope.
    /// - `logs`: assume OTLP logs regardless of envelope.
    /// - `raw`: print envelope and payload bytes; no signal decoding.
    #[arg(long, value_enum, default_value_t = SignalDecodeMode::Auto)]
    decode_signal: SignalDecodeMode,

    /// Maximum number of payload records to summarize per manifest entry.
    #[arg(long, default_value_t = 5)]
    records_per_entry: usize,
}

fn main() {
    if let Err(err) = run() {
        eprintln!("buffer-inspect: {err:?}");
        process::exit(1);
    }
}

fn run() -> Result<()> {
    let cli = Cli::parse();

    let config_bytes = std::fs::read(&cli.object_store_config).with_context(|| {
        format!(
            "reading object-store config {}",
            cli.object_store_config.display()
        )
    })?;
    let object_store_config: common::ObjectStoreConfig = serde_yaml::from_slice(&config_bytes)
        .with_context(|| {
            format!(
                "parsing object-store config {}",
                cli.object_store_config.display()
            )
        })?;
    let object_store =
        create_object_store(&object_store_config).context("creating object store")?;

    let reader = BufferReader::new(Arc::clone(&object_store), cli.manifest_path.clone());
    let options = InspectOptions {
        from_sequence: cli.from_sequence,
        limit: cli.limit,
        decode_signal: cli.decode_signal,
        records_per_entry: cli.records_per_entry,
    };

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("building tokio runtime")?;
    let report = runtime.block_on(inspect_manifest(&reader, &options))?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
