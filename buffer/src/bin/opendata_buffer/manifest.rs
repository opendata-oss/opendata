use std::path::PathBuf;
use std::process;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;
use clap::Subcommand;
use serde_json::json;

#[derive(Subcommand)]
pub enum ManifestCommand {
    /// Dump a manifest file as pretty-printed JSON
    Dump {
        /// Path to the manifest file
        file: PathBuf,
    },
}

pub fn run(command: ManifestCommand) {
    match command {
        ManifestCommand::Dump { file } => dump(&file),
    }
}

fn dump(path: &PathBuf) {
    let data = match std::fs::read(path) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("error reading {}: {e}", path.display());
            process::exit(1);
        }
    };

    let view = match buffer::parse_manifest(Bytes::from(data)) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("error parsing manifest: {e}");
            process::exit(1);
        }
    };

    let entries: Vec<serde_json::Value> = view
        .entries()
        .iter()
        .map(|entry| {
            let metadata: Vec<serde_json::Value> = entry
                .metadata
                .iter()
                .map(|m| {
                    json!({
                        "start_index": m.start_index,
                        "ingestion_time_ms": m.ingestion_time_ms,
                        "payload_base64": BASE64.encode(&m.payload),
                    })
                })
                .collect();
            json!({
                "sequence": entry.sequence,
                "location": entry.location,
                "metadata": metadata,
            })
        })
        .collect();

    let output = json!({
        "version": 1,
        "epoch": view.epoch,
        "next_sequence": view.next_sequence,
        "entries": entries,
    });

    match serde_json::to_string_pretty(&output) {
        Ok(s) => println!("{s}"),
        Err(e) => {
            eprintln!("error serializing JSON: {e}");
            process::exit(1);
        }
    }
}
