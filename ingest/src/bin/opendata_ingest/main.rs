//! CLI for inspecting and debugging opendata-ingest state.
//!
//! Built behind the `cli` feature flag so library consumers do not pull in
//! clap, serde_json, or base64.
//!
//! # Installation
//!
//! ```sh
//! cargo install opendata-ingest --features cli
//! ```
//!
//! # Subcommands
//!
//! All subcommands are grouped under a top-level resource noun.
//!
//! ## `manifest dump <file>`
//!
//! Reads a binary manifest file from disk and writes JSON to stdout.
//! Pair with `jq` for ad-hoc filtering:
//!
//! ```sh
//! opendata-ingest manifest dump /tmp/manifest | jq '.entries | length'
//! ```

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use clap::{Parser, Subcommand};

mod manifest;

#[derive(Parser)]
#[command(name = "opendata-ingest", about = "CLI tools for opendata-ingest")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Manifest file operations.
    Manifest {
        #[command(subcommand)]
        command: manifest::ManifestCommand,
    },
}

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Command::Manifest { command } => manifest::run(command),
    }
}
