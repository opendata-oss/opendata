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
    /// Manifest file operations
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
