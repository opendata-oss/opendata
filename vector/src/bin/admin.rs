use figment::Figment;
use figment::providers::{Format, Serialized, Yaml};
use std::env;
use std::process;
use tracing_subscriber::EnvFilter;
use vector::{Config, VectorDbAdmin};

fn load_vector_config(path: &str) -> Config {
    Figment::from(Serialized::defaults(Config::default()))
        .merge(Yaml::file(path))
        .extract()
        .unwrap_or_else(|e| panic!("Failed to load config file '{}': {}", path, e))
}

fn usage(program: &str) {
    eprintln!("Usage: {program} <config.yaml> <index|validate|print-tree>");
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        usage(&args[0]);
        process::exit(2);
    }

    let config = load_vector_config(&args[1]);
    let admin = VectorDbAdmin::open(config).await.unwrap_or_else(|e| {
        eprintln!("Failed to open vector database admin: {e}");
        process::exit(1);
    });

    let result = match args[2].as_str() {
        "index" => admin.index_once().await,
        "validate" => admin.validate_index().await,
        "print-tree" => admin.print_tree().await.map(|tree| {
            print!("{tree}");
        }),
        _ => {
            usage(&args[0]);
            process::exit(2);
        }
    };

    if let Err(e) = result {
        eprintln!("Command failed: {e}");
        process::exit(1);
    }
}
