//! Embedded reader binary for the vector quickstart.
//!
//! Demonstrates using `VectorDbReader` directly from Rust code to perform
//! similarity searches. Embeds query text via the embedding HTTP server
//! and searches the vector database without going through the HTTP API.

use std::env;
use std::process;

use tracing_subscriber::EnvFilter;
use vector::{Filter, Query, SearchResult, VectorDbRead, VectorDbReader};

/// Call the embedding server to get an embedding for a text query.
async fn embed_text(client: &reqwest::Client, embedding_url: &str, text: &str) -> Vec<f32> {
    let url = format!("{}/embed", embedding_url);
    let payload = serde_json::json!({"texts": [text]});

    let resp = client
        .post(&url)
        .json(&payload)
        .send()
        .await
        .unwrap_or_else(|e| {
            eprintln!("Failed to call embedding server: {e}");
            process::exit(1);
        });

    let body: serde_json::Value = resp.json().await.unwrap_or_else(|e| {
        eprintln!("Failed to parse embedding response: {e}");
        process::exit(1);
    });

    body["embeddings"][0]
        .as_array()
        .expect("Expected embeddings array")
        .iter()
        .map(|v| v.as_f64().expect("Expected f64") as f32)
        .collect()
}

fn print_results(title: &str, results: &[SearchResult]) {
    println!("\n{}", "=".repeat(70));
    println!("  {title}");
    println!("{}", "=".repeat(70));

    for (i, result) in results.iter().enumerate() {
        let book = result
            .vector
            .attribute("book")
            .map(|v| format!("{v:?}"))
            .unwrap_or_else(|| "?".to_string());
        let section = result
            .vector
            .attribute("section")
            .map(|v| format!("{v:?}"))
            .unwrap_or_else(|| "?".to_string());
        let text_preview = result
            .vector
            .attribute("text")
            .map(|v| {
                let s = format!("{v:?}");
                if s.len() > 100 {
                    let end = (0..=100).rev().find(|&i| s.is_char_boundary(i)).unwrap();
                    format!("{}...", &s[..end])
                } else {
                    s
                }
            })
            .unwrap_or_else(|| "?".to_string());

        println!(
            "  {}. [{}] {}  (score: {:.4})",
            i + 1,
            book,
            section,
            result.score
        );
        println!("     {text_preview}");
    }
    println!();
}

async fn run_query(
    reader: &VectorDbReader,
    client: &reqwest::Client,
    embedding_url: &str,
    query_text: &str,
    filter: Option<Filter>,
    title: &str,
) {
    let embedding = embed_text(client, embedding_url, query_text).await;
    let mut query = Query::new(embedding)
        .with_limit(5)
        .with_fields(vec!["book", "chapter", "section", "text"]);
    if let Some(f) = filter {
        query = query.with_filter(f);
    }
    match reader.search(&query).await {
        Ok(results) => print_results(title, &results),
        Err(e) => eprintln!("Search error: {e}"),
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!(
            "Usage: {} <config.yaml> <embedding-server-url> [query]",
            args[0]
        );
        process::exit(1);
    }

    let config_path = &args[1];
    let embedding_url = &args[2];
    let custom_query = args.get(3).map(|s| s.as_str());

    // Load config and open reader
    let config_str = std::fs::read_to_string(config_path).unwrap_or_else(|e| {
        eprintln!("Failed to read config file {config_path}: {e}");
        process::exit(1);
    });
    let config: vector::ReaderConfig = serde_yaml::from_str(&config_str).unwrap_or_else(|e| {
        eprintln!("Failed to parse config: {e}");
        process::exit(1);
    });

    println!("Opening vector database reader...");
    let reader = VectorDbReader::open(config).await.unwrap_or_else(|e| {
        eprintln!("Failed to open vector database: {e}");
        process::exit(1);
    });

    let client = reqwest::Client::new();

    // Wait for embedding server to be ready
    println!("Waiting for embedding server at {embedding_url}...");
    for i in 0..60 {
        match client.get(format!("{embedding_url}/health")).send().await {
            Ok(resp) if resp.status().is_success() => {
                println!("Embedding server is ready!");
                break;
            }
            _ => {
                if i % 10 == 0 {
                    println!("  Still waiting...");
                }
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
        }
        if i == 59 {
            eprintln!("Embedding server did not become ready");
            process::exit(1);
        }
    }

    println!("\n{}", "=".repeat(70));
    println!("  Vector Quickstart - Embedded Reader");
    println!("{}", "=".repeat(70));

    if let Some(query_text) = custom_query {
        // Run a single custom query
        println!("\nCustom query: \"{query_text}\"");

        run_query(
            &reader,
            &client,
            embedding_url,
            query_text,
            None,
            &format!("All results for: \"{query_text}\""),
        )
        .await;
    } else {
        // Run hardcoded demo queries

        // 1. Unfiltered
        run_query(
            &reader,
            &client,
            embedding_url,
            "how do I work with slices?",
            None,
            "Search: \"How do I work with slices?\"",
        )
        .await;

        // 2. Filtered: Rust book
        run_query(
            &reader,
            &client,
            embedding_url,
            "how do I work with slices?",
            Some(Filter::eq("book", "The Rust Programming Language")),
            "Search: \"How do I work with slices\" (Rust book only)",
        )
        .await;

        // 3. Filtered: Sandwich book
        run_query(
            &reader,
            &client,
            embedding_url,
            "how do I work with slices?",
            Some(Filter::eq("book", "The Sandwich Book")),
            "Search: \"How do I work with slices\" (Sandwich book only)",
        )
        .await;
    }

    println!("\nEmbedded reader complete!");
}
