//! OpenData Vector Database
//!
//! A vector database built on SlateDB with SPANN-style indexing for efficient
//! approximate nearest neighbor search.
//!
//! # Example
//!
//! ```ignore
//! use vector::{VectorDb, Vector, Config, DistanceMetric};
//! use std::time::Duration;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let config = Config {
//!         dimensions: 384,
//!         distance_metric: DistanceMetric::Cosine,
//!         flush_interval: Duration::from_secs(60),
//!         ..Default::default()
//!     };
//!     let db = VectorDb::open(config).await?;
//!
//!     let vectors = vec![
//!         Vector::builder("product-001", vec![0.1; 384])
//!             .attribute("category", "electronics")
//!             .attribute("price", 99i64)
//!             .build(),
//!     ];
//!
//!     db.write(vectors).await?;
//!     db.flush().await?;
//!     Ok(())
//! }
//! ```

pub mod db;
pub mod delta;
pub mod distance;
pub mod hnsw;
pub mod model;
pub mod serde;
pub(crate) mod storage;
#[cfg(test)]
pub(crate) mod test_utils;

// Public API exports
pub use db::{SearchResult, VectorDb};
pub use model::{
    Attribute, AttributeValue, Config, DistanceMetric, FieldType, MetadataFieldSpec, Vector,
    VectorBuilder,
};
