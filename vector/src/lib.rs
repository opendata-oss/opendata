//! OpenData Vector Database
//!
//! A vector database built on SlateDB with SPANN-style indexing for efficient
//! approximate nearest neighbor search.
//!
//! # Example
//!
//! ```
//! # use vector::{VectorDb, Vector, Config, DistanceMetric};
//! # use std::time::Duration;
//! # #[tokio::main]
//! # async fn main() -> vector::Result<()> {
//! let config = Config {
//!     dimensions: 384,
//!     distance_metric: DistanceMetric::L2,
//!     flush_interval: Duration::from_secs(60),
//!     ..Default::default()
//! };
//! let db = VectorDb::open(config).await?;
//!
//! let vectors = vec![
//!     Vector::builder("product-001", vec![0.1; 384])
//!         .attribute("category", "electronics")
//!         .attribute("price", 99i64)
//!         .build(),
//! ];
//!
//! db.write(vectors).await?;
//! db.flush().await?;
//! # Ok(())
//! # }
//! ```

pub mod db;
pub mod delta;
pub mod distance;
pub(crate) mod error;
pub mod flusher;
pub mod hnsw;
pub(crate) mod lire;
pub mod model;
pub(crate) mod query_engine;
pub mod reader;
pub mod serde;
#[cfg(feature = "http-server")]
pub mod server;
pub(crate) mod storage;
pub(crate) mod view_reader;

#[cfg(test)]
pub(crate) mod test_utils;

// Public API exports
pub use db::{VectorDb, VectorDbRead};
pub use error::{Error, Result};
pub use model::{
    Attribute, AttributeValue, Config, DistanceMetric, FieldSelection, FieldType, Filter,
    MetadataFieldSpec, Query, ReaderConfig, SearchResult, Vector, VectorBuilder,
};
pub use reader::VectorDbReader;
