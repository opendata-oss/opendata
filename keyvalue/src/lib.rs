//! OpenData KeyValue - A thin key-value wrapper over SlateDB.
//!
//! OpenData KeyValue provides a simple key-value abstraction built on SlateDB
//! with OpenData's common key conventions. It applies a standard 2-byte record
//! prefix to user keys, ensuring compatibility with OpenData's key encoding patterns.
//!
//! # Architecture
//!
//! KeyValue is a thin wrapper that adds key encoding to SlateDB operations.
//! User keys are prefixed with a version byte and record tag before storage,
//! and the prefix is stripped when reading back.
//!
//! # Key Concepts
//!
//! - **KeyValueDb**: The main entry point providing both read and write operations.
//! - **KeyValueDbReader**: A read-only view of the store, useful for consumers
//!   that should not have write access.
//! - **KeyValueRead**: Trait defining read operations shared by both types.
//!
//! # Example
//!
//! ```ignore
//! use keyvalue::{KeyValueDb, KeyValueRead, Config};
//! use bytes::Bytes;
//!
//! // Open a key-value store
//! let kv = KeyValueDb::open(config).await?;
//!
//! // Write data
//! kv.put(Bytes::from("user:123"), Bytes::from("alice")).await?;
//! kv.put(Bytes::from("user:456"), Bytes::from("bob")).await?;
//!
//! // Read data
//! let value = kv.get(Bytes::from("user:123")).await?;
//! assert_eq!(value, Some(Bytes::from("alice")));
//!
//! // Scan a range
//! let mut iter = kv.scan(Bytes::from("user:")..Bytes::from("user;")).await?;
//! while let Some(entry) = iter.next().await? {
//!     println!("{:?}: {:?}", entry.key, entry.value);
//! }
//!
//! // Delete data
//! kv.delete(Bytes::from("user:123")).await?;
//! ```

mod config;
mod error;
mod keyvalue;
mod model;
mod reader;
mod serde;
mod storage;

pub use config::{Config, WriteOptions};
pub use error::{Error, Result};
pub use keyvalue::KeyValueDb;
pub use model::KeyValueEntry;
pub use reader::{KeyValueDbReader, KeyValueIterator, KeyValueRead};
