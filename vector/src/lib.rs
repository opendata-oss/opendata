//! OpenData Vector Database
//!
//! A vector database built on SlateDB with SPANN-style indexing for efficient
//! approximate nearest neighbor search.

pub mod dictionary;
pub mod serde;

pub use dictionary::Dictionary;
