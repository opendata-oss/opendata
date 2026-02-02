//! Write coordination for managing in-memory state and flushing to storage.
//!
//! This module provides the core write coordination infrastructure for managing
//! writes to an in-memory delta and periodically flushing to persistent storage.
//!
//! # Architecture
//!
//! The write coordinator follows a producer-consumer pattern:
//!
//! - **Producers** (via [`WriteCoordinatorHandle`]) submit writes that get assigned
//!   monotonically increasing epochs
//! - **Coordinator** (via [`WriteCoordinator`]) applies writes to an in-memory delta
//!   and flushes to storage based on time or size thresholds
//! - **Consumers** can subscribe to flush events to be notified when data is persisted
//!
//! # Durability Levels
//!
//! Writes progress through three durability levels (see [`Durability`]):
//!
//! 1. **Applied** - Write is in the in-memory delta
//! 2. **Flushed** - Write has been sent to the storage layer
//! 3. **Durable** - Write has been durably persisted
//!
//! # Example
//!
//! ```ignore
//! use common::write_coordinator::{
//!     WriteCoordinator, WriteCoordinatorConfig, Delta, Flusher, Durability
//! };
//!
//! // Create coordinator with custom config
//! let config = WriteCoordinatorConfig {
//!     queue_capacity: 1000,
//!     flush_interval: Duration::from_secs(1),
//!     max_delta_size_bytes: 64 * 1024 * 1024,
//! };
//!
//! let (coordinator, handle) = WriteCoordinator::new(
//!     config,
//!     storage,
//!     flusher,
//!     initial_image,
//! );
//!
//! // Spawn coordinator in background
//! tokio::spawn(coordinator.run());
//!
//! // Submit writes
//! let write_handle = handle.write(my_write)?;
//!
//! // Wait for durability
//! write_handle.wait(Durability::Flushed).await?;
//! ```

mod coordinator;
mod coordinator_handle;
mod error;
mod flush;
mod handle;
mod traits;
mod types;

// Re-export public API
pub use coordinator::{WriteCoordinator, WriteCoordinatorConfig};
pub use coordinator_handle::WriteCoordinatorHandle;
pub use error::{Result, WriteCoordinatorError};
pub use flush::FlushEvent;
pub use handle::WriteHandle;
pub use traits::{Delta, Flusher};
pub use types::{Durability, Epoch};
