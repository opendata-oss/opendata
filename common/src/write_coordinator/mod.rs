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

#[cfg(test)]
mod integration_tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    use super::*;
    use crate::storage::in_memory::InMemoryStorage;

    /// A synchronization primitive for deterministic failpoint testing.
    ///
    /// Replaces sleeps with proper signaling to avoid flaky tests.
    /// When the failpoint is hit, it signals `reached` and blocks until `release` is called.
    struct FailpointSync {
        name: String,
        reached: Arc<std::sync::atomic::AtomicBool>,
        release_tx: std::sync::mpsc::Sender<()>,
    }

    impl FailpointSync {
        /// Configure a failpoint with synchronization.
        ///
        /// The failpoint will signal when hit, then block until `release()` is called.
        fn new(name: &str) -> Self {
            let reached = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let (release_tx, release_rx) = std::sync::mpsc::channel();

            // Wrap receiver in Mutex to satisfy Sync requirement for cfg_callback
            let release_rx = Arc::new(std::sync::Mutex::new(release_rx));

            let reached_clone = reached.clone();
            fail::cfg_callback(name, move || {
                reached_clone.store(true, Ordering::SeqCst);
                let rx = release_rx.lock().unwrap();
                let _ = rx.recv(); // Block until released
            })
            .unwrap();

            Self {
                name: name.to_string(),
                reached,
                release_tx,
            }
        }

        /// Wait until the failpoint is reached.
        async fn wait_reached(&self) {
            while !self.reached.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        }

        /// Release the blocked failpoint, allowing execution to continue.
        fn release(&self) {
            let _ = self.release_tx.send(());
        }

        /// Disable the failpoint to prevent future triggers.
        fn disable(&self) {
            fail::cfg(&self.name, "off").unwrap();
        }
    }

    // Test delta that accumulates key-value pairs
    // Demonstrates:
    // - State accumulation (values append to lists per key)
    // - Image preservation (ID counter carries forward across flushes)
    // - Size estimation for backpressure testing
    #[derive(Clone, Default)]
    struct TestDelta {
        // ID counter that must be preserved across flushes
        next_id: u32,
        // Accumulated values since last flush (key -> list of (id, value))
        data: HashMap<String, Vec<(u32, i32)>>,
    }

    impl TestDelta {
        fn get_value_count(&self) -> usize {
            self.data.values().map(|v| v.len()).sum()
        }

        fn get_key_count(&self) -> usize {
            self.data.len()
        }

        fn get_values(&self, key: &str) -> Vec<(u32, i32)> {
            self.data.get(key).cloned().unwrap_or_default()
        }
    }

    #[derive(Clone)]
    struct TestImage {
        next_id: u32,
    }

    impl Default for TestImage {
        fn default() -> Self {
            Self { next_id: 1 }
        }
    }

    // Write operation: (key, value)
    // Each write gets assigned a unique ID from the delta
    struct TestWrite {
        key: String,
        value: i32,
    }

    impl Delta for TestDelta {
        type Image = TestImage;
        type Write = TestWrite;

        fn init(&mut self, image: &Self::Image) {
            self.next_id = image.next_id;
            self.data.clear();
        }

        fn apply(&mut self, writes: Vec<Self::Write>) -> Result<()> {
            for write in writes {
                let id = self.next_id;
                self.next_id += 1;
                self.data
                    .entry(write.key)
                    .or_default()
                    .push((id, write.value));
            }
            Ok(())
        }

        fn estimate_size(&self) -> usize {
            // Each entry is approximately 12 bytes (u32 + i32 + overhead)
            self.data.values().map(|v| v.len()).sum::<usize>() * 12
        }

        fn fork_image(&self) -> Self::Image {
            TestImage {
                next_id: self.next_id,
            }
        }
    }

    struct TestFlusher<D: Delta> {
        storage: Arc<dyn crate::Storage>,
        flush_count: Arc<AtomicU32>,
        _phantom: std::marker::PhantomData<D>,
    }

    impl<D: Delta> TestFlusher<D> {
        fn new(storage: Arc<dyn crate::Storage>) -> Self {
            Self {
                storage,
                flush_count: Arc::new(AtomicU32::new(0)),
                _phantom: std::marker::PhantomData,
            }
        }

        fn get_flush_count(&self) -> u32 {
            self.flush_count.load(Ordering::SeqCst)
        }
    }

    impl<D: Delta> Clone for TestFlusher<D> {
        fn clone(&self) -> Self {
            Self {
                storage: self.storage.clone(),
                flush_count: self.flush_count.clone(),
                _phantom: std::marker::PhantomData,
            }
        }
    }

    #[async_trait::async_trait]
    impl<D: Delta + Sync> Flusher for TestFlusher<D> {
        type Delta = D;

        async fn flush(&self, _delta: Self::Delta) -> Result<Arc<dyn crate::StorageSnapshot>> {
            self.flush_count.fetch_add(1, Ordering::SeqCst);
            Ok(self.storage.snapshot().await?)
        }
    }

    #[tokio::test]
    async fn should_serialize_concurrent_writes() {
        // Concurrent writes from multiple tasks get unique, sequential epochs
        let storage = Arc::new(InMemoryStorage::new());
        let flusher: TestFlusher<TestDelta> = TestFlusher::new(storage.clone());
        let config = WriteCoordinatorConfig {
            queue_capacity: 1000,
            flush_interval: Duration::from_secs(3600),
            max_delta_size_bytes: 1024 * 1024,
        };

        let (coordinator, handle) =
            WriteCoordinator::new(config, storage, flusher, TestImage::default());
        tokio::spawn(coordinator.run());

        // when - spawn 10 tasks writing concurrently
        let mut handles = vec![];
        for i in 0..10 {
            let h = handle.clone();
            handles.push(tokio::spawn(async move {
                let mut epochs = vec![];
                for j in 0..10 {
                    let write = TestWrite {
                        key: format!("key_{}", i * 10 + j),
                        value: (i * 10 + j) as i32,
                    };
                    let mut wh = h.write(write).unwrap();
                    epochs.push(wh.epoch().await.unwrap());
                }
                epochs
            }));
        }

        let mut all_epochs = vec![];
        for handle in handles {
            all_epochs.extend(handle.await.unwrap());
        }

        // then - all epochs should be unique and sequential
        all_epochs.sort();
        assert_eq!(all_epochs.len(), 100);
        for (i, epoch) in all_epochs.iter().enumerate() {
            assert_eq!(*epoch, (i + 1) as u64);
        }
    }

    #[tokio::test]
    async fn should_include_write_in_subsequent_flush() {
        // DESIRED: A write() followed immediately by flush() includes that write
        // UNDESIRED: The write is missed (race condition from issues #82, #95)
        let storage = Arc::new(InMemoryStorage::new());
        let flusher: TestFlusher<TestDelta> = TestFlusher::new(storage.clone());
        let config = WriteCoordinatorConfig {
            queue_capacity: 100,
            flush_interval: Duration::from_secs(3600),
            max_delta_size_bytes: 1024 * 1024,
        };

        let (coordinator, handle) =
            WriteCoordinator::new(config, storage, flusher, TestImage::default());
        tokio::spawn(coordinator.run());

        let mut events = handle.subscribe();

        // when - write followed immediately by flush
        let mut h1 = handle
            .write(TestWrite {
                key: "key1".to_string(),
                value: 100,
            })
            .unwrap();
        handle.flush(None).await.unwrap();

        // then - the write should be included in the flush
        h1.wait(Durability::Flushed).await.unwrap();
        events.changed().await.unwrap();
        let event = events.borrow().clone().unwrap();
        assert_eq!(event.epoch_range, 1..2);
        assert_eq!(event.delta.get_value_count(), 1);
    }

    #[tokio::test]
    async fn should_apply_backpressure_when_queue_full() {
        // Writes fail with Backpressure error when queue is full
        let storage = Arc::new(InMemoryStorage::new());
        let flusher: TestFlusher<TestDelta> = TestFlusher::new(storage.clone());
        let config = WriteCoordinatorConfig {
            queue_capacity: 2,
            flush_interval: Duration::from_secs(3600),
            max_delta_size_bytes: 1024 * 1024,
        };

        let (coordinator, handle) =
            WriteCoordinator::new(config, storage, flusher, TestImage::default());
        tokio::spawn(coordinator.run());

        // when - fill the queue
        handle
            .write(TestWrite {
                key: "key1".to_string(),
                value: 1,
            })
            .unwrap();
        handle
            .write(TestWrite {
                key: "key2".to_string(),
                value: 2,
            })
            .unwrap();

        // then - additional writes should fail with backpressure
        let result = handle.write(TestWrite {
            key: "key3".to_string(),
            value: 3,
        });
        assert_eq!(result.unwrap_err(), WriteCoordinatorError::Backpressure);
    }

    #[tokio::test]
    async fn should_preserve_state_across_flushes() {
        // DESIRED: ID counter and state are preserved across flushes via image forking
        // UNDESIRED: IDs reset or state is lost after flush
        // This tests the core image forking mechanism that prevents data corruption
        let storage = Arc::new(InMemoryStorage::new());
        let flusher: TestFlusher<TestDelta> = TestFlusher::new(storage.clone());
        let config = WriteCoordinatorConfig {
            queue_capacity: 100,
            flush_interval: Duration::from_secs(3600),
            max_delta_size_bytes: 1024 * 1024,
        };

        let (coordinator, handle) =
            WriteCoordinator::new(config, storage, flusher, TestImage::default());
        tokio::spawn(coordinator.run());

        let mut events = handle.subscribe();

        // when - write to same key across multiple flushes
        handle
            .write(TestWrite {
                key: "key1".to_string(),
                value: 1,
            })
            .unwrap();
        handle.flush(None).await.unwrap();
        events.changed().await.unwrap();
        let event1 = events.borrow().clone().unwrap();

        handle
            .write(TestWrite {
                key: "key1".to_string(),
                value: 2,
            })
            .unwrap();
        handle.flush(None).await.unwrap();
        events.changed().await.unwrap();
        let event2 = events.borrow().clone().unwrap();

        // then - IDs should be sequential across flushes
        let values1 = event1.delta.get_values("key1");
        let values2 = event2.delta.get_values("key1");
        assert_eq!(values1[0].0, 1, "First write gets ID 1");
        assert_eq!(values2[0].0, 2, "Second write gets ID 2 (not reset)");
    }

    #[tokio::test]
    async fn should_maintain_epoch_ordering_within_task() {
        // Sequential writes from same task have strictly increasing epochs
        let storage = Arc::new(InMemoryStorage::new());
        let flusher: TestFlusher<TestDelta> = TestFlusher::new(storage.clone());
        let config = WriteCoordinatorConfig {
            queue_capacity: 100,
            flush_interval: Duration::from_secs(3600),
            max_delta_size_bytes: 1024 * 1024,
        };

        let (coordinator, handle) =
            WriteCoordinator::new(config, storage, flusher, TestImage::default());
        tokio::spawn(coordinator.run());

        // when - sequential writes from same task
        let mut h1 = handle
            .write(TestWrite {
                key: "key1".to_string(),
                value: 1,
            })
            .unwrap();
        let mut h2 = handle
            .write(TestWrite {
                key: "key2".to_string(),
                value: 2,
            })
            .unwrap();
        let mut h3 = handle
            .write(TestWrite {
                key: "key3".to_string(),
                value: 3,
            })
            .unwrap();

        let e1 = h1.epoch().await.unwrap();
        let e2 = h2.epoch().await.unwrap();
        let e3 = h3.epoch().await.unwrap();

        // then - epochs should be strictly increasing
        assert!(e1 < e2);
        assert!(e2 < e3);
    }

    #[tokio::test]
    async fn should_track_durability_levels_correctly() {
        // Writes progress through Applied → Flushed → Durable
        let storage = Arc::new(InMemoryStorage::new());
        let flusher: TestFlusher<TestDelta> = TestFlusher::new(storage.clone());
        let config = WriteCoordinatorConfig {
            queue_capacity: 100,
            flush_interval: Duration::from_millis(50),
            max_delta_size_bytes: 1024 * 1024,
        };

        let (coordinator, handle) =
            WriteCoordinator::new(config, storage, flusher, TestImage::default());
        tokio::spawn(coordinator.run());

        // when - write and track durability
        let mut h = handle
            .write(TestWrite {
                key: "key1".to_string(),
                value: 1,
            })
            .unwrap();

        // then - applied happens first
        h.wait(Durability::Applied).await.unwrap();

        // and - flushed happens after timer
        let flushed_result =
            tokio::time::timeout(Duration::from_millis(200), h.wait(Durability::Flushed)).await;
        assert!(flushed_result.is_ok());

        // and - durable is also satisfied
        h.wait(Durability::Durable).await.unwrap();
    }

    #[tokio::test]
    async fn should_allow_concurrent_readers_during_flush() {
        // Multiple readers can subscribe and receive flush events independently
        let storage = Arc::new(InMemoryStorage::new());
        let flusher: TestFlusher<TestDelta> = TestFlusher::new(storage.clone());
        let config = WriteCoordinatorConfig {
            queue_capacity: 100,
            flush_interval: Duration::from_secs(3600),
            max_delta_size_bytes: 1024 * 1024,
        };

        let (coordinator, handle) =
            WriteCoordinator::new(config, storage, flusher, TestImage::default());
        tokio::spawn(coordinator.run());

        // when - multiple readers subscribe to flush events
        let mut reader1 = handle.subscribe();
        let mut reader2 = handle.subscribe();
        let mut reader3 = handle.subscribe();

        handle
            .write(TestWrite {
                key: "key1".to_string(),
                value: 1,
            })
            .unwrap();
        handle
            .write(TestWrite {
                key: "key2".to_string(),
                value: 2,
            })
            .unwrap();
        handle.flush(None).await.unwrap();

        // then - all readers should receive the flush event
        reader1.changed().await.unwrap();
        reader2.changed().await.unwrap();
        reader3.changed().await.unwrap();

        assert_eq!(reader1.borrow().as_ref().unwrap().epoch_range, 1..3);
        assert_eq!(reader2.borrow().as_ref().unwrap().epoch_range, 1..3);
        assert_eq!(reader3.borrow().as_ref().unwrap().epoch_range, 1..3);
    }

    #[tokio::test]
    async fn should_handle_write_bursts_with_backpressure() {
        // DESIRED: When queue is full, additional writes fail with Backpressure
        // UNDESIRED: Writes succeed beyond queue capacity or system hangs
        // This validates backpressure mechanism works correctly
        let storage = Arc::new(InMemoryStorage::new());
        let flusher: TestFlusher<TestDelta> = TestFlusher::new(storage.clone());
        let config = WriteCoordinatorConfig {
            queue_capacity: 2,
            flush_interval: Duration::from_secs(3600), // No auto-flush
            max_delta_size_bytes: 1024 * 1024,
        };

        let (coordinator, handle) =
            WriteCoordinator::new(config, storage, flusher.clone(), TestImage::default());
        tokio::spawn(coordinator.run());

        // Fill queue to capacity (use try_send semantics of write())
        handle
            .write(TestWrite {
                key: "1".to_string(),
                value: 1,
            })
            .unwrap();
        handle
            .write(TestWrite {
                key: "2".to_string(),
                value: 2,
            })
            .unwrap();

        // Queue is now at capacity - next write should fail
        let result = handle.write(TestWrite {
            key: "3".to_string(),
            value: 3,
        });
        assert_eq!(
            result.unwrap_err(),
            WriteCoordinatorError::Backpressure,
            "Write should fail with backpressure when queue is full"
        );

        // Trigger flush to drain queue
        handle.flush(None).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Now writes should succeed again
        handle
            .write(TestWrite {
                key: "4".to_string(),
                value: 4,
            })
            .unwrap();
    }

    #[tokio::test]
    async fn should_preserve_correctness_under_concurrent_load() {
        // DESIRED: High concurrency maintains correctness (no lost writes, proper epochs)
        // UNDESIRED: Race conditions cause data loss or corruption
        // Stress test simulating real-world concurrent load
        let storage = Arc::new(InMemoryStorage::new());
        let flusher: TestFlusher<TestDelta> = TestFlusher::new(storage.clone());
        let config = WriteCoordinatorConfig {
            queue_capacity: 100,
            flush_interval: Duration::from_millis(100),
            max_delta_size_bytes: 1024,
        };

        let (coordinator, handle) =
            WriteCoordinator::new(config, storage, flusher, TestImage::default());
        tokio::spawn(coordinator.run());

        let write_count = Arc::new(AtomicU32::new(0));

        // when - spawn multiple tasks writing concurrently
        let mut tasks = vec![];
        for task_id in 0..5 {
            let h = handle.clone();
            let count = write_count.clone();

            tasks.push(tokio::spawn(async move {
                for i in 0..20 {
                    match h.write(TestWrite {
                        key: format!("task{}_key{}", task_id, i),
                        value: i,
                    }) {
                        Ok(mut wh) => {
                            wh.wait(Durability::Applied).await.unwrap();
                            count.fetch_add(1, Ordering::SeqCst);
                        }
                        Err(WriteCoordinatorError::Backpressure) => {
                            // Expected under load
                        }
                        Err(e) => panic!("Unexpected error: {:?}", e),
                    }
                }
            }));
        }

        for task in tasks {
            task.await.unwrap();
        }

        // then - all successful writes should be accounted for
        let total_writes = write_count.load(Ordering::SeqCst);
        assert!(total_writes > 0, "Should have successful writes");

        // Flush remaining writes
        handle.flush(None).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    #[tokio::test]
    async fn should_support_priority_writer_pattern() {
        // DESIRED: Priority writer can retry and get through during backpressure
        // UNDESIRED: Priority writer is permanently blocked by backpressure
        // This models userspace implementation of priority via retry logic
        let storage = Arc::new(InMemoryStorage::new());
        let flusher: TestFlusher<TestDelta> = TestFlusher::new(storage.clone());
        let config = WriteCoordinatorConfig {
            queue_capacity: 2,
            flush_interval: Duration::from_millis(50),
            max_delta_size_bytes: 1024 * 1024,
        };

        let (coordinator, handle) =
            WriteCoordinator::new(config, storage, flusher, TestImage::default());
        tokio::spawn(coordinator.run());

        // Fill queue with regular writes
        handle
            .write(TestWrite {
                key: "regular1".to_string(),
                value: 1,
            })
            .unwrap();
        handle
            .write(TestWrite {
                key: "regular2".to_string(),
                value: 2,
            })
            .unwrap();

        // Priority writer retries on backpressure
        let priority_handle = handle.clone();
        let priority_task = tokio::spawn(async move {
            loop {
                match priority_handle.write(TestWrite {
                    key: "priority".to_string(),
                    value: 999,
                }) {
                    Ok(mut wh) => {
                        return wh.epoch().await.unwrap();
                    }
                    Err(WriteCoordinatorError::Backpressure) => {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                        // Retry - priority writer keeps trying
                        continue;
                    }
                    Err(e) => panic!("Unexpected error: {:?}", e),
                }
            }
        });

        // Regular writer gives up on backpressure
        let regular_result = handle.write(TestWrite {
            key: "regular3".to_string(),
            value: 3,
        });
        assert_eq!(
            regular_result.unwrap_err(),
            WriteCoordinatorError::Backpressure
        );

        // Priority writer should eventually succeed as queue drains
        let priority_epoch = priority_task.await.unwrap();
        assert!(priority_epoch > 0, "Priority writer got through");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_maintain_per_thread_ordering_with_interleaving() {
        // DESIRED: Epochs within each thread are sequential, but can interleave between threads
        // UNDESIRED: Epochs within a single thread are out of order
        // Uses failpoint to guarantee interleaving between two threads
        let _guard = fail::FailScenario::setup();

        let storage = Arc::new(InMemoryStorage::new());
        let flusher: TestFlusher<TestDelta> = TestFlusher::new(storage.clone());
        let config = WriteCoordinatorConfig {
            queue_capacity: 100,
            flush_interval: Duration::from_secs(3600),
            max_delta_size_bytes: 1024 * 1024,
        };

        let (coordinator, handle) =
            WriteCoordinator::new(config, storage, flusher, TestImage::default());
        tokio::spawn(coordinator.run());

        // Pause after first write from any thread
        let fp = FailpointSync::new("write_coordinator::after_apply");

        let handle1 = handle.clone();
        let handle2 = handle.clone();

        // Thread 1: submit first write (will pause after apply)
        let mut wh1_1 = handle1
            .write(TestWrite {
                key: "t1_key1".to_string(),
                value: 1,
            })
            .unwrap();

        // Wait for failpoint to be hit (deterministic, no sleep)
        fp.wait_reached().await;

        // Thread 2: submit writes while thread 1 is paused
        let mut wh2_1 = handle2
            .write(TestWrite {
                key: "t2_key1".to_string(),
                value: 1,
            })
            .unwrap();
        let mut wh2_2 = handle2
            .write(TestWrite {
                key: "t2_key2".to_string(),
                value: 2,
            })
            .unwrap();

        // Thread 1: submit more writes (these queue after thread 2's)
        let mut wh1_2 = handle1
            .write(TestWrite {
                key: "t1_key2".to_string(),
                value: 2,
            })
            .unwrap();
        let mut wh1_3 = handle1
            .write(TestWrite {
                key: "t1_key3".to_string(),
                value: 3,
            })
            .unwrap();

        // Release the failpoint and disable future triggers
        fp.release();
        fp.disable();

        // Get all epochs
        let t1_e1 = wh1_1.epoch().await.unwrap();
        let t1_e2 = wh1_2.epoch().await.unwrap();
        let t1_e3 = wh1_3.epoch().await.unwrap();
        let t2_e1 = wh2_1.epoch().await.unwrap();
        let t2_e2 = wh2_2.epoch().await.unwrap();

        // Verify within-thread ordering
        assert!(t1_e1 < t1_e2, "Thread 1 epochs must be increasing");
        assert!(t1_e2 < t1_e3, "Thread 1 epochs must be increasing");
        assert!(t2_e1 < t2_e2, "Thread 2 epochs must be increasing");

        // Verify interleaving occurred: T1-1, T2-1, T2-2, T1-2, T1-3
        assert!(
            t2_e1 > t1_e1 && t2_e1 < t1_e2,
            "Thread 2 should have interleaved: t1=[{}, {}, {}], t2=[{}, {}]",
            t1_e1,
            t1_e2,
            t1_e3,
            t2_e1,
            t2_e2
        );
    }

    // Deterministic race condition tests using failpoints
    mod race_conditions {
        use super::*;

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn should_ensure_write_followed_by_flush_includes_write() {
            // DESIRED: Write followed by flush() includes that write, even with timing races
            // UNDESIRED: Flush races ahead and misses the write (issues #82, #95)
            // Uses failpoint to deterministically pause flush processing after write completes
            let _guard = fail::FailScenario::setup();

            let storage = Arc::new(InMemoryStorage::new());
            let flusher: TestFlusher<TestDelta> = TestFlusher::new(storage.clone());
            let config = WriteCoordinatorConfig {
                queue_capacity: 100,
                flush_interval: Duration::from_secs(3600),
                max_delta_size_bytes: 1024 * 1024,
            };

            let (coordinator, handle) =
                WriteCoordinator::new(config, storage, flusher, TestImage::default());
            tokio::spawn(coordinator.run());

            let mut events = handle.subscribe();

            // Simulate the problematic pattern: pause before processing flush command
            let fp = FailpointSync::new("write_coordinator::before_flush_command");

            // when - write and immediately flush (old code might miss the write)
            let mut h1 = handle
                .write(TestWrite {
                    key: "1".to_string(),
                    value: 100,
                })
                .unwrap();

            // Wait for write to be applied
            h1.wait(Durability::Applied).await.unwrap();

            // Issue flush - it will pause before processing
            let flush_handle = {
                let h = handle.clone();
                tokio::spawn(async move { h.flush(None).await })
            };

            // Wait for flush command to reach the pause point (deterministic, no sleep)
            fp.wait_reached().await;

            // Now release and let flush proceed
            fp.release();
            fp.disable();

            flush_handle.await.unwrap().unwrap();

            // then - the write MUST be included in the flush
            h1.wait(Durability::Flushed).await.unwrap();
            events.changed().await.unwrap();
            let event = events.borrow().clone().unwrap();
            assert_eq!(event.epoch_range, 1..2, "Write must be in flush");
            assert_eq!(event.delta.get_value_count(), 1);
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn should_maintain_epoch_ordering_under_interleaving() {
            // DESIRED: Epoch assignment matches submission order despite interleaved execution
            // UNDESIRED: Second write gets lower epoch than first (epoch inversion)
            // Uses failpoint to pause first write after apply, then submits second write
            let _guard = fail::FailScenario::setup();

            let storage = Arc::new(InMemoryStorage::new());
            let flusher: TestFlusher<TestDelta> = TestFlusher::new(storage.clone());
            let config = WriteCoordinatorConfig {
                queue_capacity: 100,
                flush_interval: Duration::from_secs(3600),
                max_delta_size_bytes: 1024 * 1024,
            };

            let (coordinator, handle) =
                WriteCoordinator::new(config, storage, flusher, TestImage::default());
            tokio::spawn(coordinator.run());

            // Pause after first write applies
            let fp = FailpointSync::new("write_coordinator::after_apply");

            // when - start first write (will pause after apply)
            let h1 = handle
                .write(TestWrite {
                    key: "1".to_string(),
                    value: 100,
                })
                .unwrap();

            // Wait for failpoint to be hit (deterministic, no sleep)
            fp.wait_reached().await;

            // Start second write while first is paused
            let h2 = handle
                .write(TestWrite {
                    key: "2".to_string(),
                    value: 100,
                })
                .unwrap();

            // Release the failpoint
            fp.release();
            fp.disable();

            // then - epochs must be assigned in submission order
            let mut h1_mut = h1;
            let mut h2_mut = h2;
            let e1 = h1_mut.epoch().await.unwrap();
            let e2 = h2_mut.epoch().await.unwrap();

            assert_eq!(e1, 1, "First write gets epoch 1");
            assert_eq!(e2, 2, "Second write gets epoch 2");
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn should_handle_concurrent_writes_during_flush() {
            // DESIRED: Writes continue during flush (non-blocking), new writes go to new delta
            // UNDESIRED: Flush blocks all writes, causing system to hang
            // Uses failpoint to pause flush after forking, then submits new write
            let _guard = fail::FailScenario::setup();

            let storage = Arc::new(InMemoryStorage::new());
            let flusher: TestFlusher<TestDelta> = TestFlusher::new(storage.clone());
            let config = WriteCoordinatorConfig {
                queue_capacity: 100,
                flush_interval: Duration::from_secs(3600),
                max_delta_size_bytes: 1024 * 1024,
            };

            let (coordinator, handle) =
                WriteCoordinator::new(config, storage, flusher, TestImage::default());
            tokio::spawn(coordinator.run());

            let mut events = handle.subscribe();

            // Write some data
            handle
                .write(TestWrite {
                    key: "1".to_string(),
                    value: 100,
                })
                .unwrap();

            // Pause during flush (after forking image but before flush completes)
            let fp = FailpointSync::new("write_coordinator::after_fork_image");

            // Start flush - it will pause after forking
            let flush_task = {
                let h = handle.clone();
                tokio::spawn(async move { h.flush(None).await })
            };

            // Wait for flush to reach pause point (deterministic, no sleep)
            fp.wait_reached().await;

            // when - write new data while flush is paused
            let mut h2 = handle
                .write(TestWrite {
                    key: "2".to_string(),
                    value: 100,
                })
                .unwrap();

            // New write should be applied immediately (non-blocking flush)
            h2.wait(Durability::Applied).await.unwrap();
            let e2 = h2.epoch().await.unwrap();
            assert_eq!(e2, 2, "New write gets epoch 2");

            // Release the failpoint
            fp.release();
            fp.disable();
            flush_task.await.unwrap().unwrap();

            // then - first flush includes epoch 1
            events.changed().await.unwrap();
            let event1 = events.borrow().clone().unwrap();
            assert_eq!(event1.epoch_range, 1..2);

            // and - second write will be in next flush
            handle.flush(None).await.unwrap();
            events.changed().await.unwrap();
            let event2 = events.borrow().clone().unwrap();
            assert_eq!(event2.epoch_range, 2..3);
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn should_preserve_image_state_during_concurrent_flush() {
            // DESIRED: Image forking preserves state (ID counter) across concurrent operations
            // UNDESIRED: ID counter resets or corrupts, causing ID collisions
            // Uses failpoint to pause after fork, then writes to same key in new delta
            let _guard = fail::FailScenario::setup();

            let storage = Arc::new(InMemoryStorage::new());
            let flusher: TestFlusher<TestDelta> = TestFlusher::new(storage.clone());
            let config = WriteCoordinatorConfig {
                queue_capacity: 100,
                flush_interval: Duration::from_secs(3600),
                max_delta_size_bytes: 1024 * 1024,
            };

            let (coordinator, handle) =
                WriteCoordinator::new(config, storage, flusher, TestImage::default());
            tokio::spawn(coordinator.run());

            let mut events = handle.subscribe();

            // Create first series (ID will be allocated)
            handle
                .write(TestWrite {
                    key: "100".to_string(),
                    value: 100,
                })
                .unwrap();

            // Pause after forking image
            let fp = FailpointSync::new("write_coordinator::after_fork_image");

            let flush_task = {
                let h = handle.clone();
                tokio::spawn(async move { h.flush(None).await })
            };

            // Wait for flush to reach pause point (deterministic, no sleep)
            fp.wait_reached().await;

            // when - write to same series while flush is paused
            // This tests that the new delta has the forked series dictionary
            handle
                .write(TestWrite {
                    key: "100".to_string(),
                    value: 100,
                })
                .unwrap();

            fp.release();
            fp.disable();
            flush_task.await.unwrap().unwrap();

            // Flush the second write
            handle.flush(None).await.unwrap();

            // then - both flushes should have the same series ID for fingerprint 100
            events.changed().await.unwrap();
            let _event1 = events.borrow().clone().unwrap();

            events.changed().await.unwrap();
            let event2 = events.borrow().clone().unwrap();

            // Series count should still be 1 (reusing the same series ID)
            assert_eq!(
                event2.delta.get_key_count(),
                1,
                "Should reuse series ID from forked image"
            );
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn should_handle_flush_during_write_application() {
            // DESIRED: Flush command waits for in-progress write to complete and includes it
            // UNDESIRED: Flush proceeds immediately, missing the in-progress write
            // Uses failpoint to pause before write applies, then issues flush command
            let _guard = fail::FailScenario::setup();

            let storage = Arc::new(InMemoryStorage::new());
            let flusher: TestFlusher<TestDelta> = TestFlusher::new(storage.clone());
            let config = WriteCoordinatorConfig {
                queue_capacity: 100,
                flush_interval: Duration::from_secs(3600),
                max_delta_size_bytes: 1024 * 1024,
            };

            let (coordinator, handle) =
                WriteCoordinator::new(config, storage, flusher, TestImage::default());
            tokio::spawn(coordinator.run());

            let mut events = handle.subscribe();

            // Pause before applying write
            let fp = FailpointSync::new("write_coordinator::before_apply");

            // Start write - will pause before apply
            let write_task = {
                let h = handle.clone();
                tokio::spawn(async move {
                    h.write(TestWrite {
                        key: "1".to_string(),
                        value: 100,
                    })
                })
            };

            // Wait for write to reach pause point (deterministic, no sleep)
            fp.wait_reached().await;

            // when - issue flush while write is paused (it will queue behind the write)
            let flush_task = {
                let h = handle.clone();
                tokio::spawn(async move { h.flush(None).await })
            };

            // Release the failpoint - write completes, then flush processes
            fp.release();
            fp.disable();

            let mut wh = write_task.await.unwrap().unwrap();
            flush_task.await.unwrap().unwrap();

            // then - write should be in the flush
            wh.wait(Durability::Flushed).await.unwrap();
            events.changed().await.unwrap();
            let event = events.borrow().clone().unwrap();
            assert_eq!(event.epoch_range, 1..2);
        }
    }
}
