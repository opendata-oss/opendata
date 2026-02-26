//! Integration tests for LogDb writer and LogDbReader reader separation.
//!
//! These tests verify that a LogDbReader can discover data written by a
//! separate LogDb instance when using persistent storage.

use std::time::Duration;

use bytes::Bytes;
use common::StorageConfig;
use common::storage::config::{LocalObjectStoreConfig, ObjectStoreConfig, SlateDbStorageConfig};
use log::{Config, LogDb, LogDbReader, LogRead, ReaderConfig, Record};
use tempfile::TempDir;

fn local_storage_config(dir: &TempDir) -> StorageConfig {
    StorageConfig::SlateDb(SlateDbStorageConfig {
        path: "log-data".to_string(),
        object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
            path: dir.path().to_string_lossy().to_string(),
        }),
        settings_path: None,
    })
}

#[tokio::test]
async fn reader_discovers_data_written_by_writer() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let storage = local_storage_config(&temp_dir);

    // Create writer and append data
    let writer_config = Config {
        storage: storage.clone(),
        ..Default::default()
    };
    let writer = LogDb::open(writer_config)
        .await
        .expect("Failed to open writer");

    let key = Bytes::from("test-key");
    writer
        .try_append(vec![
            Record {
                key: key.clone(),
                value: Bytes::from("value-0"),
            },
            Record {
                key: key.clone(),
                value: Bytes::from("value-1"),
            },
        ])
        .await
        .expect("Failed to append");

    // Flush to ensure data is persisted
    writer.flush().await.expect("Failed to flush");

    // Create reader with short refresh interval
    let reader_config = ReaderConfig {
        storage,
        refresh_interval: Duration::from_millis(50),
    };
    let reader = LogDbReader::open(reader_config)
        .await
        .expect("Failed to open reader");

    // Wait for refresh to discover the data
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Scan and verify data
    let mut iter = reader.scan(key, ..).await.expect("Failed to scan");

    let entry0 = iter
        .next()
        .await
        .expect("Failed to get next")
        .expect("Expected entry");
    assert_eq!(entry0.value, Bytes::from("value-0"));
    assert_eq!(entry0.sequence, 0);

    let entry1 = iter
        .next()
        .await
        .expect("Failed to get next")
        .expect("Expected entry");
    assert_eq!(entry1.value, Bytes::from("value-1"));
    assert_eq!(entry1.sequence, 1);

    assert!(iter.next().await.expect("Failed to get next").is_none());

    // Clean up
    reader.close().await;
    writer.close().await.expect("Failed to close writer");
}

#[tokio::test]
async fn reader_discovers_new_data_after_initial_open() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let storage = local_storage_config(&temp_dir);

    // Create writer
    let writer_config = Config {
        storage: storage.clone(),
        ..Default::default()
    };
    let writer = LogDb::open(writer_config)
        .await
        .expect("Failed to open writer");

    // Create reader before any data exists
    let reader_config = ReaderConfig {
        storage,
        refresh_interval: Duration::from_millis(50),
    };
    let reader = LogDbReader::open(reader_config)
        .await
        .expect("Failed to open reader");

    // Initially no data
    let key = Bytes::from("events");
    let mut iter = reader.scan(key.clone(), ..).await.expect("Failed to scan");
    assert!(iter.next().await.expect("Failed to get next").is_none());

    // Write data after reader is open
    writer
        .try_append(vec![Record {
            key: key.clone(),
            value: Bytes::from("event-1"),
        }])
        .await
        .expect("Failed to append");
    writer.flush().await.expect("Failed to flush");

    // Wait for refresh
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Reader should now see the data
    let mut iter = reader.scan(key, ..).await.expect("Failed to scan");
    let entry = iter
        .next()
        .await
        .expect("Failed to get next")
        .expect("Expected entry");
    assert_eq!(entry.value, Bytes::from("event-1"));

    // Clean up
    reader.close().await;
    writer.close().await.expect("Failed to close writer");
}

#[tokio::test]
async fn flush_guarantees_durability_across_reopen() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let storage = local_storage_config(&temp_dir);

    let key = Bytes::from("durable-key");

    // Write data and flush, then close
    {
        let config = Config {
            storage: storage.clone(),
            ..Default::default()
        };
        let writer = LogDb::open(config).await.expect("Failed to open writer");

        writer
            .try_append(vec![
                Record {
                    key: key.clone(),
                    value: Bytes::from("value-0"),
                },
                Record {
                    key: key.clone(),
                    value: Bytes::from("value-1"),
                },
                Record {
                    key: key.clone(),
                    value: Bytes::from("value-2"),
                },
            ])
            .await
            .expect("Failed to append");

        writer.flush().await.expect("Failed to flush");
        writer.close().await.expect("Failed to close writer");
    }

    // Reopen from the same path and verify all data survived
    {
        let config = Config {
            storage: storage.clone(),
            ..Default::default()
        };
        let reader = LogDb::open(config).await.expect("Failed to reopen");

        let mut iter = reader.scan(key, ..).await.expect("Failed to scan");

        let entry0 = iter
            .next()
            .await
            .expect("Failed to get next")
            .expect("Expected entry 0");
        assert_eq!(entry0.sequence, 0);
        assert_eq!(entry0.value, Bytes::from("value-0"));

        let entry1 = iter
            .next()
            .await
            .expect("Failed to get next")
            .expect("Expected entry 1");
        assert_eq!(entry1.sequence, 1);
        assert_eq!(entry1.value, Bytes::from("value-1"));

        let entry2 = iter
            .next()
            .await
            .expect("Failed to get next")
            .expect("Expected entry 2");
        assert_eq!(entry2.sequence, 2);
        assert_eq!(entry2.value, Bytes::from("value-2"));

        assert!(iter.next().await.expect("Failed to get next").is_none());

        reader.close().await.expect("Failed to close reader");
    }
}

#[tokio::test]
async fn close_without_explicit_flush_guarantees_durability() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let storage = local_storage_config(&temp_dir);

    let key = Bytes::from("implicit-flush-key");

    // Write data and close without calling flush()
    {
        let config = Config {
            storage: storage.clone(),
            ..Default::default()
        };
        let writer = LogDb::open(config).await.expect("Failed to open writer");

        writer
            .try_append(vec![Record {
                key: key.clone(),
                value: Bytes::from("survived"),
            }])
            .await
            .expect("Failed to append");

        writer.close().await.expect("Failed to close writer");
    }

    // Reopen and verify the record survived
    {
        let config = Config {
            storage: storage.clone(),
            ..Default::default()
        };
        let reader = LogDb::open(config).await.expect("Failed to reopen");

        let mut iter = reader.scan(key, ..).await.expect("Failed to scan");

        let entry = iter
            .next()
            .await
            .expect("Failed to get next")
            .expect("Expected entry");
        assert_eq!(entry.sequence, 0);
        assert_eq!(entry.value, Bytes::from("survived"));

        assert!(iter.next().await.expect("Failed to get next").is_none());

        reader.close().await.expect("Failed to close reader");
    }
}
