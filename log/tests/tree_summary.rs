//! Verifies `LogDbReader::tree_summary` against a real SlateDB-backed log.
//!
//! Exercises the full manifest path: the writer installs the log segment
//! extractor, so flushing routes each LogDb segment's records into its own
//! SlateDB segment. The summary must then surface those segments (system +
//! the first user segment) with the persisted extractor name and non-zero
//! estimated sizes.

use std::time::Duration;

use bytes::Bytes;
use common::StorageConfig;
use common::storage::config::{LocalObjectStoreConfig, ObjectStoreConfig, SlateDbStorageConfig};
use log::{Config, LogDb, LogDbReader, ReaderConfig, Record};
use tempfile::TempDir;

fn local_storage_config(dir: &TempDir) -> StorageConfig {
    StorageConfig::SlateDb(SlateDbStorageConfig {
        path: "log-data".to_string(),
        object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
            path: dir.path().to_string_lossy().to_string(),
        }),
        settings_path: None,
        block_cache: None,
        meta_cache: None,
    })
}

#[tokio::test]
async fn tree_summary_reports_segmented_distribution() {
    let temp_dir = TempDir::new().expect("temp dir");
    let storage = local_storage_config(&temp_dir);

    // Write a few records and flush so the manifest gains persisted SSTs,
    // then close to guarantee the manifest is durable before the reader opens.
    let log = LogDb::open(Config {
        storage: storage.clone(),
        ..Config::default()
    })
    .await
    .expect("open log");
    log.try_append(vec![
        Record {
            key: Bytes::from("orders"),
            value: Bytes::from("a"),
        },
        Record {
            key: Bytes::from("orders"),
            value: Bytes::from("b"),
        },
        Record {
            key: Bytes::from("shipments"),
            value: Bytes::from("c"),
        },
    ])
    .await
    .expect("append");
    log.flush().await.expect("flush");
    log.close().await.expect("close");

    let reader = LogDbReader::open(ReaderConfig {
        storage,
        refresh_interval: Duration::from_millis(50),
    })
    .await
    .expect("open reader");

    let summary = reader
        .tree_summary()
        .await
        .expect("tree_summary")
        .expect("slatedb backend should have a manifest");

    // The writer persists the log's segment extractor name in the manifest.
    assert_eq!(summary.extractor.as_deref(), Some("opendata-log/v1"));

    // Both the system segment (seq block + segment metadata) and the first
    // user segment (the appended entries + key listing) should be present.
    let system = summary
        .segments
        .iter()
        .find(|s| s.segment_id == Some(0))
        .expect("system segment present");
    // The first user segment id is 1 (segment 0 is reserved for system records).
    let user = summary
        .segments
        .iter()
        .find(|s| s.segment_id == Some(1))
        .expect("first user segment present");

    assert!(!system.is_empty(), "system segment should hold SSTs");
    assert!(!user.is_empty(), "user segment should hold SSTs");
    assert!(
        user.estimated_bytes() > 0,
        "user segment size should be non-zero"
    );

    // The aggregate covers every segment's bytes.
    assert!(summary.estimated_bytes() >= user.estimated_bytes() + system.estimated_bytes());

    reader.close().await;
}
