//! Smoke test confirming the SlateDB-backed `LogDbBuilder` path doesn't
//! crash with retention + compaction options configured. Does not observe
//! drains — policy correctness is covered by the unit tests in `compaction.rs`.

use std::time::Duration;

use bytes::Bytes;
use common::StorageConfig;
use common::storage::config::{LocalObjectStoreConfig, ObjectStoreConfig, SlateDbStorageConfig};
use log::{Config, LogCompactionOptions, LogDb, LogRead, Record, RetentionConfig, SegmentConfig};
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
async fn slatedb_open_with_retention_and_compaction_options() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let config = Config {
        storage: local_storage_config(&temp_dir),
        segmentation: SegmentConfig {
            seal_interval: Some(Duration::from_secs(3600)),
        },
        retention: RetentionConfig {
            retention: Some(Duration::from_secs(86400)),
            check_interval: Duration::from_secs(60),
        },
        compaction: LogCompactionOptions::default(),
        ..Config::default()
    };

    let log = LogDb::open(config).await.expect("LogDb::open failed");

    log.try_append(vec![Record {
        key: Bytes::from("k"),
        value: Bytes::from("v"),
    }])
    .await
    .expect("append failed");
    log.flush().await.expect("flush failed");

    let segments = log.list_segments(..).await.expect("list_segments failed");
    assert_eq!(segments.len(), 1);

    log.close().await.expect("close failed");
}

#[tokio::test]
async fn slatedb_open_without_retention_still_wires_compactor() {
    // Compaction strategy runs unconditionally (RFC 0005), so the scheduler
    // must still wire cleanly when retention is disabled.
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let config = Config {
        storage: local_storage_config(&temp_dir),
        ..Config::default()
    };

    let log = LogDb::open(config).await.expect("LogDb::open failed");
    log.try_append(vec![Record {
        key: Bytes::from("k"),
        value: Bytes::from("v"),
    }])
    .await
    .expect("append failed");
    log.flush().await.expect("flush failed");
    log.close().await.expect("close failed");
}
