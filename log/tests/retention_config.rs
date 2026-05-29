//! End-to-end validation that retention/segmentation config rules from
//! RFC 0005 surface as `Error::InvalidInput` through `LogDb::open`.

use std::time::Duration;

use common::StorageConfig;
use log::{Config, Error, LogCompactionOptions, LogDb, RetentionConfig, SegmentConfig};

fn in_memory_with(retention: Option<Duration>, seal_interval: Option<Duration>) -> Config {
    Config {
        storage: StorageConfig::InMemory,
        segmentation: SegmentConfig { seal_interval },
        retention: RetentionConfig {
            retention,
            ..RetentionConfig::default()
        },
        ..Config::default()
    }
}

#[tokio::test]
async fn open_rejects_retention_without_seal_interval() {
    // given
    let config = in_memory_with(Some(Duration::from_secs(60)), None);

    // when
    let err = match LogDb::open(config).await {
        Ok(_) => panic!("expected Err"),
        Err(e) => e,
    };

    // then
    assert!(
        matches!(&err, Error::InvalidInput(msg) if msg.contains("seal_interval")),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn open_rejects_retention_smaller_than_seal_interval() {
    // given
    let config = in_memory_with(Some(Duration::from_secs(30)), Some(Duration::from_secs(60)));

    // when
    let err = match LogDb::open(config).await {
        Ok(_) => panic!("expected Err"),
        Err(e) => e,
    };

    // then
    assert!(
        matches!(&err, Error::InvalidInput(msg) if msg.contains("at least as large")),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn open_accepts_retention_equal_to_seal_interval() {
    let config = in_memory_with(Some(Duration::from_secs(60)), Some(Duration::from_secs(60)));
    let log = LogDb::open(config)
        .await
        .expect("retention == seal_interval should be accepted");
    log.close().await.expect("close failed");
}

#[tokio::test]
async fn open_rejects_min_l0_above_max_l0() {
    let config = Config {
        storage: StorageConfig::InMemory,
        compaction: LogCompactionOptions {
            min_l0_per_compaction: 10,
            max_l0_per_compaction: 5,
            ..LogCompactionOptions::default()
        },
        ..Config::default()
    };

    let err = match LogDb::open(config).await {
        Ok(_) => panic!("expected Err"),
        Err(e) => e,
    };

    assert!(
        matches!(&err, Error::InvalidInput(msg) if msg.contains("min_l0_per_compaction")),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn open_accepts_retention_disabled() {
    // Default config: no retention, no seal_interval. Must build cleanly.
    let log = LogDb::open(Config::default())
        .await
        .expect("default config should open");
    log.close().await.expect("close failed");
}
