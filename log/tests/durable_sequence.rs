//! Integration tests for the `LogDb::durable_sequence` API.
//!
//! These tests exercise the public Rust surface only (no `pub(crate)` access)
//! to verify that the durable-sequence watermark advances as the underlying
//! storage confirms durability.

use std::time::Duration;

use bytes::Bytes;
use common::StorageConfig;
use common::storage::config::{LocalObjectStoreConfig, ObjectStoreConfig, SlateDbStorageConfig};
use log::{Config, LogDb, LogRead, ReadVisibility, Record};
use tempfile::TempDir;

fn slatedb_config(dir: &TempDir, read_visibility: ReadVisibility) -> Config {
    Config {
        storage: StorageConfig::SlateDb(SlateDbStorageConfig {
            path: "log-data".to_string(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: dir.path().to_string_lossy().to_string(),
            }),
            settings_path: None,
            block_cache: None,
            meta_cache: None,
        }),
        read_visibility,
        ..Default::default()
    }
}

#[tokio::test]
async fn durable_sequence_advances_with_in_memory_storage() {
    let log = LogDb::open(Config {
        storage: StorageConfig::InMemory,
        ..Default::default()
    })
    .await
    .expect("open log");

    assert_eq!(log.durable_sequence(), 0);

    let append = log
        .try_append(vec![
            Record {
                key: Bytes::from("k"),
                value: Bytes::from("v0"),
            },
            Record {
                key: Bytes::from("k"),
                value: Bytes::from("v1"),
            },
            Record {
                key: Bytes::from("k"),
                value: Bytes::from("v2"),
            },
        ])
        .await
        .expect("try_append");
    let end_seq = append.start_sequence + 3;

    let mut rx = log.subscribe_durable();
    tokio::time::timeout(Duration::from_secs(5), async {
        while *rx.borrow_and_update() < end_seq {
            rx.changed().await.expect("subscribe_durable channel");
        }
    })
    .await
    .expect("durable_sequence should advance past the append");

    assert!(log.durable_sequence() >= end_seq);
    log.close().await.expect("close");
}

#[tokio::test]
async fn durable_sequence_advances_with_slatedb_storage_after_flush() {
    let dir = TempDir::new().expect("temp dir");
    let log = LogDb::open(slatedb_config(&dir, ReadVisibility::Memory))
        .await
        .expect("open log");

    let append = log
        .try_append(vec![Record {
            key: Bytes::from("k"),
            value: Bytes::from("v0"),
        }])
        .await
        .expect("try_append");
    let end_seq = append.start_sequence + 1;

    // Force a flush so the underlying durability watermark moves.
    log.flush().await.expect("flush");

    let mut rx = log.subscribe_durable();
    tokio::time::timeout(Duration::from_secs(10), async {
        while *rx.borrow_and_update() < end_seq {
            rx.changed().await.expect("subscribe_durable channel");
        }
    })
    .await
    .expect("durable_sequence should advance past the flushed append");

    assert!(log.durable_sequence() >= end_seq);
    log.close().await.expect("close");
}

/// In Remote mode, once `durable_sequence` advances past `N`, a subsequent
/// `scan` is required to return every record with `sequence < N` — the
/// invariant the unified subscriber's drain is designed to enforce.
#[tokio::test]
async fn durable_sequence_advance_implies_scan_visibility_in_remote_mode_with_slatedb() {
    let dir = TempDir::new().expect("temp dir");
    let log = LogDb::open(slatedb_config(&dir, ReadVisibility::Remote))
        .await
        .expect("open log");

    let key = Bytes::from("orders");
    let records: Vec<Record> = (0..5u8)
        .map(|i| Record {
            key: key.clone(),
            value: Bytes::from(vec![i]),
        })
        .collect();
    let append = log.try_append(records).await.expect("try_append");
    let end_seq = append.start_sequence + 5;

    // Before durability, scan must not see the writes.
    let mut iter = log.scan(key.clone(), ..).await.expect("scan");
    assert!(
        iter.next().await.expect("next").is_none(),
        "Remote mode scan must not see non-durable writes"
    );

    log.flush().await.expect("flush");

    // Wait for durable_sequence to cover the batch.
    let mut rx = log.subscribe_durable();
    tokio::time::timeout(Duration::from_secs(10), async {
        while *rx.borrow_and_update() < end_seq {
            rx.changed().await.expect("subscribe_durable channel");
        }
    })
    .await
    .expect("durable_sequence should cover the batch after flush");

    // Now scan must see everything — no extra yields or sleeps.
    let mut iter = log.scan(key, ..).await.expect("scan");
    let mut seen = Vec::new();
    while let Some(entry) = iter.next().await.expect("next") {
        seen.push(entry.value);
    }
    assert_eq!(seen.len(), 5, "scan should see all 5 records");
    for (i, v) in seen.iter().enumerate() {
        assert_eq!(v.as_ref(), &[i as u8]);
    }

    log.close().await.expect("close");
}

/// `durable_sequence` is strictly monotonic across multiple flushed batches.
#[tokio::test]
async fn durable_sequence_is_monotonic_across_batches_with_slatedb() {
    let dir = TempDir::new().expect("temp dir");
    let log = LogDb::open(slatedb_config(&dir, ReadVisibility::Memory))
        .await
        .expect("open log");

    let key = Bytes::from("events");
    let mut prev: u64 = 0;
    for batch in 0..5 {
        let records: Vec<Record> = (0..3)
            .map(|i| Record {
                key: key.clone(),
                value: Bytes::from(format!("b{batch}-{i}")),
            })
            .collect();
        log.try_append(records).await.expect("try_append");
        log.flush().await.expect("flush");

        let mut rx = log.subscribe_durable();
        tokio::time::timeout(Duration::from_secs(10), async {
            while *rx.borrow_and_update() <= prev {
                rx.changed().await.expect("subscribe_durable channel");
            }
        })
        .await
        .expect("durable_sequence should advance after each flushed batch");

        let now = log.durable_sequence();
        assert!(
            now > prev,
            "durable_sequence regressed: now={now} prev={prev}"
        );
        prev = now;
    }

    log.close().await.expect("close");
}
