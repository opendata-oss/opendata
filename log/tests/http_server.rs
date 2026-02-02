#![cfg(feature = "http-server")]
//! Integration tests for the log HTTP server.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use common::StorageConfig;
use log::{Config, LogDb, LogRead, Record, SegmentConfig};

async fn setup_test_log() -> Arc<LogDb> {
    let config = Config {
        storage: StorageConfig::InMemory,
        ..Default::default()
    };
    Arc::new(LogDb::open(config).await.expect("Failed to open log"))
}

#[tokio::test]
async fn test_append_and_scan_roundtrip() {
    // Setup
    let log = setup_test_log().await;

    // Append some records directly to the log
    let records = vec![
        Record {
            key: Bytes::from("test-key"),
            value: Bytes::from("value-0"),
        },
        Record {
            key: Bytes::from("test-key"),
            value: Bytes::from("value-1"),
        },
        Record {
            key: Bytes::from("test-key"),
            value: Bytes::from("value-2"),
        },
    ];
    log.append(records).await.unwrap();

    // Verify we can scan the entries back
    use log::LogRead;
    let mut iter = log.scan(Bytes::from("test-key"), ..).await.unwrap();

    let entry0 = iter.next().await.unwrap().unwrap();
    assert_eq!(entry0.value, Bytes::from("value-0"));

    let entry1 = iter.next().await.unwrap().unwrap();
    assert_eq!(entry1.value, Bytes::from("value-1"));

    let entry2 = iter.next().await.unwrap().unwrap();
    assert_eq!(entry2.value, Bytes::from("value-2"));

    assert!(iter.next().await.unwrap().is_none());
}

#[tokio::test]
async fn test_list_keys() {
    // Setup
    let log = setup_test_log().await;

    // Append records with different keys
    let records = vec![
        Record {
            key: Bytes::from("key-a"),
            value: Bytes::from("value-a"),
        },
        Record {
            key: Bytes::from("key-b"),
            value: Bytes::from("value-b"),
        },
        Record {
            key: Bytes::from("key-c"),
            value: Bytes::from("value-c"),
        },
    ];
    log.append(records).await.unwrap();

    // List keys (using segment range)
    use log::LogRead;
    let mut iter = log.list_keys(..).await.unwrap();

    let mut keys = Vec::new();
    while let Some(key_entry) = iter.next().await.unwrap() {
        keys.push(key_entry.key);
    }

    assert_eq!(keys.len(), 3);
    assert_eq!(keys[0], Bytes::from("key-a"));
    assert_eq!(keys[1], Bytes::from("key-b"));
    assert_eq!(keys[2], Bytes::from("key-c"));
}

#[tokio::test]
async fn test_scan_with_sequence_range() {
    // Setup
    let log = setup_test_log().await;

    // Append 5 records with sequences 0, 1, 2, 3, 4
    let records = vec![
        Record {
            key: Bytes::from("events"),
            value: Bytes::from("event-0"),
        },
        Record {
            key: Bytes::from("events"),
            value: Bytes::from("event-1"),
        },
        Record {
            key: Bytes::from("events"),
            value: Bytes::from("event-2"),
        },
        Record {
            key: Bytes::from("events"),
            value: Bytes::from("event-3"),
        },
        Record {
            key: Bytes::from("events"),
            value: Bytes::from("event-4"),
        },
    ];
    log.append(records).await.unwrap();

    // Scan with range 1..4 (sequences 1, 2, 3 - exclusive upper bound)
    // This should return 3 entries out of the 5 appended
    use log::LogRead;
    let mut iter = log.scan(Bytes::from("events"), 1..4).await.unwrap();

    let mut entries = Vec::new();
    while let Some(entry) = iter.next().await.unwrap() {
        entries.push(entry);
    }

    // Expect 3 entries: sequences 1, 2, 3 (range is exclusive of 4)
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].sequence, 1);
    assert_eq!(entries[1].sequence, 2);
    assert_eq!(entries[2].sequence, 3);
}

#[tokio::test]
async fn test_list_segments_returns_all_segments() {
    let log = setup_test_log().await;

    // Append records to create a segment
    log.append(vec![Record {
        key: Bytes::from("key-a"),
        value: Bytes::from("value-a"),
    }])
    .await
    .unwrap();

    // List all segments
    let segments = log.list_segments(..).await.unwrap();

    assert_eq!(segments.len(), 1);
    assert_eq!(segments[0].id, 0);
    assert_eq!(segments[0].start_seq, 0);
}

#[tokio::test]
async fn test_list_segments_empty_when_no_data() {
    let log = setup_test_log().await;

    // No data appended - no segments should exist
    let segments = log.list_segments(..).await.unwrap();

    assert!(segments.is_empty());
}

#[tokio::test]
async fn test_list_keys_empty_when_no_keys_in_segment_range() {
    let log = setup_test_log().await;

    // Append records to segment 0
    log.append(vec![Record {
        key: Bytes::from("key-a"),
        value: Bytes::from("value-a"),
    }])
    .await
    .unwrap();

    // List keys in segment range 10..20 (no segments exist with these IDs)
    let mut iter = log.list_keys(10..20).await.unwrap();
    let mut keys = Vec::new();
    while let Some(key_entry) = iter.next().await.unwrap() {
        keys.push(key_entry.key);
    }

    assert!(keys.is_empty());
}

#[tokio::test]
async fn test_list_keys_in_specific_segment_range() {
    let log = setup_test_log().await;

    // Append records to segment 0
    log.append(vec![
        Record {
            key: Bytes::from("key-a"),
            value: Bytes::from("value-a"),
        },
        Record {
            key: Bytes::from("key-b"),
            value: Bytes::from("value-b"),
        },
    ])
    .await
    .unwrap();

    // List keys only in segment 0
    let mut iter = log.list_keys(0..1).await.unwrap();
    let mut keys = Vec::new();
    while let Some(key_entry) = iter.next().await.unwrap() {
        keys.push(key_entry.key);
    }

    assert_eq!(keys.len(), 2);
    assert_eq!(keys[0], Bytes::from("key-a"));
    assert_eq!(keys[1], Bytes::from("key-b"));
}

#[tokio::test]
async fn test_list_segments_then_list_keys_workflow() {
    // This test demonstrates the two-step workflow from the RFC:
    // 1. Use list_segments to find segments overlapping a sequence range
    // 2. Use list_keys with those segment IDs to get keys

    let log = setup_test_log().await;

    // Append records
    log.append(vec![
        Record {
            key: Bytes::from("orders"),
            value: Bytes::from("order-1"),
        },
        Record {
            key: Bytes::from("events"),
            value: Bytes::from("event-1"),
        },
        Record {
            key: Bytes::from("users"),
            value: Bytes::from("user-1"),
        },
    ])
    .await
    .unwrap();

    // Step 1: Find segments overlapping the sequence range 0..10
    let segments = log.list_segments(0..10).await.unwrap();
    assert!(!segments.is_empty());

    // Step 2: List keys from those segments
    let start_segment = segments.first().map(|s| s.id).unwrap_or(0);
    let end_segment = segments.last().map(|s| s.id + 1).unwrap_or(0);

    let mut iter = log.list_keys(start_segment..end_segment).await.unwrap();
    let mut keys = Vec::new();
    while let Some(key_entry) = iter.next().await.unwrap() {
        keys.push(String::from_utf8_lossy(&key_entry.key).into_owned());
    }

    // Should have all 3 keys in sorted order
    assert_eq!(keys.len(), 3);
    assert_eq!(keys[0], "events");
    assert_eq!(keys[1], "orders");
    assert_eq!(keys[2], "users");
}

async fn setup_test_log_with_segment_interval(interval: Duration) -> Arc<LogDb> {
    let config = Config {
        storage: StorageConfig::InMemory,
        segmentation: SegmentConfig {
            seal_interval: Some(interval),
        },
    };
    Arc::new(LogDb::open(config).await.expect("Failed to open log"))
}

#[tokio::test]
async fn test_list_segments_multiple_segments() {
    // Use a very short seal interval to force multiple segments
    let log = setup_test_log_with_segment_interval(Duration::from_millis(1)).await;

    // Append first batch
    log.append(vec![Record {
        key: Bytes::from("key-a"),
        value: Bytes::from("value-a"),
    }])
    .await
    .unwrap();

    // Wait to exceed seal interval
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Append second batch (should create new segment)
    log.append(vec![Record {
        key: Bytes::from("key-b"),
        value: Bytes::from("value-b"),
    }])
    .await
    .unwrap();

    // List all segments
    let segments = log.list_segments(..).await.unwrap();

    // Should have at least 2 segments
    assert!(segments.len() >= 2);
    // Segment IDs should be monotonically increasing
    for i in 1..segments.len() {
        assert!(segments[i].id > segments[i - 1].id);
        assert!(segments[i].start_seq > segments[i - 1].start_seq);
    }
}

#[tokio::test]
async fn test_list_keys_across_multiple_segments() {
    // Use a very short seal interval to force multiple segments
    let log = setup_test_log_with_segment_interval(Duration::from_millis(1)).await;

    // Append key-a to segment 0
    log.append(vec![Record {
        key: Bytes::from("key-a"),
        value: Bytes::from("value-a"),
    }])
    .await
    .unwrap();

    // Wait to exceed seal interval
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Append key-b to a new segment
    log.append(vec![Record {
        key: Bytes::from("key-b"),
        value: Bytes::from("value-b"),
    }])
    .await
    .unwrap();

    // List keys across all segments
    let mut iter = log.list_keys(..).await.unwrap();
    let mut keys = Vec::new();
    while let Some(key_entry) = iter.next().await.unwrap() {
        keys.push(key_entry.key);
    }

    // Should have both keys, deduplicated and sorted
    assert_eq!(keys.len(), 2);
    assert_eq!(keys[0], Bytes::from("key-a"));
    assert_eq!(keys[1], Bytes::from("key-b"));
}

#[tokio::test]
async fn test_list_keys_only_first_segment() {
    // Use a very short seal interval to force multiple segments
    let log = setup_test_log_with_segment_interval(Duration::from_millis(1)).await;

    // Append key-a to segment 0
    log.append(vec![Record {
        key: Bytes::from("key-a"),
        value: Bytes::from("value-a"),
    }])
    .await
    .unwrap();

    // Wait to exceed seal interval
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Append key-b to segment 1
    log.append(vec![Record {
        key: Bytes::from("key-b"),
        value: Bytes::from("value-b"),
    }])
    .await
    .unwrap();

    // List keys only from segment 0
    let mut iter = log.list_keys(0..1).await.unwrap();
    let mut keys = Vec::new();
    while let Some(key_entry) = iter.next().await.unwrap() {
        keys.push(key_entry.key);
    }

    // Should only have key-a from segment 0
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0], Bytes::from("key-a"));
}

#[tokio::test]
async fn test_list_segments_filters_by_sequence_range() {
    // Use a very short seal interval to force multiple segments
    let log = setup_test_log_with_segment_interval(Duration::from_millis(1)).await;

    // Append to create segment 0 (sequences start at 0)
    log.append(vec![Record {
        key: Bytes::from("key-a"),
        value: Bytes::from("value-a"),
    }])
    .await
    .unwrap();

    // Wait to exceed seal interval
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Append to create segment 1 (sequences start at 1)
    log.append(vec![Record {
        key: Bytes::from("key-b"),
        value: Bytes::from("value-b"),
    }])
    .await
    .unwrap();

    // Get all segments to know the boundaries
    let all_segments = log.list_segments(..).await.unwrap();
    assert!(all_segments.len() >= 2);

    // List segments overlapping only the first sequence
    let segments = log.list_segments(0..1).await.unwrap();

    // Should only include segment 0
    assert_eq!(segments.len(), 1);
    assert_eq!(segments[0].id, 0);
}
