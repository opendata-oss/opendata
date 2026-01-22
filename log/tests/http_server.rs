//! Integration tests for the log HTTP server.

use std::sync::Arc;

use base64::Engine;
use bytes::Bytes;
use common::StorageConfig;
use log::{Config, Log, Record};

async fn setup_test_log() -> Arc<Log> {
    let config = Config {
        storage: StorageConfig::InMemory,
        ..Default::default()
    };
    Arc::new(Log::open(config).await.expect("Failed to open log"))
}

fn encode_base64(data: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(data)
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

    // List keys
    use log::LogRead;
    let mut iter = log.list(..).await.unwrap();

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
async fn test_base64_encoding_helper() {
    // Test the base64 encoding used in the HTTP API
    let key = b"test-key";
    let encoded = encode_base64(key);
    assert_eq!(encoded, "dGVzdC1rZXk=");

    // Verify round-trip
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(&encoded)
        .expect("Failed to decode");
    assert_eq!(decoded, key);
}
