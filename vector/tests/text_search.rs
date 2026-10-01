//! End-to-end full-text search test for RFC-0006 Milestone 0.
//!
//! Verifies that documents with `FieldType::Text` attributes can be written,
//! tokenized, indexed, and queried via BM25 with correct ranking semantics.

use std::time::Duration;

use common::StorageConfig;
use vector::{
    AttributeValue, Config, DistanceMetric, FieldType, MetadataFieldSpec, Query, Vector, VectorDb,
    VectorDbRead,
};

const DIMS: u16 = 3;

fn make_db_config() -> Config {
    Config {
        storage: StorageConfig::InMemory,
        dimensions: DIMS,
        distance_metric: DistanceMetric::L2,
        flush_interval: Duration::from_secs(60),
        split_threshold_vectors: 10_000,
        merge_threshold_vectors: 200,
        split_search_neighbourhood: 0,
        metadata_fields: vec![MetadataFieldSpec::new("body", FieldType::Text, false)],
        ..Default::default()
    }
}

fn doc(id: &str, body: &str) -> Vector {
    Vector::builder(id, vec![0.0; DIMS as usize])
        .attribute("body", AttributeValue::String(body.to_string()))
        .build()
}

#[tokio::test]
async fn should_rank_documents_by_bm25_for_single_term_query() {
    // given
    let db = VectorDb::open(make_db_config()).await.unwrap();
    db.write(vec![
        doc("d1", "the quick brown fox jumps over the lazy dog"),
        doc("d2", "vector database stores fox embeddings"),
        doc("d3", "another fox sighting in town with a fox call"),
        doc("d4", "completely unrelated content about cats and birds"),
        doc("d5", "a vector database powers semantic search"),
    ])
    .await
    .unwrap();
    db.flush().await.unwrap();

    // when - search for "fox"
    let results = db
        .search(&Query::bm25("body", "fox").with_limit(10))
        .await
        .unwrap();

    // then - only documents containing "fox" returned
    let ids: Vec<&str> = results.iter().map(|r| r.vector.id.as_str()).collect();
    assert!(
        !ids.contains(&"d4"),
        "expected d4 (no fox) to be absent, got {:?}",
        ids
    );
    assert!(
        !ids.contains(&"d5"),
        "expected d5 (no fox) to be absent, got {:?}",
        ids
    );
    assert!(
        ids.contains(&"d1"),
        "expected d1 to be present, got {:?}",
        ids
    );
    assert!(
        ids.contains(&"d2"),
        "expected d2 to be present, got {:?}",
        ids
    );
    assert!(
        ids.contains(&"d3"),
        "expected d3 to be present, got {:?}",
        ids
    );

    // and - d3 (which has "fox" twice) ranks first
    assert_eq!(
        results.first().map(|r| r.vector.id.as_str()),
        Some("d3"),
        "expected d3 with repeated 'fox' to rank first, got {:?}",
        ids
    );

    // and - scores are non-increasing
    for window in results.windows(2) {
        assert!(
            window[0].score >= window[1].score,
            "expected non-increasing BM25 scores, got {} then {}",
            window[0].score,
            window[1].score,
        );
    }
}

#[tokio::test]
async fn should_combine_scores_across_query_terms() {
    // given
    let db = VectorDb::open(make_db_config()).await.unwrap();
    db.write(vec![
        doc("d1", "the quick brown fox jumps over the lazy dog"),
        doc("d2", "vector database stores fox embeddings"),
        doc("d3", "another fox sighting in town with a fox call"),
        doc("d4", "completely unrelated content about cats and birds"),
        doc("d5", "a vector database powers semantic search"),
    ])
    .await
    .unwrap();
    db.flush().await.unwrap();

    // when - search for "vector database"
    let results = db
        .search(&Query::bm25("body", "vector database").with_limit(10))
        .await
        .unwrap();
    let ids: Vec<&str> = results.iter().map(|r| r.vector.id.as_str()).collect();

    // then - documents with either term appear
    assert!(ids.contains(&"d2"), "expected d2, got {:?}", ids);
    assert!(ids.contains(&"d5"), "expected d5, got {:?}", ids);
    assert!(
        !ids.contains(&"d4"),
        "expected d4 to be absent, got {:?}",
        ids
    );
}

#[tokio::test]
async fn should_return_text_field_by_default_in_bm25_results() {
    // given
    let db = VectorDb::open(make_db_config()).await.unwrap();
    db.write(vec![doc("d1", "quick brown fox")]).await.unwrap();
    db.flush().await.unwrap();

    // when - default field selection includes text field
    let results = db
        .search(&Query::bm25("body", "fox").with_limit(10))
        .await
        .unwrap();

    // then
    assert_eq!(results.len(), 1);
    let body = results[0]
        .vector
        .attributes
        .iter()
        .find(|a| a.name == "body")
        .expect("body attribute should be present by default");
    match &body.value {
        AttributeValue::String(s) => assert!(s.contains("fox"), "got body={:?}", s),
        other => panic!("expected body string, got {:?}", other),
    }
}

#[tokio::test]
async fn should_exclude_deleted_documents_from_bm25_results() {
    // given - several docs where a subset contain "quick"
    let db = VectorDb::open(make_db_config()).await.unwrap();
    db.write(vec![
        doc("d1", "quick brown fox jumps over the lazy dog"),
        doc("d2", "a quick rabbit hops across the green meadow"),
        doc("d3", "the quick otter slides down the muddy bank"),
        doc("d4", "slow turtle naps under a warm rock all day"),
        doc("d5", "calm river flows gently past the old mill"),
    ])
    .await
    .unwrap();
    db.flush().await.unwrap();

    // sanity - "quick" matches the three quick docs and excludes the rest
    let results = db
        .search(&Query::bm25("body", "quick").with_limit(10))
        .await
        .unwrap();
    let ids: Vec<&str> = results.iter().map(|r| r.vector.id.as_str()).collect();
    assert!(ids.contains(&"d1"), "expected d1 present, got {:?}", ids);
    assert!(ids.contains(&"d2"), "expected d2 present, got {:?}", ids);
    assert!(ids.contains(&"d3"), "expected d3 present, got {:?}", ids);
    assert!(!ids.contains(&"d4"), "expected d4 absent, got {:?}", ids);
    assert!(!ids.contains(&"d5"), "expected d5 absent, got {:?}", ids);

    // when - delete one of the matching docs and flush
    db.delete(vec!["d2"]).await.unwrap();
    db.flush().await.unwrap();

    let results = db
        .search(&Query::bm25("body", "quick").with_limit(10))
        .await
        .unwrap();
    let ids: Vec<&str> = results.iter().map(|r| r.vector.id.as_str()).collect();

    // then - the deleted doc is gone, the other matching docs remain
    assert!(
        !ids.contains(&"d2"),
        "expected deleted d2 to be absent from results, got {:?}",
        ids
    );
    assert!(
        ids.contains(&"d1"),
        "expected d1 to still be present, got {:?}",
        ids
    );
    assert!(
        ids.contains(&"d3"),
        "expected d3 to still be present, got {:?}",
        ids
    );

    // and - scores remain non-increasing (sensible ranking)
    for window in results.windows(2) {
        assert!(
            window[0].score >= window[1].score,
            "expected non-increasing BM25 scores, got {} then {}",
            window[0].score,
            window[1].score,
        );
    }
}

#[tokio::test]
async fn should_reflect_upsert_in_bm25_results() {
    // given - a doc whose body contains "quick"
    let db = VectorDb::open(make_db_config()).await.unwrap();
    db.write(vec![doc("d1", "quick brown fox jumps high")])
        .await
        .unwrap();
    db.flush().await.unwrap();

    let results = db
        .search(&Query::bm25("body", "quick").with_limit(10))
        .await
        .unwrap();
    let ids: Vec<&str> = results.iter().map(|r| r.vector.id.as_str()).collect();
    assert!(
        ids.contains(&"d1"),
        "expected d1 present before upsert, got {:?}",
        ids
    );

    // when - upsert d1 with a body that does NOT contain "quick"
    db.write(vec![doc("d1", "sleepy badger digs a deep burrow")])
        .await
        .unwrap();
    db.flush().await.unwrap();

    // then - d1 is absent from "quick" results (old internal id hidden by the
    // deletions bitmap, new internal id has no "quick" posting)
    let results = db
        .search(&Query::bm25("body", "quick").with_limit(10))
        .await
        .unwrap();
    let ids: Vec<&str> = results.iter().map(|r| r.vector.id.as_str()).collect();
    assert!(
        !ids.contains(&"d1"),
        "expected upserted d1 to be absent from 'quick' results, got {:?}",
        ids
    );

    // and - a query for a term in the new body returns d1
    let results = db
        .search(&Query::bm25("body", "badger").with_limit(10))
        .await
        .unwrap();
    let ids: Vec<&str> = results.iter().map(|r| r.vector.id.as_str()).collect();
    assert!(
        ids.contains(&"d1"),
        "expected d1 to match a term from its new body, got {:?}",
        ids
    );
}

#[tokio::test]
async fn should_respect_field_projection_for_bm25_results() {
    // given
    let db = VectorDb::open(make_db_config()).await.unwrap();
    db.write(vec![doc("d1", "quick brown fox")]).await.unwrap();
    db.flush().await.unwrap();

    // when - exclude attributes via projection
    let results = db
        .search(&Query::bm25("body", "fox").with_limit(10).with_fields(false))
        .await
        .unwrap();

    // then - id is present, attributes are empty
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].vector.id, "d1");
    assert!(
        results[0].vector.attributes.is_empty(),
        "expected no attributes with FieldSelection::None, got {:?}",
        results[0].vector.attributes
    );
}
