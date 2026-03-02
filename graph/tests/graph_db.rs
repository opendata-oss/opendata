//! Integration tests for the OpenData Graph database.
//!
//! Written from the perspective of an end user familiar with OpenData's
//! timeseries crate. These tests exercise the full stack: InMemory storage
//! → SlateGraphStore → GrafeoDB engine → GQL queries.

use std::sync::Arc;

use common::StorageConfig;
use graph::db::GraphDb;
use graph::Config;

/// Create an in-memory GraphDb for testing — mirrors the timeseries
/// `create_test_tsdb()` pattern.
async fn setup_graph_db() -> Arc<GraphDb> {
    let config = Config {
        storage: StorageConfig::InMemory,
        ..Default::default()
    };

    Arc::new(
        GraphDb::open_with_config(&config)
            .await
            .expect("Failed to open graph database"),
    )
}

// ─── Basic lifecycle ──────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn test_open_empty_graph() {
    let db = setup_graph_db().await;
    // Just verify that opening succeeds and the store is accessible
    let _store = db.store();
}

// ─── GQL query tests ─────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[cfg(feature = "gql")]
async fn test_create_single_node() {
    let db = setup_graph_db().await;
    // CREATE should succeed without error
    db.execute("CREATE (:Person {name: 'Alice', age: 30})")
        .expect("CREATE should succeed");
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(feature = "gql")]
async fn test_create_and_match_node() {
    let db = setup_graph_db().await;

    db.execute("CREATE (:Person {name: 'Alice', age: 30})")
        .expect("CREATE should succeed");

    let result = db
        .execute("MATCH (n:Person) RETURN n.name, n.age")
        .expect("MATCH should succeed");

    assert_eq!(result.columns.len(), 2);
    assert!(!result.rows.is_empty(), "Should find the created Person");
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(feature = "gql")]
async fn test_create_multiple_nodes() {
    let db = setup_graph_db().await;

    db.execute("CREATE (:Person {name: 'Alice', age: 30})")
        .unwrap();
    db.execute("CREATE (:Person {name: 'Bob', age: 25})")
        .unwrap();
    db.execute("CREATE (:Person {name: 'Charlie', age: 35})")
        .unwrap();

    let result = db
        .execute("MATCH (n:Person) RETURN n.name")
        .expect("MATCH should succeed");

    assert_eq!(result.rows.len(), 3, "Should find all three Person nodes");
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(feature = "gql")]
async fn test_create_edge_between_nodes() {
    let db = setup_graph_db().await;

    db.execute("CREATE (:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (:Person {name: 'Bob'})").unwrap();
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .expect("CREATE edge should succeed");

    let result = db
        .execute("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name")
        .expect("MATCH edge should succeed");

    assert_eq!(result.rows.len(), 1, "Should find the KNOWS relationship");
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(feature = "gql")]
async fn test_create_edge_with_properties() {
    let db = setup_graph_db().await;

    db.execute("CREATE (:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (:Person {name: 'Bob'})").unwrap();
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) \
         CREATE (a)-[:KNOWS {since: 2020}]->(b)",
    )
    .unwrap();

    let result = db
        .execute("MATCH ()-[r:KNOWS]->() RETURN r.since")
        .expect("MATCH edge property should succeed");

    assert_eq!(result.rows.len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(feature = "gql")]
async fn test_match_with_property_filter() {
    let db = setup_graph_db().await;

    db.execute("CREATE (:Person {name: 'Alice', age: 30})")
        .unwrap();
    db.execute("CREATE (:Person {name: 'Bob', age: 25})")
        .unwrap();

    let result = db
        .execute("MATCH (n:Person {name: 'Alice'}) RETURN n.name, n.age")
        .expect("Filtered MATCH should succeed");

    assert_eq!(result.rows.len(), 1, "Filter should match only Alice");
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(feature = "gql")]
async fn test_multiple_labels() {
    let db = setup_graph_db().await;

    db.execute("CREATE (:Person:Employee {name: 'Alice'})")
        .unwrap();
    db.execute("CREATE (:Person {name: 'Bob'})").unwrap();

    let result = db
        .execute("MATCH (n:Employee) RETURN n.name")
        .expect("MATCH by second label should succeed");

    assert_eq!(result.rows.len(), 1, "Only Alice has the Employee label");
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(feature = "gql")]
async fn test_return_count() {
    let db = setup_graph_db().await;

    db.execute("CREATE (:City {name: 'New York'})").unwrap();
    db.execute("CREATE (:City {name: 'London'})").unwrap();
    db.execute("CREATE (:City {name: 'Tokyo'})").unwrap();

    let result = db
        .execute("MATCH (n:City) RETURN count(n)")
        .expect("COUNT should succeed");

    assert_eq!(result.rows.len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(feature = "gql")]
async fn test_graph_traversal_two_hops() {
    let db = setup_graph_db().await;

    db.execute("CREATE (:Person {name: 'Alice'})").unwrap();
    db.execute("CREATE (:Person {name: 'Bob'})").unwrap();
    db.execute("CREATE (:Person {name: 'Charlie'})").unwrap();

    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .unwrap();
    db.execute(
        "MATCH (b:Person {name: 'Bob'}), (c:Person {name: 'Charlie'}) CREATE (b)-[:KNOWS]->(c)",
    )
    .unwrap();

    // Two-hop traversal: Alice -> Bob -> Charlie
    let result = db
        .execute(
            "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b)-[:KNOWS]->(c) RETURN c.name",
        )
        .expect("Two-hop traversal should succeed");

    assert_eq!(result.rows.len(), 1, "Should find Charlie via two hops");
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(feature = "gql")]
async fn test_empty_match_returns_no_rows() {
    let db = setup_graph_db().await;

    let result = db
        .execute("MATCH (n:NonExistentLabel) RETURN n")
        .expect("MATCH on empty graph should succeed");

    assert!(result.rows.is_empty(), "No nodes should match");
}

#[tokio::test(flavor = "multi_thread")]
#[cfg(feature = "gql")]
async fn test_string_and_numeric_properties() {
    let db = setup_graph_db().await;

    db.execute("CREATE (:Item {name: 'Widget', price: 9.99, quantity: 42, active: true})")
        .unwrap();

    let result = db
        .execute("MATCH (n:Item) RETURN n.name, n.price, n.quantity, n.active")
        .expect("Mixed property types should work");

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.columns.len(), 4);
}
