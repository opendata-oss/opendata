#![cfg(feature = "http-server")]

use std::collections::BTreeSet;
use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use common::StorageConfig;
use reqwest::Client;
use serde_json::json;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use vector::server::{VectorServer, VectorServerConfig};
use vector::{Config, DistanceMetric, FieldType, MetadataFieldSpec, VectorDb};

#[derive(Clone, Debug)]
struct TestVector {
    id: &'static str,
    values: Vec<f32>,
    category: &'static str,
    department: &'static str,
    rank: i64,
}

struct TestServerFixture {
    base_url: String,
    client: Client,
    server_task: JoinHandle<()>,
    vectors: Vec<TestVector>,
}

impl Drop for TestServerFixture {
    fn drop(&mut self) {
        self.server_task.abort();
    }
}

fn test_vectors() -> Vec<TestVector> {
    vec![
        TestVector {
            id: "doc-1",
            values: vec![0.0, 0.0],
            category: "electronics",
            department: "hw",
            rank: 1,
        },
        TestVector {
            id: "doc-2",
            values: vec![10.0, 0.0],
            category: "electronics",
            department: "hw",
            rank: 2,
        },
        TestVector {
            id: "doc-3",
            values: vec![20.0, 0.0],
            category: "electronics",
            department: "hw",
            rank: 3,
        },
        TestVector {
            id: "doc-4",
            values: vec![30.0, 0.0],
            category: "electronics",
            department: "hw",
            rank: 4,
        },
        TestVector {
            id: "doc-5",
            values: vec![40.0, 0.0],
            category: "electronics",
            department: "hw",
            rank: 5,
        },
        TestVector {
            id: "doc-6",
            values: vec![50.0, 0.0],
            category: "books",
            department: "media",
            rank: 6,
        },
        TestVector {
            id: "doc-7",
            values: vec![60.0, 0.0],
            category: "books",
            department: "media",
            rank: 7,
        },
        TestVector {
            id: "doc-8",
            values: vec![70.0, 0.0],
            category: "books",
            department: "media",
            rank: 8,
        },
        TestVector {
            id: "doc-9",
            values: vec![80.0, 0.0],
            category: "books",
            department: "media",
            rank: 9,
        },
        TestVector {
            id: "doc-10",
            values: vec![90.0, 0.0],
            category: "books",
            department: "media",
            rank: 10,
        },
    ]
}

async fn setup_server_with_vectors() -> TestServerFixture {
    let port = pick_unused_port();
    let client = Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let vectors = test_vectors();
    let metadata_fields = vec![
        MetadataFieldSpec::new("category", FieldType::String, true),
        MetadataFieldSpec::new("department", FieldType::String, true),
        MetadataFieldSpec::new("rank", FieldType::Int64, true),
    ];
    let config = Config {
        storage: StorageConfig::InMemory,
        dimensions: 2,
        distance_metric: DistanceMetric::L2,
        metadata_fields: metadata_fields.clone(),
        ..Default::default()
    };
    let db = Arc::new(VectorDb::open(config).await.unwrap());
    let server = VectorServer::new(
        db.clone(),
        VectorServerConfig { port },
        metadata_fields.clone(),
    );
    let server_task = tokio::spawn(async move {
        server.run().await;
    });

    let base_url = format!("http://127.0.0.1:{port}");
    wait_for_server_ready(&client, &base_url).await;
    write_vectors(&client, &base_url, &vectors).await;
    db.flush().await.unwrap();

    TestServerFixture {
        base_url,
        client,
        server_task,
        vectors,
    }
}

fn pick_unused_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

async fn wait_for_server_ready(client: &Client, base_url: &str) {
    for _ in 0..50 {
        if let Ok(response) = client.get(format!("{base_url}/-/healthy")).send().await
            && response.status().is_success()
        {
            return;
        }
        sleep(Duration::from_millis(100)).await;
    }

    panic!("server did not become ready");
}

async fn write_vectors(client: &Client, base_url: &str, vectors: &[TestVector]) {
    let payload = json!({
        "upsertVectors": vectors
            .iter()
            .map(|vector| json!({
                "id": vector.id,
                "attributes": {
                    "vector": vector.values,
                    "category": vector.category,
                    "department": vector.department,
                    "rank": vector.rank,
                }
            }))
            .collect::<Vec<_>>()
    });

    let response = client
        .post(format!("{base_url}/api/v1/vector/write"))
        .header("content-type", "application/protobuf+json")
        .body(serde_json::to_vec(&payload).unwrap())
        .send()
        .await
        .unwrap();

    assert!(response.status().is_success());

    let body: serde_json::Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    assert_eq!(body["status"], "success");
    assert_eq!(body["vectorsUpserted"], vectors.len());
}

#[tokio::test(flavor = "multi_thread")]
async fn should_return_vectors_from_search() {
    // given
    let fixture = setup_server_with_vectors().await;
    let expected_ids: BTreeSet<&str> = fixture.vectors.iter().map(|vector| vector.id).collect();

    for vector in &fixture.vectors {
        // when
        let response = fixture
            .client
            .post(format!("{}/api/v1/vector/search", fixture.base_url))
            .header("content-type", "application/protobuf+json")
            .header("accept", "application/protobuf+json")
            .body(
                serde_json::to_vec(&json!({
                    "vector": vector.values,
                    "k": fixture.vectors.len(),
                    "nprobe": fixture.vectors.len(),
                }))
                .unwrap(),
            )
            .send()
            .await
            .unwrap();

        // then
        assert!(response.status().is_success());
        let body: serde_json::Value =
            serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
        assert_eq!(body["status"], "success");
        assert_eq!(
            body["results"].as_array().unwrap().len(),
            fixture.vectors.len()
        );

        let returned_ids: BTreeSet<&str> = body["results"]
            .as_array()
            .unwrap()
            .iter()
            .map(|result| result["vector"]["id"].as_str().unwrap())
            .collect();
        assert_eq!(returned_ids, expected_ids);
        assert_eq!(body["results"][0]["vector"]["id"], vector.id);
        assert_eq!(
            body["results"][0]["vector"]["attributes"]["vector"],
            json!(vector.values)
        );
        assert_eq!(
            body["results"][0]["vector"]["attributes"]["category"],
            vector.category
        );
        assert_eq!(
            body["results"][0]["vector"]["attributes"]["department"],
            vector.department
        );
        assert_eq!(
            body["results"][0]["vector"]["attributes"]["rank"],
            vector.rank
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn should_return_filtered_vectors_from_search() {
    // given
    let fixture = setup_server_with_vectors().await;

    // when
    let response = fixture
        .client
        .post(format!("{}/api/v1/vector/search", fixture.base_url))
        .header("content-type", "application/protobuf+json")
        .header("accept", "application/protobuf+json")
        .body(
            serde_json::to_vec(&json!({
                "vector": [0.0, 0.0],
                "k": fixture.vectors.len(),
                "nprobe": fixture.vectors.len(),
                "filter": {
                    "and": [
                        { "eq": { "field": "category", "value": "electronics" } },
                        { "eq": { "field": "department", "value": "hw" } }
                    ]
                }
            }))
            .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // then
    assert!(response.status().is_success());
    let body: serde_json::Value = serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
    assert_eq!(body["status"], "success");
    assert_eq!(body["results"].as_array().unwrap().len(), 5);

    let returned_ids: BTreeSet<&str> = body["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|result| result["vector"]["id"].as_str().unwrap())
        .collect();
    assert_eq!(
        returned_ids,
        BTreeSet::from(["doc-1", "doc-2", "doc-3", "doc-4", "doc-5"])
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn should_get_vectors_by_id() {
    // given
    let fixture = setup_server_with_vectors().await;

    for vector in &fixture.vectors {
        // when
        let response = fixture
            .client
            .get(format!(
                "{}/api/v1/vector/vectors/{}",
                fixture.base_url, vector.id
            ))
            .header("accept", "application/protobuf+json")
            .send()
            .await
            .unwrap();

        // then
        assert!(response.status().is_success());
        let body: serde_json::Value =
            serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
        assert_eq!(body["status"], "success");
        assert_eq!(body["vector"]["id"], vector.id);
        assert_eq!(body["vector"]["attributes"]["vector"], json!(vector.values));
        assert_eq!(body["vector"]["attributes"]["category"], vector.category);
        assert_eq!(
            body["vector"]["attributes"]["department"],
            vector.department
        );
        assert_eq!(body["vector"]["attributes"]["rank"], vector.rank);
    }
}
