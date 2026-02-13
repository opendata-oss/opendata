use std::sync::Arc;

use slatedb::object_store::path::Path;
use slatedb::object_store::{ObjectStore, PutPayload};

use crate::config::Config;
use crate::error::{Error, Result};
use crate::model::KeyValueEntry;

pub struct Ingestor {
    object_store: Arc<dyn ObjectStore>,
    http_client: reqwest::Client,
    endpoint: String,
    path_prefix: String,
}

impl Ingestor {
    pub fn new(config: Config) -> Result<Self> {
        let object_store = common::storage::factory::create_object_store(&config.object_store)
            .map_err(|e| Error::Storage(e.to_string()))?;
        Self::with_object_store(config, object_store)
    }

    pub fn with_object_store(
        config: Config,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self> {
        let http_client = reqwest::Client::new();
        Ok(Self {
            object_store,
            http_client,
            endpoint: config.endpoint,
            path_prefix: config.path_prefix,
        })
    }

    pub async fn ingest(&self, entries: Vec<KeyValueEntry>) -> Result<()> {
        let json = serde_json::to_vec(&entries)
            .map_err(|e| Error::Serialization(e.to_string()))?;

        let id = uuid::Uuid::new_v4();
        let path = Path::from(format!("{}/{}.json", self.path_prefix, id));

        self.object_store
            .put(&path, PutPayload::from(json))
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        let response = self
            .http_client
            .post(&self.endpoint)
            .json(&serde_json::json!({ "location": path.to_string() }))
            .send()
            .await
            .map_err(|e| Error::Http(e.to_string()))?;

        if !response.status().is_success() {
            return Err(Error::Http(format!(
                "endpoint returned status {}",
                response.status()
            )));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use axum::routing::post;
    use axum::Router;
    use bytes::Bytes;
    use common::storage::config::ObjectStoreConfig;
    use slatedb::object_store::ObjectStore;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::net::TcpListener;

    fn test_config(endpoint: String) -> Config {
        Config {
            object_store: ObjectStoreConfig::InMemory,
            endpoint,
            path_prefix: "test-ingest".to_string(),
        }
    }

    async fn start_mock_server(
        status: axum::http::StatusCode,
    ) -> (String, Arc<AtomicUsize>, Arc<tokio::sync::Mutex<Vec<String>>>) {
        let call_count = Arc::new(AtomicUsize::new(0));
        let locations = Arc::new(tokio::sync::Mutex::new(Vec::<String>::new()));

        let count = call_count.clone();
        let locs = locations.clone();
        let app = Router::new().route(
            "/notify",
            post(move |body: axum::Json<serde_json::Value>| {
                let count = count.clone();
                let locs = locs.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    if let Some(loc) = body.get("location").and_then(|v| v.as_str()) {
                        locs.lock().await.push(loc.to_string());
                    }
                    status
                }
            }),
        );

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        (format!("http://{}/notify", addr), call_count, locations)
    }

    #[tokio::test]
    async fn should_ingest_entries_and_notify_endpoint() {
        let (url, call_count, locations) =
            start_mock_server(axum::http::StatusCode::OK).await;

        let config = test_config(url);
        let ingestor = Ingestor::new(config.clone()).unwrap();

        let entries = vec![
            KeyValueEntry {
                key: "key1".to_string(),
                value: Bytes::from("value1"),
            },
            KeyValueEntry {
                key: "key2".to_string(),
                value: Bytes::from("value2"),
            },
        ];

        ingestor.ingest(entries).await.unwrap();

        assert_eq!(call_count.load(Ordering::SeqCst), 1);
        let locs = locations.lock().await;
        assert_eq!(locs.len(), 1);
        assert!(locs[0].starts_with("test-ingest/"));
        assert!(locs[0].ends_with(".json"));
    }

    #[tokio::test]
    async fn should_write_valid_json_to_object_store() {
        let (url, _, locations) = start_mock_server(axum::http::StatusCode::OK).await;

        let config = test_config(url);
        let object_store: Arc<dyn ObjectStore> =
            Arc::new(slatedb::object_store::memory::InMemory::new());
        let ingestor = Ingestor::with_object_store(config, object_store.clone()).unwrap();

        let entries = vec![KeyValueEntry {
            key: "mykey".to_string(),
            value: Bytes::from("myvalue"),
        }];

        ingestor.ingest(entries).await.unwrap();

        let locs = locations.lock().await;
        let path = Path::from(locs[0].as_str());
        let data = object_store.get(&path).await.unwrap().bytes().await.unwrap();
        let parsed: Vec<KeyValueEntry> = serde_json::from_slice(&data).unwrap();

        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].key, "mykey");
        assert_eq!(parsed[0].value, Bytes::from("myvalue"));
    }

    #[tokio::test]
    async fn should_fail_when_endpoint_returns_error() {
        let (url, call_count, _) =
            start_mock_server(axum::http::StatusCode::INTERNAL_SERVER_ERROR).await;

        let ingestor = Ingestor::new(test_config(url)).unwrap();

        let entries = vec![KeyValueEntry {
            key: "k".to_string(),
            value: Bytes::from("v"),
        }];

        let result = ingestor.ingest(entries).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, Error::Http(_)));
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }
}
