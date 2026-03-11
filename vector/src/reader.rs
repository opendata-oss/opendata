//! Read-only client for the vector database.
//!
//! `VectorDbReader` provides a lightweight, read-only interface to a vector
//! database. Unlike `VectorDb`, it does not participate in fencing and does
//! not hold a write lease, so multiple readers can coexist with a single
//! writer.

use crate::Vector;
use crate::db::VectorDbRead;
use crate::error::{Error, Result};
use crate::hnsw::{CentroidGraph, build_centroid_graph};
use crate::model::{ReaderConfig, SearchResult};
use crate::query_engine::{QueryEngine, QueryEngineOptions};
use crate::serde::centroid_chunk::CentroidEntry;
use crate::storage::VectorDbStorageReadExt;
use crate::storage::merge_operator::VectorDbMergeOperator;
use common::StorageSemantics;
use common::storage::factory::{StorageReaderRuntime, create_storage_read};
use std::sync::Arc;

/// Read-only client for querying a vector database.
///
/// `VectorDbReader` loads a vector db from storage for read-only access.
/// NOTE: currently the reader only works for dbs that are static as it does not
///       have a mechanism for refreshing the centroid graph. This is deferred to
///       a later improvement. In the interim we'll first add support for creating
///       a reader from a checkpoint.
pub struct VectorDbReader {
    query_engine: QueryEngine,
}

impl VectorDbReader {
    /// Open a read-only client against an existing vector database.
    ///
    /// Loads the centroid graph from storage. The database must have been
    /// previously initialized by a `VectorDb` writer.
    pub async fn open(config: ReaderConfig) -> Result<Self> {
        Self::open_with_runtime(config, StorageReaderRuntime::new()).await
    }

    /// Open a read-only client with custom runtime options (e.g. block cache).
    pub async fn open_with_runtime(
        config: ReaderConfig,
        runtime: StorageReaderRuntime,
    ) -> Result<Self> {
        let merge_op = VectorDbMergeOperator::new(config.dimensions as usize);
        let storage = create_storage_read(
            &config.storage,
            runtime,
            StorageSemantics::new().with_merge_operator(Arc::new(merge_op)),
            slatedb::config::DbReaderOptions::default(),
        )
        .await?;

        // Load centroids from storage
        let dimensions = config.dimensions as usize;
        let scan_result = storage.scan_all_centroids(dimensions).await?;

        if scan_result.entries.is_empty() {
            return Err(Error::Storage(
                "No centroids found in storage. Database must be initialized by VectorDb first."
                    .to_string(),
            ));
        }

        // Filter out deleted centroids
        let deletions = storage.get_deleted_vectors().await?;
        let live_centroids: Vec<CentroidEntry> = scan_result
            .entries
            .into_iter()
            .filter(|c| !deletions.contains(c.centroid_id))
            .collect();

        let centroid_graph = build_centroid_graph(live_centroids, config.distance_metric)?;

        let options = QueryEngineOptions {
            dimensions: config.dimensions,
            distance_metric: config.distance_metric,
            query_pruning_factor: config.query_pruning_factor,
        };

        let centroid_graph: Arc<dyn CentroidGraph> = Arc::from(centroid_graph);
        let query_engine = QueryEngine::new(options, centroid_graph, storage);
        Ok(Self { query_engine })
    }

    /// Search using brute-force centroid lookup (for diagnostics).
    pub async fn search_exact_nprobe(
        &self,
        query: &[f32],
        k: usize,
        nprobe: usize,
    ) -> Result<Vec<SearchResult>> {
        self.query_engine
            .search_exact_nprobe(query, k, nprobe)
            .await
    }
}

impl VectorDbRead for VectorDbReader {
    async fn search(&self, query: &[f32], k: usize) -> Result<Vec<SearchResult>> {
        self.query_engine.search(query, k).await
    }

    async fn search_with_nprobe(
        &self,
        query: &[f32],
        k: usize,
        nprobe: usize,
    ) -> Result<Vec<SearchResult>> {
        self.query_engine.search_with_nprobe(query, k, nprobe).await
    }

    async fn get(&self, id: &str) -> Result<Option<Vector>> {
        self.query_engine.get(id).await
    }
}

#[cfg(test)]
mod tests {
    use crate::VectorDb;
    use crate::db::VectorDbRead;
    use crate::model::{Config, ReaderConfig, Vector};
    use crate::reader::VectorDbReader;
    use crate::serde::collection_meta::DistanceMetric;
    use common::StorageConfig;
    use common::storage::config::{
        LocalObjectStoreConfig, ObjectStoreConfig, SlateDbStorageConfig,
    };
    use std::time::Duration;
    use tempfile::TempDir;

    fn local_storage_config(dir: &TempDir) -> StorageConfig {
        StorageConfig::SlateDb(SlateDbStorageConfig {
            path: "vector-data".to_string(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: dir.path().to_string_lossy().to_string(),
            }),
            settings_path: None,
            block_cache: None,
        })
    }

    #[tokio::test]
    async fn should_search_vectors_via_reader() {
        // given - write vectors via VectorDb
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let storage = local_storage_config(&temp_dir);

        let config = Config {
            storage: storage.clone(),
            dimensions: 3,
            distance_metric: DistanceMetric::L2,
            flush_interval: Duration::from_secs(60),
            split_threshold_vectors: 10_000,
            ..Default::default()
        };
        let db = VectorDb::open(config).await.unwrap();

        let vectors = vec![
            Vector::new("vec-1", vec![1.0, 0.0, 0.0]),
            Vector::new("vec-2", vec![0.0, 1.0, 0.0]),
            Vector::new("vec-3", vec![0.0, 0.0, 1.0]),
        ];
        db.write(vectors).await.unwrap();
        db.flush().await.unwrap();

        // when - open a reader and search
        let reader_config = ReaderConfig {
            storage,
            dimensions: 3,
            distance_metric: DistanceMetric::L2,
            query_pruning_factor: None,
            metadata_fields: vec![],
        };
        let reader = VectorDbReader::open(reader_config).await.unwrap();
        let results = reader.search(&[1.0, 0.0, 0.0], 2).await.unwrap();

        // then - closest vector should be vec-1
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].external_id, "vec-1");
    }
}
