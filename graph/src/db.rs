//! GraphDb: the high-level database handle that wires SlateGraphStore into GrafeoDB.

use std::sync::Arc;

use common::storage::Storage;
use common::{StorageRuntime, StorageSemantics};

use crate::storage::merge_operator::GraphMergeOperator;
use crate::storage::SlateGraphStore;
use crate::{Config, Result};

/// High-level graph database backed by SlateDB.
///
/// Wraps [`SlateGraphStore`] and, when the `grafeo-engine` feature is enabled,
/// provides a [`GrafeoDB`] query engine for GQL/Cypher/etc.
pub struct GraphDb {
    store: Arc<SlateGraphStore>,
    #[cfg(feature = "gql")]
    engine: grafeo_engine::GrafeoDB,
}

impl GraphDb {
    /// Opens a graph database from configuration.
    ///
    /// Creates the storage backend (with the correct merge operator) and
    /// initializes the graph store and query engine. This is the recommended
    /// constructor for most use cases.
    pub async fn open_with_config(config: &Config) -> Result<Self> {
        let storage = common::create_storage(
            &config.storage,
            StorageRuntime::new(),
            StorageSemantics::new().with_merge_operator(Arc::new(GraphMergeOperator)),
        )
        .await?;

        Self::open(storage, config).await
    }

    /// Opens a graph database using a pre-created storage backend.
    ///
    /// Use this when you need custom `StorageRuntime` options (e.g. a
    /// dedicated compaction runtime or block cache). The caller is
    /// responsible for providing a `GraphMergeOperator` via
    /// `StorageSemantics` when creating the storage.
    pub async fn open(storage: Arc<dyn Storage>, config: &Config) -> Result<Self> {
        let store = Arc::new(SlateGraphStore::new(storage, config).await?);

        #[cfg(feature = "gql")]
        let engine = {
            let engine_config = grafeo_engine::Config::in_memory();
            grafeo_engine::GrafeoDB::with_store(store.clone(), engine_config)?
        };

        Ok(Self {
            store,
            #[cfg(feature = "gql")]
            engine,
        })
    }

    /// Returns a reference to the underlying store.
    pub fn store(&self) -> &Arc<SlateGraphStore> {
        &self.store
    }

    /// Executes a GQL query and returns the result.
    #[cfg(feature = "gql")]
    pub fn execute(&self, query: &str) -> Result<grafeo_engine::database::QueryResult> {
        self.engine.execute(query).map_err(Into::into)
    }

    /// Executes a GQL query with parameters.
    #[cfg(feature = "gql")]
    pub fn execute_with_params(
        &self,
        query: &str,
        params: std::collections::HashMap<String, grafeo_common::types::Value>,
    ) -> Result<grafeo_engine::database::QueryResult> {
        self.engine
            .execute_with_params(query, params)
            .map_err(Into::into)
    }

    /// Returns a reference to the GrafeoDB engine (for advanced use).
    #[cfg(feature = "gql")]
    pub fn engine(&self) -> &grafeo_engine::GrafeoDB {
        &self.engine
    }

    /// Registers storage metrics into a Prometheus registry.
    #[cfg(feature = "http-server")]
    pub fn register_metrics(&self, _registry: &mut prometheus_client::registry::Registry) {
        // Storage-level metrics can be registered here in the future.
    }
}
