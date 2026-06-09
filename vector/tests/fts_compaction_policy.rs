//! Integration test for the RFC-0006 Milestone 2 custom compaction *scheduler*.
//!
//! Opens a real SlateDB-backed `VectorDb` with a text field and an aggressive
//! compaction configuration, inserts documents, deletes more than the default
//! 20% `delete_compaction_threshold`, and drives flushes. It then observes —
//! via a read-only `slatedb::admin::Admin` polling the *manifest* — that the FTS
//! segment's compacted sorted runs are rewritten only after the deletes push the
//! outstanding-delete fraction over the threshold, demonstrating that
//! [`VectorCompactionPolicy`](vector) forced a major (full) FTS-segment
//! compaction.
//!
//! # Observing through the manifest's per-segment view
//!
//! SlateDB's public `VersionedManifest` exposes per-segment LSM state via
//! [`segments()`](slatedb::manifest::VersionedManifest::segments) and
//! [`segment(prefix)`](slatedb::manifest::VersionedManifest::segment), so an
//! external reader can read the FTS segment's compacted sorted runs — and the
//! SST views backing them — directly.
//!
//! A full-segment compaction merges every sorted run into the segment's
//! *lowest-id* run, so it does not necessarily change the run count or its id
//! (a segment already holding a single run is recompacted in place under the
//! same id). What it always does is rewrite that run's postings — pruning the
//! deleted ids — into entirely new SSTs. The test therefore keys off the SST
//! view ids: once none of the baseline SSTs survive in the segment's sorted
//! runs, the compacted FTS data has been rewritten. Since no documents are
//! written after the baseline is taken, that rewrite can only be the cleanup
//! compaction reacting to the deletes.
//!
//! The forced compaction is a *background* event with no synchronous trigger or
//! completion signal (the notify/reset protocol is a known RFC-0006 TODO), so
//! the test is tolerant of background timing by polling the manifest with a
//! bounded budget.

use std::sync::Arc;
use std::time::Duration;

use common::storage::config::{LocalObjectStoreConfig, ObjectStoreConfig, SlateDbStorageConfig};
use common::{StorageConfig, create_object_store};
use slatedb::admin::Admin;
use vector::{
    AttributeValue, Config, DistanceMetric, FieldType, MetadataFieldSpec, Query, Vector, VectorDb,
    VectorDbRead,
};

const DIMS: u16 = 3;
const DB_PATH: &str = "data";

/// The 3-byte FTS routing prefix `[SUBSYSTEM, Segment::Fts, KEY_VERSION]` the
/// segment extractor and compaction policy use, and the prefix under which the
/// FTS segment is keyed in the manifest's `segments` list. `vector`'s `serde`
/// constants are crate-internal, so the FTS segment byte (`0x02`) and key
/// version (`0x01`) are documented inline; the subsystem byte comes from the
/// shared `common` registry. If any of those change this test will (correctly)
/// stop matching and must be updated alongside the routing logic.
fn fts_segment_prefix() -> Vec<u8> {
    vec![common::serde::subsystem::VECTOR, 0x02, 0x01]
}

fn slatedb_settings_toml() -> String {
    // Aggressive compaction tuning so a handful of flushes produce several FTS
    // sorted runs and the compactor reacts quickly:
    // - tiny L0 SSTs so each flush lands an SST,
    // - frequent manifest/compactor polling,
    // - `min_compaction_sources = 2` so size-tiered builds SRs from L0 fast.
    r#"
manifest_poll_interval = "100ms"
l0_sst_size_bytes = 256

[compactor_options]
poll_interval = "100ms"
max_concurrent_compactions = 4

[compactor_options.size_tiered]
min_compaction_sources = "2"
max_compaction_sources = "32"
"#
    .to_string()
}

fn make_config(dir: &std::path::Path, settings_path: &std::path::Path) -> Config {
    Config {
        storage: StorageConfig::SlateDb(SlateDbStorageConfig {
            path: DB_PATH.to_string(),
            object_store: ObjectStoreConfig::Local(LocalObjectStoreConfig {
                path: dir.to_str().unwrap().to_string(),
            }),
            settings_path: Some(settings_path.to_str().unwrap().to_string()),
            block_cache: None,
        }),
        dimensions: DIMS,
        distance_metric: DistanceMetric::L2,
        flush_interval: Duration::from_secs(60),
        split_threshold_vectors: 10_000,
        merge_threshold_vectors: 0,
        split_search_neighbourhood: 0,
        metadata_fields: vec![MetadataFieldSpec::new("body", FieldType::Text, false)],
        // default delete_compaction_threshold = 0.20
        // Poll the field stats aggressively so the scheduler's in-memory tracker
        // reflects the deletes quickly within the test's polling budget.
        fts_stats_poll_interval: Duration::from_millis(200),
        ..Default::default()
    }
}

fn doc(id: &str) -> Vector {
    // Use a shared term so every doc lands in the same posting list, giving the
    // FTS segment real content to compact.
    Vector::builder(id, vec![0.0; DIMS as usize])
        .attribute(
            "body",
            AttributeValue::String("the quick brown fox jumps".to_string()),
        )
        .build()
}

/// Snapshot of the FTS segment's compacted sorted runs as seen in the latest
/// manifest: each entry is `(sorted_run_id, sst_view_ids)`, in manifest order.
/// Empty if the manifest or the FTS segment isn't present yet, or while all FTS
/// data still sits in L0 before the first compaction has produced a sorted run.
async fn fts_compacted_runs(admin: &Admin, fts_prefix: &[u8]) -> Vec<(u32, Vec<String>)> {
    match admin.read_manifest(None).await {
        Ok(Some(manifest)) => manifest
            .segment(fts_prefix)
            .map(|segment| {
                segment
                    .compacted()
                    .iter()
                    .map(|run| {
                        let ssts = run.sst_views.iter().map(|v| v.id.to_string()).collect();
                        (run.id, ssts)
                    })
                    .collect()
            })
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

/// The flat set of SST view ids backing the FTS segment's sorted runs.
fn sst_view_ids(runs: &[(u32, Vec<String>)]) -> std::collections::HashSet<String> {
    runs.iter()
        .flat_map(|(_, ssts)| ssts.iter().cloned())
        .collect()
}

/// Polls the FTS segment's compacted sorted runs until `predicate` holds or the
/// budget expires. Returns the final observed snapshot.
async fn poll_fts_compacted_runs(
    admin: &Admin,
    fts_prefix: &[u8],
    budget: Duration,
    predicate: impl Fn(&[(u32, Vec<String>)]) -> bool,
) -> Vec<(u32, Vec<String>)> {
    let deadline = std::time::Instant::now() + budget;
    loop {
        let runs = fts_compacted_runs(admin, fts_prefix).await;
        if predicate(&runs) || std::time::Instant::now() >= deadline {
            return runs;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn should_force_full_fts_compaction_when_deletes_exceed_threshold() {
    // given - a real SlateDB-backed db with aggressive compaction settings and a
    // text field.
    let tmp = tempfile::tempdir().unwrap();
    let settings_path = tmp.path().join("slatedb.toml");
    std::fs::write(&settings_path, slatedb_settings_toml()).unwrap();
    let config = make_config(tmp.path(), &settings_path);

    let object_store = create_object_store(match &config.storage {
        StorageConfig::SlateDb(c) => &c.object_store,
        _ => unreachable!(),
    })
    .unwrap();

    let db = VectorDb::open(config).await.unwrap();

    // A read-only Admin pointed at the same path/object store to observe the
    // manifest. SlateDB allows read-only manifest reads alongside the open
    // writer.
    let admin = Admin::builder(DB_PATH, Arc::clone(&object_store)).build();
    let fts_prefix = fts_segment_prefix();

    // when - write 50 docs in batches, flushing after each so the FTS segment
    // accumulates several L0 SSTs (and, via size-tiered, sorted runs).
    let total = 50usize;
    for batch_start in (0..total).step_by(10) {
        let batch: Vec<Vector> = (batch_start..batch_start + 10)
            .map(|i| doc(&format!("doc-{i}")))
            .collect();
        db.write(batch).await.unwrap();
        db.flush().await.unwrap();
    }

    // Let size-tiered build at least one FTS sorted run so a full-segment
    // compaction has something to merge, then baseline the segment's compacted
    // sorted runs and the SST views backing them. The forced compaction will
    // rewrite those SSTs.
    let baseline = poll_fts_compacted_runs(&admin, &fts_prefix, Duration::from_secs(30), |runs| {
        !runs.is_empty()
    })
    .await;
    let baseline_ssts = sst_view_ids(&baseline);

    // Sanity: queries return matching docs before deletes.
    let before = db
        .search(&Query::bm25("body", "fox").with_limit(total))
        .await
        .unwrap();
    assert_eq!(before.len(), total, "all docs should match before deletes");

    // when - delete 30% of the docs (> 20% threshold) and flush so the flusher
    // refreshes the delete tracker and the scheduler sees the crossing.
    let to_delete: Vec<String> = (0..15).map(|i| format!("doc-{i}")).collect();
    db.delete(to_delete.clone()).await.unwrap();
    db.flush().await.unwrap();

    // then - the forced full FTS-segment compaction merges every sorted run into
    // the segment's lowest-id run, rewriting its postings (deleted ids pruned) as
    // entirely new SSTs. Poll the manifest until none of the baseline SST views
    // survive in the segment's sorted runs — proof the compacted FTS data was
    // rewritten. Since no documents are written after the baseline, that rewrite
    // can only be the cleanup compaction VectorCompactionPolicy forced once the
    // deletes crossed the threshold.
    let after = poll_fts_compacted_runs(&admin, &fts_prefix, Duration::from_secs(30), |runs| {
        let current = sst_view_ids(runs);
        !current.is_empty() && current.is_disjoint(&baseline_ssts)
    })
    .await;
    let after_ssts = sst_view_ids(&after);
    assert!(
        !after_ssts.is_empty() && after_ssts.is_disjoint(&baseline_ssts),
        "expected the forced compaction to rewrite the FTS segment's sorted-run SSTs \
         (baseline runs {baseline:?}, observed {after:?})"
    );
    // The full compaction collapses every sorted run into the lowest-id run.
    assert_eq!(
        after.len(),
        1,
        "expected the forced full compaction to collapse the FTS segment to a single \
         sorted run (baseline runs {baseline:?}, observed {after:?})"
    );

    // and - queries remain correct after the forced compaction: the deleted docs
    // are gone, the survivors remain.
    let results = db
        .search(&Query::bm25("body", "fox").with_limit(total))
        .await
        .unwrap();
    assert_eq!(
        results.len(),
        total - to_delete.len(),
        "survivors should remain queryable after forced compaction"
    );
    for r in &results {
        assert!(
            !to_delete.contains(&r.vector.id),
            "deleted doc {} should not appear in results",
            r.vector.id
        );
    }
}
