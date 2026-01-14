# Vector Database

A SPANN-style vector database for approximate nearest neighbor (ANN) search, built on SlateDB.

## Architecture

See `rfcs/0001-storage.md` for the complete storage design and `rfcs/0002-write-api.md` for the public API.

### Key Concepts

- **SPANN Index**: Cluster centroids in memory (HNSW), posting lists on disk
- **External ID**: User-provided string (max 64 bytes) -> mapped to internal u64 via IdDictionary
- **Internal ID**: System-assigned u64 for efficient bitmap operations (RoaringTreemap)
- **Centroid ID**: u32 cluster identifier. ID 0 is reserved for deleted vectors bitmap.
- **Upsert**: Delete old + insert new (no read-modify-write)

### Record Types (in `src/serde/`)

| Type | File | Description |
|------|------|-------------|
| `CollectionMeta` | `collection_meta.rs` | Schema: dimensions, distance metric, metadata fields |
| `CentroidChunk` | `centroid_chunk.rs` | Chunk of cluster centroid vectors |
| `PostingList` | `posting_list.rs` | centroid_id -> RoaringTreemap of vector_ids |
| `IdDictionary` | `id_dictionary.rs` | external_id -> internal vector_id |
| `VectorData` | `vector_data.rs` | vector_id -> f32[dimensions] |
| `VectorMeta` | `vector_meta.rs` | vector_id -> external_id + metadata fields |
| `MetadataIndex` | `metadata_index.rs` | (field, value) -> RoaringTreemap of vector_ids |

### Key Modules

- `src/db.rs` - Core `VectorDb` implementation
- `src/model.rs` - Public types (Vector, Attribute, Config)
- `src/serde/` - Record serialization
- `src/storage/` - SlateDB integration with merge operators
- `src/delta.rs` - Write batching and delta operations

### Query Path

1. Search centroid HNSW for k nearest centroids
2. Load posting lists for those centroids
3. Load deleted bitmap (centroid_id=0), subtract from candidates
4. If filtered: intersect with MetadataIndex bitmaps
5. Load VectorData, compute exact distances
6. Return top-k results

### LIRE Maintenance (background)

- Split oversized posting lists (create new centroids)
- Merge undersized posting lists
- Reassign boundary vectors
- Clean up deleted vector IDs from posting lists and metadata indexes

## Cross-References

- Serde patterns match `common/src/serde/` and `timeseries/src/serde/`
- Uses `common::SequenceAllocator` for internal ID generation
- Similar RFC structure to `timeseries/rfcs/` - reference for consistency
