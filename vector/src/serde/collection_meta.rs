//! CollectionMeta value encoding/decoding.
//!
//! Stores the global schema and configuration for a vector collection.
//!
//! ## Overview
//!
//! `CollectionMeta` is a **singleton record** that defines the structure all vectors
//! in the namespace must conform to. It stores:
//!
//! - **Vector configuration**: dimensionality and distance metric
//! - **Index configuration**: centroids per chunk (CHUNK_TARGET)
//! - **Metadata schema**: field names, types, and indexing flags
//!
//! This record is read on startup to configure the vector index and validate
//! incoming vectors.
//!
//! ## Immutability
//!
//! Several fields are immutable after collection creation:
//!
//! - `dimensions` - All vectors must have the same dimensionality
//! - `distance_metric` - Changing would invalidate all similarity computations
//! - `chunk_target` - Changing would require reorganizing all centroid chunks
//!
//! ## Schema Evolution
//!
//! The `schema_version` field tracks metadata schema changes. Supported evolutions:
//!
//! - **Adding metadata fields**: New fields can be appended. Existing vectors
//!   without the field return null/missing.
//! - **Enabling indexing**: A field's `indexed` flag can change from false to true.
//!   A background job must rebuild `MetadataIndex` entries for existing vectors.
//!
//! Unsupported changes (require creating a new collection):
//!
//! - Changing `dimensions`, `distance_metric`, or `chunk_target`
//! - Removing metadata fields or changing their types
//! - Disabling indexing on a field (index entries would become stale)

use super::{
    Decode, Encode, EncodingError, FieldType, decode_array, decode_utf8, encode_array, encode_utf8,
};
use bytes::{Bytes, BytesMut};

/// Distance metric for vector similarity computation.
///
/// The distance metric determines how vector similarity is computed during search.
/// This is set at collection creation and cannot be changed afterward.
///
/// - **L2**: Euclidean distance. Lower values = more similar. Best for normalized vectors.
/// - **Cosine**: Cosine similarity. Higher values = more similar. Automatically normalizes.
/// - **DotProduct**: Dot product. Higher values = more similar. Fastest but requires normalized vectors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DistanceMetric {
    /// Euclidean (L2) distance: sqrt(sum((a[i] - b[i])²))
    L2 = 0,
    /// Cosine similarity: dot(a, b) / (|a| * |b|)
    Cosine = 1,
    /// Dot product: sum(a[i] * b[i])
    DotProduct = 2,
}

impl DistanceMetric {
    pub fn from_byte(byte: u8) -> Result<Self, EncodingError> {
        match byte {
            0 => Ok(DistanceMetric::L2),
            1 => Ok(DistanceMetric::Cosine),
            2 => Ok(DistanceMetric::DotProduct),
            _ => Err(EncodingError {
                message: format!("Invalid distance metric: {}", byte),
            }),
        }
    }
}

/// Metadata field specification.
///
/// Defines the schema for a single metadata field that can be attached to vectors.
/// Field names must be unique within a collection.
///
/// ## Indexing
///
/// When `indexed` is true, a `MetadataIndex` inverted index is maintained for this
/// field, enabling efficient filtering during hybrid queries (e.g., "find similar
/// vectors where category='shoes'").
///
/// Non-indexed fields are stored in `VectorMeta` but cannot be used in filter
/// predicates efficiently—they require scanning all candidate vectors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataFieldSpec {
    /// Field name (must be unique within collection).
    pub name: String,
    /// Data type for this field's values.
    pub field_type: FieldType,
    /// Whether this field has a `MetadataIndex` for filtering.
    pub indexed: bool,
}

impl MetadataFieldSpec {
    pub fn new(name: impl Into<String>, field_type: FieldType, indexed: bool) -> Self {
        Self {
            name: name.into(),
            field_type,
            indexed,
        }
    }
}

impl Encode for MetadataFieldSpec {
    fn encode(&self, buf: &mut BytesMut) {
        encode_utf8(&self.name, buf);
        buf.extend_from_slice(&[self.field_type as u8]);
        buf.extend_from_slice(&[if self.indexed { 1 } else { 0 }]);
    }
}

impl Decode for MetadataFieldSpec {
    fn decode(buf: &mut &[u8]) -> Result<Self, EncodingError> {
        let name = decode_utf8(buf)?;

        if buf.len() < 2 {
            return Err(EncodingError {
                message: "Buffer too short for MetadataFieldSpec".to_string(),
            });
        }

        let field_type = FieldType::from_byte(buf[0])?;
        let indexed = buf[1] != 0;
        *buf = &buf[2..];

        Ok(MetadataFieldSpec {
            name,
            field_type,
            indexed,
        })
    }
}

/// CollectionMeta value storing collection schema and configuration.
///
/// This is a singleton record (one per collection) that defines the structure
/// all vectors must conform to. It is read on startup and cached in memory.
///
/// ## Value Layout (little-endian)
///
/// ```text
/// ┌────────────────────────────────────────────────────────────────┐
/// │  schema_version:    u32                                        │
/// │  dimensions:        u16                                        │
/// │  distance_metric:   u8   (0=L2, 1=cosine, 2=dot_product)       │
/// │  chunk_target:      u16  (centroids per chunk, default 4096)   │
/// │  metadata_fields:   Array<MetadataFieldSpec>                   │
/// │                                                                │
/// │  MetadataFieldSpec                                             │
/// │  ┌──────────────────────────────────────────────────────────┐  │
/// │  │  name:       Utf8                                        │  │
/// │  │  field_type: u8  (0=string, 1=int64, 2=float64, 3=bool)  │  │
/// │  │  indexed:    u8  (0=false, 1=true)                       │  │
/// │  └──────────────────────────────────────────────────────────┘  │
/// └────────────────────────────────────────────────────────────────┘
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectionMetaValue {
    /// Monotonically increasing version for metadata schema changes.
    ///
    /// Incremented when metadata fields are added or indexing is enabled.
    /// Does not change for vector insertions/deletions.
    pub schema_version: u32,

    /// Fixed dimensionality for all vectors (immutable after creation).
    ///
    /// Common values: 384 (MiniLM), 768 (BERT), 1536 (OpenAI ada-002).
    pub dimensions: u16,

    /// Distance function for similarity computation (immutable after creation).
    pub distance_metric: DistanceMetric,

    /// Target number of centroids per `CentroidChunk` (immutable after creation).
    ///
    /// Default is 4096, which yields ~25 MB per chunk at 1536 dimensions.
    pub chunk_target: u16,

    /// Schema for fields attached to vectors.
    ///
    /// Defines field names, types, and whether each field is indexed for filtering.
    pub fields: Vec<MetadataFieldSpec>,
}

impl CollectionMetaValue {
    pub fn new(
        dimensions: u16,
        distance_metric: DistanceMetric,
        chunk_target: u16,
        metadata_fields: Vec<MetadataFieldSpec>,
    ) -> Self {
        Self {
            schema_version: 1,
            dimensions,
            distance_metric,
            chunk_target,
            fields: metadata_fields,
        }
    }

    pub fn encode_to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&self.schema_version.to_le_bytes());
        buf.extend_from_slice(&self.dimensions.to_le_bytes());
        buf.extend_from_slice(&[self.distance_metric as u8]);
        buf.extend_from_slice(&self.chunk_target.to_le_bytes());
        encode_array(&self.fields, &mut buf);
        buf.freeze()
    }

    pub fn decode_from_bytes(buf: &[u8]) -> Result<Self, EncodingError> {
        if buf.len() < 9 {
            return Err(EncodingError {
                message: format!(
                    "Buffer too short for CollectionMetaValue: need at least 9 bytes, have {}",
                    buf.len()
                ),
            });
        }

        let schema_version = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
        let dimensions = u16::from_le_bytes([buf[4], buf[5]]);
        let distance_metric = DistanceMetric::from_byte(buf[6])?;
        let chunk_target = u16::from_le_bytes([buf[7], buf[8]]);

        let mut slice = &buf[9..];
        let metadata_fields = decode_array(&mut slice)?;

        Ok(CollectionMetaValue {
            schema_version,
            dimensions,
            distance_metric,
            chunk_target,
            fields: metadata_fields,
        })
    }

    /// Find a metadata field spec by name.
    pub fn get_field(&self, name: &str) -> Option<&MetadataFieldSpec> {
        self.fields.iter().find(|f| f.name == name)
    }

    /// Returns the names of all indexed fields.
    pub fn indexed_fields(&self) -> impl Iterator<Item = &str> {
        self.fields
            .iter()
            .filter(|f| f.indexed)
            .map(|f| f.name.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_encode_and_decode_collection_meta() {
        // given
        let value = CollectionMetaValue::new(
            1536,
            DistanceMetric::Cosine,
            4096,
            vec![
                MetadataFieldSpec::new("category", FieldType::String, true),
                MetadataFieldSpec::new("price", FieldType::Float64, true),
                MetadataFieldSpec::new("description", FieldType::String, false),
            ],
        );

        // when
        let encoded = value.encode_to_bytes();
        let decoded = CollectionMetaValue::decode_from_bytes(&encoded).unwrap();

        // then
        assert_eq!(decoded, value);
    }

    #[test]
    fn should_encode_and_decode_with_no_metadata_fields() {
        // given
        let value = CollectionMetaValue::new(384, DistanceMetric::L2, 1024, vec![]);

        // when
        let encoded = value.encode_to_bytes();
        let decoded = CollectionMetaValue::decode_from_bytes(&encoded).unwrap();

        // then
        assert_eq!(decoded, value);
        assert!(decoded.fields.is_empty());
    }

    #[test]
    fn should_find_field_by_name() {
        // given
        let value = CollectionMetaValue::new(
            1536,
            DistanceMetric::Cosine,
            4096,
            vec![
                MetadataFieldSpec::new("category", FieldType::String, true),
                MetadataFieldSpec::new("price", FieldType::Float64, false),
            ],
        );

        // when / then
        let category = value.get_field("category").unwrap();
        assert_eq!(category.field_type, FieldType::String);
        assert!(category.indexed);

        let price = value.get_field("price").unwrap();
        assert_eq!(price.field_type, FieldType::Float64);
        assert!(!price.indexed);

        assert!(value.get_field("unknown").is_none());
    }

    #[test]
    fn should_list_indexed_fields() {
        // given
        let value = CollectionMetaValue::new(
            1536,
            DistanceMetric::Cosine,
            4096,
            vec![
                MetadataFieldSpec::new("category", FieldType::String, true),
                MetadataFieldSpec::new("price", FieldType::Float64, true),
                MetadataFieldSpec::new("description", FieldType::String, false),
            ],
        );

        // when
        let indexed: Vec<&str> = value.indexed_fields().collect();

        // then
        assert_eq!(indexed, vec!["category", "price"]);
    }

    #[test]
    fn should_preserve_all_distance_metrics() {
        for metric in [
            DistanceMetric::L2,
            DistanceMetric::Cosine,
            DistanceMetric::DotProduct,
        ] {
            // given
            let value = CollectionMetaValue::new(128, metric, 2048, vec![]);

            // when
            let encoded = value.encode_to_bytes();
            let decoded = CollectionMetaValue::decode_from_bytes(&encoded).unwrap();

            // then
            assert_eq!(decoded.distance_metric, metric);
        }
    }

    #[test]
    fn should_preserve_all_field_types() {
        for field_type in [
            FieldType::String,
            FieldType::Int64,
            FieldType::Float64,
            FieldType::Bool,
        ] {
            // given
            let value = CollectionMetaValue::new(
                128,
                DistanceMetric::L2,
                2048,
                vec![MetadataFieldSpec::new("test", field_type, true)],
            );

            // when
            let encoded = value.encode_to_bytes();
            let decoded = CollectionMetaValue::decode_from_bytes(&encoded).unwrap();

            // then
            assert_eq!(decoded.fields[0].field_type, field_type);
        }
    }
}
