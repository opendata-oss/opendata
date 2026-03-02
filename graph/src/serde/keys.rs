use bytes::{BufMut, Bytes, BytesMut};
use common::serde::key_prefix::{KeyPrefix, RecordTag};
use common::serde::terminated_bytes;
use common::serde::DeserializeError;
use common::BytesRange;
use std::ops::Bound;

use super::{CatalogKind, MetadataSubType, RecordType, SequenceKind, KEY_VERSION};

// ---------------------------------------------------------------------------
// NodeRecordKey: [ver][0x10][node_id:u64 BE][epoch:u64 BE] = 18 bytes
// ---------------------------------------------------------------------------

/// Key for versioned node entity records.
///
/// Multiple versions of the same node coexist under the same `node_id` prefix,
/// ordered by epoch. The latest version with `epoch <= target` and `!FLAGS_DELETED`
/// is the visible version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NodeRecordKey {
    pub node_id: u64,
    pub epoch: u64,
}

impl NodeRecordKey {
    const SIZE: usize = 18;

    pub fn encode(&self) -> Bytes {
        let tag = RecordTag::new(RecordType::NodeRecord as u8, 0);
        let mut buf = BytesMut::with_capacity(Self::SIZE);
        KeyPrefix::new(KEY_VERSION, tag).write_to(&mut buf);
        buf.put_u64(self.node_id);
        buf.put_u64(self.epoch);
        buf.freeze()
    }

    pub fn decode(data: &[u8]) -> Result<Self, DeserializeError> {
        if data.len() < Self::SIZE {
            return Err(DeserializeError {
                message: format!("NodeRecordKey too short: need {}, got {}", Self::SIZE, data.len()),
            });
        }
        let prefix = KeyPrefix::from_bytes_versioned(data, KEY_VERSION)?;
        if prefix.tag().record_type() != RecordType::NodeRecord as u8 {
            return Err(DeserializeError {
                message: format!("expected NodeRecord tag, got {}", prefix.tag().record_type()),
            });
        }
        let node_id = u64::from_be_bytes(data[2..10].try_into().unwrap());
        let epoch = u64::from_be_bytes(data[10..18].try_into().unwrap());
        Ok(Self { node_id, epoch })
    }

    /// Scan range for all versions of a specific node.
    pub fn node_prefix(node_id: u64) -> BytesRange {
        let tag = RecordTag::new(RecordType::NodeRecord as u8, 0);
        let mut start = BytesMut::with_capacity(10);
        KeyPrefix::new(KEY_VERSION, tag).write_to(&mut start);
        start.put_u64(node_id);
        BytesRange::prefix(start.freeze())
    }

    /// Scan range for all node records (all nodes, all versions).
    pub fn all_nodes_range() -> BytesRange {
        record_type_range(RecordType::NodeRecord)
    }
}

// ---------------------------------------------------------------------------
// EdgeRecordKey: [ver][0x20][edge_id:u64 BE][epoch:u64 BE] = 18 bytes
// ---------------------------------------------------------------------------

/// Key for versioned edge entity records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EdgeRecordKey {
    pub edge_id: u64,
    pub epoch: u64,
}

impl EdgeRecordKey {
    const SIZE: usize = 18;

    pub fn encode(&self) -> Bytes {
        let tag = RecordTag::new(RecordType::EdgeRecord as u8, 0);
        let mut buf = BytesMut::with_capacity(Self::SIZE);
        KeyPrefix::new(KEY_VERSION, tag).write_to(&mut buf);
        buf.put_u64(self.edge_id);
        buf.put_u64(self.epoch);
        buf.freeze()
    }

    #[cfg(test)]
    pub fn decode(data: &[u8]) -> Result<Self, DeserializeError> {
        if data.len() < Self::SIZE {
            return Err(DeserializeError {
                message: format!("EdgeRecordKey too short: need {}, got {}", Self::SIZE, data.len()),
            });
        }
        let prefix = KeyPrefix::from_bytes_versioned(data, KEY_VERSION)?;
        if prefix.tag().record_type() != RecordType::EdgeRecord as u8 {
            return Err(DeserializeError {
                message: format!("expected EdgeRecord tag, got {}", prefix.tag().record_type()),
            });
        }
        let edge_id = u64::from_be_bytes(data[2..10].try_into().unwrap());
        let epoch = u64::from_be_bytes(data[10..18].try_into().unwrap());
        Ok(Self { edge_id, epoch })
    }

    /// Scan range for all versions of a specific edge.
    pub fn edge_prefix(edge_id: u64) -> BytesRange {
        let tag = RecordTag::new(RecordType::EdgeRecord as u8, 0);
        let mut start = BytesMut::with_capacity(10);
        KeyPrefix::new(KEY_VERSION, tag).write_to(&mut start);
        start.put_u64(edge_id);
        BytesRange::prefix(start.freeze())
    }
}

// ---------------------------------------------------------------------------
// NodePropertyKey: [ver][0x30][node_id:u64 BE][prop_key:terminated] = 10+ bytes
// ---------------------------------------------------------------------------

/// Key for a single node property.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NodePropertyKey {
    pub node_id: u64,
    pub prop_key: Bytes,
}

impl NodePropertyKey {
    pub fn encode(&self) -> Bytes {
        let tag = RecordTag::new(RecordType::NodeProperty as u8, 0);
        let mut buf = BytesMut::with_capacity(10 + self.prop_key.len() + 2);
        KeyPrefix::new(KEY_VERSION, tag).write_to(&mut buf);
        buf.put_u64(self.node_id);
        terminated_bytes::serialize(&self.prop_key, &mut buf);
        buf.freeze()
    }

    pub fn decode(data: &[u8]) -> Result<Self, DeserializeError> {
        if data.len() < 11 {
            return Err(DeserializeError {
                message: format!("NodePropertyKey too short: need >=11, got {}", data.len()),
            });
        }
        let prefix = KeyPrefix::from_bytes_versioned(data, KEY_VERSION)?;
        if prefix.tag().record_type() != RecordType::NodeProperty as u8 {
            return Err(DeserializeError {
                message: format!("expected NodeProperty tag, got {}", prefix.tag().record_type()),
            });
        }
        let node_id = u64::from_be_bytes(data[2..10].try_into().unwrap());
        let mut remaining = &data[10..];
        let prop_key = terminated_bytes::deserialize(&mut remaining)?;
        Ok(Self { node_id, prop_key })
    }

    /// Scan range for all properties of a specific node.
    pub fn node_prefix(node_id: u64) -> BytesRange {
        let tag = RecordTag::new(RecordType::NodeProperty as u8, 0);
        let mut start = BytesMut::with_capacity(10);
        KeyPrefix::new(KEY_VERSION, tag).write_to(&mut start);
        start.put_u64(node_id);
        BytesRange::prefix(start.freeze())
    }
}

// ---------------------------------------------------------------------------
// EdgePropertyKey: [ver][0x40][edge_id:u64 BE][prop_key:terminated] = 10+ bytes
// ---------------------------------------------------------------------------

/// Key for a single edge property.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EdgePropertyKey {
    pub edge_id: u64,
    pub prop_key: Bytes,
}

impl EdgePropertyKey {
    pub fn encode(&self) -> Bytes {
        let tag = RecordTag::new(RecordType::EdgeProperty as u8, 0);
        let mut buf = BytesMut::with_capacity(10 + self.prop_key.len() + 2);
        KeyPrefix::new(KEY_VERSION, tag).write_to(&mut buf);
        buf.put_u64(self.edge_id);
        terminated_bytes::serialize(&self.prop_key, &mut buf);
        buf.freeze()
    }

    pub fn decode(data: &[u8]) -> Result<Self, DeserializeError> {
        if data.len() < 11 {
            return Err(DeserializeError {
                message: format!("EdgePropertyKey too short: need >=11, got {}", data.len()),
            });
        }
        let prefix = KeyPrefix::from_bytes_versioned(data, KEY_VERSION)?;
        if prefix.tag().record_type() != RecordType::EdgeProperty as u8 {
            return Err(DeserializeError {
                message: format!("expected EdgeProperty tag, got {}", prefix.tag().record_type()),
            });
        }
        let edge_id = u64::from_be_bytes(data[2..10].try_into().unwrap());
        let mut remaining = &data[10..];
        let prop_key = terminated_bytes::deserialize(&mut remaining)?;
        Ok(Self { edge_id, prop_key })
    }

    /// Scan range for all properties of a specific edge.
    pub fn edge_prefix(edge_id: u64) -> BytesRange {
        let tag = RecordTag::new(RecordType::EdgeProperty as u8, 0);
        let mut start = BytesMut::with_capacity(10);
        KeyPrefix::new(KEY_VERSION, tag).write_to(&mut start);
        start.put_u64(edge_id);
        BytesRange::prefix(start.freeze())
    }
}

// ---------------------------------------------------------------------------
// ForwardAdjKey: [ver][0x50][src:u64 BE][type_id:u32 BE][dst:u64 BE] = 22 bytes
// ---------------------------------------------------------------------------

/// Key for forward adjacency index (outgoing edges).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ForwardAdjKey {
    pub src: u64,
    pub edge_type_id: u32,
    pub dst: u64,
}

impl ForwardAdjKey {
    const SIZE: usize = 22;

    pub fn encode(&self) -> Bytes {
        let tag = RecordTag::new(RecordType::ForwardAdj as u8, 0);
        let mut buf = BytesMut::with_capacity(Self::SIZE);
        KeyPrefix::new(KEY_VERSION, tag).write_to(&mut buf);
        buf.put_u64(self.src);
        buf.put_u32(self.edge_type_id);
        buf.put_u64(self.dst);
        buf.freeze()
    }

    pub fn decode(data: &[u8]) -> Result<Self, DeserializeError> {
        if data.len() < Self::SIZE {
            return Err(DeserializeError {
                message: format!("ForwardAdjKey too short: need {}, got {}", Self::SIZE, data.len()),
            });
        }
        let prefix = KeyPrefix::from_bytes_versioned(data, KEY_VERSION)?;
        if prefix.tag().record_type() != RecordType::ForwardAdj as u8 {
            return Err(DeserializeError {
                message: format!("expected ForwardAdj tag, got {}", prefix.tag().record_type()),
            });
        }
        let src = u64::from_be_bytes(data[2..10].try_into().unwrap());
        let edge_type_id = u32::from_be_bytes(data[10..14].try_into().unwrap());
        let dst = u64::from_be_bytes(data[14..22].try_into().unwrap());
        Ok(Self { src, edge_type_id, dst })
    }

    /// Scan range for all outgoing edges from a source node.
    pub fn src_prefix(src: u64) -> BytesRange {
        let tag = RecordTag::new(RecordType::ForwardAdj as u8, 0);
        let mut start = BytesMut::with_capacity(10);
        KeyPrefix::new(KEY_VERSION, tag).write_to(&mut start);
        start.put_u64(src);
        BytesRange::prefix(start.freeze())
    }

}

// ---------------------------------------------------------------------------
// BackwardAdjKey: [ver][0x60][dst:u64 BE][type_id:u32 BE][src:u64 BE] = 22 bytes
// ---------------------------------------------------------------------------

/// Key for backward adjacency index (incoming edges).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BackwardAdjKey {
    pub dst: u64,
    pub edge_type_id: u32,
    pub src: u64,
}

impl BackwardAdjKey {
    const SIZE: usize = 22;

    pub fn encode(&self) -> Bytes {
        let tag = RecordTag::new(RecordType::BackwardAdj as u8, 0);
        let mut buf = BytesMut::with_capacity(Self::SIZE);
        KeyPrefix::new(KEY_VERSION, tag).write_to(&mut buf);
        buf.put_u64(self.dst);
        buf.put_u32(self.edge_type_id);
        buf.put_u64(self.src);
        buf.freeze()
    }

    pub fn decode(data: &[u8]) -> Result<Self, DeserializeError> {
        if data.len() < Self::SIZE {
            return Err(DeserializeError {
                message: format!("BackwardAdjKey too short: need {}, got {}", Self::SIZE, data.len()),
            });
        }
        let prefix = KeyPrefix::from_bytes_versioned(data, KEY_VERSION)?;
        if prefix.tag().record_type() != RecordType::BackwardAdj as u8 {
            return Err(DeserializeError {
                message: format!("expected BackwardAdj tag, got {}", prefix.tag().record_type()),
            });
        }
        let dst = u64::from_be_bytes(data[2..10].try_into().unwrap());
        let edge_type_id = u32::from_be_bytes(data[10..14].try_into().unwrap());
        let src = u64::from_be_bytes(data[14..22].try_into().unwrap());
        Ok(Self { dst, edge_type_id, src })
    }

    /// Scan range for all incoming edges to a destination node.
    pub fn dst_prefix(dst: u64) -> BytesRange {
        let tag = RecordTag::new(RecordType::BackwardAdj as u8, 0);
        let mut start = BytesMut::with_capacity(10);
        KeyPrefix::new(KEY_VERSION, tag).write_to(&mut start);
        start.put_u64(dst);
        BytesRange::prefix(start.freeze())
    }

}

// ---------------------------------------------------------------------------
// LabelIndexKey: [ver][0x70][label_id:u32 BE][node_id:u64 BE] = 14 bytes
// ---------------------------------------------------------------------------

/// Key for the label-to-nodes index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LabelIndexKey {
    pub label_id: u32,
    pub node_id: u64,
}

impl LabelIndexKey {
    const SIZE: usize = 14;

    pub fn encode(&self) -> Bytes {
        let tag = RecordTag::new(RecordType::LabelIndex as u8, 0);
        let mut buf = BytesMut::with_capacity(Self::SIZE);
        KeyPrefix::new(KEY_VERSION, tag).write_to(&mut buf);
        buf.put_u32(self.label_id);
        buf.put_u64(self.node_id);
        buf.freeze()
    }

    pub fn decode(data: &[u8]) -> Result<Self, DeserializeError> {
        if data.len() < Self::SIZE {
            return Err(DeserializeError {
                message: format!("LabelIndexKey too short: need {}, got {}", Self::SIZE, data.len()),
            });
        }
        let prefix = KeyPrefix::from_bytes_versioned(data, KEY_VERSION)?;
        if prefix.tag().record_type() != RecordType::LabelIndex as u8 {
            return Err(DeserializeError {
                message: format!("expected LabelIndex tag, got {}", prefix.tag().record_type()),
            });
        }
        let label_id = u32::from_be_bytes(data[2..6].try_into().unwrap());
        let node_id = u64::from_be_bytes(data[6..14].try_into().unwrap());
        Ok(Self { label_id, node_id })
    }

    /// Scan range for all nodes with a specific label.
    pub fn label_prefix(label_id: u32) -> BytesRange {
        let tag = RecordTag::new(RecordType::LabelIndex as u8, 0);
        let mut start = BytesMut::with_capacity(6);
        KeyPrefix::new(KEY_VERSION, tag).write_to(&mut start);
        start.put_u32(label_id);
        BytesRange::prefix(start.freeze())
    }

}

// ---------------------------------------------------------------------------
// PropertyIndexKey: [ver][0x80][prop_id:u32 BE][sortable_value:var][node_id:u64 BE]
// ---------------------------------------------------------------------------

/// Key for the property value index (supports range queries).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PropertyIndexKey {
    pub prop_id: u32,
    pub sortable_value: Bytes,
    pub node_id: u64,
}

impl PropertyIndexKey {
    pub fn encode(&self) -> Bytes {
        let tag = RecordTag::new(RecordType::PropertyIndex as u8, 0);
        let mut buf = BytesMut::with_capacity(14 + self.sortable_value.len());
        KeyPrefix::new(KEY_VERSION, tag).write_to(&mut buf);
        buf.put_u32(self.prop_id);
        buf.extend_from_slice(&self.sortable_value);
        buf.put_u64(self.node_id);
        buf.freeze()
    }

    /// Scan range for nodes with a specific property and exact value.
    pub fn prop_value_prefix(prop_id: u32, sortable_value: &[u8]) -> BytesRange {
        let tag = RecordTag::new(RecordType::PropertyIndex as u8, 0);
        let mut start = BytesMut::with_capacity(6 + sortable_value.len());
        KeyPrefix::new(KEY_VERSION, tag).write_to(&mut start);
        start.put_u32(prop_id);
        start.extend_from_slice(sortable_value);
        BytesRange::prefix(start.freeze())
    }

    /// Range scan for property values between min and max (inclusive/exclusive).
    pub fn prop_value_range(
        prop_id: u32,
        min: Option<&[u8]>,
        max: Option<&[u8]>,
        min_inclusive: bool,
        max_inclusive: bool,
    ) -> BytesRange {
        let tag = RecordTag::new(RecordType::PropertyIndex as u8, 0);

        let start = match min {
            Some(min_val) => {
                let mut buf = BytesMut::with_capacity(6 + min_val.len());
                KeyPrefix::new(KEY_VERSION, tag).write_to(&mut buf);
                buf.put_u32(prop_id);
                buf.extend_from_slice(min_val);
                if min_inclusive {
                    Bound::Included(buf.freeze())
                } else {
                    // Exclude this exact value prefix by appending max suffix
                    buf.put_u64(u64::MAX);
                    Bound::Excluded(buf.freeze())
                }
            }
            None => {
                let mut buf = BytesMut::with_capacity(6);
                KeyPrefix::new(KEY_VERSION, tag).write_to(&mut buf);
                buf.put_u32(prop_id);
                Bound::Included(buf.freeze())
            }
        };

        let end = match max {
            Some(max_val) => {
                let mut buf = BytesMut::with_capacity(6 + max_val.len());
                KeyPrefix::new(KEY_VERSION, tag).write_to(&mut buf);
                buf.put_u32(prop_id);
                buf.extend_from_slice(max_val);
                if max_inclusive {
                    // Include all entries with this value (any node_id)
                    buf.put_u64(u64::MAX);
                    Bound::Included(buf.freeze())
                } else {
                    Bound::Excluded(buf.freeze())
                }
            }
            None => {
                // End at next record type
                let next_tag = RecordTag::new(RecordType::PropertyIndex as u8 + 1, 0);
                let mut buf = BytesMut::with_capacity(2);
                KeyPrefix::new(KEY_VERSION, next_tag).write_to(&mut buf);
                Bound::Excluded(buf.freeze())
            }
        };

        BytesRange::new(start, end)
    }
}

// ---------------------------------------------------------------------------
// CatalogKey: [ver][0x9x][id:u32 BE] or [ver][0x9x][name:terminated]
// ---------------------------------------------------------------------------

/// Key for catalog entries (by ID).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CatalogByIdKey {
    pub kind: CatalogKind,
    pub id: u32,
}

impl CatalogByIdKey {
    const SIZE: usize = 6;

    pub fn encode(&self) -> Bytes {
        let tag = RecordTag::new(RecordType::Catalog as u8, self.kind as u8);
        let mut buf = BytesMut::with_capacity(Self::SIZE);
        KeyPrefix::new(KEY_VERSION, tag).write_to(&mut buf);
        buf.put_u32(self.id);
        buf.freeze()
    }

    pub fn decode(data: &[u8]) -> Result<Self, DeserializeError> {
        if data.len() < Self::SIZE {
            return Err(DeserializeError {
                message: format!("CatalogByIdKey too short: need {}, got {}", Self::SIZE, data.len()),
            });
        }
        let prefix = KeyPrefix::from_bytes_versioned(data, KEY_VERSION)?;
        if prefix.tag().record_type() != RecordType::Catalog as u8 {
            return Err(DeserializeError {
                message: format!("expected Catalog tag, got {}", prefix.tag().record_type()),
            });
        }
        let kind = catalog_kind_from_reserved(prefix.tag().reserved())?;
        let id = u32::from_be_bytes(data[2..6].try_into().unwrap());
        Ok(Self { kind, id })
    }

    /// Scan range for all catalog entries of a specific kind.
    pub fn kind_prefix(kind: CatalogKind) -> BytesRange {
        let tag = RecordTag::new(RecordType::Catalog as u8, kind as u8);
        let prefix = KeyPrefix::new(KEY_VERSION, tag).to_bytes();
        BytesRange::prefix(prefix)
    }
}

/// Key for catalog entries (by name).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CatalogByNameKey {
    pub kind: CatalogKind,
    pub name: Bytes,
}

impl CatalogByNameKey {
    pub fn encode(&self) -> Bytes {
        let tag = RecordTag::new(RecordType::Catalog as u8, self.kind as u8);
        let mut buf = BytesMut::with_capacity(2 + self.name.len() + 2);
        KeyPrefix::new(KEY_VERSION, tag).write_to(&mut buf);
        terminated_bytes::serialize(&self.name, &mut buf);
        buf.freeze()
    }

    #[cfg(test)]
    pub fn decode(data: &[u8]) -> Result<Self, DeserializeError> {
        if data.len() < 3 {
            return Err(DeserializeError {
                message: format!("CatalogByNameKey too short: need >=3, got {}", data.len()),
            });
        }
        let prefix = KeyPrefix::from_bytes_versioned(data, KEY_VERSION)?;
        if prefix.tag().record_type() != RecordType::Catalog as u8 {
            return Err(DeserializeError {
                message: format!("expected Catalog tag, got {}", prefix.tag().record_type()),
            });
        }
        let kind = catalog_kind_from_reserved(prefix.tag().reserved())?;
        let mut remaining = &data[2..];
        let name = terminated_bytes::deserialize(&mut remaining)?;
        Ok(Self { kind, name })
    }
}

// ---------------------------------------------------------------------------
// MetadataKey: [ver][0xE0][sub_type:u8] = 3 bytes
// ---------------------------------------------------------------------------

/// Key for metadata counters and epoch tracking.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MetadataKey {
    pub sub_type: MetadataSubType,
}

impl MetadataKey {
    const SIZE: usize = 3;

    pub fn encode(&self) -> Bytes {
        let tag = RecordTag::new(RecordType::Metadata as u8, 0);
        let mut buf = BytesMut::with_capacity(Self::SIZE);
        KeyPrefix::new(KEY_VERSION, tag).write_to(&mut buf);
        buf.put_u8(self.sub_type as u8);
        buf.freeze()
    }

    #[cfg(test)]
    pub fn decode(data: &[u8]) -> Result<Self, DeserializeError> {
        if data.len() < Self::SIZE {
            return Err(DeserializeError {
                message: format!("MetadataKey too short: need {}, got {}", Self::SIZE, data.len()),
            });
        }
        let prefix = KeyPrefix::from_bytes_versioned(data, KEY_VERSION)?;
        if prefix.tag().record_type() != RecordType::Metadata as u8 {
            return Err(DeserializeError {
                message: format!("expected Metadata tag, got {}", prefix.tag().record_type()),
            });
        }
        let sub_type = match data[2] {
            0 => MetadataSubType::NodeCount,
            1 => MetadataSubType::EdgeCount,
            2 => MetadataSubType::CurrentEpoch,
            other => {
                return Err(DeserializeError {
                    message: format!("unknown metadata sub-type: {other}"),
                });
            }
        };
        Ok(Self { sub_type })
    }
}

// ---------------------------------------------------------------------------
// SequenceKey: [ver][0xFx] = 2 bytes (used by SequenceAllocator)
// ---------------------------------------------------------------------------

/// Key for sequence allocator blocks (node IDs, edge IDs).
pub(crate) struct SequenceKey;

impl SequenceKey {
    pub fn encode(kind: SequenceKind) -> Bytes {
        let tag = RecordTag::new(RecordType::Sequence as u8, kind as u8);
        KeyPrefix::new(KEY_VERSION, tag).to_bytes()
    }
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Creates a `BytesRange` covering all keys of a given record type.
fn record_type_range(record_type: RecordType) -> BytesRange {
    let tag_start = RecordTag::new(record_type as u8, 0);
    let start = KeyPrefix::new(KEY_VERSION, tag_start).to_bytes();

    // End: next record type (or one past last reserved bit)
    let rt = record_type as u8;
    if rt < 15 {
        let tag_end = RecordTag::new(rt + 1, 0);
        let end = KeyPrefix::new(KEY_VERSION, tag_end).to_bytes();
        BytesRange::new(Bound::Included(start), Bound::Excluded(end))
    } else {
        // Record type 15 is the last; end after 0xFF
        let end = Bytes::from_static(&[KEY_VERSION + 1]);
        BytesRange::new(Bound::Included(start), Bound::Excluded(end))
    }
}

fn catalog_kind_from_reserved(reserved: u8) -> Result<CatalogKind, DeserializeError> {
    match reserved {
        0 => Ok(CatalogKind::LabelById),
        1 => Ok(CatalogKind::LabelByName),
        2 => Ok(CatalogKind::EdgeTypeById),
        3 => Ok(CatalogKind::EdgeTypeByName),
        4 => Ok(CatalogKind::PropertyKeyById),
        5 => Ok(CatalogKind::PropertyKeyByName),
        other => Err(DeserializeError {
            message: format!("unknown catalog kind: {other}"),
        }),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_roundtrip_node_record_key() {
        // given
        let key = NodeRecordKey { node_id: 42, epoch: 7 };

        // when
        let encoded = key.encode();
        let decoded = NodeRecordKey::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, key);
        assert_eq!(encoded.len(), NodeRecordKey::SIZE);
    }

    #[test]
    fn should_roundtrip_edge_record_key() {
        // given
        let key = EdgeRecordKey { edge_id: 100, epoch: 3 };

        // when
        let encoded = key.encode();
        let decoded = EdgeRecordKey::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, key);
        assert_eq!(encoded.len(), EdgeRecordKey::SIZE);
    }

    #[test]
    fn should_roundtrip_node_property_key() {
        // given
        let key = NodePropertyKey {
            node_id: 42,
            prop_key: Bytes::from("name"),
        };

        // when
        let encoded = key.encode();
        let decoded = NodePropertyKey::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, key);
    }

    #[test]
    fn should_roundtrip_edge_property_key() {
        // given
        let key = EdgePropertyKey {
            edge_id: 99,
            prop_key: Bytes::from("weight"),
        };

        // when
        let encoded = key.encode();
        let decoded = EdgePropertyKey::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, key);
    }

    #[test]
    fn should_roundtrip_forward_adj_key() {
        // given
        let key = ForwardAdjKey {
            src: 1,
            edge_type_id: 5,
            dst: 2,
        };

        // when
        let encoded = key.encode();
        let decoded = ForwardAdjKey::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, key);
        assert_eq!(encoded.len(), ForwardAdjKey::SIZE);
    }

    #[test]
    fn should_roundtrip_backward_adj_key() {
        // given
        let key = BackwardAdjKey {
            dst: 2,
            edge_type_id: 5,
            src: 1,
        };

        // when
        let encoded = key.encode();
        let decoded = BackwardAdjKey::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, key);
        assert_eq!(encoded.len(), BackwardAdjKey::SIZE);
    }

    #[test]
    fn should_roundtrip_label_index_key() {
        // given
        let key = LabelIndexKey { label_id: 3, node_id: 42 };

        // when
        let encoded = key.encode();
        let decoded = LabelIndexKey::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, key);
        assert_eq!(encoded.len(), LabelIndexKey::SIZE);
    }

    #[test]
    fn should_roundtrip_catalog_by_id_key() {
        // given
        let key = CatalogByIdKey {
            kind: CatalogKind::LabelById,
            id: 42,
        };

        // when
        let encoded = key.encode();
        let decoded = CatalogByIdKey::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, key);
        assert_eq!(encoded.len(), CatalogByIdKey::SIZE);
    }

    #[test]
    fn should_roundtrip_catalog_by_name_key() {
        // given
        let key = CatalogByNameKey {
            kind: CatalogKind::EdgeTypeByName,
            name: Bytes::from("KNOWS"),
        };

        // when
        let encoded = key.encode();
        let decoded = CatalogByNameKey::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, key);
    }

    #[test]
    fn should_roundtrip_metadata_key() {
        // given
        let key = MetadataKey { sub_type: MetadataSubType::NodeCount };

        // when
        let encoded = key.encode();
        let decoded = MetadataKey::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, key);
        assert_eq!(encoded.len(), MetadataKey::SIZE);
    }

    #[test]
    fn should_order_node_records_by_id_then_epoch() {
        // given
        let k1 = NodeRecordKey { node_id: 1, epoch: 5 }.encode();
        let k2 = NodeRecordKey { node_id: 1, epoch: 10 }.encode();
        let k3 = NodeRecordKey { node_id: 2, epoch: 1 }.encode();

        // then: lexicographic order should be k1 < k2 < k3
        assert!(k1 < k2, "same node, earlier epoch should sort first");
        assert!(k2 < k3, "smaller node_id should sort before larger");
    }

    #[test]
    fn should_order_forward_adj_by_src_type_dst() {
        // given
        let k1 = ForwardAdjKey { src: 1, edge_type_id: 1, dst: 10 }.encode();
        let k2 = ForwardAdjKey { src: 1, edge_type_id: 1, dst: 20 }.encode();
        let k3 = ForwardAdjKey { src: 1, edge_type_id: 2, dst: 5 }.encode();
        let k4 = ForwardAdjKey { src: 2, edge_type_id: 1, dst: 1 }.encode();

        // then
        assert!(k1 < k2, "same src+type, dst 10 < dst 20");
        assert!(k2 < k3, "same src, type 1 < type 2");
        assert!(k3 < k4, "src 1 < src 2");
    }

    #[test]
    fn should_order_label_index_by_label_then_node() {
        // given
        let k1 = LabelIndexKey { label_id: 1, node_id: 100 }.encode();
        let k2 = LabelIndexKey { label_id: 1, node_id: 200 }.encode();
        let k3 = LabelIndexKey { label_id: 2, node_id: 50 }.encode();

        // then
        assert!(k1 < k2, "same label, node 100 < node 200");
        assert!(k2 < k3, "label 1 < label 2");
    }

    #[test]
    fn should_separate_record_types_lexicographically() {
        // given: one key from each record type
        let node = NodeRecordKey { node_id: 0, epoch: 0 }.encode();
        let edge = EdgeRecordKey { edge_id: 0, epoch: 0 }.encode();
        let nprop = NodePropertyKey { node_id: 0, prop_key: Bytes::from("a") }.encode();
        let eprop = EdgePropertyKey { edge_id: 0, prop_key: Bytes::from("a") }.encode();
        let fwd = ForwardAdjKey { src: 0, edge_type_id: 0, dst: 0 }.encode();
        let bwd = BackwardAdjKey { dst: 0, edge_type_id: 0, src: 0 }.encode();
        let label = LabelIndexKey { label_id: 0, node_id: 0 }.encode();
        let meta = MetadataKey { sub_type: MetadataSubType::NodeCount }.encode();

        // then: record types sort in order of their tag byte
        assert!(node < edge);
        assert!(edge < nprop);
        assert!(nprop < eprop);
        assert!(eprop < fwd);
        assert!(fwd < bwd);
        assert!(bwd < label);
        assert!(label < meta);
    }

    #[test]
    fn should_node_prefix_contain_all_epochs() {
        // given
        let prefix_range = NodeRecordKey::node_prefix(42);
        let k1 = NodeRecordKey { node_id: 42, epoch: 0 }.encode();
        let k2 = NodeRecordKey { node_id: 42, epoch: u64::MAX }.encode();
        let k3 = NodeRecordKey { node_id: 43, epoch: 0 }.encode();

        // then
        assert!(prefix_range.contains(&k1), "epoch 0 should be in prefix range");
        assert!(prefix_range.contains(&k2), "max epoch should be in prefix range");
        assert!(!prefix_range.contains(&k3), "different node should not be in prefix range");
    }

    #[test]
    fn should_forward_adj_src_prefix_contain_all_types_and_dsts() {
        // given
        let prefix_range = ForwardAdjKey::src_prefix(10);
        let k1 = ForwardAdjKey { src: 10, edge_type_id: 1, dst: 20 }.encode();
        let k2 = ForwardAdjKey { src: 10, edge_type_id: 99, dst: 999 }.encode();
        let k3 = ForwardAdjKey { src: 11, edge_type_id: 1, dst: 1 }.encode();

        // then
        assert!(prefix_range.contains(&k1));
        assert!(prefix_range.contains(&k2));
        assert!(!prefix_range.contains(&k3));
    }

    #[test]
    fn should_label_prefix_contain_all_nodes() {
        // given
        let prefix_range = LabelIndexKey::label_prefix(5);
        let k1 = LabelIndexKey { label_id: 5, node_id: 1 }.encode();
        let k2 = LabelIndexKey { label_id: 5, node_id: u64::MAX }.encode();
        let k3 = LabelIndexKey { label_id: 6, node_id: 1 }.encode();

        // then
        assert!(prefix_range.contains(&k1));
        assert!(prefix_range.contains(&k2));
        assert!(!prefix_range.contains(&k3));
    }

    #[test]
    fn should_property_key_handle_special_chars() {
        // given: property key containing bytes that need escaping
        let key = NodePropertyKey {
            node_id: 1,
            prop_key: Bytes::from_static(&[0x00, 0x01, 0xFF]),
        };

        // when
        let encoded = key.encode();
        let decoded = NodePropertyKey::decode(&encoded).unwrap();

        // then
        assert_eq!(decoded, key);
    }
}
