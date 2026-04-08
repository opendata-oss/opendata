use crate::Result;
use crate::model::{AttributeValue, MetadataFieldSpec, VECTOR_FIELD_NAME};
use crate::serde::FieldType;
use crate::serde::centroid_info::CentroidInfoValue;
use crate::serde::centroids::CentroidsValue;
use crate::serde::key::{CentroidSeqBlockKey, SeqBlockKey};
use crate::serde::vector_id::{ROOT_VECTOR_ID, VectorId};
use crate::storage::merge_operator::VectorDbMergeOperator;
use crate::write::delta::VectorWrite;
use crate::write::indexer::tree::centroids::AllCentroidsCacheWriter;
use crate::write::indexer::tree::posting_list::PostingList;
use crate::write::indexer::tree::state::{VectorIndexDelta, VectorIndexState};
use crate::write::indexer::tree::{IndexerOpts, validator, vector::WriteVectors};
use common::storage::in_memory::InMemoryStorage;
use common::{SequenceAllocator, Storage, StorageRead};
use std::collections::HashMap;
use std::sync::Arc;

const SNAPSHOT_EPOCH: u64 = 1;
const NUMBER_MASK: u64 = 0x00FF_FFFF_FFFF_FFFF;

enum IndexerOpTestNodePostings {
    Leaf(Vec<IndexerOpTestLeafVector>),
    Inner(Vec<VectorId>),
}

struct IndexerOpTestLeafVector {
    external_id: String,
    vector: Vec<f32>,
    field_values: Vec<String>,
}

pub struct TestDataVector {
    external_id: String,
    vector: Vec<f32>,
    field_values: Vec<String>,
}

impl TestDataVector {
    pub fn new(external_id: &str, vector: Vec<f32>, field_values: Vec<&str>) -> Self {
        Self {
            external_id: external_id.to_string(),
            vector,
            field_values: field_values
                .into_iter()
                .map(|value| value.to_string())
                .collect(),
        }
    }
}

pub struct IndexerOpTestHarnessBuilder {
    vector_schema: Vec<MetadataFieldSpec>,
    vectors: HashMap<VectorId, Vec<f32>>,
    postings: HashMap<VectorId, IndexerOpTestNodePostings>,
}

impl IndexerOpTestHarnessBuilder {
    pub fn new(vector_schema: Vec<MetadataFieldSpec>) -> Self {
        for f in &vector_schema {
            if f.name == VECTOR_FIELD_NAME {
                continue;
            }
            // just support string fields for now
            assert_eq!(f.field_type, FieldType::String);
        }
        Self {
            vector_schema,
            vectors: HashMap::new(),
            postings: HashMap::new(),
        }
    }

    pub fn with_leaf_centroid(
        mut self,
        centroid_id: VectorId,
        vector: Vec<f32>,
        data_vectors: Vec<TestDataVector>,
    ) -> Self {
        assert_eq!(centroid_id.level(), 1);
        let data_vectors = data_vectors
            .into_iter()
            .map(|data_vector| {
                assert_eq!(data_vector.field_values.len(), self.vector_schema.len());
                let leaf_vector = IndexerOpTestLeafVector {
                    external_id: data_vector.external_id,
                    vector: data_vector.vector,
                    field_values: data_vector.field_values,
                };
                self.assert_leaf_vector_conforms_to_schema(&leaf_vector);
                leaf_vector
            })
            .collect();
        self.vectors.insert(centroid_id, vector);
        self.postings
            .insert(centroid_id, IndexerOpTestNodePostings::Leaf(data_vectors));
        self
    }

    pub fn with_centroid(
        mut self,
        centroid_id: VectorId,
        vector: Vec<f32>,
        posting_centroid_ids: Vec<VectorId>,
    ) -> Self {
        assert!(centroid_id.level() > 1);
        self.vectors.insert(centroid_id, vector);
        self.postings.insert(
            centroid_id,
            IndexerOpTestNodePostings::Inner(posting_centroid_ids),
        );
        self
    }

    pub async fn build(self, dimensions: usize) -> IndexerOpTestHarness {
        let max_level = self.vectors.keys().map(|id| id.level()).max().unwrap_or(1);
        let depth = max_level + 2;

        let mut parent_by_child = HashMap::new();
        for (parent_id, postings) in &self.postings {
            if let IndexerOpTestNodePostings::Inner(child_ids) = postings {
                for child_id in child_ids {
                    parent_by_child.insert(*child_id, *parent_id);
                }
            }
        }

        let storage: Arc<dyn Storage> = Arc::new(InMemoryStorage::with_merge_operator(Arc::new(
            VectorDbMergeOperator::new(dimensions),
        )));

        let seq_key = SeqBlockKey.encode();
        let vector_allocator = SequenceAllocator::load(storage.as_ref(), seq_key)
            .await
            .unwrap();
        let centroid_seq_key = CentroidSeqBlockKey.encode();
        let mut centroid_allocator = SequenceAllocator::load(storage.as_ref(), centroid_seq_key)
            .await
            .unwrap();
        let max_centroid_num = self
            .vectors
            .keys()
            .map(|centroid_id| centroid_id.id() & NUMBER_MASK)
            .max()
            .unwrap_or(0);
        assert!(max_centroid_num > 0);
        let _ = centroid_allocator.allocate(max_centroid_num + 1);
        let (sequence_block_key, sequence_block) = vector_allocator.freeze();
        let (centroid_sequence_block_key, centroid_sequence_block) = centroid_allocator.freeze();
        let centroid_cache =
            AllCentroidsCacheWriter::new(Arc::new(PostingList::with_capacity(0)), vec![]);
        let mut state = VectorIndexState::new(
            HashMap::new(),
            CentroidsValue::new(depth),
            0,
            HashMap::new(),
            HashMap::new(),
            sequence_block_key,
            sequence_block,
            centroid_sequence_block_key,
            centroid_sequence_block,
            centroid_cache,
        );

        let mut delta = VectorIndexDelta::new(&state);

        let mut centroid_ids: Vec<_> = self.vectors.keys().copied().collect();
        centroid_ids.sort();
        for centroid_id in &centroid_ids {
            let vector = self
                .vectors
                .get(centroid_id)
                .unwrap_or_else(|| panic!("missing centroid vector for {}", centroid_id));
            let parent = parent_by_child
                .get(centroid_id)
                .copied()
                .unwrap_or(ROOT_VECTOR_ID);
            delta.search_index.add_test_centroid(
                *centroid_id,
                CentroidInfoValue::new(centroid_id.level(), vector.clone(), parent),
            );
        }

        for centroid_id in &centroid_ids {
            let postings = self
                .postings
                .get(centroid_id)
                .unwrap_or_else(|| panic!("missing postings for centroid {}", centroid_id));
            match postings {
                IndexerOpTestNodePostings::Leaf(data_vectors) => {
                    for data_vector in data_vectors {
                        let vector_id = delta.forward_index.add_vector(
                            &data_vector.external_id,
                            &self.make_attributes(&data_vector.vector, &data_vector.field_values),
                        );
                        delta.search_index.add_to_posting(
                            *centroid_id,
                            vector_id,
                            data_vector.vector.clone(),
                        );
                    }
                }
                IndexerOpTestNodePostings::Inner(child_ids) => {
                    for child_id in child_ids {
                        let vector = self
                            .vectors
                            .get(child_id)
                            .unwrap_or_else(|| {
                                panic!("missing child centroid vector for {}", child_id)
                            })
                            .clone();
                        delta
                            .search_index
                            .add_to_posting(*centroid_id, *child_id, vector);
                    }
                }
            }
        }

        for centroid_id in &centroid_ids {
            if !parent_by_child.contains_key(centroid_id) {
                let vector = self
                    .vectors
                    .get(centroid_id)
                    .unwrap_or_else(|| panic!("missing root centroid vector for {}", centroid_id))
                    .clone();
                delta.search_index.add_to_root(*centroid_id, vector);
            }
        }

        let ops = delta.freeze(SNAPSHOT_EPOCH, &mut state);
        storage.apply(ops).await.unwrap();

        let harness = IndexerOpTestHarness {
            storage,
            state,
            dimensions,
            vector_schema: self.vector_schema,
        };
        harness.validate().await;
        harness
    }

    fn make_attributes(
        &self,
        vector: &[f32],
        field_values: &[String],
    ) -> Vec<(String, AttributeValue)> {
        let mut attributes = Vec::with_capacity(1 + self.vector_schema.len());
        attributes.push((
            "vector".to_string(),
            AttributeValue::Vector(vector.to_vec()),
        ));
        for (field_spec, field_value) in self.vector_schema.iter().zip(field_values) {
            attributes.push((
                field_spec.name.clone(),
                AttributeValue::String(field_value.clone()),
            ));
        }
        assert_attributes_conform_to_schema(&self.vector_schema, &attributes);
        attributes
    }

    fn assert_leaf_vector_conforms_to_schema(&self, leaf_vector: &IndexerOpTestLeafVector) {
        let attributes = self.make_attributes(&leaf_vector.vector, &leaf_vector.field_values);
        assert_attributes_conform_to_schema(&self.vector_schema, &attributes);
    }
}

pub struct IndexerOpTestHarness {
    pub storage: Arc<dyn Storage>,
    pub state: VectorIndexState,
    dimensions: usize,
    vector_schema: Vec<MetadataFieldSpec>,
}

impl IndexerOpTestHarness {
    pub async fn snapshot(&self) -> Arc<dyn StorageRead> {
        self.storage.snapshot().await.unwrap()
    }

    pub async fn apply_delta(&mut self, delta: VectorIndexDelta) {
        let ops = delta.freeze(SNAPSHOT_EPOCH, &mut self.state);
        self.storage.apply(ops).await.unwrap();
        self.validate().await;
    }

    pub async fn write_and_apply(&mut self, opts: &Arc<IndexerOpts>, writes: Vec<VectorWrite>) {
        for write in &writes {
            assert_attributes_conform_to_schema(&self.vector_schema, &write.attributes);
        }
        let snapshot = self.snapshot().await;
        let mut delta = VectorIndexDelta::new(&self.state);
        let wv = WriteVectors::new(opts, &snapshot, SNAPSHOT_EPOCH, writes);
        wv.execute(&self.state, &mut delta).await.unwrap();
        self.apply_delta(delta).await;
    }

    pub async fn validate(&self) {
        let snapshot = self.storage.snapshot().await.unwrap();
        validator::validate(snapshot, &self.state, self.dimensions)
            .await
            .unwrap();
    }
}

fn assert_attributes_conform_to_schema(
    vector_schema: &[MetadataFieldSpec],
    attributes: &[(String, AttributeValue)],
) {
    assert_eq!(attributes.len(), vector_schema.len() + 1);
    let vector_attr = attributes.iter().find(|(name, _)| name == "vector");
    assert!(matches!(vector_attr, Some((_, AttributeValue::Vector(_)))));
    for spec in vector_schema {
        assert_eq!(spec.field_type, FieldType::String);
        let value = attributes
            .iter()
            .find(|(name, _)| name == &spec.name)
            .unwrap_or_else(|| panic!("missing attribute for schema field {}", spec.name));
        assert!(matches!(value.1, AttributeValue::String(_)));
    }
    for (name, value) in attributes {
        if name == "vector" {
            assert!(matches!(value, AttributeValue::Vector(_)));
            continue;
        }
        let spec = vector_schema
            .iter()
            .find(|spec| spec.name == *name)
            .unwrap_or_else(|| panic!("unexpected attribute {} for test schema", name));
        assert_eq!(spec.field_type, FieldType::String);
        assert!(matches!(value, AttributeValue::String(_)));
    }
}

#[allow(dead_code)]
pub async fn validate_harness(harness: &IndexerOpTestHarness) -> Result<()> {
    let snapshot = harness.storage.snapshot().await.unwrap();
    validator::validate(snapshot, &harness.state, harness.dimensions).await
}
