//! Parquet dataset support for the recall benchmark.
//!
//! Lets the bench read base/query vectors and ground-truth neighbour lists
//! directly from Parquet files, so users can point it at their own
//! embeddings without first converting to `fvecs`/`bvecs`.
//!
//! Supported column shapes:
//! - **Embeddings** (`vector_column`): `FixedSizeList<Float32>`,
//!   `List<Float32>`, or `List<Float64>` (cast to `f32`).
//! - **Ground-truth neighbours** (`ground_truth_column`):
//!   `List<Int32>` or `List<Int64>`.
//! - **IDs** (`id_column`, optional): `Int32` or `Int64`. When present, the
//!   ground-truth neighbour values are treated as dataset IDs and remapped
//!   to the row index each vector is ingested under. When absent, the
//!   neighbour values are assumed to already be 0-based row indices.
//! - **Metadata** (`metadata_columns`): `Utf8`/`LargeUtf8` → String,
//!   integer types → Int64, float types → Float64, `Boolean` → Bool.
//!   Ingested as attributes on each vector; null values are skipped.

use std::collections::HashMap;
use std::fs::File;
use std::path::Path;

use anyhow::{Context, bail};
use arrow::array::cast::AsArray;
use arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    LargeStringArray, ListArray, StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use vector::{Attribute, AttributeValue, FieldType};

use crate::recall::{BaseRow, VectorBatchReader, normalize_vec};

/// Number of rows the underlying Parquet reader yields per arrow batch.
/// Each call to [`ParquetVectorBatchReader::read_batch`] returns one such
/// arrow batch, so this controls the ingest chunk granularity.
const PARQUET_BATCH_ROWS: usize = 8192;

/// Streaming reader over an embedding column (and optional metadata columns)
/// of a Parquet file. Mirrors the `read_batch` contract of the fvecs/bvecs
/// reader so the ingest stream can treat all formats uniformly via
/// [`VectorBatchReader`].
pub(crate) struct ParquetVectorBatchReader {
    reader: ParquetRecordBatchReader,
    column: String,
    dimensions: usize,
    /// Metadata columns to read as attributes, with their resolved
    /// `FieldType`. Empty for query reads / fvecs-style ingests.
    metadata: Vec<(String, FieldType)>,
    remaining: Option<usize>,
    normalize: bool,
}

impl ParquetVectorBatchReader {
    pub(crate) fn open(
        path: &Path,
        column: &str,
        dimensions: usize,
        metadata: Vec<(String, FieldType)>,
        max_vectors: Option<usize>,
        normalize: bool,
    ) -> anyhow::Result<Self> {
        let file = File::open(path)
            .with_context(|| format!("failed to open parquet file {}", path.display()))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .with_context(|| format!("failed to read parquet metadata for {}", path.display()))?;
        let reader = builder
            .with_batch_size(PARQUET_BATCH_ROWS)
            .build()
            .with_context(|| format!("failed to build parquet reader for {}", path.display()))?;
        Ok(Self {
            reader,
            column: column.to_string(),
            dimensions,
            metadata,
            remaining: max_vectors,
            normalize,
        })
    }
}

impl VectorBatchReader for ParquetVectorBatchReader {
    fn read_batch(&mut self, _max_rows: usize) -> anyhow::Result<Option<Vec<BaseRow>>> {
        if self.remaining == Some(0) {
            return Ok(None);
        }
        let Some(batch) = self.reader.next() else {
            return Ok(None);
        };
        let batch = batch.context("error reading parquet record batch")?;
        let mut embeddings = extract_vectors(&batch, &self.column, self.dimensions)?;

        // Read each metadata column as a per-row attribute (column-major),
        // then transpose into per-row attribute lists below.
        let mut metadata_cols: Vec<(String, Vec<Option<AttributeValue>>)> =
            Vec::with_capacity(self.metadata.len());
        for (name, field_type) in &self.metadata {
            metadata_cols.push((
                name.clone(),
                extract_attribute_column(&batch, name, *field_type)?,
            ));
        }

        let mut take = embeddings.len();
        if let Some(remaining) = self.remaining {
            take = take.min(remaining);
        }
        embeddings.truncate(take);

        let mut rows = Vec::with_capacity(embeddings.len());
        for (i, mut embedding) in embeddings.into_iter().enumerate() {
            if self.normalize {
                normalize_vec(&mut embedding);
            }
            let mut attributes = Vec::with_capacity(metadata_cols.len());
            for (name, values) in &metadata_cols {
                if let Some(Some(value)) = values.get(i) {
                    attributes.push(Attribute::new(name.clone(), value.clone()));
                }
            }
            rows.push(BaseRow {
                embedding,
                attributes,
            });
        }

        if let Some(remaining) = &mut self.remaining {
            *remaining -= rows.len();
        }
        if rows.is_empty() {
            Ok(None)
        } else {
            Ok(Some(rows))
        }
    }
}

/// Number of rows in a Parquet file, read cheaply from its footer metadata.
pub(crate) fn row_count(path: &Path) -> anyhow::Result<usize> {
    let file = File::open(path)
        .with_context(|| format!("failed to open parquet file {}", path.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| format!("failed to read parquet metadata for {}", path.display()))?;
    Ok(builder.metadata().file_metadata().num_rows() as usize)
}

/// Read up to `limit` query vectors from the `column` of a Parquet file.
pub(crate) fn read_query_vectors(
    path: &Path,
    column: &str,
    dimensions: usize,
    limit: usize,
) -> anyhow::Result<Vec<Vec<f32>>> {
    let mut reader =
        ParquetVectorBatchReader::open(path, column, dimensions, Vec::new(), Some(limit), false)?;
    let mut out = Vec::with_capacity(limit);
    while out.len() < limit {
        match reader.read_batch(PARQUET_BATCH_ROWS)? {
            Some(batch) => out.extend(batch.into_iter().map(|r| r.embedding)),
            None => break,
        }
    }
    out.truncate(limit);
    Ok(out)
}

/// Read the named columns from up to `limit` rows of a Parquet file as
/// per-row attribute values, keyed by column name. Used to resolve query
/// filter values. Column types are inferred from the schema. A `None` entry
/// means the value was null for that row.
pub(crate) fn read_attribute_columns(
    path: &Path,
    columns: &[String],
    limit: usize,
) -> anyhow::Result<HashMap<String, Vec<Option<AttributeValue>>>> {
    let types = read_field_types(path, columns)?;
    let file = File::open(path)
        .with_context(|| format!("failed to open parquet file {}", path.display()))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| format!("failed to read parquet metadata for {}", path.display()))?
        .with_batch_size(PARQUET_BATCH_ROWS)
        .build()
        .with_context(|| format!("failed to build parquet reader for {}", path.display()))?;

    let mut out: HashMap<String, Vec<Option<AttributeValue>>> =
        columns.iter().map(|c| (c.clone(), Vec::new())).collect();
    let mut rows_read = 0usize;
    for batch in reader {
        if rows_read >= limit {
            break;
        }
        let batch = batch.context("error reading parquet query-column batch")?;
        for (name, field_type) in &types {
            let mut values = extract_attribute_column(&batch, name, *field_type)?;
            let remaining = limit - rows_read;
            if values.len() > remaining {
                values.truncate(remaining);
            }
            out.get_mut(name)
                .expect("column key present")
                .extend(values);
        }
        rows_read += batch.num_rows().min(limit - rows_read);
    }
    Ok(out)
}

/// Infer the [`FieldType`] of each requested column from a Parquet file's
/// schema. Returns `(column, field_type)` pairs in the order requested.
pub(crate) fn read_field_types(
    path: &Path,
    columns: &[String],
) -> anyhow::Result<Vec<(String, FieldType)>> {
    let file = File::open(path)
        .with_context(|| format!("failed to open parquet file {}", path.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| format!("failed to read parquet metadata for {}", path.display()))?;
    let schema = builder.schema();
    let mut out = Vec::with_capacity(columns.len());
    for name in columns {
        let field = schema.field_with_name(name).with_context(|| {
            format!("metadata column '{}' not found in {}", name, path.display())
        })?;
        let field_type = match field.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 => FieldType::String,
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => FieldType::Int64,
            DataType::Float16 | DataType::Float32 | DataType::Float64 => FieldType::Float64,
            DataType::Boolean => FieldType::Bool,
            other => bail!(
                "metadata column '{}' has unsupported type {:?}; expected \
                 string, integer, float, or boolean",
                name,
                other
            ),
        };
        out.push((name.clone(), field_type));
    }
    Ok(out)
}

/// Extract a single metadata column as one `Option<AttributeValue>` per row
/// (null → `None`), coercing to `field_type`.
fn extract_attribute_column(
    batch: &RecordBatch,
    name: &str,
    field_type: FieldType,
) -> anyhow::Result<Vec<Option<AttributeValue>>> {
    let col = column_by_name(batch, name)?;
    let n = batch.num_rows();
    let mut out = Vec::with_capacity(n);

    macro_rules! collect {
        ($arr:expr, $map:expr) => {{
            let a = $arr;
            for i in 0..n {
                out.push(if a.is_null(i) {
                    None
                } else {
                    Some($map(a.value(i)))
                });
            }
        }};
    }

    match field_type {
        FieldType::String => {
            if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
                collect!(a, |v: &str| AttributeValue::String(v.to_string()));
            } else if let Some(a) = col.as_any().downcast_ref::<LargeStringArray>() {
                collect!(a, |v: &str| AttributeValue::String(v.to_string()));
            } else {
                bail!("metadata column '{}' is not a Utf8/LargeUtf8 array", name);
            }
        }
        FieldType::Int64 => {
            if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
                collect!(a, |v: i64| AttributeValue::Int64(v));
            } else if let Some(a) = col.as_any().downcast_ref::<Int32Array>() {
                collect!(a, |v: i32| AttributeValue::Int64(v as i64));
            } else if let Some(a) = col.as_any().downcast_ref::<Int16Array>() {
                collect!(a, |v: i16| AttributeValue::Int64(v as i64));
            } else if let Some(a) = col.as_any().downcast_ref::<Int8Array>() {
                collect!(a, |v: i8| AttributeValue::Int64(v as i64));
            } else if let Some(a) = col.as_any().downcast_ref::<UInt64Array>() {
                collect!(a, |v: u64| AttributeValue::Int64(v as i64));
            } else if let Some(a) = col.as_any().downcast_ref::<UInt32Array>() {
                collect!(a, |v: u32| AttributeValue::Int64(v as i64));
            } else if let Some(a) = col.as_any().downcast_ref::<UInt16Array>() {
                collect!(a, |v: u16| AttributeValue::Int64(v as i64));
            } else if let Some(a) = col.as_any().downcast_ref::<UInt8Array>() {
                collect!(a, |v: u8| AttributeValue::Int64(v as i64));
            } else {
                bail!("metadata column '{}' is not an integer array", name);
            }
        }
        FieldType::Float64 => {
            if let Some(a) = col.as_any().downcast_ref::<Float64Array>() {
                collect!(a, |v: f64| AttributeValue::Float64(v));
            } else if let Some(a) = col.as_any().downcast_ref::<Float32Array>() {
                collect!(a, |v: f32| AttributeValue::Float64(v as f64));
            } else {
                bail!("metadata column '{}' is not an Float32/Float64 array", name);
            }
        }
        FieldType::Bool => {
            let a = col
                .as_any()
                .downcast_ref::<BooleanArray>()
                .with_context(|| format!("metadata column '{}' is not a Boolean array", name))?;
            collect!(a, |v: bool| AttributeValue::Bool(v));
        }
        FieldType::Vector => bail!("metadata column '{}' cannot be a Vector field", name),
    }
    Ok(out)
}

/// Read ground-truth neighbour lists from the `column` of a Parquet file.
/// When `id_map` is provided, each neighbour value is looked up as a dataset
/// ID and replaced with the row index it maps to; otherwise neighbour values
/// are taken to already be 0-based row indices.
pub(crate) fn read_ground_truth(
    path: &Path,
    column: &str,
    id_map: Option<&HashMap<i64, i32>>,
) -> anyhow::Result<Vec<Vec<i32>>> {
    let file = File::open(path)
        .with_context(|| format!("failed to open parquet file {}", path.display()))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| format!("failed to read parquet metadata for {}", path.display()))?
        .with_batch_size(PARQUET_BATCH_ROWS)
        .build()
        .with_context(|| format!("failed to build parquet reader for {}", path.display()))?;

    let mut out = Vec::new();
    for batch in reader {
        let batch = batch.context("error reading parquet ground-truth batch")?;
        let col = column_by_name(&batch, column)?;
        let list = col
            .as_any()
            .downcast_ref::<ListArray>()
            .with_context(|| format!("ground-truth column '{}' is not a List", column))?;
        for i in 0..list.len() {
            let values = list.value(i);
            let ids = int_values(&values)
                .with_context(|| format!("ground-truth column '{}' is not List<Int*>", column))?;
            let row = match id_map {
                Some(map) => ids
                    .iter()
                    .map(|id| {
                        map.get(id).copied().with_context(|| {
                            format!("ground-truth id {} not present in id_column", id)
                        })
                    })
                    .collect::<anyhow::Result<Vec<i32>>>()?,
                None => ids.iter().map(|id| *id as i32).collect(),
            };
            out.push(row);
        }
    }
    Ok(out)
}

/// Build a `dataset id -> row index` map from the `id_column` of the base
/// Parquet file. Row index is the position the vector is ingested under,
/// which is the external ID the bench keys results by.
pub(crate) fn read_id_index_map(
    base_path: &Path,
    id_column: &str,
) -> anyhow::Result<HashMap<i64, i32>> {
    let file = File::open(base_path)
        .with_context(|| format!("failed to open parquet file {}", base_path.display()))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| {
            format!(
                "failed to read parquet metadata for {}",
                base_path.display()
            )
        })?
        .with_batch_size(PARQUET_BATCH_ROWS)
        .build()
        .with_context(|| format!("failed to build parquet reader for {}", base_path.display()))?;

    let mut map = HashMap::new();
    let mut row_index: i32 = 0;
    for batch in reader {
        let batch = batch.context("error reading parquet id batch")?;
        let col = column_by_name(&batch, id_column)?;
        let ids = int_values(col)
            .with_context(|| format!("id_column '{}' is not an integer column", id_column))?;
        for id in ids {
            map.insert(id, row_index);
            row_index += 1;
        }
    }
    Ok(map)
}

fn column_by_name<'a>(
    batch: &'a RecordBatch,
    column: &str,
) -> anyhow::Result<&'a arrow::array::ArrayRef> {
    batch
        .column_by_name(column)
        .with_context(|| format!("parquet file has no column named '{}'", column))
}

/// Extract every row of `column` from `batch` as a `Vec<f32>`, validating
/// that each vector has exactly `dimensions` elements.
fn extract_vectors(
    batch: &RecordBatch,
    column: &str,
    dimensions: usize,
) -> anyhow::Result<Vec<Vec<f32>>> {
    let col = column_by_name(batch, column)?;
    let rows = batch.num_rows();
    let mut out = Vec::with_capacity(rows);

    match col.data_type() {
        DataType::FixedSizeList(_, len) => {
            let value_len = *len as usize;
            if value_len != dimensions {
                bail!(
                    "vector column '{}' has fixed size {} but dataset dimensions = {}",
                    column,
                    value_len,
                    dimensions
                );
            }
            let fsl = col.as_fixed_size_list();
            for i in 0..fsl.len() {
                let row = fsl.value(i);
                out.push(floats_to_f32(&row, dimensions, column)?);
            }
        }
        DataType::List(_) => {
            let list = col
                .as_any()
                .downcast_ref::<ListArray>()
                .with_context(|| format!("vector column '{}' is not a List", column))?;
            for i in 0..list.len() {
                let row = list.value(i);
                out.push(floats_to_f32(&row, dimensions, column)?);
            }
        }
        other => bail!(
            "vector column '{}' has unsupported type {:?}; expected \
             FixedSizeList<Float32>, List<Float32>, or List<Float64>",
            column,
            other
        ),
    }
    Ok(out)
}

/// Convert a single embedding's child array (`Float32` or `Float64`) into a
/// `Vec<f32>`, validating the length.
fn floats_to_f32(
    values: &arrow::array::ArrayRef,
    dimensions: usize,
    column: &str,
) -> anyhow::Result<Vec<f32>> {
    let v: Vec<f32> = if let Some(a) = values.as_any().downcast_ref::<Float32Array>() {
        a.values().to_vec()
    } else if let Some(a) = values.as_any().downcast_ref::<Float64Array>() {
        a.values().iter().map(|x| *x as f32).collect()
    } else {
        bail!(
            "vector column '{}' element type is {:?}; expected Float32 or Float64",
            column,
            values.data_type()
        );
    };
    if v.len() != dimensions {
        bail!(
            "vector column '{}' has a row of length {} but dataset dimensions = {}",
            column,
            v.len(),
            dimensions
        );
    }
    Ok(v)
}

/// Extract an integer array (`Int32` or `Int64`) as `Vec<i64>`.
fn int_values(values: &arrow::array::ArrayRef) -> anyhow::Result<Vec<i64>> {
    if let Some(a) = values.as_any().downcast_ref::<Int64Array>() {
        Ok(a.values().to_vec())
    } else if let Some(a) = values.as_any().downcast_ref::<Int32Array>() {
        Ok(a.values().iter().map(|x| *x as i64).collect())
    } else {
        bail!("expected Int32 or Int64, got {:?}", values.data_type())
    }
}
