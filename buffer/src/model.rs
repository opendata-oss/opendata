use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

const ENTRY_LEN_SIZE: usize = 4;
const FORMAT_VERSION: u16 = 1;
const COMPRESSION_TYPE_SIZE: usize = 1;
const ENTRIES_COUNT_SIZE: usize = 4;
const VERSION_SIZE: usize = 2;
const FOOTER_SIZE: usize = COMPRESSION_TYPE_SIZE + ENTRIES_COUNT_SIZE + VERSION_SIZE;

/// The default ZSTD compression level used when `CompressionType::Zstd` is selected.
const ZSTD_LEVEL: i32 = 3;

/// Compression algorithm applied to the record block of a data batch.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CompressionType {
    #[default]
    None = 0,
    Zstd = 1,
}

impl TryFrom<u8> for CompressionType {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(CompressionType::None),
            1 => Ok(CompressionType::Zstd),
            other => Err(Error::Serialization(format!(
                "unsupported compression type: {other}"
            ))),
        }
    }
}

pub(crate) fn encode_batch(entries: &[Bytes], compression: CompressionType) -> Result<Bytes> {
    let data_size: usize = entries.iter().map(|e| ENTRY_LEN_SIZE + e.len()).sum();
    let mut entry_buf = BytesMut::with_capacity(data_size);

    for entry in entries {
        debug_assert!(entry.len() <= u32::MAX as usize);
        entry_buf.put_u32_le(entry.len() as u32);
        entry_buf.put_slice(entry);
    }

    let compressed = match compression {
        CompressionType::None => entry_buf.freeze(),
        CompressionType::Zstd => {
            let compressed = zstd::bulk::compress(&entry_buf, ZSTD_LEVEL)
                .map_err(|e| Error::Serialization(format!("zstd compression failed: {e}")))?;
            Bytes::from(compressed)
        }
    };

    let mut buf = BytesMut::with_capacity(compressed.len() + FOOTER_SIZE);
    buf.put_slice(&compressed);
    buf.put_u8(compression as u8);

    debug_assert!(entries.len() <= u32::MAX as usize);
    buf.put_u32_le(entries.len() as u32);
    buf.put_u16_le(FORMAT_VERSION);

    Ok(buf.freeze())
}

pub(crate) fn decode_batch(mut data: Bytes) -> Result<Vec<Bytes>> {
    if data.len() < FOOTER_SIZE {
        return Err(Error::Serialization(
            "batch too small for footer".to_string(),
        ));
    }

    let footer_start = data.len() - FOOTER_SIZE;
    let mut footer = data.split_off(footer_start);

    let compression_type = CompressionType::try_from(footer.get_u8())?;
    let record_count = footer.get_u32_le() as usize;
    let version = footer.get_u16_le();

    if version != FORMAT_VERSION {
        return Err(Error::Serialization(format!(
            "unsupported batch version: {}",
            version
        )));
    }

    let mut entry_data = match compression_type {
        CompressionType::None => data,
        CompressionType::Zstd => {
            let decompressed = zstd::stream::decode_all(data.as_ref())
                .map_err(|e| Error::Serialization(format!("zstd decompression failed: {e}")))?;
            Bytes::from(decompressed)
        }
    };

    let mut entries = Vec::with_capacity(record_count);
    for _ in 0..record_count {
        if entry_data.remaining() < ENTRY_LEN_SIZE {
            return Err(Error::Serialization("truncated record length".to_string()));
        }
        let len = entry_data.get_u32_le() as usize;
        if entry_data.remaining() < len {
            return Err(Error::Serialization("truncated record data".to_string()));
        }
        entries.push(entry_data.split_to(len));
    }

    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_roundtrip_batch() {
        let entries = vec![
            Bytes::from("hello"),
            Bytes::from("world"),
            Bytes::from("foo"),
        ];
        let encoded = encode_batch(&entries, CompressionType::None).unwrap();
        let decoded = decode_batch(encoded).unwrap();
        assert_eq!(decoded, entries);
    }

    #[test]
    fn should_roundtrip_empty_batch() {
        let entries: Vec<Bytes> = vec![];
        let encoded = encode_batch(&entries, CompressionType::None).unwrap();
        assert_eq!(encoded.len(), FOOTER_SIZE);
        let decoded = decode_batch(encoded).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn should_roundtrip_empty_record() {
        let entries = vec![Bytes::new()];
        let encoded = encode_batch(&entries, CompressionType::None).unwrap();
        let decoded = decode_batch(encoded).unwrap();
        assert_eq!(decoded, entries);
    }

    #[test]
    fn should_reject_truncated_data() {
        let entries = vec![Bytes::from("hello")];
        let mut encoded = BytesMut::from(
            encode_batch(&entries, CompressionType::None)
                .unwrap()
                .as_ref(),
        );
        encoded.truncate(encoded.len() - FOOTER_SIZE - 1);
        encoded.put_u8(CompressionType::None as u8);
        encoded.put_u32_le(1);
        encoded.put_u16_le(FORMAT_VERSION);
        let result = decode_batch(encoded.freeze());
        assert!(result.is_err());
    }

    #[test]
    fn should_reject_unsupported_version() {
        let mut buf = BytesMut::new();
        buf.put_u8(0);
        buf.put_u32_le(0);
        buf.put_u16_le(99);
        let result = decode_batch(buf.freeze());
        assert!(result.is_err());
    }

    #[test]
    fn should_roundtrip_batch_with_zstd() {
        let entries = vec![
            Bytes::from("hello"),
            Bytes::from("world"),
            Bytes::from("foo"),
        ];
        let encoded = encode_batch(&entries, CompressionType::Zstd).unwrap();
        let decoded = decode_batch(encoded).unwrap();
        assert_eq!(decoded, entries);
    }

    #[test]
    fn should_roundtrip_empty_batch_with_zstd() {
        let entries: Vec<Bytes> = vec![];
        let encoded = encode_batch(&entries, CompressionType::Zstd).unwrap();
        let decoded = decode_batch(encoded).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn should_roundtrip_large_batch_with_zstd() {
        let entries: Vec<Bytes> = (0..1000)
            .map(|i| Bytes::from(format!("entry-{:04}", i)))
            .collect();
        let encoded = encode_batch(&entries, CompressionType::Zstd).unwrap();
        let decoded = decode_batch(encoded).unwrap();
        assert_eq!(decoded, entries);
    }

    #[test]
    fn should_compress_smaller_than_uncompressed_for_repetitive_data() {
        let entries: Vec<Bytes> = (0..100)
            .map(|_| Bytes::from("repeated-data-that-compresses-well"))
            .collect();
        let uncompressed = encode_batch(&entries, CompressionType::None).unwrap();
        let compressed = encode_batch(&entries, CompressionType::Zstd).unwrap();
        assert!(
            compressed.len() < uncompressed.len(),
            "compressed ({}) should be smaller than uncompressed ({})",
            compressed.len(),
            uncompressed.len()
        );
    }

    #[test]
    fn should_reject_unsupported_compression_type() {
        let mut buf = BytesMut::new();
        buf.put_u8(0xFF); // unsupported compression type
        buf.put_u32_le(0);
        buf.put_u16_le(FORMAT_VERSION);
        let result = decode_batch(buf.freeze());
        assert!(
            matches!(result, Err(Error::Serialization(msg)) if msg.contains("unsupported compression type"))
        );
    }
}
