use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::config::BatchCompression;
use crate::error::{Error, Result};

const ENTRY_LEN_SIZE: usize = 4;
const FORMAT_VERSION: u16 = 1;
const RECORD_COUNT_SIZE: usize = 4;
const VERSION_SIZE: usize = 2;
const COMPRESSION_SIZE: usize = 1;
const FOOTER_SIZE: usize = VERSION_SIZE + RECORD_COUNT_SIZE + COMPRESSION_SIZE;

const COMPRESSION_NONE: u8 = 0;
const COMPRESSION_ZSTD: u8 = 1;

fn compression_to_u8(compression: &BatchCompression) -> u8 {
    match compression {
        BatchCompression::None => COMPRESSION_NONE,
        BatchCompression::Zstd => COMPRESSION_ZSTD,
    }
}

pub(crate) fn encode_batch(entries: &[Bytes], compression: &BatchCompression) -> Result<Bytes> {
    let data_size: usize = entries.iter().map(|e| ENTRY_LEN_SIZE + e.len()).sum();
    let mut record_region = BytesMut::with_capacity(data_size);

    for entry in entries {
        debug_assert!(entry.len() <= u32::MAX as usize);
        record_region.put_u32_le(entry.len() as u32);
        record_region.put_slice(entry);
    }

    let record_bytes = match compression {
        BatchCompression::None => record_region.freeze(),
        BatchCompression::Zstd => {
            let compressed = zstd::bulk::compress(&record_region, 3)
                .map_err(|e| Error::Serialization(format!("zstd compression failed: {e}")))?;
            Bytes::from(compressed)
        }
    };

    let mut buf = BytesMut::with_capacity(record_bytes.len() + FOOTER_SIZE);
    buf.put(record_bytes);

    debug_assert!(entries.len() <= u32::MAX as usize);
    buf.put_u16_le(FORMAT_VERSION);
    buf.put_u32_le(entries.len() as u32);
    buf.put_u8(compression_to_u8(compression));

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

    let version = footer.get_u16_le();
    let record_count = footer.get_u32_le() as usize;
    let compression = footer.get_u8();

    if version != FORMAT_VERSION {
        return Err(Error::Serialization(format!(
            "unsupported batch version: {version}"
        )));
    }

    let mut record_region = match compression {
        COMPRESSION_NONE => data,
        COMPRESSION_ZSTD => {
            let decompressed = zstd::stream::decode_all(data.as_ref())
                .map_err(|e| Error::Serialization(format!("zstd decompression failed: {e}")))?;
            Bytes::from(decompressed)
        }
        _ => {
            return Err(Error::Serialization(format!(
                "unsupported compression codec: {compression}"
            )));
        }
    };

    let mut entries = Vec::with_capacity(record_count);
    for _ in 0..record_count {
        if record_region.remaining() < ENTRY_LEN_SIZE {
            return Err(Error::Serialization("truncated record length".to_string()));
        }
        let len = record_region.get_u32_le() as usize;
        if record_region.remaining() < len {
            return Err(Error::Serialization("truncated record data".to_string()));
        }
        entries.push(record_region.split_to(len));
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
        let encoded = encode_batch(&entries, &BatchCompression::None).unwrap();
        let decoded = decode_batch(encoded).unwrap();
        assert_eq!(decoded, entries);
    }

    #[test]
    fn should_roundtrip_empty_batch() {
        let entries: Vec<Bytes> = vec![];
        let encoded = encode_batch(&entries, &BatchCompression::None).unwrap();
        assert_eq!(encoded.len(), FOOTER_SIZE);
        let decoded = decode_batch(encoded).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn should_roundtrip_empty_record() {
        let entries = vec![Bytes::new()];
        let encoded = encode_batch(&entries, &BatchCompression::None).unwrap();
        let decoded = decode_batch(encoded).unwrap();
        assert_eq!(decoded, entries);
    }

    #[test]
    fn should_reject_truncated_data() {
        let entries = vec![Bytes::from("hello")];
        let mut encoded = BytesMut::new();
        // Write a truncated record region
        encoded.put_u32_le(100); // claims 100 bytes but we won't write them
        // Write footer
        encoded.put_u16_le(FORMAT_VERSION);
        encoded.put_u32_le(1);
        encoded.put_u8(COMPRESSION_NONE);
        let result = decode_batch(encoded.freeze());
        assert!(result.is_err());
        let _ = entries;
    }

    #[test]
    fn should_reject_unsupported_version() {
        let mut buf = BytesMut::new();
        buf.put_u16_le(99);
        buf.put_u32_le(0);
        buf.put_u8(COMPRESSION_NONE);
        let result = decode_batch(buf.freeze());
        assert!(result.is_err());
    }

    #[test]
    fn should_roundtrip_uncompressed_batch() {
        let entries = vec![
            Bytes::from("entry-a"),
            Bytes::from("entry-b"),
            Bytes::from("entry-c"),
        ];
        let encoded = encode_batch(&entries, &BatchCompression::None).unwrap();
        let decoded = decode_batch(encoded).unwrap();
        assert_eq!(decoded, entries);
    }

    #[test]
    fn should_roundtrip_zstd_compressed_batch() {
        let entries = vec![
            Bytes::from("entry-a"),
            Bytes::from("entry-b"),
            Bytes::from("entry-c"),
        ];
        let encoded = encode_batch(&entries, &BatchCompression::Zstd).unwrap();
        let decoded = decode_batch(encoded).unwrap();
        assert_eq!(decoded, entries);
    }

    #[test]
    fn should_reject_unsupported_compression_codec() {
        let mut buf = BytesMut::new();
        // empty record region, footer with unknown compression
        buf.put_u16_le(FORMAT_VERSION);
        buf.put_u32_le(0);
        buf.put_u8(99);
        let result = decode_batch(buf.freeze());
        assert!(
            matches!(result, Err(Error::Serialization(msg)) if msg.contains("unsupported compression codec"))
        );
    }
}
