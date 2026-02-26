use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::{Error, Result};

const FORMAT_VERSION: u16 = 1;
const FOOTER_SIZE: usize = 6; // record_count(4) + version(2)

pub(crate) fn encode_batch(entries: &[Bytes]) -> Bytes {
    let data_size: usize = entries.iter().map(|e| 4 + e.len()).sum();
    let mut buf = BytesMut::with_capacity(data_size + FOOTER_SIZE);

    for entry in entries {
        buf.put_u32_le(entry.len() as u32);
        buf.put_slice(entry);
    }

    buf.put_u32_le(entries.len() as u32);
    buf.put_u16_le(FORMAT_VERSION);

    buf.freeze()
}

#[allow(dead_code)]
pub(crate) fn decode_batch(mut data: Bytes) -> Result<Vec<Bytes>> {
    if data.len() < FOOTER_SIZE {
        return Err(Error::Serialization(
            "batch too small for footer".to_string(),
        ));
    }

    let footer_start = data.len() - FOOTER_SIZE;
    let mut footer = data.split_off(footer_start);

    let record_count = footer.get_u32_le() as usize;
    let version = footer.get_u16_le();

    if version != FORMAT_VERSION {
        return Err(Error::Serialization(format!(
            "unsupported batch version: {}",
            version
        )));
    }

    let mut entries = Vec::with_capacity(record_count);
    for _ in 0..record_count {
        if data.remaining() < 4 {
            return Err(Error::Serialization("truncated record length".to_string()));
        }
        let len = data.get_u32_le() as usize;
        if data.remaining() < len {
            return Err(Error::Serialization("truncated record data".to_string()));
        }
        entries.push(data.split_to(len));
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
        let encoded = encode_batch(&entries);
        let decoded = decode_batch(encoded).unwrap();
        assert_eq!(decoded, entries);
    }

    #[test]
    fn should_roundtrip_empty_batch() {
        let entries: Vec<Bytes> = vec![];
        let encoded = encode_batch(&entries);
        assert_eq!(encoded.len(), FOOTER_SIZE);
        let decoded = decode_batch(encoded).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn should_roundtrip_empty_record() {
        let entries = vec![Bytes::new()];
        let encoded = encode_batch(&entries);
        let decoded = decode_batch(encoded).unwrap();
        assert_eq!(decoded, entries);
    }

    #[test]
    fn should_reject_truncated_data() {
        let entries = vec![Bytes::from("hello")];
        let mut encoded = BytesMut::from(encode_batch(&entries).as_ref());
        encoded.truncate(encoded.len() - FOOTER_SIZE - 1); // chop record data
        // re-append a valid footer
        encoded.put_u32_le(1);
        encoded.put_u16_le(FORMAT_VERSION);
        let result = decode_batch(encoded.freeze());
        assert!(result.is_err());
    }

    #[test]
    fn should_reject_unsupported_version() {
        let mut buf = BytesMut::new();
        buf.put_u32_le(0); // record_count
        buf.put_u16_le(99); // bad version
        let result = decode_batch(buf.freeze());
        assert!(result.is_err());
    }
}
