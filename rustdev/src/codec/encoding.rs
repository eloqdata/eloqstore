//! Encoding and decoding utilities for keys, values, and metadata

use bytes::{BufMut, Bytes, BytesMut};

use crate::types::{PageId, MAX_OVERFLOW_POINTERS};
use crate::Result;

/// Encode a variable-length integer (varint)
pub fn encode_varint(mut value: u64) -> Vec<u8> {
    let mut result = Vec::with_capacity(10);

    while value >= 0x80 {
        result.push((value as u8) | 0x80);
        value >>= 7;
    }
    result.push(value as u8);

    result
}

/// Encode varint into a buffer, returning bytes written
pub fn encode_varint_into(mut value: u64, buf: &mut [u8]) -> usize {
    let mut i = 0;

    while value >= 0x80 && i < buf.len() {
        buf[i] = (value as u8) | 0x80;
        value >>= 7;
        i += 1;
    }

    if i < buf.len() {
        buf[i] = value as u8;
        i + 1
    } else {
        0 // Buffer too small
    }
}

/// Decode a variable-length integer from bytes
pub fn decode_varint(data: &[u8]) -> Option<(u64, usize)> {
    let mut result = 0u64;
    let mut shift = 0;

    for (i, &byte) in data.iter().enumerate() {
        if i >= 10 {
            return None; // Varint too long
        }

        result |= ((byte & 0x7F) as u64) << shift;

        if byte & 0x80 == 0 {
            return Some((result, i + 1));
        }

        shift += 7;
    }

    None
}

/// Encode varint32 (for compatibility)
pub fn encode_varint32(buf: &mut impl bytes::BufMut, value: u32) {
    let mut val = value;
    while val >= 0x80 {
        buf.put_u8((val as u8) | 0x80);
        val >>= 7;
    }
    buf.put_u8(val as u8);
}

/// Decode varint32 from data at offset
pub fn decode_varint32(data: &[u8], offset: usize) -> Option<(u32, usize)> {
    if offset >= data.len() {
        return None;
    }

    let mut result = 0u32;
    let mut shift = 0;
    let mut pos = offset;

    while pos < data.len() {
        let byte = data[pos];
        pos += 1;

        if shift >= 32 {
            return None;
        }

        result |= ((byte & 0x7F) as u32) << shift;

        if byte & 0x80 == 0 {
            return Some((result, pos));
        }

        shift += 7;
    }

    None
}

/// Get size of varint32
pub fn varint32_size(value: u32) -> usize {
    let mut v = value;
    let mut size = 1;
    while v >= 0x80 {
        v >>= 7;
        size += 1;
    }
    size
}

/// Encode a fixed 32-bit integer
pub fn encode_fixed32(value: u32) -> [u8; 4] {
    value.to_le_bytes()
}

/// Decode a fixed 32-bit integer
pub fn decode_fixed32(data: &[u8]) -> Option<u32> {
    if data.len() < 4 {
        return None;
    }
    Some(u32::from_le_bytes(data[..4].try_into().ok()?))
}

/// Encode a fixed 64-bit integer
pub fn encode_fixed64(value: u64) -> [u8; 8] {
    value.to_le_bytes()
}

/// Decode a fixed 64-bit integer
pub fn decode_fixed64(data: &[u8]) -> Option<u64> {
    if data.len() < 8 {
        return None;
    }
    Some(u64::from_le_bytes(data[..8].try_into().ok()?))
}

/// Encode a length-prefixed string
pub fn encode_length_prefixed(data: &[u8]) -> Vec<u8> {
    let mut result = encode_varint(data.len() as u64);
    result.extend_from_slice(data);
    result
}

/// Decode a length-prefixed string
pub fn decode_length_prefixed(data: &[u8]) -> Option<(&[u8], usize)> {
    let (len, bytes_read) = decode_varint(data)?;
    let len = len as usize;

    if data.len() < bytes_read + len {
        return None;
    }

    Some((&data[bytes_read..bytes_read + len], bytes_read + len))
}

/// Encode overflow pointers
pub fn encode_overflow_pointers(pointers: &[PageId]) -> Vec<u8> {
    let mut result = Vec::new();

    // Encode count
    result.push(pointers.len() as u8);

    // Encode each pointer
    for &ptr in pointers {
        result.extend_from_slice(&encode_fixed32(ptr));
    }

    result
}

/// Decode overflow pointers
pub fn decode_overflow_pointers(data: &[u8]) -> Option<Vec<PageId>> {
    if data.is_empty() {
        return None;
    }

    let count = data[0] as usize;
    if count > MAX_OVERFLOW_POINTERS {
        return None;
    }

    let mut pointers = Vec::with_capacity(count);
    let mut pos = 1;

    for _ in 0..count {
        if pos + 4 > data.len() {
            return None;
        }
        pointers.push(decode_fixed32(&data[pos..])?);
        pos += 4;
    }

    Some(pointers)
}

/// Encode a key-value pair with metadata
pub struct KvEncoder {
    buffer: BytesMut,
}

impl KvEncoder {
    /// Create a new encoder
    pub fn new() -> Self {
        Self {
            buffer: BytesMut::with_capacity(256),
        }
    }

    /// Encode key to target
    pub fn encode_key(&self, target: &mut Vec<u8>, key: &[u8]) -> Result<()> {
        // Encode key length as varint
        let key_len_bytes = encode_varint(key.len() as u64);
        target.extend_from_slice(&key_len_bytes);
        // Encode key data
        target.extend_from_slice(key);
        Ok(())
    }

    /// Encode value to target
    pub fn encode_value(&self, target: &mut Vec<u8>, value: &[u8]) -> Result<()> {
        // Encode value length as varint
        let val_len_bytes = encode_varint(value.len() as u64);
        target.extend_from_slice(&val_len_bytes);
        // Encode value data
        target.extend_from_slice(value);
        Ok(())
    }

    /// Encode a key-value entry
    pub fn encode_entry(
        &mut self,
        key: &[u8],
        value: &[u8],
        timestamp: u64,
        expire_ts: Option<u64>,
        is_overflow: bool,
    ) -> Bytes {
        self.buffer.clear();

        // Encode key length
        let key_len_bytes = encode_varint(key.len() as u64);
        self.buffer.put_slice(&key_len_bytes);

        // Encode value length with flags
        let mut val_len_with_flags = (value.len() as u64) << 4;
        if is_overflow {
            val_len_with_flags |= 1 << 0; // Overflow bit
        }
        if expire_ts.is_some() {
            val_len_with_flags |= 1 << 1; // Expire bit
        }
        let val_len_bytes = encode_varint(val_len_with_flags);
        self.buffer.put_slice(&val_len_bytes);

        // Encode timestamp
        let ts_bytes = encode_varint(timestamp);
        self.buffer.put_slice(&ts_bytes);

        // Encode expiration if present
        if let Some(expire) = expire_ts {
            let expire_bytes = encode_varint(expire);
            self.buffer.put_slice(&expire_bytes);
        }

        // Encode key and value
        self.buffer.put_slice(key);
        self.buffer.put_slice(value);

        self.buffer.clone().freeze()
    }

    /// Get the current buffer size
    pub fn buffer_size(&self) -> usize {
        self.buffer.len()
    }
}

/// Decode a key-value entry
pub fn decode_kv_entry(data: &[u8]) -> Option<(Vec<u8>, Vec<u8>, u64, Option<u64>, bool)> {
    let mut pos = 0;

    // Decode key length
    let (key_len, bytes_read) = decode_varint(&data[pos..])?;
    pos += bytes_read;

    // Decode value length with flags
    let (val_len_with_flags, bytes_read) = decode_varint(&data[pos..])?;
    pos += bytes_read;

    let is_overflow = (val_len_with_flags & (1 << 0)) != 0;
    let has_expire = (val_len_with_flags & (1 << 1)) != 0;
    let val_len = (val_len_with_flags >> 4) as usize;

    // Decode timestamp
    let (timestamp, bytes_read) = decode_varint(&data[pos..])?;
    pos += bytes_read;

    // Decode expiration if present
    let expire_ts = if has_expire {
        let (expire, bytes_read) = decode_varint(&data[pos..])?;
        pos += bytes_read;
        Some(expire)
    } else {
        None
    };

    // Decode key
    let key_len = key_len as usize;
    if pos + key_len > data.len() {
        return None;
    }
    let key = data[pos..pos + key_len].to_vec();
    pos += key_len;

    // Decode value
    if pos + val_len > data.len() {
        return None;
    }
    let value = data[pos..pos + val_len].to_vec();

    Some((key, value, timestamp, expire_ts, is_overflow))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_varint_encoding() {
        // Test small values
        let encoded = encode_varint(127);
        assert_eq!(encoded, vec![127]);
        let (decoded, bytes) = decode_varint(&encoded).unwrap();
        assert_eq!(decoded, 127);
        assert_eq!(bytes, 1);

        // Test medium values
        let encoded = encode_varint(300);
        assert_eq!(encoded, vec![172, 2]);
        let (decoded, bytes) = decode_varint(&encoded).unwrap();
        assert_eq!(decoded, 300);
        assert_eq!(bytes, 2);

        // Test large values
        let encoded = encode_varint(1_000_000);
        let (decoded, bytes) = decode_varint(&encoded).unwrap();
        assert_eq!(decoded, 1_000_000);
        assert_eq!(bytes, encoded.len());
    }

    #[test]
    fn test_fixed_encoding() {
        let encoded = encode_fixed32(0x12345678);
        assert_eq!(encoded, [0x78, 0x56, 0x34, 0x12]);
        assert_eq!(decode_fixed32(&encoded), Some(0x12345678));

        let encoded = encode_fixed64(0x123456789ABCDEF0);
        assert_eq!(decode_fixed64(&encoded), Some(0x123456789ABCDEF0));
    }

    #[test]
    fn test_length_prefixed() {
        let data = b"hello world";
        let encoded = encode_length_prefixed(data);

        let (decoded, bytes_read) = decode_length_prefixed(&encoded).unwrap();
        assert_eq!(decoded, data);
        assert_eq!(bytes_read, encoded.len());
    }

    #[test]
    fn test_overflow_pointers() {
        let pointers = vec![100, 200, 300];
        let encoded = encode_overflow_pointers(&pointers);

        let decoded = decode_overflow_pointers(&encoded).unwrap();
        assert_eq!(decoded, pointers);
    }

    #[test]
    fn test_kv_encoding() {
        let mut encoder = KvEncoder::new();

        let key = b"test_key";
        let value = b"test_value";
        let timestamp = 12345;
        let expire_ts = Some(67890);

        let encoded = encoder.encode_entry(key, value, timestamp, expire_ts, false);

        let (dec_key, dec_value, dec_ts, dec_expire, dec_overflow) =
            decode_kv_entry(&encoded).unwrap();

        assert_eq!(dec_key, key);
        assert_eq!(dec_value, value);
        assert_eq!(dec_ts, timestamp);
        assert_eq!(dec_expire, expire_ts);
        assert_eq!(dec_overflow, false);
    }
}