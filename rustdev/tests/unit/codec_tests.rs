//! Unit tests for codec module

use eloqstore_rs::codec::{
    Comparator, BytewiseComparator,
    encode_varint, decode_varint, encode_fixed32, decode_fixed32,
    encode_fixed64, decode_fixed64, encode_key, decode_key
};
use eloqstore_rs::types::{Key, Value};
use bytes::BytesMut;

#[cfg(test)]
mod encoding_tests {
    use super::*;

    #[test]
    fn test_varint_encoding() {
        let test_cases = vec![
            0u64,
            1,
            127,
            128,
            300,
            65535,
            65536,
            u32::MAX as u64,
            u64::MAX,
        ];

        for value in test_cases {
            let mut buf = BytesMut::new();
            encode_varint(value, &mut buf);

            let decoded = decode_varint(&buf).expect("Failed to decode varint");
            assert_eq!(value, decoded, "Varint encoding/decoding failed for {}", value);
        }
    }

    #[test]
    fn test_fixed32_encoding() {
        let test_cases = vec![0u32, 1, 42, 65535, u32::MAX];

        for value in test_cases {
            let mut buf = BytesMut::new();
            encode_fixed32(value, &mut buf);

            assert_eq!(buf.len(), 4);
            let decoded = decode_fixed32(&buf).expect("Failed to decode fixed32");
            assert_eq!(value, decoded);
        }
    }

    #[test]
    fn test_fixed64_encoding() {
        let test_cases = vec![0u64, 1, 42, 65535, u32::MAX as u64, u64::MAX];

        for value in test_cases {
            let mut buf = BytesMut::new();
            encode_fixed64(value, &mut buf);

            assert_eq!(buf.len(), 8);
            let decoded = decode_fixed64(&buf).expect("Failed to decode fixed64");
            assert_eq!(value, decoded);
        }
    }

    #[test]
    fn test_key_encoding() {
        let test_keys = vec![
            Key::from(b"".to_vec()),
            Key::from(b"a".to_vec()),
            Key::from(b"hello".to_vec()),
            Key::from(b"key_with_special_\x00\x01\xff".to_vec()),
            Key::from(vec![0u8; 100]),
        ];

        for key in test_keys {
            let mut buf = BytesMut::new();
            encode_key(&key, &mut buf);

            let decoded = decode_key(&buf).expect("Failed to decode key");
            assert_eq!(key, decoded);
        }
    }
}

#[cfg(test)]
mod comparator_tests {
    use super::*;

    #[test]
    fn test_bytewise_comparator() {
        let comparator = BytewiseComparator;

        let key1 = Key::from(b"aaa".to_vec());
        let key2 = Key::from(b"aab".to_vec());
        let key3 = Key::from(b"bbb".to_vec());

        assert!(comparator.compare(&key1, &key2).is_lt());
        assert!(comparator.compare(&key2, &key3).is_lt());
        assert!(comparator.compare(&key3, &key1).is_gt());
        assert!(comparator.compare(&key1, &key1).is_eq());
    }

    #[test]
    fn test_comparator_edge_cases() {
        let comparator = BytewiseComparator;

        let empty = Key::from(b"".to_vec());
        let non_empty = Key::from(b"a".to_vec());

        assert!(comparator.compare(&empty, &non_empty).is_lt());
        assert!(comparator.compare(&non_empty, &empty).is_gt());
        assert!(comparator.compare(&empty, &empty).is_eq());
    }
}

#[cfg(test)]
mod codec_stress_tests {
    use super::*;
    use rand::{Rng, thread_rng};

    #[test]
    fn stress_test_varint() {
        let mut rng = thread_rng();

        for _ in 0..10000 {
            let value: u64 = rng.gen();
            let mut buf = BytesMut::new();
            encode_varint(value, &mut buf);

            let decoded = decode_varint(&buf).expect("Failed to decode");
            assert_eq!(value, decoded);
        }
    }

    #[test]
    fn stress_test_key_encoding() {
        let mut rng = thread_rng();

        for _ in 0..1000 {
            let len = rng.gen_range(0..=1000);
            let key_data: Vec<u8> = (0..len).map(|_| rng.gen()).collect();
            let key = Key::from(key_data);

            let mut buf = BytesMut::new();
            encode_key(&key, &mut buf);

            let decoded = decode_key(&buf).expect("Failed to decode");
            assert_eq!(key, decoded);
        }
    }

    #[test]
    fn stress_test_comparator_transitivity() {
        let comparator = BytewiseComparator;
        let mut rng = thread_rng();

        // Generate random keys
        let mut keys: Vec<Key> = Vec::new();
        for _ in 0..100 {
            let len = rng.gen_range(0..=50);
            let key_data: Vec<u8> = (0..len).map(|_| rng.gen()).collect();
            keys.push(Key::from(key_data));
        }

        // Test transitivity: if a < b and b < c, then a < c
        for i in 0..keys.len() {
            for j in 0..keys.len() {
                for k in 0..keys.len() {
                    let a = &keys[i];
                    let b = &keys[j];
                    let c = &keys[k];

                    if comparator.compare(a, b).is_lt() && comparator.compare(b, c).is_lt() {
                        assert!(comparator.compare(a, c).is_lt(),
                            "Transitivity violated: a < b && b < c but a >= c");
                    }
                }
            }
        }
    }
}