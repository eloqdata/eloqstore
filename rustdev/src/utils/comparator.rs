//! Key comparator for ordering and comparison operations

use std::cmp::Ordering;

/// Trait for comparing keys
pub trait Comparator: Send + Sync {
    /// Compare two keys
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering;

    /// Get name of this comparator
    fn name(&self) -> &str;

    /// Check if two keys are equal
    fn equal(&self, a: &[u8], b: &[u8]) -> bool {
        self.compare(a, b) == Ordering::Equal
    }

    /// Check if a < b
    fn less_than(&self, a: &[u8], b: &[u8]) -> bool {
        self.compare(a, b) == Ordering::Less
    }

    /// Find the shortest separator between start and limit
    fn find_shortest_separator(&self, start: &mut Vec<u8>, limit: &[u8]) {
        // Default implementation: find common prefix and increment
        let min_len = std::cmp::min(start.len(), limit.len());
        let mut diff_index = 0;

        while diff_index < min_len && start[diff_index] == limit[diff_index] {
            diff_index += 1;
        }

        if diff_index >= min_len {
            // One string is prefix of another
            return;
        }

        let diff_byte = start[diff_index];
        if diff_byte < 0xff && diff_byte + 1 < limit[diff_index] {
            start[diff_index] += 1;
            start.truncate(diff_index + 1);
        }
    }

    /// Find a short key that is >= key
    fn find_short_successor(&self, key: &mut Vec<u8>) {
        // Default implementation: find first byte that can be incremented
        for i in 0..key.len() {
            if key[i] != 0xff {
                key[i] += 1;
                key.truncate(i + 1);
                return;
            }
        }
        // If all bytes are 0xff, append a zero
        key.push(0);
    }
}

/// Default bytewise comparator
#[derive(Debug, Clone)]
pub struct BytewiseComparator;

impl Comparator for BytewiseComparator {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        a.cmp(b)
    }

    fn name(&self) -> &str {
        "BytewiseComparator"
    }
}

/// Reverse comparator (for reverse iteration)
#[derive(Debug, Clone)]
pub struct ReverseComparator<C> {
    inner: C,
}

impl<C> ReverseComparator<C> {
    /// Create a new reverse comparator
    pub fn new(inner: C) -> Self {
        Self { inner }
    }
}

impl<C: Comparator> Comparator for ReverseComparator<C> {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        self.inner.compare(b, a)
    }

    fn name(&self) -> &str {
        "ReverseComparator"
    }
}

/// Integer comparator for u64 keys
#[derive(Debug, Clone)]
pub struct IntegerComparator;

impl IntegerComparator {
    fn decode_u64(data: &[u8]) -> Option<u64> {
        if data.len() != 8 {
            return None;
        }
        Some(u64::from_be_bytes(data.try_into().ok()?))
    }
}

impl Comparator for IntegerComparator {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        match (Self::decode_u64(a), Self::decode_u64(b)) {
            (Some(a_val), Some(b_val)) => a_val.cmp(&b_val),
            _ => a.cmp(b), // Fall back to bytewise comparison
        }
    }

    fn name(&self) -> &str {
        "IntegerComparator"
    }
}

/// Global default comparator instance
pub static DEFAULT_COMPARATOR: BytewiseComparator = BytewiseComparator;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bytewise_comparator() {
        let cmp = BytewiseComparator;

        assert_eq!(cmp.compare(b"abc", b"abc"), Ordering::Equal);
        assert_eq!(cmp.compare(b"abc", b"abd"), Ordering::Less);
        assert_eq!(cmp.compare(b"abd", b"abc"), Ordering::Greater);

        assert!(cmp.equal(b"test", b"test"));
        assert!(cmp.less_than(b"a", b"b"));
    }

    #[test]
    fn test_reverse_comparator() {
        let cmp = ReverseComparator::new(BytewiseComparator);

        assert_eq!(cmp.compare(b"abc", b"abc"), Ordering::Equal);
        assert_eq!(cmp.compare(b"abc", b"abd"), Ordering::Greater);
        assert_eq!(cmp.compare(b"abd", b"abc"), Ordering::Less);
    }

    #[test]
    fn test_integer_comparator() {
        let cmp = IntegerComparator;

        let a = 100u64.to_be_bytes();
        let b = 200u64.to_be_bytes();
        let c = 100u64.to_be_bytes();

        assert_eq!(cmp.compare(&a, &b), Ordering::Less);
        assert_eq!(cmp.compare(&b, &a), Ordering::Greater);
        assert_eq!(cmp.compare(&a, &c), Ordering::Equal);
    }

    #[test]
    fn test_find_shortest_separator() {
        let cmp = BytewiseComparator;

        let mut start = b"abc".to_vec();
        let limit = b"abz";
        cmp.find_shortest_separator(&mut start, limit);
        assert!(start.len() <= 3);
        assert!(start.as_slice() >= b"abc".as_ref());
        assert!(start.as_slice() < b"abz".as_ref());
    }

    #[test]
    fn test_find_short_successor() {
        let cmp = BytewiseComparator;

        let mut key = b"abc".to_vec();
        cmp.find_short_successor(&mut key);
        assert!(key.as_slice() > b"abc".as_ref());
        assert!(key.len() <= 3);

        let mut key_ff = vec![0xff, 0xff, 0xff];
        cmp.find_short_successor(&mut key_ff);
        assert_eq!(key_ff, vec![0xff, 0xff, 0xff, 0]);
    }
}