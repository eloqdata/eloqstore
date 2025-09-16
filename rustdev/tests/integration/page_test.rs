//! Integration tests for page operations

use eloqstore_rs::page::{DataPage, DataPageBuilder, DataPageIterator};
use eloqstore_rs::types::{PageType, DEFAULT_PAGE_SIZE};
use eloqstore_rs::utils::{BytewiseComparator, Comparator};

#[test]
fn test_page_build_and_iterate() {
    let mut builder = DataPageBuilder::new(DEFAULT_PAGE_SIZE);

    // Add entries in order
    let entries = vec![
        (b"apple", b"fruit1", 100u64),
        (b"banana", b"fruit2", 101u64),
        (b"cherry", b"fruit3", 102u64),
        (b"date", b"fruit4", 103u64),
        (b"elderberry", b"fruit5", 104u64),
    ];

    for (key, value, ts) in &entries {
        assert!(builder.add(*key, *value, *ts, None, false));
    }

    let page = builder.finish(1);

    // Verify page properties
    assert_eq!(page.page_id(), 1);
    assert!(!page.is_empty());
    assert!(page.restart_count() > 0);

    // Test iteration
    let mut iter = DataPageIterator::new(&page);
    let mut count = 0;

    while let Some((key, value, ts, _)) = iter.next() {
        assert_eq!(key.as_ref(), entries[count].0);
        assert_eq!(value.as_ref(), entries[count].1);
        assert_eq!(ts, entries[count].2);
        count += 1;
    }

    assert_eq!(count, entries.len());
}

#[test]
fn test_page_binary_search() {
    let mut builder = DataPageBuilder::new(DEFAULT_PAGE_SIZE)
        .with_restart_interval(3); // More frequent restarts for testing

    // Add many entries to ensure multiple restart points
    let mut entries = Vec::new();
    for i in 0..50 {
        let key = format!("key_{:04}", i);
        let value = format!("value_{}", i);
        entries.push((key.clone(), value.clone(), i as u64));

        if !builder.add(key.as_bytes(), value.as_bytes(), i as u64, None, false) {
            break; // Page full
        }
    }

    let page = builder.finish(2);
    let added_count = entries.len().min(50); // How many actually fit

    // Test seeking to various keys
    let cmp = BytewiseComparator;

    // Test seek to existing key
    let mut iter = DataPageIterator::new(&page);
    if added_count > 10 {
        assert!(iter.seek(b"key_0010"));
        let key = iter.key().unwrap();
        assert_eq!(key.as_ref(), b"key_0010");
    }

    // Test seek to non-existing key (should find next greater)
    let mut iter = DataPageIterator::new(&page);
    if added_count > 15 {
        assert!(iter.seek(b"key_0014_not_exist"));
        let key = iter.key().unwrap();
        assert!(cmp.compare(key.as_ref(), b"key_0014_not_exist") > std::cmp::Ordering::Equal);
    }

    // Test seek beyond all keys
    let mut iter = DataPageIterator::new(&page);
    assert!(!iter.seek(b"key_9999"));
}

#[test]
fn test_page_seek_floor() {
    let mut builder = DataPageBuilder::new(DEFAULT_PAGE_SIZE);

    // Add entries with gaps
    let entries = vec![
        (b"key_001", b"val1", 1u64),
        (b"key_003", b"val3", 3u64),
        (b"key_005", b"val5", 5u64),
        (b"key_007", b"val7", 7u64),
        (b"key_009", b"val9", 9u64),
    ];

    for (key, value, ts) in &entries {
        assert!(builder.add(*key, *value, *ts, None, false));
    }

    let page = builder.finish(3);

    // Test floor seek
    let mut iter = DataPageIterator::new(&page);

    // Exact match
    assert!(iter.seek_floor(b"key_005"));
    assert_eq!(iter.key().unwrap().as_ref(), b"key_005");

    // Between keys (should find previous)
    let mut iter = DataPageIterator::new(&page);
    assert!(iter.seek_floor(b"key_006"));
    assert_eq!(iter.key().unwrap().as_ref(), b"key_005");

    // Before first key
    let mut iter = DataPageIterator::new(&page);
    assert!(!iter.seek_floor(b"key_000"));

    // After last key
    let mut iter = DataPageIterator::new(&page);
    assert!(iter.seek_floor(b"key_999"));
    assert_eq!(iter.key().unwrap().as_ref(), b"key_009");
}

#[test]
fn test_page_with_expiration() {
    let mut builder = DataPageBuilder::new(DEFAULT_PAGE_SIZE);

    // Add entries with expiration
    assert!(builder.add(b"expire1", b"value1", 100, Some(1000), false));
    assert!(builder.add(b"expire2", b"value2", 101, Some(2000), false));
    assert!(builder.add(b"no_expire", b"value3", 102, None, false));

    let page = builder.finish(4);

    // Verify expiration timestamps
    let mut iter = DataPageIterator::new(&page);

    let (_, _, _, expire) = iter.next().unwrap();
    assert_eq!(expire, Some(1000));

    let (_, _, _, expire) = iter.next().unwrap();
    assert_eq!(expire, Some(2000));

    let (_, _, _, expire) = iter.next().unwrap();
    assert_eq!(expire, None);
}

#[test]
fn test_page_overflow_flag() {
    let mut builder = DataPageBuilder::new(DEFAULT_PAGE_SIZE);

    // Add regular and overflow entries
    assert!(builder.add(b"regular", b"normal_value", 100, None, false));
    assert!(builder.add(b"overflow", b"\x01\x00\x00\x00\x02\x00\x00\x00", 101, None, true)); // Overflow pointers

    let page = builder.finish(5);

    let mut iter = DataPageIterator::new(&page);

    // First entry is not overflow
    assert!(!iter.is_overflow());
    iter.next();

    // Second entry is overflow
    assert!(iter.is_overflow());
}

#[test]
fn test_page_checksum_verification() {
    let mut builder = DataPageBuilder::new(DEFAULT_PAGE_SIZE);

    builder.add(b"key1", b"value1", 100, None, false);
    builder.add(b"key2", b"value2", 101, None, false);

    let page = builder.finish(6);

    // Verify checksum is valid
    assert!(page.page().verify_checksum());
}

#[test]
fn test_page_capacity() {
    let page_size = 512; // Small page for testing
    let mut builder = DataPageBuilder::new(page_size);

    let mut added = 0;
    let value = vec![b'x'; 50]; // Medium-sized value

    // Add entries until page is full
    for i in 0..100 {
        let key = format!("key_{:03}", i);
        if builder.add(key.as_bytes(), &value, i as u64, None, false) {
            added += 1;
        } else {
            break; // Page full
        }
    }

    assert!(added > 0);
    assert!(added < 100); // Should not fit all entries in small page

    let page = builder.finish(7);

    // Verify all added entries are readable
    let mut iter = DataPageIterator::new(&page);
    let mut count = 0;
    while iter.next().is_some() {
        count += 1;
    }
    assert_eq!(count, added);
}

#[test]
fn test_empty_page() {
    let builder = DataPageBuilder::new(DEFAULT_PAGE_SIZE);
    assert!(builder.is_empty());

    let page = builder.finish(8);
    assert!(page.is_empty());
    assert_eq!(page.restart_count(), 1); // Should have at least one restart point at 0

    let mut iter = DataPageIterator::new(&page);
    assert!(iter.next().is_none());
}