//! Integration tests for page operations

extern crate eloqstore;

use eloqstore::page::{DataPage, DataPageBuilder};
use eloqstore::types::{PageId, Key, Value};
use eloqstore::codec::{BytewiseComparator, Comparator};

#[test]
fn test_page_build_basic() {
    let mut builder = DataPageBuilder::new(4096);

    // Add entries
    let entries = vec![
        (Key::from(b"apple".to_vec()), Value::from(b"fruit1".to_vec())),
        (Key::from(b"banana".to_vec()), Value::from(b"fruit2".to_vec())),
        (Key::from(b"cherry".to_vec()), Value::from(b"fruit3".to_vec())),
    ];

    for (key, value) in &entries {
        assert!(builder.add_entry(key, value).is_ok());
    }

    let page = builder.build(PageId::from(1));

    // Verify page properties
    assert_eq!(page.page_id(), PageId::from(1));
    assert_eq!(page.entry_count(), 3);
}

#[test]
fn test_page_overflow() {
    let mut builder = DataPageBuilder::new(256); // Small page

    // Try to add large entries
    let key = Key::from(b"key".to_vec());
    let large_value = Value::from(vec![0u8; 200]);

    // This should either fail or handle overflow
    let result = builder.add_entry(&key, &large_value);

    // If it succeeds, the page should be built
    if result.is_ok() {
        let page = builder.build(PageId::from(1));
        assert!(page.entry_count() <= 1);
    }
}

#[test]
fn test_data_page_creation() {
    let page_id = PageId::from(42);
    let data_page = DataPage::new(page_id, 4096);

    assert_eq!(data_page.page_id(), page_id);
    assert_eq!(data_page.entry_count(), 0);
    assert!(!data_page.is_overflow());
}

#[test]
fn test_comparator() {
    let comp = BytewiseComparator;

    let key1 = Key::from(b"aaa".to_vec());
    let key2 = Key::from(b"bbb".to_vec());
    let key3 = Key::from(b"ccc".to_vec());

    assert!(comp.compare(&key1, &key2).is_lt());
    assert!(comp.compare(&key2, &key3).is_lt());
    assert!(comp.compare(&key3, &key1).is_gt());
    assert!(comp.compare(&key1, &key1).is_eq());
}