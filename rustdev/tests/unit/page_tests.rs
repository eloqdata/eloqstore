//! Unit tests for page module

use eloqstore_rs::page::{Page, DataPage, DataPageBuilder, PageMapper, PageCache};
use eloqstore_rs::types::{Key, Value, PageId, FilePageId, TableIdent};
use bytes::{Bytes, BytesMut};
use std::sync::Arc;

#[cfg(test)]
mod page_tests {
    use super::*;

    #[test]
    fn test_page_creation() {
        let mut buffer = BytesMut::zeroed(4096);
        buffer[0] = 0x42;
        let page = Page::from_bytes(buffer.freeze());

        assert_eq!(page.size(), 4096);
        assert_eq!(page.as_bytes()[0], 0x42);
    }

    #[test]
    fn test_data_page_header() {
        let page_id = PageId::from(100);
        let data_page = DataPage::new(page_id, 4096);

        assert_eq!(data_page.page_id(), page_id);
        assert_eq!(data_page.entry_count(), 0);
        assert!(!data_page.is_overflow());
        assert_eq!(data_page.next_page_id(), PageId::MAX);
        assert_eq!(data_page.prev_page_id(), PageId::MAX);
    }

    #[test]
    fn test_data_page_builder_basic() {
        let mut builder = DataPageBuilder::new(4096);

        // Add some entries
        let key1 = Key::from(b"key001".to_vec());
        let val1 = Value::from(b"value001".to_vec());

        let key2 = Key::from(b"key002".to_vec());
        let val2 = Value::from(b"value002".to_vec());

        assert!(builder.add_entry(&key1, &val1).is_ok());
        assert!(builder.add_entry(&key2, &val2).is_ok());

        let page = builder.build(PageId::from(1));
        assert_eq!(page.entry_count(), 2);
    }

    #[test]
    fn test_data_page_builder_overflow() {
        let mut builder = DataPageBuilder::new(256); // Small page

        // Try to add a large value
        let key = Key::from(b"key".to_vec());
        let large_value = Value::from(vec![0u8; 1024]);

        match builder.add_entry(&key, &large_value) {
            Err(e) => {
                // Should fail or handle overflow
                println!("Expected overflow handling: {:?}", e);
            }
            Ok(_) => {
                // If it succeeds, check overflow handling
                let page = builder.build(PageId::from(1));
                assert!(page.entry_count() <= 1);
            }
        }
    }

    #[test]
    fn test_page_mapper_basic() {
        let mapper = PageMapper::new();

        let page_id = PageId::from(100);
        let file_page_id = FilePageId::from(0x0001_0064); // file_id=1, offset=100

        mapper.update_mapping(page_id, file_page_id);

        let snapshot = mapper.snapshot();
        assert_eq!(snapshot.to_file_page(page_id).unwrap(), file_page_id);
    }

    #[test]
    fn test_page_mapper_allocate() {
        let mapper = PageMapper::new();

        let page_id1 = mapper.allocate_page_id();
        let page_id2 = mapper.allocate_page_id();

        assert_ne!(page_id1, page_id2);
        assert!(page_id1 < page_id2);
    }

    #[tokio::test]
    async fn test_page_cache_basic() {
        let cache = Arc::new(PageCache::new(100));

        let page_id = PageId::from(1);
        let data_page = Arc::new(DataPage::new(page_id, 4096));

        cache.insert(page_id, data_page.clone()).await;

        let retrieved = cache.get_by_id(page_id).await;
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().page_id(), page_id);
    }

    #[tokio::test]
    async fn test_page_cache_eviction() {
        let cache = Arc::new(PageCache::new(2)); // Small cache

        let page1 = Arc::new(DataPage::new(PageId::from(1), 4096));
        let page2 = Arc::new(DataPage::new(PageId::from(2), 4096));
        let page3 = Arc::new(DataPage::new(PageId::from(3), 4096));

        cache.insert(PageId::from(1), page1).await;
        cache.insert(PageId::from(2), page2).await;
        cache.insert(PageId::from(3), page3).await;

        // Page 1 should be evicted
        assert!(cache.get_by_id(PageId::from(1)).await.is_none());
        assert!(cache.get_by_id(PageId::from(2)).await.is_some());
        assert!(cache.get_by_id(PageId::from(3)).await.is_some());
    }
}

#[cfg(test)]
mod page_stress_tests {
    use super::*;
    use rand::{Rng, thread_rng, distributions::Alphanumeric};
    use std::collections::HashSet;

    fn generate_random_key(len: usize) -> Key {
        let key: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(len)
            .map(char::from)
            .collect();
        Key::from(key.into_bytes())
    }

    fn generate_random_value(len: usize) -> Value {
        let value: Vec<u8> = (0..len).map(|_| thread_rng().gen()).collect();
        Value::from(value)
    }

    #[test]
    fn stress_test_page_builder() {
        let mut rng = thread_rng();

        for _ in 0..100 {
            let page_size = rng.gen_range(1024..=16384);
            let mut builder = DataPageBuilder::new(page_size);

            let num_entries = rng.gen_range(1..=50);
            let mut added = 0;

            for _ in 0..num_entries {
                let key_len = rng.gen_range(5..=50);
                let val_len = rng.gen_range(10..=200);

                let key = generate_random_key(key_len);
                let value = generate_random_value(val_len);

                if builder.add_entry(&key, &value).is_ok() {
                    added += 1;
                } else {
                    break; // Page full
                }
            }

            let page = builder.build(PageId::from(rng.gen()));
            assert_eq!(page.entry_count() as usize, added);
        }
    }

    #[tokio::test]
    async fn stress_test_page_cache() {
        let cache = Arc::new(PageCache::new(100));
        let mut rng = thread_rng();

        // Insert random pages
        let mut inserted_ids = HashSet::new();
        for _ in 0..200 {
            let page_id = PageId::from(rng.gen::<u32>());
            let page = Arc::new(DataPage::new(page_id, 4096));

            cache.insert(page_id, page).await;
            inserted_ids.insert(page_id);
        }

        // Random lookups
        for _ in 0..500 {
            if rng.gen_bool(0.7) && !inserted_ids.is_empty() {
                // Look up existing page
                let ids: Vec<_> = inserted_ids.iter().cloned().collect();
                let page_id = ids[rng.gen_range(0..ids.len())];
                let page = cache.get_by_id(page_id).await;
                // May be evicted, so we just check it doesn't panic
                let _ = page;
            } else {
                // Look up non-existent page
                let page_id = PageId::from(rng.gen::<u32>());
                let page = cache.get_by_id(page_id).await;
                if !inserted_ids.contains(&page_id) {
                    assert!(page.is_none());
                }
            }
        }
    }

    #[test]
    fn stress_test_page_mapper() {
        let mapper = PageMapper::new();
        let mut rng = thread_rng();

        // Allocate many pages
        let mut allocated = Vec::new();
        for _ in 0..1000 {
            allocated.push(mapper.allocate_page_id());
        }

        // All should be unique
        let mut seen = HashSet::new();
        for page_id in &allocated {
            assert!(seen.insert(*page_id));
        }

        // Random mappings
        for _ in 0..500 {
            let page_id = allocated[rng.gen_range(0..allocated.len())];
            let file_id = rng.gen::<u16>();
            let offset = rng.gen::<u32>();
            let file_page_id = FilePageId::from(((file_id as u64) << 32) | offset as u64);

            mapper.update_mapping(page_id, file_page_id);

            let snapshot = mapper.snapshot();
            assert_eq!(snapshot.to_file_page(page_id).unwrap(), file_page_id);
        }
    }
}