//! Unit tests for index module

use eloqstore_rs::index::{IndexPageManager, CowRootMeta, MemIndexPage};
use eloqstore_rs::types::{PageId, FilePageId, TableIdent, Key, Value};
use eloqstore_rs::page::PageMapper;
use std::sync::Arc;

#[cfg(test)]
mod index_tests {
    use super::*;

    #[test]
    fn test_index_page_manager_creation() {
        let manager = IndexPageManager::new();
        assert_eq!(manager.num_tables(), 0);
    }

    #[test]
    fn test_cow_root_creation() {
        let manager = IndexPageManager::new();
        let table_id = TableIdent::new("test_table");

        let cow_root = manager.make_cow_root(&table_id).unwrap();
        assert_eq!(cow_root.root_id, PageId::MAX);
        assert_eq!(cow_root.ttl_root_id, PageId::MAX);
    }

    #[test]
    fn test_mem_index_page() {
        let page_id = PageId::from(100);
        let mut index_page = MemIndexPage::new(page_id);

        // Test basic properties
        assert_eq!(index_page.page_id(), page_id);
        assert!(index_page.is_leaf());
        assert_eq!(index_page.num_entries(), 0);

        // Add some entries
        let key1 = Key::from(b"key1".to_vec());
        let child1 = PageId::from(200);
        index_page.insert_child(&key1, child1);

        assert_eq!(index_page.num_entries(), 1);
    }

    #[test]
    fn test_index_navigation() {
        let page_id = PageId::from(100);
        let mut index_page = MemIndexPage::new(page_id);

        // Add multiple children
        let entries = vec![
            (Key::from(b"aaa".to_vec()), PageId::from(201)),
            (Key::from(b"bbb".to_vec()), PageId::from(202)),
            (Key::from(b"ccc".to_vec()), PageId::from(203)),
            (Key::from(b"ddd".to_vec()), PageId::from(204)),
        ];

        for (key, child) in entries {
            index_page.insert_child(&key, child);
        }

        // Test finding children
        let search_key = Key::from(b"bbb".to_vec());
        let child = index_page.find_child(&search_key);
        assert_eq!(child, Some(PageId::from(202)));
    }
}

#[cfg(test)]
mod index_stress_tests {
    use super::*;
    use rand::{Rng, thread_rng};

    #[test]
    fn stress_test_index_manager() {
        let manager = IndexPageManager::new();
        let mut rng = thread_rng();

        // Create multiple tables
        let mut tables = Vec::new();
        for i in 0..10 {
            let table_id = TableIdent::new(&format!("table_{}", i));
            tables.push(table_id);
        }

        // Random operations on tables
        for _ in 0..100 {
            let table = &tables[rng.gen_range(0..tables.len())];

            // Make COW root
            let cow_root = manager.make_cow_root(table).unwrap();

            // Simulate some operations
            let new_root = PageId::from(rng.gen::<u32>());
            let ttl_root = PageId::from(rng.gen::<u32>());

            // Install new root
            manager.install_cow_root(table, cow_root, new_root, ttl_root);
        }

        // Verify all tables exist
        for table in &tables {
            let root = manager.get_root(table);
            assert!(root.is_some());
        }
    }

    #[test]
    fn stress_test_mem_index_page() {
        let mut rng = thread_rng();

        for _ in 0..10 {
            let page_id = PageId::from(rng.gen::<u32>());
            let mut index_page = MemIndexPage::new(page_id);

            // Add random entries
            let num_entries = rng.gen_range(10..100);
            for i in 0..num_entries {
                let key = Key::from(format!("key_{:06}", i).into_bytes());
                let child = PageId::from(rng.gen::<u32>());
                index_page.insert_child(&key, child);
            }

            assert_eq!(index_page.num_entries(), num_entries);

            // Verify we can find all entries
            for i in 0..num_entries {
                let key = Key::from(format!("key_{:06}", i).into_bytes());
                assert!(index_page.find_child(&key).is_some());
            }
        }
    }
}