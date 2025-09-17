//! Integration tests for manifest operations

extern crate eloqstore;

use eloqstore::storage::{ManifestData, ManifestFile};
use eloqstore::types::{PageId, FilePageId, TableIdent};
use eloqstore::index::CowRootMeta;
use eloqstore::page::PageMapper;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

#[tokio::test]
async fn test_manifest_save_and_load() {
    let temp_dir = TempDir::new().unwrap();
    let manifest_path = temp_dir.path().join("test_manifest.mf");

    // Create test data
    let mut mappings = HashMap::new();
    mappings.insert(PageId::from(1), FilePageId::from(0x0001_0001));
    mappings.insert(PageId::from(2), FilePageId::from(0x0001_0002));
    mappings.insert(PageId::from(3), FilePageId::from(0x0002_0001));

    let mut roots = HashMap::new();
    let table1 = TableIdent::new("table1");
    let table2 = TableIdent::new("table2");

    roots.insert(table1.clone(), CowRootMeta {
        root_id: PageId::from(100),
        ttl_root_id: PageId::from(200),
        page_mapper: Arc::new(PageMapper::new()),
        is_written: false,
        old_mapping: None,
    });

    roots.insert(table2.clone(), CowRootMeta {
        root_id: PageId::from(300),
        ttl_root_id: PageId::from(400),
        page_mapper: Arc::new(PageMapper::new()),
        is_written: false,
        old_mapping: None,
    });

    let manifest = ManifestData {
        version: 1,
        timestamp: 1234567890,
        mappings: mappings.clone(),
        roots: roots.clone(),
    };

    // Save manifest
    ManifestFile::save_to_file(&manifest, &manifest_path).await.unwrap();

    // Load it back
    let loaded = ManifestFile::load_from_file(&manifest_path).await.unwrap();

    // Verify
    assert_eq!(loaded.version, 1);
    assert_eq!(loaded.timestamp, 1234567890);
    assert_eq!(loaded.mappings.len(), 3);
    assert_eq!(loaded.roots.len(), 2);

    // Check specific mappings
    assert_eq!(loaded.mappings.get(&PageId::from(1)), Some(&FilePageId::from(0x0001_0001)));
    assert_eq!(loaded.mappings.get(&PageId::from(2)), Some(&FilePageId::from(0x0001_0002)));
    assert_eq!(loaded.mappings.get(&PageId::from(3)), Some(&FilePageId::from(0x0002_0001)));

    // Check roots
    let root1 = loaded.roots.get(&table1).unwrap();
    assert_eq!(root1.root_id, PageId::from(100));
    assert_eq!(root1.ttl_root_id, PageId::from(200));

    let root2 = loaded.roots.get(&table2).unwrap();
    assert_eq!(root2.root_id, PageId::from(300));
    assert_eq!(root2.ttl_root_id, PageId::from(400));
}