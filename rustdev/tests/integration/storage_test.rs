//! Integration tests for storage operations

extern crate eloqstore_rs;

use eloqstore_rs::page::{Page, PageMapper};
use eloqstore_rs::storage::FileManager;
use eloqstore_rs::types::{PageId, FilePageId, TableIdent};
use tempfile::TempDir;
use bytes::BytesMut;

#[test]
fn test_page_mapper_integration() {
    let mapper = PageMapper::new();

    // Allocate multiple pages
    let mut pages = Vec::new();
    for _ in 0..10 {
        let page_id = mapper.allocate_page().unwrap();
        pages.push(page_id);
    }

    // Map pages to file pages
    for (i, &page_id) in pages.iter().enumerate() {
        let file_page_id = FilePageId::from((1u64 << 32) | (i as u64));
        mapper.update_mapping(page_id, file_page_id);
    }

    // Take a snapshot
    let snapshot = mapper.snapshot();

    // Verify all mappings exist
    for (i, &page_id) in pages.iter().enumerate() {
        let expected = FilePageId::from((1u64 << 32) | (i as u64));
        assert_eq!(snapshot.to_file_page(page_id).unwrap(), expected);
    }
}

#[test]
fn test_file_manager_basic() {
    let temp_dir = TempDir::new().unwrap();
    let manager = FileManager::new(temp_dir.path(), 100, 4096);

    // Create a file
    let table_id = TableIdent::new("test_table");
    let file_id = manager.create_file(&table_id).unwrap();

    // Write a page
    let mut buffer = BytesMut::zeroed(4096);
    buffer[0] = 42;
    buffer[1] = 24;
    let page = Page::from_bytes(buffer.freeze());

    manager.write_page(file_id, 0, &page).unwrap();

    // Read it back
    let read_page = manager.read_page(file_id, 0).unwrap();
    assert_eq!(read_page.as_bytes()[0], 42);
    assert_eq!(read_page.as_bytes()[1], 24);

    // Sync the file
    manager.sync_file(file_id).unwrap();
}

#[test]
fn test_file_manager_multiple_pages() {
    let temp_dir = TempDir::new().unwrap();
    let manager = FileManager::new(temp_dir.path(), 100, 4096);

    let table_id = TableIdent::new("test_table");
    let file_id = manager.create_file(&table_id).unwrap();

    // Write multiple pages
    for i in 0..10 {
        let mut buffer = BytesMut::zeroed(4096);
        buffer[0] = i as u8;
        let page = Page::from_bytes(buffer.freeze());

        manager.write_page(file_id, i as u64, &page).unwrap();
    }

    // Read them back
    for i in 0..10 {
        let page = manager.read_page(file_id, i as u64).unwrap();
        assert_eq!(page.as_bytes()[0], i as u8);
    }
}

#[test]
fn test_page_mapper_snapshot() {
    let mapper = PageMapper::new();

    // Allocate and map some pages
    let page1 = mapper.allocate_page().unwrap();
    let page2 = mapper.allocate_page().unwrap();
    let page3 = mapper.allocate_page().unwrap();

    mapper.update_mapping(page1, FilePageId::from(0x0001_0000));
    mapper.update_mapping(page2, FilePageId::from(0x0001_0001));
    mapper.update_mapping(page3, FilePageId::from(0x0001_0002));

    // Take snapshot
    let snapshot = mapper.snapshot();

    // Verify snapshot is consistent
    assert_eq!(snapshot.to_file_page(page1).unwrap(), FilePageId::from(0x0001_0000));
    assert_eq!(snapshot.to_file_page(page2).unwrap(), FilePageId::from(0x0001_0001));
    assert_eq!(snapshot.to_file_page(page3).unwrap(), FilePageId::from(0x0001_0002));

    // Modify after snapshot
    mapper.update_mapping(page1, FilePageId::from(0x0002_0000));

    // Old snapshot should be unchanged
    assert_eq!(snapshot.to_file_page(page1).unwrap(), FilePageId::from(0x0001_0000));

    // New snapshot should reflect change
    let new_snapshot = mapper.snapshot();
    assert_eq!(new_snapshot.to_file_page(page1).unwrap(), FilePageId::from(0x0002_0000));
}