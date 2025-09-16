//! Integration tests for storage operations

use eloqstore_rs::page::{
    Page, DataPage, DataPageBuilder,
    PageMapper, OverflowChainBuilder, OverflowChainReader,
    PagePool, GlobalPagePool
};
use eloqstore_rs::storage::{FileManager, ManifestWriter, ManifestReader, find_latest_manifest};
use eloqstore_rs::types::{TableIdent, DEFAULT_PAGE_SIZE};
use tempfile::TempDir;

#[test]
fn test_page_mapper_integration() {
    let mapper = PageMapper::new();

    // Allocate and map multiple pages
    let mut page_mappings = Vec::new();
    for _ in 0..10 {
        let (page_id, file_page_id) = mapper.allocate_and_map().unwrap();
        page_mappings.push((page_id, file_page_id));
    }

    // Take a snapshot
    let snapshot = mapper.snapshot();
    assert_eq!(snapshot.len(), 10);

    // Verify all mappings exist
    for (page_id, file_page_id) in &page_mappings {
        assert_eq!(snapshot.get(*page_id), Some(*file_page_id));
    }

    // Clear and restore
    mapper.clear();
    assert_eq!(mapper.snapshot().len(), 0);

    mapper.restore(&snapshot).unwrap();
    assert_eq!(mapper.snapshot().len(), 10);
}

#[test]
fn test_file_manager_with_pages() {
    let temp_dir = TempDir::new().unwrap();
    let manager = FileManager::new(temp_dir.path(), DEFAULT_PAGE_SIZE, 10);
    manager.init().unwrap();

    let table_id = TableIdent::new("test", 1);
    let file_id = manager.create_file(&table_id).unwrap();

    // Create and write data pages
    let mut builder = DataPageBuilder::new(DEFAULT_PAGE_SIZE);

    for i in 0..50 {
        let key = format!("key_{:04}", i);
        let value = format!("value_{}", i);
        builder.add(key.as_bytes(), value.as_bytes(), i as u64, None, false);
    }

    let data_page = builder.finish(100);
    let page = data_page.into_page();

    // Write the page
    manager.write_page(file_id, 0, &page).unwrap();
    manager.sync_file(file_id).unwrap();

    // Read it back
    let read_page = manager.read_page(file_id, 0).unwrap();
    assert_eq!(read_page.content_length(), page.content_length());
    assert!(read_page.verify_checksum());

    // Create a data page from the read page
    let read_data_page = DataPage::from_page(100, read_page);
    assert!(read_data_page.restart_count() > 0);
}

#[test]
fn test_overflow_pages_with_storage() {
    let temp_dir = TempDir::new().unwrap();
    let manager = FileManager::new(temp_dir.path(), DEFAULT_PAGE_SIZE, 10);
    manager.init().unwrap();

    let table_id = TableIdent::new("test", 1);
    let file_id = manager.create_file(&table_id).unwrap();

    // Create a large value that needs overflow
    let large_value = vec![b'x'; 10000];

    let mut overflow_builder = OverflowChainBuilder::new(DEFAULT_PAGE_SIZE, 200);
    let page_ids = overflow_builder.add_data(&large_value);
    let overflow_pages = overflow_builder.finish();

    // Write overflow pages
    for (i, overflow_page) in overflow_pages.iter().enumerate() {
        let page = overflow_page.page().clone();
        manager.write_page(file_id, i as u64, &page).unwrap();
    }

    manager.sync_file(file_id).unwrap();

    // Read them back
    let mut reader = OverflowChainReader::new();
    for i in 0..overflow_pages.len() {
        let page = manager.read_page(file_id, i as u64).unwrap();
        let overflow_page = eloqstore_rs::page::OverflowPage::from_page(page_ids[i], page);
        reader.add_page(&overflow_page);
    }

    let recovered = reader.finish();
    assert_eq!(recovered.len(), large_value.len());
    assert_eq!(recovered.as_ref(), large_value.as_slice());
}

#[test]
fn test_manifest_with_mapper() {
    let temp_dir = TempDir::new().unwrap();
    let table_id = TableIdent::new("test", 1);

    let mapper = PageMapper::new();

    // Create mappings
    for _ in 0..20 {
        mapper.allocate_and_map().unwrap();
    }

    let snapshot = mapper.snapshot();

    // Write to manifest
    {
        let mut writer = ManifestWriter::new(temp_dir.path(), &table_id).unwrap();
        writer.write_snapshot(&snapshot).unwrap();
        writer.close().unwrap();
    }

    // Find and read manifest
    let manifest_path = find_latest_manifest(temp_dir.path(), &table_id)
        .unwrap()
        .unwrap();

    let mut reader = ManifestReader::open(manifest_path).unwrap();
    let restored = reader.reconstruct_snapshot().unwrap();

    assert_eq!(restored.len(), snapshot.len());
    assert_eq!(restored.version(), snapshot.version());

    // Verify all mappings match
    for (page_id, mapping) in snapshot.all_mappings() {
        assert_eq!(restored.get(*page_id), Some(mapping.file_page_id));
    }
}

#[test]
fn test_page_pool_memory_management() {
    let pool = PagePool::new(DEFAULT_PAGE_SIZE, 5);

    // Allocate pages
    let mut pages = Vec::new();
    for _ in 0..3 {
        pages.push(pool.allocate());
    }

    let stats = pool.stats();
    assert_eq!(stats.allocations, 3);
    assert_eq!(stats.misses, 3);

    // Return pages to pool
    for page in pages {
        pool.deallocate(page);
    }

    assert_eq!(pool.current_size(), 3);

    // Allocate again - should reuse
    let page1 = pool.allocate();
    let page2 = pool.allocate();

    let stats = pool.stats();
    assert_eq!(stats.allocations, 5);
    assert_eq!(stats.hits, 2);

    pool.deallocate(page1);
    pool.deallocate(page2);
}

#[test]
fn test_global_page_pool() {
    let global_pool = GlobalPagePool::new(10);

    // Test different page sizes
    let page_4k = global_pool.allocate(4096);
    let page_8k = global_pool.allocate(8192);

    assert_eq!(page_4k.size(), 4096);
    assert_eq!(page_8k.size(), 8192);

    global_pool.deallocate(page_4k);
    global_pool.deallocate(page_8k);

    // Check that we have two different pools
    let stats = global_pool.all_stats();
    assert_eq!(stats.len(), 2);
}

#[test]
fn test_complete_storage_workflow() {
    let temp_dir = TempDir::new().unwrap();
    let table_id = TableIdent::new("workflow", 1);

    // Setup
    let manager = FileManager::new(temp_dir.path(), DEFAULT_PAGE_SIZE, 10);
    manager.init().unwrap();

    let mapper = PageMapper::new();
    let pool = PagePool::new(DEFAULT_PAGE_SIZE, 10);

    // Create file
    let file_id = manager.create_file(&table_id).unwrap();
    mapper.switch_file(file_id).unwrap();

    // Write data pages
    let mut pages_written = Vec::new();
    for batch in 0..3 {
        let mut builder = DataPageBuilder::new(DEFAULT_PAGE_SIZE);

        for i in 0..20 {
            let key = format!("batch{}_key_{:04}", batch, i);
            let value = format!("value_{}", i);
            builder.add(key.as_bytes(), value.as_bytes(), i as u64, None, false);
        }

        let (page_id, file_page_id) = mapper.allocate_and_map().unwrap();
        let data_page = builder.finish(page_id);

        manager.write_page(file_id, file_page_id, data_page.page()).unwrap();
        pages_written.push((page_id, file_page_id));
    }

    // Save manifest
    {
        let mut writer = ManifestWriter::new(temp_dir.path(), &table_id).unwrap();
        writer.write_file_created(file_id, "workflow_data.dat").unwrap();
        writer.write_snapshot(&mapper.snapshot()).unwrap();
        writer.close().unwrap();
    }

    // Simulate restart - load from manifest
    let manifest_path = find_latest_manifest(temp_dir.path(), &table_id)
        .unwrap()
        .unwrap();

    let mut reader = ManifestReader::open(manifest_path).unwrap();
    let restored_snapshot = reader.reconstruct_snapshot().unwrap();

    // Verify all pages can be read
    for (page_id, file_page_id) in pages_written {
        assert_eq!(restored_snapshot.get(page_id), Some(file_page_id));

        let page = manager.read_page(file_id, file_page_id).unwrap();
        assert!(page.verify_checksum());

        let data_page = DataPage::from_page(page_id, page);
        assert!(!data_page.is_empty());
    }

    // Cleanup
    manager.delete_file(file_id).unwrap();
    let files = manager.list_files(&table_id).unwrap();
    assert_eq!(files.len(), 0);
}