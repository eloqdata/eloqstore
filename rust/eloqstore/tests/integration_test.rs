use eloqstore::{EloqStore, Options, ReadRequest, ScanRequest, TableIdentifier, WriteRequest};
use eloqstore_sys;
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

fn timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn temp_path(prefix: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!("{}-{}", prefix, timestamp()));
    path
}

#[test]
fn test_all_apis() {
    println!("\nEloqStore Rust FFI Demo");
    println!("=======================\n");

    let mut opts = Options::new().expect("Failed to create options");
    opts.set_num_threads(1).expect("Failed to set num threads");
    let mut store = EloqStore::new(&opts).expect("Failed to create store");
    store.start().expect("Failed to start store");

    let ts = timestamp();
    let table = TableIdentifier::new("demo_table", 0).expect("Failed to create table");

    // ============================================================
    // API 1: Simple Rocks-style (put/get/delete)
    // ============================================================
    println!("--- Rocks-style API ---\n");

    let key = b"hello";
    let value = b"world";
    store.put(&table, key, value, ts).expect("PUT");
    println!("✓ put(key={:?}, value={:?})", key, value);

    assert!(store.exists(&table, key).expect("exists"));
    println!("✓ exists(key={:?}) -> true", key);

    let v = store.get(&table, key).expect("GET").expect("not found");
    assert_eq!(v, value);
    println!("✓ get(key={:?}) -> {:?}", key, String::from_utf8_lossy(&v));

    store.delete(&table, key, ts + 1).expect("DELETE");
    assert!(!store.exists(&table, key).expect("exists"));
    assert!(store.get(&table, key).expect("GET after DELETE").is_none());
    println!("✓ delete(key={:?})", key);

    // ============================================================
    // API 2: Batch operations
    // ============================================================
    println!("\n--- Batch operations ---\n");

    let batch_keys: Vec<&[u8]> = vec![b"k1", b"k2", b"k3"];
    let batch_values: Vec<&[u8]> = vec![b"v1", b"v2", b"v3"];
    store
        .put_batch(&table, &batch_keys, &batch_values, ts + 10)
        .expect("PUT_BATCH");
    println!("✓ put_batch(3 keys)");

    for (k, expected_v) in batch_keys.iter().zip(batch_values.iter()) {
        let v = store.get(&table, k).expect("GET").expect("not found");
        assert_eq!(v, *expected_v);
        println!("✓ get({:?}) -> {:?}", k, String::from_utf8_lossy(&v));
    }

    store
        .delete_batch(&table, &batch_keys, ts + 20)
        .expect("DELETE_BATCH");
    println!("✓ delete_batch(3 keys)");

    // ============================================================
    // API 3: Floor operation
    // ============================================================
    println!("\n--- Floor operation ---\n");

    store.put(&table, b"apple", b"1", ts).expect("PUT");
    store.put(&table, b"banana", b"2", ts).expect("PUT");
    store.put(&table, b"cherry", b"3", ts).expect("PUT");
    println!("✓ put(apple), put(banana), put(cherry)");

    let result = store
        .floor(&table, b"d")
        .expect("Floor")
        .expect("not found");
    println!(
        "✓ floor('d') -> key={:?}, value={:?}",
        result.0,
        String::from_utf8_lossy(&result.1)
    );

    // ============================================================
    // API 4: Request Trait + exec_sync (C++ polymorphism style)
    // ============================================================
    println!("\n--- Request Trait + exec_sync API ---\n");

    // Using ReadRequest
    let read_req = ReadRequest::new(table.clone(), b"nonexistent");
    let resp = store.exec_sync(read_req);
    assert!(resp.is_err() && resp.unwrap_err() == eloqstore::KvError::NotFound);
    println!("✓ exec_sync(ReadRequest{{key: nonexistent}}) -> NotFound (expected)");

    // Using WriteRequest with chain
    let write_req = WriteRequest::new(table.clone()).put(b"req_key", b"req_value", ts);
    let write_resp = store.exec_sync(write_req).expect("exec_sync");
    assert!(write_resp.success);
    println!("✓ exec_sync(WriteRequest::new().put(...)) -> success");

    // Verify with ReadRequest
    let read_req = ReadRequest::new(table.clone(), b"req_key");
    let resp = store.exec_sync(read_req).expect("exec_sync");
    assert_eq!(resp.value, b"req_value");
    println!(
        "✓ exec_sync(ReadRequest{{key: req_key}}) -> value={:?}",
        String::from_utf8_lossy(&resp.value)
    );

    // Using WriteRequest for delete
    let delete_req = WriteRequest::new(table.clone()).delete(b"req_key", ts + 1);
    store.exec_sync(delete_req).expect("exec_sync");
    println!("✓ exec_sync(WriteRequest::new().delete(...))");

    // ============================================================
    // API 5: Paginated Scan operation
    // ============================================================
    println!("\n--- Paginated Scan operation ---\n");

    // Create a new table to avoid data conflicts
    let pagination_table =
        TableIdentifier::new("pagination_table", 0).expect("Failed to create pagination table");

    // Insert data specifically for pagination testing
    println!("Inserting 100 key-value pairs for pagination test...");
    // Construct batch ascending key list
    let mut keys: Vec<String> = (0..100).map(|i| format!("key_{:03}", i)).collect();
    keys.sort();
    let mut write_req = WriteRequest::new(pagination_table.clone());
    for key in keys {
        write_req = write_req.put(
            key.as_bytes(),
            format!("value_{}", key).as_bytes(),
            ts + 3000,
        );
    }
    store.exec_sync(write_req).expect("exec_sync");
    println!("✓ Inserted 100 key-value pairs for pagination test");

    let scan_req = ScanRequest::new(pagination_table.clone())
        .range(b"key_020", b"key_080", true)
        .pagination(20, usize::MAX);
    let result = store.exec_sync(scan_req).expect("exec_sync");
    println!("✓ Scanned entries: {:?}", result.entries.len());
    for entry in result.entries {
        assert!(
            entry.key >= "key_020".as_bytes().to_vec() && entry.key < "key_080".as_bytes().to_vec()
        );
    }
    println!("✓ Scanned 20 entries from [key_020, key_080)");

    store.stop();
    println!("\n=== All API tests passed! ===");
    println!("\nSummary:");
    println!("  - Rocks-style: put(), get(), delete(), put_batch(), delete_batch()");
    println!("  - Floor: floor() for range queries");
    println!("  - Request Trait: ReadRequest, WriteRequest with exec_sync()");
    println!("  - Paginated Scan: ScanRequest with pagination support");
}

#[test]
fn test_start_with_branch_api() {
    let mut opts = Options::new().expect("Failed to create options");
    opts.set_num_threads(1).expect("Failed to set num threads");

    let mut store = EloqStore::new(&opts).expect("Failed to create store");
    store
        .start_with_branch("feature-x", 7, 3)
        .expect("Failed to start with branch");

    let table = TableIdentifier::new("branch_table", 0).expect("Failed to create table");
    let ts = timestamp();
    store.put(&table, b"hello", b"world", ts).expect("PUT");
    assert_eq!(
        store.get(&table, b"hello").expect("GET"),
        Some(b"world".to_vec())
    );
    store.stop();
}

#[test]
fn test_load_from_ini_and_disk_persistence() {
    let root = temp_path("eloqstore-rust-ini");
    let store_path = root.join("data");
    fs::create_dir_all(&store_path).expect("Failed to create store path");

    let ini_path = root.join("eloqstore.ini");
    fs::write(
        &ini_path,
        "[run]\nnum_threads = 1\nbuffer_pool_size = 4MB\n\n[permanent]\ndata_page_size = 4KB\n",
    )
    .expect("Failed to write ini file");

    let table = TableIdentifier::new("disk_table", 0).expect("Failed to create table");
    let ts = timestamp();

    let mut opts = Options::new().expect("Failed to create options");
    opts.load_from_ini(&ini_path).expect("Failed to load ini");
    opts.add_store_path(&store_path)
        .expect("Failed to add store path");

    let mut store = EloqStore::new(&opts).expect("Failed to create store");
    store.start().expect("Failed to start store");
    store.put(&table, b"persist", b"value", ts).expect("PUT");
    store.stop();

    let mut reopened_opts = Options::new().expect("Failed to create options");
    reopened_opts
        .load_from_ini(&ini_path)
        .expect("Failed to load ini");
    reopened_opts
        .add_store_path(&store_path)
        .expect("Failed to add store path");

    let mut reopened = EloqStore::new(&reopened_opts).expect("Failed to create reopened store");
    reopened.start().expect("Failed to restart store");
    assert!(reopened.exists(&table, b"persist").expect("exists"));
    assert_eq!(
        reopened.get(&table, b"persist").expect("GET"),
        Some(b"value".to_vec())
    );
    reopened.stop();
}

#[test]
fn test_c_scan_range_honors_end_inclusive() {
    eloqstore_sys::ensure_library_loaded().expect("Failed to load embedded library");

    unsafe {
        let opts = eloqstore_sys::CEloqStore_Options_Create();
        assert!(!opts.is_null(), "Failed to create options");
        eloqstore_sys::CEloqStore_Options_SetNumThreads(opts, 1);

        let store = eloqstore_sys::CEloqStore_Create(opts);
        assert!(!store.is_null(), "Failed to create store");
        assert_eq!(
            eloqstore_sys::CEloqStore_Start(store),
            eloqstore_sys::CEloqStoreStatus::Ok
        );

        let table_name = std::ffi::CString::new("scan_range_table").unwrap();
        let table = eloqstore_sys::CEloqStore_TableIdent_Create(table_name.as_ptr(), 0);
        assert!(!table.is_null(), "Failed to create table");

        let ts = timestamp();
        for key in [
            b"apple".as_slice(),
            b"banana".as_slice(),
            b"cherry".as_slice(),
        ] {
            assert_eq!(
                eloqstore_sys::CEloqStore_Put(
                    store,
                    table,
                    key.as_ptr(),
                    key.len(),
                    key.as_ptr(),
                    key.len(),
                    ts,
                ),
                eloqstore_sys::CEloqStoreStatus::Ok
            );
        }

        let mut scan_req = eloqstore_sys::CEloqStore_ScanRequest_Create();
        assert!(!scan_req.is_null(), "Failed to create scan request");
        eloqstore_sys::CEloqStore_ScanRequest_SetTable(scan_req, table);
        eloqstore_sys::CEloqStore_ScanRequest_SetRange(
            scan_req,
            b"banana".as_ptr(),
            b"banana".len(),
            true,
            b"cherry".as_ptr(),
            b"cherry".len(),
            false,
        );
        let mut scan_result: eloqstore_sys::CScanResult = std::mem::zeroed();
        assert_eq!(
            eloqstore_sys::CEloqStore_ExecScan(store, scan_req, &mut scan_result),
            eloqstore_sys::CEloqStoreStatus::Ok
        );
        assert_eq!(scan_result.num_entries, 1);
        assert!(!scan_result.entries.is_null());
        let first = *scan_result.entries;
        assert_eq!(
            std::slice::from_raw_parts(first.key, first.key_len),
            b"banana"
        );
        eloqstore_sys::CEloqStore_FreeScanResult(&mut scan_result);
        eloqstore_sys::CEloqStore_ScanRequest_Destroy(scan_req);

        scan_req = eloqstore_sys::CEloqStore_ScanRequest_Create();
        assert!(!scan_req.is_null(), "Failed to create scan request");
        eloqstore_sys::CEloqStore_ScanRequest_SetTable(scan_req, table);
        eloqstore_sys::CEloqStore_ScanRequest_SetRange(
            scan_req,
            b"banana".as_ptr(),
            b"banana".len(),
            true,
            b"cherry".as_ptr(),
            b"cherry".len(),
            true,
        );
        let mut inclusive_result: eloqstore_sys::CScanResult = std::mem::zeroed();
        assert_eq!(
            eloqstore_sys::CEloqStore_ExecScan(store, scan_req, &mut inclusive_result),
            eloqstore_sys::CEloqStoreStatus::Ok
        );
        assert_eq!(inclusive_result.num_entries, 2);
        assert!(!inclusive_result.entries.is_null());
        let keys: Vec<Vec<u8>> = (0..inclusive_result.num_entries)
            .map(|i| {
                let entry = *inclusive_result.entries.add(i);
                std::slice::from_raw_parts(entry.key, entry.key_len).to_vec()
            })
            .collect();
        assert_eq!(keys, vec![b"banana".to_vec(), b"cherry".to_vec()]);
        eloqstore_sys::CEloqStore_FreeScanResult(&mut inclusive_result);
        eloqstore_sys::CEloqStore_ScanRequest_Destroy(scan_req);

        eloqstore_sys::CEloqStore_Stop(store);
        eloqstore_sys::CEloqStore_TableIdent_Destroy(table);
        eloqstore_sys::CEloqStore_Destroy(store);
        eloqstore_sys::CEloqStore_Options_Destroy(opts);
    }
}
