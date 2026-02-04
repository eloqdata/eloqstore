use eloqstore::{EloqStore, Options, ReadRequest, ScanRequest, TableIdentifier, WriteRequest};
use std::time::{SystemTime, UNIX_EPOCH};

fn timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[test]
fn test_all_apis() {
    println!("\nEloqStore Rust FFI Demo");
    println!("=======================\n");

    let mut opts = Options::new().expect("Failed to create options");
    opts.set_num_threads(1).expect("Failed to set num threads");
    opts.add_store_path("tmp/eloqstore_demo").expect("Failed to add store path");
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

    let v = store.get(&table, key).expect("GET").expect("not found");
    assert_eq!(v, value);
    println!("✓ get(key={:?}) -> {:?}", key, String::from_utf8_lossy(&v));

    store.delete(&table, key, ts + 1).expect("DELETE");
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
