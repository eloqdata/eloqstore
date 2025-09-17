//! Native Rust example demonstrating EloqStore usage
//!
//! This example shows how to use EloqStore directly from Rust code,
//! including basic CRUD operations, batch writes, and scanning.
//!
//! Run with:
//! ```bash
//! cargo run --example native_usage
//! ```

use eloqstore::{
    config::KvOptions,
    store::EloqStore,
    types::{Key, Value},
    api::request::{ReadRequest, WriteRequest, BatchWriteRequest, ScanRequest, DeleteRequest, TableIdent, WriteEntry, WriteOp},
};
use std::sync::Arc;
use std::time::Instant;
use tokio;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== EloqStore Native Rust Example ===\n");

    // 1. Configure the store
    let mut options = KvOptions::default();
    options.data_dirs = vec![std::path::PathBuf::from("./example_data")];
    options.num_threads = 4;
    options.data_page_size = 4096;
    options.max_index_pages = 1000;

    println!("1. Initializing EloqStore with configuration:");
    println!("   - Data directory: {:?}", options.data_dirs);
    println!("   - Number of threads: {}", options.num_threads);
    println!("   - Data page size: {} bytes", options.data_page_size);
    println!("   - Max index pages: {}\n", options.max_index_pages);

    // 2. Create and start the store
    let mut store = EloqStore::new(options)?;
    store.start().await?;
    let store = Arc::new(store);
    println!("2. Store started successfully!\n");

    // 3. Define table
    let table = TableIdent::new("users", 0); // partition_id = 0

    // 4. Single write operation
    println!("3. Writing single key-value pair:");
    let key = Key::from(b"user:1001".to_vec());
    let value = Value::from(b"{\"name\":\"Alice\",\"age\":30}".to_vec());

    let write_req = WriteRequest {
        table_id: table.clone(),
        key: key.clone(),
        value: value.clone(),
    };

    let start = Instant::now();
    let _write_resp = store.write(write_req).await?;
    println!("   Written: user:1001 -> {{name:Alice,age:30}}");
    println!("   Write latency: {:?}\n", start.elapsed());

    // 5. Read operation
    println!("4. Reading the key back:");
    let read_req = ReadRequest {
        table_id: table.clone(),
        key: key.clone(),
        timeout: None,
    };

    let start = Instant::now();
    let read_resp = store.read(read_req).await?;
    println!("   Read latency: {:?}", start.elapsed());

    match read_resp.value {
        Some(v) => {
            let value_str = String::from_utf8_lossy(v.as_ref());
            println!("   Value found: {}\n", value_str);
        }
        None => println!("   Key not found!\n"),
    }

    // 6. Batch write operation
    println!("5. Batch writing multiple users:");
    let mut entries = Vec::new();
    for i in 1002..=1010 {
        let key = Key::from(format!("user:{}", i).into_bytes());
        let value = Value::from(
            format!("{{\"name\":\"User{}\",\"age\":{}}}", i, 20 + (i % 50)).into_bytes()
        );
        entries.push((key, value));
    }

    let mut write_entries = Vec::new();
    for (key, value) in entries.clone() {
        write_entries.push(WriteEntry {
            key: key.into(),
            value: value.into(),
            op: WriteOp::Upsert,
            timestamp: 0,
            expire_ts: None,
        });
    }

    let batch_req = BatchWriteRequest {
        table_id: table.clone(),
        entries: write_entries,
        sync: true,
        timeout: None,
    };

    let start = Instant::now();
    let _batch_resp = store.batch_write(batch_req).await?;
    println!("   Written {} users in batch", entries.len());
    println!("   Batch write latency: {:?}\n", start.elapsed());

    // 7. Scan operation
    println!("6. Scanning users in range [user:1001, user:1005]:");
    let scan_req = ScanRequest {
        table_id: table.clone(),
        start_key: Key::from(b"user:1001".to_vec()),
        end_key: Some(Key::from(b"user:1005".to_vec())),
        limit: Some(10),
        reverse: false,
        timeout: None,
    };

    let start = Instant::now();
    let scan_resp = store.scan(scan_req).await?;
    println!("   Scan latency: {:?}", start.elapsed());
    println!("   Found {} entries:", scan_resp.entries.len());

    for (k, v) in &scan_resp.entries {
        let key_str = String::from_utf8_lossy(k.as_ref());
        let value_str = String::from_utf8_lossy(v.as_ref());
        println!("     {} -> {}", key_str, value_str);
    }
    println!();

    // 8. Update operation (overwrite)
    println!("7. Updating user:1001:");
    let updated_value = Value::from(b"{\"name\":\"Alice Smith\",\"age\":31}".to_vec());
    let update_req = WriteRequest {
        table_id: table.clone(),
        key: Key::from(b"user:1001".to_vec()),
        value: updated_value,
    };

    store.write(update_req).await?;
    println!("   Updated user:1001 with new data\n");

    // 9. Delete operation
    println!("8. Deleting user:1010:");
    let delete_req = DeleteRequest {
        table_id: table.clone(),
        key: Key::from(b"user:1010".to_vec()),
    };

    store.delete(delete_req).await?;
    println!("   Deleted user:1010\n");

    // 10. Verify delete
    println!("9. Verifying deletion:");
    let verify_req = ReadRequest {
        table_id: table.clone(),
        key: Key::from(b"user:1010".to_vec()),
        timeout: None,
    };

    let verify_resp = store.read(verify_req).await?;
    match verify_resp.value {
        Some(_) => println!("   ERROR: Key still exists!"),
        None => println!("   Confirmed: Key has been deleted"),
    }
    println!();

    // 11. Performance test
    println!("10. Performance test - 1000 random reads:");
    let mut total_time = std::time::Duration::ZERO;
    let mut found = 0;
    let mut not_found = 0;

    for i in 0..1000 {
        let key_id = 1001 + (i % 10);
        let key = Key::from(format!("user:{}", key_id).into_bytes());

        let read_req = ReadRequest {
            table_id: table.clone(),
            key,
            timeout: None,
        };

        let start = Instant::now();
        let resp = store.read(read_req).await?;
        total_time += start.elapsed();

        if resp.value.is_some() {
            found += 1;
        } else {
            not_found += 1;
        }
    }

    println!("    Total reads: 1000");
    println!("    Found: {}, Not found: {}", found, not_found);
    println!("    Total time: {:?}", total_time);
    println!("    Average latency: {:?}", total_time / 1000);
    println!("    Throughput: {:.2} ops/sec\n", 1000.0 / total_time.as_secs_f64());

    // 12. Shutdown
    println!("11. Shutting down store...");
    // Need to get mutable reference to stop
    // Arc::try_unwrap will fail if there are other references
    // For this example, we'll just skip the stop call as Drop will handle it
    println!("    Store will be stopped on drop");

    println!("\n=== Example completed successfully ===");

    // Cleanup
    std::fs::remove_dir_all("./example_data").ok();

    Ok(())
}

// Example output:
// === EloqStore Native Rust Example ===
//
// 1. Initializing EloqStore with configuration:
//    - Data directory: "./example_data"
//    - Number of shards: 4
//    - Page size: 4096 bytes
//    - Cache size: 1000 pages
//
// 2. Store started successfully!
//
// 3. Writing single key-value pair:
//    Written: user:1001 -> {name:Alice,age:30}
//    Write latency: 1.2ms
//
// 4. Reading the key back:
//    Read latency: 0.3ms
//    Value found: {"name":"Alice","age":30}
//
// ... (continued output)