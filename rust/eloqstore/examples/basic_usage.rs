//! Basic Usage Example for EloqStore Rust SDK
//!
//! This example demonstrates the most common operations:
//! - Creating and starting a store
//! - Basic put/get/delete operations
//! - Batch operations
//! - Scan operations
//!
//! Run with: `cargo run --example basic_usage`

use eloqstore::{EloqStore, Options, TableIdentifier};
use std::time::{SystemTime, UNIX_EPOCH};

fn timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn main() -> Result<(), eloqstore::KvError> {
    println!("EloqStore Rust SDK - Basic Usage Example");
    println!("==========================================\n");

    // ============================================================
    // Step 1: Configure the store
    // ============================================================
    println!("1. Configuring EloqStore...");
    let mut opts = Options::new()?;
    opts.set_num_threads(1)?;
    opts.add_store_path("tmp/eloqstore_example")?;
    println!("   ✓ Store path: tmp/eloqstore_example\n");

    // ============================================================
    // Step 2: Create and start the store
    // ============================================================
    println!("2. Creating and starting the store...");
    let mut store = EloqStore::new(&opts)?;
    store.start()?;
    println!("   ✓ Store started successfully\n");

    // ============================================================
    // Step 3: Create a table
    // ============================================================
    println!("3. Creating a table...");
    let table = TableIdentifier::new("users", 0)?;
    let ts = timestamp_ms();
    println!("   ✓ Table: users (partition 0)\n");

    // ============================================================
    // Step 4: Basic operations (put/get/delete)
    // ============================================================
    println!("4. Basic operations (put/get/delete):");
    
    // Put a key-value pair
    store.put(&table, b"user:1", b"Alice", ts)?;
    println!("   ✓ put(\"user:1\", \"Alice\")");

    // Get a value
    if let Some(value) = store.get(&table, b"user:1")? {
        println!("   ✓ get(\"user:1\") -> {}", String::from_utf8_lossy(&value));
    }

    // Put more data
    store.put(&table, b"user:2", b"Bob", ts + 1)?;
    store.put(&table, b"user:3", b"Charlie", ts + 2)?;
    println!("   ✓ put(\"user:2\", \"Bob\")");
    println!("   ✓ put(\"user:3\", \"Charlie\")\n");

    // ============================================================
    // Step 5: Batch operations
    // ============================================================
    println!("5. Batch operations:");
    let keys: Vec<&[u8]> = vec![b"user:4", b"user:5", b"user:6"];
    let values: Vec<&[u8]> = vec![b"David", b"Eve", b"Frank"];
    
    store.put_batch(&table, &keys, &values, ts + 10)?;
    println!("   ✓ put_batch(3 entries)");

    // Verify batch insert
    for key in &keys {
        if let Some(value) = store.get(&table, key)? {
            println!("   ✓ get({:?}) -> {}", 
                String::from_utf8_lossy(key),
                String::from_utf8_lossy(&value));
        }
    }
    println!();

    // ============================================================
    // Step 6: Scan operations
    // ============================================================
    println!("6. Scan operations (range query):");
    let entries = store.scan(&table, b"user:1", b"user:5")?;
    println!("   ✓ scan(\"user:1\", \"user:5\") -> {} entries", entries.len());
    for entry in entries {
        println!("     - {}: {}", 
            String::from_utf8_lossy(&entry.key),
            String::from_utf8_lossy(&entry.value));
    }
    println!();

    // ============================================================
    // Step 7: Floor operation (find greatest key <= given key)
    // ============================================================
    println!("7. Floor operation:");
    if let Some((key, value)) = store.floor(&table, b"user:3")? {
        println!("   ✓ floor(\"user:3\") -> {}: {}", 
            String::from_utf8_lossy(&key),
            String::from_utf8_lossy(&value));
    }
    println!();

    // ============================================================
    // Step 8: Delete operations
    // ============================================================
    println!("8. Delete operations:");
    store.delete(&table, b"user:1", ts + 100)?;
    println!("   ✓ delete(\"user:1\")");
    
    // Verify deletion
    if store.get(&table, b"user:1")?.is_none() {
        println!("   ✓ Verified: \"user:1\" no longer exists\n");
    }

    // Batch delete
    store.delete_batch(&table, &keys, ts + 101)?;
    println!("   ✓ delete_batch(3 keys)\n");

    // ============================================================
    // Step 9: Cleanup
    // ============================================================
    println!("9. Stopping the store...");
    store.stop();
    println!("   ✓ Store stopped\n");

    println!("Example completed successfully! 🎉");
    Ok(())
}
