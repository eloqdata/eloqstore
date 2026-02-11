//! Cloud Storage Example for EloqStore Rust SDK
//!
//! This example demonstrates how to configure and use EloqStore with
//! S3-compatible cloud storage as the primary storage backend.
//!
//! Run with: `cargo run --example cloud_storage`

use eloqstore::{EloqStore, Options, TableIdentifier};
use std::time::{SystemTime, UNIX_EPOCH};

fn timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn main() -> Result<(), eloqstore::KvError> {
    println!("EloqStore Cloud Storage Example");
    println!("===============================\n");

    // ============================================================
    // Configuration for MinIO (local S3-compatible storage)
    // ============================================================
    println!("Configuring EloqStore with MinIO...");

    let mut opts = Options::new()?;

    // Basic configuration
    opts.set_num_threads(4).expect("Failed to set num threads");
    opts.add_store_path("/tmp/eloqstore_cloud_cache").expect("Failed to add store path");
    opts.set_data_append_mode(true); // Required for cloud storage
    opts.set_enable_compression(true);

    // Cloud storage configuration
    opts.set_cloud_store_path("eloqstore/example_bucket").expect("Failed to set cloud store path");
    opts.set_cloud_provider("aws").expect("Failed to set cloud provider");
    opts.set_cloud_region("us-east-1").expect("Failed to set cloud region");
    opts.set_cloud_credentials("minioadmin", "minioadmin").expect("Failed to set cloud credentials");
    opts.set_cloud_verify_ssl(false);

    // Note: The following cloud options are not yet exposed in the C API:
    // - cloud_endpoint: "http://localhost:9000" (MinIO endpoint)
    // - max_cloud_concurrency: 20
    // - cloud_request_threads: 2
    // - prewarm_cloud_cache: true
    // - local_space_limit: 10 * 1024 * 1024 * 1024 (10GB)

    // Validate options
    if !opts.validate() {
        eprintln!("Warning: Options validation failed");
    }

    println!("Options configured successfully");
    println!("  - Cloud path: eloqstore/example_bucket");
    println!("  - Provider: aws (S3-compatible)");
    println!("  - Region: us-east-1");
    println!("  - Local cache: /tmp/eloqstore_cloud_cache");
    println!();

    // ============================================================
    // Create and start the store
    // ============================================================
    println!("Creating EloqStore instance...");
    let mut store = EloqStore::new(&opts)?;
    store.start()?;
    println!("Store started successfully\n");

    // ============================================================
    // Define tables
    // ============================================================
    let user_table = TableIdentifier::new("users", 0)?;
    let log_table = TableIdentifier::new("logs", 0)?;

    let ts = timestamp();

    // ============================================================
    // Basic operations with cloud storage
    // ============================================================
    println!("Performing operations with cloud storage backend...");

    // Write data (will be persisted to cloud)
    store.put(&user_table, b"user:1", b"Alice", ts)?;
    store.put(&user_table, b"user:2", b"Bob", ts + 1)?;
    store.put(&user_table, b"user:3", b"Charlie", ts + 2)?;

    println!("Wrote 3 users to cloud storage");

    // Read data (may come from local cache or cloud)
    if let Some(value) = store.get(&user_table, b"user:1")? {
        println!("Read user:1 = {}", String::from_utf8_lossy(&value));
    }

    // Batch operations
    let keys :Vec<&[u8]> = vec![b"log:1", b"log:2", b"log:3"];
    let values :Vec<&[u8]> = vec![b"error: connection failed", b"info: user logged in", b"debug: cache hit"];

    store.put_batch(&log_table, &keys, &values, ts + 10)?;
    println!("Wrote 3 logs in batch to cloud storage");

    // Scan operations
    let entries = store.scan(&log_table, b"log:1", b"log:4")?;
    println!("Scanned {} log entries from cloud storage", entries.len());

    for entry in entries {
        println!(
            "  - {}: {}",
            String::from_utf8_lossy(&entry.key),
            String::from_utf8_lossy(&entry.value)
        );
    }

    // ============================================================
    // Demonstrate cloud storage benefits
    // ============================================================
    println!("\nCloud Storage Benefits:");
    println!("1. Data durability: 11 9's durability from object storage");
    println!("2. Unlimited scalability: Storage grows with your data");
    println!("3. Cost efficiency: Pay only for what you use");
    println!("4. High availability: Multi-AZ redundancy");
    println!("5. Local caching: Hot data stays in local SSD cache");

    // ============================================================
    // Cleanup
    // ============================================================
    println!("\nCleaning up...");

    // Delete test data
    store.delete(&user_table, b"user:1", ts + 100)?;
    store.delete(&user_table, b"user:2", ts + 100)?;
    store.delete(&user_table, b"user:3", ts + 100)?;

    println!("Deleted test data from cloud storage");

    // Note: In a real application, you would typically:
    // 1. Keep the store running
    // 2. Handle graceful shutdown
    // 3. Implement proper error handling
    // 4. Use connection pooling for production

    println!("\nExample completed successfully!");
    println!("Note: To run this example with actual cloud storage:");
    println!("1. Start MinIO: ./minio server /tmp/minio-data");
    println!("2. Create bucket: mc mb myminio/eloqstore");
    println!("3. Update cloud_store_path to 'eloqstore/test-bucket'");

    Ok(())
}
