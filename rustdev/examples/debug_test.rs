//! Simple debug test for EloqStore
//!
//! Tests basic write and read operations with debug output
//!
//! Run with:
//! ```bash
//! cargo run --example debug_test
//! ```

use eloqstore::{
    config::KvOptions,
    store::EloqStore,
    types::{Key, Value},
    api::request::{ReadRequest, WriteRequest, TableIdent},
};
use std::sync::Arc;
use tokio;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    println!("=== Debug Test ===\n");

    // Configure with 1 shard for simplicity
    let mut options = KvOptions::default();
    options.data_dirs = vec![std::path::PathBuf::from("./debug_data")];
    options.num_threads = 1;
    options.data_page_size = 4096;

    println!("Creating store...");
    let mut store = EloqStore::new(options)?;

    println!("Starting store...");
    store.start().await?;
    let store = Arc::new(store);

    println!("Store started\n");

    // Wait a bit for shards to fully initialize
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    let table = TableIdent::new("test", 0);

    // Write
    println!("Writing key=test1, value=value1");
    let write_req = WriteRequest {
        table_id: table.clone(),
        key: Key::from(b"test1".to_vec()),
        value: Value::from(b"value1".to_vec()),
    };

    match store.write(write_req).await {
        Ok(_) => println!("Write successful"),
        Err(e) => println!("Write failed: {:?}", e),
    }

    // Read
    println!("\nReading key=test1");
    let read_req = ReadRequest {
        table_id: table.clone(),
        key: Key::from(b"test1".to_vec()),
        timeout: None,
    };

    match store.read(read_req).await {
        Ok(resp) => {
            if let Some(val) = resp.value {
                println!("Read successful: {}", String::from_utf8_lossy(&val));
            } else {
                println!("Key not found");
            }
        }
        Err(e) => println!("Read failed: {:?}", e),
    }

    // Wait before shutdown
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    println!("\n=== Test Complete ===");

    // Cleanup
    std::fs::remove_dir_all("./debug_data").ok();

    Ok(())
}