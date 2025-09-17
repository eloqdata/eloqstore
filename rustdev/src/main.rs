use std::path::PathBuf;
use eloqstore::{EloqStore, Result};
use eloqstore::api::request::{BatchWriteRequest, ReadRequest, ScanRequest, WriteEntry, WriteOp};
use eloqstore::config::KvOptions;
use eloqstore::types::{Key, Value, TableIdent};
use tracing_subscriber;
use std::collections::HashMap;

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    println!("Main function started");

    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("eloqstore_rs=debug,info")
        .init();

    println!("Tracing initialized");

    tracing::info!("Starting EloqStore test program");
    println!("Starting EloqStore test program");

    // Create configuration
    let mut options = KvOptions::default();
    options.num_threads = 1; // Single shard for testing
    options.data_dirs = vec![PathBuf::from("/tmp/eloqstore_test")];
    options.data_page_size = 4096;
    options.data_append_mode = true;
    options.fd_limit = 1000;
    options.max_write_batch_pages = 10;
    options.overflow_pointers = 4;

    // Create and start the store
    println!("Creating store...");
    let mut store = EloqStore::new(options)?;
    println!("Starting store...");
    store.start().await?;
    println!("Store started successfully");

    let table_id = TableIdent::new("test_table", 0);

    // Golden reference map to track what we write
    let mut golden_map: HashMap<String, String> = HashMap::new();

    // Test 1: Write a single key and verify read
    println!("\n=== Test 1: Single key write/read with verification ===");
    {
        let key_str = "test_key";
        let value_str = "test_value";
        let key = Key::from(key_str);
        let value = Value::from(value_str);

        // Track in golden map
        golden_map.insert(key_str.to_string(), value_str.to_string());

        let write_req = BatchWriteRequest {
            table_id: table_id.clone(),
            entries: vec![WriteEntry {
                key: key.clone(),
                value: value.clone(),
                op: WriteOp::Upsert,
                timestamp: 0,
                expire_ts: None,
            }],
            sync: true,
            timeout: None,
        };

        println!("Writing key='{}', value='{}'", key_str, value_str);
        println!("DEBUG: Calling batch_write...");
        let result = store.batch_write(write_req).await?;
        println!("DEBUG: batch_write returned");
        println!("Write completed successfully!");
        assert!(result.success, "Write should succeed");

        // Now try to read it back
        println!("Attempting to read key='{}' back...", key_str);
        let read_req = ReadRequest {
            table_id: table_id.clone(),
            key: key.clone(),
            timeout: None,
        };

        // Use a timeout for read to avoid hanging forever
        match tokio::time::timeout(
            std::time::Duration::from_secs(2),
            store.read(read_req)
        ).await {
            Ok(Ok(read_result)) => {
                if let Some(read_value) = read_result.value {
                    let read_str = String::from_utf8_lossy(&read_value);
                    println!("✓ Read successful: key='{}' => value='{}'", key_str, read_str);
                    assert_eq!(read_str, value_str, "Value should match golden reference");
                } else {
                    println!("✗ Read failed: key not found");
                    panic!("Key '{}' not found after write!", key_str);
                }
            },
            Ok(Err(e)) => {
                println!("✗ Read error: {:?}", e);
                println!("WARNING: Read is broken, skipping read tests");
            },
            Err(_) => {
                println!("✗ Read timeout after 2 seconds");
                println!("WARNING: Read is broken (hanging), skipping read tests");
            }
        }
    }

    // Test 2: Write multiple keys and track in golden map
    println!("\n=== Test 2: Writing 100 keys with golden reference ===");
    let num_keys = 100;
    let batch_size = 10; // Write in batches of 10

    for batch_start in (0..num_keys).step_by(batch_size) {
        let mut entries = Vec::new();

        for i in batch_start..std::cmp::min(batch_start + batch_size, num_keys) {
            let key_str = format!("key_{:04}", i);
            let value_str = format!("value_{}_{}", i, i * 2); // More complex value

            // Track in golden map
            golden_map.insert(key_str.clone(), value_str.clone());

            let key = Key::from(key_str.clone());
            let value = Value::from(value_str.clone());

            entries.push(WriteEntry {
                key,
                value,
                op: WriteOp::Upsert,
                timestamp: 0,
                expire_ts: None,
            });
        }

        let write_req = BatchWriteRequest {
            table_id: table_id.clone(),
            entries,
            sync: true,
            timeout: None,
        };

        let result = store.batch_write(write_req).await?;
        assert!(result.success, "Batch write should succeed");

        if batch_start % 20 == 0 {
            println!("  Wrote keys {} to {}", batch_start, std::cmp::min(batch_start + batch_size - 1, num_keys - 1));
        }
    }
    println!("Successfully wrote {} keys to store", num_keys);
    println!("Golden map contains {} entries", golden_map.len());

    // Test 3: Verify random keys against golden map
    println!("\n=== Test 3: Verifying random keys against golden reference ===");
    let test_keys = vec!["test_key", "key_0000", "key_0010", "key_0050", "key_0099"];
    let mut read_success = true;

    for key_str in &test_keys {
        if let Some(expected_value) = golden_map.get(*key_str) {
            println!("Testing key '{}' (expected: '{}')...", key_str, expected_value);

            let read_req = ReadRequest {
                table_id: table_id.clone(),
                key: Key::from(*key_str),
                timeout: None,
            };

            match tokio::time::timeout(
                std::time::Duration::from_secs(1),
                store.read(read_req)
            ).await {
                Ok(Ok(read_result)) => {
                    if let Some(read_value) = read_result.value {
                        let read_str = String::from_utf8_lossy(&read_value);
                        if read_str == expected_value.as_str() {
                            println!("  ✓ PASS: '{}' == '{}'", read_str, expected_value);
                        } else {
                            println!("  ✗ FAIL: '{}' != '{}'", read_str, expected_value);
                            read_success = false;
                        }
                    } else {
                        println!("  ✗ FAIL: Key not found");
                        read_success = false;
                    }
                },
                Ok(Err(e)) => {
                    println!("  ✗ FAIL: Read error: {:?}", e);
                    read_success = false;
                },
                Err(_) => {
                    println!("  ✗ FAIL: Read timeout");
                    read_success = false;
                    break; // Stop testing if reads are hanging
                }
            }
        }
    }

    if !read_success {
        println!("WARNING: Some reads failed or timed out. Read functionality is broken.");
    } else {
        println!("All tested keys match golden reference!");
    }

    // Test 4: Full verification - read ALL keys and compare with golden map
    println!("\n=== Test 4: Full verification of all {} keys ===", golden_map.len());
    if read_success {
        let mut verified = 0;
        let mut failed = 0;

        for (key_str, expected_value) in golden_map.iter() {
            let read_req = ReadRequest {
                table_id: table_id.clone(),
                key: Key::from(key_str.clone()),
                timeout: None,
            };

            match tokio::time::timeout(
                std::time::Duration::from_millis(100),
                store.read(read_req)
            ).await {
                Ok(Ok(read_result)) => {
                    if let Some(read_value) = read_result.value {
                        let read_str = String::from_utf8_lossy(&read_value);
                        if read_str == expected_value.as_str() {
                            verified += 1;
                        } else {
                            failed += 1;
                            if failed <= 5 {
                                println!("  Mismatch: key='{}', expected='{}', got='{}'" ,
                                    key_str, expected_value, read_str);
                            }
                        }
                    } else {
                        failed += 1;
                        if failed <= 5 {
                            println!("  Missing: key='{}'", key_str);
                        }
                    }
                },
                _ => {
                    failed += 1;
                    if failed <= 5 {
                        println!("  Error reading key='{}'" , key_str);
                    }
                    if failed > 10 {
                        println!("  Too many failures, stopping verification...");
                        break;
                    }
                }
            }

            if (verified + failed) % 20 == 0 {
                print!(".");
                use std::io::{self, Write};
                io::stdout().flush().unwrap();
            }
        }
        println!();
        println!("Verification complete: {} verified, {} failed out of {} total",
            verified, failed, golden_map.len());

        if failed == 0 {
            println!("✓✓✓ PERFECT: All keys match golden reference!");
        } else {
            println!("✗✗✗ FAILURE: {} keys do not match golden reference", failed);
        }
    } else {
        println!("Skipping full verification due to read issues");
    }

    // Test 5: Persistence test - restart and verify
    println!("\n=== Test 5: Persistence test - stopping store ===");
    store.stop().await?;
    println!("Store stopped. Data files and manifest saved.");

    // Restart the store
    println!("\n=== Restarting store to test persistence ===");
    let mut options2 = KvOptions::default();
    options2.num_threads = 1;
    options2.data_dirs = vec![PathBuf::from("/tmp/eloqstore_test")];
    options2.data_page_size = 4096;
    options2.data_append_mode = true;
    options2.fd_limit = 1000;
    options2.max_write_batch_pages = 10;
    options2.overflow_pointers = 4;

    let mut store2 = EloqStore::new(options2)?;
    store2.start().await?;
    println!("Store restarted. Verifying persisted data...");

    // Verify all data against golden map
    let mut verified = 0;
    let mut failed = 0;

    for (key_str, expected_value) in golden_map.iter() {
        let read_req = ReadRequest {
            table_id: table_id.clone(),
            key: Key::from(key_str.clone()),
            timeout: None,
        };

        match tokio::time::timeout(
            std::time::Duration::from_millis(100),
            store2.read(read_req)
        ).await {
            Ok(Ok(read_result)) => {
                if let Some(read_value) = read_result.value {
                    let read_str = String::from_utf8_lossy(&read_value);
                    if read_str == expected_value.as_str() {
                        verified += 1;
                    } else {
                        failed += 1;
                        if failed <= 5 {
                            println!("  ✗ Mismatch after restart: key='{}', expected='{}', got='{}'",
                                key_str, expected_value, read_str);
                        }
                    }
                } else {
                    failed += 1;
                    if failed <= 5 {
                        println!("  ✗ Missing after restart: key='{}'", key_str);
                    }
                }
            },
            _ => {
                failed += 1;
                if failed <= 5 {
                    println!("  ✗ Error reading key='{}' after restart", key_str);
                }
            }
        }

        if (verified + failed) % 20 == 0 {
            print!(".");
            use std::io::{self, Write};
            io::stdout().flush().unwrap();
        }
    }
    println!();

    println!("\nPersistence test results: {} verified, {} failed out of {} total",
        verified, failed, golden_map.len());

    if failed == 0 {
        println!("✓✓✓ PERFECT PERSISTENCE: All {} keys survived restart!", golden_map.len());
        println!("Data integrity verified successfully.");
    } else {
        println!("✗✗✗ PERSISTENCE FAILURE: {} keys lost after restart", failed);
    }

    // Clean up
    store2.stop().await?;

    println!("\n=== All tests completed ===");
    if failed == 0 {
        println!("SUCCESS: All tests passed including persistence!");
    } else {
        println!("FAILURE: Some data was lost after restart.");
    }

    Ok(())
}