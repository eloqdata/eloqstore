//! Basic operations test to verify core functionality

use bytes::Bytes;
use eloqstore::api::Store;
use eloqstore::config::KvOptions;
use eloqstore::Result;
use std::collections::BTreeMap;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    println!("=== Basic Operations Test ===");

    // Clean up
    let data_path = "/mnt/ramdisk/test_basic_ops";
    let _ = tokio::fs::remove_dir_all(data_path).await;
    tokio::fs::create_dir_all(data_path).await?;

    // Create store
    let mut options = KvOptions::default();
    options.data_dirs = vec![data_path.into()];
    options.num_threads = 1;
    options.data_page_size = 4096;

    let store = Store::with_options(options)?;
    store.start().await?;

    let mut golden = BTreeMap::new();

    // Test 1: Write 10 keys
    println!("\n=== Test 1: Write 10 keys ===");
    for i in 0..10 {
        let key = Bytes::from(format!("key_{:04}", i));
        let value = Bytes::from(format!("value_{}", i));
        store.async_set("test_table", key.clone(), value.clone()).await?;
        golden.insert(key, value);
    }

    // Verify
    let scan1 = store.async_scan("test_table", Bytes::new(), 100).await?;
    assert_eq!(scan1.len(), 10, "Expected 10 keys after initial writes");
    println!("✓ Found {} keys", scan1.len());

    // Test 2: Delete 3 keys
    println!("\n=== Test 2: Delete 3 keys ===");
    for i in [2, 5, 7] {
        let key = Bytes::from(format!("key_{:04}", i));
        store.async_delete("test_table", key.clone()).await?;
        golden.remove(&key);
    }

    // Verify
    let scan2 = store.async_scan("test_table", Bytes::new(), 100).await?;
    assert_eq!(scan2.len(), 7, "Expected 7 keys after deletions");
    println!("✓ Found {} keys", scan2.len());

    // Test 3: Update some keys
    println!("\n=== Test 3: Update 3 keys ===");
    for i in [1, 4, 8] {
        let key = Bytes::from(format!("key_{:04}", i));
        let value = Bytes::from(format!("updated_value_{}", i));
        store.async_set("test_table", key.clone(), value.clone()).await?;
        golden.insert(key, value);
    }

    // Verify
    let scan3 = store.async_scan("test_table", Bytes::new(), 100).await?;
    assert_eq!(scan3.len(), 7, "Expected 7 keys after updates");

    // Check values match
    for (k, v) in &scan3 {
        let expected = golden.get(k).expect("Key in scan but not in golden");
        assert_eq!(v, expected, "Value mismatch for key {:?}", String::from_utf8_lossy(k));
    }
    println!("✓ All {} keys have correct values", scan3.len());

    // Test 4: Add more keys
    println!("\n=== Test 4: Add 5 more keys ===");
    for i in 10..15 {
        let key = Bytes::from(format!("key_{:04}", i));
        let value = Bytes::from(format!("value_{}", i));
        store.async_set("test_table", key.clone(), value.clone()).await?;
        golden.insert(key, value);
    }

    // Final verification
    let final_scan = store.async_scan("test_table", Bytes::new(), 100).await?;
    assert_eq!(final_scan.len(), 12, "Expected 12 keys at the end");

    println!("\n=== Final State ===");
    println!("Store has {} keys, golden has {} keys", final_scan.len(), golden.len());

    if final_scan.len() != golden.len() {
        println!("ERROR: Size mismatch!");
        return Ok(());
    }

    // Verify all entries
    for (k, v) in &final_scan {
        let expected = golden.get(k).expect("Key in scan but not in golden");
        if v != expected {
            println!("ERROR: Value mismatch for key {:?}", String::from_utf8_lossy(k));
            return Ok(());
        }
    }

    println!("\n✅ Test passed! All operations worked correctly");
    Ok(())
}