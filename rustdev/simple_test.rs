use eloqstore_rs::EloqStore;
use eloqstore_rs::types::{TableIdent, Entry, WriteRequest};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    println!("Starting simple test...");

    let store = Arc::new(EloqStore::new(1).await?);
    let table = TableIdent {
        db: "test".to_string(),
        table: "test".to_string(),
    };

    // Test 1: Single key write
    println!("\n=== Test 1: Single key write ===");
    {
        let key = b"single_key".to_vec();
        let value = b"single_value".to_vec();

        let entry = Entry {
            key: key.clone().into(),
            value: value.clone().into(),
            ttl: None,
        };

        let req = WriteRequest {
            table_ident: table.clone(),
            entries: vec![entry],
        };

        println!("Writing single key...");
        store.batch_write(req).await?;
        println!("Write successful!");

        println!("Reading single key...");
        let read_val = store.read(&table, &key).await?;
        if let Some(val) = read_val {
            if val.as_ref() == value {
                println!("✓ Single key read successful!");
            } else {
                println!("✗ Value mismatch!");
            }
        } else {
            println!("✗ Key not found!");
        }
    }

    // Test 2: Two separate writes
    println!("\n=== Test 2: Two separate writes ===");
    {
        // First write
        let entry1 = Entry {
            key: b"key_a".to_vec().into(),
            value: b"value_a".to_vec().into(),
            ttl: None,
        };

        let req1 = WriteRequest {
            table_ident: table.clone(),
            entries: vec![entry1],
        };

        println!("Writing key_a...");
        store.batch_write(req1).await?;

        // Second write
        let entry2 = Entry {
            key: b"key_b".to_vec().into(),
            value: b"value_b".to_vec().into(),
            ttl: None,
        };

        let req2 = WriteRequest {
            table_ident: table.clone(),
            entries: vec![entry2],
        };

        println!("Writing key_b...");
        store.batch_write(req2).await?;

        // Read both keys
        println!("Reading key_a...");
        let val_a = store.read(&table, b"key_a").await?;
        if val_a.is_some() && val_a.as_ref().unwrap().as_ref() == b"value_a" {
            println!("✓ key_a read successful!");
        } else {
            println!("✗ key_a not found or wrong value!");
        }

        println!("Reading key_b...");
        let val_b = store.read(&table, b"key_b").await?;
        if val_b.is_some() && val_b.as_ref().unwrap().as_ref() == b"value_b" {
            println!("✓ key_b read successful!");
        } else {
            println!("✗ key_b not found or wrong value!");
        }
    }

    // Test 3: Multiple keys in one batch
    println!("\n=== Test 3: Multiple keys in one batch ===");
    {
        let mut entries = vec![];
        for i in 0..5 {
            let key = format!("batch_key_{:02}", i);
            let value = format!("batch_value_{:02}", i);
            entries.push(Entry {
                key: key.into_bytes().into(),
                value: value.into_bytes().into(),
                ttl: None,
            });
        }

        let req = WriteRequest {
            table_ident: table.clone(),
            entries,
        };

        println!("Writing 5 keys in batch...");
        store.batch_write(req).await?;

        // Read all keys
        let mut verified = 0;
        for i in 0..5 {
            let key = format!("batch_key_{:02}", i);
            let expected_value = format!("batch_value_{:02}", i);

            let val = store.read(&table, key.as_bytes()).await?;
            if val.is_some() && val.as_ref().unwrap().as_ref() == expected_value.as_bytes() {
                verified += 1;
            } else {
                println!("✗ {} not found or wrong value!", key);
            }
        }
        println!("{} out of 5 batch keys verified", verified);
    }

    println!("\n=== All tests complete ===");
    Ok(())
}