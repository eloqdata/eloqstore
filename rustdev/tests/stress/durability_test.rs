//! Durability tests - ensure data persists across crashes

use eloqstore_rs::store::EloqStore;
use eloqstore_rs::config::KvOptions;
use eloqstore_rs::types::{Key, Value, TableIdent};
use eloqstore_rs::api::request::{WriteRequest, ReadRequest};
use std::sync::Arc;

#[cfg(test)]
mod durability_tests {
    use super::*;

    #[tokio::test]
    async fn test_immediate_durability() {
        // Test that writes are immediately durable (no WAL, sync on write)
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir = temp_dir.path().to_path_buf();
        let table_id = TableIdent::new("durability_test");

        // Write data and immediately crash
        {
            let mut options = KvOptions::default();
            options.data_dir = data_dir.clone();

            let store = Arc::new(EloqStore::new(options).await.unwrap());
            store.start().await.unwrap();

            // Write critical data
            let key = Key::from(b"critical_key".to_vec());
            let value = Value::from(b"critical_value".to_vec());

            let request = WriteRequest {
                table_id: table_id.clone(),
                key: key.clone(),
                value: value.clone(),
            };

            store.write(request).await.unwrap();

            // Simulate immediate crash - no graceful shutdown
            // Just drop the store
        }

        // Recover and verify
        {
            let mut options = KvOptions::default();
            options.data_dir = data_dir.clone();

            let store = Arc::new(EloqStore::new(options).await.unwrap());
            store.start().await.unwrap();

            let key = Key::from(b"critical_key".to_vec());
            let request = ReadRequest {
                table_id: table_id.clone(),
                key: key.clone(),
            };

            let response = store.read(request).await.unwrap();
            assert_eq!(
                response.value.unwrap(),
                Value::from(b"critical_value".to_vec()),
                "Data not durable after crash!"
            );

            store.stop().await.unwrap();
        }
    }
}