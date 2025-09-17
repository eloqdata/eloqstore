//! Unit tests for shard module

use eloqstore_rs::shard::{Shard, ShardId, ShardState};
use eloqstore_rs::config::KvOptions;
use eloqstore_rs::types::{TableIdent, Key, Value};
use std::sync::Arc;
use tokio::sync::mpsc;

#[cfg(test)]
mod shard_basic_tests {
    use super::*;

    #[tokio::test]
    async fn test_shard_creation() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut options = KvOptions::default();
        options.data_dir = temp_dir.path().to_path_buf();

        let shard = Shard::new(0, options).await.unwrap();
        assert_eq!(shard.id(), 0);
    }

    #[tokio::test]
    async fn test_shard_lifecycle() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut options = KvOptions::default();
        options.data_dir = temp_dir.path().to_path_buf();

        let shard = Arc::new(Shard::new(0, options).await.unwrap());

        // Initialize
        shard.init().await.unwrap();

        // Start
        let shard_clone = shard.clone();
        let handle = tokio::spawn(async move {
            shard_clone.run().await;
        });

        // Give it time to start
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Stop
        shard.stop().await;

        // Wait for thread to finish
        let _ = tokio::time::timeout(
            tokio::time::Duration::from_secs(5),
            handle
        ).await;
    }
}

#[cfg(test)]
mod shard_stress_tests {
    use super::*;
    use rand::{Rng, thread_rng};

    #[tokio::test]
    async fn stress_test_shard_operations() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut options = KvOptions::default();
        options.data_dir = temp_dir.path().to_path_buf();

        // Create multiple shards
        let num_shards = 4;
        let mut shards = Vec::new();

        for i in 0..num_shards {
            let shard = Arc::new(Shard::new(i, options.clone()).await.unwrap());
            shard.init().await.unwrap();
            shards.push(shard);
        }

        // Start all shards
        let mut handles = Vec::new();
        for shard in &shards {
            let shard_clone = shard.clone();
            let handle = tokio::spawn(async move {
                shard_clone.run().await;
            });
            handles.push(handle);
        }

        // Give them time to start
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        // Simulate operations (would need request queue implementation)
        // For now, just test lifecycle

        // Stop all shards
        for shard in &shards {
            shard.stop().await;
        }

        // Wait for all to finish
        for handle in handles {
            let _ = tokio::time::timeout(
                tokio::time::Duration::from_secs(5),
                handle
            ).await;
        }
    }
}