//! Concurrent stress tests for EloqStore

use eloqstore_rs::store::EloqStore;
use eloqstore_rs::config::KvOptions;
use eloqstore_rs::types::{Key, Value, TableIdent};
use eloqstore_rs::api::request::{KvRequest, ReadRequest, WriteRequest, BatchWriteRequest, ScanRequest};

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::time::{Duration, Instant};
use std::collections::HashMap;
use tokio::sync::RwLock;
use rand::{Rng, thread_rng, distributions::Alphanumeric};

/// Statistics for stress test
#[derive(Debug, Default)]
pub struct StressStats {
    pub total_reads: AtomicU64,
    pub successful_reads: AtomicU64,
    pub failed_reads: AtomicU64,
    pub total_writes: AtomicU64,
    pub successful_writes: AtomicU64,
    pub failed_writes: AtomicU64,
    pub total_scans: AtomicU64,
    pub successful_scans: AtomicU64,
    pub failed_scans: AtomicU64,
}

impl StressStats {
    pub fn print_summary(&self) {
        println!("=== Stress Test Statistics ===");
        println!("Reads:  {} total, {} success, {} failed",
            self.total_reads.load(Ordering::Relaxed),
            self.successful_reads.load(Ordering::Relaxed),
            self.failed_reads.load(Ordering::Relaxed));
        println!("Writes: {} total, {} success, {} failed",
            self.total_writes.load(Ordering::Relaxed),
            self.successful_writes.load(Ordering::Relaxed),
            self.failed_writes.load(Ordering::Relaxed));
        println!("Scans:  {} total, {} success, {} failed",
            self.total_scans.load(Ordering::Relaxed),
            self.successful_scans.load(Ordering::Relaxed),
            self.failed_scans.load(Ordering::Relaxed));
    }
}

/// Configuration for stress test
pub struct StressConfig {
    pub num_threads: usize,
    pub num_operations: usize,
    pub key_range: usize,
    pub value_size_min: usize,
    pub value_size_max: usize,
    pub read_ratio: f64,
    pub write_ratio: f64,
    pub scan_ratio: f64,
    pub batch_size: usize,
    pub duration: Duration,
}

impl Default for StressConfig {
    fn default() -> Self {
        Self {
            num_threads: 8,
            num_operations: 10000,
            key_range: 1000,
            value_size_min: 100,
            value_size_max: 1000,
            read_ratio: 0.5,
            write_ratio: 0.4,
            scan_ratio: 0.1,
            batch_size: 10,
            duration: Duration::from_secs(10),
        }
    }
}

/// Generate random key in format "key_{number}"
fn generate_key(range: usize) -> Key {
    let mut rng = thread_rng();
    let key_num = rng.gen_range(0..range);
    Key::from(format!("key_{:08}", key_num).into_bytes())
}

/// Generate random value
fn generate_value(min_size: usize, max_size: usize) -> Value {
    let mut rng = thread_rng();
    let size = rng.gen_range(min_size..=max_size);
    let value: String = thread_rng()
        .sample_iter(&Alphanumeric)
        .take(size)
        .map(char::from)
        .collect();
    Value::from(value.into_bytes())
}

/// Worker thread for stress test
async fn stress_worker(
    store: Arc<EloqStore>,
    config: Arc<StressConfig>,
    stats: Arc<StressStats>,
    stop_flag: Arc<AtomicBool>,
    worker_id: usize,
) {
    let table_id = TableIdent::new("stress_test");
    let mut rng = thread_rng();
    let mut operation_count = 0;

    while !stop_flag.load(Ordering::Relaxed) && operation_count < config.num_operations {
        let op_type = rng.gen::<f64>();

        if op_type < config.read_ratio {
            // Read operation
            stats.total_reads.fetch_add(1, Ordering::Relaxed);
            let key = generate_key(config.key_range);

            let request = ReadRequest {
                table_id: table_id.clone(),
                key: key.clone(),
            };

            match store.read(request).await {
                Ok(_) => stats.successful_reads.fetch_add(1, Ordering::Relaxed),
                Err(_) => stats.failed_reads.fetch_add(1, Ordering::Relaxed),
            }

        } else if op_type < config.read_ratio + config.write_ratio {
            // Write operation
            stats.total_writes.fetch_add(1, Ordering::Relaxed);

            if rng.gen_bool(0.3) {
                // Batch write
                let mut entries = Vec::new();
                for _ in 0..config.batch_size {
                    let key = generate_key(config.key_range);
                    let value = generate_value(config.value_size_min, config.value_size_max);
                    entries.push((key, value));
                }

                let request = BatchWriteRequest {
                    table_id: table_id.clone(),
                    entries,
                };

                match store.batch_write(request).await {
                    Ok(_) => stats.successful_writes.fetch_add(1, Ordering::Relaxed),
                    Err(_) => stats.failed_writes.fetch_add(1, Ordering::Relaxed),
                }
            } else {
                // Single write
                let key = generate_key(config.key_range);
                let value = generate_value(config.value_size_min, config.value_size_max);

                let request = WriteRequest {
                    table_id: table_id.clone(),
                    key: key.clone(),
                    value: value.clone(),
                };

                match store.write(request).await {
                    Ok(_) => stats.successful_writes.fetch_add(1, Ordering::Relaxed),
                    Err(_) => stats.failed_writes.fetch_add(1, Ordering::Relaxed),
                }
            }

        } else {
            // Scan operation
            stats.total_scans.fetch_add(1, Ordering::Relaxed);

            let start_key = generate_key(config.key_range);
            let end_key = generate_key(config.key_range);

            // Ensure start < end
            let (start, end) = if start_key < end_key {
                (start_key, end_key)
            } else {
                (end_key, start_key)
            };

            let request = ScanRequest {
                table_id: table_id.clone(),
                start_key: start,
                end_key: end,
                limit: Some(100),
                reverse: rng.gen_bool(0.2),
            };

            match store.scan(request).await {
                Ok(_) => stats.successful_scans.fetch_add(1, Ordering::Relaxed),
                Err(_) => stats.failed_scans.fetch_add(1, Ordering::Relaxed),
            }
        }

        operation_count += 1;

        // Small delay to prevent overwhelming the system
        if operation_count % 100 == 0 {
            tokio::time::sleep(Duration::from_micros(100)).await;
        }
    }

    println!("Worker {} completed {} operations", worker_id, operation_count);
}

#[tokio::test]
async fn test_concurrent_stress() {
    let temp_dir = tempfile::tempdir().unwrap();
    let config = StressConfig::default();

    // Initialize store
    let mut kv_options = KvOptions::default();
    kv_options.data_dir = temp_dir.path().to_path_buf();
    kv_options.num_shards = 4;

    let store = Arc::new(EloqStore::new(kv_options).await.unwrap());
    store.start().await.unwrap();

    let stats = Arc::new(StressStats::default());
    let stop_flag = Arc::new(AtomicBool::new(false));
    let config = Arc::new(config);

    let start_time = Instant::now();

    // Spawn worker threads
    let mut workers = Vec::new();
    for i in 0..config.num_threads {
        let store = store.clone();
        let config = config.clone();
        let stats = stats.clone();
        let stop_flag = stop_flag.clone();

        let worker = tokio::spawn(async move {
            stress_worker(store, config, stats, stop_flag, i).await;
        });

        workers.push(worker);
    }

    // Run for specified duration
    tokio::time::sleep(config.duration).await;
    stop_flag.store(true, Ordering::Relaxed);

    // Wait for all workers to finish
    for worker in workers {
        let _ = worker.await;
    }

    let elapsed = start_time.elapsed();
    println!("Stress test completed in {:?}", elapsed);

    // Print statistics
    stats.print_summary();

    // Calculate throughput
    let total_ops = stats.total_reads.load(Ordering::Relaxed)
        + stats.total_writes.load(Ordering::Relaxed)
        + stats.total_scans.load(Ordering::Relaxed);

    let throughput = total_ops as f64 / elapsed.as_secs_f64();
    println!("Overall throughput: {:.2} ops/sec", throughput);

    // Shutdown store
    store.stop().await.unwrap();
}

#[tokio::test]
async fn test_consistency_under_stress() {
    let temp_dir = tempfile::tempdir().unwrap();

    // Initialize store
    let mut kv_options = KvOptions::default();
    kv_options.data_dir = temp_dir.path().to_path_buf();
    kv_options.num_shards = 2;

    let store = Arc::new(EloqStore::new(kv_options).await.unwrap());
    store.start().await.unwrap();

    let table_id = TableIdent::new("consistency_test");
    let expected_data = Arc::new(RwLock::new(HashMap::new()));

    // Phase 1: Write known data
    println!("Phase 1: Writing test data...");
    for i in 0..100 {
        let key = Key::from(format!("key_{:04}", i).into_bytes());
        let value = Value::from(format!("value_{:04}", i).into_bytes());

        expected_data.write().await.insert(key.clone(), value.clone());

        let request = WriteRequest {
            table_id: table_id.clone(),
            key,
            value,
        };

        store.write(request).await.unwrap();
    }

    // Phase 2: Concurrent reads and writes
    println!("Phase 2: Concurrent operations...");
    let mut handles = Vec::new();

    // Spawn readers
    for _ in 0..5 {
        let store = store.clone();
        let table_id = table_id.clone();
        let expected = expected_data.clone();

        let handle = tokio::spawn(async move {
            let mut rng = thread_rng();
            for _ in 0..100 {
                let key_num = rng.gen_range(0..100);
                let key = Key::from(format!("key_{:04}", key_num).into_bytes());

                let request = ReadRequest {
                    table_id: table_id.clone(),
                    key: key.clone(),
                };

                if let Ok(response) = store.read(request).await {
                    if let Some(value) = response.value {
                        let expected_map = expected.read().await;
                        if let Some(expected_value) = expected_map.get(&key) {
                            assert_eq!(value, *expected_value, "Data consistency check failed!");
                        }
                    }
                }

                tokio::time::sleep(Duration::from_micros(10)).await;
            }
        });

        handles.push(handle);
    }

    // Spawn writers
    for writer_id in 0..3 {
        let store = store.clone();
        let table_id = table_id.clone();
        let expected = expected_data.clone();

        let handle = tokio::spawn(async move {
            let mut rng = thread_rng();
            for _ in 0..50 {
                let key_num = rng.gen_range(0..100);
                let key = Key::from(format!("key_{:04}", key_num).into_bytes());
                let value = Value::from(format!("updated_value_{:04}_{}", key_num, writer_id).into_bytes());

                expected.write().await.insert(key.clone(), value.clone());

                let request = WriteRequest {
                    table_id: table_id.clone(),
                    key,
                    value,
                };

                store.write(request).await.unwrap();
                tokio::time::sleep(Duration::from_micros(20)).await;
            }
        });

        handles.push(handle);
    }

    // Wait for all operations to complete
    for handle in handles {
        handle.await.unwrap();
    }

    // Phase 3: Verify final state
    println!("Phase 3: Verifying final state...");
    let expected_map = expected_data.read().await;
    for (key, expected_value) in expected_map.iter() {
        let request = ReadRequest {
            table_id: table_id.clone(),
            key: key.clone(),
        };

        let response = store.read(request).await.unwrap();
        assert_eq!(response.value.unwrap(), *expected_value, "Final state verification failed!");
    }

    println!("Consistency test passed!");
    store.stop().await.unwrap();
}