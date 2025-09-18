//! Comprehensive stress test for EloqStore with golden validation
//!
//! This test performs millions of operations and validates against an in-memory
//! golden version to ensure correctness under various conditions including:
//! - Large values triggering overflow pages
//! - Garbage collection
//! - Manifest saving
//! - Stop/restart cycles

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use bytes::Bytes;
use rand::{Rng, SeedableRng, seq::SliceRandom};
use rand::rngs::StdRng;
use tokio::sync::RwLock;

use eloqstore::api::Store;
use eloqstore::config::KvOptions;
use eloqstore::Result;

// Operation types for stress testing
#[derive(Debug, Clone, Copy)]
enum Operation {
    Put,
    Get,
    Delete,
    Scan,
    BatchPut,
    BatchGet,
    StopRestart,
}

// Statistics tracking
#[derive(Default, Debug, Clone)]
struct Stats {
    puts: u64,
    gets: u64,
    deletes: u64,
    scans: u64,
    batch_puts: u64,
    batch_gets: u64,
    restarts: u64,
    mismatches: u64,
    errors: u64,
    operations: u64,
    duration_ms: u64,
}

impl Stats {
    fn print_summary(&self, test_name: &str) {
        println!("\n=== {} Results ===", test_name);
        println!("Total operations: {}", self.operations);
        println!("Duration: {} ms", self.duration_ms);
        println!("Ops/sec: {:.0}",
            self.operations as f64 / (self.duration_ms as f64 / 1000.0));
        println!("\nOperation breakdown:");
        println!("  Puts: {}", self.puts);
        println!("  Gets: {}", self.gets);
        println!("  Deletes: {}", self.deletes);
        println!("  Scans: {}", self.scans);
        println!("  Batch puts: {}", self.batch_puts);
        println!("  Batch gets: {}", self.batch_gets);
        println!("  Restarts: {}", self.restarts);

        if self.mismatches > 0 {
            println!("\n❌ VALIDATION FAILED: {} mismatches", self.mismatches);
        } else {
            println!("\n✅ All operations validated successfully");
        }

        if self.errors > 0 {
            println!("⚠️  {} errors encountered", self.errors);
        }
    }
}

// Golden version: in-memory key-value store
type GoldenStore = Arc<RwLock<BTreeMap<Bytes, Bytes>>>;

// Test configuration
#[derive(Clone)]
struct TestConfig {
    num_operations: usize,
    num_keys: usize,
    min_key_size: usize,
    max_key_size: usize,
    value_sizes: Vec<usize>,
    batch_sizes: Vec<usize>,
    operation_weights: Vec<(Operation, u32)>,
    data_path: String,
    num_shards: usize,
    seed: u64,
}

impl Default for TestConfig {
    fn default() -> Self {
        Self {
            num_operations: 10_000_000,
            num_keys: 1_000_000,
            min_key_size: 1,
            max_key_size: 100,
            // Mix of small, medium, large, and very large values
            value_sizes: vec![
                1, 10, 100, 500, 1000,  // Small
                5000, 10000,             // Medium (triggers overflow)
                16384, 32768,            // Large (16KB, 32KB)
            ],
            batch_sizes: vec![1, 10, 100, 500, 1000],
            operation_weights: vec![
                (Operation::Put, 30),
                (Operation::Get, 40),
                (Operation::Delete, 10),
                (Operation::Scan, 5),
                (Operation::BatchPut, 10),
                (Operation::BatchGet, 4),
                (Operation::StopRestart, 1),
            ],
            data_path: "/mnt/ramdisk/eloqstore_stress".to_string(),
            num_shards: 1,
            seed: 42,
        }
    }
}

// Generate random key
fn generate_key(rng: &mut StdRng, config: &TestConfig) -> Bytes {
    let key_num = rng.gen_range(0..config.num_keys);
    let key_size = rng.gen_range(config.min_key_size..=config.max_key_size);
    // Always use full key prefix to maintain sorting order
    let base_key = format!("key_{:08}", key_num);
    let mut key = base_key.into_bytes();

    // Only pad, never truncate the base key (to maintain sort order)
    let min_size = key.len(); // At least the base key size
    let actual_size = key_size.max(min_size);
    if key.len() < actual_size {
        key.extend(vec![b'X'; actual_size - key.len()]);
    }

    Bytes::from(key)
}

// Generate random value
fn generate_value(rng: &mut StdRng, config: &TestConfig) -> Bytes {
    let size = *config.value_sizes.choose(rng).unwrap();
    let mut value = vec![0u8; size];

    // Fill with pattern that's verifiable
    for i in 0..size {
        value[i] = ((i % 256) as u8) ^ rng.gen::<u8>();
    }

    Bytes::from(value)
}

// Select random operation based on weights
fn select_operation(rng: &mut StdRng, weights: &[(Operation, u32)]) -> Operation {
    let total_weight: u32 = weights.iter().map(|(_, w)| w).sum();
    let mut choice = rng.gen_range(0..total_weight);

    for (op, weight) in weights {
        if choice < *weight {
            return *op;
        }
        choice -= weight;
    }

    Operation::Get // Fallback
}

// Create store configuration
fn create_store_config(test_config: &TestConfig) -> KvOptions {
    let mut options = KvOptions::default();

    // Set data path
    options.data_dirs = vec![test_config.data_path.clone().into()];

    // Configure shards/threads
    options.num_threads = test_config.num_shards;

    // Page settings
    options.data_page_size = 4096;  // 4KB pages
    options.index_page_size = 4096;

    // GC threads for testing
    options.num_gc_threads = 1;  // Enable GC

    // File settings
    options.pages_per_file = 256;  // Small files to trigger file operations
    options.fd_limit = 1024;

    // Enable archive for testing (if needed)
    if test_config.num_shards == 1 {
        options.archive_path = Some(format!("{}_archive", test_config.data_path).into());
        options.num_retained_archives = 2;
        options.archive_interval_secs = 5;  // Archive frequently for testing
    }

    options
}

// Validate store against golden version
async fn validate_get(
    store: &Store,
    golden: &GoldenStore,
    key: &Bytes,
    stats: &Arc<Mutex<Stats>>,
) -> Result<()> {
    let store_value = store.async_get("test_table", key.clone()).await?;
    let golden_value = golden.read().await.get(key).cloned();

    if store_value != golden_value {
        let mut s = stats.lock().unwrap();
        s.mismatches += 1;
        eprintln!("Mismatch for key {:?}: store={:?}, golden={:?}",
            key, store_value, golden_value);
        std::process::exit(1);  // Crash on failure as requested
    }

    Ok(())
}

// Perform scan and validate
async fn validate_scan(
    store: &Store,
    golden: &GoldenStore,
    start_key: &Bytes,
    limit: usize,
    stats: &Arc<Mutex<Stats>>,
) -> Result<()> {
    let store_results = store.async_scan("test_table", start_key.clone(), limit).await?;

    // Get golden results
    let golden_map = golden.read().await;
    let golden_results: Vec<(Bytes, Bytes)> = golden_map
        .range(start_key.clone()..)
        .take(limit)
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    if store_results.len() != golden_results.len() {
        let mut s = stats.lock().unwrap();
        s.mismatches += 1;
        eprintln!("Scan length mismatch: store={}, golden={}",
            store_results.len(), golden_results.len());
        std::process::exit(1);
    }

    for (i, ((sk, sv), (gk, gv))) in store_results.iter().zip(golden_results.iter()).enumerate() {
        if sk != gk || sv != gv {
            let mut s = stats.lock().unwrap();
            s.mismatches += 1;
            eprintln!("Scan mismatch at position {}: store=({:?},{:?}), golden=({:?},{:?})",
                i, sk, sv, gk, gv);
            std::process::exit(1);
        }
    }

    Ok(())
}

// Run single-threaded test
async fn run_single_threaded_test(config: TestConfig) -> Result<Stats> {
    println!("\n🔧 Starting single-threaded stress test");
    println!("  Operations: {}", config.num_operations);
    println!("  Keys: {}", config.num_keys);
    println!("  Data path: {}", config.data_path);

    // Clean up any existing data
    let _ = tokio::fs::remove_dir_all(&config.data_path).await;
    tokio::fs::create_dir_all(&config.data_path).await?;

    // Create store and golden version
    let store_options = create_store_config(&config);
    let mut store = Some(Store::with_options(store_options.clone())?);

    // Start the store
    if let Some(ref s) = store {
        s.start().await?;
    }

    let golden = Arc::new(RwLock::new(BTreeMap::new()));
    let stats = Arc::new(Mutex::new(Stats::default()));

    let mut rng = StdRng::seed_from_u64(config.seed);
    let start = Instant::now();

    for op_num in 0..config.num_operations {
        if op_num % 100_000 == 0 {
            println!("Progress: {}/{} operations", op_num, config.num_operations);
        }

        let operation = select_operation(&mut rng, &config.operation_weights);

        match operation {
            Operation::Put => {
                let key = generate_key(&mut rng, &config);
                let value = generate_value(&mut rng, &config);

                // Update golden
                golden.write().await.insert(key.clone(), value.clone());

                // Update store
                if let Some(ref s) = store {
                    s.async_set("test_table", key, value).await?;
                }

                stats.lock().unwrap().puts += 1;
            }

            Operation::Get => {
                let key = generate_key(&mut rng, &config);

                if let Some(ref s) = store {
                    validate_get(s, &golden, &key, &stats).await?;
                }

                stats.lock().unwrap().gets += 1;
            }

            Operation::Delete => {
                let key = generate_key(&mut rng, &config);

                // Update golden
                golden.write().await.remove(&key);

                // Update store
                if let Some(ref s) = store {
                    s.async_delete("test_table", key).await?;
                }

                stats.lock().unwrap().deletes += 1;
            }

            Operation::Scan => {
                let start_key = generate_key(&mut rng, &config);
                let limit = rng.gen_range(1..=100);

                if let Some(ref s) = store {
                    validate_scan(s, &golden, &start_key, limit, &stats).await?;
                }

                stats.lock().unwrap().scans += 1;
            }

            Operation::BatchPut => {
                let batch_size = *config.batch_sizes.choose(&mut rng).unwrap();
                let mut batch = Vec::new();

                for _ in 0..batch_size {
                    let key = generate_key(&mut rng, &config);
                    let value = generate_value(&mut rng, &config);

                    // Update golden
                    golden.write().await.insert(key.clone(), value.clone());

                    batch.push((key, value));
                }

                // Update store
                if let Some(ref s) = store {
                    s.async_batch_set("test_table", batch).await?;
                }

                stats.lock().unwrap().batch_puts += 1;
            }

            Operation::BatchGet => {
                let batch_size = *config.batch_sizes.choose(&mut rng).unwrap();
                let mut keys = Vec::new();

                for _ in 0..batch_size {
                    keys.push(generate_key(&mut rng, &config));
                }

                if let Some(ref s) = store {
                    let results = s.async_batch_get("test_table", keys.clone()).await?;

                    // Validate each result
                    for (key, store_value) in keys.iter().zip(results.iter()) {
                        let golden_value = golden.read().await.get(key).cloned();
                        if store_value != &golden_value {
                            stats.lock().unwrap().mismatches += 1;
                            eprintln!("Batch get mismatch for key {:?}", key);
                            std::process::exit(1);
                        }
                    }
                }

                stats.lock().unwrap().batch_gets += 1;
            }

            Operation::StopRestart => {
                println!("Performing stop/restart at operation {}", op_num);

                // Stop store
                if let Some(s) = store.take() {
                    drop(s);
                }

                // Small delay
                tokio::time::sleep(Duration::from_millis(100)).await;

                // Restart store
                store = Some(Store::with_options(store_options.clone())?);

                // Start the restarted store
                if let Some(ref s) = store {
                    s.start().await?;
                }

                // Validate a few random keys after restart
                for _ in 0..10 {
                    let key = generate_key(&mut rng, &config);
                    if let Some(ref s) = store {
                        validate_get(s, &golden, &key, &stats).await?;
                    }
                }

                stats.lock().unwrap().restarts += 1;
            }
        }

        stats.lock().unwrap().operations += 1;
    }

    let duration = start.elapsed();
    stats.lock().unwrap().duration_ms = duration.as_millis() as u64;

    // Final validation: scan entire store
    println!("\nPerforming final validation scan...");
    if let Some(ref s) = store {
        let all_entries = s.async_scan("test_table", Bytes::new(), usize::MAX).await?;
        let golden_map = golden.read().await;

        if all_entries.len() != golden_map.len() {
            stats.lock().unwrap().mismatches += 1;
            eprintln!("Final size mismatch: store={}, golden={}",
                all_entries.len(), golden_map.len());
            std::process::exit(1);
        }

        for ((sk, sv), (gk, gv)) in all_entries.iter().zip(golden_map.iter()) {
            if sk != gk || sv != gv {
                stats.lock().unwrap().mismatches += 1;
                eprintln!("Final validation mismatch");
                std::process::exit(1);
            }
        }
    }

    let final_stats = stats.lock().unwrap().clone();
    Ok(final_stats)
}

// Run multi-threaded test
async fn run_multi_threaded_test(mut config: TestConfig) -> Result<Stats> {
    println!("\n🔧 Starting multi-threaded stress test");
    config.num_shards = 4;  // Use 4 shards for multi-threaded test
    println!("  Operations: {}", config.num_operations);
    println!("  Keys: {}", config.num_keys);
    println!("  Shards: {}", config.num_shards);
    println!("  Data path: {}", config.data_path);

    // Clean up any existing data
    let _ = tokio::fs::remove_dir_all(&config.data_path).await;
    tokio::fs::create_dir_all(&config.data_path).await?;

    // Create store and golden version
    let store_options = create_store_config(&config);
    let store = Arc::new(Store::with_options(store_options)?);

    // Start the store
    store.start().await?;

    let golden = Arc::new(RwLock::new(BTreeMap::new()));
    let stats = Arc::new(Mutex::new(Stats::default()));

    let start = Instant::now();
    let num_threads = 4;
    let ops_per_thread = config.num_operations / num_threads;

    // Spawn worker threads
    let mut handles = Vec::new();

    for thread_id in 0..num_threads {
        let store = store.clone();
        let golden = golden.clone();
        let stats = stats.clone();
        let config = config.clone();

        let handle = tokio::spawn(async move {
            let mut rng = StdRng::seed_from_u64(config.seed + thread_id as u64);

            for op_num in 0..ops_per_thread {
                if op_num % 25_000 == 0 {
                    println!("Thread {}: {}/{} operations", thread_id, op_num, ops_per_thread);
                }

                let operation = select_operation(&mut rng, &config.operation_weights);

                match operation {
                    Operation::Put => {
                        let key = generate_key(&mut rng, &config);
                        let value = generate_value(&mut rng, &config);

                        // Update golden (with lock)
                        golden.write().await.insert(key.clone(), value.clone());

                        // Update store
                        store.async_set("test_table", key, value).await.unwrap();

                        stats.lock().unwrap().puts += 1;
                    }

                    Operation::Get => {
                        let key = generate_key(&mut rng, &config);
                        validate_get(&store, &golden, &key, &stats).await.unwrap();
                        stats.lock().unwrap().gets += 1;
                    }

                    Operation::Delete => {
                        let key = generate_key(&mut rng, &config);

                        // Update golden
                        golden.write().await.remove(&key);

                        // Update store
                        store.async_delete("test_table", key).await.unwrap();

                        stats.lock().unwrap().deletes += 1;
                    }

                    Operation::Scan => {
                        let start_key = generate_key(&mut rng, &config);
                        let limit = rng.gen_range(1..=100);

                        validate_scan(&store, &golden, &start_key, limit, &stats).await.unwrap();

                        stats.lock().unwrap().scans += 1;
                    }

                    Operation::BatchPut => {
                        let batch_size = *config.batch_sizes.choose(&mut rng).unwrap();
                        let mut batch = Vec::new();

                        for _ in 0..batch_size {
                            let key = generate_key(&mut rng, &config);
                            let value = generate_value(&mut rng, &config);

                            // Update golden
                            golden.write().await.insert(key.clone(), value.clone());

                            batch.push((key, value));
                        }

                        store.async_batch_set("test_table", batch).await.unwrap();

                        stats.lock().unwrap().batch_puts += 1;
                    }

                    Operation::BatchGet => {
                        let batch_size = *config.batch_sizes.choose(&mut rng).unwrap();
                        let mut keys = Vec::new();

                        for _ in 0..batch_size {
                            keys.push(generate_key(&mut rng, &config));
                        }

                        let results = store.async_batch_get("test_table", keys.clone()).await.unwrap();

                        // Validate each result
                        for (key, store_value) in keys.iter().zip(results.iter()) {
                            let golden_value = golden.read().await.get(key).cloned();
                            if store_value != &golden_value {
                                stats.lock().unwrap().mismatches += 1;
                                eprintln!("Batch get mismatch in thread {}", thread_id);
                                std::process::exit(1);
                            }
                        }

                        stats.lock().unwrap().batch_gets += 1;
                    }

                    Operation::StopRestart => {
                        // Skip stop/restart in multi-threaded test
                        // as it would affect all threads
                    }
                }

                stats.lock().unwrap().operations += 1;
            }
        });

        handles.push(handle);
    }

    // Wait for all threads to complete
    for handle in handles {
        handle.await.map_err(|e| eloqstore::Error::InvalidState(format!("Thread join error: {}", e)))?;
    }

    let duration = start.elapsed();
    stats.lock().unwrap().duration_ms = duration.as_millis() as u64;

    // Final validation
    println!("\nPerforming final validation scan...");
    let all_entries = store.async_scan("test_table", Bytes::new(), usize::MAX).await?;
    let golden_map = golden.read().await;

    if all_entries.len() != golden_map.len() {
        stats.lock().unwrap().mismatches += 1;
        eprintln!("Final size mismatch: store={}, golden={}",
            all_entries.len(), golden_map.len());
        std::process::exit(1);
    }

    let final_stats = stats.lock().unwrap().clone();
    Ok(final_stats)
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    env_logger::init();

    println!("=== EloqStore Stress Test ===");
    println!("Testing with 10M operations on 1M keys");

    // Test 1: Single-threaded, single shard
    let mut config = TestConfig::default();
    config.data_path = "/mnt/ramdisk/eloqstore_stress_single".to_string();

    let single_stats = run_single_threaded_test(config).await?;
    single_stats.print_summary("Single-threaded Test");

    // Test 2: Multi-threaded, multi-shard
    let mut config = TestConfig::default();
    config.data_path = "/mnt/ramdisk/eloqstore_stress_multi".to_string();

    let multi_stats = run_multi_threaded_test(config).await?;
    multi_stats.print_summary("Multi-threaded Test");

    println!("\n=== All Tests Complete ===");

    Ok(())
}