//! Small stress test for debugging (100K operations on 10K keys)

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
        if self.duration_ms > 0 {
            println!("Ops/sec: {:.0}",
                self.operations as f64 / (self.duration_ms as f64 / 1000.0));
        }
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
            num_operations: 100_000,  // Reduced from 10M
            num_keys: 10_000,         // Reduced from 1M
            min_key_size: 1,
            max_key_size: 50,         // Reduced from 100
            // Mix of small, medium, and large values
            value_sizes: vec![
                1, 10, 100, 500, 1000,  // Small
                5000,                    // Medium (triggers overflow)
            ],
            batch_sizes: vec![1, 10, 100],
            operation_weights: vec![
                (Operation::Put, 30),
                (Operation::Get, 40),
                (Operation::Delete, 10),
                (Operation::Scan, 5),
                (Operation::BatchPut, 10),
                (Operation::BatchGet, 4),
                (Operation::StopRestart, 1),
            ],
            data_path: "/mnt/ramdisk/eloqstore_stress_small".to_string(),
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
    let base_key = format!("key_{:06}", key_num);
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

    // File settings
    options.pages_per_file = 64;  // Small files to trigger file operations
    options.fd_limit = 256;

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

// Run simple test
async fn run_simple_test(config: TestConfig) -> Result<Stats> {
    println!("\n🔧 Starting small stress test");
    println!("  Operations: {}", config.num_operations);
    println!("  Keys: {}", config.num_keys);
    println!("  Data path: {}", config.data_path);

    // Clean up any existing data
    let _ = tokio::fs::remove_dir_all(&config.data_path).await;
    tokio::fs::create_dir_all(&config.data_path).await?;

    // Create store and golden version
    let store_options = create_store_config(&config);
    let store = Store::with_options(store_options)?;

    // Start the store
    store.start().await?;

    let golden = Arc::new(RwLock::new(BTreeMap::new()));
    let stats = Arc::new(Mutex::new(Stats::default()));

    let mut rng = StdRng::seed_from_u64(config.seed);
    let start = Instant::now();

    for op_num in 0..config.num_operations {
        if op_num % 10_000 == 0 {
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
                store.async_set("test_table", key, value).await?;

                stats.lock().unwrap().puts += 1;
            }

            Operation::Get => {
                let key = generate_key(&mut rng, &config);
                validate_get(&store, &golden, &key, &stats).await?;
                stats.lock().unwrap().gets += 1;
            }

            Operation::Delete => {
                let key = generate_key(&mut rng, &config);

                // Update golden
                golden.write().await.remove(&key);

                // Update store
                store.async_delete("test_table", key).await?;

                stats.lock().unwrap().deletes += 1;
            }

            Operation::Scan => {
                let start_key = generate_key(&mut rng, &config);
                let limit = rng.gen_range(1..=20);

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

                for ((sk, sv), (gk, gv)) in store_results.iter().zip(golden_results.iter()) {
                    if sk != gk || sv != gv {
                        let mut s = stats.lock().unwrap();
                        s.mismatches += 1;
                        eprintln!("Scan mismatch");
                        std::process::exit(1);
                    }
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
                store.async_batch_set("test_table", batch).await?;

                stats.lock().unwrap().batch_puts += 1;
            }

            Operation::BatchGet => {
                let batch_size = *config.batch_sizes.choose(&mut rng).unwrap();
                let mut keys = Vec::new();

                for _ in 0..batch_size {
                    keys.push(generate_key(&mut rng, &config));
                }

                let results = store.async_batch_get("test_table", keys.clone()).await?;

                // Validate each result
                for (key, store_value) in keys.iter().zip(results.iter()) {
                    let golden_value = golden.read().await.get(key).cloned();
                    if store_value != &golden_value {
                        stats.lock().unwrap().mismatches += 1;
                        eprintln!("Batch get mismatch");
                        std::process::exit(1);
                    }
                }

                stats.lock().unwrap().batch_gets += 1;
            }

            Operation::StopRestart => {
                // Skip for small test
            }
        }

        stats.lock().unwrap().operations += 1;
    }

    let duration = start.elapsed();
    stats.lock().unwrap().duration_ms = duration.as_millis() as u64;

    // Final validation: scan entire store
    println!("\nPerforming final validation scan...");
    let all_entries = store.async_scan("test_table", Bytes::new(), usize::MAX).await?;
    let golden_map = golden.read().await;

    println!("Store has {} entries, golden has {} entries",
        all_entries.len(), golden_map.len());

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

    let final_stats = stats.lock().unwrap().clone();
    Ok(final_stats)
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    env_logger::init();

    println!("=== EloqStore Small Stress Test ===");
    println!("Testing with {} operations on {} keys",
             TestConfig::default().num_operations,
             TestConfig::default().num_keys);

    let config = TestConfig::default();

    let stats = run_simple_test(config).await?;
    stats.print_summary("Small Stress Test");

    println!("\n=== Test Complete ===");

    Ok(())
}