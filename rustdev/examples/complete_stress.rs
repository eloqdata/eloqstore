//! Comprehensive stress test for EloqStore
//!
//! This test performs 10 million operations on 1 million keys,
//! verifying correctness against an in-memory golden version.
//!
//! Features:
//! - Mixed operations: 40% puts, 30% gets, 10% deletes, 10% batch writes, 10% scans
//! - Real-time verification against golden copy
//! - Performance metrics and latency tracking
//! - Periodic progress reports with statistics
//!
//! Run with:
//! ```bash
//! cargo run --release --example complete_stress
//! ```
//!
//! For best performance, run on a ramdisk:
//! ```bash
//! cd /mnt/ramdisk && rm -rf stress_test
//! cargo run --release --example complete_stress
//! ```

use eloqstore::{EloqStore, Error};
use eloqstore::config::KvOptions;
use eloqstore::api::{request, response};
use eloqstore::types::TableIdent;
use bytes::Bytes;
use rand::{thread_rng, Rng, distributions::Alphanumeric};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, Duration};
use std::path::PathBuf;

const NUM_KEYS: usize = 1_000_000;
const NUM_OPERATIONS: usize = 10_000_000;
const KEY_SIZE: usize = 16;
const VALUE_MIN_SIZE: usize = 50;
const VALUE_MAX_SIZE: usize = 500;
const BATCH_SIZE: usize = 100;
const PROGRESS_INTERVAL: usize = 100_000;
const VERIFICATION_BATCH: usize = 100;

/// Statistics tracking
#[derive(Debug, Default)]
struct Statistics {
    puts: AtomicU64,
    gets: AtomicU64,
    gets_found: AtomicU64,
    gets_not_found: AtomicU64,
    scans: AtomicU64,
    scan_entries: AtomicU64,
    deletes: AtomicU64,
    batch_writes: AtomicU64,

    // Verification stats
    verifications: AtomicU64,
    verification_failures: AtomicU64,

    // Error tracking
    put_errors: AtomicU64,
    get_errors: AtomicU64,
    scan_errors: AtomicU64,
    delete_errors: AtomicU64,

    // Latency tracking (in microseconds)
    total_put_latency_us: AtomicU64,
    total_get_latency_us: AtomicU64,
    total_scan_latency_us: AtomicU64,
}

impl Statistics {
    fn print_summary(&self) {
        println!("\n=== Operation Statistics ===");
        println!("Puts:     {:10} (errors: {})",
            self.puts.load(Ordering::Relaxed),
            self.put_errors.load(Ordering::Relaxed));
        println!("Gets:     {:10} (found: {}, not_found: {}, errors: {})",
            self.gets.load(Ordering::Relaxed),
            self.gets_found.load(Ordering::Relaxed),
            self.gets_not_found.load(Ordering::Relaxed),
            self.get_errors.load(Ordering::Relaxed));
        println!("Scans:    {:10} (total entries: {}, errors: {})",
            self.scans.load(Ordering::Relaxed),
            self.scan_entries.load(Ordering::Relaxed),
            self.scan_errors.load(Ordering::Relaxed));
        println!("Deletes:  {:10} (errors: {})",
            self.deletes.load(Ordering::Relaxed),
            self.delete_errors.load(Ordering::Relaxed));
        println!("Batches:  {:10}", self.batch_writes.load(Ordering::Relaxed));

        println!("\n=== Verification Statistics ===");
        println!("Total verifications: {}", self.verifications.load(Ordering::Relaxed));
        println!("Failed verifications: {}", self.verification_failures.load(Ordering::Relaxed));

        println!("\n=== Latency Statistics ===");
        let puts = self.puts.load(Ordering::Relaxed);
        let gets = self.gets.load(Ordering::Relaxed);
        let scans = self.scans.load(Ordering::Relaxed);

        if puts > 0 {
            let avg_put = self.total_put_latency_us.load(Ordering::Relaxed) / puts;
            println!("Average PUT latency: {} μs", avg_put);
        }
        if gets > 0 {
            let avg_get = self.total_get_latency_us.load(Ordering::Relaxed) / gets;
            println!("Average GET latency: {} μs", avg_get);
        }
        if scans > 0 {
            let avg_scan = self.total_scan_latency_us.load(Ordering::Relaxed) / scans;
            println!("Average SCAN latency: {} μs", avg_scan);
        }
    }
}

/// Stress test harness
struct StressTest {
    store: Arc<EloqStore>,
    golden: Arc<RwLock<HashMap<Vec<u8>, Vec<u8>>>>,
    keys: Vec<Vec<u8>>,
    stats: Arc<Statistics>,
    table_id: TableIdent,
}

impl StressTest {
    async fn new(data_dir: &str) -> Result<Self, Error> {
        // Clean up old data
        println!("Cleaning up directory: {}", data_dir);
        let _ = tokio::fs::remove_dir_all(data_dir).await;
        tokio::fs::create_dir_all(data_dir).await
            .map_err(|e| Error::IoError(e))?;
        println!("Directory cleaned and recreated");

        // Configure store
        let mut options = KvOptions::default();
        options.data_dirs = vec![PathBuf::from(data_dir)];
        options.num_threads = 4;
        options.data_page_size = 4096;
        options.index_page_size = 4096;
        options.max_write_batch_pages = 32;
        options.data_append_mode = false;
        options.fd_limit = 2048;
        options.max_index_pages = 10000;

        let mut store = EloqStore::new(options)?;
        store.start().await?;
        let store = Arc::new(store);

        // Generate all keys upfront
        let mut keys = Vec::with_capacity(NUM_KEYS);
        let mut rng = thread_rng();
        for i in 0..NUM_KEYS {
            let suffix: String = (&mut rng).sample_iter(&Alphanumeric)
                .take(KEY_SIZE - 12)
                .map(char::from)
                .collect();
            let key = format!("key_{:08}_{}", i, suffix);
            keys.push(key.into_bytes());
        }

        Ok(Self {
            store,
            golden: Arc::new(RwLock::new(HashMap::with_capacity(NUM_KEYS))),
            keys,
            stats: Arc::new(Statistics::default()),
            table_id: TableIdent {
                table_name: "stress_test".to_string(),
                partition_id: 0,
            },
        })
    }

    /// Generate a random value
    fn generate_value(&self, rng: &mut impl Rng) -> Vec<u8> {
        let size = rng.gen_range(VALUE_MIN_SIZE..VALUE_MAX_SIZE);
        rng.sample_iter(&Alphanumeric)
            .take(size)
            .map(|c| c as u8)
            .collect()
    }

    /// Perform a PUT operation
    async fn do_put(&self, rng: &mut impl Rng) -> Result<(), Error> {
        let key_idx = rng.gen_range(0..self.keys.len());
        let key = &self.keys[key_idx];
        let value = self.generate_value(rng);

        let start = Instant::now();
        let req = request::WriteRequest {
            table_id: self.table_id.clone(),
            key: Bytes::from(key.clone()),
            value: Bytes::from(value.clone()),
        };

        match self.store.write(req).await {
            Ok(_) => {
                let latency = start.elapsed().as_micros() as u64;
                self.stats.total_put_latency_us.fetch_add(latency, Ordering::Relaxed);
                self.stats.puts.fetch_add(1, Ordering::Relaxed);

                // Update golden version
                let prev = self.golden.write().unwrap().insert(key.clone(), value.clone());
                if let Some(prev_val) = prev {
                    if prev_val != value {
                        tracing::debug!("Updated key with different value: {} bytes -> {} bytes",
                                       prev_val.len(), value.len());
                    }
                }
                Ok(())
            }
            Err(e) => {
                self.stats.put_errors.fetch_add(1, Ordering::Relaxed);
                Err(e)
            }
        }
    }

    /// Perform a GET operation with verification
    async fn do_get(&self, rng: &mut impl Rng) -> Result<(), Error> {
        let key_idx = rng.gen_range(0..self.keys.len());
        let key = &self.keys[key_idx];

        let start = Instant::now();
        let req = request::ReadRequest {
            table_id: self.table_id.clone(),
            key: Bytes::from(key.clone()),
            timeout: None,
        };

        match self.store.read(req).await {
            Ok(resp) => {
                let latency = start.elapsed().as_micros() as u64;
                self.stats.total_get_latency_us.fetch_add(latency, Ordering::Relaxed);
                self.stats.gets.fetch_add(1, Ordering::Relaxed);

                // Verify against golden
                let golden = self.golden.read().unwrap();
                let expected = golden.get(key);

                match (resp.value, expected) {
                    (Some(got), Some(exp)) => {
                        self.stats.gets_found.fetch_add(1, Ordering::Relaxed);
                        self.stats.verifications.fetch_add(1, Ordering::Relaxed);

                        if got.as_ref() != exp.as_slice() {
                            eprintln!("VERIFICATION FAILED: Key {:?}", String::from_utf8_lossy(key));
                            eprintln!("  Expected: {} bytes", exp.len());
                            eprintln!("  Got: {} bytes", got.len());
                            self.stats.verification_failures.fetch_add(1, Ordering::Relaxed);

                            // Exit on verification failure for debugging
                            std::process::exit(1);
                        }
                    }
                    (None, None) => {
                        self.stats.gets_not_found.fetch_add(1, Ordering::Relaxed);
                    }
                    (Some(got), None) => {
                        eprintln!("VERIFICATION FAILED: Key {:?} found but not in golden",
                            String::from_utf8_lossy(key));
                        eprintln!("  Key index: {}", key_idx);
                        eprintln!("  Total keys: {}", self.keys.len());
                        eprintln!("  Value found: {} bytes", got.len());
                        self.stats.verification_failures.fetch_add(1, Ordering::Relaxed);
                        std::process::exit(1);
                    }
                    (None, Some(_)) => {
                        eprintln!("VERIFICATION FAILED: Key {:?} not found but exists in golden",
                            String::from_utf8_lossy(key));
                        self.stats.verification_failures.fetch_add(1, Ordering::Relaxed);
                        std::process::exit(1);
                    }
                }
                Ok(())
            }
            Err(e) => {
                if matches!(e, Error::NotFound) {
                    self.stats.gets.fetch_add(1, Ordering::Relaxed);
                    self.stats.gets_not_found.fetch_add(1, Ordering::Relaxed);

                    // Verify it shouldn't exist
                    let golden = self.golden.read().unwrap();
                    if golden.contains_key(key) {
                        eprintln!("VERIFICATION FAILED: Key {:?} returned NotFound but exists in golden",
                            String::from_utf8_lossy(key));
                        self.stats.verification_failures.fetch_add(1, Ordering::Relaxed);
                        std::process::exit(1);
                    }
                    Ok(())
                } else {
                    self.stats.get_errors.fetch_add(1, Ordering::Relaxed);
                    Err(e)
                }
            }
        }
    }

    /// Perform a DELETE operation
    async fn do_delete(&self, rng: &mut impl Rng) -> Result<(), Error> {
        let key_idx = rng.gen_range(0..self.keys.len());
        let key = &self.keys[key_idx];

        let req = request::DeleteRequest {
            table_id: self.table_id.clone(),
            key: Bytes::from(key.clone()),
        };

        match self.store.delete(req).await {
            Ok(_) => {
                self.stats.deletes.fetch_add(1, Ordering::Relaxed);

                // Update golden version
                self.golden.write().unwrap().remove(key);
                Ok(())
            }
            Err(e) => {
                self.stats.delete_errors.fetch_add(1, Ordering::Relaxed);
                Err(e)
            }
        }
    }

    /// Perform a SCAN operation with verification
    async fn do_scan(&self, rng: &mut impl Rng) -> Result<(), Error> {
        // Choose random range for scan
        let start_idx = rng.gen_range(0..self.keys.len());
        let range_size = rng.gen_range(10..200);
        let end_idx = (start_idx + range_size).min(self.keys.len());

        let start_key = &self.keys[start_idx];
        let end_key = if end_idx < self.keys.len() {
            Some(Bytes::from(self.keys[end_idx].clone()))
        } else {
            None
        };

        let limit = rng.gen_range(10..100);

        let start_time = Instant::now();
        let req = request::ScanRequest {
            table_id: self.table_id.clone(),
            start_key: Bytes::from(start_key.clone()),
            end_key: end_key.clone(),
            limit: Some(limit),
            reverse: false,
            timeout: None,
        };

        match self.store.scan(req).await {
            Ok(resp) => {
                let latency = start_time.elapsed().as_micros() as u64;
                self.stats.total_scan_latency_us.fetch_add(latency, Ordering::Relaxed);
                self.stats.scans.fetch_add(1, Ordering::Relaxed);
                self.stats.scan_entries.fetch_add(resp.entries.len() as u64, Ordering::Relaxed);

                // Build expected results from golden version
                let golden = self.golden.read().unwrap();
                let mut expected_entries = Vec::new();

                // Collect all entries in range from golden version
                for i in start_idx..end_idx {
                    if expected_entries.len() >= limit {
                        break;
                    }
                    let key = &self.keys[i];
                    if let Some(value) = golden.get(key) {
                        expected_entries.push((key.clone(), value.clone()));
                    }
                }

                // Verify scan results match expected
                if resp.entries.len() != expected_entries.len() {
                    eprintln!("SCAN VERIFICATION FAILED: Got {} entries, expected {}",
                             resp.entries.len(), expected_entries.len());
                    eprintln!("  Start key: {:?}", String::from_utf8_lossy(start_key));
                    if let Some(ref ek) = end_key {
                        eprintln!("  End key: {:?}", String::from_utf8_lossy(ek));
                    }
                    eprintln!("  Limit: {}", limit);
                    self.stats.verification_failures.fetch_add(1, Ordering::Relaxed);
                    std::process::exit(1);
                }

                // Verify each returned entry
                for (i, (key, value)) in resp.entries.iter().enumerate() {
                    self.stats.verifications.fetch_add(1, Ordering::Relaxed);

                    if i < expected_entries.len() {
                        let (expected_key, expected_value) = &expected_entries[i];

                        if key.as_ref() != expected_key.as_slice() {
                            eprintln!("SCAN VERIFICATION FAILED: Key mismatch at position {}", i);
                            eprintln!("  Got: {:?}", String::from_utf8_lossy(key));
                            eprintln!("  Expected: {:?}", String::from_utf8_lossy(expected_key));
                            self.stats.verification_failures.fetch_add(1, Ordering::Relaxed);
                            std::process::exit(1);
                        }

                        if value.as_ref() != expected_value.as_slice() {
                            eprintln!("SCAN VERIFICATION FAILED: Value mismatch for key {:?}",
                                    String::from_utf8_lossy(key));
                            self.stats.verification_failures.fetch_add(1, Ordering::Relaxed);
                            std::process::exit(1);
                        }
                    }
                }

                Ok(())
            }
            Err(e) => {
                self.stats.scan_errors.fetch_add(1, Ordering::Relaxed);
                Err(e)
            }
        }
    }

    /// Perform batch write operation
    async fn do_batch_write(&self, rng: &mut impl Rng) -> Result<(), Error> {
        let batch_size = rng.gen_range(10..BATCH_SIZE);
        let mut entries = Vec::with_capacity(batch_size);
        let mut golden_updates = Vec::with_capacity(batch_size);

        for _ in 0..batch_size {
            let key_idx = rng.gen_range(0..self.keys.len());
            let key = self.keys[key_idx].clone();

            if rng.gen_bool(0.8) {
                // 80% puts
                let value = self.generate_value(rng);
                entries.push((
                    Bytes::from(key.clone()),
                    Some(Bytes::from(value.clone())),
                ));
                golden_updates.push((key, Some(value)));
            } else {
                // 20% deletes
                entries.push((
                    Bytes::from(key.clone()),
                    None,
                ));
                golden_updates.push((key, None));
            }
        }

        // Convert entries to WriteEntry format
        let write_entries: Vec<request::WriteEntry> = entries.into_iter().map(|(key, value)| {
            if let Some(v) = value {
                request::WriteEntry {
                    key,
                    value: v,
                    op: request::WriteOp::Upsert,
                    timestamp: 0,
                    expire_ts: None,
                }
            } else {
                request::WriteEntry {
                    key,
                    value: Bytes::new(),
                    op: request::WriteOp::Delete,
                    timestamp: 0,
                    expire_ts: None,
                }
            }
        }).collect();

        let req = request::BatchWriteRequest {
            table_id: self.table_id.clone(),
            entries: write_entries,
            sync: false,
            timeout: None,
        };

        match self.store.batch_write(req).await {
            Ok(_) => {
                self.stats.batch_writes.fetch_add(1, Ordering::Relaxed);

                // Update golden version
                let mut golden = self.golden.write().unwrap();
                for (key, value) in golden_updates {
                    if let Some(v) = value {
                        golden.insert(key, v);
                    } else {
                        golden.remove(&key);
                    }
                }
                Ok(())
            }
            Err(e) => {
                self.stats.put_errors.fetch_add(batch_size as u64, Ordering::Relaxed);
                Err(e)
            }
        }
    }

    /// Verify a random sample of keys
    async fn verify_sample(&self, size: usize) -> Result<(), Error> {
        let mut rng = thread_rng();

        for _ in 0..size {
            let key_idx = rng.gen_range(0..self.keys.len());
            let key = &self.keys[key_idx];

            let req = request::ReadRequest {
                table_id: self.table_id.clone(),
                key: Bytes::from(key.clone()),
                timeout: None,
            };

            match self.store.read(req).await {
                Ok(resp) => {
                    let golden = self.golden.read().unwrap();
                    let expected = golden.get(key);

                    match (resp.value, expected) {
                        (Some(got), Some(exp)) => {
                            if got.as_ref() != exp.as_slice() {
                                eprintln!("SAMPLE VERIFICATION FAILED: Key {:?}", String::from_utf8_lossy(key));
                                self.stats.verification_failures.fetch_add(1, Ordering::Relaxed);
                                return Err(Error::Corruption("Verification failed".into()));
                            }
                        }
                        (None, None) => {}
                        _ => {
                            eprintln!("SAMPLE VERIFICATION FAILED: Existence mismatch for key {:?}",
                                String::from_utf8_lossy(key));
                            self.stats.verification_failures.fetch_add(1, Ordering::Relaxed);
                            return Err(Error::Corruption("Verification failed".into()));
                        }
                    }
                }
                Err(Error::NotFound) => {
                    let golden = self.golden.read().unwrap();
                    if golden.contains_key(key) {
                        eprintln!("SAMPLE VERIFICATION FAILED: Key should exist");
                        return Err(Error::Corruption("Verification failed".into()));
                    }
                }
                Err(e) => return Err(e),
            }
        }

        self.stats.verifications.fetch_add(size as u64, Ordering::Relaxed);
        Ok(())
    }

    /// Run the stress test
    async fn run(&self) -> Result<(), Error> {
        let mut rng = thread_rng();
        let start = Instant::now();
        let mut last_report = start;

        println!("=== Starting Stress Test ===");
        println!("Keys: {}", NUM_KEYS);
        println!("Operations: {}", NUM_OPERATIONS);
        println!("Data directory: /mnt/ramdisk/stress_test");
        println!();

        // Initial population - write some data first
        println!("Initial population phase...");
        let population_size = NUM_KEYS / 10; // Populate 10% initially
        for i in 0..population_size {
            if i % 10000 == 0 {
                print!("\rPopulating: {}/{}", i, population_size);
                use std::io::{self, Write};
                io::stdout().flush().unwrap();
            }
            if let Err(e) = self.do_put(&mut rng).await {
                eprintln!("Population error at {}: {:?}", i, e);
                return Err(e);
            }

        }
        println!("\rPopulation complete: {} keys written", population_size);
        println!();

        // Main operation loop
        for op in 0..NUM_OPERATIONS {
            // Progress report
            if op % PROGRESS_INTERVAL == 0 && op > 0 {
                let now = Instant::now();
                let total_elapsed = now.duration_since(start);
                let interval_elapsed = now.duration_since(last_report);
                last_report = now;

                let ops_per_sec = PROGRESS_INTERVAL as f64 / interval_elapsed.as_secs_f64();
                let overall_ops_per_sec = op as f64 / total_elapsed.as_secs_f64();

                println!("\n=== Progress: {}/{} operations ({:.1}%) ===",
                    op, NUM_OPERATIONS, (op as f64 / NUM_OPERATIONS as f64) * 100.0);
                println!("Current rate: {:.0} ops/sec", ops_per_sec);
                println!("Overall rate: {:.0} ops/sec", overall_ops_per_sec);
                println!("Elapsed time: {:.1}s", total_elapsed.as_secs_f64());

                self.stats.print_summary();

                // Periodic verification
                if op % (PROGRESS_INTERVAL * 5) == 0 {
                    println!("\nRunning sample verification...");
                    self.verify_sample(VERIFICATION_BATCH).await?;
                    println!("Sample verification passed!");
                }
            }

            // Choose operation based on weighted distribution
            let op_type = rng.gen_range(0..100);
            let result = match op_type {
                0..40 => self.do_put(&mut rng).await,      // 40% puts
                40..70 => self.do_get(&mut rng).await,     // 30% gets
                70..80 => self.do_delete(&mut rng).await,  // 10% deletes
                80..90 => self.do_batch_write(&mut rng).await, // 10% batch writes
                90..100 => self.do_scan(&mut rng).await,   // 10% scans
                _ => unreachable!(),
            };

            if let Err(e) = result {
                eprintln!("Operation {} failed: {}", op, e);
            }
        }

        let total_elapsed = start.elapsed();

        println!("\n\n=== STRESS TEST COMPLETE ===");
        println!("Total time: {:.2}s", total_elapsed.as_secs_f64());
        println!("Overall rate: {:.0} ops/sec",
            NUM_OPERATIONS as f64 / total_elapsed.as_secs_f64());

        self.stats.print_summary();

        // Final verification
        println!("\n=== Running Final Verification ===");
        println!("Verifying {} random keys...", VERIFICATION_BATCH * 10);
        self.verify_sample(VERIFICATION_BATCH * 10).await?;

        let failures = self.stats.verification_failures.load(Ordering::Relaxed);
        if failures > 0 {
            println!("\n❌ TEST FAILED: {} verification failures detected", failures);
            std::process::exit(1);
        } else {
            println!("\n✅ TEST PASSED: All verifications successful!");
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Enable minimal tracing
    tracing_subscriber::fmt()
        .with_env_filter("eloqstore=warn")
        .init();
    // Use ramdisk for performance
    let data_dir = if std::path::Path::new("/mnt/ramdisk").exists() {
        "/mnt/ramdisk/stress_test"
    } else {
        "/tmp/stress_test"
    };

    let test = StressTest::new(data_dir).await?;
    test.run().await?;

    Ok(())
}