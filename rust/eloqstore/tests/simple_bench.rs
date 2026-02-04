//! Rust SDK version of simple_bench: simplified performance test corresponding to C++ benchmark/simple_bench.cpp
//!
//! Run (ignored by default to avoid slowing CI):
//!   cargo test simple_bench -- --ignored --nocapture
//!
//! For performance testing, use release mode:
//!   cargo test simple_bench --release -- --ignored --nocapture
//!
//! Optional environment variables (corresponding to C++ gflags):
//!   ELOQ_BENCH_KV_SIZE      Total bytes per KV pair, default 128
//!   ELOQ_BENCH_BATCH_SIZE   Number of KVs per batch, default 1024
//!   ELOQ_BENCH_WRITE_BATCHS Number of batches in write phase, default 100
//!   ELOQ_BENCH_PARTITIONS   Number of partitions, default 4
//!   ELOQ_BENCH_MAX_KEY      Maximum key value, default 100_000
//!   ELOQ_BENCH_READ_SECS    Duration of read/scan phase in seconds, default 60
//!   ELOQ_BENCH_READ_THDS    Number of read/scan threads, default 1 (corresponds to C++ FLAGS_read_thds)
//!   ELOQ_BENCH_READ_PER_PART Concurrent read/scan requests per partition, default 1 (corresponds to C++ FLAGS_read_per_part)
//!   ELOQ_BENCH_READ_STATS_INTERVAL Periodic read stats interval in seconds, default 1.0
//!   ELOQ_BENCH_SHOW_WRITE_PERF Show write performance stats (kvs/s, latency), default false
//!   ELOQ_BENCH_WRITE_STATS_INTERVAL Periodic write stats interval in seconds, default 1.0
//!   ELOQ_BENCH_WORKLOAD     write | read | scan | write-read | write-scan | load

use eloqstore::{EloqStore, Options, ScanRequest, TableIdentifier};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const TABLE_NAME: &str = "bm";

fn timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn encode_key(key: u64) -> [u8; 8] {
    key.to_be_bytes()
}


fn env_u32(name: &str, default: u32) -> u32 {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn env_workload() -> String {
    std::env::var("ELOQ_BENCH_WORKLOAD").unwrap_or_else(|_| "write-read".to_string())
}

fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .and_then(|s| match s.to_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        })
        .unwrap_or(default)
}

fn env_f64(name: &str, default: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// Write phase: multi-partition batch writes with performance stats
/// Uses multi-threading to parallelize writes across partitions (matching C++ async behavior)
fn run_write(
    store: Arc<EloqStore>,
    partitions: u32,
    batch_size: u32,
    write_batchs: u32,
    kv_size: u32,
    max_key: u64,
    load_only: bool,
    show_perf: bool,
    stats_interval_sec: f64,
) -> Result<(), eloqstore::KvError> {
    let value_len = kv_size.saturating_sub(8) as usize;
    let value: Vec<u8> = (0..value_len).map(|i| (i % 256) as u8).collect();
    let value: &[u8] = &value;

    let total_start = Instant::now();
    let key_interval = 4u64;
    let latencies = Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));
    let window_start = Arc::new(std::sync::Mutex::new(total_start));
    let last_logged_batches = Arc::new(AtomicU64::new(0));
    let completed_batches = Arc::new(AtomicU64::new(0));
    let min_window_ms = (stats_interval_sec * 1000.0).max(1.0);

    // Spawn one thread per partition for concurrent writes (matching C++ async pattern)
    let mut handles = Vec::new();
    for part in 0..partitions {
        let store = Arc::clone(&store);
        let latencies = Arc::clone(&latencies);
        let window_start = Arc::clone(&window_start);
        let last_logged_batches = Arc::clone(&last_logged_batches);
        let completed_batches = Arc::clone(&completed_batches);
        let value = value.to_vec();
        let mut writing_key = 0u64;
        let mut rng = part as u64 * 7919;

        handles.push(thread::spawn(move || -> Result<(), eloqstore::KvError> {
            for _batch_idx in 0..write_batchs {
                let tbl = TableIdentifier::new(TABLE_NAME, part)?;
                let ts = timestamp_ms();
                let mut keys: Vec<Vec<u8>> = Vec::with_capacity(batch_size as usize);
                let mut values: Vec<Vec<u8>> = Vec::with_capacity(batch_size as usize);

                for _ in 0..batch_size {
                    let k = encode_key(writing_key);
                    keys.push(k.to_vec());
                    values.push(value.clone());
                    rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
                    writing_key += if load_only {
                        1
                    } else {
                        (rng % key_interval) + 1
                    };
                    if writing_key > max_key {
                        writing_key = 0;
                    }
                }
                // C++ BatchWrite requires keys to be strictly sorted and unique
                let mut indices: Vec<usize> = (0..keys.len()).collect();
                indices.sort_by(|a, b| keys[*a].cmp(&keys[*b]));
                let mut keys_sorted: Vec<Vec<u8>> = indices.iter().map(|&i| keys[i].clone()).collect();
                let mut values_sorted: Vec<Vec<u8>> = indices.iter().map(|&i| values[i].clone()).collect();
                let mut j = 0;
                for i in 1..keys_sorted.len() {
                    if keys_sorted[i] != keys_sorted[j] {
                        j += 1;
                        if j != i {
                            keys_sorted[j] = keys_sorted[i].clone();
                            values_sorted[j] = values_sorted[i].clone();
                        }
                    }
                }
                keys_sorted.truncate(j + 1);
                values_sorted.truncate(j + 1);
                let key_refs: Vec<&[u8]> = keys_sorted.iter().map(|k| k.as_slice()).collect();
                let value_refs: Vec<&[u8]> = values_sorted.iter().map(|v| v.as_slice()).collect();

                let batch_start = Instant::now();
                store.put_batch(&tbl, &key_refs, &value_refs, ts)?;
                let batch_latency_us = batch_start.elapsed().as_micros() as u64;
                
                if show_perf {
                    latencies.lock().unwrap().push(batch_latency_us);
                }

                let current_completed = completed_batches.fetch_add(1, Ordering::Relaxed) + 1;

                // Periodic stats output (every N batches across all partitions, matching C++ behavior)
                // Only one thread should print stats (when total completed batches is a multiple of partitions)
                if show_perf && current_completed % partitions as u64 == 0 {
                    let last_logged = last_logged_batches.load(Ordering::Relaxed);
                    let mut window_start_guard = window_start.lock().unwrap();
                    let window_elapsed_ms = window_start_guard.elapsed().as_millis() as f64;
                    if window_elapsed_ms >= min_window_ms {
                        let batches_in_window = current_completed - last_logged;
                        last_logged_batches.store(current_completed, Ordering::Relaxed);
                        let num_kvs = batches_in_window * batch_size as u64;
                        let kvs_per_sec = (num_kvs as f64 * 1000.0) / window_elapsed_ms;
                        let upsert_ratio = if load_only { 1.0 } else { 0.75 };
                        let mb_per_sec = (kvs_per_sec * upsert_ratio * kv_size as f64) / (1024.0 * 1024.0);
                        println!(
                            "write speed {:.0} kvs/s | cost {:.0} ms | {:.2} MiB/s",
                            kvs_per_sec, window_elapsed_ms, mb_per_sec
                        );
                        *window_start_guard = Instant::now();
                    }
                }
            }
            Ok(())
        }));
    }

    // Wait for all threads to complete
    for h in handles {
        h.join().unwrap()?;
    }

    let elapsed = total_start.elapsed();
    let total_kvs = (write_batchs as u64) * (batch_size as u64) * (partitions as u64);
    let kvs_per_sec = total_kvs as f64 / elapsed.as_secs_f64();
    let upsert_ratio = if load_only { 1.0 } else { 0.75 };
    let mb_per_sec = (kvs_per_sec * upsert_ratio * kv_size as f64) / (1024.0 * 1024.0);

    if show_perf {
        let latencies_guard = latencies.lock().unwrap();
        if !latencies_guard.is_empty() {
            let mut sorted_latencies = latencies_guard.clone();
            sorted_latencies.sort();
            let len = sorted_latencies.len();
            let average = sorted_latencies.iter().sum::<u64>() / len as u64;
            let p50 = sorted_latencies[len * 50 / 100];
            let p90 = sorted_latencies[len * 90 / 100];
            let p99 = sorted_latencies[len * 99 / 100];
            let p999 = sorted_latencies[(len * 999 / 1000).min(len - 1)];
            let p9999 = sorted_latencies[(len * 9999 / 10000).min(len - 1)];
            let max_latency = *sorted_latencies.last().unwrap();
            println!(
                "write summary {:.0} kvs/s | cost {:.0} ms | {:.2} MiB/s | \
                 average latency {} microseconds | p50 {} microseconds | \
                 p90 {} microseconds | p99 {} microseconds | p99.9 {} microseconds | \
                 p99.99 {} microseconds | max latency {} microseconds",
                kvs_per_sec,
                elapsed.as_millis(),
                mb_per_sec,
                average,
                p50,
                p90,
                p99,
                p999,
                p9999,
                max_latency
            );
        }
    } else {
        println!(
            "[write] {} batches | {} KVs | {:.2} s | {:.0} KVs/s | {:.2} MiB/s",
            write_batchs,
            total_kvs,
            elapsed.as_secs_f64(),
            kvs_per_sec,
            mb_per_sec
        );
    }
    Ok(())
}

/// Read phase: multi-threaded random reads with periodic stats (matches C++ version)
/// Each thread creates read_per_part * partitions concurrent readers to match C++ behavior
fn run_read_multi(
    store: Arc<EloqStore>,
    partitions: u32,
    max_key: u64,
    read_secs: u64,
    read_thds: u32,
    read_per_part: u32,
    stats_interval_sec: f64,
) -> Result<(), eloqstore::KvError> {
    let stop = Arc::new(AtomicBool::new(false));
    let total_reads = Arc::new(AtomicU64::new(0));
    let latencies = Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));
    let window_start = Arc::new(std::sync::Mutex::new(Instant::now()));
    let last_logged_reads = Arc::new(AtomicU64::new(0));
    let min_window_ms = (stats_interval_sec * 1000.0).max(1.0);
    let mut handles = Vec::new();

    // Each thread creates read_per_part * partitions concurrent readers (matching C++ behavior)
    for thd_id in 0..read_thds {
        let store = Arc::clone(&store);
        let stop = Arc::clone(&stop);
        let total_reads = Arc::clone(&total_reads);
        let latencies = Arc::clone(&latencies);
        let window_start = Arc::clone(&window_start);
        let last_logged_reads = Arc::clone(&last_logged_reads);
        let num_readers = read_per_part * partitions;
        handles.push(thread::spawn(move || {
            // Create multiple concurrent readers per thread
            let mut reader_handles = Vec::new();
            for reader_id in 0..num_readers {
                let store = Arc::clone(&store);
                let stop = Arc::clone(&stop);
                let total_reads = Arc::clone(&total_reads);
                let latencies = Arc::clone(&latencies);
                let window_start = Arc::clone(&window_start);
                let last_logged_reads = Arc::clone(&last_logged_reads);
                reader_handles.push(thread::spawn(move || {
                    let mut rng = ((thd_id as u64).wrapping_mul(12345)).wrapping_add(reader_id as u64);
                    while !stop.load(Ordering::Relaxed) {
                        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
                        let key_val = rng % max_key;
                        let part = (key_val % partitions as u64) as u32;
                        let tbl = TableIdentifier::new(TABLE_NAME, part).unwrap();
                        let key = encode_key(key_val);
                        
                        let read_start = Instant::now();
                        let _ = store.get(&tbl, &key);
                        let latency_us = read_start.elapsed().as_micros() as u64;
                        
                        latencies.lock().unwrap().push(latency_us);
                        let current_total = total_reads.fetch_add(1, Ordering::Relaxed) + 1;

                        // Periodic stats output (check time window periodically, matching C++ behavior)
                        // Only check every 100 reads to avoid excessive locking overhead
                        if current_total % 100 == 0 {
                            let last_logged = last_logged_reads.load(Ordering::Relaxed);
                            let mut window_start_guard = window_start.lock().unwrap();
                            let window_elapsed_ms = window_start_guard.elapsed().as_millis() as f64;
                            if window_elapsed_ms >= min_window_ms {
                                let reads_in_window = current_total - last_logged;
                                last_logged_reads.store(current_total, Ordering::Relaxed);
                                let qps = (reads_in_window as f64 * 1000.0) / window_elapsed_ms;
                                
                                // Calculate latency statistics
                                let mut latencies_guard = latencies.lock().unwrap();
                                if !latencies_guard.is_empty() {
                                    let mut sorted_latencies = latencies_guard.clone();
                                    sorted_latencies.sort();
                                    let len = sorted_latencies.len();
                                    let average = sorted_latencies.iter().sum::<u64>() / len as u64;
                                    let p50 = sorted_latencies[len * 50 / 100];
                                    let p90 = sorted_latencies[len * 90 / 100];
                                    let p99 = sorted_latencies[len * 99 / 100];
                                    let p999 = sorted_latencies[(len * 999 / 1000).min(len - 1)];
                                    let p9999 = sorted_latencies[(len * 9999 / 10000).min(len - 1)];
                                    let max_latency = *sorted_latencies.last().unwrap();
                                    
                                    println!(
                                        "[{}]read speed {:.0} QPS | average latency {} microseconds | p50 {} microseconds | p90 {} microseconds | p99 {} microseconds | p99.9 {} microseconds | p99.99 {} microseconds | max latency {} microseconds",
                                        thd_id, qps, average, p50, p90, p99, p999, p9999, max_latency
                                    );
                                    
                                    // Clear latencies for next window
                                    latencies_guard.clear();
                                }
                                *window_start_guard = Instant::now();
                            }
                        }
                    }
                }));
            }
            // Wait for all readers in this thread to finish
            for h in reader_handles {
                let _ = h.join();
            }
        }));
    }

    thread::sleep(Duration::from_secs(read_secs));
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        let _ = h.join();
    }

    let total = total_reads.load(Ordering::Relaxed);
    let elapsed = Duration::from_secs(read_secs);
    let qps = total as f64 / elapsed.as_secs_f64();
    
    // Final summary
    let latencies_guard = latencies.lock().unwrap();
    if !latencies_guard.is_empty() {
        let mut sorted_latencies = latencies_guard.clone();
        sorted_latencies.sort();
        let len = sorted_latencies.len();
        let average = sorted_latencies.iter().sum::<u64>() / len as u64;
        let p50 = sorted_latencies[len * 50 / 100];
        let p90 = sorted_latencies[len * 90 / 100];
        let p99 = sorted_latencies[len * 99 / 100];
        let p999 = sorted_latencies[(len * 999 / 1000).min(len - 1)];
        let p9999 = sorted_latencies[(len * 9999 / 10000).min(len - 1)];
        let max_latency = *sorted_latencies.last().unwrap();
        
        println!(
            "[read] {} total reads in {} s | {:.0} QPS | average latency {} microseconds | p50 {} microseconds | p90 {} microseconds | p99 {} microseconds | p99.9 {} microseconds | p99.99 {} microseconds | max latency {} microseconds",
            total, read_secs, qps, average, p50, p90, p99, p999, p9999, max_latency
        );
    } else {
        println!(
            "[read] {} total reads in {} s | {:.0} QPS",
            total,
            read_secs,
            qps
        );
    }
    Ok(())
}

/// Scan phase: multi-threaded random range scans (matches C++ version)
fn run_scan_multi(
    store: Arc<EloqStore>,
    partitions: u32,
    max_key: u64,
    scan_secs: u64,
    page_size: usize,
    read_thds: u32,
) -> Result<(), eloqstore::KvError> {
    let stop = Arc::new(AtomicBool::new(false));
    let total_kvs = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();

    for thd_id in 0..read_thds {
        let store = Arc::clone(&store);
        let stop = Arc::clone(&stop);
        let total_kvs = Arc::clone(&total_kvs);
        handles.push(thread::spawn(move || {
            let mut rng = (thd_id as u64).wrapping_mul(98765);
            while !stop.load(Ordering::Relaxed) {
                rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
                let part = (rng % partitions as u64) as u32;
                let tbl = TableIdentifier::new(TABLE_NAME, part).unwrap();
                let start_key = rng % max_key;
                let end_key = (start_key + 256).min(max_key);
                let begin = encode_key(start_key);
                let end = encode_key(end_key);
                let req = ScanRequest::new(tbl)
                    .range(&begin, &end, true)
                    .pagination(page_size, usize::MAX);
                if let Ok(resp) = store.exec_sync(req) {
                    total_kvs.fetch_add(resp.entries.len() as u64, Ordering::Relaxed);
                }
            }
        }));
    }

    thread::sleep(Duration::from_secs(scan_secs));
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        let _ = h.join();
    }

    let kvs = total_kvs.load(Ordering::Relaxed);
    println!(
        "[scan] {} KVs in {} s | {:.0} KVs/s",
        kvs,
        scan_secs,
        kvs as f64 / scan_secs as f64
    );
    Ok(())
}

#[test]
#[ignore]
fn simple_bench() {
    let kv_size = env_u32("ELOQ_BENCH_KV_SIZE", 128);
    let batch_size = env_u32("ELOQ_BENCH_BATCH_SIZE", 1024);
    let write_batchs = env_u32("ELOQ_BENCH_WRITE_BATCHS", 100);
    let partitions = env_u32("ELOQ_BENCH_PARTITIONS", 4);
    let max_key = env_u64("ELOQ_BENCH_MAX_KEY", 1000_000);
    let read_secs = env_u64("ELOQ_BENCH_READ_SECS", 60);
    let read_thds = env_u32("ELOQ_BENCH_READ_THDS", 1);
    let read_per_part = env_u32("ELOQ_BENCH_READ_PER_PART", 1);
    let read_stats_interval = env_f64("ELOQ_BENCH_READ_STATS_INTERVAL", 1.0);
    let workload = env_workload();
    let mut write_batchs = write_batchs;
    let mut show_write_perf = env_bool("ELOQ_BENCH_SHOW_WRITE_PERF", false);
    let write_stats_interval = env_f64("ELOQ_BENCH_WRITE_STATS_INTERVAL", 1.0);

    // Handle load workload: auto-calculate write_batchs and enable perf stats
    if workload == "load" {
        let batches_per_partition = (max_key + batch_size as u64 - 1) / batch_size as u64;
        let desired_batches = batches_per_partition.max(1) * partitions as u64;
        if desired_batches > u32::MAX as u64 {
            panic!(
                "load requires write_batchs={}, which exceeds u32::MAX. Reduce max_key or batch_size.",
                desired_batches
            );
        }
        write_batchs = desired_batches as u32;
        show_write_perf = true;
        println!(
            "load=1, forcing workload=load and write_batchs set to {} ({} batches per partition to cover keys up to {})",
            write_batchs, batches_per_partition, max_key
        );
    }

    assert!(kv_size > 8, "kv_size must be > 8");
    assert!(batch_size > 0, "batch_size must be > 0");

    let dir = std::env::temp_dir().join("eloqstore_simple_bench");
    let _ = std::fs::create_dir_all(&dir);
    let path = dir.to_string_lossy();

    let mut opts = Options::new().expect("options");
    opts.set_num_threads(partitions.max(1)).expect("Failed to set num threads");
    opts.add_store_path(path.as_ref()).expect("Failed to add store path");
    let mut store = EloqStore::new(&opts).expect("store");
    store.start().expect("start");

    println!(
        "simple_bench (Rust SDK) | kv_size={} batch_size={} write_batchs={} partitions={} max_key={} workload={} read_thds={}",
        kv_size, batch_size, write_batchs, partitions, max_key, workload, read_thds
    );

    let store_arc = Arc::new(store);
    match workload.as_str() {
        "write" => {
            run_write(
                store_arc.clone(),
                partitions,
                batch_size,
                write_batchs,
                kv_size,
                max_key,
                false,
                show_write_perf,
                write_stats_interval,
            )
            .expect("write");
        }
        "load" => {
            run_write(
                store_arc.clone(),
                partitions,
                batch_size,
                write_batchs,
                kv_size,
                max_key,
                true,
                true, // Always show perf for load
                write_stats_interval,
            )
            .expect("load");
        }
        "read" => {
            run_read_multi(store_arc.clone(), partitions, max_key, read_secs, read_thds, read_per_part, read_stats_interval)
                .expect("read");
        }
        "scan" => {
            run_scan_multi(store_arc.clone(), partitions, max_key, read_secs, 256, read_thds)
                .expect("scan");
        }
        "write-read" => {
            run_write(
                store_arc.clone(),
                partitions,
                batch_size,
                write_batchs,
                kv_size,
                max_key,
                false,
                show_write_perf,
                write_stats_interval,
            )
            .expect("write");
            run_read_multi(store_arc.clone(), partitions, max_key, read_secs, read_thds, read_per_part, read_stats_interval)
                .expect("read");
        }
        "write-scan" => {
            run_write(
                store_arc.clone(),
                partitions,
                batch_size,
                write_batchs,
                kv_size,
                max_key,
                false,
                show_write_perf,
                write_stats_interval,
            )
            .expect("write");
            run_scan_multi(store_arc.clone(), partitions, max_key, read_secs, 256, read_thds)
                .expect("scan");
        }
        _ => {
            println!("unknown workload '{}', defaulting to write-read", workload);
            run_write(
                store_arc.clone(),
                partitions,
                batch_size,
                write_batchs,
                kv_size,
                max_key,
                false,
                show_write_perf,
                write_stats_interval,
            )
            .expect("write");
            run_read_multi(store_arc.clone(), partitions, max_key, read_secs, read_thds, read_per_part, read_stats_interval)
                .expect("read");
        }
    }

    // Extract store from Arc to stop it
    if let Ok(mut store) = Arc::try_unwrap(store_arc) {
        store.stop();
    }
    println!("simple_bench done");
}
