//! Rust SDK simple_bench: Simplified performance test corresponding to C++ benchmark/simple_bench.cpp.
//!
//! How to run (disabled by default to avoid slowing down CI):
//!   cargo test simple_bench -- --ignored --nocapture
//!
//! For performance testing, use release mode:
//!   cargo test simple_bench --release -- --ignored --nocapture
//!
//! Optional environment variables (corresponding to C++ gflags):
//!   ELOQ_BENCH_KV_SIZE      Total KV pair bytes, default 128
//!   ELOQ_BENCH_BATCH_SIZE   Number of KVs per batch, default 1024
//!   ELOQ_BENCH_WRITE_BATCHS Write phase batch count, default 100
//!   ELOQ_BENCH_PARTITIONS   Number of partitions, default 4
//!   ELOQ_BENCH_MAX_KEY      Key space upper limit, default 100_000
//!   ELOQ_BENCH_READ_SECS    Read phase seconds, default 5
//!   ELOQ_BENCH_WORKLOAD     write | read | scan | write-read | write-scan

use eloqstore::{EloqStore, Options, ScanRequest, TableIdentifier};
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

/// Write phase: Multi-partition batch writes
fn run_write(
    store: &EloqStore,
    partitions: u32,
    batch_size: u32,
    write_batchs: u32,
    kv_size: u32,
    max_key: u64,
    load_only: bool,
) -> Result<(), eloqstore::KvError> {
    let value_len = kv_size.saturating_sub(8) as usize;
    let value: Vec<u8> = (0..value_len).map(|i| (i % 256) as u8).collect();
    let value: &[u8] = &value;

    let total_start = Instant::now();
    let mut written_batches = 0u64;
    let key_interval = 4u64;
    let mut writing_key_per_part: Vec<u64> = (0..partitions as usize).map(|_| 0).collect();

    for _batch_idx in 0..write_batchs {
        for part in 0..partitions {
            let tbl = TableIdentifier::new(TABLE_NAME, part)?;
            let ts = timestamp_ms();
            let mut keys: Vec<Vec<u8>> = Vec::with_capacity(batch_size as usize);
            let mut values: Vec<Vec<u8>> = Vec::with_capacity(batch_size as usize);
            let w = &mut writing_key_per_part[part as usize];
            let mut rng = part as u64 * 7919;

            for _ in 0..batch_size {
                let k = encode_key(*w);
                keys.push(k.to_vec());
                values.push(value.to_vec());
                rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
                *w += if load_only {
                    1
                } else {
                    (rng % key_interval) + 1
                };
                if *w > max_key {
                    *w = 0;
                }
            }
            // C++ BatchWrite requires keys to be strictly ordered and unique, so we sort and deduplicate
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
            store.put_batch(&tbl, &key_refs, &value_refs, ts)?;
        }
        written_batches += 1;
    }

    let elapsed = total_start.elapsed();
    let total_kvs = (write_batchs as u64) * (batch_size as u64) * (partitions as u64);
    let kvs_per_sec = total_kvs as f64 / elapsed.as_secs_f64();
    let mb_per_sec = (kvs_per_sec * kv_size as f64) / (1024.0 * 1024.0);
    println!(
        "[write] {} batches | {} KVs | {:.2} s | {:.0} KVs/s | {:.2} MiB/s",
        written_batches,
        total_kvs,
        elapsed.as_secs_f64(),
        kvs_per_sec,
        mb_per_sec
    );
    Ok(())
}

/// Read phase: Single-threaded random reads (Rust SDK currently lacks a simple way to share store across threads)
fn run_read_single(
    store: &EloqStore,
    partitions: u32,
    max_key: u64,
    read_secs: u64,
    _read_thds: u32,
) -> Result<(), eloqstore::KvError> {
    // Single-threaded read version to avoid cloning store
    let start = Instant::now();
    let mut rng = 12345u64;
    let mut reads: u64 = 0;
    while start.elapsed() < Duration::from_secs(read_secs) {
        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
        let key_val = rng % max_key;
        let part = (key_val % partitions as u64) as u32;
        let tbl = TableIdentifier::new(TABLE_NAME, part)?;
        let key = encode_key(key_val);
        let _ = store.get(&tbl, &key)?;
        reads += 1;
    }
    let elapsed = start.elapsed().as_secs_f64();
    println!("[read] {} reads in {:.2} s | {:.0} QPS", reads, elapsed, reads as f64 / elapsed);
    Ok(())
}

/// Scan phase: Single-threaded random range scans
fn run_scan_single(
    store: &EloqStore,
    partitions: u32,
    max_key: u64,
    scan_secs: u64,
    page_size: usize,
) -> Result<(), eloqstore::KvError> {
    let start = Instant::now();
    let mut rng = 98765u64;
    let mut total_kvs: u64 = 0;
    while start.elapsed() < Duration::from_secs(scan_secs) {
        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
        let part = (rng % partitions as u64) as u32;
        let tbl = TableIdentifier::new(TABLE_NAME, part)?;
        let start_key = rng % max_key;
        let end_key = (start_key + 256).min(max_key);
        let begin = encode_key(start_key);
        let end = encode_key(end_key);
        let req = ScanRequest::new(tbl)
            .range(&begin, &end, true)
            .pagination(page_size, usize::MAX);
        let resp = store.exec_sync(req)?;
        total_kvs += resp.entries.len() as u64;
    }
    let elapsed = start.elapsed().as_secs_f64();
    println!(
        "[scan] {} KVs in {:.2} s | {:.0} KVs/s",
        total_kvs,
        elapsed,
        total_kvs as f64 / elapsed
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
    let max_key = env_u64("ELOQ_BENCH_MAX_KEY", 100_000);
    let read_secs = env_u64("ELOQ_BENCH_READ_SECS", 5);
    let workload = env_workload();

    assert!(kv_size > 8, "kv_size must be > 8");
    assert!(batch_size > 0, "batch_size must be > 0");

    let dir = std::env::temp_dir().join("eloqstore_simple_bench");
    let _ = std::fs::create_dir_all(&dir);
    let path = dir.to_string_lossy();

    let mut opts = Options::new().expect("options");
    opts.set_num_threads(partitions.max(1));
    opts.add_store_path(path.as_ref());
    let mut store = EloqStore::new(&opts).expect("store");
    store.start().expect("start");

    println!(
        "simple_bench (Rust SDK) | kv_size={} batch_size={} write_batchs={} partitions={} max_key={} workload={}",
        kv_size, batch_size, write_batchs, partitions, max_key, workload
    );

    match workload.as_str() {
        "write" => {
            run_write(&store, partitions, batch_size, write_batchs, kv_size, max_key, false)
                .expect("write");
        }
        "load" => {
            run_write(&store, partitions, batch_size, write_batchs, kv_size, max_key, true)
                .expect("load");
        }
        "read" => {
            run_read_single(&store, partitions, max_key, read_secs, 1).expect("read");
        }
        "scan" => {
            run_scan_single(&store, partitions, max_key, read_secs, 256).expect("scan");
        }
        "write-read" => {
            run_write(&store, partitions, batch_size, write_batchs, kv_size, max_key, false)
                .expect("write");
            run_read_single(&store, partitions, max_key, read_secs, 1).expect("read");
        }
        "write-scan" => {
            run_write(&store, partitions, batch_size, write_batchs, kv_size, max_key, false)
                .expect("write");
            run_scan_single(&store, partitions, max_key, read_secs, 256).expect("scan");
        }
        _ => {
            println!("unknown workload '{}', defaulting to write-read", workload);
            run_write(&store, partitions, batch_size, write_batchs, kv_size, max_key, false)
                .expect("write");
            run_read_single(&store, partitions, max_key, read_secs, 1).expect("read");
        }
    }

    store.stop();
    println!("simple_bench done");
}
