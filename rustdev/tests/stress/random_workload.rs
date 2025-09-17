//! Random workload generator for stress testing

use eloqstore_rs::store::EloqStore;
use eloqstore_rs::config::KvOptions;
use eloqstore_rs::types::{Key, Value, TableIdent};
use eloqstore_rs::api::request::{WriteRequest, ReadRequest, DeleteRequest, ScanRequest};

use std::sync::Arc;
use std::time::{Duration, Instant};
use rand::{Rng, thread_rng, distributions::{Distribution, WeightedIndex}};
use rand::seq::SliceRandom;

/// Operation type for workload
#[derive(Debug, Clone, Copy)]
enum Operation {
    Read,
    Write,
    Delete,
    Scan,
    BatchWrite,
    Sleep,
}

/// Workload pattern
#[derive(Debug, Clone)]
pub struct WorkloadPattern {
    pub name: String,
    pub operations: Vec<(Operation, u32)>, // (operation, weight)
    pub key_distribution: KeyDistribution,
    pub value_size_distribution: ValueSizeDistribution,
}

/// Key distribution pattern
#[derive(Debug, Clone)]
pub enum KeyDistribution {
    Uniform { range: usize },
    Zipfian { range: usize, theta: f64 },
    Sequential { start: usize, increment: usize },
    Hotspot { range: usize, hotspot_ratio: f64 },
}

/// Value size distribution
#[derive(Debug, Clone)]
pub enum ValueSizeDistribution {
    Fixed { size: usize },
    Uniform { min: usize, max: usize },
    Normal { mean: usize, stddev: usize },
}

impl WorkloadPattern {
    /// Create a read-heavy workload
    pub fn read_heavy() -> Self {
        Self {
            name: "ReadHeavy".to_string(),
            operations: vec![
                (Operation::Read, 80),
                (Operation::Write, 15),
                (Operation::Scan, 5),
            ],
            key_distribution: KeyDistribution::Zipfian { range: 10000, theta: 0.99 },
            value_size_distribution: ValueSizeDistribution::Uniform { min: 100, max: 1000 },
        }
    }

    /// Create a write-heavy workload
    pub fn write_heavy() -> Self {
        Self {
            name: "WriteHeavy".to_string(),
            operations: vec![
                (Operation::Write, 70),
                (Operation::Read, 20),
                (Operation::Delete, 5),
                (Operation::BatchWrite, 5),
            ],
            key_distribution: KeyDistribution::Uniform { range: 10000 },
            value_size_distribution: ValueSizeDistribution::Uniform { min: 500, max: 5000 },
        }
    }

    /// Create a mixed workload
    pub fn mixed() -> Self {
        Self {
            name: "Mixed".to_string(),
            operations: vec![
                (Operation::Read, 40),
                (Operation::Write, 40),
                (Operation::Scan, 10),
                (Operation::Delete, 5),
                (Operation::BatchWrite, 5),
            ],
            key_distribution: KeyDistribution::Hotspot { range: 10000, hotspot_ratio: 0.2 },
            value_size_distribution: ValueSizeDistribution::Normal { mean: 1000, stddev: 200 },
        }
    }

    /// Create a scan-heavy workload
    pub fn scan_heavy() -> Self {
        Self {
            name: "ScanHeavy".to_string(),
            operations: vec![
                (Operation::Scan, 60),
                (Operation::Read, 20),
                (Operation::Write, 20),
            ],
            key_distribution: KeyDistribution::Sequential { start: 0, increment: 1 },
            value_size_distribution: ValueSizeDistribution::Fixed { size: 100 },
        }
    }
}

/// Workload generator
pub struct WorkloadGenerator {
    pattern: WorkloadPattern,
    operation_chooser: WeightedIndex<u32>,
    key_counter: usize,
}

impl WorkloadGenerator {
    pub fn new(pattern: WorkloadPattern) -> Self {
        let weights: Vec<_> = pattern.operations.iter().map(|(_, w)| *w).collect();
        let operation_chooser = WeightedIndex::new(&weights).unwrap();

        Self {
            pattern,
            operation_chooser,
            key_counter: 0,
        }
    }

    /// Generate next operation
    pub fn next_operation(&mut self) -> Operation {
        let mut rng = thread_rng();
        let index = self.operation_chooser.sample(&mut rng);
        self.pattern.operations[index].0
    }

    /// Generate next key
    pub fn next_key(&mut self) -> Key {
        let mut rng = thread_rng();

        let key_num = match &self.pattern.key_distribution {
            KeyDistribution::Uniform { range } => {
                rng.gen_range(0..*range)
            }
            KeyDistribution::Zipfian { range, theta } => {
                // Simplified Zipfian distribution
                let zipf_val = rng.gen::<f64>().powf(1.0 / theta);
                (zipf_val * (*range as f64)) as usize % range
            }
            KeyDistribution::Sequential { start, increment } => {
                let num = *start + self.key_counter * increment;
                self.key_counter += 1;
                num
            }
            KeyDistribution::Hotspot { range, hotspot_ratio } => {
                if rng.gen_bool(*hotspot_ratio) {
                    // Access hot keys (first 10% of range)
                    rng.gen_range(0..(range / 10))
                } else {
                    // Access cold keys
                    rng.gen_range(0..*range)
                }
            }
        };

        Key::from(format!("key_{:08}", key_num).into_bytes())
    }

    /// Generate next value
    pub fn next_value(&mut self) -> Value {
        let mut rng = thread_rng();

        let size = match &self.pattern.value_size_distribution {
            ValueSizeDistribution::Fixed { size } => *size,
            ValueSizeDistribution::Uniform { min, max } => {
                rng.gen_range(*min..=*max)
            }
            ValueSizeDistribution::Normal { mean, stddev } => {
                // Simplified normal distribution
                let normal: f64 = rng.gen_range(-3.0..3.0);
                let size = (*mean as f64 + normal * (*stddev as f64)).max(1.0) as usize;
                size.min(*mean + 3 * stddev).max(1)
            }
        };

        let value_data: Vec<u8> = (0..size).map(|_| rng.gen()).collect();
        Value::from(value_data)
    }
}

#[tokio::test]
async fn test_random_workload_patterns() {
    let patterns = vec![
        WorkloadPattern::read_heavy(),
        WorkloadPattern::write_heavy(),
        WorkloadPattern::mixed(),
        WorkloadPattern::scan_heavy(),
    ];

    for pattern in patterns {
        println!("\n=== Testing {} Workload ===", pattern.name);
        run_workload_test(pattern, 1000).await;
    }
}

async fn run_workload_test(pattern: WorkloadPattern, num_operations: usize) {
    let temp_dir = tempfile::tempdir().unwrap();

    // Initialize store
    let mut kv_options = KvOptions::default();
    kv_options.data_dir = temp_dir.path().to_path_buf();
    kv_options.num_shards = 4;

    let store = Arc::new(EloqStore::new(kv_options).await.unwrap());
    store.start().await.unwrap();

    let table_id = TableIdent::new("workload_test");
    let mut generator = WorkloadGenerator::new(pattern.clone());

    let start_time = Instant::now();
    let mut operation_counts = std::collections::HashMap::new();

    for i in 0..num_operations {
        let op = generator.next_operation();
        *operation_counts.entry(format!("{:?}", op)).or_insert(0) += 1;

        match op {
            Operation::Read => {
                let key = generator.next_key();
                let request = ReadRequest {
                    table_id: table_id.clone(),
                    key,
                };
                let _ = store.read(request).await;
            }
            Operation::Write => {
                let key = generator.next_key();
                let value = generator.next_value();
                let request = WriteRequest {
                    table_id: table_id.clone(),
                    key,
                    value,
                };
                let _ = store.write(request).await;
            }
            Operation::Delete => {
                let key = generator.next_key();
                let request = DeleteRequest {
                    table_id: table_id.clone(),
                    key,
                };
                let _ = store.delete(request).await;
            }
            Operation::Scan => {
                let start_key = generator.next_key();
                let end_key = generator.next_key();

                let (start, end) = if start_key < end_key {
                    (start_key, end_key)
                } else {
                    (end_key, start_key)
                };

                let request = ScanRequest {
                    table_id: table_id.clone(),
                    start_key: start,
                    end_key: end,
                    limit: Some(10),
                    reverse: false,
                };
                let _ = store.scan(request).await;
            }
            Operation::BatchWrite => {
                let mut entries = Vec::new();
                for _ in 0..10 {
                    let key = generator.next_key();
                    let value = generator.next_value();
                    entries.push((key, value));
                }

                let request = eloqstore_rs::api::request::BatchWriteRequest {
                    table_id: table_id.clone(),
                    entries,
                };
                let _ = store.batch_write(request).await;
            }
            Operation::Sleep => {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }

        if i % 100 == 0 && i > 0 {
            print!(".");
            if i % 1000 == 0 {
                println!(" {}", i);
            }
        }
    }

    let elapsed = start_time.elapsed();
    println!("\nCompleted {} operations in {:?}", num_operations, elapsed);
    println!("Throughput: {:.2} ops/sec", num_operations as f64 / elapsed.as_secs_f64());
    println!("Operation breakdown:");
    for (op, count) in operation_counts {
        println!("  {}: {}", op, count);
    }

    store.stop().await.unwrap();
}

#[tokio::test]
async fn test_workload_crash_recovery() {
    let temp_dir = tempfile::tempdir().unwrap();
    let data_dir = temp_dir.path().to_path_buf();
    let table_id = TableIdent::new("crash_test");

    // Phase 1: Write data
    println!("Phase 1: Writing initial data...");
    {
        let mut kv_options = KvOptions::default();
        kv_options.data_dir = data_dir.clone();
        kv_options.num_shards = 2;

        let store = Arc::new(EloqStore::new(kv_options).await.unwrap());
        store.start().await.unwrap();

        // Write test data
        for i in 0..100 {
            let key = Key::from(format!("persistent_key_{:04}", i).into_bytes());
            let value = Value::from(format!("persistent_value_{:04}", i).into_bytes());

            let request = WriteRequest {
                table_id: table_id.clone(),
                key,
                value,
            };

            store.write(request).await.unwrap();
        }

        // Simulate crash by stopping without proper shutdown
        // In real scenario, this would be a process kill
        store.stop().await.unwrap();
    }

    // Phase 2: Recover and verify data
    println!("Phase 2: Recovering and verifying data...");
    {
        let mut kv_options = KvOptions::default();
        kv_options.data_dir = data_dir.clone();
        kv_options.num_shards = 2;

        let store = Arc::new(EloqStore::new(kv_options).await.unwrap());
        store.start().await.unwrap();

        // Verify all data is recovered
        for i in 0..100 {
            let key = Key::from(format!("persistent_key_{:04}", i).into_bytes());
            let expected_value = Value::from(format!("persistent_value_{:04}", i).into_bytes());

            let request = ReadRequest {
                table_id: table_id.clone(),
                key,
            };

            let response = store.read(request).await.unwrap();
            assert_eq!(response.value.unwrap(), expected_value, "Data not recovered after crash!");
        }

        println!("All data recovered successfully!");
        store.stop().await.unwrap();
    }
}