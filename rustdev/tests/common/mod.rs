//! Common test utilities and helpers

use eloqstore::types::{Key, Value, TableIdent};
use eloqstore::config::KvOptions;
use std::path::{Path, PathBuf};
use rand::{Rng, thread_rng, distributions::Alphanumeric};
use std::sync::Arc;
use std::time::Instant;

/// Test data generator
pub struct TestDataGenerator {
    key_prefix: String,
    value_prefix: String,
    counter: usize,
}

impl TestDataGenerator {
    pub fn new(key_prefix: &str, value_prefix: &str) -> Self {
        Self {
            key_prefix: key_prefix.to_string(),
            value_prefix: value_prefix.to_string(),
            counter: 0,
        }
    }

    pub fn next_key(&mut self) -> Key {
        let key = format!("{}_{:06}", self.key_prefix, self.counter);
        self.counter += 1;
        Key::from(key.into_bytes())
    }

    pub fn next_value(&mut self) -> Value {
        let value = format!("{}_{:06}_{}", self.value_prefix, self.counter,
            thread_rng().gen::<u64>());
        Value::from(value.into_bytes())
    }

    pub fn random_key(&self, max: usize) -> Key {
        let num = thread_rng().gen_range(0..max);
        Key::from(format!("{}_{:06}", self.key_prefix, num).into_bytes())
    }

    pub fn random_value(&self, size: usize) -> Value {
        let value: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(size)
            .map(char::from)
            .collect();
        Value::from(value.into_bytes())
    }
}

/// Test environment setup
pub struct TestEnvironment {
    pub temp_dir: tempfile::TempDir,
    pub data_dir: PathBuf,
    pub options: KvOptions,
}

impl TestEnvironment {
    pub fn new(name: &str) -> Self {
        let temp_dir = tempfile::Builder::new()
            .prefix(&format!("eloqstore_test_{}_", name))
            .tempdir()
            .unwrap();

        let data_dir = temp_dir.path().to_path_buf();

        let mut options = KvOptions::default();
        options.data_dir = data_dir.clone();
        options.num_shards = 2;
        options.page_size = 4096;
        options.cache_size = 1000;

        Self {
            temp_dir,
            data_dir,
            options,
        }
    }

    pub fn with_shards(mut self, num_shards: usize) -> Self {
        self.options.num_shards = num_shards;
        self
    }

    pub fn with_page_size(mut self, page_size: usize) -> Self {
        self.options.page_size = page_size;
        self
    }

    pub fn with_cache_size(mut self, cache_size: usize) -> Self {
        self.options.cache_size = cache_size;
        self
    }
}

/// Performance timer for benchmarking
pub struct PerfTimer {
    name: String,
    start: Instant,
    operations: usize,
}

impl PerfTimer {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            start: Instant::now(),
            operations: 0,
        }
    }

    pub fn inc_ops(&mut self, count: usize) {
        self.operations += count;
    }

    pub fn elapsed(&self) -> std::time::Duration {
        self.start.elapsed()
    }

    pub fn throughput(&self) -> f64 {
        self.operations as f64 / self.elapsed().as_secs_f64()
    }

    pub fn print_summary(&self) {
        let elapsed = self.elapsed();
        println!("[{}] Completed {} operations in {:?}", self.name, self.operations, elapsed);
        println!("[{}] Throughput: {:.2} ops/sec", self.name, self.throughput());
    }
}

impl Drop for PerfTimer {
    fn drop(&mut self) {
        self.print_summary();
    }
}

/// Data verification helper
pub struct DataVerifier {
    expected: std::collections::HashMap<Key, Value>,
}

impl DataVerifier {
    pub fn new() -> Self {
        Self {
            expected: std::collections::HashMap::new(),
        }
    }

    pub fn record_write(&mut self, key: Key, value: Value) {
        self.expected.insert(key, value);
    }

    pub fn record_delete(&mut self, key: Key) {
        self.expected.remove(&key);
    }

    pub fn verify_read(&self, key: &Key, value: Option<&Value>) -> bool {
        match (self.expected.get(key), value) {
            (Some(expected), Some(actual)) => expected == actual,
            (None, None) => true,
            _ => false,
        }
    }

    pub fn verify_all<F>(&self, mut reader: F) -> Result<(), String>
    where
        F: FnMut(&Key) -> Option<Value>,
    {
        for (key, expected_value) in &self.expected {
            match reader(key) {
                Some(actual_value) if actual_value == *expected_value => continue,
                Some(actual_value) => {
                    return Err(format!(
                        "Value mismatch for key {:?}: expected {:?}, got {:?}",
                        key, expected_value, actual_value
                    ));
                }
                None => {
                    return Err(format!(
                        "Missing value for key {:?}: expected {:?}",
                        key, expected_value
                    ));
                }
            }
        }
        Ok(())
    }
}

/// Random operation selector with weights
pub struct OperationSelector<T> {
    operations: Vec<(T, u32)>,
    total_weight: u32,
}

impl<T: Clone> OperationSelector<T> {
    pub fn new(operations: Vec<(T, u32)>) -> Self {
        let total_weight = operations.iter().map(|(_, w)| w).sum();
        Self {
            operations,
            total_weight,
        }
    }

    pub fn select(&self) -> T {
        let mut rng = thread_rng();
        let mut weight = rng.gen_range(0..self.total_weight);

        for (op, w) in &self.operations {
            if weight < *w {
                return op.clone();
            }
            weight -= w;
        }

        // Should never reach here
        self.operations[0].0.clone()
    }
}

/// Assertion helpers
#[macro_export]
macro_rules! assert_error {
    ($result:expr, $expected_error:pat) => {
        match $result {
            Err($expected_error) => (),
            Ok(v) => panic!("Expected error {:?}, got Ok({:?})", stringify!($expected_error), v),
            Err(e) => panic!("Expected error {:?}, got {:?}", stringify!($expected_error), e),
        }
    };
}

#[macro_export]
macro_rules! assert_ok {
    ($result:expr) => {
        match $result {
            Ok(v) => v,
            Err(e) => panic!("Expected Ok, got Err({:?})", e),
        }
    };
}

/// Test assertion that a future completes within a timeout
#[macro_export]
macro_rules! assert_timeout {
    ($duration:expr, $future:expr) => {
        match tokio::time::timeout($duration, $future).await {
            Ok(result) => result,
            Err(_) => panic!("Operation timed out after {:?}", $duration),
        }
    };
}