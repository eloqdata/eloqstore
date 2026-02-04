# EloqStore Rust SDK API Documentation

## Overview

The EloqStore Rust SDK provides two styles of APIs for accessing the EloqStore embedded key-value database:

1. **RocksDB-style API** – A simple and direct interface, similar to RocksDB usage
2. **EloqStore Request Trait API** – A type-safe and composable interface

The SDK is built on top of a C FFI layer that bridges Rust and the C++ EloqStore implementation.

---

## Quick Start

### Adding Dependencies

We recommend directly adding the source code from our GitHub repository as a crate.

> Currently, this SDK has not yet been merged into the `main` branch, so a direct Git URL cannot be provided.

---

### Basic Usage Example

```rust
use eloqstore::{EloqStore, Options, TableIdentifier};

fn main() -> Result<(), eloqstore::KvError> {
    // Configure storage options
    let mut opts = Options::new()?;
    opts.set_num_threads(4)?;
    opts.add_store_path("/tmp/eloqstore_data")?;

    // Create and start the store instance
    let store = EloqStore::new(&opts)?;
    store.start()?;

    // Define table identifier
    let table = TableIdentifier::new("my_table", 0)?;

    // RocksDB-style API
    store.put(&table, b"key1", b"value1", 1000)?;
    let value = store.get(&table, b"key1")?;
    assert_eq!(value, Some(b"value1".to_vec()));

    // Request Trait API
    use eloqstore::{ReadRequest, WriteRequest};

    let read_req = ReadRequest::new(table.clone(), b"key1");
    let resp = store.exec_sync(read_req)?;
    assert!(!resp.value.is_empty());

    Ok(())
}
```

---

## Core Types

### Error Type `KvError`

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum KvError {
    NoError,           // No error
    InvalidArgs,       // Invalid arguments
    NotFound,          // Key not found
    NotRunning,        // Store not running
    Corrupted,         // Data corruption
    EndOfFile,         // End of file
    OutOfSpace,        // Out of disk space
    OutOfMem,          // Out of memory
    OpenFileLimit,     // Open file limit reached
    TryAgain,          // Retry operation
    Busy,              // Busy
    Timeout,           // Timeout
    NoPermission,      // Permission denied
    CloudErr,          // Cloud storage error
    IoFail,            // I/O failure
    Unknown,           // Unknown error code
}
```

---

### Storage Options `Options`

Options used to configure an EloqStore instance.

```rust
impl Options {
    /// Create a new options instance
    pub fn new() -> Result<Self, KvError>;
    
    /// Set number of worker threads
    /// Returns an error if n exceeds u16::MAX (65535)
    pub fn set_num_threads(&mut self, n: u32) -> Result<(), KvError>;
    
    /// Add a storage path
    pub fn add_store_path(&mut self, path: &str) -> Result<(), KvError>;
    
    /// Set data page size (bytes)
    /// Returns an error if size exceeds u16::MAX (65535)
    pub fn set_data_page_size(&mut self, size: u32) -> Result<(), KvError>;
    
    /// Set buffer pool size (bytes)
    pub fn set_buffer_pool_size(&mut self, size: u64);
    
    /// Enable data append mode
    pub fn set_data_append_mode(&mut self, enable: bool);
    
    /// Enable compression
    pub fn set_enable_compression(&mut self, enable: bool);
    
    /// Validate option configuration
    pub fn validate(&self) -> bool;
}
```

---

### Table Identifier `TableIdentifier`

Identifies a table (or column family) in the database.

```rust
impl TableIdentifier {
    /// Create a new table identifier
    ///
    /// # Arguments
    /// - `name`: Table name
    /// - `partition`: Partition ID
    pub fn new(name: &str, partition: u32) -> Result<Self, KvError>;
    
    /// Get table name
    pub fn name(&self) -> &str;
    
    /// Get partition ID
    pub fn partition(&self) -> u32;
}
```

---

### Store Instance `EloqStore`

The main storage interface, providing two API styles.

```rust
impl EloqStore {
    /// Create a new store instance
    pub fn new(opts: &Options) -> Result<Self, KvError>;
    
    /// Start the store
    pub fn start(&mut self) -> Result<(), KvError>;
    
    /// Stop the store
    pub fn stop(&mut self);
    
    /// Check whether the store is stopped
    pub fn is_stopped(&self) -> bool;
    
    /// Execute a Request trait operation synchronously
    pub fn exec_sync<R: Request>(&self, req: R) -> Result<R::Response, KvError>;
}
```

---

## RocksDB-style API

### Basic Operations

```rust
impl EloqStore {
    /// Get the value associated with a key
    ///
    /// # Returns
    /// - `Ok(Some(value))`: Key exists
    /// - `Ok(None)`: Key not found
    /// - `Err(KvError)`: Error occurred
    pub fn get(&self, tbl: &TableIdentifier, key: &[u8]) -> Result<Option<Vec<u8>>, KvError>;
    
    /// Insert or update a key-value pair
    ///
    /// # Arguments
    /// - `tbl`: Table identifier
    /// - `key`: Key
    /// - `value`: Value
    /// - `ts`: Timestamp (used for versioning)
    pub fn put(&self, tbl: &TableIdentifier, key: &[u8], value: &[u8], ts: u64) -> Result<(), KvError>;
    
    /// Delete a key
    ///
    /// # Arguments
    /// - `tbl`: Table identifier
    /// - `key`: Key to delete
    /// - `ts`: Deletion timestamp
    pub fn delete(&self, tbl: &TableIdentifier, key: &[u8], ts: u64) -> Result<(), KvError>;
}
```

---

### Batch Operations

```rust
impl EloqStore {
    /// Batch insert key-value pairs
    ///
    /// # Notes
    /// - All keys must be sorted in ascending order
    /// - All keys must be unique
    /// - A single timestamp must be used
    /// - Keys and values arrays must have the same length
    pub fn put_batch(
        &self,
        tbl: &TableIdentifier,
        keys: &[&[u8]],
        values: &[&[u8]],
        ts: u64,
    ) -> Result<(), KvError>;
    
    /// Batch delete keys
    ///
    /// # Notes
    /// - All keys must be sorted in ascending order
    /// - All keys must be unique
    /// - A single timestamp must be used
    pub fn delete_batch(
        &self,
        tbl: &TableIdentifier,
        keys: &[&[u8]],
        ts: u64,
    ) -> Result<(), KvError>;
}
```

---

### Range Query Operations

```rust
impl EloqStore {
    /// Find the largest key less than or equal to the specified key (Floor operation)
    ///
    /// # Returns
    /// - `Ok(Some((key, value)))`: Matching key-value pair found
    /// - `Ok(None)`: No matching key found
    /// - `Err(KvError)`: Error occurred
    pub fn floor(
        &self,
        tbl: &TableIdentifier,
        key: &[u8],
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>, KvError>;
    
    /// Scan all key-value pairs within a specified range
    ///
    /// # Arguments
    /// - `begin`: Start key (inclusive)
    /// - `end`: End key (exclusive)
    ///
    /// # Notes
    /// This method returns all matching entries and may not be suitable for large datasets.
    /// For large datasets, use `ScanRequest` with pagination.
    pub fn scan(
        &self,
        tbl: &TableIdentifier,
        begin: &[u8],
        end: &[u8],
    ) -> Result<Vec<KvEntry>, KvError>;
}
```

---

## EloqStore Request Trait API

### Request Trait

```rust
/// Request trait defining executable operations
pub trait Request {
    /// Response type
    type Response: Response;
    
    /// Execute the request
    fn execute(&self, store: &EloqStore) -> Result<Self::Response, KvError>;
}

/// Marker trait for responses
pub trait Response {}
```

---

### Read Request `ReadRequest`

```rust
#[derive(Debug)]
pub struct ReadRequest {
    table: TableIdentifier,
    key: Vec<u8>,
}

impl ReadRequest {
    /// Create a read request
    pub fn new(table: TableIdentifier, key: &[u8]) -> Self;
}

impl Request for ReadRequest {
    type Response = ReadResponse;
}

/// Read response
#[derive(Debug, Clone)]
pub struct ReadResponse {
    pub value: Vec<u8>,      // Value
    pub timestamp: u64,      // Timestamp
    pub expire_ts: u64,      // Expiration timestamp
}
```

---

### Write Request `WriteRequest`

```rust
#[derive(Debug)]
pub struct WriteRequest {
    table: TableIdentifier,
    entries: Vec<WriteEntry>,
}

/// Write entry
#[derive(Debug)]
pub struct WriteEntry {
    pub key: Vec<u8>,        // Key
    pub value: Vec<u8>,      // Value
    pub timestamp: u64,      // Timestamp
    pub op: WriteOp,         // Operation type
    pub expire_ts: u64,      // Expiration timestamp
}

/// Write operation type
#[derive(Debug, Clone, Copy)]
pub enum WriteOp {
    Upsert,  // Insert or update
    Delete,  // Delete
}

impl WriteRequest {
    /// Create a write request
    pub fn new(table: TableIdentifier) -> Self;
    
    /// Add an insert/update operation (chainable)
    pub fn put(mut self, key: &[u8], value: &[u8], timestamp: u64) -> Self;
    
    /// Add a delete operation (chainable)
    pub fn delete(mut self, key: &[u8], timestamp: u64) -> Self;
    
    /// Check whether the request is empty
    pub fn is_empty(&self) -> bool;
}

impl Request for WriteRequest {
    type Response = WriteResponse;
}

/// Write response
#[derive(Debug, Clone)]
pub struct WriteResponse {
    pub success: bool,  // Whether the operation succeeded
}
```

---

### Scan Request `ScanRequest`

```rust
#[derive(Debug)]
pub struct ScanRequest {
    table: TableIdentifier,
    begin: Vec<u8>,
    end: Vec<u8>,
    begin_inclusive: bool,
    max_entries: usize,  // Maximum number of entries
    max_size: usize,     // Maximum result size in bytes
}

impl ScanRequest {
    /// Create a scan request
    pub fn new(table: TableIdentifier) -> Self;
    
    /// Set scan range (chainable)
    ///
    /// # Arguments
    /// - `begin`: Start key
    /// - `end`: End key (exclusive)
    /// - `inclusive`: Whether the start key is inclusive
    pub fn range(mut self, begin: &[u8], end: &[u8], inclusive: bool) -> Self;
    
    /// Set pagination parameters (chainable)
    ///
    /// # Arguments
    /// - `max_entries`: Maximum entries per page
    /// - `max_size`: Maximum bytes per page
    ///
    /// # Notes
    /// If both parameters are `usize::MAX`, the scan is unlimited.
    /// The actual number of returned entries is limited by the smaller of the two.
    pub fn pagination(mut self, max_entries: usize, max_size: usize) -> Self;
}

impl Request for ScanRequest {
    type Response = ScanResponse;
}

/// Scan response
#[derive(Debug, Clone)]
pub struct ScanResponse {
    pub entries: Vec<KvEntry>,  // Scan result entries
    pub has_more: bool,         // Whether more data is available
}

/// Key-value entry
#[derive(Debug, Clone)]
pub struct KvEntry {
    pub key: Vec<u8>,        // Key
    pub value: Vec<u8>,      // Value
    pub timestamp: u64,      // Timestamp
    pub expire_ts: u64,      // Expiration timestamp
}
```

---

### Floor Request `FloorRequest`

```rust
#[derive(Debug)]
pub struct FloorRequest {
    table: TableIdentifier,
    key: Vec<u8>,
}

impl FloorRequest {
    /// Create a floor request
    pub fn new(table: TableIdentifier, key: &[u8]) -> Self;
}

impl Request for FloorRequest {
    type Response = FloorResponse;
}

/// Floor response
#[derive(Debug, Clone)]
pub struct FloorResponse {
    pub key: Vec<u8>,        // Found key
    pub value: Vec<u8>,      // Corresponding value
    pub timestamp: u64,      // Timestamp
    pub expire_ts: u64,      // Expiration timestamp
}
```

---

## Advanced Usage Examples

### Example 1: Paginated Scan

```rust
use eloqstore::{EloqStore, Options, TableIdentifier, ScanRequest};

fn paginated_scan_example(store: &EloqStore, table: &TableIdentifier) -> Result<(), KvError> {
    let mut start_key = vec![];
    let end_key = b"z";
    let mut all_entries = Vec::new();
    
    loop {
        let scan_req = ScanRequest::new(table.clone())
            .range(&start_key, end_key, false)  // exclusive start
            .pagination(100, 1024 * 1024);      // 100 entries or 1MB per page
        
        let resp = store.exec_sync(scan_req)?;
        
        // Process current page
        all_entries.extend(resp.entries);
        
        // Check if more data is available
        if !resp.has_more {
            break;
        }
        
        // Update start key to the last entry's key
        if let Some(last_entry) = resp.entries.last() {
            start_key = last_entry.key.clone();
        }
    }
    
    println!("Total entries: {}", all_entries.len());
    Ok(())
}
```

---

### Example 2: Batch Write (Sorted Keys Required)

```rust
use eloqstore::{EloqStore, Options, TableIdentifier, WriteRequest};

fn batch_write_example(store: &EloqStore, table: &TableIdentifier) -> Result<(), KvError> {
    let mut write_req = WriteRequest::new(table.clone());
    
    // Create and sort keys (batch write requires ascending order)
    let mut keys: Vec<String> = (0..1000)
        .map(|i| format!("key_{:06}", i))
        .collect();
    keys.sort();
    
    // Add sorted key-value pairs
    for key in keys {
        write_req = write_req.put(
            key.as_bytes(),
            b"value",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        );
    }
    
    store.exec_sync(write_req)?;
    Ok(())
}
```

---

### Example 3: Mixed Operations

```rust
use eloqstore::{
    EloqStore, Options, TableIdentifier,
    ReadRequest, WriteRequest, ScanRequest, FloorRequest
};

fn mixed_operations_example() -> Result<(), KvError> {
    let mut opts = Options::new()?;
    opts.add_store_path("/tmp/eloqstore_mixed")?;
    let mut store = EloqStore::new(&opts)?;
    store.start()?;
    
    let table = TableIdentifier::new("mixed_table", 0)?;
    let ts = 1000;
    
    // Write data
    let write_req = WriteRequest::new(table.clone())
        .put(b"apple", b"red", ts)
        .put(b"banana", b"yellow", ts)
        .put(b"cherry", b"red", ts);
    store.exec_sync(write_req)?;
    
    // Read data
    let read_req = ReadRequest::new(table.clone(), b"banana");
    let read_resp = store.exec_sync(read_req)?;
    println!("banana: {}", String::from_utf8_lossy(&read_resp.value));
    
    // Floor query
    let floor_req = FloorRequest::new(table.clone(), b"c");
    if let Ok(floor_resp) = store.exec_sync(floor_req) {
        println!("floor of 'c': {}", String::from_utf8_lossy(&floor_resp.key));
    }
    
    // Range scan
    let scan_req = ScanRequest::new(table.clone())
        .range(b"a", b"d", true)
        .pagination(10, usize::MAX);
    let scan_resp = store.exec_sync(scan_req)?;
    println!("Found {} entries in range [a, d)", scan_resp.entries.len());
    
    store.stop();
    Ok(())
}
```

---

## C API Design (FFI Layer)

The Rust SDK is built on top of a C FFI layer that provides a C-compatible interface to the C++ EloqStore implementation. This section documents the C API design principles and structure.

### Design Principles

1. **Flattened API**
   Simple operations directly pass parameters without creating Request objects.

2. **Synchronous Execution**
   Calls return results immediately.

3. **Clear Memory Ownership**
   - **Input**: Owned by the Rust/C++ caller. C++ must not retain pointers after the call returns.
   - **Output**: Either provided by the caller, or explicitly defined which side allocates and frees memory.

4. **Error Handling**
   Status codes plus optional error messages.

5. **Opaque Handles**
   Use `void*` as handle types.

---

### C API Header Structure

The C API is defined in `eloqstore_capi.h` and includes:

#### Error Codes

```c
typedef enum CEloqStoreStatus {
    CEloqStoreStatus_Ok = 0,
    CEloqStoreStatus_InvalidArgs,
    CEloqStoreStatus_NotFound,
    CEloqStoreStatus_NotRunning,
    CEloqStoreStatus_Corrupted,
    CEloqStoreStatus_EndOfFile,
    CEloqStoreStatus_OutOfSpace,
    CEloqStoreStatus_OutOfMem,
    CEloqStoreStatus_OpenFileLimit,
    CEloqStoreStatus_TryAgain,
    CEloqStoreStatus_Busy,
    CEloqStoreStatus_Timeout,
    CEloqStoreStatus_NoPermission,
    CEloqStoreStatus_CloudErr,
    CEloqStoreStatus_IoFail,
} CEloqStoreStatus;
```

#### Opaque Handle Types

```c
typedef void* CEloqStoreHandle;      // Database instance handle
typedef void* CTableIdentHandle;     // Table identifier handle
typedef void* CScanRequestHandle;    // Scan request handle
typedef void* CBatchWriteHandle;     // BatchWrite request handle
```

#### Constants

```c
#define CEloqStore_MaxKeySize 4096
#define CEloqStore_MaxValueSize (1024 * 1024)  // 1MB
```

#### Data Structures

- `CWriteEntry`: Input structure for write operations
- `CGetResult`: Output structure for get operations
- `CFloorResult`: Output structure for floor operations
- `CScanEntry`: Scan result entry
- `CScanResult`: Scan operation result

---

### Memory Management Conventions

#### Input Buffers

```c
// Rust passes (ptr, len); C++ must not retain the pointer after the call
CEloqStore_Put(store, table, key_ptr, key_len, value_ptr, value_len, ts);
// After the call returns, C++ must not access key_ptr/value_ptr
```

#### Output Buffers

```c
// Option A: Caller-provided buffer (recommended)
CGetResult result;
CEloqStore_Get(store, table, key, key_len, &result);
// result.value points to caller-provided or internal C++ buffer
// Caller releases it when no longer needed (if required)

// Option B: C++ allocates, caller frees (Scan result)
CScanResult result;
CEloqStore_ExecScan(store, req, &result);
// result.entries array is allocated by C++
// Caller must call CEloqStore_FreeScanResult(&result) to free it
```

#### Strings

- **Input**: Caller must keep strings valid until the call returns
- **Output**: Strings allocated by C++, caller is responsible for freeing them

---

### C API Function Categories

1. **Options API**: Configuration management
   - `CEloqStore_Options_Create()`
   - `CEloqStore_Options_SetNumThreads()`
   - `CEloqStore_Options_AddStorePath()`
   - etc.

2. **Engine Lifecycle**: Store creation and management
   - `CEloqStore_Create()`
   - `CEloqStore_Start()`
   - `CEloqStore_Stop()`
   - `CEloqStore_Destroy()`

3. **Table Identifier**: Table management
   - `CEloqStore_TableIdent_Create()`
   - `CEloqStore_TableIdent_Destroy()`

4. **Flattened Write API**: Simple operations
   - `CEloqStore_Put()`
   - `CEloqStore_Delete()`

5. **Flattened Read API**: Simple operations
   - `CEloqStore_Get()`
   - `CEloqStore_Floor()`

6. **Scan Request API**: Complex operations (Request model)
   - `CEloqStore_ScanRequest_Create()`
   - `CEloqStore_ScanRequest_SetRange()`
   - `CEloqStore_ExecScan()`

7. **BatchWrite Request API**: Complex operations (Request model)
   - `CEloqStore_BatchWrite_Create()`
   - `CEloqStore_BatchWrite_AddEntry()`
   - `CEloqStore_ExecBatchWrite()`

---

### Rust FFI Binding Design

The Rust SDK consists of two layers:

#### 1. FFI Bindings (`eloqstore-sys`)

Low-level unsafe bindings that directly map to the C API:

```rust
pub mod ffi {
    #[repr(C)]
    pub enum CEloqStoreStatus { ... }
    
    pub type CEloqStoreHandle = *mut c_void;
    pub type CTableIdentHandle = *mut c_void;
    pub type CScanRequestHandle = *mut c_void;
    pub type CBatchWriteHandle = *mut c_void;
    
    #[repr(C)]
    pub struct CGetResult { ... }
    
    // External function declarations...
}
```

#### 2. Safe Wrapper (`eloqstore`)

High-level safe Rust API that wraps the FFI layer:

```rust
pub struct EloqStore {
    handle: CEloqStoreHandle,
}

impl EloqStore {
    pub fn put(
        &self,
        table: &TableIdentifier,
        key: &[u8],
        value: &[u8],
        ts: u64
    ) -> Result<()> {
        unsafe {
            match ffi::CEloqStore_Put(...) {
                ffi::CEloqStoreStatus_Ok => Ok(()),
                err => Err(err.into()),
            }
        }
    }
}
```

---

## Performance Considerations

1. **Batch write requirements**

   * Keys must be sorted in ascending order
   * Keys must be unique
   * All operations must use the same timestamp
   * Keys and values arrays must have matching lengths

2. **Paginated scanning**

   * Always use pagination for large datasets
   * Set `max_entries` and `max_size` appropriately
   * Use `has_more` to determine whether more data exists

3. **Memory management**

   * `KvEntry` contains full key-value data; be mindful of memory usage
   * For large values, consider overflow or external storage
   * Scan results are deep-copied to avoid dangling pointers

4. **Concurrency limitations**

   * Due to the use of global C++ pointers, only one active EloqStore instance is allowed at a time
   * Tests must be run with `--test-threads=1`
   * However, `EloqStore` is `Send + Sync` and can be shared across threads using `Arc`

---

## Error Handling

```rust
use eloqstore::KvError;

match store.get(&table, b"key") {
    Ok(Some(value)) => {
        println!("Found: {:?}", value);
    }
    Ok(None) => {
        println!("Key not found");
    }
    Err(KvError::NotFound) => {
        println!("Key not found (error variant)");
    }
    Err(KvError::OutOfMem) => {
        println!("Out of memory");
    }
    Err(KvError::Unknown) => {
        println!("Unknown error code from C API");
    }
    Err(e) => {
        println!("Error: {:?}", e);
    }
}
```

---

## Build and Test

### Build

```bash
cargo build -p eloqstore
```

---

## Limitations

1. **Serial execution**: Due to global C++ pointers, only one active EloqStore instance is allowed
2. **Build time**: Initial build compiles approximately 31MB of C++ source code
3. **Batch write sorting**: Batch writes require keys to be sorted in ascending order
4. **Paginated scans**: Pagination state must be managed manually

---

## License

The EloqStore Rust SDK is licensed under the same license as EloqStore.
