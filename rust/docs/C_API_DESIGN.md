# EloqStore C API Design Document

## 1. Design Principles

1. **Flattened API**
   Simple operations directly pass parameters without creating Request objects.

2. **Synchronous Execution**
   Calls return results immediately.

3. **Clear Memory Ownership**

   * **Input**: Owned by the Rust/C++ caller. C++ must not retain pointers after the call returns.
   * **Output**: Either provided by the caller, or explicitly defined which side allocates and frees memory.

4. **Error Handling**
   Status codes plus optional error messages.

5. **Opaque Handles**
   Use `void*` as handle types.

---

## 2. Header File Definition

```c
// eloqsore_capi.h

#pragma once

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================
// Error Codes
// ============================================================
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

// ============================================================
// Opaque Handle Types
// ============================================================
typedef void* CEloqStoreHandle;      // Database instance handle
typedef void* CTableIdentHandle;     // Table identifier handle
typedef void* CScanRequestHandle;    // Scan request handle
typedef void* CBatchWriteHandle;     // BatchWrite request handle

// ============================================================
// Constants
// ============================================================
#define CEloqStore_MaxKeySize 4096
#define CEloqStore_MaxValueSize (1024 * 1024)  // 1MB

// ============================================================
// Write Operation Enum
// ============================================================
typedef enum CWriteOp {
    CWriteOp_Upsert = 0,
    CWriteOp_Delete = 1,
} CWriteOp;

// ============================================================
// Input Structures (for KV entries)
// ============================================================
typedef struct CKvEntry {
    const uint8_t* key;
    size_t key_len;
    const uint8_t* value;
    size_t value_len;
    uint64_t timestamp;
    uint64_t expire_ts;
} CKvEntry;

typedef struct CWriteEntry {
    const uint8_t* key;
    size_t key_len;
    const uint8_t* value;
    size_t value_len;
    uint64_t timestamp;
    CWriteOp op;
    uint64_t expire_ts;
} CWriteEntry;

// ============================================================
// Output Structures
// ============================================================

// Get operation result
typedef struct CGetResult {
    const uint8_t* value;
    size_t value_len;
    uint64_t timestamp;
    uint64_t expire_ts;
    bool found;
} CGetResult;

// Floor operation result
typedef struct CFloorResult {
    const uint8_t* key;
    size_t key_len;
    const uint8_t* value;
    size_t value_len;
    uint64_t timestamp;
    uint64_t expire_ts;
    bool found;
} CFloorResult;

// Scan result entry
typedef struct CScanEntry {
    const uint8_t* key;
    size_t key_len;
    const uint8_t* value;
    size_t value_len;
    uint64_t timestamp;
    uint64_t expire_ts;
} CScanEntry;

// Scan operation result
typedef struct CScanResult {
    CScanEntry* entries;
    size_t num_entries;
    size_t total_size;
    bool has_more;
} CScanResult;

// ============================================================
// Options API
// ============================================================

CEloqStoreHandle CEloqStore_Options_Create(void);
void CEloqStore_Options_Destroy(CEloqStoreHandle opts);

void CEloqStore_Options_SetNumThreads(CEloqStoreHandle opts, uint16_t n);
void CEloqStore_Options_SetBufferPoolSize(CEloqStoreHandle opts, uint64_t size);
void CEloqStore_Options_SetDataPageSize(CEloqStoreHandle opts, uint16_t size);
void CEloqStore_Options_SetManifestLimit(CEloqStoreHandle opts, uint32_t limit);
void CEloqStore_Options_SetFdLimit(CEloqStoreHandle opts, uint32_t limit);
void CEloqStore_Options_SetPagesPerFileShift(CEloqStoreHandle opts, uint8_t shift);
void CEloqStore_Options_SetOverflowPointers(CEloqStoreHandle opts, uint8_t n);
void CEloqStore_Options_SetDataAppendMode(CEloqStoreHandle opts, bool enable);
void CEloqStore_Options_SetEnableCompression(CEloqStoreHandle opts, bool enable);

void CEloqStore_Options_AddStorePath(CEloqStoreHandle opts, const char* path);
void CEloqStore_Options_SetCloudStorePath(CEloqStoreHandle opts, const char* path);
void CEloqStore_Options_SetCloudProvider(CEloqStoreHandle opts, const char* provider);
void CEloqStore_Options_SetCloudRegion(CEloqStoreHandle opts, const char* region);
void CEloqStore_Options_SetCloudCredentials(
    CEloqStoreHandle opts,
    const char* access_key,
    const char* secret_key
);
void CEloqStore_Options_SetCloudVerifySsl(CEloqStoreHandle opts, bool verify);

bool CEloqStore_Options_Validate(CEloqStoreHandle opts);

// ============================================================
// Engine Lifecycle
// ============================================================

CEloqStoreHandle CEloqStore_Create(CEloqStoreHandle options);
void CEloqStore_Destroy(CEloqStoreHandle store);

CEloqStoreStatus CEloqStore_Start(CEloqStoreHandle store);
void CEloqStore_Stop(CEloqStoreHandle store);
bool CEloqStore_IsStopped(CEloqStoreHandle store);

// ============================================================
// Table Identifier
// ============================================================

CTableIdentHandle CEloqStore_TableIdent_Create(
    const char* table_name,
    uint32_t partition_id
);
void CEloqStore_TableIdent_Destroy(CTableIdentHandle ident);
const char* CEloqStore_TableIdent_GetName(CTableIdentHandle ident);
uint32_t CEloqStore_TableIdent_GetPartition(CTableIdentHandle ident);

// ============================================================
// Flattened Write API (Simple Operations)
// ============================================================

// Single Put operation
CEloqStoreStatus CEloqStore_Put(
    CEloqStoreHandle store,
    CTableIdentHandle table,
    const uint8_t* key,
    size_t key_len,
    const uint8_t* value,
    size_t value_len,
    uint64_t timestamp
);

// Single Delete operation
CEloqStoreStatus CEloqStore_Delete(
    CEloqStoreHandle store,
    CTableIdentHandle table,
    const uint8_t* key,
    size_t key_len,
    uint64_t timestamp
);

// ============================================================
// Flattened Read API (Simple Operations)
// ============================================================

// Get operation: result buffer provided by caller
CEloqStoreStatus CEloqStore_Get(
    CEloqStoreHandle store,
    CTableIdentHandle table,
    const uint8_t* key,
    size_t key_len,
    CGetResult* out_result
);

// Floor operation: find the largest entry <= given key
CEloqStoreStatus CEloqStore_Floor(
    CEloqStoreHandle store,
    CTableIdentHandle table,
    const uint8_t* key,
    size_t key_len,
    CFloorResult* out_result
);

// ============================================================
// Scan Request API (Complex Operations – Request Model)
// ============================================================

CScanRequestHandle CEloqStore_ScanRequest_Create(void);
void CEloqStore_ScanRequest_Destroy(CScanRequestHandle req);

void CEloqStore_ScanRequest_SetTable(
    CScanRequestHandle req,
    CTableIdentHandle table
);
void CEloqStore_ScanRequest_SetRange(
    CScanRequestHandle req,
    const uint8_t* begin_key,
    size_t begin_key_len,
    bool begin_inclusive,
    const uint8_t* end_key,
    size_t end_key_len,
    bool end_inclusive
);
void CEloqStore_ScanRequest_SetPagination(
    CScanRequestHandle req,
    size_t max_entries,
    size_t max_size
);
void CEloqStore_ScanRequest_SetPrefetch(
    CScanRequestHandle req,
    size_t num_pages
);

CEloqStoreStatus CEloqStore_ExecScan(
    CEloqStoreHandle store,
    CScanRequestHandle req,
    CScanResult* out_result
);

// ============================================================
// BatchWrite Request API (Complex Operations – Request Model)
// ============================================================

CBatchWriteHandle CEloqStore_BatchWrite_Create(void);
void CEloqStore_BatchWrite_Destroy(CBatchWriteHandle req);

void CEloqStore_BatchWrite_SetTable(
    CBatchWriteHandle req,
    CTableIdentHandle table
);
void CEloqStore_BatchWrite_AddEntry(
    CBatchWriteHandle req,
    const uint8_t* key,
    size_t key_len,
    const uint8_t* value,
    size_t value_len,
    uint64_t timestamp,
    CWriteOp op,
    uint64_t expire_ts
);
void CEloqStore_BatchWrite_Clear(CBatchWriteHandle req);

CEloqStoreStatus CEloqStore_ExecBatchWrite(
    CEloqStoreHandle store,
    CBatchWriteHandle req
);

// ============================================================
// Error Message Query
// ============================================================

// Get the description of the last error (caller must free the returned string)
char* CEloqStore_GetLastError(CEloqStoreHandle store);

#ifdef __cplusplus
}
#endif
```

---

## 3. Memory Management Conventions

### 3.1 Input Buffers

```c
// Rust passes (ptr, len); C++ must not retain the pointer after the call
CEloqStore_Put(store, table, key_ptr, key_len, value_ptr, value_len, ts);
// After the call returns, C++ must not access key_ptr/value_ptr
```

---

### 3.2 Output Buffers

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

---

### 3.3 Strings

* **Input**: Caller must keep strings valid until the call returns
* **Output**: Strings allocated by C++, caller is responsible for freeing them

---

## 4. Rust Binding Design

### 4.1 FFI Bindings (`eloqstore-sys`)

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
    
    #[repr(C)]
    pub struct CFloorResult { ... }
    
    #[repr(C)]
    pub struct CScanResult { ... }
    
    // External function declarations...
}
```

---

### 4.2 Safe Wrapper (`eloqstore`)

```rust
pub struct Store {
    handle: CEloqStoreHandle,
}

impl Store {
    pub fn put(
        &mut self,
        table: &TableIdent,
        key: &[u8],
        value: &[u8],
        ts: u64
    ) -> Result<()> {
        unsafe {
            match ffi::CEloqStore_Put(
                self.handle,
                table.as_ptr(),
                key.as_ptr(),
                key.len(),
                value.as_ptr(),
                value.len(),
                ts
            ) {
                ffi::CEloqStoreStatus_Ok => Ok(()),
                err => Err(err.into()),
            }
        }
    }
    
    pub fn get(
        &mut self,
        table: &TableIdent,
        key: &[u8]
    ) -> Result<Option<Vec<u8>>> {
        unsafe {
            let mut result: ffi::CGetResult = std::mem::zeroed();
            match ffi::CEloqStore_Get(
                self.handle,
                table.as_ptr(),
                key.as_ptr(),
                key.len(),
                &mut result
            ) {
                ffi::CEloqStoreStatus_Ok => {
                    if result.found {
                        Ok(Some(
                            std::slice::from_raw_parts(
                                result.value,
                                result.value_len
                            ).to_vec()
                        ))
                    } else {
                        Ok(None)
                    }
                }
                err => Err(err.into()),
            }
        }
    }
}
```

---

## 5. Usage Examples

### 5.1 C Usage Example

```c
#include "eloqsore_capi.h"
#include <stdio.h>
#include <stdlib.h>

int main() {
    // Create options
    CEloqStoreHandle opts = CEloqStore_Options_Create();
    CEloqStore_Options_AddStorePath(opts, "/tmp/test_db");
    CEloqStore_Options_SetNumThreads(opts, 4);
    
    // Create engine
    CEloqStoreHandle store = CEloqStore_Create(opts);
    CEloqStore_Options_Destroy(opts);
    
    // Start engine
    CEloqStore_Start(store);
    
    // Create table identifier
    CTableIdentHandle table = CEloqStore_TableIdent_Create("test_table", 0);
    
    // Put operation
    const char* key = "hello";
    const char* value = "world";
    CEloqStore_Put(
        store,
        table,
        (uint8_t*)key,
        strlen(key),
        (uint8_t*)value,
        strlen(value),
        0
    );
    
    // Get operation
    CGetResult result;
    CEloqStore_Get(store, table, (uint8_t*)key, strlen(key), &result);
    if (result.found) {
        printf("Found: %.*s\n", (int)result.value_len, result.value);
    }
    
    // Scan operation
    CScanRequestHandle scan_req = CEloqStore_ScanRequest_Create();
    CEloqStore_ScanRequest_SetTable(scan_req, table);
    CScanResult scan_result;
    CEloqStore_ExecScan(store, scan_req, &scan_result);
    printf("Scan got %zu entries\n", scan_result.num_entries);
    CEloqStore_ScanRequest_Destroy(scan_req);
    
    // Cleanup
    CEloqStore_TableIdent_Destroy(table);
    CEloqStore_Stop(store);
    CEloqStore_Destroy(store);
    
    return 0;
}
```

---

### 5.2 Rust Usage Example

```rust
use eloqstore::{Store, Options, TableIdent, Result};

fn main() -> Result<()> {
    // Create options
    let mut opts = Options::new()?;
    opts.add_store_path("/tmp/test_db");
    opts.set_num_threads(4);
    
    // Create engine
    let mut store = Store::new(&opts)?;
    store.start()?;
    
    // Create table identifier
    let table = TableIdent::new("test_table", 0)?;
    
    // Put operation
    store.put(&table, b"hello", b"world", 0)?;
    
    // Get operation
    if let Some(value) = store.get(&table, b"hello")? {
        println!("Found: {}", String::from_utf8_lossy(&value));
    }
    
    // Scan operation
    let entries = store.scan(&table, &[], &[])?;
    println!("Scan got {} entries", entries.len());
    
    store.stop()?;
    Ok(())
}
```

---

## 6. Comparison with Legacy API

| Legacy API                                                                   | New API                                    | Description                                    |
| ---------------------------------------------------------------------------- | ------------------------------------------ | ---------------------------------------------- |
| `CReadRequest_Create()` + `CReadRequest_SetArgs()` + `CEloqStore_ExecRead()` | `CEloqStore_Get()`                         | Flattened into a single call                   |
| `CBatchWriteRequest_Create()` + `AddEntry()` + `CEloqStore_ExecBatchWrite()` | `CEloqStore_Put()` / `CEloqStore_Delete()` | Simple operations flattened                    |
| `CScanRequest_*`                                                             | `CScanRequest_*` + `CEloqStore_ExecScan()` | Request model preserved for complex operations |

