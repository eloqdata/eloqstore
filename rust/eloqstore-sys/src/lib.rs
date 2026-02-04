#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(unused)]

mod embedded_lib;

use std::os::raw::{c_char, c_uchar, c_uint, c_ulonglong, c_ushort, c_void};
use std::sync::{Once, Mutex};

// Ensure embedded library is available at startup
static INIT: Once = Once::new();
// Store initialization result: None = not initialized yet, Some(Ok(())) = success, Some(Err(_)) = failure
static INIT_RESULT: Mutex<Option<Result<(), String>>> = Mutex::new(None);

/// Ensure the embedded library is extracted and available before FFI calls
/// This is called automatically when creating Options, but can be called manually if needed
/// Returns an error if library extraction or loading fails
pub fn ensure_library_loaded() -> Result<(), String> {
    // Check if we've already initialized (success or failure)
    {
        let result_guard = INIT_RESULT.lock().map_err(|e| format!("Mutex poison: {}", e))?;
        if let Some(ref result) = *result_guard {
            return result.clone();
        }
    }
    
    // Initialize (only runs once)
    INIT.call_once(|| {
        let result = embedded_lib::ensure_library_available()
            .map(|_| ())
            .map_err(|e| e);
        
        // Store the result for future calls
        if let Ok(mut guard) = INIT_RESULT.lock() {
            *guard = Some(result);
        }
    });
    
    // Retrieve and return the stored result
    let result_guard = INIT_RESULT.lock().map_err(|e| format!("Mutex poison: {}", e))?;
    result_guard
        .as_ref()
        .expect("Initialization should have completed")
        .clone()
}

pub use self::ffi::CEloqStoreStatus;
pub use self::ffi::CWriteOp;

pub type CEloqStoreHandle = *mut c_void;
pub type CTableIdentHandle = *mut c_void;
pub type CScanRequestHandle = *mut c_void;
pub type CBatchWriteHandle = *mut c_void;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct CKvEntry {
    pub key: *const c_uchar,
    pub key_len: usize,
    pub value: *const c_uchar,
    pub value_len: usize,
    pub timestamp: u64,
    pub expire_ts: u64,
}

#[repr(C)]
pub struct CWriteEntry {
    pub key: *const c_uchar,
    pub key_len: usize,
    pub value: *const c_uchar,
    pub value_len: usize,
    pub timestamp: u64,
    pub op: CWriteOp,
    pub expire_ts: u64,
}

#[repr(C)]
pub struct CScanResult {
    pub entries: *mut CKvEntry,
    pub num_entries: usize,
    pub total_size: usize,
    pub has_more: bool,
}

#[repr(C)]
pub struct CGetResult {
    pub value: *const c_uchar,
    pub value_len: usize,
    pub timestamp: u64,
    pub expire_ts: u64,
    pub found: bool,
}

#[repr(C)]
pub struct CFloorResult {
    pub key: *const c_uchar,
    pub key_len: usize,
    pub value: *const c_uchar,
    pub value_len: usize,
    pub timestamp: u64,
    pub expire_ts: u64,
    pub found: bool,
}

mod ffi {
    #![allow(non_camel_case_types)]
    #![allow(non_snake_case)]
    #![allow(unused)]

    use std::os::raw::{c_char, c_uchar, c_uint, c_ulonglong, c_ushort, c_void};

    #[repr(u8)]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum CEloqStoreStatus {
        Ok = 0,
        InvalidArgs,
        NotFound,
        NotRunning,
        Corrupted,
        EndOfFile,
        OutOfSpace,
        OutOfMem,
        OpenFileLimit,
        TryAgain,
        Busy,
        Timeout,
        NoPermission,
        CloudErr,
        IoFail,
        ExpiredTerm,
        OssInsufficientStorage,
    }

    #[repr(u8)]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum CWriteOp {
        Upsert = 0,
        Delete = 1,
    }

    pub type CEloqStoreHandle = *mut c_void;
    pub type CTableIdentHandle = *mut c_void;
    pub type CScanRequestHandle = *mut c_void;
    pub type CBatchWriteHandle = *mut c_void;

    unsafe extern "C" {
        pub fn CEloqStore_Options_Create() -> CEloqStoreHandle;
        pub fn CEloqStore_Options_Destroy(opts: CEloqStoreHandle);
        pub fn CEloqStore_Options_SetNumThreads(opts: CEloqStoreHandle, n: c_ushort);
        pub fn CEloqStore_Options_SetBufferPoolSize(opts: CEloqStoreHandle, size: c_ulonglong);
        pub fn CEloqStore_Options_SetDataPageSize(opts: CEloqStoreHandle, size: c_ushort);
        pub fn CEloqStore_Options_AddStorePath(opts: CEloqStoreHandle, path: *const c_char);
        pub fn CEloqStore_Options_SetDataAppendMode(opts: CEloqStoreHandle, enable: bool);
        pub fn CEloqStore_Options_SetEnableCompression(opts: CEloqStoreHandle, enable: bool);
        pub fn CEloqStore_Options_SetCloudStorePath(opts: CEloqStoreHandle, path: *const c_char);
        pub fn CEloqStore_Options_SetCloudProvider(opts: CEloqStoreHandle, provider: *const c_char);
        pub fn CEloqStore_Options_SetCloudRegion(opts: CEloqStoreHandle, region: *const c_char);
        pub fn CEloqStore_Options_SetCloudCredentials(
            opts: CEloqStoreHandle,
            access_key: *const c_char,
            secret_key: *const c_char,
        );
        pub fn CEloqStore_Options_SetCloudVerifySsl(opts: CEloqStoreHandle, verify: bool);
        pub fn CEloqStore_Options_Validate(opts: CEloqStoreHandle) -> bool;

        pub fn CEloqStore_Create(options: CEloqStoreHandle) -> CEloqStoreHandle;
        pub fn CEloqStore_Destroy(store: CEloqStoreHandle);
        pub fn CEloqStore_Start(store: CEloqStoreHandle) -> CEloqStoreStatus;
        pub fn CEloqStore_Stop(store: CEloqStoreHandle);
        pub fn CEloqStore_IsStopped(store: CEloqStoreHandle) -> bool;

        pub fn CEloqStore_TableIdent_Create(
            tbl_name: *const c_char,
            partition_id: c_uint,
        ) -> CTableIdentHandle;
        pub fn CEloqStore_TableIdent_Destroy(ident: CTableIdentHandle);
        pub fn CEloqStore_TableIdent_GetName(ident: CTableIdentHandle) -> *const c_char;
        pub fn CEloqStore_TableIdent_GetPartition(ident: CTableIdentHandle) -> c_uint;

        pub fn CEloqStore_Put(
            store: CEloqStoreHandle,
            table: CTableIdentHandle,
            key: *const c_uchar,
            key_len: usize,
            value: *const c_uchar,
            value_len: usize,
            timestamp: u64,
        ) -> CEloqStoreStatus;

        pub fn CEloqStore_PutBatch(
            store: CEloqStoreHandle,
            table: CTableIdentHandle,
            keys: *const *const c_uchar,
            key_lens: *const usize,
            values: *const *const c_uchar,
            value_lens: *const usize,
            count: usize,
            timestamp: u64,
        ) -> CEloqStoreStatus;

        pub fn CEloqStore_PutEntries(
            store: CEloqStoreHandle,
            table: CTableIdentHandle,
            entries: *const super::CWriteEntry,
            count: usize,
        ) -> CEloqStoreStatus;

        pub fn CEloqStore_Delete(
            store: CEloqStoreHandle,
            table: CTableIdentHandle,
            key: *const c_uchar,
            key_len: usize,
            timestamp: u64,
        ) -> CEloqStoreStatus;

        pub fn CEloqStore_DeleteBatch(
            store: CEloqStoreHandle,
            table: CTableIdentHandle,
            keys: *const *const c_uchar,
            key_lens: *const usize,
            count: usize,
            timestamp: u64,
        ) -> CEloqStoreStatus;

        pub fn CEloqStore_Get(
            store: CEloqStoreHandle,
            table: CTableIdentHandle,
            key: *const c_uchar,
            key_len: usize,
            out_result: *mut super::CGetResult,
        ) -> CEloqStoreStatus;

        pub fn CEloqStore_Floor(
            store: CEloqStoreHandle,
            table: CTableIdentHandle,
            key: *const c_uchar,
            key_len: usize,
            out_result: *mut super::CFloorResult,
        ) -> CEloqStoreStatus;

        pub fn CEloqStore_ScanRequest_Create() -> CScanRequestHandle;
        pub fn CEloqStore_ScanRequest_Destroy(req: CScanRequestHandle);
        pub fn CEloqStore_ScanRequest_SetTable(req: CScanRequestHandle, table: CTableIdentHandle);
        pub fn CEloqStore_ScanRequest_SetRange(
            req: CScanRequestHandle,
            begin_key: *const c_uchar,
            begin_key_len: usize,
            begin_inclusive: bool,
            end_key: *const c_uchar,
            end_key_len: usize,
            end_inclusive: bool,
        );
        pub fn CEloqStore_ScanRequest_SetPagination(
            req: CScanRequestHandle,
            max_entries: usize,
            max_size: usize,
        );
        pub fn CEloqStore_ScanRequest_SetPrefetch(req: CScanRequestHandle, num_pages: usize);
        pub fn CEloqStore_ExecScan(
            store: CEloqStoreHandle,
            req: CScanRequestHandle,
            out_result: *mut super::CScanResult,
        ) -> CEloqStoreStatus;
        pub fn CEloqStore_FreeScanResult(result: *mut super::CScanResult);

        pub fn CEloqStore_BatchWrite_Create() -> CBatchWriteHandle;
        pub fn CEloqStore_BatchWrite_Destroy(req: CBatchWriteHandle);
        pub fn CEloqStore_BatchWrite_SetTable(req: CBatchWriteHandle, table: CTableIdentHandle);
        pub fn CEloqStore_BatchWrite_AddEntry(
            req: CBatchWriteHandle,
            key: *const c_uchar,
            key_len: usize,
            value: *const c_uchar,
            value_len: usize,
            timestamp: u64,
            op: CWriteOp,
            expire_ts: u64,
        );
        pub fn CEloqStore_BatchWrite_Clear(req: CBatchWriteHandle);
        pub fn CEloqStore_ExecBatchWrite(
            store: CEloqStoreHandle,
            req: CBatchWriteHandle,
        ) -> CEloqStoreStatus;

        pub fn CEloqStore_FreeGetResult(result: *mut super::CGetResult);
        pub fn CEloqStore_FreeFloorResult(result: *mut super::CFloorResult);

        pub fn CEloqStore_GetLastError(store: CEloqStoreHandle) -> *const c_char;
    }
}

pub use self::ffi::CEloqStore_Options_AddStorePath;
pub use self::ffi::CEloqStore_Options_Create;
pub use self::ffi::CEloqStore_Options_Destroy;
pub use self::ffi::CEloqStore_Options_SetBufferPoolSize;
pub use self::ffi::CEloqStore_Options_SetCloudCredentials;
pub use self::ffi::CEloqStore_Options_SetCloudProvider;
pub use self::ffi::CEloqStore_Options_SetCloudRegion;
pub use self::ffi::CEloqStore_Options_SetCloudStorePath;
pub use self::ffi::CEloqStore_Options_SetCloudVerifySsl;
pub use self::ffi::CEloqStore_Options_SetDataAppendMode;
pub use self::ffi::CEloqStore_Options_SetDataPageSize;
pub use self::ffi::CEloqStore_Options_SetEnableCompression;
pub use self::ffi::CEloqStore_Options_SetNumThreads;
pub use self::ffi::CEloqStore_Options_Validate;

pub use self::ffi::CEloqStore_Create;
pub use self::ffi::CEloqStore_Destroy;
pub use self::ffi::CEloqStore_IsStopped;
pub use self::ffi::CEloqStore_Start;
pub use self::ffi::CEloqStore_Stop;

pub use self::ffi::CEloqStore_TableIdent_Create;
pub use self::ffi::CEloqStore_TableIdent_Destroy;
pub use self::ffi::CEloqStore_TableIdent_GetName;
pub use self::ffi::CEloqStore_TableIdent_GetPartition;

pub use self::ffi::CEloqStore_Delete;
pub use self::ffi::CEloqStore_DeleteBatch;
pub use self::ffi::CEloqStore_Floor;
pub use self::ffi::CEloqStore_Get;
pub use self::ffi::CEloqStore_Put;
pub use self::ffi::CEloqStore_PutBatch;
pub use self::ffi::CEloqStore_PutEntries;

pub use self::ffi::CEloqStore_ExecScan;
pub use self::ffi::CEloqStore_FreeScanResult;
pub use self::ffi::CEloqStore_ScanRequest_Create;
pub use self::ffi::CEloqStore_ScanRequest_Destroy;
pub use self::ffi::CEloqStore_ScanRequest_SetPagination;
pub use self::ffi::CEloqStore_ScanRequest_SetPrefetch;
pub use self::ffi::CEloqStore_ScanRequest_SetRange;
pub use self::ffi::CEloqStore_ScanRequest_SetTable;

pub use self::ffi::CEloqStore_BatchWrite_AddEntry;
pub use self::ffi::CEloqStore_BatchWrite_Clear;
pub use self::ffi::CEloqStore_BatchWrite_Create;
pub use self::ffi::CEloqStore_BatchWrite_Destroy;
pub use self::ffi::CEloqStore_BatchWrite_SetTable;
pub use self::ffi::CEloqStore_ExecBatchWrite;

pub use self::ffi::CEloqStore_FreeFloorResult;
pub use self::ffi::CEloqStore_FreeGetResult;

pub use self::ffi::CEloqStore_GetLastError;
