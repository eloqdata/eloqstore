use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_void};
use std::ptr;

pub type PageId = u32;
pub type FilePageId = u64;
pub type FileId = u64;
pub type Timestamp = u64;

pub const KB: usize = 1 << 10;
pub const MB: usize = 1 << 20;
pub const GB: usize = 1 << 30;

#[repr(C)]
pub struct TableIdent {
    tbl_name: *mut c_char,
    partition_id: u32,
}

impl TableIdent {
    pub fn new(tbl_name: &str, partition_id: u32) -> Result<Self, String> {
        let c_name = CString::new(tbl_name)
            .map_err(|e| format!("Invalid table name: contains null byte - {}", e))?;
        let ptr = c_name.into_raw();
        Ok(Self {
            tbl_name: ptr,
            partition_id,
        })
    }

    pub fn table_name(&self) -> Option<String> {
        if self.tbl_name.is_null() {
            None
        } else {
            unsafe {
                CStr::from_ptr(self.tbl_name)
                    .to_str()
                    .ok()
                    .map(|s| s.to_string())
            }
        }
    }

    pub fn partition_id(&self) -> u32 {
        self.partition_id
    }
}

impl Drop for TableIdent {
    fn drop(&mut self) {
        if !self.tbl_name.is_null() {
            unsafe {
                let _ = CString::from_raw(self.tbl_name);
            }
        }
    }
}

#[repr(C)]
pub struct KvEntry {
    key: *mut c_char,
    key_len: usize,
    value: *mut c_char,
    value_len: usize,
    timestamp: Timestamp,
    expire_ts: Timestamp,
}

impl KvEntry {
    pub fn key(&self) -> Option<&[u8]> {
        if self.key.is_null() || self.key_len == 0 {
            None
        } else {
            unsafe {
                Some(std::slice::from_raw_parts(
                    self.key as *const u8,
                    self.key_len,
                ))
            }
        }
    }

    pub fn value(&self) -> Option<&[u8]> {
        if self.value.is_null() || self.value_len == 0 {
            None
        } else {
            unsafe {
                Some(std::slice::from_raw_parts(
                    self.value as *const u8,
                    self.value_len,
                ))
            }
        }
    }

    pub fn timestamp(&self) -> Timestamp {
        self.timestamp
    }

    pub fn expire_ts(&self) -> Timestamp {
        self.expire_ts
    }
}

#[repr(C)]
pub struct WriteDataEntry {
    key: *mut c_char,
    key_len: usize,
    value: *mut c_char,
    value_len: usize,
    timestamp: Timestamp,
    op: u8,
    expire_ts: Timestamp,
}

impl WriteDataEntry {
    pub fn key(&self) -> Option<&[u8]> {
        if self.key.is_null() || self.key_len == 0 {
            None
        } else {
            unsafe {
                Some(std::slice::from_raw_parts(
                    self.key as *const u8,
                    self.key_len,
                ))
            }
        }
    }

    pub fn value(&self) -> Option<&[u8]> {
        if self.value.is_null() || self.value_len == 0 {
            None
        } else {
            unsafe {
                Some(std::slice::from_raw_parts(
                    self.value as *const u8,
                    self.value_len,
                ))
            }
        }
    }

    pub fn timestamp(&self) -> Timestamp {
        self.timestamp
    }

    pub fn op(&self) -> WriteOp {
        if self.op == 0 {
            WriteOp::Upsert
        } else {
            WriteOp::Delete
        }
    }

    pub fn expire_ts(&self) -> Timestamp {
        self.expire_ts
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteOp {
    Upsert = 0,
    Delete = 1,
}

#[repr(C)]
pub struct KvOptions {
    num_threads: u16,
    buffer_pool_size: u64,
    manifest_limit: u32,
    fd_limit: u32,
    data_page_size: u16,
    pages_per_file_shift: u8,
    overflow_pointers: u8,
    data_append_mode: bool,
    enable_compression: bool,
    cloud_store_path: *mut c_char,
    cloud_provider: *mut c_char,
    cloud_region: *mut c_char,
    cloud_access_key: *mut c_char,
    cloud_secret_key: *mut c_char,
    cloud_verify_ssl: bool,
}

impl Default for KvOptions {
    fn default() -> Self {
        Self {
            num_threads: 1,
            buffer_pool_size: 32 * MB as u64,
            manifest_limit: 8 * MB as u32,
            fd_limit: 10000,
            data_page_size: 4 * KB as u16,
            pages_per_file_shift: 18,
            overflow_pointers: 16,
            data_append_mode: false,
            enable_compression: false,
            cloud_store_path: ptr::null_mut(),
            cloud_provider: ptr::null_mut(),
            cloud_region: ptr::null_mut(),
            cloud_access_key: ptr::null_mut(),
            cloud_secret_key: ptr::null_mut(),
            cloud_verify_ssl: false,
        }
    }
}

impl KvOptions {
    /// Helper function to safely set a CString pointer, freeing the old one if it exists
    fn set_cstring_ptr(ptr: &mut *mut c_char, value: &str) {
        unsafe {
            // Free the old CString if it exists
            if !ptr.is_null() {
                let _ = CString::from_raw(*ptr);
            }
        }
        // Create new CString and take ownership of the raw pointer
        let c_str = CString::new(value).unwrap();
        *ptr = c_str.into_raw();
    }

    pub fn with_path<P: AsRef<std::path::Path>>(path: P) -> Self {
        let mut opts = Self::default();
        opts.set_store_path(path.as_ref());
        opts
    }

    pub fn set_num_threads(&mut self, n: u16) {
        self.num_threads = n;
    }

    pub fn set_buffer_pool_size(&mut self, size: usize) {
        self.buffer_pool_size = size as u64;
    }

    pub fn set_data_page_size(&mut self, size: u16) {
        self.data_page_size = size;
    }

    pub fn set_store_path<P: AsRef<std::path::Path>>(&mut self, path: P) {
        let path_str = path.as_ref().to_string_lossy();
        Self::set_cstring_ptr(&mut self.cloud_store_path, &path_str);
    }

    pub fn set_cloud_credentials(
        &mut self,
        provider: &str,
        region: &str,
        access_key: &str,
        secret_key: &str,
    ) {
        Self::set_cstring_ptr(&mut self.cloud_provider, provider);
        Self::set_cstring_ptr(&mut self.cloud_region, region);
        Self::set_cstring_ptr(&mut self.cloud_access_key, access_key);
        Self::set_cstring_ptr(&mut self.cloud_secret_key, secret_key);
    }
}

impl Drop for KvOptions {
    fn drop(&mut self) {
        unsafe {
            if !self.cloud_store_path.is_null() {
                let _ = CString::from_raw(self.cloud_store_path);
            }
            if !self.cloud_provider.is_null() {
                let _ = CString::from_raw(self.cloud_provider);
            }
            if !self.cloud_region.is_null() {
                let _ = CString::from_raw(self.cloud_region);
            }
            if !self.cloud_access_key.is_null() {
                let _ = CString::from_raw(self.cloud_access_key);
            }
            if !self.cloud_secret_key.is_null() {
                let _ = CString::from_raw(self.cloud_secret_key);
            }
        }
    }
}

#[repr(C)]
pub struct ScanResult {
    entries: *mut KvEntry,
    num_entries: usize,
    total_size: usize,
    has_more: bool,
}

impl ScanResult {
    pub fn entries(&self) -> &[KvEntry] {
        if self.entries.is_null() || self.num_entries == 0 {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(self.entries, self.num_entries) }
        }
    }

    pub fn has_more(&self) -> bool {
        self.has_more
    }
}
