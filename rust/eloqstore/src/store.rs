use eloqstore_sys::{self, CEloqStoreHandle, CTableIdentHandle};
use std::ffi::CString;
use std::path::Path;

use crate::{error::KvError, request::WriteRequest, traits::Request};

pub struct Options {
    ptr: CEloqStoreHandle,
}

impl Options {
    pub fn new() -> Result<Self, KvError> {
        // Ensure embedded library is available before calling FFI functions
        eloqstore_sys::ensure_library_loaded();
        let ptr = unsafe { eloqstore_sys::CEloqStore_Options_Create() };
        if ptr.is_null() {
            Err(KvError::OutOfMem)
        } else {
            Ok(Self { ptr })
        }
    }

    pub fn set_num_threads(&mut self, n: u32) {
        unsafe { eloqstore_sys::CEloqStore_Options_SetNumThreads(self.ptr, n) }
    }

    pub fn set_buffer_pool_size(&mut self, size: u64) {
        unsafe { eloqstore_sys::CEloqStore_Options_SetBufferPoolSize(self.ptr, size) }
    }

    pub fn set_data_page_size(&mut self, size: u32) {
        unsafe { eloqstore_sys::CEloqStore_Options_SetDataPageSize(self.ptr, size) }
    }

    pub fn add_store_path<P: AsRef<Path>>(&mut self, path: P) {
        let path = CString::new(path.as_ref().to_string_lossy().as_bytes()).unwrap();
        unsafe { eloqstore_sys::CEloqStore_Options_AddStorePath(self.ptr, path.as_ptr()) }
    }

    pub fn set_data_append_mode(&mut self, enable: bool) {
        unsafe { eloqstore_sys::CEloqStore_Options_SetDataAppendMode(self.ptr, enable) }
    }

    pub fn set_enable_compression(&mut self, enable: bool) {
        unsafe { eloqstore_sys::CEloqStore_Options_SetEnableCompression(self.ptr, enable) }
    }

    pub fn set_cloud_store_path(&mut self, path: &str) {
        let path = CString::new(path).unwrap();
        unsafe { eloqstore_sys::CEloqStore_Options_SetCloudStorePath(self.ptr, path.as_ptr()) }
    }

    pub fn set_cloud_provider(&mut self, provider: &str) {
        let provider = CString::new(provider).unwrap();
        unsafe { eloqstore_sys::CEloqStore_Options_SetCloudProvider(self.ptr, provider.as_ptr()) }
    }

    pub fn set_cloud_region(&mut self, region: &str) {
        let region = CString::new(region).unwrap();
        unsafe { eloqstore_sys::CEloqStore_Options_SetCloudRegion(self.ptr, region.as_ptr()) }
    }

    pub fn set_cloud_credentials(&mut self, access_key: &str, secret_key: &str) {
        let access_key = CString::new(access_key).unwrap();
        let secret_key = CString::new(secret_key).unwrap();
        unsafe {
            eloqstore_sys::CEloqStore_Options_SetCloudCredentials(
                self.ptr,
                access_key.as_ptr(),
                secret_key.as_ptr(),
            )
        }
    }

    pub fn set_cloud_verify_ssl(&mut self, verify: bool) {
        unsafe { eloqstore_sys::CEloqStore_Options_SetCloudVerifySsl(self.ptr, verify) }
    }

    pub fn validate(&self) -> bool {
        unsafe { eloqstore_sys::CEloqStore_Options_Validate(self.ptr) }
    }

    pub(crate) fn as_ptr(&self) -> CEloqStoreHandle {
        self.ptr
    }
}

impl Drop for Options {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe { eloqstore_sys::CEloqStore_Options_Destroy(self.ptr) }
        }
    }
}

#[derive(Debug)]
pub struct TableIdentifier {
    pub(crate) ptr: CTableIdentHandle,
}

impl TableIdentifier {
    pub fn new(name: &str, partition_id: u32) -> Result<Self, KvError> {
        let name = CString::new(name).unwrap();
        let ptr =
            unsafe { eloqstore_sys::CEloqStore_TableIdent_Create(name.as_ptr(), partition_id) };
        if ptr.is_null() {
            Err(KvError::OutOfMem)
        } else {
            Ok(Self { ptr })
        }
    }

    pub fn table_name(&self) -> String {
        unsafe {
            let c_str = eloqstore_sys::CEloqStore_TableIdent_GetName(self.ptr);
            if c_str.is_null() {
                String::new()
            } else {
                std::ffi::CStr::from_ptr(c_str)
                    .to_string_lossy()
                    .into_owned()
            }
        }
    }

    pub fn partition_id(&self) -> u32 {
        unsafe { eloqstore_sys::CEloqStore_TableIdent_GetPartition(self.ptr) }
    }
}

impl Clone for TableIdentifier {
    fn clone(&self) -> Self {
        let name = self.table_name();
        let partition = self.partition_id();
        TableIdentifier::new(&name, partition).unwrap()
    }
}

impl Drop for TableIdentifier {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe { eloqstore_sys::CEloqStore_TableIdent_Destroy(self.ptr) }
        }
    }
}

pub struct EloqStore {
    pub(crate) ptr: CEloqStoreHandle,
}

impl EloqStore {
    pub fn new(opts: &Options) -> Result<Self, KvError> {
        let ptr = unsafe { eloqstore_sys::CEloqStore_Create(opts.as_ptr()) };
        if ptr.is_null() {
            Err(KvError::OutOfMem)
        } else {
            Ok(Self { ptr })
        }
    }

    pub fn start(&mut self) -> Result<(), KvError> {
        let status = unsafe { eloqstore_sys::CEloqStore_Start(self.ptr) };
        match status {
            eloqstore_sys::CEloqStoreStatus::Ok => Ok(()),
            _ => Err(status.into()),
        }
    }

    pub fn stop(&mut self) {
        unsafe { eloqstore_sys::CEloqStore_Stop(self.ptr) }
    }

    pub fn is_stopped(&self) -> bool {
        unsafe { eloqstore_sys::CEloqStore_IsStopped(self.ptr) }
    }

    pub fn exec_sync<R: Request>(&self, req: R) -> Result<R::Response, KvError>
    where
        R: Request,
    {
        req.execute(self)
    }

    pub fn get(&self, tbl: &TableIdentifier, key: &[u8]) -> Result<Option<Vec<u8>>, KvError> {
        let req = crate::ReadRequest::new(tbl.clone(), key);
        let resp = self.exec_sync(req)?;
        if resp.value.is_empty() {
            Ok(None)
        } else {
            Ok(Some(resp.value))
        }
    }

    pub fn put(
        &self,
        tbl: &TableIdentifier,
        key: &[u8],
        value: &[u8],
        ts: u64,
    ) -> Result<(), KvError> {
        let req = WriteRequest::new(tbl.clone()).put(key, value, ts);
        self.exec_sync(req)?;
        Ok(())
    }

    pub fn delete(&self, tbl: &TableIdentifier, key: &[u8], ts: u64) -> Result<(), KvError> {
        let req = WriteRequest::new(tbl.clone()).delete(key, ts);
        self.exec_sync(req)?;
        Ok(())
    }

    pub fn put_batch(
        &self,
        tbl: &TableIdentifier,
        keys: &[&[u8]],
        values: &[&[u8]],
        ts: u64,
    ) -> Result<(), KvError> {
        let mut req = WriteRequest::new(tbl.clone());
        for (k, v) in keys.iter().zip(values.iter()) {
            req = req.put(k, v, ts);
        }
        self.exec_sync(req)?;
        Ok(())
    }

    pub fn delete_batch(
        &self,
        tbl: &TableIdentifier,
        keys: &[&[u8]],
        ts: u64,
    ) -> Result<(), KvError> {
        let mut req = WriteRequest::new(tbl.clone());
        for k in keys {
            req = req.delete(k, ts);
        }
        self.exec_sync(req)?;
        Ok(())
    }

    pub fn floor(
        &self,
        tbl: &TableIdentifier,
        key: &[u8],
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>, KvError> {
        let req = crate::FloorRequest::new(tbl.clone(), key);
        let resp = self.exec_sync(req)?;
        Ok(Some((resp.key, resp.value)))
    }

    pub fn scan(
        &self,
        tbl: &TableIdentifier,
        begin: &[u8],
        end: &[u8],
    ) -> Result<Vec<crate::KvEntry>, KvError> {
        let req = crate::ScanRequest::new(tbl.clone()).range(begin, end, true);
        let resp = self.exec_sync(req)?;
        Ok(resp.entries)
    }
}

impl Drop for EloqStore {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe { eloqstore_sys::CEloqStore_Destroy(self.ptr) }
        }
    }
}
