use super::*;

#[derive(Debug)]
pub struct ReadRequest {
    table: TableIdentifier,
    key: Vec<u8>,
}

impl ReadRequest {
    pub fn new(table: TableIdentifier, key: &[u8]) -> Self {
        Self {
            table,
            key: key.to_vec(),
        }
    }
}

impl Request for ReadRequest {
    type Response = ReadResponse;

    fn execute(&self, store: &EloqStore) -> Result<Self::Response, KvError> {
        unsafe {
            let mut result: eloqstore_sys::CGetResult = std::mem::zeroed();
            let status = eloqstore_sys::CEloqStore_Get(
                store.ptr,
                self.table.ptr,
                self.key.as_ptr(),
                self.key.len(),
                &mut result,
            );

            match status {
                eloqstore_sys::CEloqStoreStatus::Ok if result.found => {
                    let value = std::slice::from_raw_parts(result.value, result.value_len).to_vec();
                    // Save timestamp and expire_ts before freeing the result
                    let timestamp = result.timestamp;
                    let expire_ts = result.expire_ts;
                    eloqstore_sys::CEloqStore_FreeGetResult(&mut result);
                    Ok(ReadResponse {
                        value,
                        timestamp,
                        expire_ts,
                    })
                }
                eloqstore_sys::CEloqStoreStatus::Ok => {
                    if !result.value.is_null() {
                        eloqstore_sys::CEloqStore_FreeGetResult(&mut result);
                    }
                    Err(KvError::NotFound)
                }
                _ => Err(status.into()),
            }
        }
    }
}

#[derive(Debug)]
pub struct FloorRequest {
    table: TableIdentifier,
    key: Vec<u8>,
}

impl FloorRequest {
    pub fn new(table: TableIdentifier, key: &[u8]) -> Self {
        Self {
            table,
            key: key.to_vec(),
        }
    }
}

impl Request for FloorRequest {
    type Response = FloorResponse;

    fn execute(&self, store: &EloqStore) -> Result<Self::Response, KvError> {
        unsafe {
            let mut result: eloqstore_sys::CFloorResult = std::mem::zeroed();
            let status = eloqstore_sys::CEloqStore_Floor(
                store.ptr,
                self.table.ptr,
                self.key.as_ptr(),
                self.key.len(),
                &mut result,
            );

            match status {
                eloqstore_sys::CEloqStoreStatus::Ok if result.found => {
                    let key = std::slice::from_raw_parts(result.key, result.key_len).to_vec();
                    let value = std::slice::from_raw_parts(result.value, result.value_len).to_vec();
                    // Save timestamp and expire_ts before freeing the result
                    let timestamp = result.timestamp;
                    let expire_ts = result.expire_ts;
                    eloqstore_sys::CEloqStore_FreeFloorResult(&mut result);
                    Ok(FloorResponse {
                        key,
                        value,
                        timestamp,
                        expire_ts,
                    })
                }
                eloqstore_sys::CEloqStoreStatus::Ok => Err(KvError::NotFound),
                _ => Err(status.into()),
            }
        }
    }
}

#[derive(Debug)]
pub struct ScanRequest {
    table: TableIdentifier,
    begin: Vec<u8>,
    end: Vec<u8>,
    begin_inclusive: bool,
    max_entries: usize,
    max_size: usize,
}

impl ScanRequest {
    pub fn new(table: TableIdentifier) -> Self {
        Self {
            table,
            begin: vec![],
            end: vec![],
            begin_inclusive: true,
            max_entries: usize::MAX,
            max_size: usize::MAX,
        }
    }

    pub fn range(mut self, begin: &[u8], end: &[u8], inclusive: bool) -> Self {
        self.begin = begin.to_vec();
        self.end = end.to_vec();
        self.begin_inclusive = inclusive;
        self
    }

    pub fn pagination(mut self, max_entries: usize, max_size: usize) -> Self {
        self.max_entries = max_entries;
        self.max_size = max_size;
        self
    }
}

impl Request for ScanRequest {
    type Response = ScanResponse;

    fn execute(&self, store: &EloqStore) -> Result<Self::Response, KvError> {
        unsafe {
            let req = eloqstore_sys::CEloqStore_ScanRequest_Create();
            if req.is_null() {
                return Err(KvError::OutOfMem);
            }

            eloqstore_sys::CEloqStore_ScanRequest_SetTable(req, self.table.ptr);

            if !self.begin.is_empty() && !self.end.is_empty() {
                eloqstore_sys::CEloqStore_ScanRequest_SetRange(
                    req,
                    self.begin.as_ptr(),
                    self.begin.len(),
                    self.begin_inclusive,
                    self.end.as_ptr(),
                    self.end.len(),
                    true,
                );
            }

            if self.max_entries != usize::MAX || self.max_size != usize::MAX {
                eloqstore_sys::CEloqStore_ScanRequest_SetPagination(
                    req,
                    self.max_entries,
                    self.max_size,
                );
            }

            let mut result: eloqstore_sys::CScanResult = std::mem::zeroed();
            let status = eloqstore_sys::CEloqStore_ExecScan(store.ptr, req, &mut result);

            let response = match status {
                eloqstore_sys::CEloqStoreStatus::Ok => {
                    let mut entries: Vec<KvEntry> = Vec::with_capacity(result.num_entries);
                    for i in 0..result.num_entries {
                        let e = *result.entries.add(i);
                        let key = std::slice::from_raw_parts(e.key, e.key_len).to_vec();
                        let value = std::slice::from_raw_parts(e.value, e.value_len).to_vec();
                        entries.push(KvEntry {
                            key,
                            value,
                            timestamp: e.timestamp,
                            expire_ts: e.expire_ts,
                        });
                    }
                    // Save has_more before freeing the result
                    let has_more = result.has_more;
                    eloqstore_sys::CEloqStore_FreeScanResult(&mut result);
                    ScanResponse {
                        entries,
                        has_more,
                    }
                }
                _ => {
                    eloqstore_sys::CEloqStore_ScanRequest_Destroy(req);
                    return Err(status.into());
                }
            };

            eloqstore_sys::CEloqStore_ScanRequest_Destroy(req);
            Ok(response)
        }
    }
}

#[derive(Debug)]
pub struct WriteRequest {
    table: TableIdentifier,
    entries: Vec<WriteEntry>,
}

#[derive(Debug)]
pub struct WriteEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub timestamp: u64,
    pub op: WriteOp,
    pub expire_ts: u64,
}

#[derive(Debug, Clone, Copy)]
pub enum WriteOp {
    Upsert,
    Delete,
}

impl WriteRequest {
    pub fn new(table: TableIdentifier) -> Self {
        Self {
            table,
            entries: vec![],
        }
    }

    pub fn put(mut self, key: &[u8], value: &[u8], timestamp: u64) -> Self {
        self.entries.push(WriteEntry {
            key: key.to_vec(),
            value: value.to_vec(),
            timestamp,
            op: WriteOp::Upsert,
            expire_ts: 0,
        });
        self
    }

    pub fn delete(mut self, key: &[u8], timestamp: u64) -> Self {
        self.entries.push(WriteEntry {
            key: key.to_vec(),
            value: vec![],
            timestamp,
            op: WriteOp::Delete,
            expire_ts: 0,
        });
        self
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl Request for WriteRequest {
    type Response = WriteResponse;

    fn execute(&self, store: &EloqStore) -> Result<Self::Response, KvError> {
        if self.entries.is_empty() {
            return Ok(WriteResponse { success: true });
        }

        if self.entries.len() == 1 {
            self.execute_single(store)
        } else {
            self.execute_batch(store)
        }
    }
}

impl WriteRequest {
    fn execute_single(&self, store: &EloqStore) -> Result<WriteResponse, KvError> {
        let entry = &self.entries[0];
        let status = unsafe {
            match entry.op {
                WriteOp::Upsert => eloqstore_sys::CEloqStore_Put(
                    store.ptr,
                    self.table.ptr,
                    entry.key.as_ptr(),
                    entry.key.len(),
                    entry.value.as_ptr(),
                    entry.value.len(),
                    entry.timestamp,
                ),
                WriteOp::Delete => eloqstore_sys::CEloqStore_Delete(
                    store.ptr,
                    self.table.ptr,
                    entry.key.as_ptr(),
                    entry.key.len(),
                    entry.timestamp,
                ),
            }
        };
        match status {
            eloqstore_sys::CEloqStoreStatus::Ok => Ok(WriteResponse { success: true }),
            _ => Err(status.into()),
        }
    }

    fn execute_batch(&self, store: &EloqStore) -> Result<WriteResponse, KvError> {
        unsafe {
            let mut c_entries: Vec<eloqstore_sys::CWriteEntry> =
                Vec::with_capacity(self.entries.len());
            for e in &self.entries {
                c_entries.push(eloqstore_sys::CWriteEntry {
                    key: e.key.as_ptr(),
                    key_len: e.key.len(),
                    value: e.value.as_ptr(),
                    value_len: e.value.len(),
                    timestamp: e.timestamp,
                    op: match e.op {
                        WriteOp::Upsert => eloqstore_sys::CWriteOp::Upsert,
                        WriteOp::Delete => eloqstore_sys::CWriteOp::Delete,
                    },
                    expire_ts: e.expire_ts,
                });
            }

            let status = eloqstore_sys::CEloqStore_PutEntries(
                store.ptr,
                self.table.ptr,
                c_entries.as_ptr(),
                c_entries.len(),
            );

            match status {
                eloqstore_sys::CEloqStoreStatus::Ok => Ok(WriteResponse { success: true }),
                _ => Err(status.into()),
            }
        }
    }
}
