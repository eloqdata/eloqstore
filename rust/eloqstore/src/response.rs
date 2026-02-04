pub trait Response {}

#[derive(Debug, Clone)]
pub struct KvEntry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub timestamp: u64,
    pub expire_ts: u64,
}

impl Response for KvEntry {}

#[derive(Debug, Clone)]
pub struct ReadResponse {
    pub value: Vec<u8>,
    pub timestamp: u64,
    pub expire_ts: u64,
}

impl Response for ReadResponse {}

#[derive(Debug, Clone)]
pub struct FloorResponse {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub timestamp: u64,
    pub expire_ts: u64,
}

impl Response for FloorResponse {}

#[derive(Debug, Clone)]
pub struct ScanResponse {
    pub entries: Vec<KvEntry>,
    pub has_more: bool,
}

impl Response for ScanResponse {}

#[derive(Debug, Clone)]
pub struct WriteResponse {
    pub success: bool,
}

impl Response for WriteResponse {}
