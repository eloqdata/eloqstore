use eloqstore_sys::CEloqStoreStatus;

#[derive(Debug, Clone, PartialEq)]
pub enum KvError {
    NoError,
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
    Unknown,
}

impl From<CEloqStoreStatus> for KvError {
    fn from(status: CEloqStoreStatus) -> Self {
        match status as u8 {
            0 => KvError::NoError,
            1 => KvError::InvalidArgs,
            2 => KvError::NotFound,
            3 => KvError::NotRunning,
            4 => KvError::Corrupted,
            5 => KvError::EndOfFile,
            6 => KvError::OutOfSpace,
            7 => KvError::OutOfMem,
            8 => KvError::OpenFileLimit,
            9 => KvError::TryAgain,
            10 => KvError::Busy,
            11 => KvError::Timeout,
            12 => KvError::NoPermission,
            13 => KvError::CloudErr,
            14 => KvError::IoFail,
            15 => KvError::ExpiredTerm,
            16 => KvError::OssInsufficientStorage,
            _ => KvError::Unknown,
        }
    }
}

impl std::fmt::Display for KvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KvError::NoError => write!(f, "no error"),
            KvError::InvalidArgs => write!(f, "invalid arguments"),
            KvError::NotFound => write!(f, "not found"),
            KvError::NotRunning => write!(f, "not running"),
            KvError::Corrupted => write!(f, "data corrupted"),
            KvError::EndOfFile => write!(f, "end of file"),
            KvError::OutOfSpace => write!(f, "out of space"),
            KvError::OutOfMem => write!(f, "out of memory"),
            KvError::OpenFileLimit => write!(f, "open file limit"),
            KvError::TryAgain => write!(f, "try again"),
            KvError::Busy => write!(f, "busy"),
            KvError::Timeout => write!(f, "timeout"),
            KvError::NoPermission => write!(f, "no permission"),
            KvError::CloudErr => write!(f, "cloud error"),
            KvError::IoFail => write!(f, "I/O failure"),
            KvError::ExpiredTerm => write!(f, "expired term"),
            KvError::OssInsufficientStorage => write!(f, "object storage insufficient storage"),
            KvError::Unknown => write!(f, "unknown error"),
        }
    }
}

impl std::error::Error for KvError {}

impl std::convert::From<KvError> for std::io::Error {
    fn from(err: KvError) -> Self {
        use std::io::ErrorKind::*;
        match err {
            KvError::NoError => Self::new(Other, "ok"),
            KvError::InvalidArgs => Self::new(InvalidInput, "invalid arguments"),
            KvError::NotFound => Self::new(NotFound, "not found"),
            KvError::NotRunning => Self::new(Other, "not running"),
            KvError::Corrupted => Self::new(Other, "data corrupted"),
            KvError::EndOfFile => Self::new(UnexpectedEof, "end of file"),
            KvError::OutOfSpace => Self::new(StorageFull, "out of space"),
            KvError::OutOfMem => Self::new(Other, "out of memory"),
            KvError::OpenFileLimit => Self::new(Other, "open file limit"),
            KvError::TryAgain => Self::new(Other, "try again"),
            KvError::Busy => Self::new(Other, "busy"),
            KvError::Timeout => Self::new(TimedOut, "timeout"),
            KvError::NoPermission => Self::new(PermissionDenied, "no permission"),
            KvError::CloudErr => Self::new(Other, "cloud error"),
            KvError::IoFail => Self::new(Other, "I/O failure"),
            KvError::ExpiredTerm => Self::new(Other, "expired term"),
            KvError::OssInsufficientStorage => Self::new(StorageFull, "object storage insufficient storage"),
            KvError::Unknown => Self::new(Other, "unknown error"),
        }
    }
}
