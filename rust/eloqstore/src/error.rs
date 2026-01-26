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
}

impl From<CEloqStoreStatus> for KvError {
    fn from(status: CEloqStoreStatus) -> Self {
        match status {
            CEloqStoreStatus::Ok => KvError::NoError,
            CEloqStoreStatus::InvalidArgs => KvError::InvalidArgs,
            CEloqStoreStatus::NotFound => KvError::NotFound,
            CEloqStoreStatus::NotRunning => KvError::NotRunning,
            CEloqStoreStatus::Corrupted => KvError::Corrupted,
            CEloqStoreStatus::EndOfFile => KvError::EndOfFile,
            CEloqStoreStatus::OutOfSpace => KvError::OutOfSpace,
            CEloqStoreStatus::OutOfMem => KvError::OutOfMem,
            CEloqStoreStatus::OpenFileLimit => KvError::OpenFileLimit,
            CEloqStoreStatus::TryAgain => KvError::TryAgain,
            CEloqStoreStatus::Busy => KvError::Busy,
            CEloqStoreStatus::Timeout => KvError::Timeout,
            CEloqStoreStatus::NoPermission => KvError::NoPermission,
            CEloqStoreStatus::CloudErr => KvError::CloudErr,
            CEloqStoreStatus::IoFail => KvError::IoFail,
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
        }
    }
}
