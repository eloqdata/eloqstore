use libc::c_int;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KvError {
    NoError = 0,
    InvalidArgs = 1,
    NotFound = 2,
    NotRunning = 3,
    Corrupted = 4,
    EndOfFile = 5,
    OutOfSpace = 6,
    OutOfMem = 7,
    OpenFileLimit = 8,
    TryAgain = 9,
    Busy = 10,
    Timeout = 11,
    NoPermission = 12,
    CloudErr = 13,
    IoFail = 14,
    ExpiredTerm = 15,
    OssInsufficientStorage = 16,
    Unknown = 255,
}

impl KvError {
    pub fn from_raw(err: c_int) -> Self {
        match err {
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
            _ => {
                #[cfg(debug_assertions)]
                eprintln!("Unknown error code from C API: {}", err);
                KvError::Unknown
            }
        }
    }

    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            KvError::OpenFileLimit | KvError::Busy | KvError::TryAgain
        )
    }
}

impl std::fmt::Display for KvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let msg = match self {
            KvError::NoError => "Succeed",
            KvError::InvalidArgs => "Invalid arguments",
            KvError::NotFound => "Resource not found",
            KvError::NotRunning => "EloqStore is not running",
            KvError::Corrupted => "Disk data corrupted",
            KvError::EndOfFile => "End of file",
            KvError::OutOfSpace => "Out of disk space",
            KvError::OutOfMem => "Out of memory",
            KvError::OpenFileLimit => "Too many opened files",
            KvError::TryAgain => "Try again later",
            KvError::Busy => "Device or resource busy",
            KvError::IoFail => "I/O failure",
            KvError::CloudErr => "Cloud service is unavailable",
            KvError::Timeout => "Operation timeout",
            KvError::NoPermission => "Operation not permitted",
            KvError::ExpiredTerm => "Expired term",
            KvError::OssInsufficientStorage => "Object storage insufficient storage",
            KvError::Unknown => "Unknown error",
        };
        write!(f, "{}", msg)
    }
}

impl std::error::Error for KvError {}
