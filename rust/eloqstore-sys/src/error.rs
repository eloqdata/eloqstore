use libc::c_int;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KvError {
    NoError = 0,
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
            _ => KvError::NoError,
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
        };
        write!(f, "{}", msg)
    }
}

impl std::error::Error for KvError {}
