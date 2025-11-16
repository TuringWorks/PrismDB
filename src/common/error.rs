//! Error handling for PrismDB Rust port

use thiserror::Error;

/// Main error type for PrismDB operations
#[derive(Error, Debug)]
pub enum PrismDBError {
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    #[error("Invalid value: {0}")]
    InvalidValue(String),

    #[error("Invalid type: {0}")]
    InvalidType(String),

    #[error("Out of memory")]
    OutOfMemory,

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Parse error: {0}")]
    Parse(String),

    #[error("Type error: {0}")]
    Type(String),

    #[error("Transaction error: {0}")]
    Transaction(String),

    #[error("Catalog error: {0}")]
    Catalog(String),

    #[error("Execution error: {0}")]
    Execution(String),

    #[error("Not implemented: {0}")]
    NotImplemented(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Compression error: {0}")]
    Compression(String),

    #[error("Extension error: {0}")]
    Extension(String),

    #[error("WAL error: {0}")]
    Wal(String),

    #[error("Storage error: {0}")]
    Storage(String),
}

/// Result type alias for convenience
pub type Result<T> = std::result::Result<T, PrismDBError>;

/// Result type alias for PrismDB operations (alias for Result)
pub type PrismDBResult<T> = std::result::Result<T, PrismDBError>;

/// Macro for creating internal errors
#[macro_export]
macro_rules! internal_err {
    ($msg:expr) => {
        $crate::common::error::PrismDBError::Internal($msg.to_string())
    };
    ($fmt:expr, $($arg:tt)*) => {
        $crate::common::error::PrismDBError::Internal(format!($fmt, $($arg)*))
    };
}

/// Macro for creating not implemented errors
#[macro_export]
macro_rules! not_implemented_err {
    ($msg:expr) => {
        $crate::common::error::PrismDBError::NotImplemented($msg.to_string())
    };
    ($fmt:expr, $($arg:tt)*) => {
        $crate::common::error::PrismDBError::NotImplemented(format!($fmt, $($arg)*))
    };
}
