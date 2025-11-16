//! Python error handling for PrismDB

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use crate::common::error::PrismDBError;

/// Convert PrismDB errors to Python exceptions
pub fn to_py_err(error: PrismDBError) -> PyErr {
    match error {
        PrismDBError::InvalidValue(msg) => PyValueError::new_err(msg),
        PrismDBError::InvalidArgument(msg) => PyValueError::new_err(msg),
        PrismDBError::InvalidType(msg) => PyValueError::new_err(msg),
        PrismDBError::NotImplemented(msg) => PyRuntimeError::new_err(format!("Not implemented: {}", msg)),
        PrismDBError::Parse(msg) => PyRuntimeError::new_err(format!("Parse error: {}", msg)),
        PrismDBError::Type(msg) => PyValueError::new_err(format!("Type error: {}", msg)),
        PrismDBError::Execution(msg) => PyRuntimeError::new_err(format!("Execution error: {}", msg)),
        PrismDBError::Storage(msg) => PyRuntimeError::new_err(format!("Storage error: {}", msg)),
        PrismDBError::Transaction(msg) => PyRuntimeError::new_err(format!("Transaction error: {}", msg)),
        PrismDBError::Catalog(msg) => PyRuntimeError::new_err(format!("Catalog error: {}", msg)),
        PrismDBError::Io(e) => PyRuntimeError::new_err(format!("IO error: {}", e)),
        PrismDBError::Internal(msg) => PyRuntimeError::new_err(format!("Internal error: {}", msg)),
        PrismDBError::Serialization(msg) => PyRuntimeError::new_err(format!("Serialization error: {}", msg)),
        PrismDBError::Compression(msg) => PyRuntimeError::new_err(format!("Compression error: {}", msg)),
        PrismDBError::Extension(msg) => PyRuntimeError::new_err(format!("Extension error: {}", msg)),
        PrismDBError::Wal(msg) => PyRuntimeError::new_err(format!("WAL error: {}", msg)),
        PrismDBError::OutOfMemory => PyRuntimeError::new_err("Out of memory"),
    }
}
