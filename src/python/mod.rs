//! Python bindings for PrismDB
//!
//! This module provides Python bindings using PyO3, allowing PrismDB to be used
//! from Python as `import prismdb`.

// Suppress non-local impl warning from PyO3 0.20 macros
#![allow(non_local_definitions)]

#[cfg(feature = "python")]
mod connection;
#[cfg(feature = "python")]
mod cursor;
#[cfg(feature = "python")]
mod result;
#[cfg(feature = "python")]
mod error;

#[cfg(feature = "python")]
pub use connection::*;
#[cfg(feature = "python")]
pub use cursor::*;
#[cfg(feature = "python")]
pub use result::*;
#[cfg(feature = "python")]
pub use error::*;

#[cfg(feature = "python")]
use pyo3::prelude::*;

/// Initialize the Python module
#[cfg(feature = "python")]
#[pymodule]
fn prismdb(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyPrismDB>()?;
    m.add_class::<PyCursor>()?;
    m.add_class::<PyQueryResult>()?;

    // Module metadata
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add("__author__", "PrismDB Contributors")?;

    // Convenience function
    m.add_function(wrap_pyfunction!(connect, m)?)?;

    Ok(())
}

/// Connect to a PrismDB database
///
/// Args:
///     path (str, optional): Path to database file. If None, creates an in-memory database.
///
/// Returns:
///     PyPrismDB: A connection to the database
///
/// Examples:
///     >>> import prismdb
///     >>> db = prismdb.connect()  # In-memory
///     >>> db = prismdb.connect('mydata.db')  # File-based
#[cfg(feature = "python")]
#[pyfunction]
#[pyo3(signature = (path=None))]
fn connect(path: Option<String>) -> PyResult<PyPrismDB> {
    PyPrismDB::new(path)
}
