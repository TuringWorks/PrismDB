//! Python connection class for PrismDB

use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use crate::Database;
use super::cursor::PyCursor;
use super::result::PyQueryResult;

/// PrismDB database connection
///
/// This class represents a connection to a PrismDB database.
/// It can be used to execute SQL queries and manage transactions.
#[pyclass(name = "Connection")]
pub struct PyPrismDB {
    pub(crate) db: Database,
}

#[pymethods]
impl PyPrismDB {
    /// Create a new database connection
    ///
    /// Args:
    ///     path (str, optional): Path to database file. If None, creates an in-memory database.
    ///
    /// Returns:
    ///     Connection: A new database connection
    ///
    /// Examples:
    ///     >>> db = prismdb.Connection()  # In-memory
    ///     >>> db = prismdb.Connection('mydata.db')  # File-based
    #[new]
    #[pyo3(signature = (path=None))]
    pub fn new(path: Option<String>) -> PyResult<Self> {
        let db = if let Some(p) = path {
            Database::open(p).map_err(|e| PyRuntimeError::new_err(format!("Failed to open database: {}", e)))?
        } else {
            Database::new_in_memory().map_err(|e| PyRuntimeError::new_err(format!("Failed to create in-memory database: {}", e)))?
        };
        Ok(PyPrismDB { db })
    }

    /// Execute a SQL query and return results
    ///
    /// Args:
    ///     sql (str): SQL query to execute
    ///
    /// Returns:
    ///     QueryResult: Query results
    ///
    /// Examples:
    ///     >>> result = db.execute("SELECT * FROM users")
    ///     >>> for row in result:
    ///     ...     print(row)
    pub fn execute(&self, sql: &str) -> PyResult<PyQueryResult> {
        let result = self.db.execute_sql_collect(sql)
            .map_err(|e| PyRuntimeError::new_err(format!("Query execution failed: {}", e)))?;
        Ok(PyQueryResult::new(result))
    }

    /// Execute a SQL statement (no results expected)
    ///
    /// Args:
    ///     sql (str): SQL statement to execute
    ///
    /// Returns:
    ///     int: Number of affected rows
    ///
    /// Examples:
    ///     >>> db.execute_many("CREATE TABLE users (id INTEGER, name VARCHAR)")
    ///     0
    ///     >>> db.execute_many("INSERT INTO users VALUES (1, 'Alice')")
    ///     1
    pub fn execute_many(&self, sql: &str) -> PyResult<usize> {
        let result = self.db.execute_sql_collect(sql)
            .map_err(|e| PyRuntimeError::new_err(format!("Query execution failed: {}", e)))?;
        Ok(result.row_count())
    }

    /// Create a cursor for executing queries
    ///
    /// Returns:
    ///     Cursor: A new cursor object
    ///
    /// Examples:
    ///     >>> cursor = db.cursor()
    ///     >>> cursor.execute("SELECT * FROM users")
    ///     >>> rows = cursor.fetchall()
    pub fn cursor(&self) -> PyResult<PyCursor> {
        Ok(PyCursor::new(self.db.clone()))
    }

    /// Execute a SQL query and fetch all results
    ///
    /// Args:
    ///     sql (str): SQL query to execute
    ///
    /// Returns:
    ///     list: List of rows (each row is a list of values)
    ///
    /// Examples:
    ///     >>> rows = db.sql("SELECT * FROM users").fetchall()
    ///     >>> print(rows)
    ///     [[1, 'Alice'], [2, 'Bob']]
    pub fn sql(&self, sql: &str) -> PyResult<PyQueryResult> {
        self.execute(sql)
    }

    /// Convert query result to a dictionary
    ///
    /// Args:
    ///     sql (str): SQL query to execute
    ///
    /// Returns:
    ///     dict: Dictionary with column names as keys
    ///
    /// Examples:
    ///     >>> data = db.to_dict("SELECT * FROM users")
    ///     >>> print(data)
    ///     {'id': [1, 2], 'name': ['Alice', 'Bob']}
    pub fn to_dict(&self, sql: &str, py: Python) -> PyResult<PyObject> {
        let result = self.execute(sql)?;
        result.to_dict(py)
    }

    /// Close the database connection
    ///
    /// Examples:
    ///     >>> db.close()
    pub fn close(&self) -> PyResult<()> {
        // Database will be closed when dropped
        Ok(())
    }

    /// String representation
    fn __repr__(&self) -> String {
        "PrismDB(connected)".to_string()
    }

    /// Context manager entry
    fn __enter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    /// Context manager exit
    fn __exit__(&self, _exc_type: PyObject, _exc_value: PyObject, _traceback: PyObject) -> PyResult<bool> {
        self.close()?;
        Ok(false)
    }
}
