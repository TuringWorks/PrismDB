//! Python cursor class for PrismDB

use pyo3::prelude::*;
use pyo3::exceptions::PyRuntimeError;
use crate::Database;
use super::result::PyQueryResult;

/// Database cursor for executing queries
///
/// A cursor allows you to execute queries and fetch results incrementally.
#[pyclass(name = "Cursor")]
pub struct PyCursor {
    pub(crate) db: Database,
    pub(crate) last_result: Option<PyQueryResult>,
}

impl PyCursor {
    pub fn new(db: Database) -> Self {
        Self {
            db,
            last_result: None,
        }
    }
}

#[pymethods]
impl PyCursor {
    /// Execute a SQL query
    ///
    /// Args:
    ///     sql (str): SQL query to execute
    ///     parameters (tuple, optional): Query parameters (not yet implemented)
    ///
    /// Returns:
    ///     Cursor: Self for method chaining
    ///
    /// Examples:
    ///     >>> cursor.execute("SELECT * FROM users")
    ///     >>> cursor.execute("SELECT * FROM users WHERE id = ?", (1,))
    #[pyo3(signature = (sql, parameters=None))]
    pub fn execute(&mut self, sql: &str, parameters: Option<Vec<PyObject>>) -> PyResult<()> {
        if parameters.is_some() {
            return Err(PyRuntimeError::new_err("Parameterized queries not yet supported"));
        }

        let result = self.db.execute_sql_collect(sql)
            .map_err(|e| PyRuntimeError::new_err(format!("Query execution failed: {}", e)))?;

        self.last_result = Some(PyQueryResult::new(result));
        Ok(())
    }

    /// Execute a SQL query with multiple parameter sets
    ///
    /// Args:
    ///     sql (str): SQL query to execute
    ///     seq_of_parameters (list): List of parameter tuples
    ///
    /// Examples:
    ///     >>> cursor.executemany("INSERT INTO users VALUES (?, ?)",
    ///     ...                    [(1, 'Alice'), (2, 'Bob')])
    pub fn executemany(&mut self, sql: &str, seq_of_parameters: Vec<Vec<PyObject>>) -> PyResult<()> {
        if !seq_of_parameters.is_empty() {
            return Err(PyRuntimeError::new_err("Parameterized queries not yet supported"));
        }

        self.execute(sql, None)
    }

    /// Fetch the next row from the result set
    ///
    /// Returns:
    ///     list or None: Next row as a list, or None if no more rows
    ///
    /// Examples:
    ///     >>> cursor.execute("SELECT * FROM users")
    ///     >>> row = cursor.fetchone()
    ///     >>> print(row)
    ///     [1, 'Alice']
    pub fn fetchone(&mut self, py: Python) -> PyResult<Option<PyObject>> {
        match &self.last_result {
            Some(result) => result.fetchone(py),
            None => Ok(None),
        }
    }

    /// Fetch multiple rows from the result set
    ///
    /// Args:
    ///     size (int, optional): Number of rows to fetch. If None, fetch all.
    ///
    /// Returns:
    ///     list: List of rows
    ///
    /// Examples:
    ///     >>> cursor.execute("SELECT * FROM users")
    ///     >>> rows = cursor.fetchmany(10)
    ///     >>> print(len(rows))
    ///     10
    #[pyo3(signature = (size=None))]
    pub fn fetchmany(&mut self, size: Option<usize>, py: Python) -> PyResult<Vec<PyObject>> {
        match &self.last_result {
            Some(result) => result.fetchmany(size, py),
            None => Ok(Vec::new()),
        }
    }

    /// Fetch all remaining rows from the result set
    ///
    /// Returns:
    ///     list: List of all rows
    ///
    /// Examples:
    ///     >>> cursor.execute("SELECT * FROM users")
    ///     >>> rows = cursor.fetchall()
    ///     >>> print(rows)
    ///     [[1, 'Alice'], [2, 'Bob']]
    pub fn fetchall(&mut self, py: Python) -> PyResult<Vec<PyObject>> {
        match &self.last_result {
            Some(result) => result.fetchall(py),
            None => Ok(Vec::new()),
        }
    }

    /// Get column descriptions
    ///
    /// Returns:
    ///     list: List of (name, type_code, display_size, internal_size, precision, scale, null_ok) tuples
    ///
    /// Examples:
    ///     >>> cursor.execute("SELECT * FROM users")
    ///     >>> print(cursor.description)
    ///     [('id', 'INTEGER', None, None, None, None, True), ('name', 'VARCHAR', None, None, None, None, True)]
    #[getter]
    pub fn description(&self, py: Python) -> PyResult<Option<Vec<PyObject>>> {
        match &self.last_result {
            Some(result) => result.description(py),
            None => Ok(None),
        }
    }

    /// Get the number of rows affected by the last operation
    ///
    /// Returns:
    ///     int: Number of rows affected
    #[getter]
    pub fn rowcount(&self) -> PyResult<i64> {
        match &self.last_result {
            Some(result) => Ok(result.row_count() as i64),
            None => Ok(-1),
        }
    }

    /// Close the cursor
    pub fn close(&mut self) -> PyResult<()> {
        self.last_result = None;
        Ok(())
    }

    /// String representation
    fn __repr__(&self) -> String {
        "PrismDB.Cursor()".to_string()
    }

    /// Iterator support
    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    /// Next item in iterator
    fn __next__(&mut self, py: Python) -> PyResult<Option<PyObject>> {
        self.fetchone(py)
    }
}
