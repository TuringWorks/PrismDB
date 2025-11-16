//! Python query result class for PrismDB

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use crate::database::QueryResult;
use crate::types::Value;
use std::cell::RefCell;

/// Query result wrapper for Python
#[pyclass(name = "QueryResult")]
pub struct PyQueryResult {
    pub(crate) result: QueryResult,
    pub(crate) current_row: RefCell<usize>,
}

impl PyQueryResult {
    pub fn new(result: QueryResult) -> Self {
        Self {
            result,
            current_row: RefCell::new(0),
        }
    }

    pub fn row_count(&self) -> usize {
        self.result.row_count()
    }
}

/// Convert a PrismDB Value to a Python object
fn value_to_pyobject(value: &Value, py: Python) -> PyResult<PyObject> {
    match value {
        Value::Null => Ok(py.None()),
        Value::Boolean(b) => Ok(b.to_object(py)),
        Value::TinyInt(i) => Ok(i.to_object(py)),
        Value::SmallInt(i) => Ok(i.to_object(py)),
        Value::Integer(i) => Ok(i.to_object(py)),
        Value::BigInt(i) => Ok(i.to_object(py)),
        Value::Float(f) => Ok(f.to_object(py)),
        Value::Double(f) => Ok(f.to_object(py)),
        Value::Decimal { value, scale, .. } => {
            // Convert decimal to float for Python
            let divisor = 10_f64.powi(*scale as i32);
            let float_value = *value as f64 / divisor;
            Ok(float_value.to_object(py))
        }
        Value::Varchar(s) => Ok(s.to_object(py)),
        Value::Date(d) => Ok(d.to_string().to_object(py)),
        Value::Time(t) => Ok(t.to_string().to_object(py)),
        Value::Timestamp(ts) => Ok(ts.to_string().to_object(py)),
        Value::Blob(b) => {
            let bytes = pyo3::types::PyBytes::new(py, b);
            Ok(bytes.to_object(py))
        }
        _ => Ok(value.to_string().to_object(py)),
    }
}

#[pymethods]
impl PyQueryResult {
    /// Fetch the next row
    ///
    /// Returns:
    ///     list or None: Next row as a list, or None if no more rows
    pub fn fetchone(&self, py: Python) -> PyResult<Option<PyObject>> {
        let mut current = self.current_row.borrow_mut();
        if *current >= self.result.row_count() {
            return Ok(None);
        }

        let row_list = PyList::empty(py);
        for chunk in self.result.chunks() {
            if *current >= chunk.len() {
                *current -= chunk.len();
                continue;
            }

            for col_idx in 0..chunk.column_count() {
                if let Some(vector) = chunk.get_vector(col_idx) {
                    if let Ok(value) = vector.get_value(*current) {
                        row_list.append(value_to_pyobject(&value, py)?)?;
                    }
                }
            }

            *current = *current + 1;
            return Ok(Some(row_list.to_object(py)));
        }

        Ok(None)
    }

    /// Fetch multiple rows
    ///
    /// Args:
    ///     size (int, optional): Number of rows to fetch. If None, fetch all.
    ///
    /// Returns:
    ///     list: List of rows
    #[pyo3(signature = (size=None))]
    pub fn fetchmany(&self, size: Option<usize>, py: Python) -> PyResult<Vec<PyObject>> {
        let count = size.unwrap_or(self.result.row_count());
        let mut rows = Vec::new();

        for _ in 0..count {
            if let Some(row) = self.fetchone(py)? {
                rows.push(row);
            } else {
                break;
            }
        }

        Ok(rows)
    }

    /// Fetch all remaining rows
    ///
    /// Returns:
    ///     list: List of all rows
    pub fn fetchall(&self, py: Python) -> PyResult<Vec<PyObject>> {
        let mut rows = Vec::new();
        let mut row_idx = 0;

        for chunk in self.result.chunks() {
            for i in 0..chunk.len() {
                let row_list = PyList::empty(py);

                for col_idx in 0..chunk.column_count() {
                    if let Some(vector) = chunk.get_vector(col_idx) {
                        if let Ok(value) = vector.get_value(i) {
                            row_list.append(value_to_pyobject(&value, py)?)?;
                        }
                    }
                }

                rows.push(row_list.to_object(py));
                row_idx += 1;
            }
        }

        // Update current row to the end
        *self.current_row.borrow_mut() = row_idx;

        Ok(rows)
    }

    /// Convert result to a dictionary
    ///
    /// Returns:
    ///     dict: Dictionary with column names as keys and lists of values
    pub fn to_dict(&self, py: Python) -> PyResult<PyObject> {
        let dict = PyDict::new(py);

        // Initialize column lists
        for col in &self.result.columns {
            dict.set_item(&col.name, PyList::empty(py))?;
        }

        // Fill data
        for chunk in self.result.chunks() {
            for row_idx in 0..chunk.len() {
                for (col_idx, col) in self.result.columns.iter().enumerate() {
                    if let Some(vector) = chunk.get_vector(col_idx) {
                        if let Ok(value) = vector.get_value(row_idx) {
                            let py_value = value_to_pyobject(&value, py)?;
                            if let Ok(Some(list)) = dict.get_item(&col.name) {
                                if let Ok(py_list) = list.downcast::<PyList>() {
                                    py_list.append(py_value)?;
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(dict.to_object(py))
    }

    /// Get column descriptions
    ///
    /// Returns:
    ///     list: List of (name, type_code, display_size, internal_size, precision, scale, null_ok) tuples
    pub fn description(&self, py: Python) -> PyResult<Option<Vec<PyObject>>> {
        if self.result.columns.is_empty() {
            return Ok(None);
        }

        let mut desc = Vec::new();
        for col in &self.result.columns {
            let tuple = (
                col.name.clone(),
                col.data_type.to_string(),
                py.None(),  // display_size
                py.None(),  // internal_size
                py.None(),  // precision
                py.None(),  // scale
                true,       // null_ok
            );
            desc.push(tuple.to_object(py));
        }

        Ok(Some(desc))
    }

    /// Get number of rows
    #[getter]
    pub fn rowcount(&self) -> usize {
        self.result.row_count()
    }

    /// Get number of columns
    #[getter]
    pub fn column_count(&self) -> usize {
        self.result.column_count()
    }

    /// String representation
    fn __repr__(&self) -> String {
        format!("QueryResult({} rows, {} columns)", self.result.row_count(), self.result.column_count())
    }

    /// Length support
    fn __len__(&self) -> usize {
        self.result.row_count()
    }

    /// Iterator support
    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        *slf.current_row.borrow_mut() = 0;
        slf
    }

    /// Next item in iterator
    fn __next__(&self, py: Python) -> PyResult<Option<PyObject>> {
        self.fetchone(py)
    }
}
