//! SQLite Reading Functionality
//!
//! Implements sqlite_scan() table function

use crate::common::error::{PrismDBError, PrismDBResult};
use crate::types::{DataChunk, LogicalType, Value, Vector};
use rusqlite::{Connection, types::ValueRef};

/// SQLite reader
pub struct SqliteReader {
    data: Vec<u8>,
}

impl SqliteReader {
    /// Create a new SQLite reader from bytes
    pub fn new(data: Vec<u8>) -> Self {
        Self { data }
    }

    /// Read a table from SQLite and return a DataChunk
    pub fn read_table(&self, table_name: &str) -> PrismDBResult<DataChunk> {
        // Write data to a temporary file
        let temp_file = std::env::temp_dir().join(format!("prismdb_sqlite_{}.db", uuid::Uuid::new_v4()));
        std::fs::write(&temp_file, &self.data)
            .map_err(|e| PrismDBError::Io(e))?;

        // Open SQLite database
        let conn = Connection::open(&temp_file)
            .map_err(|e| PrismDBError::Parse(format!("Failed to open SQLite database: {}", e)))?;

        // Get column information
        let mut column_names = Vec::new();
        let mut column_types = Vec::new();

        {
            let mut stmt = conn.prepare(&format!("PRAGMA table_info({})", table_name))
                .map_err(|e| PrismDBError::Parse(format!("Failed to get table info: {}", e)))?;

            let rows = stmt.query_map([], |row| {
                Ok((
                    row.get::<_, String>(1)?, // column name
                    row.get::<_, String>(2)?, // column type
                ))
            }).map_err(|e| PrismDBError::Parse(format!("Failed to query table info: {}", e)))?;

            for row_result in rows {
                let (name, sql_type) = row_result
                    .map_err(|e| PrismDBError::Parse(format!("Failed to read column info: {}", e)))?;
                column_names.push(name);
                column_types.push(self.sqlite_type_to_logical_type(&sql_type)?);
            }
        } // stmt is dropped here

        println!("SQLite Table '{}' Schema:", table_name);
        for (i, (name, typ)) in column_names.iter().zip(column_types.iter()).enumerate() {
            println!("  Column {}: {} ({:?})", i, name, typ);
        }

        // Read all data from the table
        let mut stmt = conn.prepare(&format!("SELECT * FROM {}", table_name))
            .map_err(|e| PrismDBError::Parse(format!("Failed to prepare SELECT: {}", e)))?;

        let mut all_columns: Vec<Vec<Value>> = vec![Vec::new(); column_names.len()];
        let mut total_rows = 0;

        let mut rows = stmt.query([])
            .map_err(|e| PrismDBError::Parse(format!("Failed to execute SELECT: {}", e)))?;

        while let Some(row) = rows.next()
            .map_err(|e| PrismDBError::Parse(format!("Failed to fetch row: {}", e)))? {
            total_rows += 1;

            for (col_idx, _col_type) in column_types.iter().enumerate() {
                let value = self.convert_sqlite_value(row.get_ref(col_idx)
                    .map_err(|e| PrismDBError::Parse(format!("Failed to get column {}: {}", col_idx, e)))?)?;
                all_columns[col_idx].push(value);
            }
        }

        println!("Read {} rows from table '{}'", total_rows, table_name);

        // Clean up - drop in reverse dependency order
        drop(rows);
        drop(stmt);
        drop(conn);
        let _ = std::fs::remove_file(&temp_file);

        if total_rows == 0 {
            // Empty result
            let mut vectors = Vec::new();
            for col_type in column_types {
                vectors.push(Vector::new(col_type, 0));
            }
            return DataChunk::from_vectors(vectors);
        }

        // Build vectors for each column
        let mut vectors = Vec::new();
        for (col_idx, col_data) in all_columns.into_iter().enumerate() {
            let logical_type = column_types[col_idx].clone();
            let row_count = col_data.len();
            let mut vector = Vector::new(logical_type, row_count);
            for (row_idx, value) in col_data.into_iter().enumerate() {
                vector.set_value(row_idx, &value)
                    .map_err(|e| PrismDBError::Internal(format!(
                        "Failed to set value in column {}: {}", col_idx, e
                    )))?;
            }
            vector.resize(row_count)?;
            vectors.push(vector);
        }

        DataChunk::from_vectors(vectors)
    }

    /// Get column names from SQLite table
    pub fn get_column_names(&self, table_name: &str) -> PrismDBResult<Vec<String>> {
        let temp_file = std::env::temp_dir().join(format!("prismdb_sqlite_{}.db", uuid::Uuid::new_v4()));
        std::fs::write(&temp_file, &self.data)
            .map_err(|e| PrismDBError::Io(e))?;

        let conn = Connection::open(&temp_file)
            .map_err(|e| PrismDBError::Parse(format!("Failed to open SQLite database: {}", e)))?;

        let mut stmt = conn.prepare(&format!("PRAGMA table_info({})", table_name))
            .map_err(|e| PrismDBError::Parse(format!("Failed to get table info: {}", e)))?;

        let mut column_names = Vec::new();
        let rows = stmt.query_map([], |row| row.get::<_, String>(1))
            .map_err(|e| PrismDBError::Parse(format!("Failed to query table info: {}", e)))?;

        for row_result in rows {
            column_names.push(row_result
                .map_err(|e| PrismDBError::Parse(format!("Failed to read column name: {}", e)))?);
        }

        drop(stmt);
        drop(conn);
        let _ = std::fs::remove_file(&temp_file);

        Ok(column_names)
    }

    /// Get column types from SQLite table
    pub fn get_column_types(&self, table_name: &str) -> PrismDBResult<Vec<LogicalType>> {
        let temp_file = std::env::temp_dir().join(format!("prismdb_sqlite_{}.db", uuid::Uuid::new_v4()));
        std::fs::write(&temp_file, &self.data)
            .map_err(|e| PrismDBError::Io(e))?;

        let conn = Connection::open(&temp_file)
            .map_err(|e| PrismDBError::Parse(format!("Failed to open SQLite database: {}", e)))?;

        let mut stmt = conn.prepare(&format!("PRAGMA table_info({})", table_name))
            .map_err(|e| PrismDBError::Parse(format!("Failed to get table info: {}", e)))?;

        let mut column_types = Vec::new();
        let rows = stmt.query_map([], |row| row.get::<_, String>(2))
            .map_err(|e| PrismDBError::Parse(format!("Failed to query table info: {}", e)))?;

        for row_result in rows {
            let sql_type = row_result
                .map_err(|e| PrismDBError::Parse(format!("Failed to read column type: {}", e)))?;
            column_types.push(self.sqlite_type_to_logical_type(&sql_type)?);
        }

        drop(stmt);
        drop(conn);
        let _ = std::fs::remove_file(&temp_file);

        Ok(column_types)
    }

    /// Convert SQLite value to PrismDB Value
    fn convert_sqlite_value(&self, value: ValueRef) -> PrismDBResult<Value> {
        match value {
            ValueRef::Null => Ok(Value::Null),
            ValueRef::Integer(i) => Ok(Value::BigInt(i)),
            ValueRef::Real(f) => Ok(Value::Double(f)),
            ValueRef::Text(s) => {
                let text = std::str::from_utf8(s)
                    .map_err(|e| PrismDBError::Internal(format!("Invalid UTF-8 in SQLite text: {}", e)))?;
                Ok(Value::Varchar(text.to_string()))
            }
            ValueRef::Blob(b) => {
                Ok(Value::Blob(b.to_vec()))
            }
        }
    }

    /// Convert SQLite data type to PrismDB logical type
    fn sqlite_type_to_logical_type(&self, sqlite_type: &str) -> PrismDBResult<LogicalType> {
        let upper_type = sqlite_type.to_uppercase();

        // SQLite type affinity rules
        if upper_type.contains("INT") {
            Ok(LogicalType::BigInt)
        } else if upper_type.contains("CHAR") || upper_type.contains("CLOB") || upper_type.contains("TEXT") {
            Ok(LogicalType::Varchar)
        } else if upper_type.contains("BLOB") || upper_type.is_empty() {
            Ok(LogicalType::Blob)
        } else if upper_type.contains("REAL") || upper_type.contains("FLOA") || upper_type.contains("DOUB") {
            Ok(LogicalType::Double)
        } else if upper_type.contains("DATE") {
            Ok(LogicalType::Date)
        } else if upper_type.contains("TIME") {
            Ok(LogicalType::Timestamp)
        } else {
            // Default to VARCHAR for unknown types
            Ok(LogicalType::Varchar)
        }
    }

    /// List all tables in the SQLite database
    pub fn list_tables(&self) -> PrismDBResult<Vec<String>> {
        let temp_file = std::env::temp_dir().join(format!("prismdb_sqlite_{}.db", uuid::Uuid::new_v4()));
        std::fs::write(&temp_file, &self.data)
            .map_err(|e| PrismDBError::Io(e))?;

        let conn = Connection::open(&temp_file)
            .map_err(|e| PrismDBError::Parse(format!("Failed to open SQLite database: {}", e)))?;

        let mut stmt = conn.prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            .map_err(|e| PrismDBError::Parse(format!("Failed to list tables: {}", e)))?;

        let mut tables = Vec::new();
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))
            .map_err(|e| PrismDBError::Parse(format!("Failed to query tables: {}", e)))?;

        for row_result in rows {
            tables.push(row_result
                .map_err(|e| PrismDBError::Parse(format!("Failed to read table name: {}", e)))?);
        }

        drop(stmt);
        drop(conn);
        let _ = std::fs::remove_file(&temp_file);

        Ok(tables)
    }
}
