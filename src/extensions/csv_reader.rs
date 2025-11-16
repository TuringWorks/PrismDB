//! CSV Reading Functionality
//!
//! Implements read_csv_auto() table function

use crate::common::error::{PrismDBError, PrismDBResult};
use crate::types::{DataChunk, LogicalType, Value, Vector};
use csv::ReaderBuilder;
use std::io::Cursor;

/// CSV reader that auto-detects schema
pub struct CsvReader {
    data: Vec<u8>,
}

impl CsvReader {
    /// Create a new CSV reader from bytes
    pub fn new(data: Vec<u8>) -> Self {
        Self { data }
    }

    /// Read CSV and return a DataChunk
    pub fn read(&self) -> PrismDBResult<DataChunk> {
        let cursor = Cursor::new(&self.data);
        let mut csv_reader = ReaderBuilder::new()
            .has_headers(true)
            .from_reader(cursor);

        // Get headers
        let headers = csv_reader.headers()
            .map_err(|e| PrismDBError::Parse(format!("Failed to read CSV headers: {}", e)))?
            .clone();

        let header_names: Vec<String> = headers.iter().map(|h| h.to_string()).collect();
        let column_count = header_names.len();

        println!("CSV Schema detected:");
        for (i, name) in header_names.iter().enumerate() {
            println!("  Column {}: {}", i, name);
        }

        // Read all records first to detect types and collect data
        let mut records = Vec::new();
        for result in csv_reader.records() {
            let record = result
                .map_err(|e| PrismDBError::Parse(format!("Failed to read CSV record: {}", e)))?;
            records.push(record);
        }

        println!("Read {} rows", records.len());

        if records.is_empty() {
            // Empty result - create empty vectors
            let mut vectors = Vec::new();
            for _ in 0..column_count {
                vectors.push(Vector::new(LogicalType::Varchar, 0));
            }
            return DataChunk::from_vectors(vectors);
        }

        // For simplicity, treat all columns as VARCHAR for now
        // TODO: Implement type inference
        let column_types = vec![LogicalType::Varchar; column_count];

        // Build vectors for each column
        let mut columns: Vec<Vec<Value>> = vec![Vec::new(); column_count];

        for record in records {
            for (col_idx, field) in record.iter().enumerate() {
                if col_idx < column_count {
                    if field.is_empty() {
                        columns[col_idx].push(Value::Null);
                    } else {
                        columns[col_idx].push(Value::Varchar(field.to_string()));
                    }
                }
            }
        }

        // Convert to vectors
        let mut vectors = Vec::new();
        for (col_idx, col_data) in columns.into_iter().enumerate() {
            let row_count = col_data.len();
            let mut vector = Vector::new(column_types[col_idx].clone(), row_count);
            for (row_idx, value) in col_data.into_iter().enumerate() {
                vector.set_value(row_idx, &value)
                    .map_err(|e| PrismDBError::Internal(format!(
                        "Failed to set value in column {}: {}", col_idx, e
                    )))?;
            }
            // Set the count to match the number of rows we just inserted
            vector.resize(row_count)?;
            vectors.push(vector);
        }

        DataChunk::from_vectors(vectors)
    }

    /// Get column names from CSV header
    pub fn get_column_names(&self) -> PrismDBResult<Vec<String>> {
        let cursor = Cursor::new(&self.data);
        let mut csv_reader = ReaderBuilder::new()
            .has_headers(true)
            .from_reader(cursor);

        let headers = csv_reader.headers()
            .map_err(|e| PrismDBError::Parse(format!("Failed to read CSV headers: {}", e)))?;

        Ok(headers.iter().map(|h| h.to_string()).collect())
    }
}
