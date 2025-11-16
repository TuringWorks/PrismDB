//! JSON Reading Functionality
//!
//! Implements read_json_auto() table function

use crate::common::error::{PrismDBError, PrismDBResult};
use crate::types::{DataChunk, LogicalType, Value, Vector};
use serde_json;

/// JSON reader
pub struct JsonReader {
    data: Vec<u8>,
}

impl JsonReader {
    /// Create a new JSON reader from bytes
    pub fn new(data: Vec<u8>) -> Self {
        Self { data }
    }

    /// Read JSON and return a DataChunk
    pub fn read(&self) -> PrismDBResult<DataChunk> {
        let text = std::str::from_utf8(&self.data)
            .map_err(|e| PrismDBError::Parse(format!("Invalid UTF-8 in JSON file: {}", e)))?;

        // Try to parse as JSON array first
        if let Ok(array) = serde_json::from_str::<Vec<serde_json::Value>>(text) {
            return self.read_json_array(array);
        }

        // Try newline-delimited JSON (NDJSON)
        let lines: Vec<&str> = text.lines().filter(|l| !l.trim().is_empty()).collect();
        if !lines.is_empty() {
            let mut objects = Vec::new();
            for line in lines {
                match serde_json::from_str::<serde_json::Value>(line) {
                    Ok(obj) => objects.push(obj),
                    Err(_) => {
                        // Not NDJSON, return error
                        return Err(PrismDBError::Parse(
                            "Invalid JSON format: expected array of objects or newline-delimited JSON".to_string()
                        ));
                    }
                }
            }
            return self.read_json_array(objects);
        }

        Err(PrismDBError::Parse("Empty or invalid JSON file".to_string()))
    }

    /// Read a JSON array and convert to DataChunk
    fn read_json_array(&self, array: Vec<serde_json::Value>) -> PrismDBResult<DataChunk> {
        if array.is_empty() {
            return DataChunk::from_vectors(vec![]);
        }

        // Infer schema from first object
        let first_obj = array.first().ok_or_else(|| {
            PrismDBError::Parse("Empty JSON array".to_string())
        })?;

        let obj = first_obj.as_object().ok_or_else(|| {
            PrismDBError::Parse("JSON array must contain objects".to_string())
        })?;

        // Get column names and infer types
        let mut column_names = Vec::new();
        let mut column_types = Vec::new();

        for (key, value) in obj.iter() {
            column_names.push(key.clone());
            column_types.push(self.infer_json_type(value)?);
        }

        println!("JSON Schema detected:");
        for (i, (name, typ)) in column_names.iter().zip(column_types.iter()).enumerate() {
            println!("  Column {}: {} ({:?})", i, name, typ);
        }

        // Read all rows
        let row_count = array.len();
        let mut all_columns: Vec<Vec<Value>> = vec![Vec::new(); column_names.len()];

        for obj_value in array.iter() {
            let obj = obj_value.as_object().ok_or_else(|| {
                PrismDBError::Parse("Expected JSON object in array".to_string())
            })?;

            for (col_idx, col_name) in column_names.iter().enumerate() {
                let value = obj.get(col_name)
                    .unwrap_or(&serde_json::Value::Null);
                let duck_value = self.convert_json_value(value, &column_types[col_idx])?;
                all_columns[col_idx].push(duck_value);
            }
        }

        println!("Read {} rows", row_count);

        // Build vectors for each column
        let mut vectors = Vec::new();
        for (col_idx, col_data) in all_columns.into_iter().enumerate() {
            let logical_type = column_types[col_idx].clone();
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

    /// Get column names from JSON
    pub fn get_column_names(&self) -> PrismDBResult<Vec<String>> {
        let text = std::str::from_utf8(&self.data)
            .map_err(|e| PrismDBError::Parse(format!("Invalid UTF-8 in JSON file: {}", e)))?;

        // Try to parse as JSON array first
        let array = if let Ok(arr) = serde_json::from_str::<Vec<serde_json::Value>>(text) {
            arr
        } else {
            // Try NDJSON
            let lines: Vec<&str> = text.lines().filter(|l| !l.trim().is_empty()).collect();
            if lines.is_empty() {
                return Ok(vec![]);
            }
            let first_obj = serde_json::from_str::<serde_json::Value>(lines[0])
                .map_err(|e| PrismDBError::Parse(format!("Failed to parse JSON: {}", e)))?;
            vec![first_obj]
        };

        if array.is_empty() {
            return Ok(vec![]);
        }

        let obj = array[0].as_object().ok_or_else(|| {
            PrismDBError::Parse("JSON array must contain objects".to_string())
        })?;

        Ok(obj.keys().cloned().collect())
    }

    /// Get column types from JSON
    pub fn get_column_types(&self) -> PrismDBResult<Vec<LogicalType>> {
        let text = std::str::from_utf8(&self.data)
            .map_err(|e| PrismDBError::Parse(format!("Invalid UTF-8 in JSON file: {}", e)))?;

        // Try to parse as JSON array first
        let array = if let Ok(arr) = serde_json::from_str::<Vec<serde_json::Value>>(text) {
            arr
        } else {
            // Try NDJSON
            let lines: Vec<&str> = text.lines().filter(|l| !l.trim().is_empty()).collect();
            if lines.is_empty() {
                return Ok(vec![]);
            }
            let first_obj = serde_json::from_str::<serde_json::Value>(lines[0])
                .map_err(|e| PrismDBError::Parse(format!("Failed to parse JSON: {}", e)))?;
            vec![first_obj]
        };

        if array.is_empty() {
            return Ok(vec![]);
        }

        let obj = array[0].as_object().ok_or_else(|| {
            PrismDBError::Parse("JSON array must contain objects".to_string())
        })?;

        let mut types = Vec::new();
        for (_key, value) in obj.iter() {
            types.push(self.infer_json_type(value)?);
        }

        Ok(types)
    }

    /// Infer DuckDB logical type from JSON value
    fn infer_json_type(&self, value: &serde_json::Value) -> PrismDBResult<LogicalType> {
        match value {
            serde_json::Value::Null => Ok(LogicalType::Varchar), // Default to VARCHAR for nullable
            serde_json::Value::Bool(_) => Ok(LogicalType::Boolean),
            serde_json::Value::Number(n) => {
                if n.is_i64() || n.is_u64() {
                    Ok(LogicalType::BigInt)
                } else {
                    Ok(LogicalType::Double)
                }
            }
            serde_json::Value::String(_) => Ok(LogicalType::Varchar),
            serde_json::Value::Array(_) => Ok(LogicalType::Varchar), // Convert arrays to JSON strings
            serde_json::Value::Object(_) => Ok(LogicalType::Varchar), // Convert objects to JSON strings
        }
    }

    /// Convert JSON value to DuckDB Value
    fn convert_json_value(&self, json_value: &serde_json::Value, expected_type: &LogicalType) -> PrismDBResult<Value> {
        match json_value {
            serde_json::Value::Null => Ok(Value::Null),
            serde_json::Value::Bool(b) => Ok(Value::Boolean(*b)),
            serde_json::Value::Number(n) => {
                match expected_type {
                    LogicalType::BigInt => {
                        if let Some(i) = n.as_i64() {
                            Ok(Value::BigInt(i))
                        } else if let Some(u) = n.as_u64() {
                            Ok(Value::BigInt(u as i64))
                        } else {
                            Ok(Value::BigInt(n.as_f64().unwrap_or(0.0) as i64))
                        }
                    }
                    LogicalType::Double => {
                        Ok(Value::Double(n.as_f64().unwrap_or(0.0)))
                    }
                    LogicalType::Float => {
                        Ok(Value::Float(n.as_f64().unwrap_or(0.0) as f32))
                    }
                    LogicalType::Integer => {
                        Ok(Value::Integer(n.as_i64().unwrap_or(0) as i32))
                    }
                    _ => {
                        // Default to string representation
                        Ok(Value::Varchar(n.to_string()))
                    }
                }
            }
            serde_json::Value::String(s) => Ok(Value::Varchar(s.clone())),
            serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
                // Convert nested structures to JSON strings
                Ok(Value::Varchar(json_value.to_string()))
            }
        }
    }
}
