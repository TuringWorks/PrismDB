//! Parquet Reading Functionality
//!
//! Implements read_parquet() table function

use crate::common::error::{PrismDBError, PrismDBResult};
use crate::types::{DataChunk, LogicalType, Value, Vector};
use arrow::array::*;
use arrow::datatypes::DataType as ArrowDataType;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::sync::Arc;

/// Parquet reader
pub struct ParquetReader {
    data: Vec<u8>,
}

impl ParquetReader {
    /// Create a new Parquet reader from bytes
    pub fn new(data: Vec<u8>) -> Self {
        Self { data }
    }

    /// Read Parquet and return a DataChunk
    pub fn read(&self) -> PrismDBResult<DataChunk> {
        let bytes = Bytes::from(self.data.clone());

        // Build parquet reader
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(|e| PrismDBError::Parse(format!("Failed to create Parquet reader: {}", e)))?;

        let schema = builder.schema().clone();
        println!("Parquet Schema detected:");
        for (i, field) in schema.fields().iter().enumerate() {
            println!("  Column {}: {} ({})", i, field.name(), field.data_type());
        }

        let mut reader = builder.build()
            .map_err(|e| PrismDBError::Parse(format!("Failed to build Parquet reader: {}", e)))?;

        // Read all batches
        let mut all_columns: Vec<Vec<Value>> = vec![Vec::new(); schema.fields().len()];
        let mut total_rows = 0;

        while let Some(batch_result) = reader.next() {
            let batch = batch_result
                .map_err(|e| PrismDBError::Parse(format!("Failed to read Parquet batch: {}", e)))?;

            total_rows += batch.num_rows();

            // Convert each column
            for (col_idx, array) in batch.columns().iter().enumerate() {
                let values = self.convert_arrow_array(array)?;
                all_columns[col_idx].extend(values);
            }
        }

        println!("Read {} rows", total_rows);

        if total_rows == 0 {
            // Empty result
            let mut vectors = Vec::new();
            for field in schema.fields() {
                let logical_type = self.arrow_type_to_logical_type(field.data_type())?;
                vectors.push(Vector::new(logical_type, 0));
            }
            return DataChunk::from_vectors(vectors);
        }

        // Build vectors for each column
        let mut vectors = Vec::new();
        for (col_idx, col_data) in all_columns.into_iter().enumerate() {
            let field = &schema.fields()[col_idx];
            let logical_type = self.arrow_type_to_logical_type(field.data_type())?;

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

    /// Get column names from Parquet schema
    pub fn get_column_names(&self) -> PrismDBResult<Vec<String>> {
        let bytes = Bytes::from(self.data.clone());
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(|e| PrismDBError::Parse(format!("Failed to read Parquet schema: {}", e)))?;

        let schema = builder.schema();
        Ok(schema.fields().iter().map(|f| f.name().clone()).collect())
    }

    /// Get column types from Parquet schema
    pub fn get_column_types(&self) -> PrismDBResult<Vec<LogicalType>> {
        let bytes = Bytes::from(self.data.clone());
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(|e| PrismDBError::Parse(format!("Failed to read Parquet schema: {}", e)))?;

        let schema = builder.schema();
        schema.fields()
            .iter()
            .map(|f| self.arrow_type_to_logical_type(f.data_type()))
            .collect()
    }

    /// Convert Arrow array to vector of Values
    fn convert_arrow_array(&self, array: &Arc<dyn arrow::array::Array>) -> PrismDBResult<Vec<Value>> {
        let mut values = Vec::with_capacity(array.len());

        match array.data_type() {
            ArrowDataType::Boolean => {
                let arr = array.as_any().downcast_ref::<BooleanArray>()
                    .ok_or_else(|| PrismDBError::Internal("Failed to downcast to BooleanArray".to_string()))?;
                for i in 0..arr.len() {
                    values.push(if arr.is_null(i) { Value::Null } else { Value::Boolean(arr.value(i)) });
                }
            }
            ArrowDataType::Int8 => {
                let arr = array.as_any().downcast_ref::<Int8Array>()
                    .ok_or_else(|| PrismDBError::Internal("Failed to downcast to Int8Array".to_string()))?;
                for i in 0..arr.len() {
                    values.push(if arr.is_null(i) { Value::Null } else { Value::TinyInt(arr.value(i)) });
                }
            }
            ArrowDataType::Int16 => {
                let arr = array.as_any().downcast_ref::<Int16Array>()
                    .ok_or_else(|| PrismDBError::Internal("Failed to downcast to Int16Array".to_string()))?;
                for i in 0..arr.len() {
                    values.push(if arr.is_null(i) { Value::Null } else { Value::SmallInt(arr.value(i)) });
                }
            }
            ArrowDataType::Int32 => {
                let arr = array.as_any().downcast_ref::<Int32Array>()
                    .ok_or_else(|| PrismDBError::Internal("Failed to downcast to Int32Array".to_string()))?;
                for i in 0..arr.len() {
                    values.push(if arr.is_null(i) { Value::Null } else { Value::Integer(arr.value(i)) });
                }
            }
            ArrowDataType::Int64 => {
                let arr = array.as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| PrismDBError::Internal("Failed to downcast to Int64Array".to_string()))?;
                for i in 0..arr.len() {
                    values.push(if arr.is_null(i) { Value::Null } else { Value::BigInt(arr.value(i)) });
                }
            }
            ArrowDataType::Float32 => {
                let arr = array.as_any().downcast_ref::<Float32Array>()
                    .ok_or_else(|| PrismDBError::Internal("Failed to downcast to Float32Array".to_string()))?;
                for i in 0..arr.len() {
                    values.push(if arr.is_null(i) { Value::Null } else { Value::Float(arr.value(i)) });
                }
            }
            ArrowDataType::Float64 => {
                let arr = array.as_any().downcast_ref::<Float64Array>()
                    .ok_or_else(|| PrismDBError::Internal("Failed to downcast to Float64Array".to_string()))?;
                for i in 0..arr.len() {
                    values.push(if arr.is_null(i) { Value::Null } else { Value::Double(arr.value(i)) });
                }
            }
            ArrowDataType::Utf8 => {
                let arr = array.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| PrismDBError::Internal("Failed to downcast to StringArray".to_string()))?;
                for i in 0..arr.len() {
                    values.push(if arr.is_null(i) { Value::Null } else { Value::Varchar(arr.value(i).to_string()) });
                }
            }
            ArrowDataType::LargeUtf8 => {
                let arr = array.as_any().downcast_ref::<LargeStringArray>()
                    .ok_or_else(|| PrismDBError::Internal("Failed to downcast to LargeStringArray".to_string()))?;
                for i in 0..arr.len() {
                    values.push(if arr.is_null(i) { Value::Null } else { Value::Varchar(arr.value(i).to_string()) });
                }
            }
            ArrowDataType::Date32 => {
                let arr = array.as_any().downcast_ref::<Date32Array>()
                    .ok_or_else(|| PrismDBError::Internal("Failed to downcast to Date32Array".to_string()))?;
                for i in 0..arr.len() {
                    values.push(if arr.is_null(i) { Value::Null } else { Value::Date(arr.value(i) as i32) });
                }
            }
            ArrowDataType::Date64 => {
                let arr = array.as_any().downcast_ref::<Date64Array>()
                    .ok_or_else(|| PrismDBError::Internal("Failed to downcast to Date64Array".to_string()))?;
                for i in 0..arr.len() {
                    // Date64 is milliseconds since epoch, convert to days
                    values.push(if arr.is_null(i) { Value::Null } else { Value::Date((arr.value(i) / (1000 * 60 * 60 * 24)) as i32) });
                }
            }
            ArrowDataType::Timestamp(unit, _) => {
                // Handle different timestamp units
                match unit {
                    arrow::datatypes::TimeUnit::Microsecond => {
                        let arr = array.as_any().downcast_ref::<arrow::array::TimestampMicrosecondArray>()
                            .ok_or_else(|| PrismDBError::Internal("Failed to downcast to TimestampMicrosecondArray".to_string()))?;
                        for i in 0..arr.len() {
                            values.push(if arr.is_null(i) { Value::Null } else { Value::Timestamp(arr.value(i)) });
                        }
                    }
                    arrow::datatypes::TimeUnit::Nanosecond => {
                        let arr = array.as_any().downcast_ref::<arrow::array::TimestampNanosecondArray>()
                            .ok_or_else(|| PrismDBError::Internal("Failed to downcast to TimestampNanosecondArray".to_string()))?;
                        for i in 0..arr.len() {
                            // Convert nanoseconds to microseconds (PrismDB uses microseconds)
                            values.push(if arr.is_null(i) { Value::Null } else { Value::Timestamp(arr.value(i) / 1000) });
                        }
                    }
                    arrow::datatypes::TimeUnit::Millisecond => {
                        let arr = array.as_any().downcast_ref::<arrow::array::TimestampMillisecondArray>()
                            .ok_or_else(|| PrismDBError::Internal("Failed to downcast to TimestampMillisecondArray".to_string()))?;
                        for i in 0..arr.len() {
                            // Convert milliseconds to microseconds
                            values.push(if arr.is_null(i) { Value::Null } else { Value::Timestamp(arr.value(i) * 1000) });
                        }
                    }
                    arrow::datatypes::TimeUnit::Second => {
                        let arr = array.as_any().downcast_ref::<arrow::array::TimestampSecondArray>()
                            .ok_or_else(|| PrismDBError::Internal("Failed to downcast to TimestampSecondArray".to_string()))?;
                        for i in 0..arr.len() {
                            // Convert seconds to microseconds
                            values.push(if arr.is_null(i) { Value::Null } else { Value::Timestamp(arr.value(i) * 1_000_000) });
                        }
                    }
                }
            }
            _ => {
                // For unsupported types, convert to string
                for i in 0..array.len() {
                    if array.is_null(i) {
                        values.push(Value::Null);
                    } else {
                        // For unknown types, convert entire column to VARCHAR for safety
                        values.push(Value::Varchar(format!("unsupported_{}", i)));
                    }
                }
            }
        }

        Ok(values)
    }

    /// Convert Arrow data type to PrismDB logical type
    fn arrow_type_to_logical_type(&self, arrow_type: &ArrowDataType) -> PrismDBResult<LogicalType> {
        match arrow_type {
            ArrowDataType::Boolean => Ok(LogicalType::Boolean),
            ArrowDataType::Int8 => Ok(LogicalType::TinyInt),
            ArrowDataType::Int16 => Ok(LogicalType::SmallInt),
            ArrowDataType::Int32 => Ok(LogicalType::Integer),
            ArrowDataType::Int64 => Ok(LogicalType::BigInt),
            ArrowDataType::UInt8 => Ok(LogicalType::TinyInt),
            ArrowDataType::UInt16 => Ok(LogicalType::SmallInt),
            ArrowDataType::UInt32 => Ok(LogicalType::Integer),
            ArrowDataType::UInt64 => Ok(LogicalType::BigInt),
            ArrowDataType::Float32 => Ok(LogicalType::Float),
            ArrowDataType::Float64 => Ok(LogicalType::Double),
            ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => Ok(LogicalType::Varchar),
            ArrowDataType::Binary | ArrowDataType::LargeBinary => Ok(LogicalType::Blob),
            ArrowDataType::Date32 | ArrowDataType::Date64 => Ok(LogicalType::Date),
            ArrowDataType::Time32(_) | ArrowDataType::Time64(_) => Ok(LogicalType::Time),
            ArrowDataType::Timestamp(_, _) => Ok(LogicalType::Timestamp),
            _ => Ok(LogicalType::Varchar), // Fallback to VARCHAR for unknown types
        }
    }
}
