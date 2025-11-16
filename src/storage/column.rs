//! Column data storage for DuckDB
//!
//! This module provides column-level data storage with:
//! - Type-specific storage optimization
//! - Null value handling
//! - Memory-efficient operations
//! - Concurrent access support

use crate::common::error::{PrismDBError, PrismDBResult};
use crate::types::{LogicalType, Value, Vector};

// Import ColumnInfo from table module to avoid duplication
use crate::storage::table::ColumnInfo;

/// Column data storage trait
pub trait ColumnDataStorage: Send + Sync {
    /// Get the column info
    fn get_info(&self) -> &ColumnInfo;

    /// Get number of values
    fn len(&self) -> usize;

    /// Get the capacity
    fn capacity(&self) -> usize;

    /// Get a value by index
    fn get_value(&self, index: usize) -> PrismDBResult<Value>;

    /// Set a value by index
    fn set_value(&mut self, index: usize, value: &Value) -> PrismDBResult<()>;

    /// Push a value to end
    fn push_value(&mut self, value: &Value) -> PrismDBResult<()>;

    /// Delete a value by index (mark as null)
    fn delete_value(&mut self, index: usize) -> PrismDBResult<()>;

    /// Resize the column
    fn resize(&mut self, new_capacity: usize) -> PrismDBResult<()>;

    /// Estimate memory usage
    fn estimate_memory_usage(&self) -> usize;

    /// Create a vector from a range of values
    fn create_vector(&self, start: usize, count: usize) -> PrismDBResult<Vector>;
}

/// Generic column data storage
#[derive(Debug, Clone)]
pub struct ColumnData {
    /// Column information
    pub info: ColumnInfo,
    /// Storage for values
    values: Vec<Value>,
    /// Null mask
    null_mask: Vec<bool>,
    /// Capacity
    capacity: usize,
}

impl ColumnData {
    /// Create a new column with the given info and capacity
    pub fn new(info: ColumnInfo, capacity: usize) -> PrismDBResult<Self> {
        Ok(Self {
            info,
            values: Vec::with_capacity(capacity),
            null_mask: Vec::with_capacity(capacity),
            capacity,
        })
    }

    /// Get the column type
    pub fn get_type(&self) -> &LogicalType {
        &self.info.column_type
    }

    /// Get the number of values
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Get the capacity
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Get a value by index
    pub fn get_value(&self, index: usize) -> PrismDBResult<Value> {
        if index >= self.values.len() {
            return Err(PrismDBError::InvalidValue(format!(
                "Index {} out of bounds for column with {} values",
                index,
                self.values.len()
            )));
        }

        if index < self.null_mask.len() && self.null_mask[index] {
            Ok(Value::Null)
        } else {
            Ok(self.values[index].clone())
        }
    }

    /// Set a value by index
    pub fn set_value(&mut self, index: usize, value: &Value) -> PrismDBResult<()> {
        if index >= self.values.len() {
            return Err(PrismDBError::InvalidValue(format!(
                "Index {} out of bounds for column with {} values",
                index,
                self.values.len()
            )));
        }

        self.values[index] = value.clone();

        // Update null mask
        if index >= self.null_mask.len() {
            self.null_mask.resize(index + 1, false);
        }
        self.null_mask[index] = value.is_null();

        Ok(())
    }

    /// Push a value to end
    pub fn push_value(&mut self, value: &Value) -> PrismDBResult<()> {
        if self.values.len() >= self.capacity {
            return Err(PrismDBError::InvalidValue(
                "Column capacity exceeded".to_string(),
            ));
        }

        self.values.push(value.clone());
        self.null_mask.push(value.is_null());

        Ok(())
    }

    /// Delete a value by index (mark as null)
    pub fn delete_value(&mut self, index: usize) -> PrismDBResult<()> {
        if index >= self.values.len() {
            return Err(PrismDBError::InvalidValue(format!(
                "Index {} out of bounds for column with {} values",
                index,
                self.values.len()
            )));
        }

        if index < self.null_mask.len() {
            self.null_mask[index] = true;
        }

        Ok(())
    }

    /// Resize the column
    pub fn resize(&mut self, new_capacity: usize) -> PrismDBResult<()> {
        if new_capacity < self.values.len() {
            return Err(PrismDBError::InvalidValue(
                "Cannot resize to smaller than current size".to_string(),
            ));
        }

        self.capacity = new_capacity;
        self.values.reserve(new_capacity - self.values.len());
        self.null_mask.reserve(new_capacity - self.null_mask.len());

        Ok(())
    }

    /// Clear all values from the column
    pub fn clear(&mut self) {
        self.values.clear();
        self.null_mask.clear();
    }

    /// Estimate memory usage
    pub fn estimate_memory_usage(&self) -> usize {
        let values_size = self.values.len() * std::mem::size_of::<Value>();
        let null_mask_size = self.null_mask.len() * std::mem::size_of::<bool>();
        let info_size = std::mem::size_of::<ColumnInfo>();
        values_size + null_mask_size + info_size
    }

    /// Estimate size (alias for estimate_memory_usage)
    pub fn estimate_size(&self) -> usize {
        self.estimate_memory_usage()
    }

    /// Create a vector from a range of values
    pub fn create_vector(&self, start: usize, count: usize) -> PrismDBResult<Vector> {
        if start + count > self.values.len() {
            return Err(PrismDBError::InvalidValue(format!(
                "Range {}..{} out of bounds for column with {} values",
                start,
                start + count,
                self.values.len()
            )));
        }

        // Create a simple vector - this is a simplified implementation
        let mut vector_values = Vec::with_capacity(count);

        for i in 0..count {
            let value = if start + i < self.null_mask.len() && self.null_mask[start + i] {
                Value::Null
            } else {
                self.values[start + i].clone()
            };
            vector_values.push(value);
        }

        // Create vector from values - simplified approach
        if vector_values.is_empty() {
            return Err(PrismDBError::InvalidValue(
                "Cannot create vector from empty values".to_string(),
            ));
        }

        Vector::from_values(&vector_values)
    }

    /// Get the number of rows in the column
    pub fn row_count(&self) -> usize {
        self.values.len()
    }

    /// Get the number of null values in the column
    pub fn null_count(&self) -> usize {
        self.null_mask.iter().filter(|&&is_null| is_null).count()
    }
}

impl ColumnDataStorage for ColumnData {
    fn get_info(&self) -> &ColumnInfo {
        &self.info
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn capacity(&self) -> usize {
        self.capacity
    }

    fn get_value(&self, index: usize) -> PrismDBResult<Value> {
        if index >= self.values.len() {
            return Err(PrismDBError::InvalidValue(format!(
                "Index {} out of bounds for column with {} values",
                index,
                self.values.len()
            )));
        }

        if index < self.null_mask.len() && self.null_mask[index] {
            Ok(Value::Null)
        } else {
            Ok(self.values[index].clone())
        }
    }

    fn set_value(&mut self, index: usize, value: &Value) -> PrismDBResult<()> {
        if index >= self.values.len() {
            return Err(PrismDBError::InvalidValue(format!(
                "Index {} out of bounds for column with {} values",
                index,
                self.values.len()
            )));
        }

        self.values[index] = value.clone();

        // Update null mask
        if index >= self.null_mask.len() {
            self.null_mask.resize(index + 1, false);
        }
        self.null_mask[index] = value.is_null();

        Ok(())
    }

    fn push_value(&mut self, value: &Value) -> PrismDBResult<()> {
        if self.values.len() >= self.capacity {
            return Err(PrismDBError::InvalidValue(
                "Column capacity exceeded".to_string(),
            ));
        }

        self.values.push(value.clone());
        self.null_mask.push(value.is_null());

        Ok(())
    }

    fn delete_value(&mut self, index: usize) -> PrismDBResult<()> {
        if index >= self.values.len() {
            return Err(PrismDBError::InvalidValue(format!(
                "Index {} out of bounds for column with {} values",
                index,
                self.values.len()
            )));
        }

        if index < self.null_mask.len() {
            self.null_mask[index] = true;
        }

        Ok(())
    }

    fn resize(&mut self, new_capacity: usize) -> PrismDBResult<()> {
        if new_capacity < self.values.len() {
            return Err(PrismDBError::InvalidValue(
                "Cannot resize to smaller than current size".to_string(),
            ));
        }

        self.capacity = new_capacity;
        self.values.reserve(new_capacity - self.values.len());
        self.null_mask.reserve(new_capacity - self.null_mask.len());

        Ok(())
    }

    fn estimate_memory_usage(&self) -> usize {
        let values_size = self.values.len() * std::mem::size_of::<Value>();
        let null_mask_size = self.null_mask.len() * std::mem::size_of::<bool>();
        let info_size = std::mem::size_of::<ColumnInfo>();
        values_size + null_mask_size + info_size
    }

    fn create_vector(&self, start: usize, count: usize) -> PrismDBResult<Vector> {
        if start + count > self.values.len() {
            return Err(PrismDBError::InvalidValue(format!(
                "Range {}..{} out of bounds for column with {} values",
                start,
                start + count,
                self.values.len()
            )));
        }

        // Create a simple vector - this is a simplified implementation
        let mut vector_values = Vec::with_capacity(count);

        for i in 0..count {
            let value = if start + i < self.null_mask.len() && self.null_mask[start + i] {
                Value::Null
            } else {
                self.values[start + i].clone()
            };
            vector_values.push(value);
        }

        // Create vector from values - simplified approach
        if vector_values.is_empty() {
            return Err(PrismDBError::InvalidValue(
                "Cannot create vector from empty values".to_string(),
            ));
        }

        Vector::from_values(&vector_values)
    }
}

/// Factory for creating column data
pub struct ColumnDataFactory;

impl ColumnDataFactory {
    /// Create a column data instance based on the column type
    pub fn create_column_data(
        info: ColumnInfo,
        capacity: usize,
    ) -> PrismDBResult<Box<dyn ColumnDataStorage>> {
        let column = ColumnData::new(info, capacity)?;
        Ok(Box::new(column))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_basic_operations() -> PrismDBResult<()> {
        let info = ColumnInfo::new("test_col".to_string(), LogicalType::Integer, 0);
        let mut column = ColumnData::new(info, 5)?;

        // Test inserting values
        column.push_value(&Value::Integer(42))?;
        column.push_value(&Value::Integer(84))?;
        column.push_value(&Value::Null)?;

        assert_eq!(column.len(), 3);

        // Test retrieving values
        assert_eq!(column.get_value(0)?, Value::Integer(42));
        assert_eq!(column.get_value(1)?, Value::Integer(84));
        assert_eq!(column.get_value(2)?, Value::Null);

        // Test updating values
        column.set_value(1, &Value::Integer(100))?;
        assert_eq!(column.get_value(1)?, Value::Integer(100));

        // Test deleting values
        column.delete_value(0)?;
        assert_eq!(column.get_value(0)?, Value::Null);

        Ok(())
    }

    #[test]
    fn test_column_varchar() -> PrismDBResult<()> {
        let info = ColumnInfo::new("name".to_string(), LogicalType::Varchar, 0);
        let mut column = ColumnData::new(info, 5)?;

        // Test inserting string values
        column.push_value(&Value::Varchar("Alice".to_string()))?;
        column.push_value(&Value::Varchar("Bob".to_string()))?;
        column.push_value(&Value::Null)?;

        assert_eq!(column.len(), 3);

        // Test retrieving values
        assert_eq!(column.get_value(0)?, Value::Varchar("Alice".to_string()));
        assert_eq!(column.get_value(1)?, Value::Varchar("Bob".to_string()));
        assert_eq!(column.get_value(2)?, Value::Null);

        Ok(())
    }

    #[test]
    fn test_column_factory() -> PrismDBResult<()> {
        let int_info = ColumnInfo::new("id".to_string(), LogicalType::Integer, 0);
        let int_column = ColumnDataFactory::create_column_data(int_info, 10)?;
        assert_eq!(int_column.get_info().column_type, LogicalType::Integer);

        let str_info = ColumnInfo::new("name".to_string(), LogicalType::Varchar, 0);
        let str_column = ColumnDataFactory::create_column_data(str_info, 10)?;
        assert_eq!(str_column.get_info().column_type, LogicalType::Varchar);

        Ok(())
    }

    #[test]
    fn test_column_memory_usage() -> PrismDBResult<()> {
        let info = ColumnInfo::new("test".to_string(), LogicalType::Integer, 0);
        let mut column = ColumnData::new(info, 100)?;

        // Add some values
        for i in 0..10 {
            column.push_value(&Value::Integer(i))?;
        }

        let memory_usage = column.estimate_memory_usage();
        assert!(memory_usage > 0);

        Ok(())
    }

    #[test]
    fn test_column_vector_creation() -> PrismDBResult<()> {
        let info = ColumnInfo::new("test".to_string(), LogicalType::Integer, 0);
        let mut column = ColumnData::new(info, 10)?;

        // Add test values
        for i in 0..5 {
            column.push_value(&Value::Integer(i * 10))?;
        }

        // Create vector from first 3 values
        let vector = column.create_vector(0, 3)?;
        assert_eq!(vector.len(), 3);

        Ok(())
    }

    #[test]
    fn test_column_error_handling() -> PrismDBResult<()> {
        let info = ColumnInfo::new("test".to_string(), LogicalType::Integer, 0);
        let mut column = ColumnData::new(info, 2)?; // Small capacity

        // Fill to capacity
        column.push_value(&Value::Integer(1))?;
        column.push_value(&Value::Integer(2))?;

        // Should fail when exceeding capacity
        let result = column.push_value(&Value::Integer(3));
        assert!(result.is_err());

        // Should fail for out-of-bounds access
        let result = column.get_value(10);
        assert!(result.is_err());

        // Should fail for out-of-bounds set
        let result = column.set_value(10, &Value::Integer(42));
        assert!(result.is_err());

        Ok(())
    }
}
