//! Table data management for PrismDB
//!
//! This module provides the core table storage functionality including:
//! - Table metadata and schema
//! - Row storage and retrieval
//! - Index management
//! - Statistics tracking

use crate::common::error::{PrismDBError, PrismDBResult};
use crate::storage::column::ColumnData;
use crate::types::{DataChunk, LogicalType, Value};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, RwLock};

/// Row identifier for table rows
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RowId {
    pub id: usize,
}

impl RowId {
    pub fn new(id: usize) -> Self {
        Self { id }
    }

    pub fn as_usize(&self) -> usize {
        self.id
    }
}

/// Column-level statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStatistics {
    /// Number of non-null values
    pub non_null_count: usize,
    /// Number of null values
    pub null_count: usize,
    /// Number of distinct values (estimated)
    pub distinct_count: Option<usize>,
    /// Minimum value
    pub min_value: Option<Value>,
    /// Maximum value
    pub max_value: Option<Value>,
    /// Average value length (for variable-length types)
    pub avg_value_length: Option<f64>,
    /// Column size in bytes
    pub column_size: usize,
}

impl ColumnStatistics {
    pub fn new() -> Self {
        Self {
            non_null_count: 0,
            null_count: 0,
            distinct_count: None,
            min_value: None,
            max_value: None,
            avg_value_length: None,
            column_size: 0,
        }
    }

    pub fn update_for_value(&mut self, value: &Value) {
        if value.is_null() {
            self.null_count += 1;
        } else {
            self.non_null_count += 1;

            // Update min/max values
            match (&self.min_value, &self.max_value) {
                (None, None) => {
                    self.min_value = Some(value.clone());
                    self.max_value = Some(value.clone());
                }
                (Some(min), Some(max)) => {
                    if value.compare(min).unwrap_or(std::cmp::Ordering::Equal)
                        == std::cmp::Ordering::Less
                    {
                        self.min_value = Some(value.clone());
                    }
                    if value.compare(max).unwrap_or(std::cmp::Ordering::Equal)
                        == std::cmp::Ordering::Greater
                    {
                        self.max_value = Some(value.clone());
                    }
                }
                _ => {}
            }

            // Update average value length for strings
            if let Value::Varchar(s) = value {
                let len = s.len();
                self.avg_value_length = match self.avg_value_length {
                    None => Some(len as f64),
                    Some(avg) => Some(
                        (avg * (self.non_null_count - 1) as f64 + len as f64)
                            / self.non_null_count as f64,
                    ),
                };
            }
        }

        // Update column size estimate
        self.column_size += value.get_size();
    }
}

/// Table statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableStatistics {
    /// Number of rows in the table
    pub row_count: usize,
    /// Number of columns in the table
    pub column_count: usize,
    /// Estimated table size in bytes
    pub estimated_size: usize,
    /// Number of data pages
    pub page_count: usize,
    /// Whether statistics are up to date
    pub stats_up_to_date: bool,
    /// Column-level statistics
    pub column_stats: Vec<ColumnStatistics>,
    /// Last updated timestamp
    pub last_updated: u64,
    /// Number of inserts since last statistics update
    pub inserts_since_update: usize,
    /// Number of deletes since last statistics update
    pub deletes_since_update: usize,
    /// Number of updates since last statistics update
    pub updates_since_update: usize,
}

impl TableStatistics {
    pub fn new(column_count: usize) -> Self {
        Self {
            row_count: 0,
            column_count,
            estimated_size: 0,
            page_count: 0,
            stats_up_to_date: true,
            column_stats: vec![ColumnStatistics::new(); column_count],
            last_updated: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            inserts_since_update: 0,
            deletes_since_update: 0,
            updates_since_update: 0,
        }
    }

    pub fn mark_dirty(&mut self) {
        self.stats_up_to_date = false;
    }

    pub fn mark_clean(&mut self) {
        self.stats_up_to_date = true;
        self.last_updated = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        self.inserts_since_update = 0;
        self.deletes_since_update = 0;
        self.updates_since_update = 0;
    }

    pub fn update_for_insert(&mut self, _row_index: usize, values: &[Value]) {
        self.row_count += 1;
        self.inserts_since_update += 1;
        self.mark_dirty();

        for (i, value) in values.iter().enumerate() {
            if i < self.column_stats.len() {
                self.column_stats[i].update_for_value(value);
            }
        }

        self.update_estimated_size();
    }

    pub fn update_for_delete(&mut self) {
        if self.row_count > 0 {
            self.row_count -= 1;
            self.deletes_since_update += 1;
            self.mark_dirty();
            self.update_estimated_size();
        }
    }

    pub fn update_for_update(&mut self, column_index: usize, old_value: &Value, new_value: &Value) {
        // Note: updates_since_update is now tracked at row level in update_row
        self.mark_dirty();

        if column_index < self.column_stats.len() {
            // Subtract old value contribution
            if !old_value.is_null() {
                self.column_stats[column_index].non_null_count -= 1;
                self.column_stats[column_index].column_size = self.column_stats[column_index]
                    .column_size
                    .saturating_sub(old_value.get_size());
            } else {
                self.column_stats[column_index].null_count -= 1;
            }

            // Add new value contribution
            self.column_stats[column_index].update_for_value(new_value);
        }

        self.update_estimated_size();
    }

    fn update_estimated_size(&mut self) {
        self.estimated_size = self.column_stats.iter().map(|s| s.column_size).sum();

        // Estimate page count (assuming 4KB pages)
        const PAGE_SIZE: usize = 4096;
        self.page_count = (self.estimated_size + PAGE_SIZE - 1) / PAGE_SIZE;
    }

    pub fn needs_update(&self, threshold: usize) -> bool {
        !self.stats_up_to_date
            || self.inserts_since_update > threshold
            || self.deletes_since_update > threshold
            || self.updates_since_update > threshold
    }

    pub fn get_column_stat(&self, column_index: usize) -> Option<&ColumnStatistics> {
        self.column_stats.get(column_index)
    }
}

/// Column information for tables
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    /// Column name
    pub name: String,
    /// Column type
    pub column_type: LogicalType,
    /// Whether column can contain null values
    pub nullable: bool,
    /// Default value for the column
    pub default_value: Option<Value>,
    /// Column position in the table
    pub column_index: usize,
    /// Whether column is part of primary key
    pub is_primary_key: bool,
    /// Whether column has unique constraint
    pub is_unique: bool,
}

impl ColumnInfo {
    pub fn new(name: String, column_type: LogicalType, column_index: usize) -> Self {
        Self {
            name,
            column_type,
            nullable: true,
            default_value: None,
            column_index,
            is_primary_key: false,
            is_unique: false,
        }
    }

    pub fn primary_key(name: String, column_type: LogicalType, column_index: usize) -> Self {
        Self {
            name,
            column_type,
            nullable: false,
            default_value: None,
            column_index,
            is_primary_key: true,
            is_unique: true,
        }
    }

    pub fn unique(name: String, column_type: LogicalType, column_index: usize) -> Self {
        Self {
            name,
            column_type,
            nullable: true,
            default_value: None,
            column_index,
            is_primary_key: false,
            is_unique: true,
        }
    }
}

/// Table metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableInfo {
    /// Table name
    pub name: String,
    /// Table name (alias for compatibility)
    pub table_name: String,
    /// Schema name
    pub schema_name: String,
    /// Table columns
    pub columns: Vec<ColumnInfo>,
    /// Primary key columns
    pub primary_key: Vec<usize>,
    /// Table statistics
    pub statistics: TableStatistics,
    /// Whether table is temporary
    pub is_temporary: bool,
}

impl TableInfo {
    pub fn new(name: String) -> Self {
        Self {
            table_name: name.clone(),
            name,
            schema_name: "main".to_string(),
            columns: Vec::new(),
            primary_key: Vec::new(),
            statistics: TableStatistics::new(0),
            is_temporary: false,
        }
    }

    pub fn new_with_schema(schema_name: String, table_name: String) -> Self {
        Self {
            name: table_name.clone(),
            table_name,
            schema_name,
            columns: Vec::new(),
            primary_key: Vec::new(),
            statistics: TableStatistics::new(0),
            is_temporary: false,
        }
    }

    pub fn add_column(&mut self, column: ColumnInfo) -> PrismDBResult<()> {
        // Check for duplicate column names
        if self.columns.iter().any(|c| c.name == column.name) {
            return Err(PrismDBError::InvalidValue(format!(
                "Column '{}' already exists in table '{}'",
                column.name, self.name
            )));
        }

        self.columns.push(column);
        self.statistics.column_count = self.columns.len();

        // Add column statistics
        self.statistics.column_stats.push(ColumnStatistics::new());
        Ok(())
    }

    pub fn get_column(&self, name: &str) -> Option<&ColumnInfo> {
        self.columns.iter().find(|c| c.name == name)
    }

    pub fn get_column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    pub fn set_primary_key(&mut self, columns: Vec<String>) -> PrismDBResult<()> {
        self.primary_key.clear();

        for column_name in columns {
            if let Some(index) = self.get_column_index(&column_name) {
                self.primary_key.push(index);
                self.columns[index].is_primary_key = true;
                self.columns[index].is_unique = true;
            } else {
                return Err(PrismDBError::InvalidValue(format!(
                    "Column '{}' not found in table '{}'",
                    column_name, self.name
                )));
            }
        }

        Ok(())
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }
}

/// Table data storage
#[derive(Debug)]
pub struct TableData {
    /// Table metadata
    pub info: TableInfo,
    /// Column data storage
    pub columns: Vec<Arc<RwLock<ColumnData>>>,
    /// Row data (stored column-wise)
    pub row_count: usize,
    /// Table capacity
    pub capacity: usize,
    /// Bitmap to track deleted rows (true = deleted, false = active)
    pub deleted_rows: Vec<bool>,
}

impl TableData {
    /// Create a new table with the given schema
    pub fn new(mut info: TableInfo, capacity: usize) -> PrismDBResult<Self> {
        let mut columns = Vec::with_capacity(info.columns.len());

        // Ensure statistics are properly initialized
        if info.statistics.column_stats.len() != info.columns.len() {
            info.statistics = TableStatistics::new(info.columns.len());
        }

        for column_info in &info.columns {
            let column_data = ColumnData::new(column_info.clone(), capacity)?;
            columns.push(Arc::new(RwLock::new(column_data)));
        }

        Ok(Self {
            info,
            columns,
            row_count: 0,
            capacity,
            deleted_rows: Vec::new(),
        })
    }

    /// Get the number of active (non-deleted) rows in the table
    pub fn row_count(&self) -> usize {
        // Count rows that are not marked as deleted
        let deleted_count = self.deleted_rows.iter().filter(|&&is_deleted| is_deleted).count();
        self.row_count - deleted_count
    }

    /// Get the total physical row count (including deleted rows)
    /// This is used internally for iterating over all rows
    pub fn physical_row_count(&self) -> usize {
        self.row_count
    }

    /// Get the number of columns in the table
    pub fn column_count(&self) -> usize {
        self.info.columns.len()
    }

    /// Get column data by index
    pub fn get_column(&self, index: usize) -> Option<Arc<RwLock<ColumnData>>> {
        self.columns.get(index).cloned()
    }

    /// Get column data by name
    pub fn get_column_by_name(&self, name: &str) -> Option<Arc<RwLock<ColumnData>>> {
        if let Some(index) = self.info.get_column_index(name) {
            self.get_column(index)
        } else {
            None
        }
    }

    /// Insert a row into the table
    pub fn insert_row(&mut self, row: &[Value]) -> PrismDBResult<usize> {
        if row.len() != self.columns.len() {
            return Err(PrismDBError::InvalidValue(format!(
                "Row has {} values but table has {} columns",
                row.len(),
                self.columns.len()
            )));
        }

        if self.row_count >= self.capacity {
            return Err(PrismDBError::InvalidValue(
                "Table capacity exceeded".to_string(),
            ));
        }

        // Insert values into each column
        for (i, value) in row.iter().enumerate() {
            let mut column_data = self.columns[i]
                .write()
                .map_err(|_| PrismDBError::Internal("Column lock poisoned".to_string()))?;
            column_data.push_value(value)?;
        }

        let row_id = self.row_count;
        self.row_count += 1;

        // Mark row as not deleted
        self.deleted_rows.push(false);

        // Update statistics
        self.info.statistics.update_for_insert(row_id, row);

        Ok(row_id)
    }

    /// Get a row from the table
    pub fn get_row(&self, row_id: usize) -> PrismDBResult<Vec<Value>> {
        if row_id >= self.row_count {
            return Err(PrismDBError::InvalidValue(format!(
                "Row ID {} out of bounds (max: {})",
                row_id, self.row_count
            )));
        }

        let mut row = Vec::with_capacity(self.columns.len());

        for column_data in &self.columns {
            let column = column_data
                .read()
                .map_err(|_| PrismDBError::Internal("Column lock poisoned".to_string()))?;
            let value = column.get_value(row_id)?;
            row.push(value);
        }

        Ok(row)
    }

    /// Update a row in the table
    pub fn update_row(&mut self, row_id: usize, row: &[Value]) -> PrismDBResult<()> {
        if row_id >= self.row_count {
            return Err(PrismDBError::InvalidValue(format!(
                "Row ID {} out of bounds (max: {})",
                row_id, self.row_count
            )));
        }

        if row.len() != self.columns.len() {
            return Err(PrismDBError::InvalidValue(format!(
                "Row has {} values but Table has {} columns",
                row.len(),
                self.columns.len()
            )));
        }

        // Collect old values for statistics before updating
        let mut old_values = Vec::with_capacity(row.len());
        for (i, _value) in row.iter().enumerate() {
            let old_value = {
                let column_data = self.columns[i]
                    .read()
                    .map_err(|_| PrismDBError::Internal("Column lock poisoned".to_string()))?;
                column_data.get_value(row_id)?
            };
            old_values.push(old_value);
        }

        // Update values in each column
        for (i, value) in row.iter().enumerate() {
            // Update the value
            {
                let mut column_data = self.columns[i]
                    .write()
                    .map_err(|_| PrismDBError::Internal("Column lock poisoned".to_string()))?;
                column_data.set_value(row_id, value)?;
            }

            // Update column-level statistics
            self.info
                .statistics
                .update_for_update(i, &old_values[i], value);
        }

        // Update row-level statistics once per row update
        self.info.statistics.updates_since_update += 1;

        Ok(())
    }

    /// Delete a row from the table
    pub fn delete_row(&mut self, row_id: usize) -> PrismDBResult<()> {
        if row_id >= self.row_count {
            return Err(PrismDBError::InvalidValue(format!(
                "Row ID {} out of bounds (max: {})",
                row_id, self.row_count
            )));
        }

        // Check if row is already deleted
        if row_id < self.deleted_rows.len() && self.deleted_rows[row_id] {
            return Ok(()); // Already deleted, nothing to do
        }

        // Mark row as deleted in the bitmap
        if row_id >= self.deleted_rows.len() {
            self.deleted_rows.resize(row_id + 1, false);
        }
        self.deleted_rows[row_id] = true;

        self.info.statistics.update_for_delete();
        Ok(())
    }

    /// Create a data chunk from the table data including all rows (even deleted ones)
    /// This is used by UPDATE and DELETE operations that need to see all physical rows
    pub fn create_chunk_unfiltered(&self, start_row: usize, max_rows: usize) -> PrismDBResult<DataChunk> {
        let end_row = std::cmp::min(start_row + max_rows, self.row_count);
        let actual_rows = end_row - start_row;

        if actual_rows == 0 {
            return Ok(DataChunk::new());
        }

        let mut vectors = Vec::with_capacity(self.columns.len());

        for column_data in &self.columns {
            let column = column_data
                .read()
                .map_err(|_| PrismDBError::Internal("Column lock poisoned".to_string()))?;

            let vector = column.create_vector(start_row, actual_rows)?;
            vectors.push(vector);
        }

        DataChunk::from_vectors(vectors)
    }

    /// Create a data chunk from the table data
    pub fn create_chunk(&self, start_row: usize, max_rows: usize) -> PrismDBResult<DataChunk> {
        // When filtering deleted rows, we need to scan beyond max_rows to find enough active rows
        // So we scan all physical rows starting from start_row
        let end_row = self.row_count;  // Scan all physical rows

        // Collect row indices that are not deleted
        let mut active_rows = Vec::new();
        for row_id in start_row..end_row {
            // Check if row is deleted (if deleted_rows is smaller than row_id, the row is not deleted)
            let is_deleted = row_id < self.deleted_rows.len() && self.deleted_rows[row_id];
            if !is_deleted {
                active_rows.push(row_id);
                // Stop once we have collected max_rows active rows
                if active_rows.len() >= max_rows {
                    break;
                }
            }
        }

        if active_rows.is_empty() {
            return Ok(DataChunk::new());
        }

        // Create vectors with only active (non-deleted) rows
        let mut vectors = Vec::with_capacity(self.columns.len());

        for column_data in &self.columns {
            let column = column_data
                .read()
                .map_err(|_| PrismDBError::Internal("Column lock poisoned".to_string()))?;

            // Collect values from active rows only
            let mut column_values = Vec::with_capacity(active_rows.len());
            for &row_id in &active_rows {
                let value = column.get_value(row_id)?;
                column_values.push(value);
            }

            // Create vector from the collected values
            if !column_values.is_empty() {
                let vector = crate::types::Vector::from_values(&column_values)?;
                vectors.push(vector);
            }
        }

        if vectors.is_empty() {
            return Ok(DataChunk::new());
        }

        DataChunk::from_vectors(vectors)
    }

    /// Get table statistics
    pub fn get_statistics(&self) -> &TableStatistics {
        &self.info.statistics
    }

    /// Update table statistics
    pub fn update_statistics(&mut self) {
        if !self.info.statistics.stats_up_to_date {
            // Recalculate statistics
            self.info.statistics.row_count = self.row_count;
            self.info.statistics.column_count = self.columns.len();

            // Estimate size based on column data
            let mut total_size = 0;
            for column_data in &self.columns {
                let column = column_data.read().unwrap(); // Safe unwrap for statistics calculation
                total_size += column.estimate_size();
            }
            self.info.statistics.estimated_size = total_size;

            self.info.statistics.mark_clean();
        }
    }

    /// Resize the table capacity
    pub fn resize(&mut self, new_capacity: usize) -> PrismDBResult<()> {
        if new_capacity < self.row_count {
            return Err(PrismDBError::InvalidValue(format!(
                "Cannot resize to {} - current row count is {}",
                new_capacity, self.row_count
            )));
        }

        if new_capacity == self.capacity {
            return Ok(());
        }

        // Resize each column
        for column_data in &self.columns {
            let mut column = column_data
                .write()
                .map_err(|_| PrismDBError::Internal("Column lock poisoned".to_string()))?;
            column.resize(new_capacity)?;
        }

        self.capacity = new_capacity;
        Ok(())
    }

    /// Clear all rows from the table
    pub fn clear_rows(&mut self) -> PrismDBResult<()> {
        // Clear each column
        for column_data in &self.columns {
            let mut column = column_data
                .write()
                .map_err(|_| PrismDBError::Internal("Column lock poisoned".to_string()))?;

            // Clear values and null mask
            column.clear();
        }

        self.row_count = 0;

        // Update statistics
        self.info.statistics.row_count = 0;
        self.info.statistics.mark_dirty();

        Ok(())
    }

    /// Insert values into the table (wrapper for insert_row)
    pub fn insert(&mut self, values: &[Value]) -> PrismDBResult<()> {
        self.insert_row(values)?;
        Ok(())
    }

    /// Add a column to the table
    pub fn add_column(&mut self, column_info: &ColumnInfo) -> PrismDBResult<()> {
        // Check for duplicate column names
        if self.info.columns.iter().any(|c| c.name == column_info.name) {
            return Err(PrismDBError::InvalidValue(format!(
                "Column '{}' already exists in table '{}'",
                column_info.name, self.info.name
            )));
        }

        // Add column to info
        self.info.columns.push(column_info.clone());

        // Create new column data
        let column_data = ColumnData::new(column_info.clone(), self.capacity)?;
        self.columns.push(Arc::new(RwLock::new(column_data)));

        // Add column statistics
        self.info
            .statistics
            .column_stats
            .push(ColumnStatistics::new());
        self.info.statistics.column_count = self.columns.len();

        Ok(())
    }

    /// Remove a column from the table
    pub fn remove_column(&mut self, column_name: &str) -> PrismDBResult<()> {
        let column_index = self.info.get_column_index(column_name).ok_or_else(|| {
            PrismDBError::InvalidValue(format!(
                "Column '{}' not found in table '{}'",
                column_name, self.info.name
            ))
        })?;

        // Remove from info
        self.info.columns.remove(column_index);

        // Remove column data
        self.columns.remove(column_index);

        // Remove column statistics
        if column_index < self.info.statistics.column_stats.len() {
            self.info.statistics.column_stats.remove(column_index);
        }
        self.info.statistics.column_count = self.columns.len();

        Ok(())
    }

    /// Rename a column in the table
    pub fn rename_column(&mut self, old_name: &str, new_name: &str) -> PrismDBResult<()> {
        let column_index = self.info.get_column_index(old_name).ok_or_else(|| {
            PrismDBError::InvalidValue(format!(
                "Column '{}' not found in table '{}'",
                old_name, self.info.name
            ))
        })?;

        // Check if new name already exists
        if self.info.columns.iter().any(|c| c.name == new_name) {
            return Err(PrismDBError::InvalidValue(format!(
                "Column '{}' already exists in table '{}'",
                new_name, self.info.name
            )));
        }

        // Update column name in info
        self.info.columns[column_index].name = new_name.to_string();

        // Update column name in column data
        if let Some(column_arc) = self.columns.get(column_index) {
            let mut column = column_arc
                .write()
                .map_err(|_| PrismDBError::Internal("Column lock poisoned".to_string()))?;
            column.info.name = new_name.to_string();
        }

        Ok(())
    }

    /// Get column data by name
    pub fn get_column_data(&self, column_name: &str) -> PrismDBResult<Arc<ColumnData>> {
        let column_index = self.info.get_column_index(column_name).ok_or_else(|| {
            PrismDBError::InvalidValue(format!(
                "Column '{}' not found in table '{}'",
                column_name, self.info.name
            ))
        })?;

        if let Some(column_arc) = self.columns.get(column_index) {
            let column = column_arc
                .read()
                .map_err(|_| PrismDBError::Internal("Column lock poisoned".to_string()))?;

            // Create a new Arc with a clone of the ColumnData
            Ok(Arc::new(column.clone()))
        } else {
            Err(PrismDBError::InvalidValue(format!(
                "Column data not found for column '{}'",
                column_name
            )))
        }
    }

    /// Get table size in bytes
    pub fn size_bytes(&self) -> u64 {
        let mut total_size = 0u64;
        for column_data in &self.columns {
            if let Ok(column) = column_data.read() {
                total_size += column.estimate_size() as u64;
            }
        }
        total_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_info() {
        let mut table = TableInfo::new("users".to_string());

        let id_col = ColumnInfo::primary_key("id".to_string(), LogicalType::Integer, 0);
        let name_col = ColumnInfo::new("name".to_string(), LogicalType::Varchar, 1);

        table.add_column(id_col).unwrap();
        table.add_column(name_col).unwrap();

        assert_eq!(table.column_count(), 2);
        assert!(table.get_column("id").is_some());
        assert!(table.get_column("name").is_some());
        assert!(table.get_column("nonexistent").is_none());
    }

    #[test]
    fn test_table_data() -> PrismDBResult<()> {
        let mut table_info = TableInfo::new("test".to_string());
        table_info
            .add_column(ColumnInfo::new("id".to_string(), LogicalType::Integer, 0))
            .unwrap();
        table_info
            .add_column(ColumnInfo::new("name".to_string(), LogicalType::Varchar, 1))
            .unwrap();

        let mut table = TableData::new(table_info, 10)?;

        // Insert a row
        let row = vec![Value::integer(1), Value::varchar("Alice".to_string())];
        let row_id = table.insert_row(&row)?;
        assert_eq!(row_id, 0);
        assert_eq!(table.row_count(), 1);

        // Retrieve the row
        let retrieved = table.get_row(0)?;
        assert_eq!(retrieved[0], Value::integer(1));
        assert_eq!(retrieved[1], Value::varchar("Alice".to_string()));

        Ok(())
    }
}
