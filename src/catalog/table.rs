//! Table Management
//!
//! Provides table management functionality including metadata and statistics.

use crate::common::error::{PrismDBError, PrismDBResult};
use crate::storage::{ColumnData, ColumnInfo, TableData, TableInfo};
use crate::types::LogicalType;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Database table
#[derive(Debug)]
pub struct Table {
    /// Table information
    pub info: TableInfo,
    /// Table data
    data: Arc<RwLock<TableData>>,
    /// Table statistics
    statistics: Arc<RwLock<TableStatistics>>,
    /// Table metadata
    pub metadata: ObjectMetadata,
}

/// Object metadata (re-export from catalog mod)
use crate::catalog::ObjectMetadata;

impl Table {
    /// Create a new table
    pub fn new(info: TableInfo) -> PrismDBResult<Self> {
        let data = TableData::new(info.clone(), 1024)?;
        let statistics = TableStatistics::new(&info);

        Ok(Self {
            info,
            data: Arc::new(RwLock::new(data)),
            statistics: Arc::new(RwLock::new(statistics)),
            metadata: ObjectMetadata::new(),
        })
    }

    /// Get table name
    pub fn get_name(&self) -> &str {
        &self.info.table_name
    }

    /// Get schema name
    pub fn get_schema_name(&self) -> &str {
        &self.info.schema_name
    }

    /// Get table data
    pub fn get_data(&self) -> Arc<RwLock<TableData>> {
        self.data.clone()
    }

    /// Get table statistics
    pub fn get_statistics(&self) -> Arc<RwLock<TableStatistics>> {
        self.statistics.clone()
    }

    /// Get column information
    pub fn get_columns(&self) -> &[ColumnInfo] {
        &self.info.columns
    }

    /// Get column information by name
    pub fn get_column(&self, name: &str) -> Option<&ColumnInfo> {
        self.info.columns.iter().find(|col| col.name == name)
    }

    /// Check if table has a column
    pub fn has_column(&self, name: &str) -> bool {
        self.get_column(name).is_some()
    }

    /// Get column index by name
    pub fn get_column_index(&self, name: &str) -> Option<usize> {
        self.info.columns.iter().position(|col| col.name == name)
    }

    /// Get row count
    pub fn row_count(&self) -> usize {
        self.data.read().unwrap().row_count()
    }

    /// Get table size in bytes
    pub fn size_bytes(&self) -> u64 {
        self.data.read().unwrap().size_bytes()
    }

    /// Add a column
    pub fn add_column(&mut self, column_info: ColumnInfo) -> PrismDBResult<()> {
        // Check if column already exists
        if self.has_column(&column_info.name) {
            return Err(PrismDBError::Catalog(format!(
                "Column '{}' already exists in table '{}'",
                column_info.name, self.info.table_name
            )));
        }

        // Add column to table info
        self.info.columns.push(column_info.clone());

        // Add column to table data
        self.data.write().unwrap().add_column(&column_info)?;

        // Update statistics
        self.statistics.write().unwrap().add_column(&column_info);

        self.metadata.touch();
        Ok(())
    }

    /// Drop a column
    pub fn drop_column(&mut self, column_name: &str) -> PrismDBResult<()> {
        // Check if column exists
        let column_index = self.get_column_index(column_name).ok_or_else(|| {
            PrismDBError::Catalog(format!(
                "Column '{}' does not exist in table '{}'",
                column_name, self.info.table_name
            ))
        })?;

        // Remove column from table info
        self.info.columns.remove(column_index);

        // Remove column from table data
        self.data.write().unwrap().remove_column(column_name)?;

        // Update statistics
        self.statistics.write().unwrap().remove_column(column_name);

        self.metadata.touch();
        Ok(())
    }

    /// Rename a column
    pub fn rename_column(&mut self, old_name: &str, new_name: &str) -> PrismDBResult<()> {
        // Check if old column exists
        let _column = self.get_column(old_name).ok_or_else(|| {
            PrismDBError::Catalog(format!(
                "Column '{}' does not exist in table '{}'",
                old_name, self.info.table_name
            ))
        })?;

        // Check if new name already exists
        if self.has_column(new_name) {
            return Err(PrismDBError::Catalog(format!(
                "Column '{}' already exists in table '{}'",
                new_name, self.info.table_name
            )));
        }

        // Update column name in table info
        for col in &mut self.info.columns {
            if col.name == old_name {
                col.name = new_name.to_string();
                break;
            }
        }

        // Update column name in table data
        self.data
            .write()
            .unwrap()
            .rename_column(old_name, new_name)?;

        // Update statistics
        self.statistics
            .write()
            .unwrap()
            .rename_column(old_name, new_name);

        self.metadata.touch();
        Ok(())
    }

    /// Get column data
    pub fn get_column_data(&self, column_name: &str) -> PrismDBResult<Arc<ColumnData>> {
        self.data.read().unwrap().get_column_data(column_name)
    }

    /// Insert data
    pub fn insert(&self, values: &[crate::Value]) -> PrismDBResult<()> {
        if values.len() != self.info.columns.len() {
            return Err(PrismDBError::Execution(format!(
                "Expected {} values, got {}",
                self.info.columns.len(),
                values.len()
            )));
        }

        // Validate types
        for (i, value) in values.iter().enumerate() {
            let expected_type = &self.info.columns[i].column_type;
            let actual_type = value.get_type();
            if !self.compatible_types(&actual_type, expected_type) {
                return Err(PrismDBError::Execution(format!(
                    "Type mismatch for column '{}': expected {:?}, got {:?}",
                    self.info.columns[i].name, expected_type, actual_type
                )));
            }
        }

        // Insert into table data
        self.data.write().unwrap().insert(values)?;

        // Update statistics
        self.statistics
            .write()
            .unwrap()
            .update_for_insert(0, values);

        Ok(())
    }

    /// Check if types are compatible
    fn compatible_types(&self, actual: &LogicalType, expected: &LogicalType) -> bool {
        match (actual, expected) {
            (LogicalType::Null, _) => true, // NULL can be inserted into any type
            (LogicalType::Integer, LogicalType::BigInt) => true,
            (LogicalType::SmallInt, LogicalType::Integer) => true,
            (LogicalType::TinyInt, LogicalType::SmallInt) => true,
            (LogicalType::Float, LogicalType::Double) => true,
            (a, b) => a == b,
        }
    }

    /// Update table statistics
    pub fn update_statistics(&self) -> PrismDBResult<()> {
        let data = self.data.read().unwrap();
        let mut stats = self.statistics.write().unwrap();

        for column_info in &self.info.columns {
            if let Ok(column_data) = data.get_column_data(&column_info.name) {
                stats.update_column_stats(&column_info.name, &column_data);
            }
        }

        Ok(())
    }

    /// Validate table integrity
    pub fn validate(&self) -> PrismDBResult<()> {
        // Check that all columns have data
        let data = self.data.read().unwrap();
        for column_info in &self.info.columns {
            data.get_column_data(&column_info.name)?;
        }

        // Validate statistics
        let stats = self.statistics.read().unwrap();
        stats.validate()?;

        Ok(())
    }

    /// Get table info as a copy
    pub fn get_table_info(&self) -> TableInfo {
        self.info.clone()
    }
}

/// Table statistics
#[derive(Debug, Clone)]
pub struct TableStatistics {
    /// Row count
    pub row_count: usize,
    /// Table size in bytes
    pub size_bytes: u64,
    /// Column statistics
    pub column_stats: HashMap<String, ColumnStatistics>,
    /// Last updated
    pub last_updated: chrono::DateTime<chrono::Utc>,
}

impl TableStatistics {
    /// Create new table statistics
    pub fn new(table_info: &TableInfo) -> Self {
        let mut column_stats = HashMap::new();
        for column_info in &table_info.columns {
            column_stats.insert(
                column_info.name.clone(),
                ColumnStatistics::new(&column_info.column_type),
            );
        }

        Self {
            row_count: 0,
            size_bytes: 0,
            column_stats,
            last_updated: chrono::Utc::now(),
        }
    }

    /// Update statistics for insert
    pub fn update_for_insert(&mut self, _row_index: usize, values: &[crate::Value]) {
        self.row_count += 1;

        // Update column statistics
        let column_names: Vec<String> = self.column_stats.keys().cloned().collect();
        for (i, value) in values.iter().enumerate() {
            if let Some(column_name) = column_names.get(i) {
                if let Some(col_stats) = self.column_stats.get_mut(column_name) {
                    col_stats.update_for_value(value);
                }
            }
        }

        self.last_updated = chrono::Utc::now();
    }

    /// Get column name by index (helper method)
    #[allow(dead_code)]
    fn get_column_name_by_index(&self, index: usize) -> Option<String> {
        self.column_stats.keys().nth(index).cloned()
    }

    /// Add column statistics
    pub fn add_column(&mut self, column_info: &ColumnInfo) {
        self.column_stats.insert(
            column_info.name.clone(),
            ColumnStatistics::new(&column_info.column_type),
        );
        self.last_updated = chrono::Utc::now();
    }

    /// Remove column statistics
    pub fn remove_column(&mut self, column_name: &str) {
        self.column_stats.remove(column_name);
        self.last_updated = chrono::Utc::now();
    }

    /// Rename column statistics
    pub fn rename_column(&mut self, old_name: &str, new_name: &str) {
        if let Some(col_stats) = self.column_stats.remove(old_name) {
            self.column_stats.insert(new_name.to_string(), col_stats);
        }
        self.last_updated = chrono::Utc::now();
    }

    /// Update column statistics from column data
    pub fn update_column_stats(&mut self, column_name: &str, column_data: &ColumnData) {
        if let Some(col_stats) = self.column_stats.get_mut(column_name) {
            // Update statistics based on column data
            col_stats.update_from_column_data(column_data);
        }
        self.last_updated = chrono::Utc::now();
    }

    /// Get column statistics
    pub fn get_column_stats(&self, column_name: &str) -> Option<&ColumnStatistics> {
        self.column_stats.get(column_name)
    }

    /// Validate statistics
    pub fn validate(&self) -> PrismDBResult<()> {
        // Basic validation - ensure row count is reasonable
        if self.row_count > 0 && self.column_stats.is_empty() {
            return Err(PrismDBError::Catalog(
                "Table has rows but no column statistics".to_string(),
            ));
        }
        Ok(())
    }
}

/// Column statistics
#[derive(Debug, Clone)]
pub struct ColumnStatistics {
    /// Data type
    pub logical_type: LogicalType,
    /// Number of non-null values
    pub non_null_count: usize,
    /// Number of distinct values (estimated)
    pub distinct_count: usize,
    /// Minimum value
    pub min_value: Option<crate::Value>,
    /// Maximum value
    pub max_value: Option<crate::Value>,
    /// Average value (for numeric types)
    pub avg_value: Option<f64>,
    /// Standard deviation (for numeric types)
    pub std_dev: Option<f64>,
    /// Last updated timestamp
    pub last_updated: chrono::DateTime<chrono::Utc>,
}

impl ColumnStatistics {
    /// Create new column statistics
    pub fn new(logical_type: &LogicalType) -> Self {
        Self {
            logical_type: logical_type.clone(),
            non_null_count: 0,
            distinct_count: 0,
            min_value: None,
            max_value: None,
            avg_value: None,
            std_dev: None,
            last_updated: chrono::Utc::now(),
        }
    }

    /// Update statistics for a value
    pub fn update_for_value(&mut self, value: &crate::Value) {
        if !value.is_null() {
            self.non_null_count += 1;

            // Update min/max
            match (&self.min_value, &self.max_value) {
                (None, None) => {
                    self.min_value = Some(value.clone());
                    self.max_value = Some(value.clone());
                }
                (Some(min), Some(max)) => {
                    // Simple comparison - in a real implementation this would be more sophisticated
                    if value.to_string() < min.to_string() {
                        self.min_value = Some(value.clone());
                    }
                    if value.to_string() > max.to_string() {
                        self.max_value = Some(value.clone());
                    }
                }
                _ => {}
            }
        }
    }

    /// Update statistics from column data
    pub fn update_from_column_data(&mut self, column_data: &ColumnData) {
        // This would scan the column data and update statistics
        // For now, just update basic counts
        self.non_null_count = column_data.row_count() - column_data.null_count();
        self.last_updated = chrono::Utc::now();
    }

    /// Get selectivity estimate for a predicate
    pub fn get_selectivity(&self, predicate_type: PredicateType) -> f64 {
        match predicate_type {
            PredicateType::Equals => {
                if self.non_null_count > 0 {
                    1.0 / (self.distinct_count as f64)
                } else {
                    0.0
                }
            }
            PredicateType::NotEquals => {
                if self.non_null_count > 0 {
                    1.0 - (1.0 / (self.distinct_count as f64))
                } else {
                    1.0
                }
            }
            PredicateType::LessThan | PredicateType::LessThanOrEqual => {
                0.33 // Default estimate
            }
            PredicateType::GreaterThan | PredicateType::GreaterThanOrEqual => {
                0.33 // Default estimate
            }
            PredicateType::IsNull => {
                if self.non_null_count > 0 {
                    0.05 // Default estimate for null predicates
                } else {
                    1.0
                }
            }
            PredicateType::IsNotNull => {
                if self.non_null_count > 0 {
                    0.95
                } else {
                    0.0
                }
            }
        }
    }
}

/// Predicate type for selectivity estimation
#[derive(Debug, Clone, PartialEq)]
pub enum PredicateType {
    Equals,
    NotEquals,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    IsNull,
    IsNotNull,
}
