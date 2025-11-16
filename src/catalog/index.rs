//! Index Management
//!
//! Provides index management functionality for performance optimization.

use crate::catalog::ObjectMetadata;
use crate::common::error::{PrismDBError, PrismDBResult};
use std::collections::HashSet;

/// Index information
#[derive(Debug, Clone)]
pub struct IndexInfo {
    /// Index name
    pub index_name: String,
    /// Schema name
    pub schema_name: String,
    /// Table name
    pub table_name: String,
    /// Column names
    pub column_names: Vec<String>,
    /// Index type
    pub index_type: IndexType,
    /// Unique constraint
    pub unique: bool,
    /// Index options
    pub options: IndexOptions,
}

/// Index type
#[derive(Debug, Clone, PartialEq)]
pub enum IndexType {
    BTree,
    Hash,
    GIST,   // Generalized Search Tree
    GIN,    // Generalized Inverted Index
    SPGiST, // Space-Partitioned GiST
    BRIN,   // Block Range Index
}

/// Index options
#[derive(Debug, Clone, Default)]
pub struct IndexOptions {
    /// Fill factor for B-tree indexes
    pub fill_factor: Option<f32>,
    /// Page size
    pub page_size: Option<usize>,
    /// Custom options
    pub custom_options: std::collections::HashMap<String, String>,
}

/// Database index
#[derive(Debug)]
pub struct Index {
    /// Index information
    pub info: IndexInfo,
    /// Index metadata
    pub metadata: ObjectMetadata,
    /// Index statistics
    pub statistics: IndexStatistics,
}

impl Index {
    /// Create a new index
    pub fn new(info: IndexInfo) -> PrismDBResult<Self> {
        let statistics = IndexStatistics::new(&info);

        Ok(Self {
            info,
            metadata: ObjectMetadata::new(),
            statistics,
        })
    }

    /// Get index name
    pub fn get_name(&self) -> &str {
        &self.info.index_name
    }

    /// Get table name
    pub fn get_table_name(&self) -> &str {
        &self.info.table_name
    }

    /// Get column names
    pub fn get_column_names(&self) -> &[String] {
        &self.info.column_names
    }

    /// Get index type
    pub fn get_index_type(&self) -> &IndexType {
        &self.info.index_type
    }

    /// Check if index is unique
    pub fn is_unique(&self) -> bool {
        self.info.unique
    }

    /// Get column count
    pub fn column_count(&self) -> usize {
        self.info.column_names.len()
    }

    /// Check if index contains a column
    pub fn contains_column(&self, column_name: &str) -> bool {
        self.info.column_names.contains(&column_name.to_string())
    }

    /// Get column position
    pub fn get_column_position(&self, column_name: &str) -> Option<usize> {
        self.info
            .column_names
            .iter()
            .position(|name| name == column_name)
    }

    /// Validate index definition
    pub fn validate(&self) -> PrismDBResult<()> {
        if self.info.index_name.is_empty() {
            return Err(PrismDBError::Catalog(
                "Index name cannot be empty".to_string(),
            ));
        }

        if self.info.table_name.is_empty() {
            return Err(PrismDBError::Catalog(
                "Table name cannot be empty".to_string(),
            ));
        }

        if self.info.column_names.is_empty() {
            return Err(PrismDBError::Catalog(
                "Index must have at least one column".to_string(),
            ));
        }

        // Check for duplicate column names
        let mut seen_columns = HashSet::new();
        for column_name in &self.info.column_names {
            if !seen_columns.insert(column_name) {
                return Err(PrismDBError::Catalog(format!(
                    "Duplicate column '{}' in index",
                    column_name
                )));
            }
        }

        // Validate fill factor for B-tree indexes
        if let IndexType::BTree = self.info.index_type {
            if let Some(fill_factor) = self.info.options.fill_factor {
                if fill_factor <= 0.0 || fill_factor > 1.0 {
                    return Err(PrismDBError::Catalog(
                        "Fill factor must be between 0.0 and 1.0".to_string(),
                    ));
                }
            }
        }

        Ok(())
    }

    /// Get index information
    pub fn get_info(&self) -> &IndexInfo {
        &self.info
    }

    /// Update index statistics
    pub fn update_statistics(&mut self) {
        self.statistics.update();
    }
}

/// Index statistics
#[derive(Debug, Clone)]
pub struct IndexStatistics {
    /// Number of entries in the index
    pub entry_count: usize,
    /// Index size in bytes
    pub size_bytes: u64,
    /// Number of levels (for tree indexes)
    pub levels: Option<usize>,
    /// Average key size
    pub avg_key_size: f64,
    /// Clustering factor (how ordered the index is)
    pub clustering_factor: f64,
    /// Last updated
    pub last_updated: chrono::DateTime<chrono::Utc>,
}

impl IndexStatistics {
    /// Create new index statistics
    pub fn new(info: &IndexInfo) -> Self {
        Self {
            entry_count: 0,
            size_bytes: 0,
            levels: match info.index_type {
                IndexType::BTree => Some(1),
                _ => None,
            },
            avg_key_size: 0.0,
            clustering_factor: 1.0,
            last_updated: chrono::Utc::now(),
        }
    }

    /// Update statistics
    pub fn update(&mut self) {
        self.last_updated = chrono::Utc::now();
        // In a real implementation, this would scan the index and update statistics
    }

    /// Get selectivity estimate
    pub fn get_selectivity(&self) -> f64 {
        if self.entry_count == 0 {
            1.0
        } else {
            1.0 / (self.entry_count as f64)
        }
    }

    /// Estimate cost of index lookup
    pub fn estimate_lookup_cost(&self) -> f64 {
        match self.levels {
            Some(levels) => levels as f64, // Tree index: cost = number of levels
            None => 1.0,                   // Hash index: constant time lookup
        }
    }

    /// Estimate cost of index range scan
    pub fn estimate_range_scan_cost(&self, selectivity: f64) -> f64 {
        let base_cost = self.estimate_lookup_cost();
        let scan_cost = (self.entry_count as f64) * selectivity;
        base_cost + scan_cost
    }
}
