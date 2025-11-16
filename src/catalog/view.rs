//! View Management
//!
//! Provides view management functionality for stored queries.

use crate::catalog::ObjectMetadata;
use crate::common::error::{PrismDBError, PrismDBResult};

/// Database view
#[derive(Debug)]
pub struct View {
    /// View name
    pub name: String,
    /// View query
    pub query: String,
    /// Column names
    pub column_names: Vec<String>,
    /// View metadata
    pub metadata: ObjectMetadata,
}

impl View {
    /// Create a new view
    pub fn new(name: String, query: String, column_names: Vec<String>) -> PrismDBResult<Self> {
        Ok(Self {
            name,
            query,
            column_names,
            metadata: ObjectMetadata::new(),
        })
    }

    /// Get view name
    pub fn get_name(&self) -> &str {
        &self.name
    }

    /// Get view query
    pub fn get_query(&self) -> &str {
        &self.query
    }

    /// Get column names
    pub fn get_column_names(&self) -> &[String] {
        &self.column_names
    }

    /// Get column count
    pub fn column_count(&self) -> usize {
        self.column_names.len()
    }

    /// Check if view has a column
    pub fn has_column(&self, column_name: &str) -> bool {
        self.column_names.contains(&column_name.to_string())
    }

    /// Get column index by name
    pub fn get_column_index(&self, column_name: &str) -> Option<usize> {
        self.column_names
            .iter()
            .position(|name| name == column_name)
    }

    /// Validate view definition
    pub fn validate(&self) -> PrismDBResult<()> {
        if self.name.is_empty() {
            return Err(PrismDBError::Catalog(
                "View name cannot be empty".to_string(),
            ));
        }

        if self.query.is_empty() {
            return Err(PrismDBError::Catalog(
                "View query cannot be empty".to_string(),
            ));
        }

        // In a real implementation, we would parse and validate the query
        // For now, just check basic syntax
        if !self.query.to_lowercase().contains("select") {
            return Err(PrismDBError::Catalog(
                "View query must be a SELECT statement".to_string(),
            ));
        }

        Ok(())
    }
}
