//! View Management
//!
//! Provides view management functionality for stored queries.

use crate::catalog::ObjectMetadata;
use crate::common::error::{PrismDBError, PrismDBResult};
use crate::types::DataChunk;

/// Refresh strategy for materialized views
#[derive(Debug, Clone, PartialEq)]
pub enum RefreshStrategy {
    /// Manual refresh via REFRESH MATERIALIZED VIEW command
    Manual,
    /// Automatic refresh on commit (eager)
    OnCommit,
    /// Automatic refresh on demand (lazy)
    OnDemand,
    /// Incremental refresh (only update changes)
    Incremental,
}

/// Materialized view metadata
#[derive(Debug)]
pub struct MaterializedViewMetadata {
    /// Last refresh timestamp
    pub last_refresh: Option<u64>,
    /// Number of rows in materialized data
    pub row_count: usize,
    /// Refresh strategy
    pub refresh_strategy: RefreshStrategy,
    /// Whether data is currently stale
    pub is_stale: bool,
    /// Base tables this view depends on
    pub dependencies: Vec<String>,
}

impl MaterializedViewMetadata {
    pub fn new(refresh_strategy: RefreshStrategy) -> Self {
        Self {
            last_refresh: None,
            row_count: 0,
            refresh_strategy,
            is_stale: true,
            dependencies: Vec::new(),
        }
    }

    pub fn mark_refreshed(&mut self, row_count: usize) {
        use std::time::{SystemTime, UNIX_EPOCH};
        self.last_refresh = Some(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        );
        self.row_count = row_count;
        self.is_stale = false;
    }

    pub fn mark_stale(&mut self) {
        self.is_stale = true;
    }
}

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
    /// Whether this is a materialized view
    pub is_materialized: bool,
    /// Materialized view specific metadata
    pub materialized_metadata: Option<MaterializedViewMetadata>,
    /// Stored data chunks (only for materialized views)
    pub materialized_data: Option<Vec<DataChunk>>,
}

impl View {
    /// Create a new regular (non-materialized) view
    pub fn new(name: String, query: String, column_names: Vec<String>) -> PrismDBResult<Self> {
        Ok(Self {
            name,
            query,
            column_names,
            metadata: ObjectMetadata::new(),
            is_materialized: false,
            materialized_metadata: None,
            materialized_data: None,
        })
    }

    /// Create a new materialized view
    pub fn new_materialized(
        name: String,
        query: String,
        column_names: Vec<String>,
        refresh_strategy: RefreshStrategy,
    ) -> PrismDBResult<Self> {
        Ok(Self {
            name,
            query,
            column_names,
            metadata: ObjectMetadata::new(),
            is_materialized: true,
            materialized_metadata: Some(MaterializedViewMetadata::new(refresh_strategy)),
            materialized_data: Some(Vec::new()),
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

    /// Refresh materialized view data
    pub fn refresh(&mut self, data: Vec<DataChunk>) -> PrismDBResult<()> {
        if !self.is_materialized {
            return Err(PrismDBError::Catalog(
                "Cannot refresh non-materialized view".to_string(),
            ));
        }

        let total_rows: usize = data.iter().map(|chunk| chunk.count()).sum();

        self.materialized_data = Some(data);

        if let Some(ref mut metadata) = self.materialized_metadata {
            metadata.mark_refreshed(total_rows);
        }

        Ok(())
    }

    /// Get materialized data
    pub fn get_materialized_data(&self) -> PrismDBResult<&Vec<DataChunk>> {
        if !self.is_materialized {
            return Err(PrismDBError::Catalog(
                "View is not materialized".to_string(),
            ));
        }

        self.materialized_data.as_ref().ok_or_else(|| {
            PrismDBError::Catalog("Materialized data not available".to_string())
        })
    }

    /// Check if materialized view is stale
    pub fn is_stale(&self) -> bool {
        self.materialized_metadata
            .as_ref()
            .map(|m| m.is_stale)
            .unwrap_or(false)
    }

    /// Mark materialized view as stale
    pub fn mark_stale(&mut self) -> PrismDBResult<()> {
        if !self.is_materialized {
            return Err(PrismDBError::Catalog(
                "Cannot mark non-materialized view as stale".to_string(),
            ));
        }

        if let Some(ref mut metadata) = self.materialized_metadata {
            metadata.mark_stale();
        }

        Ok(())
    }

    /// Get refresh strategy
    pub fn get_refresh_strategy(&self) -> Option<&RefreshStrategy> {
        self.materialized_metadata
            .as_ref()
            .map(|m| &m.refresh_strategy)
    }

    /// Add dependency
    pub fn add_dependency(&mut self, table_name: String) -> PrismDBResult<()> {
        if !self.is_materialized {
            return Ok(()); // Regular views don't track dependencies for staleness
        }

        if let Some(ref mut metadata) = self.materialized_metadata {
            if !metadata.dependencies.contains(&table_name) {
                metadata.dependencies.push(table_name);
            }
        }

        Ok(())
    }

    /// Get row count (for materialized views)
    pub fn get_row_count(&self) -> Option<usize> {
        self.materialized_metadata.as_ref().map(|m| m.row_count)
    }
}
