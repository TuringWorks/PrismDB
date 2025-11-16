//! Schema Management
//!
//! Provides schema management functionality for organizing database objects.

use crate::catalog::{Index, IndexInfo, ObjectMetadata, Table, TableInfo, View};
use crate::common::error::{PrismDBError, PrismDBResult};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Database schema
#[derive(Debug)]
pub struct Schema {
    /// Schema name
    pub name: String,
    /// Tables in the schema
    tables: HashMap<String, Arc<RwLock<Table>>>,
    /// Views in the schema
    views: HashMap<String, Arc<RwLock<View>>>,
    /// Indexes in the schema
    indexes: HashMap<String, Arc<RwLock<Index>>>,
    /// Schema metadata
    pub metadata: ObjectMetadata,
}

impl Schema {
    /// Create a new schema
    pub fn new(name: String) -> Self {
        Self {
            name,
            tables: HashMap::new(),
            views: HashMap::new(),
            indexes: HashMap::new(),
            metadata: ObjectMetadata::new(),
        }
    }

    /// Get schema name
    pub fn get_name(&self) -> &str {
        &self.name
    }

    /// Create a table
    pub fn create_table(&mut self, table_info: &TableInfo) -> PrismDBResult<()> {
        if self.tables.contains_key(&table_info.table_name) {
            return Err(PrismDBError::Catalog(format!(
                "Table '{}' already exists in schema '{}'",
                table_info.table_name, self.name
            )));
        }

        let table = Table::new(table_info.clone())?;
        self.tables
            .insert(table_info.table_name.clone(), Arc::new(RwLock::new(table)));
        self.metadata.touch();
        Ok(())
    }

    /// Drop a table
    pub fn drop_table(&mut self, table_name: &str) -> PrismDBResult<()> {
        if !self.tables.contains_key(table_name) {
            return Err(PrismDBError::Catalog(format!(
                "Table '{}' does not exist in schema '{}'",
                table_name, self.name
            )));
        }

        // Check for dependent indexes
        let dependent_indexes: Vec<String> = self
            .indexes
            .values()
            .filter(|index| {
                let index = index.read().unwrap();
                index.get_table_name() == table_name
            })
            .map(|index| index.read().unwrap().get_name().to_string())
            .collect();

        // Drop dependent indexes
        for index_name in dependent_indexes {
            self.drop_index(&index_name)?;
        }

        self.tables.remove(table_name);
        self.metadata.touch();
        Ok(())
    }

    /// Get a table
    pub fn get_table(&self, table_name: &str) -> PrismDBResult<Arc<RwLock<Table>>> {
        self.tables.get(table_name).cloned().ok_or_else(|| {
            PrismDBError::Catalog(format!(
                "Table '{}' does not exist in schema '{}'",
                table_name, self.name
            ))
        })
    }

    /// Check if a table exists
    pub fn table_exists(&self, table_name: &str) -> bool {
        self.tables.contains_key(table_name)
    }

    /// List all tables
    pub fn list_tables(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    /// Create a view
    pub fn create_view(
        &mut self,
        view_name: &str,
        query: &str,
        column_names: Vec<String>,
    ) -> PrismDBResult<()> {
        if self.views.contains_key(view_name) {
            return Err(PrismDBError::Catalog(format!(
                "View '{}' already exists in schema '{}'",
                view_name, self.name
            )));
        }

        let view = View::new(view_name.to_string(), query.to_string(), column_names)?;
        self.views
            .insert(view_name.to_string(), Arc::new(RwLock::new(view)));
        self.metadata.touch();
        Ok(())
    }

    /// Drop a view
    pub fn drop_view(&mut self, view_name: &str) -> PrismDBResult<()> {
        if !self.views.contains_key(view_name) {
            return Err(PrismDBError::Catalog(format!(
                "View '{}' does not exist in schema '{}'",
                view_name, self.name
            )));
        }

        self.views.remove(view_name);
        self.metadata.touch();
        Ok(())
    }

    /// Get a view
    pub fn get_view(&self, view_name: &str) -> PrismDBResult<Arc<RwLock<View>>> {
        self.views.get(view_name).cloned().ok_or_else(|| {
            PrismDBError::Catalog(format!(
                "View '{}' does not exist in schema '{}'",
                view_name, self.name
            ))
        })
    }

    /// Check if a view exists
    pub fn view_exists(&self, view_name: &str) -> bool {
        self.views.contains_key(view_name)
    }

    /// List all views
    pub fn list_views(&self) -> Vec<String> {
        self.views.keys().cloned().collect()
    }

    /// Create an index
    pub fn create_index(&mut self, index_info: &IndexInfo) -> PrismDBResult<()> {
        if self.indexes.contains_key(&index_info.index_name) {
            return Err(PrismDBError::Catalog(format!(
                "Index '{}' already exists in schema '{}'",
                index_info.index_name, self.name
            )));
        }

        // Verify the table exists
        if !self.tables.contains_key(&index_info.table_name) {
            return Err(PrismDBError::Catalog(format!(
                "Table '{}' does not exist in schema '{}'",
                index_info.table_name, self.name
            )));
        }

        let index = Index::new(index_info.clone())?;
        self.indexes
            .insert(index_info.index_name.clone(), Arc::new(RwLock::new(index)));
        self.metadata.touch();
        Ok(())
    }

    /// Drop an index
    pub fn drop_index(&mut self, index_name: &str) -> PrismDBResult<()> {
        if !self.indexes.contains_key(index_name) {
            return Err(PrismDBError::Catalog(format!(
                "Index '{}' does not exist in schema '{}'",
                index_name, self.name
            )));
        }

        self.indexes.remove(index_name);
        self.metadata.touch();
        Ok(())
    }

    /// Get an index
    pub fn get_index(&self, index_name: &str) -> PrismDBResult<Arc<RwLock<Index>>> {
        self.indexes.get(index_name).cloned().ok_or_else(|| {
            PrismDBError::Catalog(format!(
                "Index '{}' does not exist in schema '{}'",
                index_name, self.name
            ))
        })
    }

    /// Check if an index exists
    pub fn index_exists(&self, index_name: &str) -> bool {
        self.indexes.contains_key(index_name)
    }

    /// List all indexes
    pub fn list_indexes(&self) -> Vec<String> {
        self.indexes.keys().cloned().collect()
    }

    /// Get indexes for a specific table
    pub fn get_table_indexes(&self, table_name: &str) -> Vec<Arc<RwLock<Index>>> {
        self.indexes
            .values()
            .filter(|index| {
                let index = index.read().unwrap();
                index.get_table_name() == table_name
            })
            .cloned()
            .collect()
    }

    /// Get all objects in the schema
    pub fn get_all_objects(&self) -> SchemaObjects {
        SchemaObjects {
            tables: self.tables.values().cloned().collect(),
            views: self.views.values().cloned().collect(),
            indexes: self.indexes.values().cloned().collect(),
        }
    }

    /// Get object count
    pub fn get_object_count(&self) -> usize {
        self.tables.len() + self.views.len() + self.indexes.len()
    }

    /// Clear all objects (for testing)
    pub fn clear(&mut self) {
        self.tables.clear();
        self.views.clear();
        self.indexes.clear();
        self.metadata.touch();
    }
}

/// Collection of all objects in a schema
#[derive(Debug)]
pub struct SchemaObjects {
    pub tables: Vec<Arc<RwLock<Table>>>,
    pub views: Vec<Arc<RwLock<View>>>,
    pub indexes: Vec<Arc<RwLock<Index>>>,
}

/// Schema statistics
#[derive(Debug, Clone)]
pub struct SchemaStats {
    pub table_count: usize,
    pub view_count: usize,
    pub index_count: usize,
    pub total_rows: usize,
    pub total_size_bytes: u64,
}

impl Schema {
    /// Get schema statistics
    pub fn get_stats(&self) -> SchemaStats {
        let mut total_rows = 0;
        let mut total_size_bytes = 0;

        for table in self.tables.values() {
            let table = table.read().unwrap();
            total_rows += table.row_count();
            total_size_bytes += table.size_bytes();
        }

        SchemaStats {
            table_count: self.tables.len(),
            view_count: self.views.len(),
            index_count: self.indexes.len(),
            total_rows,
            total_size_bytes,
        }
    }

    /// Validate schema integrity
    pub fn validate(&self) -> PrismDBResult<()> {
        // Check that all indexes reference existing tables
        for index in self.indexes.values() {
            let index = index.read().unwrap();
            if !self.tables.contains_key(index.get_table_name()) {
                return Err(PrismDBError::Catalog(format!(
                    "Index '{}' references non-existent table '{}'",
                    index.get_name(),
                    index.get_table_name()
                )));
            }
        }

        // Check that all index columns exist in their respective tables
        for index in self.indexes.values() {
            let index = index.read().unwrap();
            if let Ok(table) = self.get_table(index.get_table_name()) {
                let table = table.read().unwrap();
                for column_name in index.get_column_names() {
                    if !table.has_column(column_name) {
                        return Err(PrismDBError::Catalog(format!(
                            "Index '{}' references non-existent column '{}' in table '{}'",
                            index.get_name(),
                            column_name,
                            index.get_table_name()
                        )));
                    }
                }
            }
        }

        Ok(())
    }
}
