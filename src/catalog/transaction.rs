//! Transaction Catalog Integration
//!
//! Provides catalog integration for transaction management.

use crate::catalog::Catalog;
use crate::common::error::{PrismDBError, PrismDBResult};
use crate::storage::Transaction;
use std::sync::{Arc, RwLock};

/// Transaction-aware catalog wrapper
#[derive(Debug)]
pub struct TransactionCatalog {
    /// Base catalog
    base_catalog: Arc<RwLock<Catalog>>,
    /// Transaction
    transaction: Arc<Transaction>,
    /// Local changes made in this transaction
    local_changes: TransactionChanges,
}

/// Changes made in a transaction
#[derive(Debug, Default)]
pub struct TransactionChanges {
    /// Created schemas
    created_schemas: std::collections::HashSet<String>,
    /// Dropped schemas
    dropped_schemas: std::collections::HashSet<String>,
    /// Created tables
    created_tables: std::collections::HashSet<(String, String)>, // (schema, table)
    /// Dropped tables
    dropped_tables: std::collections::HashSet<(String, String)>, // (schema, table)
    /// Created views
    created_views: std::collections::HashSet<(String, String)>, // (schema, view)
    /// Dropped views
    dropped_views: std::collections::HashSet<(String, String)>, // (schema, view)
    /// Created indexes
    created_indexes: std::collections::HashSet<(String, String)>, // (schema, index)
    /// Dropped indexes
    dropped_indexes: std::collections::HashSet<(String, String)>, // (schema, index)
}

impl TransactionCatalog {
    /// Create a new transaction catalog
    pub fn new(base_catalog: Arc<RwLock<Catalog>>, transaction: Arc<Transaction>) -> Self {
        Self {
            base_catalog,
            transaction,
            local_changes: TransactionChanges::default(),
        }
    }

    /// Get the base catalog
    pub fn get_base_catalog(&self) -> Arc<RwLock<Catalog>> {
        self.base_catalog.clone()
    }

    /// Get the transaction
    pub fn get_transaction(&self) -> Arc<Transaction> {
        self.transaction.clone()
    }

    /// Create a schema
    pub fn create_schema(&mut self, name: &str) -> PrismDBResult<()> {
        // Check if schema was dropped in this transaction
        if self.local_changes.dropped_schemas.contains(name) {
            return Err(PrismDBError::Catalog(format!(
                "Schema '{}' was dropped in this transaction",
                name
            )));
        }

        // Check if schema already exists
        if self.schema_exists(name)? {
            return Err(PrismDBError::Catalog(format!(
                "Schema '{}' already exists",
                name
            )));
        }

        // Record the creation
        self.local_changes.created_schemas.insert(name.to_string());
        Ok(())
    }

    /// Drop a schema
    pub fn drop_schema(&mut self, name: &str) -> PrismDBResult<()> {
        // Check if schema was created in this transaction
        if self.local_changes.created_schemas.contains(name) {
            self.local_changes.created_schemas.remove(name);
            return Ok(());
        }

        // Check if schema exists
        if !self.schema_exists(name)? {
            return Err(PrismDBError::Catalog(format!(
                "Schema '{}' does not exist",
                name
            )));
        }

        // Record the drop
        self.local_changes.dropped_schemas.insert(name.to_string());
        Ok(())
    }

    /// Check if a schema exists
    pub fn schema_exists(&self, name: &str) -> PrismDBResult<bool> {
        // Check if it was created in this transaction
        if self.local_changes.created_schemas.contains(name) {
            return Ok(true);
        }

        // Check if it was dropped in this transaction
        if self.local_changes.dropped_schemas.contains(name) {
            return Ok(false);
        }

        // Check base catalog
        let catalog = self.base_catalog.read().unwrap();
        Ok(catalog.list_schemas().contains(&name.to_string()))
    }

    /// Create a table
    pub fn create_table(
        &mut self,
        schema_name: &str,
        table_info: &crate::storage::TableInfo,
    ) -> PrismDBResult<()> {
        let table_key = (schema_name.to_string(), table_info.table_name.clone());

        // Check if table was dropped in this transaction
        if self.local_changes.dropped_tables.contains(&table_key) {
            return Err(PrismDBError::Catalog(format!(
                "Table '{}' was dropped in this transaction",
                table_info.table_name
            )));
        }

        // Check if table already exists
        if self.table_exists(schema_name, &table_info.table_name)? {
            return Err(PrismDBError::Catalog(format!(
                "Table '{}' already exists",
                table_info.table_name
            )));
        }

        // Record the creation
        self.local_changes.created_tables.insert(table_key);
        Ok(())
    }

    /// Drop a table
    pub fn drop_table(&mut self, schema_name: &str, table_name: &str) -> PrismDBResult<()> {
        let table_key = (schema_name.to_string(), table_name.to_string());

        // Check if table was created in this transaction
        if self.local_changes.created_tables.contains(&table_key) {
            self.local_changes.created_tables.remove(&table_key);
            return Ok(());
        }

        // Check if table exists
        if !self.table_exists(schema_name, table_name)? {
            return Err(PrismDBError::Catalog(format!(
                "Table '{}' does not exist",
                table_name
            )));
        }

        // Record the drop
        self.local_changes.dropped_tables.insert(table_key);
        Ok(())
    }

    /// Check if a table exists
    pub fn table_exists(&self, schema_name: &str, table_name: &str) -> PrismDBResult<bool> {
        let table_key = (schema_name.to_string(), table_name.to_string());

        // Check if it was created in this transaction
        if self.local_changes.created_tables.contains(&table_key) {
            return Ok(true);
        }

        // Check if it was dropped in this transaction
        if self.local_changes.dropped_tables.contains(&table_key) {
            return Ok(false);
        }

        // Check base catalog
        let catalog = self.base_catalog.read().unwrap();
        Ok(catalog.table_exists(schema_name, table_name))
    }

    /// Create a view
    pub fn create_view(
        &mut self,
        schema_name: &str,
        view_name: &str,
        _query: &str,
        _column_names: Vec<String>,
    ) -> PrismDBResult<()> {
        let view_key = (schema_name.to_string(), view_name.to_string());

        // Check if view was dropped in this transaction
        if self.local_changes.dropped_views.contains(&view_key) {
            return Err(PrismDBError::Catalog(format!(
                "View '{}' was dropped in this transaction",
                view_name
            )));
        }

        // Check if view already exists
        if self.view_exists(schema_name, view_name)? {
            return Err(PrismDBError::Catalog(format!(
                "View '{}' already exists",
                view_name
            )));
        }

        // Record the creation
        self.local_changes.created_views.insert(view_key);
        Ok(())
    }

    /// Drop a view
    pub fn drop_view(&mut self, schema_name: &str, view_name: &str) -> PrismDBResult<()> {
        let view_key = (schema_name.to_string(), view_name.to_string());

        // Check if view was created in this transaction
        if self.local_changes.created_views.contains(&view_key) {
            self.local_changes.created_views.remove(&view_key);
            return Ok(());
        }

        // Check if view exists
        if !self.view_exists(schema_name, view_name)? {
            return Err(PrismDBError::Catalog(format!(
                "View '{}' does not exist",
                view_name
            )));
        }

        // Record the drop
        self.local_changes.dropped_views.insert(view_key);
        Ok(())
    }

    /// Check if a view exists
    pub fn view_exists(&self, schema_name: &str, view_name: &str) -> PrismDBResult<bool> {
        let view_key = (schema_name.to_string(), view_name.to_string());

        // Check if it was created in this transaction
        if self.local_changes.created_views.contains(&view_key) {
            return Ok(true);
        }

        // Check if it was dropped in this transaction
        if self.local_changes.dropped_views.contains(&view_key) {
            return Ok(false);
        }

        // Check base catalog
        let catalog = self.base_catalog.read().unwrap();
        Ok(catalog.view_exists(schema_name, view_name))
    }

    /// Create an index
    pub fn create_index(
        &mut self,
        schema_name: &str,
        index_info: &crate::catalog::IndexInfo,
    ) -> PrismDBResult<()> {
        let index_key = (schema_name.to_string(), index_info.index_name.clone());

        // Check if index was dropped in this transaction
        if self.local_changes.dropped_indexes.contains(&index_key) {
            return Err(PrismDBError::Catalog(format!(
                "Index '{}' was dropped in this transaction",
                index_info.index_name
            )));
        }

        // Check if index already exists
        if self.index_exists(schema_name, &index_info.index_name)? {
            return Err(PrismDBError::Catalog(format!(
                "Index '{}' already exists",
                index_info.index_name
            )));
        }

        // Record the creation
        self.local_changes.created_indexes.insert(index_key);
        Ok(())
    }

    /// Drop an index
    pub fn drop_index(&mut self, schema_name: &str, index_name: &str) -> PrismDBResult<()> {
        let index_key = (schema_name.to_string(), index_name.to_string());

        // Check if index was created in this transaction
        if self.local_changes.created_indexes.contains(&index_key) {
            self.local_changes.created_indexes.remove(&index_key);
            return Ok(());
        }

        // Check if index exists
        if !self.index_exists(schema_name, index_name)? {
            return Err(PrismDBError::Catalog(format!(
                "Index '{}' does not exist",
                index_name
            )));
        }

        // Record the drop
        self.local_changes.dropped_indexes.insert(index_key);
        Ok(())
    }

    /// Check if an index exists
    pub fn index_exists(&self, schema_name: &str, index_name: &str) -> PrismDBResult<bool> {
        let index_key = (schema_name.to_string(), index_name.to_string());

        // Check if it was created in this transaction
        if self.local_changes.created_indexes.contains(&index_key) {
            return Ok(true);
        }

        // Check if it was dropped in this transaction
        if self.local_changes.dropped_indexes.contains(&index_key) {
            return Ok(false);
        }

        // Check base catalog
        let catalog = self.base_catalog.read().unwrap();
        Ok(catalog.index_exists(schema_name, index_name))
    }

    /// Commit the transaction changes to the base catalog
    pub fn commit(self) -> PrismDBResult<()> {
        let mut catalog = self.base_catalog.write().unwrap();

        // Apply schema changes
        for schema_name in &self.local_changes.created_schemas {
            catalog.create_schema(schema_name)?;
        }

        for schema_name in &self.local_changes.dropped_schemas {
            catalog.drop_schema(schema_name)?;
        }

        // Apply table changes
        for (_schema_name, _table_name) in &self.local_changes.created_tables {
            // In a real implementation, we would need to store the actual table info
            // For now, just record that the change was applied
        }

        for (schema_name, table_name) in &self.local_changes.dropped_tables {
            catalog.drop_table(schema_name, table_name)?;
        }

        // Apply view changes
        for (_schema_name, _view_name) in &self.local_changes.created_views {
            // In a real implementation, we would need to store the actual view info
        }

        for (schema_name, view_name) in &self.local_changes.dropped_views {
            catalog.drop_view(schema_name, view_name)?;
        }

        // Apply index changes
        for (_schema_name, _index_name) in &self.local_changes.created_indexes {
            // In a real implementation, we would need to store the actual index info
        }

        for (schema_name, index_name) in &self.local_changes.dropped_indexes {
            catalog.drop_index(schema_name, index_name)?;
        }

        Ok(())
    }

    /// Rollback the transaction changes
    pub fn rollback(self) -> PrismDBResult<()> {
        // Simply discard the local changes
        Ok(())
    }

    /// Get transaction statistics
    pub fn get_transaction_stats(&self) -> TransactionStats {
        TransactionStats {
            created_schemas: self.local_changes.created_schemas.len(),
            dropped_schemas: self.local_changes.dropped_schemas.len(),
            created_tables: self.local_changes.created_tables.len(),
            dropped_tables: self.local_changes.dropped_tables.len(),
            created_views: self.local_changes.created_views.len(),
            dropped_views: self.local_changes.dropped_views.len(),
            created_indexes: self.local_changes.created_indexes.len(),
            dropped_indexes: self.local_changes.dropped_indexes.len(),
        }
    }
}

/// Transaction statistics
#[derive(Debug, Clone)]
pub struct TransactionStats {
    pub created_schemas: usize,
    pub dropped_schemas: usize,
    pub created_tables: usize,
    pub dropped_tables: usize,
    pub created_views: usize,
    pub dropped_views: usize,
    pub created_indexes: usize,
    pub dropped_indexes: usize,
}
