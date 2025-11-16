//! Catalog System for DuckDB
//!
//! This module provides the catalog system that manages database metadata
//! including schemas, tables, views, indexes, and other database objects.

pub mod function;
pub mod index;
pub mod schema;
pub mod table;
pub mod transaction;
pub mod view;

pub use function::*;
pub use index::*;
pub use schema::*;
pub use table::*;
pub use transaction::*;
pub use view::*;

use crate::common::error::{PrismDBError, PrismDBResult};
use crate::storage::TableInfo;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Main catalog that manages all database metadata
#[derive(Debug)]
pub struct Catalog {
    /// Schemas in the catalog
    schemas: HashMap<String, Arc<RwLock<Schema>>>,
    /// Default schema name
    default_schema: String,
    /// Catalog metadata
    metadata: CatalogMetadata,
}

impl Catalog {
    /// Create a new catalog
    pub fn new() -> Self {
        let mut catalog = Self {
            schemas: HashMap::new(),
            default_schema: "main".to_string(),
            metadata: CatalogMetadata::new(),
        };

        // Create default schema
        catalog.create_schema("main").unwrap();
        catalog
    }

    /// Create a new schema
    pub fn create_schema(&mut self, name: &str) -> PrismDBResult<()> {
        if self.schemas.contains_key(name) {
            return Err(PrismDBError::Catalog(format!(
                "Schema '{}' already exists",
                name
            )));
        }

        let schema = Schema::new(name.to_string());
        self.schemas
            .insert(name.to_string(), Arc::new(RwLock::new(schema)));
        Ok(())
    }

    /// Drop a schema
    pub fn drop_schema(&mut self, name: &str) -> PrismDBResult<()> {
        if name == self.default_schema {
            return Err(PrismDBError::Catalog(
                "Cannot drop default schema".to_string(),
            ));
        }

        if !self.schemas.contains_key(name) {
            return Err(PrismDBError::Catalog(format!(
                "Schema '{}' does not exist",
                name
            )));
        }

        self.schemas.remove(name);
        Ok(())
    }

    /// Get a schema
    pub fn get_schema(&self, name: &str) -> PrismDBResult<Arc<RwLock<Schema>>> {
        self.schemas
            .get(name)
            .cloned()
            .ok_or_else(|| PrismDBError::Catalog(format!("Schema '{}' does not exist", name)))
    }

    /// Get the default schema
    pub fn get_default_schema(&self) -> Arc<RwLock<Schema>> {
        self.schemas.get(&self.default_schema).unwrap().clone()
    }

    /// Create a table
    pub fn create_table(&self, table_info: &TableInfo) -> PrismDBResult<()> {
        let schema = self.get_schema(&table_info.schema_name)?;
        let result = schema.write().unwrap().create_table(table_info);
        result
    }

    /// Drop a table
    pub fn drop_table(&self, schema_name: &str, table_name: &str) -> PrismDBResult<()> {
        let schema = self.get_schema(schema_name)?;
        let result = schema.write().unwrap().drop_table(table_name);
        result
    }

    /// Get a table
    pub fn get_table(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> PrismDBResult<Arc<RwLock<Table>>> {
        let schema = self.get_schema(schema_name)?;
        let result = schema.read().unwrap().get_table(table_name);
        result
    }

    /// Create a view
    pub fn create_view(
        &self,
        schema_name: &str,
        view_name: &str,
        query: &str,
        column_names: Vec<String>,
    ) -> PrismDBResult<()> {
        let schema = self.get_schema(schema_name)?;
        let result = schema
            .write()
            .unwrap()
            .create_view(view_name, query, column_names);
        result
    }

    /// Drop a view
    pub fn drop_view(&self, schema_name: &str, view_name: &str) -> PrismDBResult<()> {
        let schema = self.get_schema(schema_name)?;
        let result = schema.write().unwrap().drop_view(view_name);
        result
    }

    /// Get a view
    pub fn get_view(&self, schema_name: &str, view_name: &str) -> PrismDBResult<Arc<RwLock<View>>> {
        let schema = self.get_schema(schema_name)?;
        let result = schema.read().unwrap().get_view(view_name);
        result
    }

    /// Create an index
    pub fn create_index(&self, index_info: &IndexInfo) -> PrismDBResult<()> {
        let schema = self.get_schema(&index_info.schema_name)?;
        let result = schema.write().unwrap().create_index(index_info);
        result
    }

    /// Drop an index
    pub fn drop_index(&self, schema_name: &str, index_name: &str) -> PrismDBResult<()> {
        let schema = self.get_schema(schema_name)?;
        let result = schema.write().unwrap().drop_index(index_name);
        result
    }

    /// Get an index
    pub fn get_index(
        &self,
        schema_name: &str,
        index_name: &str,
    ) -> PrismDBResult<Arc<RwLock<Index>>> {
        let schema = self.get_schema(schema_name)?;
        let result = schema.read().unwrap().get_index(index_name);
        result
    }

    /// List all schemas
    pub fn list_schemas(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }

    /// List all tables in a schema
    pub fn list_tables(&self, schema_name: &str) -> PrismDBResult<Vec<String>> {
        let schema = self.get_schema(schema_name)?;
        let result = schema.read().unwrap().list_tables();
        Ok(result)
    }

    /// List all views in a schema
    pub fn list_views(&self, schema_name: &str) -> PrismDBResult<Vec<String>> {
        let schema = self.get_schema(schema_name)?;
        let result = schema.read().unwrap().list_views();
        Ok(result)
    }

    /// List all indexes in a schema
    pub fn list_indexes(&self, schema_name: &str) -> PrismDBResult<Vec<String>> {
        let schema = self.get_schema(schema_name)?;
        let result = schema.read().unwrap().list_indexes();
        Ok(result)
    }

    /// Get catalog metadata
    pub fn get_metadata(&self) -> &CatalogMetadata {
        &self.metadata
    }

    /// Check if a table exists
    pub fn table_exists(&self, schema_name: &str, table_name: &str) -> bool {
        if let Ok(schema) = self.get_schema(schema_name) {
            schema.read().unwrap().table_exists(table_name)
        } else {
            false
        }
    }

    /// Check if a view exists
    pub fn view_exists(&self, schema_name: &str, view_name: &str) -> bool {
        if let Ok(schema) = self.get_schema(schema_name) {
            schema.read().unwrap().view_exists(view_name)
        } else {
            false
        }
    }

    /// Check if an index exists
    pub fn index_exists(&self, schema_name: &str, index_name: &str) -> bool {
        if let Ok(schema) = self.get_schema(schema_name) {
            schema.read().unwrap().index_exists(index_name)
        } else {
            false
        }
    }
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}

/// Catalog metadata
#[derive(Debug, Clone)]
pub struct CatalogMetadata {
    /// Catalog version
    pub version: u64,
    /// Creation time
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Last modified time
    pub modified_at: chrono::DateTime<chrono::Utc>,
}

impl CatalogMetadata {
    pub fn new() -> Self {
        let now = chrono::Utc::now();
        Self {
            version: 1,
            created_at: now,
            modified_at: now,
        }
    }

    pub fn increment_version(&mut self) {
        self.version += 1;
        self.modified_at = chrono::Utc::now();
    }
}

/// Database object type
#[derive(Debug, Clone, PartialEq)]
pub enum DatabaseObjectType {
    Schema,
    Table,
    View,
    Index,
    Function,
    Trigger,
    Sequence,
}

/// Database object identifier
#[derive(Debug, Clone, PartialEq)]
pub struct DatabaseObjectId {
    pub schema_name: String,
    pub object_name: String,
    pub object_type: DatabaseObjectType,
}

impl DatabaseObjectId {
    pub fn new(schema_name: String, object_name: String, object_type: DatabaseObjectType) -> Self {
        Self {
            schema_name,
            object_name,
            object_type,
        }
    }

    pub fn table(schema_name: &str, table_name: &str) -> Self {
        Self::new(
            schema_name.to_string(),
            table_name.to_string(),
            DatabaseObjectType::Table,
        )
    }

    pub fn view(schema_name: &str, view_name: &str) -> Self {
        Self::new(
            schema_name.to_string(),
            view_name.to_string(),
            DatabaseObjectType::View,
        )
    }

    pub fn index(schema_name: &str, index_name: &str) -> Self {
        Self::new(
            schema_name.to_string(),
            index_name.to_string(),
            DatabaseObjectType::Index,
        )
    }

    pub fn schema(schema_name: &str) -> Self {
        Self::new(
            schema_name.to_string(),
            schema_name.to_string(),
            DatabaseObjectType::Schema,
        )
    }
}

/// Catalog entry for database objects
#[derive(Debug, Clone)]
pub struct CatalogEntry {
    /// Object identifier
    pub id: DatabaseObjectId,
    /// Object metadata
    pub metadata: ObjectMetadata,
    /// Dependencies
    pub dependencies: Vec<DatabaseObjectId>,
    /// Dependents
    pub dependents: Vec<DatabaseObjectId>,
}

/// Object metadata
#[derive(Debug, Clone)]
pub struct ObjectMetadata {
    /// Creation time
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Last modified time
    pub modified_at: chrono::DateTime<chrono::Utc>,
    /// Creator
    pub creator: Option<String>,
    /// Comments
    pub comment: Option<String>,
    /// Additional properties
    pub properties: HashMap<String, String>,
}

impl ObjectMetadata {
    pub fn new() -> Self {
        let now = chrono::Utc::now();
        Self {
            created_at: now,
            modified_at: now,
            creator: None,
            comment: None,
            properties: HashMap::new(),
        }
    }

    pub fn touch(&mut self) {
        self.modified_at = chrono::Utc::now();
    }
}

impl Default for ObjectMetadata {
    fn default() -> Self {
        Self::new()
    }
}
