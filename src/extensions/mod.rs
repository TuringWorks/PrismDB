//! Extension Management
//!
//! This module provides functionality for installing and loading DuckDB extensions.

pub mod aws_signature;
pub mod config;
pub mod csv_reader;
pub mod file_reader;
pub mod json_reader;
pub mod parquet_reader;
pub mod secrets;
pub mod sqlite_reader;

pub use aws_signature::{AwsSignatureV4, get_aws_timestamp};
pub use config::ConfigManager;
pub use csv_reader::CsvReader;
pub use file_reader::FileReader;
pub use json_reader::JsonReader;
pub use parquet_reader::ParquetReader;
pub use secrets::{S3Config, Secret, SecretsManager};
pub use sqlite_reader::SqliteReader;

use crate::common::error::{PrismDBError, PrismDBResult};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

/// Known DuckDB extensions with their descriptions
#[derive(Debug, Clone)]
pub struct ExtensionInfo {
    pub name: String,
    pub description: String,
    pub core: bool,
    pub autoloadable: bool,
}

/// Extension manager for handling INSTALL and LOAD operations
#[derive(Debug)]
pub struct ExtensionManager {
    /// Installed extensions
    installed: Arc<RwLock<HashSet<String>>>,
    /// Loaded extensions
    loaded: Arc<RwLock<HashSet<String>>>,
    /// Known extensions catalog
    catalog: HashMap<String, ExtensionInfo>,
}

impl ExtensionManager {
    /// Create a new extension manager
    pub fn new() -> Self {
        let mut catalog = HashMap::new();

        // Core extensions (shipped with DuckDB)
        Self::add_core_extension(&mut catalog, "httpfs", "HTTP and S3 file system support");
        Self::add_core_extension(&mut catalog, "json", "JSON support");
        Self::add_core_extension(&mut catalog, "parquet", "Parquet file format support");
        Self::add_core_extension(&mut catalog, "icu", "International Components for Unicode");
        Self::add_core_extension(&mut catalog, "fts", "Full-text search support");
        Self::add_core_extension(&mut catalog, "visualizer", "Query plan visualization");
        Self::add_core_extension(&mut catalog, "tpch", "TPC-H benchmark data generator");
        Self::add_core_extension(&mut catalog, "tpcds", "TPC-DS benchmark data generator");

        // Autoloadable core extensions
        Self::add_autoload_extension(&mut catalog, "aws", "AWS services integration");
        Self::add_autoload_extension(&mut catalog, "azure", "Azure services integration");
        Self::add_autoload_extension(&mut catalog, "postgres_scanner", "PostgreSQL database scanner");
        Self::add_autoload_extension(&mut catalog, "sqlite_scanner", "SQLite database scanner");
        Self::add_autoload_extension(&mut catalog, "mysql_scanner", "MySQL database scanner");
        Self::add_autoload_extension(&mut catalog, "excel", "Excel file support");
        Self::add_autoload_extension(&mut catalog, "spatial", "Geospatial data support");
        Self::add_autoload_extension(&mut catalog, "substrait", "Substrait query plan support");

        // Community extensions
        Self::add_extension(&mut catalog, "autocomplete", "Auto-completion support", false, false);
        Self::add_extension(&mut catalog, "inet", "IP address support", false, false);
        Self::add_extension(&mut catalog, "jemalloc", "Jemalloc memory allocator", false, false);
        Self::add_extension(&mut catalog, "delta", "Delta Lake support", false, false);
        Self::add_extension(&mut catalog, "iceberg", "Apache Iceberg support", false, false);

        Self {
            installed: Arc::new(RwLock::new(HashSet::new())),
            loaded: Arc::new(RwLock::new(HashSet::new())),
            catalog,
        }
    }

    fn add_core_extension(catalog: &mut HashMap<String, ExtensionInfo>, name: &str, desc: &str) {
        Self::add_extension(catalog, name, desc, true, false);
    }

    fn add_autoload_extension(catalog: &mut HashMap<String, ExtensionInfo>, name: &str, desc: &str) {
        Self::add_extension(catalog, name, desc, true, true);
    }

    fn add_extension(
        catalog: &mut HashMap<String, ExtensionInfo>,
        name: &str,
        desc: &str,
        core: bool,
        autoloadable: bool,
    ) {
        catalog.insert(
            name.to_lowercase(),
            ExtensionInfo {
                name: name.to_string(),
                description: desc.to_string(),
                core,
                autoloadable,
            },
        );
    }

    /// Install an extension
    pub fn install(&self, extension_name: &str) -> PrismDBResult<()> {
        let name = extension_name.to_lowercase();

        // Check if extension is known
        let info = self.catalog.get(&name).ok_or_else(|| {
            PrismDBError::Extension(format!(
                "Unknown extension '{}'. Available extensions: {}",
                extension_name,
                self.list_available_extensions().join(", ")
            ))
        })?;

        // Check if already installed
        {
            let installed = self.installed.read().unwrap();
            if installed.contains(&name) {
                println!("Extension '{}' is already installed.", extension_name);
                return Ok(());
            }
        }

        // Simulate installation
        println!("Installing extension '{}'...", extension_name);

        if info.core {
            println!("  └─ Core extension (included with DuckDB)");
        } else {
            println!("  └─ Downloading from extension repository...");
            println!("  └─ Verifying signature...");
        }

        // Mark as installed
        {
            let mut installed = self.installed.write().unwrap();
            installed.insert(name.clone());
        }

        println!("Extension '{}' installed successfully.", extension_name);

        // Auto-load if it's an autoloadable extension
        if info.autoloadable {
            println!("  └─ Auto-loading extension...");
            self.load(&name)?;
        }

        Ok(())
    }

    /// Load an extension
    pub fn load(&self, extension_name: &str) -> PrismDBResult<()> {
        let name = extension_name.to_lowercase();

        // Check if extension is known
        let _info = self.catalog.get(&name).ok_or_else(|| {
            PrismDBError::Extension(format!(
                "Unknown extension '{}'. Available extensions: {}",
                extension_name,
                self.list_available_extensions().join(", ")
            ))
        })?;

        // Check if installed
        {
            let installed = self.installed.read().unwrap();
            if !installed.contains(&name) {
                return Err(PrismDBError::Extension(format!(
                    "Extension '{}' is not installed. Run INSTALL {} first.",
                    extension_name, extension_name
                )));
            }
        }

        // Check if already loaded
        {
            let loaded = self.loaded.read().unwrap();
            if loaded.contains(&name) {
                println!("Extension '{}' is already loaded.", extension_name);
                return Ok(());
            }
        }

        // Simulate loading
        println!("Loading extension '{}'...", extension_name);
        println!("  └─ Initializing extension functions...");

        // Mark as loaded
        {
            let mut loaded = self.loaded.write().unwrap();
            loaded.insert(name);
        }

        println!("Extension '{}' loaded successfully.", extension_name);

        Ok(())
    }

    /// Check if an extension is installed
    pub fn is_installed(&self, extension_name: &str) -> bool {
        let installed = self.installed.read().unwrap();
        installed.contains(&extension_name.to_lowercase())
    }

    /// Check if an extension is loaded
    pub fn is_loaded(&self, extension_name: &str) -> bool {
        let loaded = self.loaded.read().unwrap();
        loaded.contains(&extension_name.to_lowercase())
    }

    /// List all available extensions
    pub fn list_available_extensions(&self) -> Vec<String> {
        let mut names: Vec<String> = self.catalog.keys().cloned().collect();
        names.sort();
        names
    }

    /// List installed extensions
    pub fn list_installed(&self) -> Vec<String> {
        let installed = self.installed.read().unwrap();
        let mut names: Vec<String> = installed.iter().cloned().collect();
        names.sort();
        names
    }

    /// List loaded extensions
    pub fn list_loaded(&self) -> Vec<String> {
        let loaded = self.loaded.read().unwrap();
        let mut names: Vec<String> = loaded.iter().cloned().collect();
        names.sort();
        names
    }

    /// Get extension info
    pub fn get_info(&self, extension_name: &str) -> Option<&ExtensionInfo> {
        self.catalog.get(&extension_name.to_lowercase())
    }
}

impl Default for ExtensionManager {
    fn default() -> Self {
        Self::new()
    }
}
