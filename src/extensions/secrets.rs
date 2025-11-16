//! Secrets Management
//!
//! Handles CREATE SECRET statements for S3 and other credential management

use crate::common::error::{PrismDBError, PrismDBResult};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// A secret with its configuration
#[derive(Debug, Clone)]
pub struct Secret {
    pub name: String,
    pub secret_type: String,
    pub options: HashMap<String, String>,
}

/// Secrets manager for credential management
#[derive(Debug)]
pub struct SecretsManager {
    secrets: Arc<RwLock<HashMap<String, Secret>>>,
}

impl SecretsManager {
    /// Create a new secrets manager
    pub fn new() -> Self {
        Self {
            secrets: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create or replace a secret
    pub fn create_secret(&self, name: String, secret_type: String, options: HashMap<String, String>, or_replace: bool) -> PrismDBResult<()> {
        let mut secrets = self.secrets.write().unwrap();

        // Check if secret already exists
        if secrets.contains_key(&name) && !or_replace {
            return Err(PrismDBError::Execution(format!(
                "Secret '{}' already exists. Use CREATE OR REPLACE SECRET to replace it.",
                name
            )));
        }

        let secret = Secret {
            name: name.clone(),
            secret_type: secret_type.clone(),
            options: options.clone(),
        };

        secrets.insert(name.clone(), secret);

        println!("CREATE SECRET {} (TYPE {})", name, secret_type);

        // Print configuration (mask sensitive values)
        for (key, value) in options.iter() {
            let display_value = if key.to_lowercase().contains("secret")
                || key.to_lowercase().contains("password")
                || key.to_lowercase().contains("key") && key.to_lowercase() != "key_id" {
                "***"
            } else {
                value
            };
            println!("  {} = {}", key, display_value);
        }

        Ok(())
    }

    /// Get a secret by name
    pub fn get_secret(&self, name: &str) -> Option<Secret> {
        let secrets = self.secrets.read().unwrap();
        secrets.get(name).cloned()
    }

    /// List all secrets (names only for security)
    pub fn list_secrets(&self) -> Vec<String> {
        let secrets = self.secrets.read().unwrap();
        secrets.keys().cloned().collect()
    }

    /// Drop a secret
    pub fn drop_secret(&self, name: &str) -> PrismDBResult<()> {
        let mut secrets = self.secrets.write().unwrap();
        if secrets.remove(name).is_some() {
            println!("DROP SECRET {}", name);
            Ok(())
        } else {
            Err(PrismDBError::Execution(format!("Secret '{}' does not exist", name)))
        }
    }

    /// Get S3 configuration from secrets and settings
    pub fn get_s3_config(&self, config_manager: &super::config::ConfigManager) -> S3Config {
        let secrets = self.secrets.read().unwrap();

        // Try to find an S3 secret
        let s3_secret = secrets.values().find(|s| s.secret_type.to_lowercase() == "s3");

        S3Config {
            endpoint: s3_secret
                .and_then(|s| s.options.get("endpoint").cloned())
                .or_else(|| config_manager.get("s3_endpoint")),
            access_key_id: s3_secret
                .and_then(|s| s.options.get("key_id").cloned())
                .or_else(|| config_manager.get("s3_access_key_id")),
            secret_access_key: s3_secret
                .and_then(|s| s.options.get("secret").cloned())
                .or_else(|| config_manager.get("s3_secret_access_key")),
            region: s3_secret
                .and_then(|s| s.options.get("region").cloned())
                .or_else(|| config_manager.get("s3_region"))
                .unwrap_or_else(|| "us-east-1".to_string()),
            use_ssl: s3_secret
                .and_then(|s| s.options.get("use_ssl").and_then(|v| v.parse().ok()))
                .or_else(|| config_manager.get("s3_use_ssl").and_then(|v| v.parse().ok()))
                .unwrap_or(true),
            url_style: s3_secret
                .and_then(|s| s.options.get("url_style").cloned())
                .or_else(|| config_manager.get("s3_url_style"))
                .unwrap_or_else(|| "virtual".to_string()),
        }
    }
}

impl Default for SecretsManager {
    fn default() -> Self {
        Self::new()
    }
}

/// S3 configuration
#[derive(Debug, Clone)]
pub struct S3Config {
    pub endpoint: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub region: String,
    pub use_ssl: bool,
    pub url_style: String,
}
