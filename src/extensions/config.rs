//! Configuration Management
//!
//! Handles SET variable = value statements

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Configuration manager for database settings
#[derive(Debug)]
pub struct ConfigManager {
    settings: Arc<RwLock<HashMap<String, String>>>,
}

impl ConfigManager {
    /// Create a new configuration manager
    pub fn new() -> Self {
        Self {
            settings: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Set a configuration variable
    pub fn set(&self, key: &str, value: String) {
        let mut settings = self.settings.write().unwrap();
        settings.insert(key.to_lowercase(), value);
        println!("SET {} = '{}'", key, settings.get(&key.to_lowercase()).unwrap());
    }

    /// Get a configuration variable
    pub fn get(&self, key: &str) -> Option<String> {
        let settings = self.settings.read().unwrap();
        settings.get(&key.to_lowercase()).cloned()
    }

    /// List all configuration variables
    pub fn list_all(&self) -> Vec<(String, String)> {
        let settings = self.settings.read().unwrap();
        settings.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }

    /// Clear all settings
    pub fn clear(&self) {
        let mut settings = self.settings.write().unwrap();
        settings.clear();
    }
}

impl Default for ConfigManager {
    fn default() -> Self {
        Self::new()
    }
}
