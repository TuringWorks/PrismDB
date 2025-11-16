//! Function Management
//!
//! Provides function management for user-defined and built-in functions.

use crate::catalog::ObjectMetadata;
use crate::common::error::{PrismDBError, PrismDBResult};
use crate::types::LogicalType;
use std::collections::HashMap;
use std::sync::Arc;

/// Function information
#[derive(Debug, Clone)]
pub struct FunctionInfo {
    /// Function name
    pub function_name: String,
    /// Schema name
    pub schema_name: String,
    /// Argument types
    pub argument_types: Vec<LogicalType>,
    /// Return type
    pub return_type: LogicalType,
    /// Function type
    pub function_type: FunctionType,
    /// Function properties
    pub properties: FunctionProperties,
}

/// Function type
#[derive(Debug, Clone, PartialEq)]
pub enum FunctionType {
    Scalar,
    Aggregate,
    Window,
    Table,
}

/// Function properties
#[derive(Debug, Clone, Default)]
pub struct FunctionProperties {
    /// Function is deterministic
    pub deterministic: bool,
    /// Function can be parallelized
    pub parallel_safe: bool,
    /// Function description
    pub description: Option<String>,
    /// Function examples
    pub examples: Vec<String>,
    /// Custom properties
    pub custom_properties: HashMap<String, String>,
}

/// Database function
#[derive(Debug)]
pub struct Function {
    /// Function information
    pub info: FunctionInfo,
    /// Function metadata
    pub metadata: ObjectMetadata,
}

impl Function {
    /// Create a new function
    pub fn new(info: FunctionInfo) -> PrismDBResult<Self> {
        Ok(Self {
            info,
            metadata: ObjectMetadata::new(),
        })
    }

    /// Get function name
    pub fn get_name(&self) -> &str {
        &self.info.function_name
    }

    /// Get schema name
    pub fn get_schema_name(&self) -> &str {
        &self.info.schema_name
    }

    /// Get argument types
    pub fn get_argument_types(&self) -> &[LogicalType] {
        &self.info.argument_types
    }

    /// Get return type
    pub fn get_return_type(&self) -> &LogicalType {
        &self.info.return_type
    }

    /// Get function type
    pub fn get_function_type(&self) -> &FunctionType {
        &self.info.function_type
    }

    /// Get argument count
    pub fn argument_count(&self) -> usize {
        self.info.argument_types.len()
    }

    /// Check if function is variadic
    pub fn is_variadic(&self) -> bool {
        self.info
            .properties
            .custom_properties
            .get("variadic")
            .map(|v| v == "true")
            .unwrap_or(false)
    }

    /// Check if function is deterministic
    pub fn is_deterministic(&self) -> bool {
        self.info.properties.deterministic
    }

    /// Check if function is parallel safe
    pub fn is_parallel_safe(&self) -> bool {
        self.info.properties.parallel_safe
    }

    /// Validate function definition
    pub fn validate(&self) -> PrismDBResult<()> {
        if self.info.function_name.is_empty() {
            return Err(PrismDBError::Catalog(
                "Function name cannot be empty".to_string(),
            ));
        }

        if self.info.schema_name.is_empty() {
            return Err(PrismDBError::Catalog(
                "Schema name cannot be empty".to_string(),
            ));
        }

        // Validate argument types
        for _arg_type in &self.info.argument_types {
            // All logical types are valid for function arguments
        }

        // Validate return type
        // All logical types are valid for function return types

        Ok(())
    }

    /// Get function signature
    pub fn get_signature(&self) -> String {
        let args: Vec<String> = self
            .info
            .argument_types
            .iter()
            .map(|t| format!("{:?}", t))
            .collect();
        format!(
            "{}({}) -> {:?}",
            self.info.function_name,
            args.join(", "),
            self.info.return_type
        )
    }

    /// Get function information
    pub fn get_info(&self) -> &FunctionInfo {
        &self.info
    }
}

/// Function registry for managing functions
#[derive(Debug)]
pub struct FunctionRegistry {
    functions: HashMap<String, Vec<Arc<Function>>>,
}

impl FunctionRegistry {
    /// Create a new function registry
    pub fn new() -> Self {
        Self {
            functions: HashMap::new(),
        }
    }

    /// Register a function
    pub fn register_function(&mut self, function: Arc<Function>) -> PrismDBResult<()> {
        let name = function.get_name().to_string();
        self.functions
            .entry(name)
            .or_insert_with(Vec::new)
            .push(function);
        Ok(())
    }

    /// Unregister a function
    pub fn unregister_function(
        &mut self,
        name: &str,
        arg_types: &[LogicalType],
    ) -> PrismDBResult<()> {
        if let Some(functions) = self.functions.get_mut(name) {
            functions.retain(|f| f.get_argument_types() != arg_types);

            if functions.is_empty() {
                self.functions.remove(name);
            }
        }
        Ok(())
    }

    /// Lookup a function by name and argument types
    pub fn lookup_function(&self, name: &str, arg_types: &[LogicalType]) -> Option<Arc<Function>> {
        if let Some(functions) = self.functions.get(name) {
            // Exact match first
            for function in functions {
                if function.get_argument_types() == arg_types {
                    return Some(function.clone());
                }
            }

            // Type coercion match
            for function in functions {
                if self.can_coerce_types(arg_types, function.get_argument_types()) {
                    return Some(function.clone());
                }
            }
        }
        None
    }

    /// Check if types can be coerced
    fn can_coerce_types(&self, from: &[LogicalType], to: &[LogicalType]) -> bool {
        if from.len() != to.len() {
            return false;
        }

        for (from_type, to_type) in from.iter().zip(to.iter()) {
            if !self.can_coerce_type(from_type, to_type) {
                return false;
            }
        }

        true
    }

    /// Check if a type can be coerced to another type
    fn can_coerce_type(&self, from: &LogicalType, to: &LogicalType) -> bool {
        match (from, to) {
            (LogicalType::Null, _) => true,
            (LogicalType::TinyInt, LogicalType::SmallInt) => true,
            (LogicalType::TinyInt, LogicalType::Integer) => true,
            (LogicalType::TinyInt, LogicalType::BigInt) => true,
            (LogicalType::SmallInt, LogicalType::Integer) => true,
            (LogicalType::SmallInt, LogicalType::BigInt) => true,
            (LogicalType::Integer, LogicalType::BigInt) => true,
            (LogicalType::Float, LogicalType::Double) => true,
            (LogicalType::Varchar, LogicalType::Date) => true, // With parsing
            (LogicalType::Varchar, LogicalType::Time) => true, // With parsing
            (LogicalType::Varchar, LogicalType::Timestamp) => true, // With parsing
            (a, b) => a == b,
        }
    }

    /// List all functions
    pub fn list_functions(&self) -> Vec<String> {
        self.functions.keys().cloned().collect()
    }

    /// List functions by name
    pub fn list_functions_by_name(&self, name: &str) -> Vec<Arc<Function>> {
        self.functions.get(name).cloned().unwrap_or_default()
    }

    /// Get function count
    pub fn function_count(&self) -> usize {
        self.functions.values().map(|funcs| funcs.len()).sum()
    }
}

impl Default for FunctionRegistry {
    fn default() -> Self {
        Self::new()
    }
}
