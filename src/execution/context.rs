//! Execution Context
//!
//! Provides context and resources for query execution.

use crate::catalog::Catalog;
use crate::common::error::{PrismDBError, PrismDBResult};
use crate::execution::parallel::ParallelContext;
use crate::storage::{Transaction, TransactionManager};
use crate::types::LogicalType;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use uuid::Uuid;

/// Execution context for query execution
#[derive(Debug, Clone)]
pub struct ExecutionContext {
    /// Transaction manager
    pub transaction_manager: Arc<TransactionManager>,
    /// Catalog reference for accessing tables
    pub catalog: Arc<RwLock<Catalog>>,
    /// Current transaction ID
    pub transaction_id: Option<Uuid>,
    /// Current transaction
    pub transaction: Option<Arc<Transaction>>,
    /// Execution parameters
    pub parameters: HashMap<String, ContextValue>,
    /// Execution mode
    pub mode: ExecutionMode,
    /// Memory limit in bytes
    pub memory_limit: Option<usize>,
    /// Thread limit
    pub thread_limit: Option<usize>,
    /// Parallel execution context
    pub parallel_context: ParallelContext,
}

/// Execution mode
#[derive(Debug, Clone, PartialEq)]
pub enum ExecutionMode {
    Standard,
    Pipeline,
    Parallel,
}

impl ExecutionContext {
    /// Create a new execution context
    pub fn new(
        transaction_manager: Arc<TransactionManager>,
        catalog: Arc<RwLock<Catalog>>,
    ) -> Self {
        let parallel_context = ParallelContext::from_system();

        Self {
            transaction_manager,
            catalog,
            transaction_id: None,
            transaction: None,
            parameters: HashMap::new(),
            mode: ExecutionMode::Parallel, // Enable parallel mode by default
            memory_limit: None,
            thread_limit: None,
            parallel_context,
        }
    }

    /// Begin a transaction
    pub fn begin_transaction(&mut self) -> PrismDBResult<()> {
        if self.transaction.is_some() {
            return Err(PrismDBError::Transaction(
                "Transaction already active".to_string(),
            ));
        }

        let transaction_id = self
            .transaction_manager
            .begin_transaction(crate::storage::transaction::IsolationLevel::ReadCommitted)?;
        self.transaction_id = Some(transaction_id);
        // TODO: Create actual transaction object
        // For now, we'll work with the ID only
        Ok(())
    }

    /// Commit the current transaction
    pub fn commit_transaction(&mut self) -> PrismDBResult<()> {
        if let Some(transaction_id) = self.transaction_id.take() {
            self.transaction_manager
                .commit_transaction(transaction_id)?;
        }
        self.transaction = None;
        Ok(())
    }

    /// Rollback the current transaction
    pub fn rollback_transaction(&mut self) -> PrismDBResult<()> {
        if let Some(transaction_id) = self.transaction_id.take() {
            self.transaction_manager.abort_transaction(transaction_id)?;
        }
        self.transaction = None;
        Ok(())
    }

    /// Get current transaction ID
    pub fn get_transaction_id(&self) -> Option<Uuid> {
        self.transaction_id
    }

    /// Get the current transaction
    pub fn get_transaction(&self) -> PrismDBResult<Arc<Transaction>> {
        // TODO: Return actual transaction object
        Err(PrismDBError::Transaction(
            "Transaction not implemented".to_string(),
        ))
    }

    /// Set a parameter
    pub fn set_parameter(&mut self, name: String, value: ContextValue) {
        self.parameters.insert(name, value);
    }

    /// Get a parameter
    pub fn get_parameter(&self, name: &str) -> Option<&ContextValue> {
        self.parameters.get(name)
    }

    /// Set execution mode
    pub fn set_mode(&mut self, mode: ExecutionMode) {
        self.mode = mode;
    }

    /// Set memory limit
    pub fn set_memory_limit(&mut self, limit: Option<usize>) {
        self.memory_limit = limit;
    }

    /// Set thread limit
    pub fn set_thread_limit(&mut self, limit: Option<usize>) {
        self.thread_limit = limit;
    }
}

/// Value type for parameters
#[derive(Debug, Clone)]
pub enum ContextValue {
    Null,
    Boolean(bool),
    TinyInt(i8),
    SmallInt(i16),
    Integer(i32),
    BigInt(i64),
    HugeInt(i128),
    Float(f32),
    Double(f64),
    String(String),
    Date(chrono::NaiveDate),
    Time(chrono::NaiveTime),
    Timestamp(chrono::NaiveDateTime),
    Interval(Interval),
    Decimal(rust_decimal::Decimal),
    List(Vec<ContextValue>),
    Struct(Vec<(String, ContextValue)>),
}

/// Interval type
#[derive(Debug, Clone)]
pub struct Interval {
    pub months: i32,
    pub days: i32,
    pub micros: i64,
}

impl Interval {
    pub fn new(months: i32, days: i32, micros: i64) -> Self {
        Self {
            months,
            days,
            micros,
        }
    }
}

impl ContextValue {
    /// Get the logical type of this value
    pub fn get_type(&self) -> LogicalType {
        match self {
            ContextValue::Null => LogicalType::Null,
            ContextValue::Boolean(_) => LogicalType::Boolean,
            ContextValue::TinyInt(_) => LogicalType::TinyInt,
            ContextValue::SmallInt(_) => LogicalType::SmallInt,
            ContextValue::Integer(_) => LogicalType::Integer,
            ContextValue::BigInt(_) => LogicalType::BigInt,
            ContextValue::HugeInt(_) => LogicalType::HugeInt,
            ContextValue::Float(_) => LogicalType::Float,
            ContextValue::Double(_) => LogicalType::Double,
            ContextValue::String(_) => LogicalType::Varchar,
            ContextValue::Date(_) => LogicalType::Date,
            ContextValue::Time(_) => LogicalType::Time,
            ContextValue::Timestamp(_) => LogicalType::Timestamp,
            ContextValue::Interval(_) => LogicalType::Interval,
            ContextValue::Decimal(_) => LogicalType::Decimal {
                precision: 18,
                scale: 6,
            },
            ContextValue::List(values) => {
                if values.is_empty() {
                    LogicalType::List(Box::new(LogicalType::Null))
                } else {
                    LogicalType::List(Box::new(values[0].get_type()))
                }
            }
            ContextValue::Struct(fields) => {
                let field_types: Vec<(String, LogicalType)> = fields
                    .iter()
                    .map(|(name, value)| (name.clone(), value.get_type()))
                    .collect();
                LogicalType::Struct(field_types)
            }
        }
    }

    /// Check if this value is null
    pub fn is_null(&self) -> bool {
        matches!(self, ContextValue::Null)
    }

    /// Convert to string representation
    pub fn to_string(&self) -> String {
        match self {
            ContextValue::Null => "NULL".to_string(),
            ContextValue::Boolean(b) => b.to_string(),
            ContextValue::TinyInt(i) => i.to_string(),
            ContextValue::SmallInt(i) => i.to_string(),
            ContextValue::Integer(i) => i.to_string(),
            ContextValue::BigInt(i) => i.to_string(),
            ContextValue::HugeInt(i) => i.to_string(),
            ContextValue::Float(f) => f.to_string(),
            ContextValue::Double(d) => d.to_string(),
            ContextValue::String(s) => format!("'{}'", s),
            ContextValue::Date(d) => d.to_string(),
            ContextValue::Time(t) => t.to_string(),
            ContextValue::Timestamp(ts) => ts.to_string(),
            ContextValue::Interval(interval) => {
                format!(
                    "INTERVAL '{} months {} days {} micros'",
                    interval.months, interval.days, interval.micros
                )
            }
            ContextValue::Decimal(d) => d.to_string(),
            ContextValue::List(values) => {
                let items: Vec<String> = values.iter().map(|v| v.to_string()).collect();
                format!("[{}]", items.join(", "))
            }
            ContextValue::Struct(fields) => {
                let items: Vec<String> = fields
                    .iter()
                    .map(|(name, value)| format!("{}: {}", name, value.to_string()))
                    .collect();
                format!("{{{}}}", items.join(", "))
            }
        }
    }
}
