//! Expression system for DuckDB
//!
//! This module provides the core expression framework used throughout DuckDB for
//! representing and evaluating expressions in queries.

pub mod aggregate;
pub mod binder;
pub mod datetime_functions;
pub mod executor;
pub mod expression;
pub mod function;
pub mod math_functions;
pub mod operator;
pub mod string_functions;
pub mod window;
pub mod window_functions;

pub use aggregate::*;
pub use binder::*;
pub use executor::*;
pub use expression::*;
pub use function::*;
pub use operator::*;
pub use window::*;

use crate::common::error::PrismDBResult;
use crate::types::{DataChunk, LogicalType, Value, Vector};
use std::sync::Arc;

/// Expression reference type
pub type ExpressionRef = Arc<dyn Expression>;

/// Expression trait that all expressions must implement
pub trait Expression: std::fmt::Debug + Send + Sync {
    /// Get the return type of this expression
    fn return_type(&self) -> &LogicalType;

    /// Evaluate this expression on a data chunk
    fn evaluate(&self, chunk: &DataChunk) -> PrismDBResult<Vector>;

    /// Evaluate this expression on a single row
    fn evaluate_row(&self, chunk: &DataChunk, row_idx: usize) -> PrismDBResult<Value>;

    /// Check if this expression is deterministic
    fn is_deterministic(&self) -> bool {
        true
    }

    /// Check if this expression is nullable
    fn is_nullable(&self) -> bool {
        true
    }
}

/// Expression utilities
pub mod utils {
    use super::*;

    /// Check if two expressions are equal
    pub fn expressions_equal(expr1: &dyn Expression, expr2: &dyn Expression) -> bool {
        // This is a simplified comparison - in a real implementation,
        // we'd need to compare the actual expression structure
        std::ptr::addr_eq(expr1 as *const _, expr2 as *const _)
    }
}
