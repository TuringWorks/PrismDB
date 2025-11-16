//! Utility functions for PIVOT/UNPIVOT execution
//!
//! This module provides helper functions for extracting information from
//! ExpressionRef types used in PIVOT and UNPIVOT operations.

use crate::expression::expression::ExpressionRef;

/// Attempt to extract aggregate function name from an ExpressionRef
///
/// This function uses debug format parsing to extract aggregate function information.
/// Returns None if the expression is not a recognized aggregate function.
pub fn extract_aggregate_name(expr: &ExpressionRef) -> Option<String> {
    let debug_str = format!("{:?}", expr);

    // Check if this is an AggregateExpression
    if debug_str.contains("AggregateExpression") {
        // Extract function name from debug output
        // Format: AggregateExpression { function_name: "SUM", ... }
        if let Some(start) = debug_str.find("function_name: \"") {
            let after_start = &debug_str[start + 16..];
            if let Some(end) = after_start.find("\"") {
                return Some(after_start[..end].to_lowercase());
            }
        }
    }

    // Check if this is a FunctionExpression with is_aggregate: true
    if debug_str.contains("FunctionExpression") && debug_str.contains("is_aggregate: true") {
        // Extract function name from debug output
        if let Some(start) = debug_str.find("function_name: \"") {
            let after_start = &debug_str[start + 16..];
            if let Some(end) = after_start.find("\"") {
                return Some(after_start[..end].to_lowercase());
            }
        }
    }

    None
}

/// Extract constant value from an expression by evaluating it
///
/// This creates a minimal DataChunk and evaluates the expression to get
/// its constant value. Returns the string representation of the value.
pub fn extract_constant_value(expr: &ExpressionRef, context: &crate::execution::ExecutionContext) -> Option<String> {
    use crate::types::DataChunk;

    // Create a single-row chunk for evaluation
    let eval_chunk = DataChunk::with_rows(1);

    match expr.evaluate(&eval_chunk, context) {
        Ok(vector) if vector.len() > 0 => {
            match vector.get_value(0) {
                Ok(val) => Some(val.to_string()),
                Err(_) => None,
            }
        }
        _ => None,
    }
}

/// Extract column name from an expression
///
/// Attempts to extract a meaningful column name from various expression types.
pub fn extract_column_name(expr: &ExpressionRef) -> String {
    // Try to use debug format and extract useful information
    let debug_str = format!("{:?}", expr);

    // Look for ColumnRef pattern
    if debug_str.contains("ColumnRef") {
        if let Some(start) = debug_str.find("name: \"") {
            let after_start = &debug_str[start + 7..];
            if let Some(end) = after_start.find("\"") {
                return after_start[..end].to_string();
            }
        }
    }

    // Fallback to first 50 characters of debug output
    if debug_str.len() > 50 {
        format!("{}...", &debug_str[..50])
    } else {
        debug_str
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::error::PrismDBResult;
    use crate::expression::expression::{ConstantExpression, ExpressionRef};
    use crate::types::Value;
    use std::sync::{Arc, RwLock};
    use crate::catalog::Catalog;
    use crate::TransactionManager;
    use crate::execution::ExecutionContext;

    #[test]
    fn test_extract_constant_value() -> PrismDBResult<()> {
        let const_expr = Arc::new(ConstantExpression::new(
            Value::Varchar("test".to_string()),
        )?) as ExpressionRef;

        // Create minimal context for testing
        let txn_mgr = Arc::new(TransactionManager::new());
        let catalog = Arc::new(RwLock::new(Catalog::new()));
        let context = ExecutionContext::new(txn_mgr, catalog);

        let value = extract_constant_value(&const_expr, &context);
        assert_eq!(value, Some("'test'".to_string()));
        Ok(())
    }

    #[test]
    fn test_extract_column_name() -> PrismDBResult<()> {
        let const_expr = Arc::new(ConstantExpression::new(
            Value::Integer(42),
        )?) as ExpressionRef;

        let name = extract_column_name(&const_expr);
        // Should return some debug representation
        assert!(!name.is_empty());
        Ok(())
    }
}
