//! Expression Execution
//!
//! This module handles execution of expressions, evaluating them
//! against data chunks and producing result vectors.

use crate::common::PrismDBResult;
use crate::expression::ExpressionRef;
use crate::types::DataChunk;

/// Expression executor
pub struct ExpressionExecutor {
    expressions: Vec<ExpressionRef>,
}

impl ExpressionExecutor {
    pub fn new() -> Self {
        Self {
            expressions: Vec::new(),
        }
    }

    pub fn with_expressions(expressions: Vec<ExpressionRef>) -> Self {
        Self { expressions }
    }

    /// Add an expression to execute
    pub fn add_expression(&mut self, expression: ExpressionRef) {
        self.expressions.push(expression);
    }

    /// Execute all expressions against a data chunk
    pub fn execute(&self, chunk: &DataChunk) -> PrismDBResult<Vec<crate::types::Vector>> {
        let mut results = Vec::with_capacity(self.expressions.len());

        for expression in &self.expressions {
            let result = self.execute_expression(expression, chunk)?;
            results.push(result);
        }

        Ok(results)
    }

    /// Execute a single expression
    pub fn execute_expression(
        &self,
        expression: &ExpressionRef,
        chunk: &DataChunk,
    ) -> PrismDBResult<crate::types::Vector> {
        // Use the Expression trait's evaluate method directly
        expression.evaluate(chunk)
    }
}

/// Vectorized expression executor for performance
pub struct VectorizedExecutor {
    expressions: Vec<ExpressionRef>,
}

impl VectorizedExecutor {
    pub fn new(expressions: Vec<ExpressionRef>) -> Self {
        Self { expressions }
    }

    /// Execute expressions using vectorized operations
    pub fn execute_vectorized(&self, chunk: &DataChunk) -> PrismDBResult<Vec<crate::types::Vector>> {
        let mut results = Vec::with_capacity(self.expressions.len());

        for expression in &self.expressions {
            let result = self.execute_vectorized_expression(expression, chunk)?;
            results.push(result);
        }

        Ok(results)
    }

    /// Execute a single expression using vectorized operations
    fn execute_vectorized_expression(
        &self,
        expression: &ExpressionRef,
        chunk: &DataChunk,
    ) -> PrismDBResult<crate::types::Vector> {
        // This would implement SIMD and other vectorized optimizations
        // For now, fall back to regular execution
        let executor = ExpressionExecutor::new();
        executor.execute_expression(expression, chunk)
    }
}
