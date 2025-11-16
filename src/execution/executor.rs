//! Query Executor
//!
//! High-level executor that coordinates query execution.

use crate::common::error::{PrismDBError, PrismDBResult};
use crate::execution::{ExecutionContext, ExecutionEngine, ExecutionStats};
use crate::parser::{SqlParser, Statement};
use crate::planner::{QueryOptimizer, QueryPlanner};
use crate::types::DataChunk;
use std::time::Instant;

/// Query executor
pub struct QueryExecutor {
    execution_engine: ExecutionEngine,
    planner: QueryPlanner,
    optimizer: QueryOptimizer,
    parser: SqlParser,
}

impl QueryExecutor {
    /// Create a new query executor
    pub fn new(context: ExecutionContext) -> Self {
        let execution_engine = ExecutionEngine::new(context.clone());
        let planner = QueryPlanner::new();
        let optimizer = QueryOptimizer::new()
            .with_context(context.catalog.clone(), context.transaction_manager.clone());
        let parser = SqlParser::new();

        Self {
            execution_engine,
            planner,
            optimizer,
            parser,
        }
    }

    /// Execute a SQL query and return results
    pub fn execute_sql(&mut self, sql: &str) -> PrismDBResult<QueryResult> {
        let start_time = std::time::Instant::now();

        let mut planner = QueryPlanner::new();
        let mut parser = crate::parser::SqlParser::new();
        let parsed = parser.parse(sql)?;
        let logical_plan = planner.plan_statement(&parsed)?;
        // Use the pre-configured optimizer with catalog/transaction context
        let physical_plan = self.optimizer.optimize(logical_plan)?;

        let mut stream = self.execution_engine.execute(physical_plan)?;
        let mut chunks = Vec::new();
        let mut rows_processed = 0;

        while let Some(chunk_result) = stream.next() {
            let chunk = chunk_result?;
            rows_processed += chunk.len();
            chunks.push(chunk);
        }

        let execution_time = start_time.elapsed();

        Ok(QueryResult {
            chunks,
            rows_processed,
            execution_time_ms: execution_time.as_millis() as u64,
            stats: ExecutionStats {
                rows_processed,
                execution_time_ms: execution_time.as_millis() as u64,
                memory_used_bytes: 0,  // TODO: Track memory usage
                operators_executed: 0, // TODO: Track operator count
            },
        })
    }

    /// Execute a SQL query and collect all results into a single result set
    pub fn execute_sql_collect(&mut self, sql: &str) -> PrismDBResult<CollectedResult> {
        let query_result = self.execute_sql(sql)?;
        query_result.collect()
    }

    /// Execute multiple SQL statements
    pub fn execute_sql_multiple(&mut self, sql: &str) -> PrismDBResult<Vec<QueryResult>> {
        let statements = self.parser.parse_multiple(sql)?;
        let mut results = Vec::new();

        for statement in statements {
            let result = self.execute_statement(statement)?;
            results.push(result);
        }

        Ok(results)
    }

    /// Execute a single statement
    fn execute_statement(&mut self, statement: Statement) -> PrismDBResult<QueryResult> {
        let start_time = Instant::now();

        // Plan the query
        let logical_plan = self.planner.plan_statement(&statement)?;

        // Optimize the plan
        let physical_plan = self.optimizer.optimize(logical_plan)?;

        // Execute the plan
        let mut stream = self.execution_engine.execute(physical_plan)?;
        let mut chunks = Vec::new();
        let mut rows_processed = 0;

        while let Some(chunk_result) = stream.next() {
            let chunk = chunk_result?;
            rows_processed += chunk.len();
            chunks.push(chunk);
        }

        let execution_time = start_time.elapsed();

        Ok(QueryResult {
            chunks,
            rows_processed,
            execution_time_ms: execution_time.as_millis() as u64,
            stats: ExecutionStats {
                rows_processed,
                execution_time_ms: execution_time.as_millis() as u64,
                memory_used_bytes: 0,
                operators_executed: 0,
            },
        })
    }
}

/// Query result containing data chunks and statistics
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub chunks: Vec<DataChunk>,
    pub rows_processed: usize,
    pub execution_time_ms: u64,
    pub stats: ExecutionStats,
}

impl QueryResult {
    /// Get the total number of rows
    pub fn row_count(&self) -> usize {
        self.rows_processed
    }

    /// Get the number of chunks
    pub fn chunk_count(&self) -> usize {
        self.chunks.len()
    }

    /// Get the column count (assuming all chunks have the same structure)
    pub fn column_count(&self) -> usize {
        self.chunks.first().map(|c| c.column_count()).unwrap_or(0)
    }

    /// Collect all results into a single result set
    pub fn collect(self) -> PrismDBResult<CollectedResult> {
        let mut all_rows = Vec::new();

        for chunk in self.chunks {
            for row_idx in 0..chunk.len() {
                let mut row = Vec::new();
                for col_idx in 0..chunk.column_count() {
                    let vector = chunk.get_vector(col_idx).ok_or_else(|| {
                        PrismDBError::InvalidValue(format!("Column {} not found", col_idx))
                    })?;
                    let value = vector.get_value(row_idx)?;
                    row.push(value);
                }
                all_rows.push(row);
            }
        }

        Ok(CollectedResult {
            rows: all_rows,
            stats: self.stats,
        })
    }

    /// Get the first row (if any)
    pub fn first_row(&self) -> Option<Vec<crate::types::Value>> {
        self.chunks.first().and_then(|chunk| {
            if chunk.len() > 0 {
                let mut row = Vec::new();
                for col_idx in 0..chunk.column_count() {
                    let vector = chunk.get_vector(col_idx)?;
                    let value = vector.get_value(0).ok()?;
                    row.push(value);
                }
                Some(row)
            } else {
                None
            }
        })
    }

    /// Get the first value (if any)
    pub fn first_value(&self) -> Option<crate::Value> {
        self.chunks.first().and_then(|chunk| {
            if chunk.len() > 0 && chunk.column_count() > 0 {
                let vector = chunk.get_vector(0)?;
                vector.get_value(0).ok()
            } else {
                None
            }
        })
    }
}

/// Collected result with all rows in memory
#[derive(Debug, Clone)]
pub struct CollectedResult {
    pub rows: Vec<Vec<crate::Value>>,
    pub stats: ExecutionStats,
}

impl CollectedResult {
    /// Get the number of rows
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Get the number of columns
    pub fn column_count(&self) -> usize {
        self.rows.first().map(|row| row.len()).unwrap_or(0)
    }

    /// Get a specific row
    pub fn get_row(&self, index: usize) -> Option<&[crate::Value]> {
        self.rows.get(index).map(|row| row.as_slice())
    }

    /// Get a specific value
    pub fn get_value(&self, row: usize, col: usize) -> Option<&crate::Value> {
        self.rows.get(row).and_then(|r| r.get(col))
    }

    /// Convert to a table-like string representation
    pub fn to_table_string(&self) -> String {
        if self.rows.is_empty() {
            return "(no rows)".to_string();
        }

        let mut result = String::new();

        // Simple table representation
        for row in &self.rows {
            let row_str: Vec<String> = row.iter().map(|v| v.to_string()).collect();
            result.push_str(&row_str.join(" | "));
            result.push('\n');
        }

        result
    }
}

/// Query execution options
#[derive(Debug, Clone)]
pub struct QueryOptions {
    pub enable_optimization: bool,
    pub parallel_execution: bool,
    pub memory_limit: Option<usize>,
    pub timeout_ms: Option<u64>,
}

impl Default for QueryOptions {
    fn default() -> Self {
        Self {
            enable_optimization: true,
            parallel_execution: false,
            memory_limit: None,
            timeout_ms: None,
        }
    }
}
