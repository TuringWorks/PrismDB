//! Execution Operators
//!
//! Implements various execution operators for different physical plan nodes.

use crate::common::error::{PrismDBError, PrismDBResult};
use crate::execution::context::ExecutionContext;
use crate::planner::{
    DataChunkStream, ExecutionOperator, PhysicalAggregate, PhysicalColumn, PhysicalCreateTable,
    PhysicalDelete, PhysicalDropTable, PhysicalFilter, PhysicalHashJoin, PhysicalInsert,
    PhysicalLimit, PhysicalPlan, PhysicalProjection, PhysicalQualify, PhysicalSort, PhysicalTableScan,
    PhysicalUnion, PhysicalUpdate,
};
use crate::types::{DataChunk, Value};

/// Serialize a Value to a string for hash key (without Display formatting which adds quotes)
fn value_to_key_string(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Boolean(b) => b.to_string(),
        Value::TinyInt(i) => i.to_string(),
        Value::SmallInt(i) => i.to_string(),
        Value::Integer(i) => i.to_string(),
        Value::BigInt(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Double(f) => f.to_string(),
        Value::Varchar(s) => s.clone(), // Don't add quotes!
        Value::Char(s) => s.clone(),
        Value::Decimal { value, scale, .. } => {
            let divisor = 10_i128.pow(*scale as u32);
            let integer_part = value / divisor;
            let fractional_part = (value % divisor).abs();
            format!("{}.{:0width$}", integer_part, fractional_part, width = *scale as usize)
        }
        _ => format!("{:?}", value), // Fallback for other types
    }
}

/// Simple iterator-based data chunk stream
pub struct SimpleDataChunkStream {
    chunks: Vec<DataChunk>,
    index: usize,
}

impl SimpleDataChunkStream {
    pub fn new(chunks: Vec<DataChunk>) -> Self {
        Self { chunks, index: 0 }
    }

    pub fn empty() -> Self {
        Self {
            chunks: Vec::new(),
            index: 0,
        }
    }
}

impl Iterator for SimpleDataChunkStream {
    type Item = PrismDBResult<DataChunk>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.chunks.len() {
            let chunk = std::mem::replace(&mut self.chunks[self.index], DataChunk::new());
            self.index += 1;
            Some(Ok(chunk))
        } else {
            None
        }
    }
}

impl DataChunkStream for SimpleDataChunkStream {}

impl ExecutionOperator for SimpleDataChunkStream {
    fn execute(&self) -> PrismDBResult<Box<dyn DataChunkStream>> {
        Ok(Box::new(Self {
            chunks: self.chunks.clone(),
            index: 0,
        }))
    }

    fn schema(&self) -> Vec<PhysicalColumn> {
        vec![]
    }
}

/// Table scan operator (PrismDB-faithful implementation)
/// Reads data from the storage layer
pub struct TableScanOperator {
    scan: PhysicalTableScan,
    context: ExecutionContext,
}

impl TableScanOperator {
    pub fn new(scan: PhysicalTableScan, context: ExecutionContext) -> Self {
        Self { scan, context }
    }

    /// Apply a pushed-down filter to a chunk using SelectionVector (PrismDB-faithful)
    fn apply_filter_to_chunk(
        &self,
        chunk: DataChunk,
        filter_expr: &crate::expression::expression::ExpressionRef,
    ) -> PrismDBResult<DataChunk> {
        use crate::common::error::PrismDBError;
        use crate::types::{SelectionVector, Value};

        if chunk.len() == 0 {
            return Ok(chunk);
        }

        // Evaluate the filter predicate on this chunk
        let result_vector = filter_expr.evaluate(&chunk, &self.context)?;

        // Build SelectionVector with indices of rows that pass the filter
        let mut selection = SelectionVector::new(chunk.len());

        for i in 0..chunk.len() {
            let value = result_vector.get_value(i)?;

            // Check if this row passes the filter
            let passes = match value {
                Value::Boolean(b) => b,
                Value::Null => false, // NULL in filter evaluates to false
                _ => {
                    return Err(PrismDBError::Execution(format!(
                        "Filter predicate must return boolean, got {:?}",
                        value
                    )));
                }
            };

            if passes {
                selection.append(i);
            }
        }

        // Optimization: If all rows pass, return original chunk unchanged
        if selection.count() == chunk.len() {
            return Ok(chunk);
        }

        // Optimization: If no rows pass, return empty chunk
        if selection.is_empty() {
            return Ok(DataChunk::new());
        }

        // Apply selection vector to create filtered chunk
        chunk.slice(&selection)
    }

    /// Static version of filter application for use in closures (parallel execution)
    fn apply_filter_inline(
        chunk: DataChunk,
        filter_expr: &crate::expression::expression::ExpressionRef,
        context: &ExecutionContext,
    ) -> PrismDBResult<DataChunk> {
        use crate::common::error::PrismDBError;
        use crate::types::{SelectionVector, Value};

        if chunk.len() == 0 {
            return Ok(chunk);
        }

        let result_vector = filter_expr.evaluate(&chunk, context)?;
        let mut selection = SelectionVector::new(chunk.len());

        for i in 0..chunk.len() {
            let value = result_vector.get_value(i)?;
            let passes = match value {
                Value::Boolean(b) => b,
                Value::Null => false,
                _ => {
                    return Err(PrismDBError::Execution(format!(
                        "Filter predicate must return boolean, got {:?}",
                        value
                    )))
                }
            };
            if passes {
                selection.append(i);
            }
        }

        if selection.count() == chunk.len() {
            return Ok(chunk);
        }
        if selection.is_empty() {
            return Ok(DataChunk::new());
        }
        chunk.slice(&selection)
    }
}

impl ExecutionOperator for TableScanOperator {
    fn execute(&self) -> PrismDBResult<Box<dyn DataChunkStream>> {
        use crate::common::error::PrismDBError;
        use crate::execution::context::ExecutionMode;
        use crate::execution::parallel::{parallel_table_scan, MORSEL_SIZE};

        // Get the table from the catalog
        let catalog = &self.context.catalog;
        let catalog_guard = catalog.read().unwrap();

        // Get the default schema (typically "main")
        let schema = catalog_guard.get_default_schema();
        let schema_guard = schema.read().unwrap();

        // Get the table
        let table_arc = schema_guard.get_table(&self.scan.table_name).map_err(|_| {
            PrismDBError::Catalog(format!("Table '{}' not found", self.scan.table_name))
        })?;

        let table = table_arc.read().unwrap();

        // Get the table's data storage
        let table_data_arc = table.get_data();
        let table_data = table_data_arc.read().unwrap();

        let total_rows = table_data.row_count();
        let max_rows = self.scan.limit.unwrap_or(usize::MAX);

        // PrismDB uses 2048 as the standard VECTOR_SIZE for chunk processing
        const CHUNK_SIZE: usize = 2048;

        // Decide whether to use parallel execution
        let use_parallel = self.context.mode == ExecutionMode::Parallel
            && total_rows >= MORSEL_SIZE
            && self.context.parallel_context.parallel_enabled;

        if use_parallel {
            // PARALLEL EXECUTION PATH (PrismDB morsel-driven parallelism)
            let filters = self.scan.filters.clone();
            let table_data_clone = table_data_arc.clone();
            let context = self.context.clone();

            let chunks = parallel_table_scan(
                std::cmp::min(total_rows, max_rows),
                &self.context.parallel_context,
                |morsel| {
                    let table_data = table_data_clone.read().unwrap();
                    let mut chunk = table_data.create_chunk(morsel.offset, morsel.count)?;

                    // Apply filters within parallel worker (inline implementation)
                    if !filters.is_empty() {
                        for filter_expr in &filters {
                            chunk = Self::apply_filter_inline(chunk, filter_expr, &context)?;
                        }
                    }

                    Ok(chunk)
                },
            )?;

            Ok(Box::new(SimpleDataChunkStream::new(chunks)))
        } else {
            // SINGLE-THREADED EXECUTION PATH (for small tables or when parallel is disabled)
            let mut chunks = Vec::new();
            let mut offset = 0;
            let mut rows_collected = 0;

            while offset < total_rows && rows_collected < max_rows {
                // Don't read more than needed if we have a limit
                let chunk_size = std::cmp::min(
                    std::cmp::min(CHUNK_SIZE, total_rows - offset),
                    max_rows - rows_collected,
                );

                // Use TableData's create_chunk method which efficiently reads from column storage
                let mut chunk = table_data.create_chunk(offset, chunk_size)?;

                // Apply pushed-down filters (PrismDB-faithful filter pushdown optimization)
                if !self.scan.filters.is_empty() {
                    for filter_expr in &self.scan.filters {
                        chunk = self.apply_filter_to_chunk(chunk, filter_expr)?;
                    }
                }

                if chunk.len() > 0 {
                    rows_collected += chunk.len();
                    chunks.push(chunk);
                }

                offset += chunk_size;

                // Early exit if we've collected enough rows (limit optimization)
                if rows_collected >= max_rows {
                    break;
                }
            }

            Ok(Box::new(SimpleDataChunkStream::new(chunks)))
        }
    }

    fn schema(&self) -> Vec<PhysicalColumn> {
        self.scan.schema.clone()
    }
}

/// Filter operator (PrismDB-faithful implementation)
/// Uses SelectionVector for zero-copy filtering
pub struct FilterOperator {
    filter: PhysicalFilter,
    context: ExecutionContext,
}

impl FilterOperator {
    pub fn new(filter: PhysicalFilter, context: ExecutionContext) -> Self {
        Self { filter, context }
    }

    /// Apply filter to a single chunk using SelectionVector
    /// This is the core PrismDB pattern for efficient filtering
    fn apply_filter(&self, chunk: DataChunk) -> PrismDBResult<DataChunk> {
        use crate::common::error::PrismDBError;
        use crate::types::{SelectionVector, Value};

        if chunk.len() == 0 {
            return Ok(chunk);
        }

        // Evaluate the filter predicate on this chunk
        // Returns a boolean vector indicating which rows pass
        let result_vector = self.filter.predicate.evaluate(&chunk, &self.context)?;

        // Build SelectionVector with indices of rows that pass the filter
        let mut selection = SelectionVector::new(chunk.len());

        for i in 0..chunk.len() {
            let value = result_vector.get_value(i)?;

            // Check if this row passes the filter
            let passes = match value {
                Value::Boolean(b) => b,
                Value::Null => false, // NULL in filter evaluates to false
                _ => {
                    return Err(PrismDBError::Execution(format!(
                        "Filter predicate must return boolean, got {:?}",
                        value
                    )));
                }
            };

            if passes {
                selection.append(i);
            }
        }

        // Optimization: If all rows pass, return original chunk unchanged
        if selection.count() == chunk.len() {
            return Ok(chunk);
        }

        // Optimization: If no rows pass, return empty chunk
        if selection.is_empty() {
            return Ok(DataChunk::new());
        }

        // Apply selection vector to create filtered chunk
        // This is zero-copy - we're just marking which rows to include
        chunk.slice(&selection)
    }
}

impl ExecutionOperator for FilterOperator {
    fn execute(&self) -> PrismDBResult<Box<dyn DataChunkStream>> {
        use crate::execution::ExecutionEngine;

        // Execute the input operator to get source data
        let mut engine = ExecutionEngine::new(self.context.clone());
        let input_plan = (*self.filter.input).clone();
        let mut input_stream = engine.execute(input_plan)?;

        // Filter each chunk as it comes from input
        let mut filtered_chunks = Vec::new();

        while let Some(chunk_result) = input_stream.next() {
            let chunk = chunk_result?;

            // Apply filter to this chunk
            let filtered_chunk = self.apply_filter(chunk)?;

            // Only include non-empty chunks
            if filtered_chunk.len() > 0 {
                filtered_chunks.push(filtered_chunk);
            }
        }

        Ok(Box::new(SimpleDataChunkStream::new(filtered_chunks)))
    }

    fn schema(&self) -> Vec<PhysicalColumn> {
        self.filter.input.schema()
    }
}

/// QUALIFY operator (PrismDB extension - filters on window function results)
/// Applied after window functions are computed but before ORDER BY/LIMIT
/// Very similar to Filter operator, but semantically operates after window computation
pub struct QualifyOperator {
    qualify: PhysicalQualify,
    context: ExecutionContext,
}

impl QualifyOperator {
    pub fn new(qualify: PhysicalQualify, context: ExecutionContext) -> Self {
        Self { qualify, context }
    }

    /// Apply QUALIFY filter to a single chunk using SelectionVector
    /// Same filtering logic as FilterOperator, but operates on window function results
    fn apply_qualify(&self, chunk: DataChunk) -> PrismDBResult<DataChunk> {
        use crate::common::error::PrismDBError;
        use crate::types::{SelectionVector, Value};

        if chunk.len() == 0 {
            return Ok(chunk);
        }

        // Evaluate the QUALIFY predicate on this chunk
        // At this point, window functions must already be computed
        let result_vector = self.qualify.predicate.evaluate(&chunk, &self.context)?;

        // Build SelectionVector with indices of rows that pass the filter
        let mut selection = SelectionVector::new(chunk.len());

        for i in 0..chunk.len() {
            let value = result_vector.get_value(i)?;

            // Check if this row passes the QUALIFY filter
            let passes = match value {
                Value::Boolean(b) => b,
                Value::Null => false, // NULL in QUALIFY evaluates to false
                _ => {
                    return Err(PrismDBError::Execution(format!(
                        "QUALIFY predicate must return boolean, got {:?}",
                        value
                    )));
                }
            };

            if passes {
                selection.append(i);
            }
        }

        // Optimization: If all rows pass, return original chunk unchanged
        if selection.count() == chunk.len() {
            return Ok(chunk);
        }

        // Optimization: If no rows pass, return empty chunk
        if selection.is_empty() {
            return Ok(DataChunk::new());
        }

        // Apply selection vector to create filtered chunk
        chunk.slice(&selection)
    }
}

impl ExecutionOperator for QualifyOperator {
    fn execute(&self) -> PrismDBResult<Box<dyn DataChunkStream>> {
        use crate::execution::ExecutionEngine;

        // Execute the input operator to get source data (with window functions computed)
        let mut engine = ExecutionEngine::new(self.context.clone());
        let input_plan = (*self.qualify.input).clone();
        let mut input_stream = engine.execute(input_plan)?;

        // Filter each chunk as it comes from input
        let mut filtered_chunks = Vec::new();

        while let Some(chunk_result) = input_stream.next() {
            let chunk = chunk_result?;

            // Apply QUALIFY filter to this chunk
            let filtered_chunk = self.apply_qualify(chunk)?;

            // Only include non-empty chunks
            if filtered_chunk.len() > 0 {
                filtered_chunks.push(filtered_chunk);
            }
        }

        Ok(Box::new(SimpleDataChunkStream::new(filtered_chunks)))
    }

    fn schema(&self) -> Vec<PhysicalColumn> {
        self.qualify.input.schema()
    }
}

/// Projection operator (PrismDB-faithful implementation)
/// Projects columns from the input stream
pub struct ProjectionOperator {
    projection: PhysicalProjection,
    context: ExecutionContext,
}

impl ProjectionOperator {
    pub fn new(projection: PhysicalProjection, context: ExecutionContext) -> Self {
        Self {
            projection,
            context,
        }
    }
}

impl ExecutionOperator for ProjectionOperator {
    fn execute(&self) -> PrismDBResult<Box<dyn DataChunkStream>> {
        use crate::execution::ExecutionEngine;

        // Execute the input operator to get source data
        let mut engine = ExecutionEngine::new(self.context.clone());
        let input_plan = (*self.projection.input).clone();

        let mut input_stream = engine.execute(input_plan)?;

        // Project each chunk as it comes from input
        let mut projected_chunks = Vec::new();

        while let Some(chunk_result) = input_stream.next() {
            let chunk = chunk_result?;

            if chunk.len() == 0 {
                continue;
            }

            // Create a result chunk with projected columns
            let mut result_chunk = DataChunk::with_rows(chunk.len());

            for (i, expression) in self.projection.expressions.iter().enumerate() {
                // Evaluate the expression on the input chunk
                let result_vector = expression.evaluate(&chunk, &self.context)?;

                result_chunk.set_vector(i, result_vector)?;
            }

            if result_chunk.len() > 0 {
                projected_chunks.push(result_chunk);
            }
        }

        Ok(Box::new(SimpleDataChunkStream::new(projected_chunks)))
    }

    fn schema(&self) -> Vec<PhysicalColumn> {
        self.projection.schema.clone()
    }
}

/// Limit operator
pub struct LimitOperator {
    limit: PhysicalLimit,
    context: ExecutionContext,
}

impl LimitOperator {
    pub fn new(limit: PhysicalLimit, context: ExecutionContext) -> Self {
        Self {
            limit,
            context,
        }
    }
}

impl ExecutionOperator for LimitOperator {
    fn execute(&self) -> PrismDBResult<Box<dyn DataChunkStream>> {
        use crate::execution::ExecutionEngine;

        // Execute the input plan
        let mut engine = ExecutionEngine::new(self.context.clone());
        let input_plan = (*self.limit.input).clone();
        let mut input_stream = engine.execute(input_plan)?;

        // Collect rows up to the limit
        let limit = self.limit.limit;
        let offset = self.limit.offset;
        let mut all_rows: Vec<Vec<Value>> = Vec::new();
        let mut schema: Vec<PhysicalColumn> = Vec::new();
        let mut total_rows = 0;

        while let Some(chunk_result) = input_stream.next() {
            let chunk = chunk_result?;

            // Extract schema from first chunk
            if schema.is_empty() {
                for col_idx in 0..chunk.column_count() {
                    if let Some(vector) = chunk.get_vector(col_idx) {
                        schema.push(PhysicalColumn {
                            name: format!("col_{}", col_idx),
                            data_type: vector.get_type().clone(),
                        });
                    }
                }
            }

            // Process rows from this chunk
            for row_idx in 0..chunk.len() {
                // Skip rows before offset
                if total_rows < offset {
                    total_rows += 1;
                    continue;
                }

                // Stop if we've reached the limit
                if all_rows.len() >= limit {
                    break;
                }

                // Extract row
                let mut row = Vec::new();
                for col_idx in 0..chunk.column_count() {
                    if let Some(vector) = chunk.get_vector(col_idx) {
                        row.push(vector.get_value(row_idx)?);
                    }
                }
                all_rows.push(row);
                total_rows += 1;
            }

            // Break early if we've reached the limit
            if all_rows.len() >= limit {
                break;
            }
        }

        // Create result chunk
        if all_rows.is_empty() {
            return Ok(Box::new(SimpleDataChunkStream::empty()));
        }

        // Convert rows back to DataChunk
        let num_rows = all_rows.len();
        let num_columns = schema.len();
        let mut result_chunk = DataChunk::with_rows(num_rows);

        for col_idx in 0..num_columns {
            let column_values: Vec<Value> =
                all_rows.iter().map(|row| row[col_idx].clone()).collect();

            let vector = crate::types::Vector::from_values(&column_values)?;
            result_chunk.set_vector(col_idx, vector)?;
        }

        Ok(Box::new(SimpleDataChunkStream::new(vec![result_chunk])))
    }

    fn schema(&self) -> Vec<PhysicalColumn> {
        self.limit.input.schema()
    }
}

/// Sort operator (in-memory sorting)
pub struct SortOperator {
    sort: PhysicalSort,
    context: ExecutionContext,
}

impl SortOperator {
    pub fn new(sort: PhysicalSort, context: ExecutionContext) -> Self {
        Self { sort, context }
    }
}

impl ExecutionOperator for SortOperator {
    fn execute(&self) -> PrismDBResult<Box<dyn DataChunkStream>> {
        use crate::common::error::PrismDBError;
        use crate::execution::ExecutionEngine;

        // Execute the input plan and collect all rows
        let mut engine = ExecutionEngine::new(self.context.clone());
        let input_plan = (*self.sort.input).clone();
        let mut input_stream = engine.execute(input_plan)?;

        // Collect all rows from input
        let mut all_rows: Vec<Vec<Value>> = Vec::new();
        let mut schema: Vec<PhysicalColumn> = Vec::new();
        let mut num_columns = 0;

        while let Some(chunk_result) = input_stream.next() {
            let chunk = chunk_result?;
            num_columns = chunk.column_count();

            // Extract schema from first chunk
            if schema.is_empty() {
                for col_idx in 0..num_columns {
                    let vector = chunk.get_vector(col_idx).ok_or_else(|| {
                        PrismDBError::InvalidValue(format!("Column {} not found", col_idx))
                    })?;
                    schema.push(PhysicalColumn {
                        name: format!("col_{}", col_idx),
                        data_type: vector.get_type().clone(),
                    });
                }
            }

            // Collect all rows from this chunk
            for row_idx in 0..chunk.len() {
                let mut row_values = Vec::new();
                for col_idx in 0..num_columns {
                    let vector = chunk.get_vector(col_idx).ok_or_else(|| {
                        PrismDBError::InvalidValue(format!("Column {} not found", col_idx))
                    })?;
                    let value = vector.get_value(row_idx)?;
                    row_values.push(value);
                }
                all_rows.push(row_values);
            }
        }

        if all_rows.is_empty() {
            return Ok(Box::new(SimpleDataChunkStream::empty()));
        }

        // Sort the rows
        all_rows.sort_by(|a, b| self.compare_rows(a, b).unwrap_or(std::cmp::Ordering::Equal));

        // Convert sorted rows back to DataChunk
        let num_rows = all_rows.len();
        let mut result_chunk = DataChunk::with_rows(num_rows);

        for col_idx in 0..num_columns {
            let column_values: Vec<Value> =
                all_rows.iter().map(|row| row[col_idx].clone()).collect();

            let vector = crate::types::Vector::from_values(&column_values)?;
            result_chunk.set_vector(col_idx, vector)?;
        }

        Ok(Box::new(SimpleDataChunkStream::new(vec![result_chunk])))
    }

    fn schema(&self) -> Vec<PhysicalColumn> {
        // For now, return empty - will be populated during execution
        vec![]
    }
}

impl SortOperator {
    /// Compare two rows based on sort expressions
    fn compare_rows(&self, a: &[Value], b: &[Value]) -> PrismDBResult<std::cmp::Ordering> {
        use std::cmp::Ordering;

        for sort_expr in &self.sort.expressions {
            // Extract the actual column index from the sort expression
            // If it's a ColumnRefExpression, use its column_index
            // Otherwise, fall back to evaluating the expression (not yet implemented)

            use crate::expression::expression::ColumnRefExpression;

            // Downcast to ColumnRefExpression to get the column index
            let column_idx = if let Some(col_ref) = sort_expr.expression.as_any().downcast_ref::<ColumnRefExpression>() {
                col_ref.column_index()
            } else {
                // For non-column expressions, we'd need to evaluate them
                // For now, skip this sort expression
                continue;
            };

            if column_idx >= a.len() || column_idx >= b.len() {
                continue;
            }

            let val_a = &a[column_idx];
            let val_b = &b[column_idx];

            // Handle NULL ordering
            let cmp_result = match (val_a, val_b) {
                (Value::Null, Value::Null) => Ordering::Equal,
                (Value::Null, _) => {
                    if sort_expr.nulls_first {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    }
                }
                (_, Value::Null) => {
                    if sort_expr.nulls_first {
                        Ordering::Greater
                    } else {
                        Ordering::Less
                    }
                }
                _ => {
                    // Compare values
                    let cmp_i32 = AggregateState::compare_values(val_a, val_b)?;
                    match cmp_i32 {
                        -1 => Ordering::Less,
                        0 => Ordering::Equal,
                        1 => Ordering::Greater,
                        _ => Ordering::Equal,
                    }
                }
            };

            // Apply ascending/descending
            let final_cmp = if sort_expr.ascending {
                cmp_result
            } else {
                cmp_result.reverse()
            };

            if final_cmp != Ordering::Equal {
                return Ok(final_cmp);
            }
        }

        Ok(Ordering::Equal)
    }
}

/// Aggregate operator (hash-based aggregation)
pub struct AggregateOperator {
    aggregate: PhysicalAggregate,
    context: ExecutionContext,
}

impl AggregateOperator {
    pub fn new(aggregate: PhysicalAggregate, context: ExecutionContext) -> Self {
        Self { aggregate, context }
    }

    /// Parse a string value back to the correct Value type based on schema
    fn parse_value_from_string(&self, s: &str, logical_type: &crate::types::LogicalType) -> PrismDBResult<Value> {
        use crate::types::LogicalType;

        // Handle NULL special case
        if s == "NULL" {
            return Ok(Value::Null);
        }

        match logical_type {
            LogicalType::Boolean => {
                s.parse::<bool>()
                    .map(Value::Boolean)
                    .map_err(|_| PrismDBError::InvalidValue(format!("Cannot parse '{}' as BOOLEAN", s)))
            }
            LogicalType::TinyInt => {
                s.parse::<i8>()
                    .map(Value::TinyInt)
                    .map_err(|_| PrismDBError::InvalidValue(format!("Cannot parse '{}' as TINYINT", s)))
            }
            LogicalType::SmallInt => {
                s.parse::<i16>()
                    .map(Value::SmallInt)
                    .map_err(|_| PrismDBError::InvalidValue(format!("Cannot parse '{}' as SMALLINT", s)))
            }
            LogicalType::Integer => {
                s.parse::<i32>()
                    .map(Value::Integer)
                    .map_err(|_| PrismDBError::InvalidValue(format!("Cannot parse '{}' as INTEGER", s)))
            }
            LogicalType::BigInt => {
                s.parse::<i64>()
                    .map(Value::BigInt)
                    .map_err(|_| PrismDBError::InvalidValue(format!("Cannot parse '{}' as BIGINT", s)))
            }
            LogicalType::Float => {
                s.parse::<f32>()
                    .map(Value::Float)
                    .map_err(|_| PrismDBError::InvalidValue(format!("Cannot parse '{}' as FLOAT", s)))
            }
            LogicalType::Double => {
                s.parse::<f64>()
                    .map(Value::Double)
                    .map_err(|_| PrismDBError::InvalidValue(format!("Cannot parse '{}' as DOUBLE", s)))
            }
            LogicalType::Varchar => Ok(Value::Varchar(s.to_string())),
            LogicalType::Date => {
                // Parse date string (assuming format YYYY-MM-DD)
                Ok(Value::Varchar(s.to_string())) // TODO: proper date parsing
            }
            LogicalType::Timestamp => {
                // Parse timestamp string
                Ok(Value::Varchar(s.to_string())) // TODO: proper timestamp parsing
            }
            _ => Ok(Value::Varchar(s.to_string())),
        }
    }
}

impl ExecutionOperator for AggregateOperator {
    fn execute(&self) -> PrismDBResult<Box<dyn DataChunkStream>> {
        use crate::execution::ExecutionEngine;
        use crate::types::Value;
        use std::collections::HashMap;

        // Execute the input plan
        let mut engine = ExecutionEngine::new(self.context.clone());
        let input_plan = (*self.aggregate.input).clone();
        let mut input_stream = engine.execute(input_plan)?;

        // Hash table: group_key -> aggregate_states
        // group_key is a string representation of the GROUP BY column values
        // aggregate_states is a Vec of AggregateState (one per aggregate expression)
        let mut hash_table: HashMap<String, Vec<AggregateState>> = HashMap::new();

        // Process all input chunks
        while let Some(chunk_result) = input_stream.next() {
            let chunk = chunk_result?;

            for row_idx in 0..chunk.len() {
                // Extract group key from GROUP BY columns
                let group_key = if self.aggregate.group_by.is_empty() {
                    // No GROUP BY - single group for the entire dataset
                    String::from("__global__")
                } else {
                    // Evaluate GROUP BY expressions and create composite key
                    let mut key_parts = Vec::new();
                    for group_expr in &self.aggregate.group_by {
                        let result_vector = group_expr.evaluate(&chunk, &self.context)?;
                        let value = result_vector.get_value(row_idx)?;
                        key_parts.push(value_to_key_string(&value));
                    }
                    key_parts.join("|")
                };

                // Get or create aggregate states for this group
                let states = hash_table.entry(group_key.clone()).or_insert_with(|| {
                    self.aggregate
                        .aggregates
                        .iter()
                        .map(|_| AggregateState::new())
                        .collect()
                });

                // Update each aggregate state with this row's values
                for (agg_idx, agg_expr) in self.aggregate.aggregates.iter().enumerate() {
                    // Evaluate the aggregate's argument expression
                    let arg_value = if agg_expr.arguments.is_empty() {
                        // COUNT(*) - no arguments
                        Value::Integer(1)
                    } else {
                        let result_vector = agg_expr.arguments[0].evaluate(&chunk, &self.context)?;
                        result_vector.get_value(row_idx)?
                    };

                    // Update the aggregate state
                    states[agg_idx].update(&agg_expr.function_name, arg_value)?;
                }
            }
        }

        // Build result from hash table
        if hash_table.is_empty() {
            // No groups - for aggregates without GROUP BY, return a single row with initial values
            if self.aggregate.group_by.is_empty() {
                let mut result_chunk = DataChunk::with_rows(1);

                // Set aggregate results (e.g., COUNT(*) = 0 for empty table)
                for (col_idx, agg_expr) in self.aggregate.aggregates.iter().enumerate() {
                    let state = AggregateState::new();
                    let result_value = state.finalize(&agg_expr.function_name)?;
                    let vector = crate::types::Vector::from_values(&[result_value])?;
                    result_chunk.set_vector(col_idx, vector)?;
                }

                return Ok(Box::new(SimpleDataChunkStream::new(vec![result_chunk])));
            } else {
                return Ok(Box::new(SimpleDataChunkStream::empty()));
            }
        }

        // Convert hash table to result rows
        let num_groups = hash_table.len();
        let _num_columns = self.aggregate.group_by.len() + self.aggregate.aggregates.len();

        let mut result_chunk = DataChunk::with_rows(num_groups);

        // Build columns for GROUP BY expressions
        for (group_col_idx, _group_expr) in self.aggregate.group_by.iter().enumerate() {
            let mut group_values = Vec::new();

            // Get the correct type from schema
            let expected_type = &self.aggregate.schema[group_col_idx].data_type;

            for group_key in hash_table.keys() {
                // Parse the group key back to values
                let key_parts: Vec<&str> = group_key.split('|').collect();
                if group_col_idx < key_parts.len() {
                    // Parse the value back to the correct type based on schema
                    let value = self.parse_value_from_string(key_parts[group_col_idx], expected_type)?;
                    group_values.push(value);
                } else {
                    group_values.push(Value::Null);
                }
            }

            let vector = crate::types::Vector::from_values(&group_values)?;
            result_chunk.set_vector(group_col_idx, vector)?;
        }

        // Build columns for aggregate results
        for (agg_idx, agg_expr) in self.aggregate.aggregates.iter().enumerate() {
            let col_idx = self.aggregate.group_by.len() + agg_idx;
            let mut agg_values = Vec::new();

            for states in hash_table.values() {
                let result_value = states[agg_idx].finalize(&agg_expr.function_name)?;
                agg_values.push(result_value);
            }

            let vector = crate::types::Vector::from_values(&agg_values)?;
            result_chunk.set_vector(col_idx, vector)?;
        }

        Ok(Box::new(SimpleDataChunkStream::new(vec![result_chunk])))
    }

    fn schema(&self) -> Vec<PhysicalColumn> {
        self.aggregate.schema.clone()
    }
}

/// State for a single aggregate function (COUNT, SUM, AVG, etc.)
#[derive(Debug, Clone)]
struct AggregateState {
    count: i64,
    sum: f64,
    min: Option<Value>,
    max: Option<Value>,
}

impl AggregateState {
    fn new() -> Self {
        Self {
            count: 0,
            sum: 0.0,
            min: None,
            max: None,
        }
    }

    fn update(&mut self, function_name: &str, value: Value) -> PrismDBResult<()> {
        use crate::common::error::PrismDBError;

        // Skip NULL values for most aggregates (COUNT(*) is handled separately)
        if matches!(value, Value::Null) {
            return Ok(());
        }

        self.count += 1;

        match function_name.to_uppercase().as_str() {
            "COUNT" => {
                // Count is already updated above
            }
            "SUM" | "AVG" => {
                // Convert value to f64 for sum
                let numeric_value = match value {
                    Value::Integer(i) => i as f64,
                    Value::BigInt(i) => i as f64,
                    Value::SmallInt(i) => i as f64,
                    Value::TinyInt(i) => i as f64,
                    Value::Float(f) => f as f64,
                    Value::Double(d) => d,
                    _ => {
                        return Err(PrismDBError::InvalidValue(format!(
                            "Cannot compute {} on non-numeric value",
                            function_name
                        )))
                    }
                };
                self.sum += numeric_value;
            }
            "MIN" => {
                if self.min.is_none()
                    || Self::compare_values(&value, self.min.as_ref().unwrap())? < 0
                {
                    self.min = Some(value);
                }
            }
            "MAX" => {
                if self.max.is_none()
                    || Self::compare_values(&value, self.max.as_ref().unwrap())? > 0
                {
                    self.max = Some(value);
                }
            }
            _ => {
                return Err(PrismDBError::NotImplemented(format!(
                    "Aggregate function {} not implemented",
                    function_name
                )));
            }
        }

        Ok(())
    }

    fn finalize(&self, function_name: &str) -> PrismDBResult<Value> {
        use crate::common::error::PrismDBError;

        match function_name.to_uppercase().as_str() {
            "COUNT" => Ok(Value::BigInt(self.count)),
            "SUM" => {
                if self.count == 0 {
                    Ok(Value::Null)
                } else {
                    Ok(Value::Double(self.sum))
                }
            }
            "AVG" => {
                if self.count == 0 {
                    Ok(Value::Null)
                } else {
                    Ok(Value::Double(self.sum / self.count as f64))
                }
            }
            "MIN" => Ok(self.min.clone().unwrap_or(Value::Null)),
            "MAX" => Ok(self.max.clone().unwrap_or(Value::Null)),
            _ => Err(PrismDBError::NotImplemented(format!(
                "Aggregate function {} not implemented",
                function_name
            ))),
        }
    }

    /// Compare two values, returning -1 if a < b, 0 if a == b, 1 if a > b
    fn compare_values(a: &Value, b: &Value) -> PrismDBResult<i32> {
        match (a, b) {
            (Value::Null, Value::Null) => Ok(0),
            (Value::Null, _) => Ok(-1),
            (_, Value::Null) => Ok(1),

            (Value::Integer(a), Value::Integer(b)) => Ok(match a.cmp(b) {
                std::cmp::Ordering::Less => -1,
                std::cmp::Ordering::Equal => 0,
                std::cmp::Ordering::Greater => 1,
            }),
            (Value::BigInt(a), Value::BigInt(b)) => Ok(match a.cmp(b) {
                std::cmp::Ordering::Less => -1,
                std::cmp::Ordering::Equal => 0,
                std::cmp::Ordering::Greater => 1,
            }),
            (Value::SmallInt(a), Value::SmallInt(b)) => Ok(match a.cmp(b) {
                std::cmp::Ordering::Less => -1,
                std::cmp::Ordering::Equal => 0,
                std::cmp::Ordering::Greater => 1,
            }),
            (Value::TinyInt(a), Value::TinyInt(b)) => Ok(match a.cmp(b) {
                std::cmp::Ordering::Less => -1,
                std::cmp::Ordering::Equal => 0,
                std::cmp::Ordering::Greater => 1,
            }),

            (Value::Float(a), Value::Float(b)) => {
                if a < b {
                    Ok(-1)
                } else if a > b {
                    Ok(1)
                } else {
                    Ok(0)
                }
            }
            (Value::Double(a), Value::Double(b)) => {
                if a < b {
                    Ok(-1)
                } else if a > b {
                    Ok(1)
                } else {
                    Ok(0)
                }
            }

            (Value::Varchar(a), Value::Varchar(b)) => Ok(match a.cmp(b) {
                std::cmp::Ordering::Less => -1,
                std::cmp::Ordering::Equal => 0,
                std::cmp::Ordering::Greater => 1,
            }),
            (Value::Boolean(a), Value::Boolean(b)) => Ok(match a.cmp(b) {
                std::cmp::Ordering::Less => -1,
                std::cmp::Ordering::Equal => 0,
                std::cmp::Ordering::Greater => 1,
            }),

            // For mixed types, try to convert to f64
            _ => {
                let a_num = Self::value_to_f64(a)?;
                let b_num = Self::value_to_f64(b)?;
                if a_num < b_num {
                    Ok(-1)
                } else if a_num > b_num {
                    Ok(1)
                } else {
                    Ok(0)
                }
            }
        }
    }

    fn value_to_f64(v: &Value) -> PrismDBResult<f64> {
        use crate::common::error::PrismDBError;

        match v {
            Value::Integer(i) => Ok(*i as f64),
            Value::BigInt(i) => Ok(*i as f64),
            Value::SmallInt(i) => Ok(*i as f64),
            Value::TinyInt(i) => Ok(*i as f64),
            Value::Float(f) => Ok(*f as f64),
            Value::Double(d) => Ok(*d),
            _ => Err(PrismDBError::InvalidValue(format!(
                "Cannot convert {:?} to f64 for comparison",
                v
            ))),
        }
    }
}

/// Hash join operator
pub struct HashJoinOperator {
    join: PhysicalHashJoin,
    context: ExecutionContext,
}

impl HashJoinOperator {
    pub fn new(join: PhysicalHashJoin, context: ExecutionContext) -> Self {
        Self { join, context }
    }
}

impl ExecutionOperator for HashJoinOperator {
    fn execute(&self) -> PrismDBResult<Box<dyn DataChunkStream>> {
        use crate::common::error::PrismDBError;
        use crate::execution::ExecutionEngine;
        use crate::types::Value;
        use std::collections::HashMap;

        // Execute both sides of the join
        let mut left_engine = ExecutionEngine::new(self.context.clone());
        let mut right_engine = ExecutionEngine::new(self.context.clone());

        let left_plan = (*self.join.left).clone();
        let right_plan = (*self.join.right).clone();

        // Collect all data from the right (build) side
        let mut right_data = Vec::new();
        let mut right_stream = right_engine.execute(right_plan)?;

        while let Some(chunk_result) = right_stream.next() {
            let chunk = chunk_result?;
            // Store each row from the right side
            for row_idx in 0..chunk.len() {
                let mut row_values = Vec::new();
                for col_idx in 0..chunk.column_count() {
                    let vector = chunk.get_vector(col_idx).ok_or_else(|| {
                        PrismDBError::InvalidValue(format!("Column {} not found", col_idx))
                    })?;
                    let value = vector.get_value(row_idx)?;
                    row_values.push(value);
                }
                right_data.push(row_values);
            }
        }

        // Build hash table from right side using actual join keys
        let mut hash_table: HashMap<String, Vec<Vec<Value>>> = HashMap::new();

        for right_row in &right_data {
            if !right_row.is_empty() {
                if self.join.right_keys.is_empty() {
                    // Fallback to first column if no join keys
                    let key = right_row[0].to_string();
                    hash_table.entry(key).or_insert_with(Vec::new).push(right_row.clone());
                    continue;
                }
                // Evaluate right join key(s) to build hash key
                let mut key_parts = Vec::new();
                for right_key_expr in &self.join.right_keys {
                    // For column references, extract the column index and get the value
                    if let Some(col_ref) = right_key_expr.as_any().downcast_ref::<crate::expression::ColumnRefExpression>() {
                        let col_idx = col_ref.column_index();
                        if col_idx < right_row.len() {
                            key_parts.push(right_row[col_idx].to_string());
                        }
                    }
                }
                let key = key_parts.join("|");
                hash_table
                    .entry(key)
                    .or_insert_with(Vec::new)
                    .push(right_row.clone());
            }
        }

        // Probe with left side
        let mut result_rows = Vec::new();
        let mut left_stream = left_engine.execute(left_plan)?;

        while let Some(chunk_result) = left_stream.next() {
            let chunk = chunk_result?;

            for row_idx in 0..chunk.len() {
                // Extract left row
                let mut left_row = Vec::new();
                for col_idx in 0..chunk.column_count() {
                    let vector = chunk.get_vector(col_idx).ok_or_else(|| {
                        PrismDBError::InvalidValue(format!("Column {} not found", col_idx))
                    })?;
                    let value = vector.get_value(row_idx)?;
                    left_row.push(value);
                }

                // Probe hash table using actual join keys
                if !left_row.is_empty() && !self.join.left_keys.is_empty() {
                    // Evaluate left join key(s) to build probe key
                    let mut key_parts = Vec::new();
                    for left_key_expr in &self.join.left_keys {
                        if let Some(col_ref) = left_key_expr.as_any().downcast_ref::<crate::expression::ColumnRefExpression>() {
                            let col_idx = col_ref.column_index();
                            if col_idx < left_row.len() {
                                key_parts.push(left_row[col_idx].to_string());
                            }
                        }
                    }
                    let probe_key = key_parts.join("|");

                    if let Some(matching_rows) = hash_table.get(&probe_key) {
                        // Found matches - emit joined rows
                        for right_row in matching_rows {
                            let mut joined_row = left_row.clone();
                            joined_row.extend(right_row.clone());
                            result_rows.push(joined_row);
                        }
                    } else if self.join.join_type == crate::planner::PhysicalJoinType::Left {
                        // LEFT JOIN: emit left row with NULLs for right side
                        let mut joined_row = left_row.clone();
                        // Add NULLs for right side columns
                        for _ in 0..right_data.first().map(|r| r.len()).unwrap_or(0) {
                            joined_row.push(Value::Null);
                        }
                        result_rows.push(joined_row);
                    }
                    // For INNER JOIN, we simply don't emit rows without matches
                }
            }
        }

        // Convert result rows to DataChunks
        if result_rows.is_empty() {
            return Ok(Box::new(SimpleDataChunkStream::empty()));
        }

        // Determine schema
        let num_columns = result_rows[0].len();
        let mut data_chunk = DataChunk::with_rows(result_rows.len());

        for col_idx in 0..num_columns {
            // Collect all values for this column
            let column_values: Vec<Value> =
                result_rows.iter().map(|row| row[col_idx].clone()).collect();

            // Create vector from values
            let vector = crate::types::Vector::from_values(&column_values)?;
            data_chunk.set_vector(col_idx, vector)?;
        }

        Ok(Box::new(SimpleDataChunkStream::new(vec![data_chunk])))
    }

    fn schema(&self) -> Vec<PhysicalColumn> {
        self.join.schema.clone()
    }
}

/// Insert operator
pub struct InsertOperator {
    insert: PhysicalInsert,
    context: ExecutionContext,
}

impl InsertOperator {
    pub fn new(insert: PhysicalInsert, context: ExecutionContext) -> Self {
        Self { insert, context }
    }
}

impl ExecutionOperator for InsertOperator {
    fn execute(&self) -> PrismDBResult<Box<dyn DataChunkStream>> {
        use crate::common::error::PrismDBError;
        use crate::execution::ExecutionEngine;

        // Get the table from the catalog
        let catalog_arc = self.context.catalog.clone();
        let catalog = catalog_arc
            .read()
            .map_err(|_| PrismDBError::Internal("Failed to lock catalog".to_string()))?;

        let schema_arc = catalog.get_schema("main")?;
        let schema = schema_arc
            .read()
            .map_err(|_| PrismDBError::Internal("Failed to lock schema".to_string()))?;

        let table_arc = schema.get_table(&self.insert.table_name)?;

        // Drop locks before getting table data to avoid holding multiple locks
        drop(schema);
        drop(catalog);

        let table = table_arc
            .read()
            .map_err(|_| PrismDBError::Internal("Failed to lock table".to_string()))?;

        let table_data_arc = table.get_data();

        // Drop table read lock
        drop(table);

        // Execute the input plan to get the data to insert
        let mut engine = ExecutionEngine::new(self.context.clone());
        let input_plan = (*self.insert.input).clone();
        let mut input_stream = engine.execute(input_plan)?;

        // Insert all rows from the input stream
        let mut total_rows_inserted = 0;

        while let Some(chunk_result) = input_stream.next() {
            let chunk = chunk_result?;

            // Insert each row from the chunk
            let mut table_data = table_data_arc
                .write()
                .map_err(|_| PrismDBError::Internal("Failed to lock table data".to_string()))?;

            for row_idx in 0..chunk.len() {
                // Extract values from this row
                let mut values = Vec::new();
                for col_idx in 0..chunk.column_count() {
                    let vector = chunk.get_vector(col_idx).ok_or_else(|| {
                        PrismDBError::InvalidValue(format!("Column {} not found", col_idx))
                    })?;
                    let value = vector.get_value(row_idx)?;
                    values.push(value);
                }

                // Insert the row
                table_data.insert_row(&values)?;
                total_rows_inserted += 1;
            }

            // Drop the lock after each chunk to allow concurrent access
            drop(table_data);
        }

        // Return a DataChunk with the affected row count
        use crate::types::{LogicalType, Vector};
        let mut result_chunk = DataChunk::new();
        let mut count_vector = Vector::new(LogicalType::BigInt, 1);
        count_vector.push(&Value::BigInt(total_rows_inserted as i64))?;
        result_chunk.add_vector(count_vector)?;

        Ok(Box::new(SimpleDataChunkStream::new(vec![result_chunk])))
    }

    fn schema(&self) -> Vec<PhysicalColumn> {
        // INSERT typically doesn't return data, just a count
        vec![]
    }
}

/// Update operator
pub struct UpdateOperator {
    update: PhysicalUpdate,
    context: ExecutionContext,
}

impl UpdateOperator {
    pub fn new(update: PhysicalUpdate, context: ExecutionContext) -> Self {
        Self {
            update,
            context,
        }
    }
}

impl ExecutionOperator for UpdateOperator {
    fn execute(&self) -> PrismDBResult<Box<dyn DataChunkStream>> {
        use crate::common::error::PrismDBError;

        // Get the table from the catalog
        let catalog_arc = self.context.catalog.clone();
        let catalog = catalog_arc
            .read()
            .map_err(|_| PrismDBError::Internal("Failed to lock catalog".to_string()))?;

        let schema_arc = catalog.get_schema("main")?;
        let schema = schema_arc
            .read()
            .map_err(|_| PrismDBError::Internal("Failed to lock schema".to_string()))?;

        let table_arc = schema.get_table(&self.update.table_name)?;

        // Drop locks before updating
        drop(schema);
        drop(catalog);

        let table = table_arc
            .read()
            .map_err(|_| PrismDBError::Internal("Failed to lock table".to_string()))?;

        let table_info = table.get_table_info();
        let table_data_arc = table.get_data();

        // Drop table read lock
        drop(table);

        // Get column indices for the assignments
        let mut column_indices = std::collections::HashMap::new();
        for (col_name, _) in &self.update.assignments {
            let col_idx = table_info
                .columns
                .iter()
                .position(|c| &c.name == col_name)
                .ok_or_else(|| {
                    PrismDBError::InvalidValue(format!("Column '{}' not found", col_name))
                })?;
            column_indices.insert(col_name.clone(), col_idx);
        }

        // Lock table data for reading and updating
        let mut table_data = table_data_arc
            .write()
            .map_err(|_| PrismDBError::Internal("Failed to lock table data".to_string()))?;

        // Get the total physical number of rows (including deleted ones)
        // We need to iterate over all rows to find which ones match the WHERE clause
        let row_count = table_data.physical_row_count();
        let mut rows_updated = 0;

        // Process rows in chunks
        const CHUNK_SIZE: usize = 1024;
        for chunk_start in (0..row_count).step_by(CHUNK_SIZE) {
            let chunk_end = std::cmp::min(chunk_start + CHUNK_SIZE, row_count);
            // Use unfiltered chunk to see all physical rows including deleted ones
            let chunk = table_data.create_chunk_unfiltered(chunk_start, chunk_end - chunk_start)?;

            for row_idx in 0..chunk.len() {
                // Evaluate WHERE condition if present
                let should_update = if let Some(ref condition) = self.update.condition {
                    let result = condition.evaluate_row(&chunk, row_idx, &self.context)?;
                    match result {
                        Value::Boolean(b) => b,
                        _ => false,
                    }
                } else {
                    true // No WHERE clause means update all rows
                };

                if should_update {
                    // Get the actual row ID in the table
                    let actual_row_id = chunk_start + row_idx;

                    // Extract current row values
                    let mut row_values = Vec::new();
                    for col_idx in 0..chunk.column_count() {
                        let vector = chunk.get_vector(col_idx).ok_or_else(|| {
                            PrismDBError::InvalidValue(format!("Column {} not found", col_idx))
                        })?;
                        row_values.push(vector.get_value(row_idx)?);
                    }

                    // Apply assignments to create updated row
                    for (col_name, expr) in &self.update.assignments {
                        let new_value = expr.evaluate_row(&chunk, row_idx, &self.context)?;
                        let col_idx = column_indices[col_name];
                        row_values[col_idx] = new_value;
                    }

                    // Update the row using the actual row ID
                    table_data.update_row(actual_row_id, &row_values)?;
                    rows_updated += 1;
                }
            }
        }

        // Drop table data lock
        drop(table_data);

        // Return a DataChunk with the affected row count
        use crate::types::{LogicalType, Vector};
        let mut result_chunk = DataChunk::new();
        let mut count_vector = Vector::new(LogicalType::BigInt, 1);
        count_vector.push(&Value::BigInt(rows_updated as i64))?;
        result_chunk.add_vector(count_vector)?;

        Ok(Box::new(SimpleDataChunkStream::new(vec![result_chunk])))
    }

    fn schema(&self) -> Vec<PhysicalColumn> {
        // UPDATE doesn't return rows
        vec![]
    }
}

/// Delete operator
pub struct DeleteOperator {
    delete: PhysicalDelete,
    context: ExecutionContext,
}

impl DeleteOperator {
    pub fn new(delete: PhysicalDelete, context: ExecutionContext) -> Self {
        Self {
            delete,
            context,
        }
    }
}

impl ExecutionOperator for DeleteOperator {
    fn execute(&self) -> PrismDBResult<Box<dyn DataChunkStream>> {
        use crate::common::error::PrismDBError;

        // Get the table from the catalog
        let catalog_arc = self.context.catalog.clone();
        let catalog = catalog_arc
            .read()
            .map_err(|_| PrismDBError::Internal("Failed to lock catalog".to_string()))?;

        let schema_arc = catalog.get_schema("main")?;
        let schema = schema_arc
            .read()
            .map_err(|_| PrismDBError::Internal("Failed to lock schema".to_string()))?;

        let table_arc = schema.get_table(&self.delete.table_name)?;

        // Drop locks before deleting
        drop(schema);
        drop(catalog);

        let table = table_arc
            .read()
            .map_err(|_| PrismDBError::Internal("Failed to lock table".to_string()))?;

        let table_data_arc = table.get_data();

        // Drop table read lock
        drop(table);

        // Lock table data for reading and deleting
        let mut table_data = table_data_arc
            .write()
            .map_err(|_| PrismDBError::Internal("Failed to lock table data".to_string()))?;

        // Get the total physical number of rows (including deleted ones)
        // We need to iterate over all rows to find which ones match the WHERE clause
        let row_count = table_data.physical_row_count();

        // Collect row IDs to delete (iterate backwards to avoid index shifting issues)
        let mut rows_to_delete = Vec::new();

        // Process rows in chunks
        const CHUNK_SIZE: usize = 1024;
        for chunk_start in (0..row_count).step_by(CHUNK_SIZE) {
            let chunk_end = std::cmp::min(chunk_start + CHUNK_SIZE, row_count);
            // Use unfiltered chunk to see all physical rows including deleted ones
            let chunk = table_data.create_chunk_unfiltered(chunk_start, chunk_end - chunk_start)?;

            for row_idx in 0..chunk.len() {
                // Evaluate WHERE condition if present
                let should_delete = if let Some(ref condition) = self.delete.condition {
                    let result = condition.evaluate_row(&chunk, row_idx, &self.context)?;
                    match result {
                        Value::Boolean(b) => b,
                        _ => false,
                    }
                } else {
                    true // No WHERE clause means delete all rows
                };

                if should_delete {
                    let actual_row_id = chunk_start + row_idx;
                    rows_to_delete.push(actual_row_id);
                }
            }
        }

        // Delete rows in reverse order to avoid index issues
        rows_to_delete.sort_by(|a, b| b.cmp(a));  // Sort descending
        let rows_deleted = rows_to_delete.len();
        for row_id in rows_to_delete {
            table_data.delete_row(row_id)?;
        }

        // Drop table data lock
        drop(table_data);

        // Return a DataChunk with the affected row count
        use crate::types::{LogicalType, Vector};
        let mut result_chunk = DataChunk::new();
        let mut count_vector = Vector::new(LogicalType::BigInt, 1);
        count_vector.push(&Value::BigInt(rows_deleted as i64))?;
        result_chunk.add_vector(count_vector)?;

        Ok(Box::new(SimpleDataChunkStream::new(vec![result_chunk])))
    }

    fn schema(&self) -> Vec<PhysicalColumn> {
        // DELETE doesn't return rows
        vec![]
    }
}

/// Create table operator
pub struct CreateTableOperator {
    create_table: PhysicalCreateTable,
    context: ExecutionContext,
}

impl CreateTableOperator {
    pub fn new(create_table: PhysicalCreateTable, context: ExecutionContext) -> Self {
        Self {
            create_table,
            context,
        }
    }
}

impl ExecutionOperator for CreateTableOperator {
    fn execute(&self) -> PrismDBResult<Box<dyn DataChunkStream>> {
        use crate::common::error::PrismDBError;
        use crate::storage::{ColumnInfo, TableInfo};

        // Get the catalog
        let catalog_arc = self.context.catalog.clone();
        let catalog = catalog_arc
            .read()
            .map_err(|_| PrismDBError::Internal("Failed to lock catalog".to_string()))?;

        let schema_arc = catalog.get_schema("main")?;

        // Drop catalog lock before modifying schema
        drop(catalog);

        let mut schema = schema_arc
            .write()
            .map_err(|_| PrismDBError::Internal("Failed to lock schema".to_string()))?;

        // Check if table already exists
        if schema.get_table(&self.create_table.table_name).is_ok() {
            if self.create_table.if_not_exists {
                // Table exists but IF NOT EXISTS was specified, just return success
                return Ok(Box::new(SimpleDataChunkStream::empty()));
            } else {
                return Err(PrismDBError::Catalog(format!(
                    "Table '{}' already exists",
                    self.create_table.table_name
                )));
            }
        }

        // Create table info
        let mut table_info = TableInfo::new(self.create_table.table_name.clone());

        // Add columns to the table
        for (idx, col) in self.create_table.schema.iter().enumerate() {
            table_info.add_column(ColumnInfo::new(
                col.name.clone(),
                col.data_type.clone(),
                idx,
            ))?;
        }

        // Create the table in the schema
        schema.create_table(&table_info)?;

        // Return empty result
        Ok(Box::new(SimpleDataChunkStream::empty()))
    }

    fn schema(&self) -> Vec<PhysicalColumn> {
        // CREATE TABLE doesn't return data
        vec![]
    }
}

/// Drop table operator
pub struct DropTableOperator {
    drop_table: PhysicalDropTable,
    context: ExecutionContext,
}

impl DropTableOperator {
    pub fn new(drop_table: PhysicalDropTable, context: ExecutionContext) -> Self {
        Self {
            drop_table,
            context,
        }
    }
}

impl ExecutionOperator for DropTableOperator {
    fn execute(&self) -> PrismDBResult<Box<dyn DataChunkStream>> {
        use crate::common::error::PrismDBError;

        // Get the catalog
        let catalog_arc = self.context.catalog.clone();
        let catalog = catalog_arc
            .read()
            .map_err(|_| PrismDBError::Internal("Failed to lock catalog".to_string()))?;

        let schema_arc = catalog.get_schema("main")?;

        // Drop catalog lock before modifying schema
        drop(catalog);

        let mut schema = schema_arc
            .write()
            .map_err(|_| PrismDBError::Internal("Failed to lock schema".to_string()))?;

        // Check if table exists
        if schema.get_table(&self.drop_table.table_name).is_err() {
            if self.drop_table.if_exists {
                // Table doesn't exist but IF EXISTS was specified, just return success
                return Ok(Box::new(SimpleDataChunkStream::empty()));
            } else {
                return Err(PrismDBError::Catalog(format!(
                    "Table '{}' does not exist",
                    self.drop_table.table_name
                )));
            }
        }

        // Drop the table
        schema.drop_table(&self.drop_table.table_name)?;

        // Return empty result
        Ok(Box::new(SimpleDataChunkStream::empty()))
    }

    fn schema(&self) -> Vec<PhysicalColumn> {
        // DROP TABLE doesn't return data
        vec![]
    }
}

/// Values operator (produces constant rows)
pub struct ValuesOperator {
    values: crate::planner::PhysicalValues,
    #[allow(dead_code)]
    context: ExecutionContext,
}

impl ValuesOperator {
    pub fn new(values: crate::planner::PhysicalValues, context: ExecutionContext) -> Self {
        Self { values, context }
    }
}

impl ExecutionOperator for ValuesOperator {
    fn execute(&self) -> PrismDBResult<Box<dyn DataChunkStream>> {
        use crate::types::Vector;

        // Evaluate all value expressions and create data chunks
        let mut chunks = Vec::new();

        if !self.values.values.is_empty() {
            let num_rows = self.values.values.len();

            // Special case: if schema is empty (SELECT without FROM), create a chunk with rows but no columns
            if self.values.schema.is_empty() {
                let chunk = DataChunk::with_rows(num_rows);
                chunks.push(chunk);
            } else {
                let mut column_vectors: Vec<Vector> = Vec::new();

                // Initialize vectors for each column
                for col in &self.values.schema {
                    column_vectors.push(Vector::new(col.data_type.clone(), num_rows));
                }

                // Evaluate each row and populate column vectors
                // Create a dummy chunk with 1 row for expression evaluation
                let dummy_chunk = DataChunk::with_rows(1);

                for (_row_idx, row) in self.values.values.iter().enumerate() {
                    for (col_idx, expr) in row.iter().enumerate() {
                        // Evaluate expression against dummy chunk
                        let result_vector = expr.evaluate(&dummy_chunk, &self.context)?;
                        let value = result_vector.get_value(0)?;
                        // Use push instead of set_value to properly update count
                        column_vectors[col_idx].push(&value)?;
                    }
                }

                // Create data chunk from vectors
                let mut chunk = DataChunk::new();
                for vector in column_vectors {
                    chunk.add_vector(vector)?;
                }

                chunks.push(chunk);
            }
        }

        Ok(Box::new(SimpleDataChunkStream::new(chunks)))
    }

    fn schema(&self) -> Vec<PhysicalColumn> {
        self.values.schema.clone()
    }
}

/// Empty result operator
pub struct EmptyResultOperator {
    _context: ExecutionContext,
}

impl EmptyResultOperator {
    pub fn new(context: ExecutionContext) -> Self {
        Self { _context: context }
    }
}

impl ExecutionOperator for EmptyResultOperator {
    fn execute(&self) -> PrismDBResult<Box<dyn DataChunkStream>> {
        Ok(Box::new(SimpleDataChunkStream::empty()))
    }

    fn schema(&self) -> Vec<PhysicalColumn> {
        vec![]
    }
}

/// PIVOT operator - transforms rows to columns
pub struct PivotOperator {
    pivot: crate::planner::PhysicalPivot,
    context: ExecutionContext,
}

impl PivotOperator {
    pub fn new(pivot: crate::planner::PhysicalPivot, context: ExecutionContext) -> Self {
        Self { pivot, context }
    }
}

impl ExecutionOperator for PivotOperator {
    fn execute(&self) -> PrismDBResult<Box<dyn DataChunkStream>> {
        use crate::common::error::PrismDBError;
        use crate::execution::ExecutionEngine;
        use crate::expression::aggregate::AggregateState;
        use crate::types::Value;
        use std::collections::HashMap;

        // Execute the input plan
        let mut engine = ExecutionEngine::new(self.context.clone());
        let input_plan = (*self.pivot.input).clone();
        let mut input_stream = engine.execute(input_plan)?;

        // Collect pivot values (distinct values from ON columns or explicit IN values)
        // For initial implementation, we'll use explicit IN values
        let pivot_values = if let Some(in_vals) = &self.pivot.in_values {
            in_vals.clone()
        } else {
            // Without explicit IN values, we'd need to scan data first to get distinct values
            // For now, return error requiring explicit IN clause
            return Err(PrismDBError::Execution(
                "PIVOT requires explicit IN clause for pivot values".to_string(),
            ));
        };

        // Hash table: (group_key, pivot_key) -> aggregate_states
        // group_key: concatenation of GROUP BY column values
        // pivot_key: concatenation of ON column values
        // aggregate_states: Vec of Box<dyn AggregateState> (one per USING aggregate expression)
        let mut hash_table: HashMap<(String, String), Vec<Box<dyn AggregateState>>> = HashMap::new();

        // Process all input chunks
        while let Some(chunk_result) = input_stream.next() {
            let chunk = chunk_result?;

            for row_idx in 0..chunk.len() {
                // Extract group key from GROUP BY columns
                let group_key = if self.pivot.group_by.is_empty() {
                    String::from("__global__")
                } else {
                    let mut key_parts = Vec::new();
                    for group_expr in &self.pivot.group_by {
                        let result_vector = group_expr.evaluate(&chunk, &self.context)?;
                        let value = result_vector.get_value(row_idx)?;
                        key_parts.push(value_to_key_string(&value));
                    }
                    key_parts.join("|")
                };

                // Extract pivot key from ON columns
                let mut pivot_key_parts = Vec::new();
                for on_expr in &self.pivot.on_columns {
                    let result_vector = on_expr.evaluate(&chunk, &self.context)?;
                    let value = result_vector.get_value(row_idx)?;
                    pivot_key_parts.push(value_to_key_string(&value));
                }
                let pivot_key = pivot_key_parts.join("|");

                // Get or create aggregate states for this (group, pivot) combination
                let states = hash_table
                    .entry((group_key.clone(), pivot_key.clone()))
                    .or_insert_with(|| {
                        self.pivot
                            .using_values
                            .iter()
                            .map(|using_val| {
                                // Extract aggregate function name from expression using utility function
                                let agg_name = crate::execution::pivot_utils::extract_aggregate_name(&using_val.expression)
                                    .unwrap_or_else(|| "sum".to_string()); // Default to SUM if detection fails

                                // Create appropriate aggregate state
                                crate::expression::aggregate::create_aggregate_state(&agg_name)
                                    .unwrap_or_else(|_| Box::new(crate::expression::aggregate::SumState::new()))
                            })
                            .collect()
                    });

                // Update each aggregate state with this row's values
                for (agg_idx, using_val) in self.pivot.using_values.iter().enumerate() {
                    // For PIVOT, we need to extract aggregate function name and argument
                    // For simplicity, assume using_values are aggregate function calls
                    // We'll evaluate the expression and update state
                    let arg_vector = using_val.expression.evaluate(&chunk, &self.context)?;
                    let arg_value = arg_vector.get_value(row_idx)?;

                    // Update the aggregate state
                    states[agg_idx].update(&arg_value)?;
                }
            }
        }

        // Build result from hash table
        if hash_table.is_empty() {
            return Ok(Box::new(SimpleDataChunkStream::empty()));
        }

        // Group the hash table entries by group_key
        let mut group_map: HashMap<String, HashMap<String, Vec<Box<dyn AggregateState>>>> = HashMap::new();
        for ((group_key, pivot_key), states) in hash_table {
            group_map
                .entry(group_key)
                .or_insert_with(HashMap::new)
                .insert(pivot_key, states);
        }

        // Build output rows (one per group)
        let num_groups = group_map.len();
        let num_columns = self.pivot.schema.len();

        // Collect all rows first, then build vectors column-by-column
        let mut all_rows: Vec<Vec<Value>> = Vec::new();

        for (group_key, pivot_map) in group_map {
            let mut column_values = Vec::new();

            // Add GROUP BY column values (parse from group_key)
            if self.pivot.group_by.is_empty() {
                // No GROUP BY columns
            } else {
                let key_parts: Vec<&str> = group_key.split('|').collect();
                for part in key_parts {
                    // Parse value back (simplified - assumes integers or strings)
                    let val = if let Ok(i) = part.parse::<i32>() {
                        Value::Integer(i)
                    } else if let Ok(i) = part.parse::<i64>() {
                        Value::BigInt(i)
                    } else {
                        Value::Varchar(part.to_string())
                    };
                    column_values.push(val);
                }
            }

            // Add pivot columns (one for each pivot_value * using_value)
            for pivot_val in &pivot_values {
                // Extract constant value using utility function
                let pivot_key = crate::execution::pivot_utils::extract_constant_value(&pivot_val.value, &self.context)
                    .unwrap_or_else(|| "unknown".to_string());

                for (agg_idx, _using_val) in self.pivot.using_values.iter().enumerate() {
                    let value = if let Some(states) = pivot_map.get(&pivot_key) {
                        states[agg_idx].finalize()?
                    } else {
                        Value::Null
                    };
                    column_values.push(value);
                }
            }

            all_rows.push(column_values);
        }

        // Build result chunk column-by-column
        let mut result_chunk = DataChunk::with_rows(num_groups);
        for col_idx in 0..num_columns {
            let mut column_data = Vec::new();
            for row in &all_rows {
                if col_idx < row.len() {
                    column_data.push(row[col_idx].clone());
                } else {
                    column_data.push(Value::Null);
                }
            }
            let vector = crate::types::Vector::from_values(&column_data)?;
            result_chunk.set_vector(col_idx, vector)?;
        }

        Ok(Box::new(SimpleDataChunkStream::new(vec![result_chunk])))
    }

    fn schema(&self) -> Vec<PhysicalColumn> {
        self.pivot.schema.clone()
    }
}

/// UNPIVOT operator - transforms columns to rows
pub struct UnpivotOperator {
    unpivot: crate::planner::PhysicalUnpivot,
    context: ExecutionContext,
}

impl UnpivotOperator {
    pub fn new(unpivot: crate::planner::PhysicalUnpivot, context: ExecutionContext) -> Self {
        Self { unpivot, context }
    }
}

impl ExecutionOperator for UnpivotOperator {
    fn execute(&self) -> PrismDBResult<Box<dyn DataChunkStream>> {
        use crate::execution::ExecutionEngine;
        use crate::types::Value;

        // Execute the input plan
        let mut engine = ExecutionEngine::new(self.context.clone());
        let input_plan = (*self.unpivot.input).clone();
        let mut input_stream = engine.execute(input_plan)?;

        let mut output_rows: Vec<Vec<Value>> = Vec::new();

        // Process all input chunks
        while let Some(chunk_result) = input_stream.next() {
            let chunk = chunk_result?;

            for row_idx in 0..chunk.len() {
                // For each input row, create N output rows (one per unpivoted column)
                for on_expr in self.unpivot.on_columns.iter() {
                    // Evaluate the column value
                    let result_vector = on_expr.evaluate(&chunk, &self.context)?;
                    let column_value = result_vector.get_value(row_idx)?;

                    // Skip NULL values if include_nulls is false
                    if !self.unpivot.include_nulls && column_value == Value::Null {
                        continue;
                    }

                    // Build output row
                    let mut output_row = Vec::new();

                    // Add values from non-unpivoted columns (passthrough columns)
                    // These are columns not in the IN clause
                    // For this implementation, we identify them by checking the schema
                    // (Simplified: we'd need to track which input columns to preserve)

                    // Extract column name using utility function
                    let column_name = crate::execution::pivot_utils::extract_column_name(on_expr);

                    // Add name column (the original column name being unpivoted)
                    output_row.push(Value::Varchar(column_name));

                    // Add value column(s)
                    for _value_col in &self.unpivot.value_columns {
                        output_row.push(column_value.clone());
                    }

                    output_rows.push(output_row);
                }
            }
        }

        // Build result chunk from output rows
        if output_rows.is_empty() {
            return Ok(Box::new(SimpleDataChunkStream::empty()));
        }

        let num_rows = output_rows.len();
        let num_columns = self.unpivot.schema.len();
        let mut result_chunk = DataChunk::with_rows(num_rows);

        // Transpose rows to columns
        for col_idx in 0..num_columns {
            let mut column_values = Vec::new();
            for row in &output_rows {
                if col_idx < row.len() {
                    column_values.push(row[col_idx].clone());
                } else {
                    column_values.push(Value::Null);
                }
            }
            let vector = crate::types::Vector::from_values(&column_values)?;
            result_chunk.set_vector(col_idx, vector)?;
        }

        Ok(Box::new(SimpleDataChunkStream::new(vec![result_chunk])))
    }

    fn schema(&self) -> Vec<PhysicalColumn> {
        self.unpivot.schema.clone()
    }
}

/// Union operator - concatenates results from two queries
pub struct UnionOperator {
    union: PhysicalUnion,
    context: ExecutionContext,
}

impl UnionOperator {
    pub fn new(union: PhysicalUnion, context: ExecutionContext) -> Self {
        Self { union, context }
    }
}

impl ExecutionOperator for UnionOperator {
    fn execute(&self) -> PrismDBResult<Box<dyn DataChunkStream>> {
        use crate::execution::ExecutionEngine;
        use std::collections::HashSet;

        let mut engine = ExecutionEngine::new(self.context.clone());

        // Execute left child
        let mut left_stream = engine.execute(*self.union.left.clone())?;
        let mut all_chunks = Vec::new();

        while let Some(chunk_result) = left_stream.next() {
            all_chunks.push(chunk_result?);
        }

        // Execute right child
        let mut right_stream = engine.execute(*self.union.right.clone())?;

        while let Some(chunk_result) = right_stream.next() {
            all_chunks.push(chunk_result?);
        }

        // If UNION (not UNION ALL), remove duplicates
        if !self.union.all {
            // Deduplicate rows
            let mut unique_rows = HashSet::new();
            let mut dedup_chunks = Vec::new();

            for chunk in &all_chunks {
                let mut unique_chunk_rows = Vec::new();

                for row_idx in 0..chunk.len() {
                    // Create a row representation for hashing
                    let mut row_values = Vec::new();
                    for col_idx in 0..chunk.column_count() {
                        let vector = chunk.get_vector(col_idx)
                            .ok_or_else(|| PrismDBError::Execution(format!("Missing column {}", col_idx)))?;
                        row_values.push(vector.get_value(row_idx)?);
                    }

                    // Use string representation for hashing (simple but works)
                    let row_key = format!("{:?}", row_values);
                    if unique_rows.insert(row_key) {
                        unique_chunk_rows.push(row_values);
                    }
                }

                // Build deduplicated chunk
                if !unique_chunk_rows.is_empty() {
                    let num_rows = unique_chunk_rows.len();
                    let num_cols = chunk.column_count();
                    let mut dedup_chunk = DataChunk::with_rows(num_rows);

                    for col_idx in 0..num_cols {
                        let mut col_values = Vec::new();
                        for row in &unique_chunk_rows {
                            col_values.push(row[col_idx].clone());
                        }
                        let vector = crate::types::Vector::from_values(&col_values)?;
                        dedup_chunk.set_vector(col_idx, vector)?;
                    }

                    dedup_chunks.push(dedup_chunk);
                }
            }

            Ok(Box::new(SimpleDataChunkStream::new(dedup_chunks)))
        } else {
            // UNION ALL - just concatenate
            Ok(Box::new(SimpleDataChunkStream::new(all_chunks)))
        }
    }

    fn schema(&self) -> Vec<PhysicalColumn> {
        self.union.schema.clone()
    }
}

/// Intersect operator - returns rows that appear in both left and right
pub struct IntersectOperator {
    left: Box<PhysicalPlan>,
    right: Box<PhysicalPlan>,
    schema: Vec<PhysicalColumn>,
    context: ExecutionContext,
}

impl IntersectOperator {
    pub fn new(left: PhysicalPlan, right: PhysicalPlan, schema: Vec<PhysicalColumn>, context: ExecutionContext) -> Self {
        Self {
            left: Box::new(left),
            right: Box::new(right),
            schema,
            context
        }
    }
}

impl ExecutionOperator for IntersectOperator {
    fn execute(&self) -> PrismDBResult<Box<dyn DataChunkStream>> {
        use crate::execution::ExecutionEngine;
        use std::collections::HashSet;

        let mut engine = ExecutionEngine::new(self.context.clone());

        // Execute left child and collect all rows into a HashSet
        let mut left_stream = engine.execute(*self.left.clone())?;
        let mut left_rows = HashSet::new();

        while let Some(chunk_result) = left_stream.next() {
            let chunk = chunk_result?;
            for row_idx in 0..chunk.len() {
                let mut row_values = Vec::new();
                for col_idx in 0..chunk.column_count() {
                    let vector = chunk.get_vector(col_idx)
                        .ok_or_else(|| PrismDBError::Execution(format!("Missing column {}", col_idx)))?;
                    row_values.push(vector.get_value(row_idx)?);
                }
                let row_key = format!("{:?}", row_values);
                left_rows.insert(row_key);
            }
        }

        // Execute right child and keep only rows that exist in left
        let mut right_stream = engine.execute(*self.right.clone())?;
        let mut result_rows = Vec::new();
        let mut seen = HashSet::new();

        while let Some(chunk_result) = right_stream.next() {
            let chunk = chunk_result?;
            for row_idx in 0..chunk.len() {
                let mut row_values = Vec::new();
                for col_idx in 0..chunk.column_count() {
                    let vector = chunk.get_vector(col_idx)
                        .ok_or_else(|| PrismDBError::Execution(format!("Missing column {}", col_idx)))?;
                    row_values.push(vector.get_value(row_idx)?);
                }
                let row_key = format!("{:?}", row_values);

                // Only include if in left and not already added (dedup)
                if left_rows.contains(&row_key) && seen.insert(row_key) {
                    result_rows.push(row_values);
                }
            }
        }

        // Build result chunk
        if result_rows.is_empty() {
            return Ok(Box::new(SimpleDataChunkStream::empty()));
        }

        let num_rows = result_rows.len();
        let num_cols = self.schema.len();
        let mut result_chunk = DataChunk::with_rows(num_rows);

        for col_idx in 0..num_cols {
            let mut col_values = Vec::new();
            for row in &result_rows {
                col_values.push(row[col_idx].clone());
            }
            let vector = crate::types::Vector::from_values(&col_values)?;
            result_chunk.set_vector(col_idx, vector)?;
        }

        Ok(Box::new(SimpleDataChunkStream::new(vec![result_chunk])))
    }

    fn schema(&self) -> Vec<PhysicalColumn> {
        self.schema.clone()
    }
}

/// Except operator - returns rows in left that are NOT in right
pub struct ExceptOperator {
    left: Box<PhysicalPlan>,
    right: Box<PhysicalPlan>,
    schema: Vec<PhysicalColumn>,
    context: ExecutionContext,
}

impl ExceptOperator {
    pub fn new(left: PhysicalPlan, right: PhysicalPlan, schema: Vec<PhysicalColumn>, context: ExecutionContext) -> Self {
        Self {
            left: Box::new(left),
            right: Box::new(right),
            schema,
            context
        }
    }
}

impl ExecutionOperator for ExceptOperator {
    fn execute(&self) -> PrismDBResult<Box<dyn DataChunkStream>> {
        use crate::execution::ExecutionEngine;
        use std::collections::HashSet;

        let mut engine = ExecutionEngine::new(self.context.clone());

        // Execute right child and collect all rows into a HashSet
        let mut right_stream = engine.execute(*self.right.clone())?;
        let mut right_rows = HashSet::new();

        while let Some(chunk_result) = right_stream.next() {
            let chunk = chunk_result?;
            for row_idx in 0..chunk.len() {
                let mut row_values = Vec::new();
                for col_idx in 0..chunk.column_count() {
                    let vector = chunk.get_vector(col_idx)
                        .ok_or_else(|| PrismDBError::Execution(format!("Missing column {}", col_idx)))?;
                    row_values.push(vector.get_value(row_idx)?);
                }
                let row_key = format!("{:?}", row_values);
                right_rows.insert(row_key);
            }
        }

        // Execute left child and keep only rows NOT in right
        let mut left_stream = engine.execute(*self.left.clone())?;
        let mut result_rows = Vec::new();
        let mut seen = HashSet::new();

        while let Some(chunk_result) = left_stream.next() {
            let chunk = chunk_result?;
            for row_idx in 0..chunk.len() {
                let mut row_values = Vec::new();
                for col_idx in 0..chunk.column_count() {
                    let vector = chunk.get_vector(col_idx)
                        .ok_or_else(|| PrismDBError::Execution(format!("Missing column {}", col_idx)))?;
                    row_values.push(vector.get_value(row_idx)?);
                }
                let row_key = format!("{:?}", row_values);

                // Only include if NOT in right and not already added (dedup)
                if !right_rows.contains(&row_key) && seen.insert(row_key) {
                    result_rows.push(row_values);
                }
            }
        }

        // Build result chunk
        if result_rows.is_empty() {
            return Ok(Box::new(SimpleDataChunkStream::empty()));
        }

        let num_rows = result_rows.len();
        let num_cols = self.schema.len();
        let mut result_chunk = DataChunk::with_rows(num_rows);

        for col_idx in 0..num_cols {
            let mut col_values = Vec::new();
            for row in &result_rows {
                col_values.push(row[col_idx].clone());
            }
            let vector = crate::types::Vector::from_values(&col_values)?;
            result_chunk.set_vector(col_idx, vector)?;
        }

        Ok(Box::new(SimpleDataChunkStream::new(vec![result_chunk])))
    }

    fn schema(&self) -> Vec<PhysicalColumn> {
        self.schema.clone()
    }
}

/// Recursive CTE operator - implements fixpoint iteration
pub struct RecursiveCTEOperator {
    name: String,
    base_case: Box<PhysicalPlan>,
    recursive_case: Box<PhysicalPlan>,
    schema: Vec<PhysicalColumn>,
    context: ExecutionContext,
}

impl RecursiveCTEOperator {
    pub fn new(rcte: &crate::planner::physical_plan::PhysicalRecursiveCTE, context: ExecutionContext) -> Self {
        Self {
            name: rcte.name.clone(),
            base_case: rcte.base_case.clone(),
            recursive_case: rcte.recursive_case.clone(),
            schema: rcte.schema.clone(),
            context,
        }
    }
}

impl ExecutionOperator for RecursiveCTEOperator {
    fn execute(&self) -> PrismDBResult<Box<dyn DataChunkStream>> {
        use crate::execution::ExecutionEngine;
        use crate::types::Vector;
        use std::collections::HashSet;

        let mut engine = ExecutionEngine::new(self.context.clone());

        // Step 1: Execute base case to get initial results
        let mut base_stream = engine.execute(*self.base_case.clone())?;
        let mut all_rows = Vec::new();
        let mut seen_rows: HashSet<String> = HashSet::new();

        while let Some(chunk_result) = base_stream.next() {
            let chunk = chunk_result?;
            for row_idx in 0..chunk.len() {
                let mut row_values = Vec::new();
                for col_idx in 0..chunk.column_count() {
                    let vector = chunk.get_vector(col_idx)
                        .ok_or_else(|| PrismDBError::Execution(format!("Missing column {}", col_idx)))?;
                    row_values.push(vector.get_value(row_idx)?);
                }
                let row_key = format!("{:?}", row_values);
                if seen_rows.insert(row_key) {
                    all_rows.push(row_values);
                }
            }
        }

        // If no base results, return empty
        if all_rows.is_empty() {
            return Ok(Box::new(SimpleDataChunkStream::empty()));
        }

        // Step 2: Iterative fixpoint loop
        let max_iterations = 100; // Safety limit
        let mut working_table = all_rows.clone();

        // Create the temporary table once before the loop
        let table_ref = {
            let catalog_lock = self.context.catalog.write().unwrap();
            if let Ok(schema_ref) = catalog_lock.get_schema("main") {
                let mut schema_lock = schema_ref.write().unwrap();

                // Drop any existing table with this name
                let _ = schema_lock.drop_table(&self.name);

                // Create new table with the schema
                let columns: Vec<crate::storage::table::ColumnInfo> = self.schema.iter()
                    .enumerate()
                    .map(|(idx, col)| crate::storage::table::ColumnInfo {
                        name: col.name.clone(),
                        column_type: col.data_type.clone(),
                        nullable: true,
                        default_value: None,
                        column_index: idx,
                        is_primary_key: false,
                        is_unique: false,
                    })
                    .collect();

                let table_info = crate::storage::table::TableInfo {
                    name: self.name.clone(),
                    table_name: self.name.clone(),
                    schema_name: "main".to_string(),
                    columns: columns.clone(),
                    primary_key: vec![],
                    statistics: crate::storage::table::TableStatistics::new(columns.len()),
                    is_temporary: true,
                };

                schema_lock.create_table(&table_info)?;
                schema_lock.get_table(&self.name).ok()
            } else {
                None
            }
        };

        let Some(table_ref) = table_ref else {
            return Err(PrismDBError::Execution("Failed to create temporary table for recursive CTE".to_string()));
        };

        for iteration in 0..max_iterations {
            // Clear and repopulate the table with working_table data
            {
                let table_lock = table_ref.write().unwrap();
                let data_ref = table_lock.get_data();
                let mut data_lock = data_ref.write().unwrap();

                // Clear existing data by clearing each column
                for col in &data_lock.columns {
                    let mut col_lock = col.write().unwrap();
                    col_lock.clear();
                }
                data_lock.row_count = 0;

                // Insert working table data
                drop(data_lock); // Release lock before inserting
                for row in &working_table {
                    table_lock.insert(&row)?;
                }
            }

            // Execute recursive case
            let mut recursive_engine = ExecutionEngine::new(self.context.clone());
            let mut recursive_stream = recursive_engine.execute(*self.recursive_case.clone())?;
            let mut new_rows = Vec::new();

            while let Some(chunk_result) = recursive_stream.next() {
                let chunk = chunk_result?;
                for row_idx in 0..chunk.len() {
                    let mut row_values = Vec::new();
                    for col_idx in 0..chunk.column_count() {
                        let vector = chunk.get_vector(col_idx)
                            .ok_or_else(|| PrismDBError::Execution(format!("Missing column {}", col_idx)))?;
                        row_values.push(vector.get_value(row_idx)?);
                    }
                    let row_key = format!("{:?}", row_values);
                    if seen_rows.insert(row_key) {
                        new_rows.push(row_values);
                    }
                }
            }

            // If no new rows, we've reached fixpoint
            if new_rows.is_empty() {
                break;
            }

            // Add new results to both all_rows and working_table
            all_rows.extend(new_rows.clone());
            working_table = new_rows; // Next iteration only works with new rows

            // Safety check
            if iteration >= max_iterations - 1 {
                // Clean up temporary table
                drop(table_ref);
                let catalog_lock = self.context.catalog.write().unwrap();
                if let Ok(schema_ref) = catalog_lock.get_schema("main") {
                    let mut schema_lock = schema_ref.write().unwrap();
                    let _ = schema_lock.drop_table(&self.name);
                }
                return Err(PrismDBError::Execution(format!(
                    "Recursive CTE '{}' exceeded maximum iterations ({})",
                    self.name, max_iterations
                )));
            }
        }

        // Clean up temporary table
        drop(table_ref);
        {
            let catalog_lock = self.context.catalog.write().unwrap();
            if let Ok(schema_ref) = catalog_lock.get_schema("main") {
                let mut schema_lock = schema_ref.write().unwrap();
                let _ = schema_lock.drop_table(&self.name);
            }
        }

        // Build result chunks
        if all_rows.is_empty() {
            return Ok(Box::new(SimpleDataChunkStream::empty()));
        }

        let num_rows = all_rows.len();
        let num_cols = self.schema.len();
        let mut result_chunk = DataChunk::with_rows(num_rows);

        for col_idx in 0..num_cols {
            let mut col_values = Vec::new();
            for row in &all_rows {
                col_values.push(row[col_idx].clone());
            }
            let vector = Vector::from_values(&col_values)?;
            result_chunk.set_vector(col_idx, vector)?;
        }

        Ok(Box::new(SimpleDataChunkStream::new(vec![result_chunk])))
    }

    fn schema(&self) -> Vec<PhysicalColumn> {
        self.schema.clone()
    }
}
