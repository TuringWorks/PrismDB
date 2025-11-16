//! Pipeline Execution
//!
//! Implements pipeline-based execution for vectorized query processing.

use crate::common::error::{PrismDBError, PrismDBResult};
use crate::execution::context::ExecutionContext;
use crate::planner::{DataChunkStream, PhysicalPlan};
use crate::types::DataChunk;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, RwLock};

/// Pipeline executor for vectorized execution
pub struct PipelineExecutor {
    context: ExecutionContext,
    pipeline_size: usize,
    max_threads: usize,
}

impl PipelineExecutor {
    /// Create a new pipeline executor
    pub fn new(context: ExecutionContext) -> Self {
        Self {
            context,
            pipeline_size: 1024, // Default chunk size
            max_threads: num_cpus::get(),
        }
    }

    /// Set pipeline chunk size
    pub fn set_pipeline_size(&mut self, size: usize) {
        self.pipeline_size = size;
    }

    /// Set maximum number of threads
    pub fn set_max_threads(&mut self, threads: usize) {
        self.max_threads = threads;
    }

    /// Execute a physical plan using pipeline execution
    pub fn execute(&mut self, plan: PhysicalPlan) -> PrismDBResult<Box<dyn DataChunkStream>> {
        let pipeline = self.create_pipeline(plan)?;
        Ok(Box::new(PipelineStream::new(pipeline)))
    }

    /// Create a pipeline from a physical plan
    fn create_pipeline(&self, plan: PhysicalPlan) -> PrismDBResult<Pipeline> {
        match plan {
            PhysicalPlan::TableScan(scan) => {
                let source =
                    PipelineSource::TableScan(TableScanSource::new(scan, self.context.clone())?);
                let pipeline = Pipeline::new(source, self.pipeline_size);
                Ok(pipeline)
            }
            PhysicalPlan::Filter(filter) => {
                let input = *filter.input.clone();
                let child_pipeline = self.create_pipeline(input)?;
                let filter_op = FilterPipelineOperator::new(filter, self.context.clone());
                let pipeline = Pipeline::with_operator(child_pipeline, Box::new(filter_op));
                Ok(pipeline)
            }
            PhysicalPlan::Projection(projection) => {
                let input = *projection.input.clone();
                let child_pipeline = self.create_pipeline(input)?;
                let proj_op = ProjectionPipelineOperator::new(projection, self.context.clone());
                let pipeline = Pipeline::with_operator(child_pipeline, Box::new(proj_op));
                Ok(pipeline)
            }
            PhysicalPlan::Limit(limit) => {
                let input = *limit.input.clone();
                let child_pipeline = self.create_pipeline(input)?;
                let limit_op = LimitPipelineOperator::new(limit);
                let pipeline = Pipeline::with_operator(child_pipeline, Box::new(limit_op));
                Ok(pipeline)
            }
            _ => {
                // For unsupported plans, fall back to standard execution
                Err(PrismDBError::Execution(format!(
                    "Pipeline execution not supported for: {:?}",
                    plan
                )))
            }
        }
    }
}

/// Pipeline for vectorized execution
pub struct Pipeline {
    source: PipelineSource,
    operators: Vec<Box<dyn PipelineOperator>>,
    chunk_size: usize,
    buffer: VecDeque<DataChunk>,
}

impl Pipeline {
    /// Create a new pipeline with a source
    pub fn new(source: PipelineSource, chunk_size: usize) -> Self {
        Self {
            source,
            operators: Vec::new(),
            chunk_size,
            buffer: VecDeque::new(),
        }
    }

    /// Create a pipeline with a child pipeline and operator
    pub fn with_operator(child: Pipeline, operator: Box<dyn PipelineOperator>) -> Self {
        let mut pipeline = Self {
            source: child.source,
            operators: child.operators,
            chunk_size: child.chunk_size,
            buffer: VecDeque::new(),
        };
        pipeline.operators.push(operator);
        pipeline
    }

    /// Add an operator to the pipeline
    pub fn add_operator(&mut self, operator: Box<dyn PipelineOperator>) {
        self.operators.push(operator);
    }

    /// Execute the pipeline and return the next chunk
    pub fn execute_next(&mut self) -> PrismDBResult<Option<DataChunk>> {
        // Try to get from buffer first
        if let Some(chunk) = self.buffer.pop_front() {
            return Ok(Some(chunk));
        }

        // Get next chunk from source
        let chunk = match self.source.next_chunk(self.chunk_size)? {
            Some(chunk) => chunk,
            None => return Ok(None),
        };

        // Apply all operators
        let mut current_chunk = chunk;
        for operator in &self.operators {
            current_chunk = operator.process_chunk(current_chunk)?;

            // If the operator returns multiple chunks, buffer the extras
            if current_chunk.len() > self.chunk_size {
                let extra_chunks = self.split_chunk(current_chunk.clone(), self.chunk_size)?;
                if let Some(first_chunk) = extra_chunks.first() {
                    current_chunk = first_chunk.clone();
                    for extra_chunk in extra_chunks.iter().skip(1) {
                        self.buffer.push_back(extra_chunk.clone());
                    }
                }
            }
        }

        Ok(Some(current_chunk))
    }

    /// Split a chunk into smaller chunks
    fn split_chunk(&self, chunk: DataChunk, max_size: usize) -> PrismDBResult<Vec<DataChunk>> {
        let mut chunks = Vec::new();
        let mut offset = 0;

        while offset < chunk.len() {
            let size = std::cmp::min(max_size, chunk.len() - offset);
            let sub_chunk = chunk.slice_range(offset, size)?;
            chunks.push(sub_chunk);
            offset += size;
        }

        Ok(chunks)
    }
}

/// Pipeline source
pub enum PipelineSource {
    TableScan(TableScanSource),
    // Add more sources as needed
}

impl PipelineSource {
    /// Get the next chunk from the source
    pub fn next_chunk(&mut self, chunk_size: usize) -> PrismDBResult<Option<DataChunk>> {
        match self {
            PipelineSource::TableScan(scanner) => scanner.next_chunk(chunk_size),
        }
    }
}

/// Table scan source for pipeline
pub struct TableScanSource {
    #[allow(dead_code)]
    scan: crate::planner::PhysicalTableScan,
    #[allow(dead_code)]
    context: ExecutionContext,
    table_data: Option<Arc<RwLock<crate::storage::TableData>>>,
    current_offset: usize,
}

impl TableScanSource {
    pub fn new(
        scan: crate::planner::PhysicalTableScan,
        context: ExecutionContext,
    ) -> PrismDBResult<Self> {
        // Get table data from catalog using the scan's table name
        let table_data = {
            let catalog_arc = context.catalog.clone();
            let catalog = catalog_arc
                .read()
                .map_err(|_| PrismDBError::Internal("Failed to lock catalog".to_string()))?;

            // Get the schema (assuming "main" schema for now)
            let schema_arc = catalog.get_schema("main")?;

            let schema = schema_arc
                .read()
                .map_err(|_| PrismDBError::Internal("Failed to lock schema".to_string()))?;

            // Get the table
            let table_arc = schema.get_table(&scan.table_name)?;

            // Get the table data - this returns an Arc that we can keep
            let table = table_arc
                .read()
                .map_err(|_| PrismDBError::Internal("Failed to lock table".to_string()))?;
            table.get_data()
        };

        Ok(Self {
            scan,
            context,
            table_data: Some(table_data),
            current_offset: 0,
        })
    }

    pub fn next_chunk(&mut self, chunk_size: usize) -> PrismDBResult<Option<DataChunk>> {
        if self.table_data.is_none() {
            return Ok(None);
        }

        let table_data = self.table_data.as_ref().unwrap();
        let table_guard = table_data
            .read()
            .map_err(|_| PrismDBError::Internal("Failed to lock table data".to_string()))?;

        if self.current_offset >= table_guard.row_count() {
            return Ok(None);
        }

        let actual_size = std::cmp::min(chunk_size, table_guard.row_count() - self.current_offset);

        // Use the existing create_chunk method from TableData!
        let chunk = table_guard.create_chunk(self.current_offset, actual_size)?;

        self.current_offset += actual_size;
        Ok(Some(chunk))
    }
}

/// Pipeline operator trait
pub trait PipelineOperator: Send + Sync {
    /// Process a chunk and return the processed chunk
    fn process_chunk(&self, chunk: DataChunk) -> PrismDBResult<DataChunk>;
}

/// Filter pipeline operator
pub struct FilterPipelineOperator {
    filter: crate::planner::PhysicalFilter,
    context: crate::execution::ExecutionContext,
}

impl FilterPipelineOperator {
    pub fn new(filter: crate::planner::PhysicalFilter, context: crate::execution::ExecutionContext) -> Self {
        Self { filter, context }
    }
}

impl PipelineOperator for FilterPipelineOperator {
    fn process_chunk(&self, chunk: DataChunk) -> PrismDBResult<DataChunk> {
        let filter_vector = self.filter.predicate.evaluate(&chunk, &self.context)?;
        let validity = filter_vector.get_validity_mask();

        let mut keep_rows = Vec::new();
        for i in 0..chunk.len() {
            if validity.is_valid(i) {
                let value = filter_vector.get_value(i)?;
                if let crate::Value::Boolean(true) = value {
                    keep_rows.push(i);
                }
            }
        }

        if keep_rows.is_empty() {
            return Ok(DataChunk::with_rows(0));
        }

        let mut result_chunk = DataChunk::with_rows(keep_rows.len());
        for i in 0..chunk.column_count() {
            let source_vector = chunk
                .get_vector(i)
                .ok_or_else(|| PrismDBError::InvalidValue(format!("Column {} not found", i)))?;
            let mut target_vector =
                crate::types::Vector::new(source_vector.get_type().clone(), keep_rows.len());

            for (target_idx, &source_idx) in keep_rows.iter().enumerate() {
                let value = source_vector.get_value(source_idx)?;
                target_vector.set_value(target_idx, &value)?;
            }

            result_chunk.set_vector(i, target_vector)?;
        }

        Ok(result_chunk)
    }
}

/// Projection pipeline operator
pub struct ProjectionPipelineOperator {
    projection: crate::planner::PhysicalProjection,
    context: crate::execution::ExecutionContext,
}

impl ProjectionPipelineOperator {
    pub fn new(projection: crate::planner::PhysicalProjection, context: crate::execution::ExecutionContext) -> Self {
        Self { projection, context }
    }
}

impl PipelineOperator for ProjectionPipelineOperator {
    fn process_chunk(&self, chunk: DataChunk) -> PrismDBResult<DataChunk> {
        let mut result_chunk = DataChunk::with_rows(chunk.len());

        for (i, expression) in self.projection.expressions.iter().enumerate() {
            // Evaluate the expression on the input chunk
            let result_vector = expression.evaluate(&chunk, &self.context)?;
            result_chunk.set_vector(i, result_vector)?;
        }

        Ok(result_chunk)
    }
}

/// Limit pipeline operator
pub struct LimitPipelineOperator {
    limit: crate::planner::PhysicalLimit,
    rows_returned: Arc<Mutex<usize>>,
}

impl LimitPipelineOperator {
    pub fn new(limit: crate::planner::PhysicalLimit) -> Self {
        Self {
            limit,
            rows_returned: Arc::new(Mutex::new(0)),
        }
    }
}

impl PipelineOperator for LimitPipelineOperator {
    fn process_chunk(&self, chunk: DataChunk) -> PrismDBResult<DataChunk> {
        let mut rows_returned = self.rows_returned.lock().unwrap();

        if *rows_returned >= self.limit.limit {
            return Ok(DataChunk::with_rows(0));
        }

        let remaining = self.limit.limit - *rows_returned;
        if chunk.len() > remaining {
            let limited_chunk = chunk.slice_range(0, remaining)?;
            *rows_returned += remaining;
            Ok(limited_chunk)
        } else {
            *rows_returned += chunk.len();
            Ok(chunk)
        }
    }
}

/// Pipeline stream implementation
pub struct PipelineStream {
    pipeline: Arc<Mutex<Pipeline>>,
}

impl PipelineStream {
    pub fn new(pipeline: Pipeline) -> Self {
        Self {
            pipeline: Arc::new(Mutex::new(pipeline)),
        }
    }
}

impl Iterator for PipelineStream {
    type Item = PrismDBResult<DataChunk>;

    fn next(&mut self) -> Option<Self::Item> {
        self.pipeline.lock().unwrap().execute_next().transpose()
    }
}

impl DataChunkStream for PipelineStream {
    // Iterator implementation provides the required methods
}
