//! Execution Engine for PrismDB
//!
//! This module provides the execution engine that executes physical query plans
//! and produces results. It includes operators for various SQL operations.

pub mod context;
pub mod executor;
pub mod hash_table;
pub mod operators;
pub mod parallel;
pub mod parallel_operators;
pub mod pipeline;
pub mod pivot_utils;

pub use context::*;
pub use executor::*;
pub use hash_table::*;
pub use operators::*;
pub use parallel::*;
pub use parallel_operators::*;
pub use pipeline::*;
pub use pivot_utils::*;

use crate::common::error::{PrismDBError, PrismDBResult};
use crate::planner::{DataChunkStream, ExecutionOperator, PhysicalPlan};
use crate::types::DataChunk;

/// Execution engine that runs physical plans
pub struct ExecutionEngine {
    context: ExecutionContext,
}

impl ExecutionEngine {
    /// Create a new execution engine
    pub fn new(context: ExecutionContext) -> Self {
        Self { context }
    }

    /// Execute a physical plan and return a stream of results
    pub fn execute(&mut self, plan: PhysicalPlan) -> PrismDBResult<Box<dyn DataChunkStream>> {
        let operator = self.create_operator(plan)?;
        operator.execute()
    }

    /// Execute a physical plan and collect all results
    pub fn execute_collect(&mut self, plan: PhysicalPlan) -> PrismDBResult<Vec<DataChunk>> {
        let mut stream = self.execute(plan)?;
        let mut results = Vec::new();

        while let Some(chunk_result) = stream.next() {
            let chunk = chunk_result?;
            results.push(chunk);
        }

        Ok(results)
    }

    /// Create an execution operator from a physical plan
    fn create_operator(&self, plan: PhysicalPlan) -> PrismDBResult<Box<dyn ExecutionOperator>> {
        match plan {
            PhysicalPlan::TableScan(scan) => {
                Ok(Box::new(TableScanOperator::new(scan, self.context.clone())))
            }
            PhysicalPlan::Filter(filter) => {
                Ok(Box::new(FilterOperator::new(filter, self.context.clone())))
            }
            PhysicalPlan::Qualify(qualify) => {
                Ok(Box::new(QualifyOperator::new(qualify, self.context.clone())))
            }
            PhysicalPlan::Projection(projection) => {
                let input = *projection.input.clone();
                let _child = self.create_operator(input)?;
                Ok(Box::new(ProjectionOperator::new(
                    projection,
                    self.context.clone(),
                )))
            }
            PhysicalPlan::Limit(limit) => {
                let input = *limit.input.clone();
                let _child = self.create_operator(input)?;
                Ok(Box::new(LimitOperator::new(limit, self.context.clone())))
            }
            PhysicalPlan::Sort(sort) => {
                // Use high-performance parallel sort
                Ok(Box::new(ParallelSortOperator::new(
                    sort,
                    self.context.clone(),
                )))
            }
            PhysicalPlan::Aggregate(aggregate) => {
                // Use high-performance parallel hash aggregate
                Ok(Box::new(ParallelHashAggregateOperator::new(
                    aggregate,
                    self.context.clone(),
                )))
            }
            PhysicalPlan::HashAggregate(hash_aggregate) => {
                // Convert PhysicalHashAggregate to PhysicalAggregate for execution
                // They're functionally the same, just different optimizer representations
                let aggregate = crate::planner::PhysicalAggregate {
                    input: hash_aggregate.input.clone(),
                    group_by: hash_aggregate.group_by.clone(),
                    aggregates: hash_aggregate.aggregates.clone(),
                    schema: hash_aggregate.schema.clone(),
                };
                Ok(Box::new(ParallelHashAggregateOperator::new(
                    aggregate,
                    self.context.clone(),
                )))
            }
            PhysicalPlan::HashJoin(join) => {
                // Use high-performance parallel hash join
                Ok(Box::new(ParallelHashJoinOperator::new(
                    join,
                    self.context.clone(),
                )))
            }
            PhysicalPlan::Insert(insert) => {
                let input = *insert.input.clone();
                let _child = self.create_operator(input)?;
                Ok(Box::new(InsertOperator::new(insert, self.context.clone())))
            }
            PhysicalPlan::Update(update) => {
                Ok(Box::new(UpdateOperator::new(update, self.context.clone())))
            }
            PhysicalPlan::Delete(delete) => {
                Ok(Box::new(DeleteOperator::new(delete, self.context.clone())))
            }
            PhysicalPlan::CreateTable(create) => Ok(Box::new(CreateTableOperator::new(
                create,
                self.context.clone(),
            ))),
            PhysicalPlan::DropTable(drop) => {
                Ok(Box::new(DropTableOperator::new(drop, self.context.clone())))
            }
            PhysicalPlan::Values(values) => {
                Ok(Box::new(ValuesOperator::new(values, self.context.clone())))
            }
            PhysicalPlan::Pivot(pivot) => {
                Ok(Box::new(PivotOperator::new(pivot, self.context.clone())))
            }
            PhysicalPlan::Unpivot(unpivot) => {
                Ok(Box::new(UnpivotOperator::new(unpivot, self.context.clone())))
            }
            PhysicalPlan::Union(union) => {
                Ok(Box::new(UnionOperator::new(union, self.context.clone())))
            }
            PhysicalPlan::Intersect(intersect) => {
                Ok(Box::new(IntersectOperator::new(
                    *intersect.left.clone(),
                    *intersect.right.clone(),
                    intersect.schema.clone(),
                    self.context.clone(),
                )))
            }
            PhysicalPlan::Except(except) => {
                Ok(Box::new(ExceptOperator::new(
                    *except.left.clone(),
                    *except.right.clone(),
                    except.schema.clone(),
                    self.context.clone(),
                )))
            }
            PhysicalPlan::RecursiveCTE(rcte) => {
                Ok(Box::new(RecursiveCTEOperator::new(
                    &rcte,
                    self.context.clone(),
                )))
            }
            PhysicalPlan::IteratorStream(stream) => {
                Ok(Box::new(IteratorStreamOperator::new(stream)))
            }
            PhysicalPlan::CreateMaterializedView(create_mv) => {
                Ok(Box::new(CreateMaterializedViewOperator::new(
                    create_mv,
                    self.context.clone(),
                )))
            }
            PhysicalPlan::DropMaterializedView(drop_mv) => {
                Ok(Box::new(DropMaterializedViewOperator::new(
                    drop_mv,
                    self.context.clone(),
                )))
            }
            PhysicalPlan::RefreshMaterializedView(refresh_mv) => {
                Ok(Box::new(RefreshMaterializedViewOperator::new(
                    refresh_mv,
                    self.context.clone(),
                )))
            }
            PhysicalPlan::EmptyResult(_) => Ok(Box::new(SimpleDataChunkStream::empty())),
            _ => Err(PrismDBError::Execution(format!(
                "Unsupported physical plan: {:?}",
                plan
            ))),
        }
    }
}

/// Execution statistics
#[derive(Debug, Clone)]
pub struct ExecutionStats {
    pub rows_processed: usize,
    pub execution_time_ms: u64,
    pub memory_used_bytes: usize,
    pub operators_executed: usize,
}

impl Default for ExecutionStats {
    fn default() -> Self {
        Self {
            rows_processed: 0,
            execution_time_ms: 0,
            memory_used_bytes: 0,
            operators_executed: 0,
        }
    }
}

// Re-export ExecutionMode from context to avoid duplication
pub use context::ExecutionMode;
