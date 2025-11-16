//! PrismDB - High Performance Analytical Database
//!
//! PrismDB is a high-performance analytical database inspired by DuckDB, maintaining high fidelity to original
//! the DuckDB architecture while leveraging Rust's safety and performance features.
//!
pub mod catalog;
pub mod common;
pub mod database;
pub mod execution;
pub mod expression;
pub mod extensions;
pub mod parser;
pub mod planner;
pub mod storage;
pub mod types;

// Re-export catalog system for convenience
pub use catalog::Catalog;
// pub mod main; // Commented out to avoid binary/library conflict

// Re-export common types for convenience
pub use common::{PrismDBError, PrismDBResult};

// Re-export type system for convenience
pub use types::{
    DataChunk, LogicalType, PhysicalType, SelectionVector, TypeUtils, ValidityMask, Value, Vector,
};

// Re-export expression system for convenience
pub use expression::{
    CastExpression, ColumnRefExpression, ComparisonExpression, ConstantExpression, Expression,
    ExpressionRef, ExpressionType, FunctionExpression,
};

// Re-export storage system for convenience
pub use storage::{
    BufferManager, BufferPool, ColumnData, ColumnInfo, ColumnStatistics, IsolationLevel,
    MemoryBuffer, PageBuffer, RowId, TableData, TableInfo, TableStatistics, Transaction,
    TransactionContext, TransactionManager, TransactionMetadata, TransactionOperation,
    TransactionState, WalManager, WalRecord, WalRecordData, WalRecordType,
};

// Re-export database for convenience
pub use crate::database::{Database, DatabaseConfig, QueryResult};

// Re-export extensions for convenience
pub use extensions::{ConfigManager, ExtensionInfo, ExtensionManager, S3Config, Secret, SecretsManager};

// Re-export planner system for convenience
pub use planner::{
    plan_statement, Binder, BoxedDataChunkStream, Column, DataChunkStream, ExecutionOperator,
    LogicalAggregate, LogicalCreateTable, LogicalDelete, LogicalDropTable, LogicalExplain,
    LogicalFilter, LogicalInsert, LogicalJoin, LogicalLimit, LogicalPlan, LogicalProjection,
    LogicalSort, LogicalTableScan, LogicalUnion, LogicalUpdate, PhysicalAggregate, PhysicalColumn,
    PhysicalCreateTable, PhysicalDelete, PhysicalDropTable, PhysicalEmptyResult, PhysicalExplain,
    PhysicalFilter, PhysicalHashAggregate, PhysicalHashJoin, PhysicalInsert, PhysicalJoin,
    PhysicalLimit, PhysicalPlan, PhysicalProjection, PhysicalSort, PhysicalSortMergeJoin,
    PhysicalTableScan, PhysicalUnion, PhysicalUpdate, QueryOptimizer, QueryPlanner,
};

#[cfg(test)]
mod tests {

    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
