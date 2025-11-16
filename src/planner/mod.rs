//! Query Planner for DuckDB
//!
//! This module provides query planning capabilities that transform parsed SQL
//! statements into executable plans. The planner consists of several phases:
//!
//! 1. **Binding**: Resolves column references and validates semantic correctness
//! 2. **Logical Planning**: Creates a logical representation of the query
//! 3. **Optimization**: Transforms logical plan into an optimized physical plan
//! 4. **Execution**: Generates executable operators

pub mod binder;
pub mod logical_plan;
pub mod optimizer;
pub mod physical_plan;
pub mod planner;

#[cfg(test)]
mod tests;

pub use binder::*;
pub use logical_plan::*;
pub use optimizer::*;
pub use physical_plan::*;
pub use planner::*;
