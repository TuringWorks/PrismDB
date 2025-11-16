//! Query Planner
//!
//! The main query planner that coordinates the binding and planning process
//! to transform parsed SQL statements into logical plans.

use crate::catalog::Catalog;
use crate::common::error::PrismDBResult;
use crate::parser::ast::Statement;
use crate::planner::binder::Binder;
use crate::planner::logical_plan::LogicalPlan;
use std::sync::{Arc, RwLock};

/// Main query planner
pub struct QueryPlanner {
    binder: Binder,
}

impl QueryPlanner {
    /// Create a new query planner
    pub fn new() -> Self {
        Self {
            binder: Binder::new(),
        }
    }

    /// Create a new query planner with catalog access
    pub fn new_with_catalog(catalog: Arc<RwLock<Catalog>>) -> Self {
        Self {
            binder: Binder::new_with_catalog(catalog),
        }
    }

    /// Plan a SQL statement
    pub fn plan_statement(&mut self, statement: &Statement) -> PrismDBResult<LogicalPlan> {
        // Bind the statement to resolve names and validate semantics
        let logical_plan = self.binder.bind_statement(statement)?;

        // TODO: Apply logical optimizations
        let optimized_plan = self.optimize_logical_plan(logical_plan)?;

        Ok(optimized_plan)
    }

    /// Get CTEs from the binder context (for passing to optimizer)
    pub fn get_ctes(&self) -> std::collections::HashMap<String, LogicalPlan> {
        self.binder.get_ctes()
    }

    /// Apply logical optimizations to a plan
    fn optimize_logical_plan(&self, plan: LogicalPlan) -> PrismDBResult<LogicalPlan> {
        // TODO: Implement logical optimizations
        // For now, just return the plan as-is
        Ok(plan)
    }
}

impl Default for QueryPlanner {
    fn default() -> Self {
        Self::new()
    }
}

/// Convenience function to plan a statement
pub fn plan_statement(statement: &Statement) -> PrismDBResult<LogicalPlan> {
    let mut planner = QueryPlanner::new();
    planner.plan_statement(statement)
}
