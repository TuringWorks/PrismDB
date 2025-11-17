//! Query Optimizer
//!
//! Transforms logical plans into optimized physical plans by applying various
//! optimization rules and choosing the best physical operators.

use crate::common::error::PrismDBResult;
use crate::expression::binder::{BinderContext, ColumnBinding, ExpressionBinder};
use crate::expression::expression::ExpressionRef;
use crate::planner::logical_plan::*;
use crate::planner::physical_plan::*;
use std::collections::HashMap;
use std::sync::Arc;

/// Query optimizer
pub struct QueryOptimizer {
    rules: Vec<Box<dyn OptimizationRule>>,
    catalog: Option<Arc<std::sync::RwLock<crate::catalog::Catalog>>>,
    transaction_manager: Option<Arc<crate::storage::transaction::TransactionManager>>,
    ctes: HashMap<String, crate::planner::logical_plan::LogicalPlan>,
}

impl QueryOptimizer {
    /// Create a new optimizer with default rules
    pub fn new() -> Self {
        let mut rules: Vec<Box<dyn OptimizationRule>> = Vec::new();

        // Add default optimization rules (order matters!)
        rules.push(Box::new(ConstantFoldingRule)); // Fold constants first
        rules.push(Box::new(FilterPushdownRule)); // Push filters down
        rules.push(Box::new(LimitPushdownRule)); // Push limits down
        rules.push(Box::new(ProjectionPushdownRule)); // Push projections down
        rules.push(Box::new(JoinOrderingRule)); // Optimize join order
        rules.push(Box::new(AggregateRule)); // Optimize aggregates

        Self {
            rules,
            catalog: None,
            transaction_manager: None,
            ctes: HashMap::new(),
        }
    }

    /// Set catalog and transaction manager for expression binding
    pub fn with_context(
        mut self,
        catalog: Arc<std::sync::RwLock<crate::catalog::Catalog>>,
        transaction_manager: Arc<crate::storage::transaction::TransactionManager>,
    ) -> Self {
        self.catalog = Some(catalog);
        self.transaction_manager = Some(transaction_manager);
        self
    }

    /// Set CTEs for subquery binding
    pub fn with_ctes(mut self, ctes: HashMap<String, crate::planner::logical_plan::LogicalPlan>) -> Self {
        self.ctes = ctes;
        self
    }

    /// Optimize a logical plan into a physical plan
    pub fn optimize(&mut self, logical_plan: LogicalPlan) -> PrismDBResult<PhysicalPlan> {
        // Apply logical optimization rules
        let mut optimized_logical = logical_plan;
        for rule in &self.rules {
            optimized_logical = rule.apply_logical(&optimized_logical)?;
        }

        // Convert to physical plan
        let physical_plan = self.convert_to_physical(optimized_logical)?;

        // Apply physical optimization rules
        let mut optimized_physical = physical_plan;
        for rule in &self.rules {
            if let Some(physical) = rule.apply_physical(&optimized_physical)? {
                optimized_physical = physical;
            }
        }

        Ok(optimized_physical)
    }

    /// Convert logical plan to physical plan
    fn convert_to_physical(&self, logical_plan: LogicalPlan) -> PrismDBResult<PhysicalPlan> {
        match logical_plan {
            LogicalPlan::TableScan(scan) => {
                let physical_schema = scan
                    .schema
                    .iter()
                    .map(|col| PhysicalColumn::new(col.name.clone(), col.data_type.clone()))
                    .collect();

                // Bind pushed-down filters
                let binder_context = Self::create_binder_context(&scan.schema);
                let binder = self.create_expression_binder(binder_context);

                let bound_filters: Result<Vec<_>, _> = scan
                    .filters
                    .iter()
                    .map(|filter| binder.bind_expression(filter))
                    .collect();
                let bound_filters = bound_filters?;

                let mut physical_scan = PhysicalTableScan::new(scan.table_name, physical_schema);
                physical_scan.filters = bound_filters;
                physical_scan.limit = scan.limit;

                Ok(PhysicalPlan::TableScan(physical_scan))
            }
            LogicalPlan::Filter(filter) => {
                // Get schema from input for binding
                let input_schema = Self::get_input_schema(&filter.input);
                let binder_context = Self::create_binder_context(&input_schema);
                let binder = self.create_expression_binder(binder_context);

                // Bind the predicate expression
                let bound_predicate = binder.bind_expression(&filter.predicate)?;

                // Convert input plan
                let input = self.convert_to_physical(*filter.input)?;
                Ok(PhysicalPlan::Filter(PhysicalFilter::new(
                    input,
                    bound_predicate,
                )))
            }
            LogicalPlan::Qualify(qualify) => {
                // Get schema from input for binding (includes window function results)
                let input_schema = Self::get_input_schema(&qualify.input);
                let binder_context = Self::create_binder_context(&input_schema);
                let binder = self.create_expression_binder(binder_context);

                // Bind the QUALIFY predicate expression
                let bound_predicate = binder.bind_expression(&qualify.predicate)?;

                // Convert input plan (window functions must be computed before QUALIFY)
                let input = self.convert_to_physical(*qualify.input)?;
                Ok(PhysicalPlan::Qualify(PhysicalQualify::new(
                    input,
                    bound_predicate,
                )))
            }
            LogicalPlan::Projection(proj) => {
                // Get schema from input for binding
                let input_schema = Self::get_input_schema(&proj.input);
                let binder_context = Self::create_binder_context(&input_schema);
                let binder = self.create_expression_binder(binder_context);

                // Bind all projection expressions
                let bound_expressions: Result<Vec<_>, _> = proj
                    .expressions
                    .iter()
                    .map(|expr| binder.bind_expression(expr))
                    .collect();
                let bound_expressions = bound_expressions?;

                // Convert input and create physical projection
                let input = self.convert_to_physical(*proj.input)?;
                let physical_schema = proj
                    .schema
                    .into_iter()
                    .map(|col| PhysicalColumn::new(col.name, col.data_type))
                    .collect();

                Ok(PhysicalPlan::Projection(PhysicalProjection::new(
                    input,
                    bound_expressions,
                    physical_schema,
                )))
            }
            LogicalPlan::Limit(limit) => {
                let input = self.convert_to_physical(*limit.input)?;
                Ok(PhysicalPlan::Limit(PhysicalLimit::new(
                    input,
                    limit.limit,
                    limit.offset,
                )))
            }
            LogicalPlan::Sort(sort) => {
                // Get schema from input for binding
                let input_schema = Self::get_input_schema(&sort.input);
                let binder_context = Self::create_binder_context(&input_schema);
                let binder = self.create_expression_binder(binder_context);

                // Bind all sort expressions
                let sort_exprs: Result<Vec<_>, _> = sort
                    .expressions
                    .into_iter()
                    .map(|expr| -> PrismDBResult<PhysicalSortExpression> {
                        let bound_expr = binder.bind_expression(&expr.expression)?;
                        Ok(PhysicalSortExpression {
                            expression: bound_expr,
                            ascending: expr.ascending,
                            nulls_first: expr.nulls_first,
                        })
                    })
                    .collect();
                let sort_exprs = sort_exprs?;

                let input = self.convert_to_physical(*sort.input)?;
                Ok(PhysicalPlan::Sort(PhysicalSort::new(input, sort_exprs)))
            }
            LogicalPlan::Aggregate(agg) => {
                // Get schema from input for binding
                let input_schema = Self::get_input_schema(&agg.input);
                let binder_context = Self::create_binder_context(&input_schema);
                let binder = self.create_expression_binder(binder_context);

                // Bind group_by expressions
                let bound_group_by: Result<Vec<_>, _> = agg
                    .group_by
                    .iter()
                    .map(|expr| binder.bind_expression(expr))
                    .collect();
                let bound_group_by = bound_group_by?;

                // Bind aggregate expressions
                let physical_aggs: Result<Vec<_>, _> = agg
                    .aggregates
                    .into_iter()
                    .map(|agg_expr| -> PrismDBResult<PhysicalAggregateExpression> {
                        let bound_args: Result<Vec<_>, _> = agg_expr
                            .arguments
                            .iter()
                            .map(|arg| binder.bind_expression(arg))
                            .collect();
                        Ok(PhysicalAggregateExpression {
                            function_name: agg_expr.function_name,
                            arguments: bound_args?,
                            distinct: agg_expr.distinct,
                            return_type: agg_expr.return_type,
                        })
                    })
                    .collect();
                let physical_aggs = physical_aggs?;

                let input = self.convert_to_physical(*agg.input)?;
                let physical_schema: Vec<PhysicalColumn> = agg
                    .schema
                    .into_iter()
                    .map(|col| PhysicalColumn::new(col.name, col.data_type))
                    .collect();

                // Choose between hash aggregate and regular aggregate
                if !bound_group_by.is_empty() {
                    Ok(PhysicalPlan::HashAggregate(PhysicalHashAggregate::new(
                        input,
                        bound_group_by,
                        physical_aggs,
                        physical_schema,
                    )))
                } else {
                    Ok(PhysicalPlan::Aggregate(PhysicalAggregate::new(
                        input,
                        bound_group_by,
                        physical_aggs,
                        physical_schema,
                    )))
                }
            }
            LogicalPlan::Join(join) => {
                let physical_join_type = match join.join_type {
                    JoinType::Inner => PhysicalJoinType::Inner,
                    JoinType::Left => PhysicalJoinType::Left,
                    JoinType::Right => PhysicalJoinType::Right,
                    JoinType::Full => PhysicalJoinType::Full,
                    JoinType::Cross => PhysicalJoinType::Cross,
                    JoinType::Semi => PhysicalJoinType::Semi,
                    JoinType::Anti => PhysicalJoinType::Anti,
                };

                // Bind condition if present
                let bound_condition = if let Some(condition) = &join.condition {
                    // Get combined schema from both sides for binding
                    let join_schema = join.schema.clone();
                    let binder_context = Self::create_binder_context(&join_schema);
                    let binder = self.create_expression_binder(binder_context);
                    Some(binder.bind_expression(condition)?)
                } else {
                    None
                };

                let left = self.convert_to_physical(*join.left)?;
                let right = self.convert_to_physical(*join.right)?;
                let physical_schema = join
                    .schema
                    .into_iter()
                    .map(|col| PhysicalColumn::new(col.name, col.data_type))
                    .collect();

                // Choose join strategy based on condition
                if let Some(ref condition) = bound_condition {
                    // Extract join keys from condition for hash join
                    let (left_keys, right_keys) = self.extract_join_keys(condition, &left, &right)?;

                    Ok(PhysicalPlan::HashJoin(PhysicalHashJoin::new(
                        left,
                        right,
                        physical_join_type,
                        left_keys,
                        right_keys,
                        bound_condition,
                        physical_schema,
                    )))
                } else {
                    Ok(PhysicalPlan::Join(PhysicalJoin::new(
                        left,
                        right,
                        physical_join_type,
                        None,
                        physical_schema,
                    )))
                }
            }
            LogicalPlan::Union(union) => {
                let left = self.convert_to_physical(*union.left)?;
                let right = self.convert_to_physical(*union.right)?;
                let physical_schema = union
                    .schema
                    .into_iter()
                    .map(|col| PhysicalColumn::new(col.name, col.data_type))
                    .collect();

                Ok(PhysicalPlan::Union(PhysicalUnion::new(
                    left,
                    right,
                    union.all,
                    physical_schema,
                )))
            }
            LogicalPlan::Intersect(intersect) => {
                let left = self.convert_to_physical(*intersect.left)?;
                let right = self.convert_to_physical(*intersect.right)?;
                let physical_schema = intersect
                    .schema
                    .into_iter()
                    .map(|col| PhysicalColumn::new(col.name, col.data_type))
                    .collect();

                Ok(PhysicalPlan::Intersect(PhysicalIntersect::new(
                    left,
                    right,
                    physical_schema,
                )))
            }
            LogicalPlan::Except(except) => {
                let left = self.convert_to_physical(*except.left)?;
                let right = self.convert_to_physical(*except.right)?;
                let physical_schema = except
                    .schema
                    .into_iter()
                    .map(|col| PhysicalColumn::new(col.name, col.data_type))
                    .collect();

                Ok(PhysicalPlan::Except(PhysicalExcept::new(
                    left,
                    right,
                    physical_schema,
                )))
            }
            LogicalPlan::Insert(insert) => {
                let input = self.convert_to_physical(*insert.input)?;
                Ok(PhysicalPlan::Insert(PhysicalInsert::new(
                    insert.table_name,
                    input,
                    insert.column_names,
                )))
            }
            LogicalPlan::Update(update) => {
                // Use the table schema from LogicalUpdate for binding
                let binder_context = Self::create_binder_context(&update.schema);
                let binder = self.create_expression_binder(binder_context);

                // Bind assignments
                let bound_assignments: HashMap<String, ExpressionRef> = update
                    .assignments
                    .into_iter()
                    .map(|(col, expr)| -> PrismDBResult<(String, ExpressionRef)> {
                        let bound_expr = binder.bind_expression(&expr)?;
                        Ok((col, bound_expr))
                    })
                    .collect::<PrismDBResult<HashMap<_, _>>>()?;

                // Bind condition if present
                let bound_condition = if let Some(condition) = &update.condition {
                    Some(binder.bind_expression(condition)?)
                } else {
                    None
                };

                Ok(PhysicalPlan::Update(PhysicalUpdate::new(
                    update.table_name,
                    bound_assignments,
                    bound_condition,
                )))
            }
            LogicalPlan::Delete(delete) => {
                // Use the table schema from LogicalDelete for binding
                let binder_context = Self::create_binder_context(&delete.schema);
                let binder = self.create_expression_binder(binder_context);

                // Bind condition if present
                let bound_condition = if let Some(condition) = &delete.condition {
                    Some(binder.bind_expression(condition)?)
                } else {
                    None
                };

                Ok(PhysicalPlan::Delete(PhysicalDelete::new(
                    delete.table_name,
                    bound_condition,
                )))
            }
            LogicalPlan::CreateTable(create) => {
                let physical_schema = create
                    .schema
                    .into_iter()
                    .map(|col| PhysicalColumn::new(col.name, col.data_type))
                    .collect();

                Ok(PhysicalPlan::CreateTable(PhysicalCreateTable::new(
                    create.table_name,
                    physical_schema,
                    create.if_not_exists,
                )))
            }
            LogicalPlan::DropTable(drop) => Ok(PhysicalPlan::DropTable(PhysicalDropTable::new(
                drop.table_name,
                drop.if_exists,
            ))),
            LogicalPlan::CreateMaterializedView(create_mv) => {
                // Convert query to physical plan
                let query = self.convert_to_physical(*create_mv.query)?;

                Ok(PhysicalPlan::CreateMaterializedView(
                    PhysicalCreateMaterializedView {
                        view_name: create_mv.view_name,
                        schema_name: None, // Use default schema
                        columns: create_mv.columns,
                        query: Box::new(query),
                        refresh_strategy: create_mv.refresh_strategy,
                        or_replace: create_mv.or_replace,
                        if_not_exists: create_mv.if_not_exists,
                    },
                ))
            }
            LogicalPlan::DropMaterializedView(drop_mv) => {
                Ok(PhysicalPlan::DropMaterializedView(
                    PhysicalDropMaterializedView {
                        view_name: drop_mv.view_name,
                        schema_name: None, // Use default schema
                        if_exists: drop_mv.if_exists,
                    },
                ))
            }
            LogicalPlan::RefreshMaterializedView(refresh_mv) => {
                // Convert query to physical plan
                let query = self.convert_to_physical(*refresh_mv.query)?;

                Ok(PhysicalPlan::RefreshMaterializedView(
                    PhysicalRefreshMaterializedView {
                        view_name: refresh_mv.view_name,
                        schema_name: None, // Use default schema
                        query: Box::new(query),
                        concurrently: refresh_mv.concurrently,
                    },
                ))
            }
            LogicalPlan::Explain(explain) => {
                let input = self.convert_to_physical(*explain.input)?;
                Ok(PhysicalPlan::Explain(PhysicalExplain::new(
                    input,
                    explain.analyze,
                    explain.verbose,
                )))
            }
            LogicalPlan::Values(values) => {
                // Bind all value expressions
                let binder_context = BinderContext {
                    alias_map: std::collections::HashMap::new(),
                    column_bindings: Vec::new(),
                    depth: 0,
                };
                let binder = self.create_expression_binder(binder_context);
                let mut bound_values = Vec::new();

                for row in values.values {
                    let mut bound_row = Vec::new();
                    for expr in row {
                        let bound_expr = binder.bind_expression(&expr)?;
                        bound_row.push(bound_expr);
                    }
                    bound_values.push(bound_row);
                }

                let physical_schema = values
                    .schema
                    .into_iter()
                    .map(|col| PhysicalColumn::new(col.name, col.data_type))
                    .collect();

                Ok(PhysicalPlan::Values(PhysicalValues::new(
                    bound_values,
                    physical_schema,
                )))
            }
            LogicalPlan::Pivot(pivot) => {
                use crate::planner::physical_plan::{PhysicalPivot, PhysicalPivotInValue, PhysicalPivotValue};

                // Get schema from input for binding
                let input_schema = Self::get_input_schema(&pivot.input);
                let binder_context = Self::create_binder_context(&input_schema);
                let binder = self.create_expression_binder(binder_context);

                // Bind ON columns (columns to pivot on)
                let bound_on_columns: Result<Vec<_>, _> = pivot
                    .on_columns
                    .iter()
                    .map(|expr| binder.bind_expression(expr))
                    .collect();
                let bound_on_columns = bound_on_columns?;

                // Bind USING values (aggregate expressions)
                let bound_using_values: PrismDBResult<Vec<_>> = pivot
                    .using_values
                    .iter()
                    .map(|v| -> PrismDBResult<PhysicalPivotValue> {
                        let bound_expr = binder.bind_expression(&v.expression)?;
                        Ok(PhysicalPivotValue {
                            expression: bound_expr,
                            alias: v.alias.clone(),
                        })
                    })
                    .collect();
                let bound_using_values = bound_using_values?;

                // Bind IN values (explicit pivot values)
                let bound_in_values = if let Some(in_vals) = &pivot.in_values {
                    let bound: PrismDBResult<Vec<_>> = in_vals
                        .iter()
                        .map(|v| -> PrismDBResult<PhysicalPivotInValue> {
                            let bound_expr = binder.bind_expression(&v.value)?;
                            Ok(PhysicalPivotInValue {
                                value: bound_expr,
                                alias: v.alias.clone(),
                            })
                        })
                        .collect();
                    Some(bound?)
                } else {
                    None
                };

                // Bind GROUP BY columns
                let bound_group_by: Result<Vec<_>, _> = pivot
                    .group_by
                    .iter()
                    .map(|expr| binder.bind_expression(expr))
                    .collect();
                let bound_group_by = bound_group_by?;

                // Convert input plan
                let input = self.convert_to_physical(*pivot.input)?;

                // Convert schema to physical schema
                let physical_schema = pivot
                    .schema
                    .into_iter()
                    .map(|col| PhysicalColumn::new(col.name, col.data_type))
                    .collect();

                Ok(PhysicalPlan::Pivot(PhysicalPivot::new(
                    input,
                    bound_on_columns,
                    bound_using_values,
                    bound_in_values,
                    bound_group_by,
                    physical_schema,
                )))
            }
            LogicalPlan::Unpivot(unpivot) => {
                use crate::planner::physical_plan::PhysicalUnpivot;

                // Get schema from input for binding
                let input_schema = Self::get_input_schema(&unpivot.input);
                let binder_context = Self::create_binder_context(&input_schema);
                let binder = self.create_expression_binder(binder_context);

                // Bind ON columns (columns to unpivot)
                let bound_on_columns: Result<Vec<_>, _> = unpivot
                    .on_columns
                    .iter()
                    .map(|expr| binder.bind_expression(expr))
                    .collect();
                let bound_on_columns = bound_on_columns?;

                // Convert input plan
                let input = self.convert_to_physical(*unpivot.input)?;

                // Convert schema to physical schema
                let physical_schema = unpivot
                    .schema
                    .into_iter()
                    .map(|col| PhysicalColumn::new(col.name, col.data_type))
                    .collect();

                Ok(PhysicalPlan::Unpivot(PhysicalUnpivot::new(
                    input,
                    bound_on_columns,
                    unpivot.name_column,
                    unpivot.value_columns,
                    unpivot.include_nulls,
                    physical_schema,
                )))
            }
            LogicalPlan::RecursiveCTE(rcte) => {
                use crate::planner::physical_plan::PhysicalRecursiveCTE;

                // Convert base case and recursive case to physical plans
                let base_case = self.convert_to_physical(*rcte.base_case)?;
                let recursive_case = self.convert_to_physical(*rcte.recursive_case)?;

                // Convert schema to physical schema
                let physical_schema = rcte
                    .schema
                    .into_iter()
                    .map(|col| PhysicalColumn::new(col.name, col.data_type))
                    .collect();

                Ok(PhysicalPlan::RecursiveCTE(PhysicalRecursiveCTE::new(
                    rcte.name,
                    base_case,
                    recursive_case,
                    physical_schema,
                )))
            }
            LogicalPlan::Empty => Ok(PhysicalPlan::EmptyResult(PhysicalEmptyResult::new(vec![]))),
        }
    }

    /// Create an expression binder with catalog/transaction context and CTEs if available
    fn create_expression_binder(&self, binder_context: BinderContext) -> ExpressionBinder {
        if let (Some(catalog), Some(txn_mgr)) = (&self.catalog, &self.transaction_manager) {
            ExpressionBinder::new_with_ctes(
                binder_context,
                catalog.clone(),
                txn_mgr.clone(),
                self.ctes.clone(),
            )
        } else {
            ExpressionBinder::new(binder_context)
        }
    }

    /// Helper method to create a BinderContext from a schema
    fn create_binder_context(schema: &[Column]) -> BinderContext {
        let mut column_bindings = Vec::new();

        for (idx, col) in schema.iter().enumerate() {
            column_bindings.push(ColumnBinding::new(
                0, // table_index - using 0 for single table context
                idx,
                col.name.clone(),
                col.data_type.clone(),
            ));
        }

        BinderContext {
            alias_map: HashMap::new(),
            column_bindings,
            depth: 0,
        }
    }

    /// Extract join keys from an equality condition for hash join
    /// Returns (left_keys, right_keys) extracted from the condition
    fn extract_join_keys(
        &self,
        condition: &ExpressionRef,
        _left_plan: &PhysicalPlan,
        _right_plan: &PhysicalPlan,
    ) -> PrismDBResult<(Vec<ExpressionRef>, Vec<ExpressionRef>)> {
        use crate::expression::{ComparisonExpression, ComparisonType};

        // For simple equality joins like "left.col = right.col"
        // Extract the column references from both sides
        if let Some(cmp_expr) = condition.as_any().downcast_ref::<ComparisonExpression>() {
            if cmp_expr.comparison_type() == &ComparisonType::Equal {
                // Found an equality - extract both sides as join keys
                let left_key = cmp_expr.left_ref().clone();
                let right_key = cmp_expr.right_ref().clone();
                return Ok((vec![left_key], vec![right_key]));
            }
        }

        // For more complex conditions (AND, OR, etc.), we would need more sophisticated extraction
        // For now, return empty keys which will cause a fallback behavior
        Ok((vec![], vec![]))
    }

    /// Get input schema from a logical plan
    fn get_input_schema(plan: &LogicalPlan) -> Vec<Column> {
        match plan {
            LogicalPlan::TableScan(scan) => scan.schema.clone(),
            LogicalPlan::Filter(filter) => Self::get_input_schema(&filter.input),
            LogicalPlan::Qualify(qualify) => Self::get_input_schema(&qualify.input),
            LogicalPlan::Projection(proj) => proj.schema.clone(),
            LogicalPlan::Limit(limit) => Self::get_input_schema(&limit.input),
            LogicalPlan::Sort(sort) => Self::get_input_schema(&sort.input),
            LogicalPlan::Aggregate(agg) => agg.schema.clone(),
            LogicalPlan::Join(join) => join.schema.clone(),
            LogicalPlan::Union(union) => Self::get_input_schema(&union.left),
            LogicalPlan::Intersect(intersect) => Self::get_input_schema(&intersect.left),
            LogicalPlan::Except(except) => Self::get_input_schema(&except.left),
            LogicalPlan::Insert(_) => vec![],
            LogicalPlan::Update(_) => vec![],
            LogicalPlan::Delete(_) => vec![],
            LogicalPlan::CreateTable(_) => vec![],
            LogicalPlan::DropTable(_) => vec![],
            LogicalPlan::CreateMaterializedView(_) => vec![],
            LogicalPlan::DropMaterializedView(_) => vec![],
            LogicalPlan::RefreshMaterializedView(_) => vec![],
            LogicalPlan::Explain(explain) => Self::get_input_schema(&explain.input),
            LogicalPlan::Values(values) => values.schema.clone(),
            LogicalPlan::Pivot(pivot) => pivot.schema.clone(),
            LogicalPlan::Unpivot(unpivot) => unpivot.schema.clone(),
            LogicalPlan::RecursiveCTE(rcte) => rcte.schema.clone(),
            LogicalPlan::Empty => vec![],
        }
    }
}

impl Default for QueryOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

/// Optimization rule trait
pub trait OptimizationRule: Send + Sync {
    /// Apply the rule to a logical plan
    fn apply_logical(&self, plan: &LogicalPlan) -> PrismDBResult<LogicalPlan>;

    /// Apply the rule to a physical plan (optional)
    fn apply_physical(&self, _plan: &PhysicalPlan) -> PrismDBResult<Option<PhysicalPlan>> {
        Ok(None)
    }
}

/// Constant folding rule - evaluate constant expressions at compile time
struct ConstantFoldingRule;

impl OptimizationRule for ConstantFoldingRule {
    fn apply_logical(&self, plan: &LogicalPlan) -> PrismDBResult<LogicalPlan> {
        use crate::parser::ast::Expression;

        // Helper function to fold constants in an expression
        fn fold_expression(expr: &Expression) -> Expression {
            match expr {
                Expression::Binary {
                    left,
                    operator,
                    right,
                } => {
                    let folded_left = fold_expression(left);
                    let folded_right = fold_expression(right);

                    // If both operands are literals, try to evaluate
                    if let (Expression::Literal(l_val), Expression::Literal(r_val)) =
                        (&folded_left, &folded_right)
                    {
                        // Try to evaluate the binary operation
                        if let Some(result) = evaluate_constant_binary(operator, l_val, r_val) {
                            return Expression::Literal(result);
                        }
                    }

                    Expression::Binary {
                        left: Box::new(folded_left),
                        operator: operator.clone(),
                        right: Box::new(folded_right),
                    }
                }
                Expression::Unary {
                    operator,
                    expression,
                } => {
                    let folded_expr = fold_expression(expression);

                    // If operand is a literal, try to evaluate
                    if let Expression::Literal(val) = &folded_expr {
                        if let Some(result) = evaluate_constant_unary(operator, val) {
                            return Expression::Literal(result);
                        }
                    }

                    Expression::Unary {
                        operator: operator.clone(),
                        expression: Box::new(folded_expr),
                    }
                }
                Expression::FunctionCall {
                    name,
                    arguments,
                    distinct,
                } => {
                    let folded_args: Vec<_> =
                        arguments.iter().map(|arg| fold_expression(arg)).collect();

                    // If all arguments are literals, try to evaluate
                    let all_literals = folded_args
                        .iter()
                        .all(|arg| matches!(arg, Expression::Literal(_)));
                    if all_literals {
                        // Could evaluate constant functions here
                        // For now, just return the folded version
                    }

                    Expression::FunctionCall {
                        name: name.clone(),
                        arguments: folded_args,
                        distinct: *distinct,
                    }
                }
                Expression::Cast {
                    expression,
                    data_type,
                } => {
                    let folded_expr = fold_expression(expression);
                    Expression::Cast {
                        expression: Box::new(folded_expr),
                        data_type: data_type.clone(),
                    }
                }
                _ => expr.clone(),
            }
        }

        // Helper to evaluate constant binary operations
        fn evaluate_constant_binary(
            operator: &crate::parser::ast::BinaryOperator,
            left: &crate::parser::ast::LiteralValue,
            right: &crate::parser::ast::LiteralValue,
        ) -> Option<crate::parser::ast::LiteralValue> {
            use crate::parser::ast::{BinaryOperator, LiteralValue};

            match (left, right) {
                (LiteralValue::Integer(l), LiteralValue::Integer(r)) => {
                    let result = match operator {
                        BinaryOperator::Add => l + r,
                        BinaryOperator::Subtract => l - r,
                        BinaryOperator::Multiply => l * r,
                        BinaryOperator::Divide => {
                            if *r != 0 {
                                l / r
                            } else {
                                return None;
                            }
                        }
                        BinaryOperator::Modulo => {
                            if *r != 0 {
                                l % r
                            } else {
                                return None;
                            }
                        }
                        _ => return None,
                    };
                    Some(LiteralValue::Integer(result))
                }
                (LiteralValue::Float(l), LiteralValue::Float(r)) => {
                    let result = match operator {
                        BinaryOperator::Add => l + r,
                        BinaryOperator::Subtract => l - r,
                        BinaryOperator::Multiply => l * r,
                        BinaryOperator::Divide => l / r,
                        _ => return None,
                    };
                    Some(LiteralValue::Float(result))
                }
                _ => None,
            }
        }

        // Helper to evaluate constant unary operations
        fn evaluate_constant_unary(
            operator: &crate::parser::ast::UnaryOperator,
            operand: &crate::parser::ast::LiteralValue,
        ) -> Option<crate::parser::ast::LiteralValue> {
            use crate::parser::ast::{LiteralValue, UnaryOperator};

            match operand {
                LiteralValue::Integer(val) => match operator {
                    UnaryOperator::Minus => Some(LiteralValue::Integer(-val)),
                    UnaryOperator::Plus => Some(LiteralValue::Integer(*val)),
                    _ => None,
                },
                LiteralValue::Float(val) => match operator {
                    UnaryOperator::Minus => Some(LiteralValue::Float(-val)),
                    UnaryOperator::Plus => Some(LiteralValue::Float(*val)),
                    _ => None,
                },
                LiteralValue::Boolean(val) => match operator {
                    UnaryOperator::Not => Some(LiteralValue::Boolean(!val)),
                    _ => None,
                },
                _ => None,
            }
        }

        // Apply constant folding to the plan
        match plan {
            LogicalPlan::Filter(filter) => {
                let folded_predicate = fold_expression(&filter.predicate);
                let folded_input = self.apply_logical(&filter.input)?;
                Ok(LogicalPlan::Filter(LogicalFilter::new(
                    folded_input,
                    folded_predicate,
                )))
            }
            LogicalPlan::Projection(proj) => {
                let folded_expressions: Vec<_> = proj
                    .expressions
                    .iter()
                    .map(|expr| fold_expression(expr))
                    .collect();
                let folded_input = self.apply_logical(&proj.input)?;
                Ok(LogicalPlan::Projection(LogicalProjection::new(
                    folded_input,
                    folded_expressions,
                    proj.schema.clone(),
                )))
            }
            _ => {
                // Apply to children
                let mut new_plan = plan.clone();
                for child in new_plan.children_mut() {
                    *child = self.apply_logical(child)?;
                }
                Ok(new_plan)
            }
        }
    }
}

/// Filter pushdown rule
struct FilterPushdownRule;

impl OptimizationRule for FilterPushdownRule {
    fn apply_logical(&self, plan: &LogicalPlan) -> PrismDBResult<LogicalPlan> {
        match plan {
            LogicalPlan::Filter(filter) => {
                // Try to push filter down through children
                let mut new_input = self.apply_logical(&filter.input)?;

                // If input is a table scan, push filter into scan
                if let LogicalPlan::TableScan(scan) = &mut new_input {
                    scan.filters.push(filter.predicate.clone());
                    Ok(new_input)
                } else {
                    // Can't push down, keep filter as is
                    Ok(LogicalPlan::Filter(LogicalFilter::new(
                        new_input,
                        filter.predicate.clone(),
                    )))
                }
            }
            _ => {
                // Apply to children
                let mut new_plan = plan.clone();
                for child in new_plan.children_mut() {
                    *child = self.apply_logical(child)?;
                }
                Ok(new_plan)
            }
        }
    }
}

/// Limit pushdown rule
struct LimitPushdownRule;

impl OptimizationRule for LimitPushdownRule {
    fn apply_logical(&self, plan: &LogicalPlan) -> PrismDBResult<LogicalPlan> {
        match plan {
            LogicalPlan::Limit(limit) => {
                // Try to push limit down through children
                let mut new_input = self.apply_logical(&limit.input)?;

                // If input is a table scan, push limit into scan
                if let LogicalPlan::TableScan(scan) = &mut new_input {
                    scan.limit = Some(limit.limit);
                    Ok(new_input)
                } else {
                    // Can't push down, keep limit as is
                    Ok(LogicalPlan::Limit(LogicalLimit::new(
                        new_input,
                        limit.limit,
                        limit.offset,
                    )))
                }
            }
            _ => {
                // Apply to children
                let mut new_plan = plan.clone();
                for child in new_plan.children_mut() {
                    *child = self.apply_logical(child)?;
                }
                Ok(new_plan)
            }
        }
    }
}

/// Projection pushdown rule - push column selection down to table scans
struct ProjectionPushdownRule;

impl OptimizationRule for ProjectionPushdownRule {
    fn apply_logical(&self, plan: &LogicalPlan) -> PrismDBResult<LogicalPlan> {
        use crate::parser::ast::Expression;
        use std::collections::HashSet;

        // Helper to extract column references from an expression
        fn extract_columns(expr: &Expression, columns: &mut HashSet<String>) {
            match expr {
                Expression::ColumnReference { column, .. } => {
                    columns.insert(column.clone());
                }
                Expression::Binary { left, right, .. } => {
                    extract_columns(left, columns);
                    extract_columns(right, columns);
                }
                Expression::Unary { expression, .. } => {
                    extract_columns(expression, columns);
                }
                Expression::FunctionCall { arguments, .. } => {
                    for arg in arguments {
                        extract_columns(arg, columns);
                    }
                }
                Expression::AggregateFunction { arguments, .. } => {
                    for arg in arguments {
                        extract_columns(arg, columns);
                    }
                }
                Expression::Case {
                    operand,
                    conditions,
                    results,
                    else_result,
                } => {
                    if let Some(op) = operand {
                        extract_columns(op, columns);
                    }
                    for cond in conditions {
                        extract_columns(cond, columns);
                    }
                    for result in results {
                        extract_columns(result, columns);
                    }
                    if let Some(else_r) = else_result {
                        extract_columns(else_r, columns);
                    }
                }
                Expression::Cast { expression, .. } => {
                    extract_columns(expression, columns);
                }
                Expression::Between {
                    expression,
                    low,
                    high,
                    ..
                } => {
                    extract_columns(expression, columns);
                    extract_columns(low, columns);
                    extract_columns(high, columns);
                }
                Expression::InList {
                    expression, list, ..
                } => {
                    extract_columns(expression, columns);
                    for item in list {
                        extract_columns(item, columns);
                    }
                }
                Expression::IsNull(expr) | Expression::IsNotNull(expr) => {
                    extract_columns(expr, columns);
                }
                _ => {}
            }
        }

        match plan {
            LogicalPlan::Projection(proj) => {
                // Collect all referenced columns
                let mut referenced_columns = HashSet::new();
                for expr in &proj.expressions {
                    extract_columns(expr, &mut referenced_columns);
                }

                // Apply to children with column information
                let new_input =
                    self.apply_logical_with_columns(&proj.input, &referenced_columns)?;

                Ok(LogicalPlan::Projection(LogicalProjection::new(
                    new_input,
                    proj.expressions.clone(),
                    proj.schema.clone(),
                )))
            }
            _ => {
                // Apply to children
                let mut new_plan = plan.clone();
                for child in new_plan.children_mut() {
                    *child = self.apply_logical(child)?;
                }
                Ok(new_plan)
            }
        }
    }
}

impl ProjectionPushdownRule {
    fn apply_logical_with_columns(
        &self,
        plan: &LogicalPlan,
        needed_columns: &std::collections::HashSet<String>,
    ) -> PrismDBResult<LogicalPlan> {
        match plan {
            LogicalPlan::TableScan(scan) => {
                // Find column IDs for needed columns
                let mut column_ids = Vec::new();
                for (idx, col) in scan.schema.iter().enumerate() {
                    if needed_columns.contains(&col.name) {
                        column_ids.push(idx);
                    }
                }

                // If we're reading all columns anyway, keep as is
                if column_ids.len() == scan.schema.len() || column_ids.is_empty() {
                    return Ok(plan.clone());
                }

                // Create new scan with pruned columns
                let mut new_scan = scan.clone();
                new_scan.column_ids = column_ids;
                Ok(LogicalPlan::TableScan(new_scan))
            }
            _ => {
                // For other nodes, just recurse
                let mut new_plan = plan.clone();
                for child in new_plan.children_mut() {
                    *child = self.apply_logical_with_columns(child, needed_columns)?;
                }
                Ok(new_plan)
            }
        }
    }
}

/// Join ordering rule
struct JoinOrderingRule;

impl OptimizationRule for JoinOrderingRule {
    fn apply_logical(&self, plan: &LogicalPlan) -> PrismDBResult<LogicalPlan> {
        // TODO: Implement join ordering optimization
        Ok(plan.clone())
    }
}

/// Aggregate optimization rule
struct AggregateRule;

impl OptimizationRule for AggregateRule {
    fn apply_logical(&self, plan: &LogicalPlan) -> PrismDBResult<LogicalPlan> {
        // TODO: Implement aggregate optimization
        Ok(plan.clone())
    }
}
