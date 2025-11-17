//! Query Binder
//!
//! The binder is responsible for resolving column references, validating semantic
//! correctness, and converting the AST into a bound representation that can be
//! used for planning.

use crate::catalog::Catalog;
use crate::common::error::{PrismDBError, PrismDBResult};
use crate::parser::ast::{Expression as AstExpression, JoinType as AstJoinType, *};
use crate::planner::logical_plan::*;
use crate::types::LogicalType;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Binding context that tracks tables and columns in scope
#[derive(Debug, Clone)]
pub struct BindingContext {
    /// Table bindings: table_name -> (table_index, column_bindings)
    pub tables: HashMap<String, (usize, HashMap<String, usize>)>,
    /// Column bindings: column_name -> (table_index, column_index, column_type)
    pub columns: HashMap<String, (usize, usize, LogicalType)>,
    /// CTE bindings: cte_name -> LogicalPlan
    pub ctes: HashMap<String, LogicalPlan>,
    /// Next table index to assign
    pub next_table_index: usize,
}

impl BindingContext {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
            columns: HashMap::new(),
            ctes: HashMap::new(),
            next_table_index: 0,
        }
    }

    /// Add a table to the binding context
    pub fn add_table(&mut self, table_name: &str, schema: &[Column]) -> usize {
        let table_index = self.next_table_index;
        self.next_table_index += 1;

        let mut column_bindings = HashMap::new();
        for (col_index, column) in schema.iter().enumerate() {
            // Check if the column name is already qualified (contains '.')
            let (unqualified_name, qualified_name) = if column.name.contains('.') {
                // Column name is already qualified (e.g., "employees.department")
                // Extract the unqualified part after the last dot
                let unqualified = column.name.split('.').last().unwrap_or(&column.name);
                (unqualified.to_string(), column.name.clone())
            } else {
                // Column name is unqualified, add table prefix
                let qualified = format!("{}.{}", table_name, column.name);
                (column.name.clone(), qualified)
            };

            column_bindings.insert(unqualified_name.clone(), col_index);
            self.columns.insert(
                unqualified_name,
                (table_index, col_index, column.data_type.clone()),
            );
            self.columns.insert(
                qualified_name,
                (table_index, col_index, column.data_type.clone()),
            );
        }

        self.tables
            .insert(table_name.to_string(), (table_index, column_bindings));
        table_index
    }

    /// Resolve a column reference
    pub fn resolve_column(
        &self,
        table: Option<&str>,
        column: &str,
    ) -> PrismDBResult<(usize, usize, LogicalType)> {
        if let Some(table_name) = table {
            let qualified_name = format!("{}.{}", table_name, column);
            if let Some((table_idx, col_idx, data_type)) = self.columns.get(&qualified_name) {
                return Ok((*table_idx, *col_idx, data_type.clone()));
            }
            return Err(PrismDBError::Parse(format!(
                "Column {}.{} does not exist",
                table_name, column
            )));
        } else {
            // Try exact match first (for unqualified column names)
            if let Some((table_idx, col_idx, data_type)) = self.columns.get(column) {
                return Ok((*table_idx, *col_idx, data_type.clone()));
            }

            // If no exact match, try to match unqualified name against qualified columns
            // For example, "department" should match "employees.department"
            let mut matches = Vec::new();
            for (col_name, (table_idx, col_idx, data_type)) in &self.columns {
                // Check if this is a qualified column name (contains '.')
                if let Some(dot_pos) = col_name.rfind('.') {
                    let unqualified = &col_name[dot_pos + 1..];
                    if unqualified == column {
                        matches.push((*table_idx, *col_idx, data_type.clone()));
                    }
                }
            }

            // If we found exactly one match, return it
            if matches.len() == 1 {
                return Ok(matches[0].clone());
            } else if matches.len() > 1 {
                return Err(PrismDBError::Parse(format!(
                    "Column '{}' is ambiguous",
                    column
                )));
            }

            return Err(PrismDBError::Parse(format!(
                "Column {} does not exist",
                column
            )));
        }
    }

    /// Get table schema
    pub fn get_table_schema(&self, table_name: &str) -> Option<Vec<Column>> {
        if let Some((_, column_bindings)) = self.tables.get(table_name) {
            let mut schema = Vec::new();
            for (col_name, &_col_idx) in column_bindings {
                if let Some((_, _, data_type)) = self.columns.get(col_name) {
                    schema.push(Column::new(col_name.clone(), data_type.clone()));
                }
            }
            Some(schema)
        } else {
            None
        }
    }
}

/// Query binder that converts AST to bound expressions
pub struct Binder {
    context: BindingContext,
    catalog: Option<Arc<RwLock<Catalog>>>,
    /// Outer row values for correlated subqueries: (table_name, column_name) -> Value
    outer_row_values: std::collections::HashMap<(String, String), crate::types::Value>,
}

impl Binder {
    pub fn new() -> Self {
        Self {
            context: BindingContext::new(),
            catalog: None,
            outer_row_values: std::collections::HashMap::new(),
        }
    }

    /// Create a new binder with catalog access
    pub fn new_with_catalog(catalog: Arc<RwLock<Catalog>>) -> Self {
        Self {
            context: BindingContext::new(),
            catalog: Some(catalog),
            outer_row_values: std::collections::HashMap::new(),
        }
    }

    /// Register outer row values for correlated subquery execution
    /// This creates a temporary single-row table with the outer row's data
    pub fn register_outer_row(
        &mut self,
        table_name: &str,
        chunk: &crate::types::DataChunk,
        row_idx: usize,
    ) -> PrismDBResult<()> {
        // Get the catalog to create a temporary table
        let Some(catalog) = &self.catalog else {
            return Err(PrismDBError::Execution(
                "Cannot register outer row without catalog".to_string()
            ));
        };

        let catalog_lock = catalog.write().unwrap();
        let schema_ref = catalog_lock.get_schema("main")?;
        let mut schema_lock = schema_ref.write().unwrap();

        // Get the existing table to understand its schema
        let existing_table = schema_lock.get_table(table_name)?;
        let existing_table_lock = existing_table.read().unwrap();
        let table_info = existing_table_lock.get_table_info();

        // Create a temporary table name for this outer reference
        let temp_table_name = format!("__outer_{}", table_name);

        // Drop if exists
        let _ = schema_lock.drop_table(&temp_table_name);

        // Create new temp table with same schema
        let mut temp_table_info = table_info.clone();
        temp_table_info.name = temp_table_name.clone();
        temp_table_info.table_name = temp_table_name.clone();
        temp_table_info.is_temporary = true;

        schema_lock.create_table(&temp_table_info)?;

        // Insert the single row from the outer chunk
        if let Ok(temp_table_ref) = schema_lock.get_table(&temp_table_name) {
            let temp_table_lock = temp_table_ref.write().unwrap();

            // Extract values from the chunk for this row
            let mut row_values = Vec::new();
            for col_idx in 0..chunk.column_count() {
                if let Some(vector) = chunk.get_vector(col_idx) {
                    row_values.push(vector.get_value(row_idx)?);
                }
            }

            temp_table_lock.insert(&row_values)?;
        }

        // Register the temp table under BOTH the temp name and the original name
        // This allows the subquery to reference "d" and find the temp single-row table
        let columns: Vec<crate::planner::Column> = table_info.columns.iter().map(|col| {
            crate::planner::Column::new(col.name.clone(), col.column_type.clone())
        }).collect();

        self.context.add_table(&temp_table_name, &columns);
        // IMPORTANT: Also register under the original name so subquery finds it
        self.context.add_table(table_name, &columns);

        // Store the fact that we need to clean up this temp table later
        self.outer_row_values.insert(
            (table_name.to_string(), "__temp_table_name".to_string()),
            crate::types::Value::Varchar(temp_table_name)
        );

        Ok(())
    }

    /// Register a CTE (for use by subqueries)
    pub fn register_cte(&mut self, name: String, plan: LogicalPlan) -> PrismDBResult<()> {
        let schema = plan.schema();
        self.context.add_table(&name, &schema);
        self.context.ctes.insert(name, plan);
        Ok(())
    }

    /// Get the current CTE map (for passing to subquery expressions)
    pub fn get_ctes(&self) -> std::collections::HashMap<String, LogicalPlan> {
        self.context.ctes.clone()
    }

    /// Bind a statement to a logical plan
    pub fn bind_statement(&mut self, statement: &Statement) -> PrismDBResult<LogicalPlan> {
        match statement {
            Statement::Select(select) => self.bind_select_statement(select),
            Statement::Insert(insert) => self.bind_insert_statement(insert),
            Statement::Update(update) => self.bind_update_statement(update),
            Statement::Delete(delete) => self.bind_delete_statement(delete),
            Statement::CreateTable(create) => self.bind_create_table_statement(create),
            Statement::DropTable(drop) => self.bind_drop_table_statement(drop),
            Statement::CreateView(create_view) => self.bind_create_view_statement(create_view),
            Statement::DropView(drop_view) => self.bind_drop_view_statement(drop_view),
            Statement::RefreshMaterializedView(refresh) => self.bind_refresh_materialized_view_statement(refresh),
            Statement::Explain(explain) => self.bind_explain_statement(explain),
            _ => Err(PrismDBError::Parse(format!(
                "Statement type not yet supported: {:?}",
                statement
            ))),
        }
    }

    /// Bind a SELECT statement
    pub fn bind_select_statement(&mut self, select: &SelectStatement) -> PrismDBResult<LogicalPlan> {
        // Start with a fresh context for this query
        let _context = BindingContext::new();

        // Bind WITH clause (CTEs) first if present
        if let Some(with_clause) = &select.with_clause {
            self.bind_with_clause(with_clause)?;
        }

        // Bind FROM clause first to establish table context
        let mut plan = if let Some(from) = &select.from {
            self.bind_table_reference(from)?
        } else {
            // No FROM clause - create a single-row dummy table
            // This allows SELECT expressions like "SELECT 1+1" or "SELECT ABS(-10)" to work
            use crate::planner::logical_plan::LogicalValues;
            LogicalPlan::Values(LogicalValues::new(vec![vec![]], vec![]))
        };

        // Update context with tables from FROM clause
        // BUT: if the FROM clause references a CTE, we've already registered
        // the CTE's schema in bind_table_reference, so we should NOT recurse
        // into the CTE's internal plan (which would overwrite the CTE's column bindings)
        let is_cte_reference = if let Some(from) = &select.from {
            match from {
                TableReference::Table { name, .. } => self.context.ctes.contains_key(name),
                _ => false,
            }
        } else {
            false
        };

        if !is_cte_reference {
            self.update_context_from_plan(&plan)?;
        }

        // Bind WHERE clause
        if let Some(where_clause) = &select.where_clause {
            let predicate = self.convert_ast_expression(where_clause)?;
            plan = LogicalPlan::Filter(LogicalFilter::new(plan, predicate));
        }

        // Check if we need aggregation (GROUP BY, HAVING, or aggregates in SELECT list)
        let mut aggregates = Vec::new();

        // Extract aggregate functions from SELECT list
        for item in &select.select_list {
            let expr = match item {
                SelectItem::Expression(e) => e,
                SelectItem::Alias(e, _) => e,
                _ => continue,
            };
            self.extract_aggregates(expr, &mut aggregates)?;
        }

        // Also extract from HAVING clause if present
        if let Some(having_expr) = &select.having {
            self.extract_aggregates(having_expr, &mut aggregates)?;
        }

        // Create Aggregate node if needed
        let has_aggregates = !aggregates.is_empty();
        let has_group_by = !select.group_by.is_empty();
        let has_having = select.having.is_some();

        // Track if we created an aggregate node for SELECT list processing
        let created_aggregate = has_aggregates || has_group_by || has_having;

        if created_aggregate {
            let group_by_exprs = select
                .group_by
                .iter()
                .map(|expr| self.convert_ast_expression(expr))
                .collect::<PrismDBResult<_>>()?;

            let having = if let Some(having_expr) = &select.having {
                let converted = self.convert_ast_expression(having_expr)?;
                // Replace aggregate function calls with column references
                Some(self.replace_aggregates_with_columns(&converted, &aggregates, select.group_by.len())?)
            } else {
                None
            };

            // Build schema for aggregate node output
            // Schema = [group_by columns] + [aggregate result columns]
            let mut agg_schema = Vec::new();

            // Add GROUP BY columns to schema
            for group_expr in &select.group_by {
                let col_name = self.expression_to_string(group_expr);
                let col_type = self.infer_expression_type(group_expr)?;
                agg_schema.push(Column::new(col_name, col_type));
            }

            // Add aggregate result columns to schema
            for agg in &aggregates {
                // Generate a name for the aggregate (will be overridden by alias if present)
                let agg_name = format!("{}(...)", agg.function_name);
                agg_schema.push(Column::new(agg_name, agg.return_type.clone()));
            }

            plan = LogicalPlan::Aggregate(LogicalAggregate::new(
                plan,
                group_by_exprs,
                aggregates.clone(),
                agg_schema,
            ));

            // Apply HAVING if present
            if let Some(having_expr) = having {
                plan = LogicalPlan::Filter(LogicalFilter::new(plan, having_expr));
            }

            // Update context to include aggregate outputs
            // This allows the SELECT list to reference aggregate results
            self.update_context_from_plan(&plan)?;
        }

        // Bind SELECT list (projection)
        let mut expressions = Vec::new();
        let mut schema = Vec::new();

        for item in &select.select_list {
            match item {
                SelectItem::Expression(expr) => {
                    // If we have an aggregate node, rewrite aggregate functions to column refs
                    let bound_expr = if created_aggregate {
                        self.bind_select_expression_with_aggregates(
                            expr,
                            &select.group_by,
                            &aggregates,
                        )?
                    } else {
                        self.convert_ast_expression(expr)?
                    };
                    let data_type = self.infer_expression_type(expr)?;
                    expressions.push(bound_expr);
                    schema.push(Column::new(self.expression_to_string(expr), data_type));
                }
                SelectItem::Wildcard => {
                    // Expand wildcard to all columns
                    for column in plan.schema() {
                        let expr = crate::parser::ast::Expression::ColumnReference {
                            table: None,
                            column: column.name.clone(),
                        };
                        expressions.push(expr);
                        schema.push(column);
                    }
                }
                SelectItem::QualifiedWildcard(table) => {
                    // Expand table.* to all columns from that table
                    for column in plan.schema() {
                        // TODO: Filter by table name
                        let expr = crate::parser::ast::Expression::ColumnReference {
                            table: Some(table.clone()),
                            column: column.name.clone(),
                        };
                        expressions.push(expr);
                        schema.push(column);
                    }
                }
                SelectItem::Alias(expr, alias) => {
                    // If we have an aggregate node, rewrite aggregate functions to column refs
                    let bound_expr = if created_aggregate {
                        self.bind_select_expression_with_aggregates(
                            expr,
                            &select.group_by,
                            &aggregates,
                        )?
                    } else {
                        self.convert_ast_expression(expr)?
                    };
                    let data_type = self.infer_expression_type(expr)?;
                    expressions.push(bound_expr);
                    schema.push(Column::new(alias.clone(), data_type));
                }
            }
        }

        // Bind QUALIFY (PrismDB extension - filter on window function results)
        // QUALIFY is applied after window functions are computed, but before ORDER BY
        if let Some(qualify_expr) = &select.qualify {
            let predicate = self.convert_ast_expression(qualify_expr)?;
            plan = LogicalPlan::Qualify(LogicalQualify::new(plan, predicate));
        }

        // Bind ORDER BY
        // Note: ORDER BY can reference SELECT list aliases, so we need to replace them with the actual expressions
        if !select.order_by.is_empty() {
            let sort_exprs = select
                .order_by
                .iter()
                .map(|order_expr| {
                    // Check if this is a simple column reference that might be an alias
                    let expr = if let AstExpression::ColumnReference { table: None, column } = &order_expr.expression {
                        // Check if this matches any SELECT alias and get the corresponding expression
                        let mut found_expr: Option<&AstExpression> = None;
                        for item in &select.select_list {
                            if let SelectItem::Alias(select_expr, alias) = item {
                                if alias == column {
                                    // Found matching alias - use the SELECT expression
                                    found_expr = Some(select_expr);
                                    break;
                                }
                            }
                        }

                        if let Some(aliased_expr) = found_expr {
                            // Replace alias with the actual SELECT expression
                            // If we have an aggregate, rewrite it like we do for SELECT
                            if created_aggregate {
                                self.bind_select_expression_with_aggregates(aliased_expr, &select.group_by, &aggregates)?
                            } else {
                                self.convert_ast_expression(aliased_expr)?
                            }
                        } else {
                            // Not an alias, bind normally
                            self.convert_ast_expression(&order_expr.expression)?
                        }
                    } else {
                        // Not a simple column reference, bind normally
                        self.convert_ast_expression(&order_expr.expression)?
                    };

                    Ok(SortExpression {
                        expression: expr,
                        ascending: order_expr.ascending,
                        nulls_first: order_expr.nulls_first,
                    })
                })
                .collect::<PrismDBResult<_>>()?;

            plan = LogicalPlan::Sort(LogicalSort::new(plan, sort_exprs));
        }

        // Bind LIMIT and OFFSET
        if let Some(limit_clause) = &select.limit {
            let offset = select.offset.unwrap_or(0);
            plan = LogicalPlan::Limit(LogicalLimit::new(plan, limit_clause.limit, offset));
        }

        // Apply SELECT list (projection) last so it's the outermost operation
        plan = LogicalPlan::Projection(LogicalProjection::new(plan, expressions, schema));

        // Bind DISTINCT
        if select.distinct {
            // TODO: Implement distinct properly
        }

        // Bind set operations (UNION, INTERSECT, EXCEPT)
        if !select.set_operations.is_empty() {
            plan = self.bind_set_operations(plan, &select.set_operations)?;
        }

        Ok(plan)
    }

    /// Bind WITH clause (Common Table Expressions)
    fn bind_with_clause(&mut self, with_clause: &WithClause) -> PrismDBResult<()> {
        // For recursive CTEs, we need to infer schema from base case first
        if with_clause.recursive {
            for cte in &with_clause.ctes {
                // For recursive CTEs, the base case is typically the left side of UNION ALL
                // We need to bind it first to get the schema
                let base_case_query = if !cte.query.set_operations.is_empty() {
                    // Has set operations - assume first is base case
                    // Create a temporary SELECT with just the main query (before set ops)
                    cte.query.clone()
                } else {
                    // No set operations - use the whole query
                    cte.query.clone()
                };

                // Bind just the main SELECT (base case) to get schema
                // Clone the query and clear set operations for base case
                let mut base_query = base_case_query.clone();
                base_query.set_operations.clear();

                let base_plan = self.bind_select_statement(&base_query)?;
                let schema = base_plan.schema();

                // Register CTE with the base case schema
                self.context.add_table(&cte.name, &schema);

                // Add base plan as placeholder (so recursive part can reference the schema)
                self.context.ctes.insert(
                    cte.name.clone(),
                    base_plan,
                );
            }
        }

        // Now bind all CTEs
        for cte in &with_clause.ctes {
            let cte_plan = if with_clause.recursive && !cte.query.set_operations.is_empty() {
                // Recursive CTE with UNION ALL - split into base and recursive cases
                // Base case is the main SELECT (already bound above)
                let mut base_query = (*cte.query).clone();
                base_query.set_operations.clear();
                let base_plan = self.bind_select_statement(&base_query)?;

                // Recursive case is the right side of the UNION ALL
                if let Some(first_set_op) = cte.query.set_operations.first() {
                    let recursive_plan = self.bind_select_statement(&first_set_op.query)?;

                    let schema = base_plan.schema();
                    use crate::planner::logical_plan::LogicalRecursiveCTE;
                    LogicalPlan::RecursiveCTE(LogicalRecursiveCTE::new(
                        cte.name.clone(),
                        base_plan,
                        recursive_plan,
                        schema,
                    ))
                } else {
                    // No set operations, just treat as regular CTE
                    self.bind_select_statement(&cte.query)?
                }
            } else {
                // Regular CTE - bind the full query
                self.bind_select_statement(&cte.query)?
            };

            // Get the schema from the CTE
            let schema = cte_plan.schema();

            // Update CTE in context
            self.context.ctes.insert(cte.name.clone(), cte_plan.clone());

            // Add/update the CTE in the binding context
            self.context.add_table(&cte.name, &schema);
        }
        Ok(())
    }

    /// Bind set operations (UNION, INTERSECT, EXCEPT)
    fn bind_set_operations(
        &mut self,
        left: LogicalPlan,
        operations: &[SetOperation],
    ) -> PrismDBResult<LogicalPlan> {
        let mut result = left;

        for op in operations {
            let right = self.bind_select_statement(&op.query)?;

            // Create the appropriate set operation plan
            result = match op.op_type {
                SetOperationType::Union => {
                    LogicalPlan::Union(LogicalUnion::new(result, right, op.all))
                }
                SetOperationType::Intersect => {
                    LogicalPlan::Intersect(LogicalIntersect::new(result, right))
                }
                SetOperationType::Except => {
                    LogicalPlan::Except(LogicalExcept::new(result, right))
                }
            };
        }

        Ok(result)
    }

    /// Bind a table reference
    fn bind_table_reference(&mut self, table_ref: &TableReference) -> PrismDBResult<LogicalPlan> {
        match table_ref {
            TableReference::Table { name, alias } => {
                // First check if this is a CTE
                if let Some(cte_plan) = self.context.ctes.get(name).cloned() {
                    let cte_schema = cte_plan.schema();
                    let table_name = alias.as_ref().unwrap_or(name);

                    // Register the CTE in the binding context
                    // This allows outer queries to reference CTE columns
                    self.context.add_table(table_name, &cte_schema);

                    // Return the CTE plan as-is
                    // It's already fully bound and produces the correct output schema
                    return Ok(cte_plan);
                }

                // Determine the table name (alias takes precedence)
                let table_name = alias.as_ref().unwrap_or(name);

                // Look up table in catalog
                let schema = if let Some(catalog) = &self.catalog {
                    let catalog_guard = catalog.read().unwrap();
                    let default_schema = catalog_guard.get_default_schema();
                    let schema_guard = default_schema.read().unwrap();

                    // Try to get the table
                    match schema_guard.get_table(name) {
                        Ok(table_arc) => {
                            let table = table_arc.read().unwrap();
                            let table_info = table.get_table_info();

                            // Convert TableInfo columns to LogicalPlan Columns
                            // Qualify column names with table name/alias
                            table_info
                                .columns
                                .iter()
                                .map(|col_info| {
                                    let qualified_name = format!("{}.{}", table_name, col_info.name);
                                    Column::new(qualified_name, col_info.column_type.clone())
                                })
                                .collect()
                        }
                        Err(_) => {
                            return Err(PrismDBError::Catalog(format!(
                                "Table '{}' does not exist",
                                name
                            )));
                        }
                    }
                } else {
                    // Fallback to dummy schema if no catalog
                    // Qualify column names with table name/alias
                    vec![
                        Column::new(format!("{}.id", table_name), LogicalType::Integer),
                        Column::new(format!("{}.name", table_name), LogicalType::Text),
                    ]
                };

                self.context.add_table(table_name, &schema);

                Ok(LogicalPlan::TableScan(LogicalTableScan::new(
                    name.clone(),
                    schema,
                )))
            }
            TableReference::Join {
                left,
                join_type,
                right,
                condition,
            } => {
                let left_plan = self.bind_table_reference(left)?;
                let right_plan = self.bind_table_reference(right)?;

                let condition_expr = match condition {
                    JoinCondition::On(expr) => Some(self.convert_ast_expression(expr)?),
                    JoinCondition::Using(_columns) => {
                        // TODO: Implement USING clause
                        None
                    }
                };

                let left_schema = left_plan.schema();
                let right_schema = right_plan.schema();
                let mut schema = left_schema;
                schema.extend(right_schema);

                let logical_join_type = match join_type {
                    AstJoinType::Inner => crate::planner::logical_plan::JoinType::Inner,
                    AstJoinType::Left => crate::planner::logical_plan::JoinType::Left,
                    AstJoinType::Right => crate::planner::logical_plan::JoinType::Right,
                    AstJoinType::Full => crate::planner::logical_plan::JoinType::Full,
                    AstJoinType::Cross => crate::planner::logical_plan::JoinType::Cross,
                };

                Ok(LogicalPlan::Join(LogicalJoin::new(
                    left_plan,
                    right_plan,
                    logical_join_type,
                    condition_expr,
                    schema,
                )))
            }
            TableReference::Subquery { subquery, alias } => {
                let subplan = self.bind_select_statement(subquery)?;
                self.context.add_table(alias, &subplan.schema());
                Ok(subplan)
            }
            TableReference::Pivot {
                source,
                pivot_spec,
                alias,
            } => {
                use crate::planner::logical_plan::{LogicalPivot, PivotInValue, PivotValue};

                // Bind the source table/subquery
                let input_plan = self.bind_table_reference(source)?;
                let input_schema = input_plan.schema();

                // Convert PivotSpec from AST to logical plan structures
                let on_columns = pivot_spec.on_columns.clone();

                let using_values = pivot_spec
                    .using_values
                    .iter()
                    .map(|v| PivotValue {
                        expression: v.expression.clone(),
                        alias: v.alias.clone(),
                    })
                    .collect();

                let in_values = pivot_spec.in_values.as_ref().map(|vals| {
                    vals.iter()
                        .map(|v| PivotInValue {
                            value: v.value.clone(),
                            alias: v.alias.clone(),
                        })
                        .collect()
                });

                let group_by = pivot_spec.group_by.clone();

                // Compute output schema:
                // - GROUP BY columns (if specified)
                // - For each pivot value * aggregate: create a column
                // For now, use a simplified schema (just carry forward input columns)
                // Full schema inference would require evaluating distinct pivot values
                let mut output_schema = Vec::new();

                // Add GROUP BY columns to schema
                for group_expr in &group_by {
                    if let AstExpression::ColumnReference { table: _, column } = group_expr {
                        if let Some(col) = input_schema.iter().find(|c| &c.name == column) {
                            output_schema.push(col.clone());
                        }
                    }
                }

                // For now, add placeholder columns for pivoted data
                // Full implementation would need to evaluate IN values and create columns
                // Format: <pivot_value>_<aggregate_alias>
                if let Some(ast_in_vals) = &pivot_spec.in_values {
                    for pivot_val in ast_in_vals {
                        for agg in &pivot_spec.using_values {
                            let pivot_name = match &pivot_val.alias {
                                Some(name) => name.as_str(),
                                None => "val",
                            };
                            let col_name = if let Some(alias) = &agg.alias {
                                format!("{}_{}", pivot_name, alias)
                            } else {
                                format!("{}_agg", pivot_name)
                            };
                            output_schema.push(Column::new(col_name, LogicalType::Double));
                        }
                    }
                }

                let logical_pivot = LogicalPivot::new(
                    input_plan,
                    on_columns,
                    using_values,
                    in_values,
                    group_by,
                    output_schema,
                );

                // Register the pivot result with alias if provided
                if let Some(alias_name) = alias {
                    self.context.add_table(alias_name, &logical_pivot.schema.clone());
                }

                Ok(LogicalPlan::Pivot(logical_pivot))
            }
            TableReference::Unpivot {
                source,
                unpivot_spec,
                alias,
            } => {
                use crate::planner::logical_plan::LogicalUnpivot;

                // Bind the source table/subquery
                let input_plan = self.bind_table_reference(source)?;
                let input_schema = input_plan.schema();

                // Convert UnpivotSpec from AST to logical plan
                let on_columns = unpivot_spec.on_columns.clone();
                let name_column = unpivot_spec.name_column.clone();
                let value_columns = unpivot_spec.value_columns.clone();
                let include_nulls = unpivot_spec.include_nulls;

                // Compute output schema:
                // - Columns NOT in on_columns (these are the "identifier" columns)
                // - name_column (contains the unpivoted column names)
                // - value_column(s) (contains the values from unpivoted columns)
                let mut output_schema = Vec::new();

                // Add identifier columns (columns not being unpivoted)
                // For now, we'll try to identify which columns are NOT in on_columns
                for col in &input_schema {
                    let is_unpivot_col = on_columns.iter().any(|expr| {
                        if let AstExpression::ColumnReference { table: _, column } = expr {
                            column == &col.name
                        } else {
                            false
                        }
                    });

                    if !is_unpivot_col {
                        output_schema.push(col.clone());
                    }
                }

                // Add the name column (contains original column names)
                output_schema.push(Column::new(name_column.clone(), LogicalType::Text));

                // Add the value column(s)
                for value_col in &value_columns {
                    // Type inference: for now, assume variant type or text
                    // Full implementation would need proper type inference
                    output_schema.push(Column::new(value_col.clone(), LogicalType::Text));
                }

                let logical_unpivot = LogicalUnpivot::new(
                    input_plan,
                    on_columns,
                    name_column,
                    value_columns,
                    include_nulls,
                    output_schema,
                );

                // Register the unpivot result with alias if provided
                if let Some(alias_name) = alias {
                    self.context.add_table(alias_name, &logical_unpivot.schema.clone());
                }

                Ok(LogicalPlan::Unpivot(logical_unpivot))
            }
            TableReference::TableFunction { name, arguments: _, alias: _ } => {
                // For now, return an error - table functions need special handling
                Err(PrismDBError::NotImplemented(format!(
                    "Table function '{}' is not yet fully implemented. Table functions like read_csv_auto() require special execution handling.",
                    name
                )))
            }
        }
    }

    /// Bind an expression
    #[allow(dead_code)]
    fn bind_expression(&self, expr: &AstExpression) -> PrismDBResult<AstExpression> {
        match expr {
            AstExpression::Literal(value) => Ok(AstExpression::Literal(value.clone())),
            AstExpression::ColumnReference { table, column } => {
                let (_table_idx, _col_idx, _data_type) =
                    self.context.resolve_column(table.as_deref(), column)?;
                Ok(AstExpression::ColumnReference {
                    table: table.clone(),
                    column: column.clone(),
                })
            }
            AstExpression::Binary {
                left,
                operator,
                right,
            } => {
                let bound_left = self.bind_expression(left)?;
                let bound_right = self.bind_expression(right)?;
                Ok(AstExpression::Binary {
                    left: Box::new(bound_left),
                    operator: operator.clone(),
                    right: Box::new(bound_right),
                })
            }
            AstExpression::Unary {
                operator,
                expression,
            } => {
                let bound_expr = self.bind_expression(expression)?;
                Ok(AstExpression::Unary {
                    operator: operator.clone(),
                    expression: Box::new(bound_expr),
                })
            }
            AstExpression::FunctionCall {
                name,
                arguments,
                distinct,
            } => {
                let bound_args = arguments
                    .iter()
                    .map(|arg| self.bind_expression(arg))
                    .collect::<PrismDBResult<_>>()?;
                Ok(AstExpression::FunctionCall {
                    name: name.clone(),
                    arguments: bound_args,
                    distinct: *distinct,
                })
            }
            AstExpression::Cast {
                expression,
                data_type,
            } => {
                let bound_expr = self.bind_expression(expression)?;
                Ok(AstExpression::Cast {
                    expression: Box::new(bound_expr),
                    data_type: data_type.clone(),
                })
            }
            // TODO: Implement other expression types
            _ => Err(PrismDBError::Parse(format!(
                "Expression type not yet supported: {:?}",
                expr
            ))),
        }
    }

    /// Infer the type of an expression
    fn infer_expression_type(&self, expr: &AstExpression) -> PrismDBResult<LogicalType> {
        match expr {
            AstExpression::Literal(value) => {
                match value {
                    LiteralValue::Null => Ok(LogicalType::Text), // Use Text for NULL values for now
                    LiteralValue::Boolean(_) => Ok(LogicalType::Boolean),
                    LiteralValue::Integer(_) => Ok(LogicalType::BigInt),
                    LiteralValue::Float(_) => Ok(LogicalType::Double),
                    LiteralValue::String(_) => Ok(LogicalType::Text),
                    _ => Ok(LogicalType::Text),
                }
            }
            AstExpression::ColumnReference { table, column } => {
                // Try to resolve column type from context
                match self.context.resolve_column(table.as_deref(), column) {
                    Ok((_, _, data_type)) => Ok(data_type),
                    Err(_) => {
                        // If column still can't be resolved (shouldn't happen with improved resolve_column),
                        // use Integer as a reasonable default for backward compatibility
                        Ok(LogicalType::Integer)
                    }
                }
            }
            AstExpression::Binary {
                left,
                operator: _operator,
                right,
            } => {
                let left_type = self.infer_expression_type(left)?;
                let _right_type = self.infer_expression_type(right)?;
                // TODO: Implement proper type inference for binary operations
                Ok(left_type)
            }
            AstExpression::Unary {
                operator: _operator,
                expression,
            } => self.infer_expression_type(expression),
            AstExpression::FunctionCall {
                name: _name,
                arguments: _arguments,
                distinct: _distinct,
            } => {
                // TODO: Look up function return type
                Ok(LogicalType::Text)
            }
            AstExpression::Cast {
                expression: _expression,
                data_type,
            } => Ok(data_type.clone()),
            _ => Ok(LogicalType::Text),
        }
    }

    /// Convert AST expression to logical plan expression
    fn convert_ast_expression(
        &mut self,
        expr: &AstExpression,
    ) -> PrismDBResult<crate::parser::ast::Expression> {
        // For now, just clone the expression
        // Subquery execution during binding requires transaction manager access
        // which is not available at this stage
        // TODO: Thread transaction manager through or handle subqueries as special operators
        Ok(expr.clone())
    }


    /// Convert expression to string for column naming
    fn expression_to_string(&self, expr: &AstExpression) -> String {
        match expr {
            AstExpression::ColumnReference { table, column } => {
                if let Some(table_name) = table {
                    format!("{}.{}", table_name, column)
                } else {
                    column.clone()
                }
            }
            AstExpression::Literal(value) => {
                format!("{:?}", value)
            }
            _ => "expr".to_string(),
        }
    }

    /// Update binding context from a plan
    fn update_context_from_plan(&mut self, plan: &LogicalPlan) -> PrismDBResult<()> {
        match plan {
            LogicalPlan::TableScan(scan) => {
                // Only register the table if it hasn't been registered yet
                // This prevents CTE bindings from being overwritten
                if !self.context.tables.contains_key(&scan.table_name) {
                    self.context.add_table(&scan.table_name, &scan.schema);
                }
            }
            LogicalPlan::Join(join) => {
                self.update_context_from_plan(&join.left)?;
                self.update_context_from_plan(&join.right)?;
            }
            LogicalPlan::Projection(_proj) => {
                // Don't recurse for projections that are CTEs
                // We can detect this by checking if the projection's schema
                // has already been registered (via add_table in bind_table_reference)
                // For now, just don't recurse at all - regular projections
                // should have their input tables registered elsewhere
                // self.update_context_from_plan(&proj.input)?;
            }
            LogicalPlan::Filter(filter) => {
                self.update_context_from_plan(&filter.input)?;
            }
            LogicalPlan::Aggregate(_agg) => {
                // Don't register aggregate outputs here - they should be handled
                // by the projection that wraps them
                // self.update_context_from_plan(&agg.input)?;
            }
            LogicalPlan::Sort(sort) => {
                self.update_context_from_plan(&sort.input)?;
            }
            LogicalPlan::Limit(limit) => {
                self.update_context_from_plan(&limit.input)?;
            }
            _ => {}
        }
        Ok(())
    }

    /// Bind INSERT statement
    fn bind_insert_statement(&mut self, insert: &InsertStatement) -> PrismDBResult<LogicalPlan> {
        use crate::parser::ast::InsertSource;
        use crate::planner::logical_plan::{Column, LogicalInsert, LogicalValues};

        // Verify table exists in catalog
        if let Some(catalog) = &self.catalog {
            let catalog_guard = catalog.read().unwrap();
            let default_schema = catalog_guard.get_default_schema();
            let schema_guard = default_schema.read().unwrap();

            if schema_guard.get_table(&insert.table_name).is_err() {
                return Err(PrismDBError::Catalog(format!(
                    "Table '{}' does not exist",
                    insert.table_name
                )));
            }
        }

        // Handle different insert sources
        let input_plan = match &insert.source {
            InsertSource::Values(rows) => {
                // For VALUES clause, create a LogicalValues plan
                // Determine schema from table or use provided column names
                let schema = if let Some(catalog) = &self.catalog {
                    let catalog_guard = catalog.read().unwrap();
                    let default_schema = catalog_guard.get_default_schema();
                    let schema_guard = default_schema.read().unwrap();
                    let table_arc = schema_guard.get_table(&insert.table_name)?;
                    let table = table_arc.read().unwrap();
                    let table_info = table.get_table_info();

                    // If specific columns are provided, use only those
                    if insert.columns.is_empty() {
                        table_info
                            .columns
                            .iter()
                            .map(|col_info| {
                                Column::new(col_info.name.clone(), col_info.column_type.clone())
                            })
                            .collect()
                    } else {
                        let mut selected_columns = Vec::new();
                        for col_name in &insert.columns {
                            if let Some(col_info) =
                                table_info.columns.iter().find(|c| &c.name == col_name)
                            {
                                selected_columns.push(Column::new(
                                    col_info.name.clone(),
                                    col_info.column_type.clone(),
                                ));
                            } else {
                                return Err(PrismDBError::Catalog(format!(
                                    "Column '{}' does not exist in table '{}'",
                                    col_name, insert.table_name
                                )));
                            }
                        }
                        selected_columns
                    }
                } else {
                    // No catalog - create dummy schema
                    vec![Column::new("col0".to_string(), LogicalType::Integer)]
                };

                LogicalPlan::Values(LogicalValues::new(rows.clone(), schema))
            }
            InsertSource::Select(select) => {
                // For INSERT INTO ... SELECT, bind the SELECT statement
                self.bind_select_statement(select)?
            }
            InsertSource::DefaultValues => {
                // For DEFAULT VALUES, create an empty values list
                let schema = Vec::new();
                LogicalPlan::Values(LogicalValues::new(vec![vec![]], schema))
            }
        };

        // Create the INSERT plan
        Ok(LogicalPlan::Insert(LogicalInsert::new(
            insert.table_name.clone(),
            input_plan,
            insert.columns.clone(),
        )))
    }

    /// Bind UPDATE statement
    fn bind_update_statement(&mut self, update: &UpdateStatement) -> PrismDBResult<LogicalPlan> {
        // Verify table exists and get schema from catalog
        let table_schema = if let Some(catalog) = &self.catalog {
            let catalog_guard = catalog.read().unwrap();
            let default_schema = catalog_guard.get_default_schema();
            let schema_guard = default_schema.read().unwrap();

            let table_arc = schema_guard.get_table(&update.table_name)?;
            let table = table_arc.read().unwrap();
            let table_info = table.get_table_info();

            table_info
                .columns
                .iter()
                .map(|col_info| Column::new(col_info.name.clone(), col_info.column_type.clone()))
                .collect::<Vec<_>>()
        } else {
            return Err(PrismDBError::Catalog(format!(
                "Cannot UPDATE without catalog"
            )));
        };

        // Register table for column binding
        self.context.add_table(&update.table_name, &table_schema);

        // Convert assignments to HashMap<column_name, expression>
        let mut assignments = std::collections::HashMap::new();
        for assignment in &update.assignments {
            // Validate that the column exists in the table
            if !table_schema.iter().any(|col| col.name == assignment.column) {
                return Err(PrismDBError::Parse(format!(
                    "Column '{}' does not exist in table '{}'",
                    assignment.column, update.table_name
                )));
            }
            assignments.insert(assignment.column.clone(), assignment.value.clone());
        }

        // Bind WHERE clause if present
        let condition = if let Some(where_expr) = &update.where_clause {
            Some(where_expr.clone())
        } else {
            None
        };

        Ok(LogicalPlan::Update(LogicalUpdate::with_schema(
            update.table_name.clone(),
            assignments,
            condition,
            table_schema,
        )))
    }

    /// Bind DELETE statement
    fn bind_delete_statement(&mut self, delete: &DeleteStatement) -> PrismDBResult<LogicalPlan> {
        // Verify table exists and get schema from catalog
        let table_schema = if let Some(catalog) = &self.catalog {
            let catalog_guard = catalog.read().unwrap();
            let default_schema = catalog_guard.get_default_schema();
            let schema_guard = default_schema.read().unwrap();

            let table_arc = schema_guard.get_table(&delete.table_name)?;
            let table = table_arc.read().unwrap();
            let table_info = table.get_table_info();

            table_info
                .columns
                .iter()
                .map(|col_info| Column::new(col_info.name.clone(), col_info.column_type.clone()))
                .collect::<Vec<_>>()
        } else {
            return Err(PrismDBError::Catalog(format!(
                "Cannot DELETE without catalog"
            )));
        };

        // Register table for column binding
        self.context.add_table(&delete.table_name, &table_schema);

        // Bind WHERE clause if present
        let condition = if let Some(where_expr) = &delete.where_clause {
            Some(where_expr.clone())
        } else {
            None
        };

        Ok(LogicalPlan::Delete(LogicalDelete::with_schema(
            delete.table_name.clone(),
            condition,
            table_schema,
        )))
    }

    /// Bind CREATE TABLE statement
    fn bind_create_table_statement(
        &mut self,
        create: &CreateTableStatement,
    ) -> PrismDBResult<LogicalPlan> {
        let schema = create
            .columns
            .iter()
            .map(|col| Column::new(col.name.clone(), col.data_type.clone()))
            .collect();

        Ok(LogicalPlan::CreateTable(LogicalCreateTable::new(
            create.table_name.clone(),
            schema,
            create.if_not_exists,
        )))
    }

    /// Bind DROP TABLE statement
    fn bind_drop_table_statement(
        &mut self,
        drop: &DropTableStatement,
    ) -> PrismDBResult<LogicalPlan> {
        Ok(LogicalPlan::DropTable(LogicalDropTable::new(
            drop.table_name.clone(),
            drop.if_exists,
        )))
    }

    /// Bind CREATE [MATERIALIZED] VIEW statement
    fn bind_create_view_statement(
        &mut self,
        create_view: &crate::parser::ast::CreateViewStatement,
    ) -> PrismDBResult<LogicalPlan> {
        use crate::planner::logical_plan::{LogicalCreateMaterializedView, LogicalCreateTable};

        // Bind the query
        let query_plan = self.bind_select_statement(&create_view.query)?;

        if create_view.materialized {
            // Create materialized view
            let refresh_strategy = match &create_view.refresh_strategy {
                Some(crate::parser::ast::ViewRefreshStrategy::Manual) => "Manual",
                Some(crate::parser::ast::ViewRefreshStrategy::OnCommit) => "OnCommit",
                Some(crate::parser::ast::ViewRefreshStrategy::OnDemand) => "OnDemand",
                Some(crate::parser::ast::ViewRefreshStrategy::Incremental) => "Incremental",
                None => "Manual",
            };

            Ok(LogicalPlan::CreateMaterializedView(
                LogicalCreateMaterializedView::new(
                    create_view.view_name.clone(),
                    query_plan,
                    create_view.columns.clone(),
                    refresh_strategy.to_string(),
                    create_view.or_replace,
                    create_view.if_not_exists,
                ),
            ))
        } else {
            // Regular view - for now, we'll treat it as DDL that doesn't need execution
            // In a full implementation, we'd store the view definition in the catalog
            Ok(LogicalPlan::CreateTable(LogicalCreateTable::new(
                create_view.view_name.clone(),
                vec![],
                create_view.if_not_exists,
            )))
        }
    }

    /// Bind DROP [MATERIALIZED] VIEW statement
    fn bind_drop_view_statement(
        &mut self,
        drop_view: &crate::parser::ast::DropViewStatement,
    ) -> PrismDBResult<LogicalPlan> {
        use crate::planner::logical_plan::{LogicalDropMaterializedView, LogicalDropTable};

        if drop_view.materialized {
            Ok(LogicalPlan::DropMaterializedView(
                LogicalDropMaterializedView::new(
                    drop_view.view_name.clone(),
                    drop_view.if_exists,
                ),
            ))
        } else {
            // Regular view
            Ok(LogicalPlan::DropTable(LogicalDropTable::new(
                drop_view.view_name.clone(),
                drop_view.if_exists,
            )))
        }
    }

    /// Bind REFRESH MATERIALIZED VIEW statement
    fn bind_refresh_materialized_view_statement(
        &mut self,
        refresh: &crate::parser::ast::RefreshMaterializedViewStatement,
    ) -> PrismDBResult<LogicalPlan> {
        use crate::planner::logical_plan::{LogicalRefreshMaterializedView, LogicalTableScan};

        // For refresh, we create a simple placeholder plan
        // The actual query will be retrieved from the catalog during execution
        let placeholder_query = LogicalPlan::TableScan(LogicalTableScan {
            table_name: refresh.view_name.clone(),
            schema: vec![],
            column_ids: vec![],
            filters: vec![],
            limit: None,
        });

        Ok(LogicalPlan::RefreshMaterializedView(
            LogicalRefreshMaterializedView::new(
                refresh.view_name.clone(),
                placeholder_query,
                refresh.concurrently,
            ),
        ))
    }

    /// Bind EXPLAIN statement
    fn bind_explain_statement(&mut self, explain: &ExplainStatement) -> PrismDBResult<LogicalPlan> {
        let input_plan = self.bind_statement(&explain.statement)?;
        Ok(LogicalPlan::Explain(LogicalExplain::new(
            input_plan,
            explain.analyze,
            explain.verbose,
        )))
    }

    /// Extract aggregate functions from an AST expression
    fn extract_aggregates(
        &mut self,
        expr: &AstExpression,
        aggregates: &mut Vec<AggregateExpression>,
    ) -> PrismDBResult<()> {
        match expr {
            AstExpression::AggregateFunction {
                name,
                arguments,
                distinct,
            } => {
                // Convert arguments
                let arg_exprs: Result<Vec<_>, _> = arguments
                    .iter()
                    .map(|arg| self.convert_ast_expression(arg))
                    .collect();
                let arg_exprs = arg_exprs?;

                // Determine return type using the original AST arguments
                let arg_types: Result<Vec<_>, _> = arguments
                    .iter()
                    .map(|arg| self.infer_expression_type(arg))
                    .collect();
                let arg_types = arg_types?;
                let return_type = self.infer_aggregate_type(name, &arg_types)?;

                aggregates.push(AggregateExpression {
                    function_name: name.clone(),
                    arguments: arg_exprs,
                    distinct: *distinct,
                    return_type,
                });
            }
            // Also handle FunctionCall that are actually aggregates
            AstExpression::FunctionCall {
                name,
                arguments,
                distinct,
            } => {
                // Check if this is an aggregate function
                if Self::is_aggregate_function(name) {
                    // Convert arguments
                    let arg_exprs: Result<Vec<_>, _> = arguments
                        .iter()
                        .map(|arg| self.convert_ast_expression(arg))
                        .collect();
                    let arg_exprs = arg_exprs?;

                    // Determine return type using the original AST arguments
                    let arg_types: Result<Vec<_>, _> = arguments
                        .iter()
                        .map(|arg| self.infer_expression_type(arg))
                        .collect();
                    let arg_types = arg_types?;
                    let return_type = self.infer_aggregate_type(name, &arg_types)?;

                    aggregates.push(AggregateExpression {
                        function_name: name.clone(),
                        arguments: arg_exprs,
                        distinct: *distinct,
                        return_type,
                    });
                } else {
                    // Not an aggregate, but might contain aggregates in arguments
                    for arg in arguments {
                        self.extract_aggregates(arg, aggregates)?;
                    }
                }
            }
            // Recursively search in binary expressions
            AstExpression::Binary { left, right, .. } => {
                self.extract_aggregates(left, aggregates)?;
                self.extract_aggregates(right, aggregates)?;
            }
            // Recursively search in unary expressions
            AstExpression::Unary { expression, .. } => {
                self.extract_aggregates(expression, aggregates)?;
            }
            // Recursively search in CASE expressions
            AstExpression::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                if let Some(op) = operand {
                    self.extract_aggregates(op, aggregates)?;
                }
                for cond in conditions {
                    self.extract_aggregates(cond, aggregates)?;
                }
                for res in results {
                    self.extract_aggregates(res, aggregates)?;
                }
                if let Some(else_res) = else_result {
                    self.extract_aggregates(else_res, aggregates)?;
                }
            }
            // Other expression types don't contain aggregates
            _ => {}
        }
        Ok(())
    }

    /// Check if a function name is an aggregate function
    fn is_aggregate_function(name: &str) -> bool {
        matches!(
            name.to_uppercase().as_str(),
            "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "STDDEV" | "VARIANCE" | "STRING_AGG"
                | "MEDIAN" | "MODE" | "PERCENTILE_CONT" | "PERCENTILE_DISC"
                | "APPROX_COUNT_DISTINCT" | "APPROX_QUANTILE"
                | "FIRST" | "LAST" | "ARG_MIN" | "ARG_MAX"
                | "BOOL_AND" | "BOOL_OR"
                | "CORR" | "COVAR_POP" | "COVAR_SAMP"
                | "REGR_COUNT" | "REGR_R2"
        )
    }

    /// Replace aggregate function calls with column references to aggregated results
    fn replace_aggregates_with_columns(
        &self,
        expr: &Expression,
        aggregates: &[AggregateExpression],
        group_by_count: usize,
    ) -> PrismDBResult<Expression> {
        use crate::parser::ast::Expression as AstExpr;

        match expr {
            AstExpr::FunctionCall { name, arguments, distinct } | AstExpr::AggregateFunction { name, arguments, distinct } => {
                // Check if this is an aggregate function
                if Self::is_aggregate_function(name) {
                    // Find matching aggregate in the list
                    for (_idx, agg) in aggregates.iter().enumerate() {
                        if agg.function_name.to_uppercase() == name.to_uppercase() && agg.arguments.len() == arguments.len() {
                            // Found a match - replace with column reference
                            // Use the same naming convention as the aggregate schema
                            let agg_name = format!("{}(...)", agg.function_name);
                            return Ok(AstExpr::ColumnReference {
                                table: None,
                                column: agg_name,
                            });
                        }
                    }
                    // If not found, this might be a different aggregate - keep as is for now
                    Ok(expr.clone())
                } else {
                    // Not an aggregate - recursively process arguments
                    let new_args: Result<Vec<_>, _> = arguments
                        .iter()
                        .map(|arg| self.replace_aggregates_with_columns(arg, aggregates, group_by_count))
                        .collect();
                    let new_args = new_args?;
                    Ok(AstExpr::FunctionCall {
                        name: name.clone(),
                        arguments: new_args,
                        distinct: *distinct,
                    })
                }
            }
            AstExpr::Binary { left, operator, right } => {
                let new_left = self.replace_aggregates_with_columns(left, aggregates, group_by_count)?;
                let new_right = self.replace_aggregates_with_columns(right, aggregates, group_by_count)?;
                Ok(AstExpr::Binary {
                    left: Box::new(new_left),
                    operator: operator.clone(),
                    right: Box::new(new_right),
                })
            }
            AstExpr::Unary { operator, expression } => {
                let new_expr = self.replace_aggregates_with_columns(expression, aggregates, group_by_count)?;
                Ok(AstExpr::Unary {
                    operator: operator.clone(),
                    expression: Box::new(new_expr),
                })
            }
            _ => Ok(expr.clone()),
        }
    }

    /// Helper to infer the type of an aggregate function
    fn infer_aggregate_type(
        &self,
        function_name: &str,
        arg_types: &[LogicalType],
    ) -> PrismDBResult<LogicalType> {
        match function_name.to_uppercase().as_str() {
            "COUNT" => Ok(LogicalType::BigInt),
            "SUM" => {
                if arg_types.is_empty() {
                    Ok(LogicalType::BigInt)
                } else {
                    Ok(arg_types[0].clone())
                }
            }
            "AVG" => Ok(LogicalType::Double),
            "MIN" | "MAX" => {
                if arg_types.is_empty() {
                    Ok(LogicalType::Integer)
                } else {
                    Ok(arg_types[0].clone())
                }
            }
            _ => Ok(LogicalType::Integer), // Default
        }
    }

    /// Bind a SELECT expression when we have an Aggregate node
    /// This rewrites aggregate functions to column references pointing to Aggregate outputs
    fn bind_select_expression_with_aggregates(
        &mut self,
        expr: &AstExpression,
        group_by_exprs: &[AstExpression],
        aggregates: &[AggregateExpression],
    ) -> PrismDBResult<AstExpression> {
        match expr {
            // If it's an aggregate function, replace with column reference to Aggregate output
            AstExpression::AggregateFunction {
                name,
                arguments,
                distinct,
            } => {
                // Find this aggregate in the list
                for (idx, agg) in aggregates.iter().enumerate() {
                    // Check if this aggregate matches
                    if agg.function_name.to_uppercase() == name.to_uppercase()
                        && agg.distinct == *distinct
                        && agg.arguments.len() == arguments.len()
                    {
                        // Found a match - return column reference to aggregate output
                        // Aggregate outputs start after GROUP BY columns
                        let _column_index = group_by_exprs.len() + idx;
                        let column_name = format!("{}(...)", name);

                        return Ok(AstExpression::ColumnReference {
                            table: None,
                            column: column_name,
                        });
                    }
                }

                // If not found, this shouldn't happen - return error
                Err(PrismDBError::InvalidValue(format!(
                    "Aggregate function {} not found in aggregate list",
                    name
                )))
            }

            // If it's a column reference, check if it's a GROUP BY column
            AstExpression::ColumnReference { table, column } => {
                // Check if this column is in GROUP BY
                for (_idx, group_expr) in group_by_exprs.iter().enumerate() {
                    if let AstExpression::ColumnReference {
                        table: _,
                        column: group_col,
                    } = group_expr
                    {
                        if group_col == column {
                            // Found in GROUP BY - return column reference to GROUP BY output
                            return Ok(AstExpression::ColumnReference {
                                table: table.clone(),
                                column: column.clone(),
                            });
                        }
                    }
                }

                // Not in GROUP BY - just bind normally
                self.convert_ast_expression(expr)
            }

            // For other expressions, recursively process to replace nested aggregates
            AstExpression::FunctionCall { name, arguments, distinct } => {
                // Recursively process arguments to replace nested aggregates
                let processed_args: Result<Vec<_>, _> = arguments
                    .iter()
                    .map(|arg| self.bind_select_expression_with_aggregates(arg, group_by_exprs, aggregates))
                    .collect();
                Ok(AstExpression::FunctionCall {
                    name: name.clone(),
                    arguments: processed_args?,
                    distinct: *distinct,
                })
            }

            AstExpression::Binary { left, operator, right } => {
                let processed_left = self.bind_select_expression_with_aggregates(left, group_by_exprs, aggregates)?;
                let processed_right = self.bind_select_expression_with_aggregates(right, group_by_exprs, aggregates)?;
                Ok(AstExpression::Binary {
                    left: Box::new(processed_left),
                    operator: operator.clone(),
                    right: Box::new(processed_right),
                })
            }

            AstExpression::Unary { operator, expression } => {
                let processed_expr = self.bind_select_expression_with_aggregates(expression, group_by_exprs, aggregates)?;
                Ok(AstExpression::Unary {
                    operator: operator.clone(),
                    expression: Box::new(processed_expr),
                })
            }

            AstExpression::Case { operand, conditions, results, else_result } => {
                let processed_operand = if let Some(op) = operand {
                    Some(Box::new(self.bind_select_expression_with_aggregates(op, group_by_exprs, aggregates)?))
                } else {
                    None
                };
                let processed_conditions: Result<Vec<_>, _> = conditions
                    .iter()
                    .map(|cond| self.bind_select_expression_with_aggregates(cond, group_by_exprs, aggregates))
                    .collect();
                let processed_results: Result<Vec<_>, _> = results
                    .iter()
                    .map(|res| self.bind_select_expression_with_aggregates(res, group_by_exprs, aggregates))
                    .collect();
                let processed_else = if let Some(else_r) = else_result {
                    Some(Box::new(self.bind_select_expression_with_aggregates(else_r, group_by_exprs, aggregates)?))
                } else {
                    None
                };
                Ok(AstExpression::Case {
                    operand: processed_operand,
                    conditions: processed_conditions?,
                    results: processed_results?,
                    else_result: processed_else,
                })
            }

            AstExpression::Cast { expression, data_type } => {
                let processed_expr = self.bind_select_expression_with_aggregates(expression, group_by_exprs, aggregates)?;
                Ok(AstExpression::Cast {
                    expression: Box::new(processed_expr),
                    data_type: data_type.clone(),
                })
            }

            // For literals and other simple expressions, just return as-is
            _ => Ok(expr.clone())
        }
    }
}

impl Default for Binder {
    fn default() -> Self {
        Self::new()
    }
}
