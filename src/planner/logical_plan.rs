//! Logical Plan Representation
//!
//! Defines the logical representation of query plans that describe what to do
//! without specifying how to do it. Logical plans are database-agnostic and
//! focus on the relational algebra operations.

use crate::parser::ast::Expression;
use crate::types::LogicalType;
use std::collections::HashMap;

/// Logical plan node types
#[derive(Debug, Clone)]
pub enum LogicalPlan {
    /// Scan data from a table
    TableScan(LogicalTableScan),
    /// Filter rows based on a predicate
    Filter(LogicalFilter),
    /// Filter rows based on window function results (QUALIFY clause)
    Qualify(LogicalQualify),
    /// Project (select) columns
    Projection(LogicalProjection),
    /// Limit the number of rows
    Limit(LogicalLimit),
    /// Sort rows
    Sort(LogicalSort),
    /// Aggregate rows
    Aggregate(LogicalAggregate),
    /// Join two relations
    Join(LogicalJoin),
    /// Union two relations
    Union(LogicalUnion),
    /// Intersect two relations
    Intersect(LogicalIntersect),
    /// Except (difference) of two relations
    Except(LogicalExcept),
    /// Insert data into a table
    Insert(LogicalInsert),
    /// Update data in a table
    Update(LogicalUpdate),
    /// Delete data from a table
    Delete(LogicalDelete),
    /// Create a table
    CreateTable(LogicalCreateTable),
    /// Drop a table
    DropTable(LogicalDropTable),
    /// Explain a plan
    Explain(LogicalExplain),
    /// Values list (constant rows)
    Values(LogicalValues),
    /// Pivot operation (rows to columns)
    Pivot(LogicalPivot),
    /// Unpivot operation (columns to rows)
    Unpivot(LogicalUnpivot),
    /// Recursive CTE with base and recursive cases
    RecursiveCTE(LogicalRecursiveCTE),
    /// Empty plan (placeholder)
    Empty,
}

impl LogicalPlan {
    /// Get the schema (output columns) of this plan node
    pub fn schema(&self) -> Vec<Column> {
        match self {
            LogicalPlan::TableScan(scan) => scan.schema.clone(),
            LogicalPlan::Filter(filter) => filter.input.schema(),
            LogicalPlan::Qualify(qualify) => qualify.input.schema(),
            LogicalPlan::Projection(proj) => proj.schema.clone(),
            LogicalPlan::Limit(limit) => limit.input.schema(),
            LogicalPlan::Sort(sort) => sort.input.schema(),
            LogicalPlan::Aggregate(agg) => agg.schema.clone(),
            LogicalPlan::Join(join) => join.schema.clone(),
            LogicalPlan::Union(union) => union.schema.clone(),
            LogicalPlan::Intersect(intersect) => intersect.schema.clone(),
            LogicalPlan::Except(except) => except.schema.clone(),
            LogicalPlan::Insert(_) => vec![],
            LogicalPlan::Update(_) => vec![],
            LogicalPlan::Delete(_) => vec![],
            LogicalPlan::CreateTable(_) => vec![],
            LogicalPlan::DropTable(_) => vec![],
            LogicalPlan::Explain(_) => vec![Column::new("plan".to_string(), LogicalType::Text)],
            LogicalPlan::Values(values) => values.schema.clone(),
            LogicalPlan::Pivot(pivot) => pivot.schema.clone(),
            LogicalPlan::Unpivot(unpivot) => unpivot.schema.clone(),
            LogicalPlan::RecursiveCTE(rcte) => rcte.schema.clone(),
            LogicalPlan::Empty => vec![],
        }
    }

    /// Get all child plans of this plan node
    pub fn children(&self) -> Vec<&LogicalPlan> {
        match self {
            LogicalPlan::TableScan(_) => vec![],
            LogicalPlan::Filter(filter) => vec![&filter.input],
            LogicalPlan::Qualify(qualify) => vec![&qualify.input],
            LogicalPlan::Projection(proj) => vec![&proj.input],
            LogicalPlan::Limit(limit) => vec![&limit.input],
            LogicalPlan::Sort(sort) => vec![&sort.input],
            LogicalPlan::Aggregate(agg) => vec![&agg.input],
            LogicalPlan::Join(join) => vec![&join.left, &join.right],
            LogicalPlan::Union(union) => vec![&union.left, &union.right],
            LogicalPlan::Intersect(intersect) => vec![&intersect.left, &intersect.right],
            LogicalPlan::Except(except) => vec![&except.left, &except.right],
            LogicalPlan::Insert(insert) => vec![&insert.input],
            LogicalPlan::Update(_) => vec![],
            LogicalPlan::Delete(_) => vec![],
            LogicalPlan::CreateTable(_) => vec![],
            LogicalPlan::DropTable(_) => vec![],
            LogicalPlan::Explain(explain) => vec![&explain.input],
            LogicalPlan::Values(_) => vec![],
            LogicalPlan::Pivot(pivot) => vec![&pivot.input],
            LogicalPlan::Unpivot(unpivot) => vec![&unpivot.input],
            LogicalPlan::RecursiveCTE(rcte) => vec![&rcte.base_case, &rcte.recursive_case],
            LogicalPlan::Empty => vec![],
        }
    }

    /// Get mutable references to child plans
    pub fn children_mut(&mut self) -> Vec<&mut LogicalPlan> {
        match self {
            LogicalPlan::TableScan(_) => vec![],
            LogicalPlan::Filter(filter) => vec![&mut filter.input],
            LogicalPlan::Qualify(qualify) => vec![&mut qualify.input],
            LogicalPlan::Projection(proj) => vec![&mut proj.input],
            LogicalPlan::Limit(limit) => vec![&mut limit.input],
            LogicalPlan::Sort(sort) => vec![&mut sort.input],
            LogicalPlan::Aggregate(agg) => vec![&mut agg.input],
            LogicalPlan::Join(join) => vec![&mut join.left, &mut join.right],
            LogicalPlan::Union(union) => vec![&mut union.left, &mut union.right],
            LogicalPlan::Intersect(intersect) => vec![&mut intersect.left, &mut intersect.right],
            LogicalPlan::Except(except) => vec![&mut except.left, &mut except.right],
            LogicalPlan::Insert(insert) => vec![&mut insert.input],
            LogicalPlan::Update(_) => vec![],
            LogicalPlan::Delete(_) => vec![],
            LogicalPlan::CreateTable(_) => vec![],
            LogicalPlan::DropTable(_) => vec![],
            LogicalPlan::Explain(explain) => vec![&mut explain.input],
            LogicalPlan::Values(_) => vec![],
            LogicalPlan::Pivot(pivot) => vec![&mut pivot.input],
            LogicalPlan::Unpivot(unpivot) => vec![&mut unpivot.input],
            LogicalPlan::RecursiveCTE(rcte) => vec![&mut rcte.base_case, &mut rcte.recursive_case],
            LogicalPlan::Empty => vec![],
        }
    }
}

/// Column definition in a schema
#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: LogicalType,
}

impl Column {
    pub fn new(name: String, data_type: LogicalType) -> Self {
        Self { name, data_type }
    }
}

/// Table scan operation
#[derive(Debug, Clone)]
pub struct LogicalTableScan {
    pub table_name: String,
    pub schema: Vec<Column>,
    pub filters: Vec<Expression>, // Pushed down filters
    pub limit: Option<usize>,     // Pushed down limit
    pub column_ids: Vec<usize>,   // Which columns to read (None means all)
}

impl LogicalTableScan {
    pub fn new(table_name: String, schema: Vec<Column>) -> Self {
        let schema_len = schema.len();
        Self {
            table_name,
            schema,
            filters: Vec::new(),
            limit: None,
            column_ids: (0..schema_len).collect(),
        }
    }
}

/// Filter operation
#[derive(Debug, Clone)]
pub struct LogicalFilter {
    pub input: Box<LogicalPlan>,
    pub predicate: Expression,
}

impl LogicalFilter {
    pub fn new(input: LogicalPlan, predicate: Expression) -> Self {
        Self {
            input: Box::new(input),
            predicate,
        }
    }
}

/// QUALIFY operation - filter rows based on window function results (DuckDB extension)
/// This is applied after window functions are computed but before ORDER BY/LIMIT
#[derive(Debug, Clone)]
pub struct LogicalQualify {
    pub input: Box<LogicalPlan>,
    pub predicate: Expression,
}

impl LogicalQualify {
    pub fn new(input: LogicalPlan, predicate: Expression) -> Self {
        Self {
            input: Box::new(input),
            predicate,
        }
    }
}

/// Projection operation
#[derive(Debug, Clone)]
pub struct LogicalProjection {
    pub input: Box<LogicalPlan>,
    pub expressions: Vec<Expression>,
    pub schema: Vec<Column>,
}

impl LogicalProjection {
    pub fn new(input: LogicalPlan, expressions: Vec<Expression>, schema: Vec<Column>) -> Self {
        Self {
            input: Box::new(input),
            expressions,
            schema,
        }
    }
}

/// Limit operation
#[derive(Debug, Clone)]
pub struct LogicalLimit {
    pub input: Box<LogicalPlan>,
    pub limit: usize,
    pub offset: usize,
}

impl LogicalLimit {
    pub fn new(input: LogicalPlan, limit: usize, offset: usize) -> Self {
        Self {
            input: Box::new(input),
            limit,
            offset,
        }
    }
}

/// Sort operation
#[derive(Debug, Clone)]
pub struct LogicalSort {
    pub input: Box<LogicalPlan>,
    pub expressions: Vec<SortExpression>,
}

#[derive(Debug, Clone)]
pub struct SortExpression {
    pub expression: Expression,
    pub ascending: bool,
    pub nulls_first: bool,
}

impl LogicalSort {
    pub fn new(input: LogicalPlan, expressions: Vec<SortExpression>) -> Self {
        Self {
            input: Box::new(input),
            expressions,
        }
    }
}

/// Aggregate operation
#[derive(Debug, Clone)]
pub struct LogicalAggregate {
    pub input: Box<LogicalPlan>,
    pub group_by: Vec<Expression>,
    pub aggregates: Vec<AggregateExpression>,
    pub schema: Vec<Column>,
}

#[derive(Debug, Clone)]
pub struct AggregateExpression {
    pub function_name: String,
    pub arguments: Vec<Expression>,
    pub distinct: bool,
    pub return_type: LogicalType,
}

impl LogicalAggregate {
    pub fn new(
        input: LogicalPlan,
        group_by: Vec<Expression>,
        aggregates: Vec<AggregateExpression>,
        schema: Vec<Column>,
    ) -> Self {
        Self {
            input: Box::new(input),
            group_by,
            aggregates,
            schema,
        }
    }
}

/// Join operation
#[derive(Debug, Clone)]
pub struct LogicalJoin {
    pub left: Box<LogicalPlan>,
    pub right: Box<LogicalPlan>,
    pub join_type: JoinType,
    pub condition: Option<Expression>,
    pub schema: Vec<Column>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
    Semi,
    Anti,
}

impl LogicalJoin {
    pub fn new(
        left: LogicalPlan,
        right: LogicalPlan,
        join_type: JoinType,
        condition: Option<Expression>,
        schema: Vec<Column>,
    ) -> Self {
        Self {
            left: Box::new(left),
            right: Box::new(right),
            join_type,
            condition,
            schema,
        }
    }
}

/// Union operation
#[derive(Debug, Clone)]
pub struct LogicalUnion {
    pub left: Box<LogicalPlan>,
    pub right: Box<LogicalPlan>,
    pub all: bool,  // true for UNION ALL, false for UNION DISTINCT
    pub schema: Vec<Column>,
}

impl LogicalUnion {
    pub fn new(left: LogicalPlan, right: LogicalPlan, all: bool) -> Self {
        let schema = left.schema();  // Use left schema (schemas must match for UNION)
        Self {
            left: Box::new(left),
            right: Box::new(right),
            all,
            schema,
        }
    }
}

/// Intersect operation (returns rows in both left and right)
#[derive(Debug, Clone)]
pub struct LogicalIntersect {
    pub left: Box<LogicalPlan>,
    pub right: Box<LogicalPlan>,
    pub schema: Vec<Column>,
}

impl LogicalIntersect {
    pub fn new(left: LogicalPlan, right: LogicalPlan) -> Self {
        let schema = left.schema();
        Self {
            left: Box::new(left),
            right: Box::new(right),
            schema,
        }
    }
}

/// Except operation (returns rows in left but not in right)
#[derive(Debug, Clone)]
pub struct LogicalExcept {
    pub left: Box<LogicalPlan>,
    pub right: Box<LogicalPlan>,
    pub schema: Vec<Column>,
}

impl LogicalExcept {
    pub fn new(left: LogicalPlan, right: LogicalPlan) -> Self {
        let schema = left.schema();
        Self {
            left: Box::new(left),
            right: Box::new(right),
            schema,
        }
    }
}

/// Insert operation
#[derive(Debug, Clone)]
pub struct LogicalInsert {
    pub table_name: String,
    pub input: Box<LogicalPlan>,
    pub column_names: Vec<String>,
}

impl LogicalInsert {
    pub fn new(table_name: String, input: LogicalPlan, column_names: Vec<String>) -> Self {
        Self {
            table_name,
            input: Box::new(input),
            column_names,
        }
    }
}

/// Update operation
#[derive(Debug, Clone)]
pub struct LogicalUpdate {
    pub table_name: String,
    pub assignments: HashMap<String, Expression>,
    pub condition: Option<Expression>,
    pub schema: Vec<Column>,  // Table schema for expression binding
}

impl LogicalUpdate {
    pub fn new(
        table_name: String,
        assignments: HashMap<String, Expression>,
        condition: Option<Expression>,
    ) -> Self {
        Self {
            table_name,
            assignments,
            condition,
            schema: Vec::new(),  // Will be set by binder
        }
    }

    pub fn with_schema(
        table_name: String,
        assignments: HashMap<String, Expression>,
        condition: Option<Expression>,
        schema: Vec<Column>,
    ) -> Self {
        Self {
            table_name,
            assignments,
            condition,
            schema,
        }
    }
}

/// Delete operation
#[derive(Debug, Clone)]
pub struct LogicalDelete {
    pub table_name: String,
    pub condition: Option<Expression>,
    pub schema: Vec<Column>,  // Table schema for expression binding
}

impl LogicalDelete {
    pub fn new(table_name: String, condition: Option<Expression>) -> Self {
        Self {
            table_name,
            condition,
            schema: Vec::new(),
        }
    }

    pub fn with_schema(
        table_name: String,
        condition: Option<Expression>,
        schema: Vec<Column>,
    ) -> Self {
        Self {
            table_name,
            condition,
            schema,
        }
    }
}

/// Create table operation
#[derive(Debug, Clone)]
pub struct LogicalCreateTable {
    pub table_name: String,
    pub schema: Vec<Column>,
    pub if_not_exists: bool,
}

impl LogicalCreateTable {
    pub fn new(table_name: String, schema: Vec<Column>, if_not_exists: bool) -> Self {
        Self {
            table_name,
            schema,
            if_not_exists,
        }
    }
}

/// Drop table operation
#[derive(Debug, Clone)]
pub struct LogicalDropTable {
    pub table_name: String,
    pub if_exists: bool,
}

impl LogicalDropTable {
    pub fn new(table_name: String, if_exists: bool) -> Self {
        Self {
            table_name,
            if_exists,
        }
    }
}

/// Explain operation
#[derive(Debug, Clone)]
pub struct LogicalExplain {
    pub input: Box<LogicalPlan>,
    pub analyze: bool,
    pub verbose: bool,
}

impl LogicalExplain {
    pub fn new(input: LogicalPlan, analyze: bool, verbose: bool) -> Self {
        Self {
            input: Box::new(input),
            analyze,
            verbose,
        }
    }
}

/// Values operation (produces constant rows)
#[derive(Debug, Clone)]
pub struct LogicalValues {
    /// List of rows, where each row is a list of expressions
    pub values: Vec<Vec<Expression>>,
    /// Schema of the values
    pub schema: Vec<Column>,
}

impl LogicalValues {
    pub fn new(values: Vec<Vec<Expression>>, schema: Vec<Column>) -> Self {
        Self { values, schema }
    }
}

/// PIVOT operation - transforms rows to columns
#[derive(Debug, Clone)]
pub struct LogicalPivot {
    pub input: Box<LogicalPlan>,
    /// Columns to pivot (create new columns for each distinct value)
    pub on_columns: Vec<Expression>,
    /// Aggregate expressions to compute for each pivot column
    pub using_values: Vec<PivotValue>,
    /// Optional: explicitly list which pivot values to create columns for
    pub in_values: Option<Vec<PivotInValue>>,
    /// Columns to group by (determines output rows)
    pub group_by: Vec<Expression>,
    /// Output schema (computed based on pivot values and aggregates)
    pub schema: Vec<Column>,
}

/// PIVOT value specification (aggregate expression with optional alias)
#[derive(Debug, Clone)]
pub struct PivotValue {
    pub expression: Expression,
    pub alias: Option<String>,
}

/// PIVOT IN value (explicit column value specification)
#[derive(Debug, Clone)]
pub struct PivotInValue {
    pub value: Expression,
    pub alias: Option<String>,
}

impl LogicalPivot {
    pub fn new(
        input: LogicalPlan,
        on_columns: Vec<Expression>,
        using_values: Vec<PivotValue>,
        in_values: Option<Vec<PivotInValue>>,
        group_by: Vec<Expression>,
        schema: Vec<Column>,
    ) -> Self {
        Self {
            input: Box::new(input),
            on_columns,
            using_values,
            in_values,
            group_by,
            schema,
        }
    }
}

/// UNPIVOT operation - transforms columns to rows
#[derive(Debug, Clone)]
pub struct LogicalUnpivot {
    pub input: Box<LogicalPlan>,
    /// Columns to unpivot (stack into rows)
    pub on_columns: Vec<Expression>,
    /// Column name for the "name" column (contains original column names)
    pub name_column: String,
    /// Column name(s) for the "value" column(s)
    pub value_columns: Vec<String>,
    /// Whether to include NULL values
    pub include_nulls: bool,
    /// Output schema (group-by columns + name column + value columns)
    pub schema: Vec<Column>,
}

impl LogicalUnpivot {
    pub fn new(
        input: LogicalPlan,
        on_columns: Vec<Expression>,
        name_column: String,
        value_columns: Vec<String>,
        include_nulls: bool,
        schema: Vec<Column>,
    ) -> Self {
        Self {
            input: Box::new(input),
            on_columns,
            name_column,
            value_columns,
            include_nulls,
            schema,
        }
    }
}

/// Recursive CTE logical plan
#[derive(Debug, Clone)]
pub struct LogicalRecursiveCTE {
    /// CTE name (for debugging)
    pub name: String,
    /// Base case (non-recursive part, typically before first UNION ALL)
    pub base_case: Box<LogicalPlan>,
    /// Recursive case (references the CTE itself)
    pub recursive_case: Box<LogicalPlan>,
    /// Output schema
    pub schema: Vec<Column>,
}

impl LogicalRecursiveCTE {
    pub fn new(
        name: String,
        base_case: LogicalPlan,
        recursive_case: LogicalPlan,
        schema: Vec<Column>,
    ) -> Self {
        Self {
            name,
            base_case: Box::new(base_case),
            recursive_case: Box::new(recursive_case),
            schema,
        }
    }
}
