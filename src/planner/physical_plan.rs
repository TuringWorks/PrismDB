//! Physical Plan Representation
//!
//! Defines the physical representation of query plans that describe how to execute
//! queries. Physical plans contain specific operators and execution strategies.

use crate::common::error::PrismDBResult;
use crate::expression::expression::ExpressionRef;
use crate::types::{DataChunk, LogicalType};
use std::collections::HashMap;

/// Physical plan node types
#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    /// Scan data from a table
    TableScan(PhysicalTableScan),
    /// Filter rows based on a predicate
    Filter(PhysicalFilter),
    /// Filter rows based on window function results (QUALIFY clause)
    Qualify(PhysicalQualify),
    /// Project (select) columns
    Projection(PhysicalProjection),
    /// Limit the number of rows
    Limit(PhysicalLimit),
    /// Sort rows
    Sort(PhysicalSort),
    /// Aggregate rows
    Aggregate(PhysicalAggregate),
    /// Join two relations
    Join(PhysicalJoin),
    /// Union two relations
    Union(PhysicalUnion),
    /// Intersect two relations
    Intersect(PhysicalIntersect),
    /// Except (difference) of two relations
    Except(PhysicalExcept),
    /// Hash aggregate
    HashAggregate(PhysicalHashAggregate),
    /// Sort merge join
    SortMergeJoin(PhysicalSortMergeJoin),
    /// Hash join
    HashJoin(PhysicalHashJoin),
    /// Insert data into a table
    Insert(PhysicalInsert),
    /// Update data in a table
    Update(PhysicalUpdate),
    /// Delete data from a table
    Delete(PhysicalDelete),
    /// Create a table
    CreateTable(PhysicalCreateTable),
    /// Drop a table
    DropTable(PhysicalDropTable),
    /// Explain a plan
    Explain(PhysicalExplain),
    /// Values list (constant rows)
    Values(PhysicalValues),
    /// Pivot operation (rows to columns)
    Pivot(PhysicalPivot),
    /// Unpivot operation (columns to rows)
    Unpivot(PhysicalUnpivot),
    /// Recursive CTE with fixpoint iteration
    RecursiveCTE(PhysicalRecursiveCTE),
    /// Empty result
    EmptyResult(PhysicalEmptyResult),
}

impl PhysicalPlan {
    /// Get the schema (output columns) of this plan node
    pub fn schema(&self) -> Vec<PhysicalColumn> {
        match self {
            PhysicalPlan::TableScan(scan) => scan.schema.clone(),
            PhysicalPlan::Filter(filter) => filter.input.schema(),
            PhysicalPlan::Qualify(qualify) => qualify.input.schema(),
            PhysicalPlan::Projection(proj) => proj.schema.clone(),
            PhysicalPlan::Limit(limit) => limit.input.schema(),
            PhysicalPlan::Sort(sort) => sort.input.schema(),
            PhysicalPlan::Aggregate(agg) => agg.schema.clone(),
            PhysicalPlan::Join(join) => join.schema.clone(),
            PhysicalPlan::Union(union) => union.schema.clone(),
            PhysicalPlan::Intersect(intersect) => intersect.schema.clone(),
            PhysicalPlan::Except(except) => except.schema.clone(),
            PhysicalPlan::HashAggregate(agg) => agg.schema.clone(),
            PhysicalPlan::SortMergeJoin(join) => join.schema.clone(),
            PhysicalPlan::HashJoin(join) => join.schema.clone(),
            PhysicalPlan::Insert(_) => vec![],
            PhysicalPlan::Update(_) => vec![],
            PhysicalPlan::Delete(_) => vec![],
            PhysicalPlan::CreateTable(_) => vec![],
            PhysicalPlan::DropTable(_) => vec![],
            PhysicalPlan::Explain(_) => {
                vec![PhysicalColumn::new("plan".to_string(), LogicalType::Text)]
            }
            PhysicalPlan::Values(values) => values.schema.clone(),
            PhysicalPlan::Pivot(pivot) => pivot.schema.clone(),
            PhysicalPlan::Unpivot(unpivot) => unpivot.schema.clone(),
            PhysicalPlan::RecursiveCTE(rcte) => rcte.schema.clone(),
            PhysicalPlan::EmptyResult(_) => vec![],
        }
    }

    /// Get all child plans of this plan node
    pub fn children(&self) -> Vec<&PhysicalPlan> {
        match self {
            PhysicalPlan::TableScan(_) => vec![],
            PhysicalPlan::Filter(filter) => vec![&filter.input],
            PhysicalPlan::Qualify(qualify) => vec![&qualify.input],
            PhysicalPlan::Projection(proj) => vec![&proj.input],
            PhysicalPlan::Limit(limit) => vec![&limit.input],
            PhysicalPlan::Sort(sort) => vec![&sort.input],
            PhysicalPlan::Aggregate(agg) => vec![&agg.input],
            PhysicalPlan::Join(join) => vec![&join.left, &join.right],
            PhysicalPlan::Union(union) => vec![&union.left, &union.right],
            PhysicalPlan::Intersect(intersect) => vec![&intersect.left, &intersect.right],
            PhysicalPlan::Except(except) => vec![&except.left, &except.right],
            PhysicalPlan::HashAggregate(agg) => vec![&agg.input],
            PhysicalPlan::SortMergeJoin(join) => vec![&join.left, &join.right],
            PhysicalPlan::HashJoin(join) => vec![&join.left, &join.right],
            PhysicalPlan::Insert(insert) => vec![&insert.input],
            PhysicalPlan::Update(_) => vec![],
            PhysicalPlan::Delete(_) => vec![],
            PhysicalPlan::CreateTable(_) => vec![],
            PhysicalPlan::DropTable(_) => vec![],
            PhysicalPlan::Explain(explain) => vec![&explain.input],
            PhysicalPlan::Values(_) => vec![],
            PhysicalPlan::Pivot(pivot) => vec![&pivot.input],
            PhysicalPlan::Unpivot(unpivot) => vec![&unpivot.input],
            PhysicalPlan::RecursiveCTE(rcte) => vec![&rcte.base_case, &rcte.recursive_case],
            PhysicalPlan::EmptyResult(_) => vec![],
        }
    }
}

/// Physical column definition
#[derive(Debug, Clone)]
pub struct PhysicalColumn {
    pub name: String,
    pub data_type: LogicalType,
}

impl PhysicalColumn {
    pub fn new(name: String, data_type: LogicalType) -> Self {
        Self { name, data_type }
    }
}

/// Physical table scan operator
#[derive(Debug, Clone)]
pub struct PhysicalTableScan {
    pub table_name: String,
    pub schema: Vec<PhysicalColumn>,
    pub column_ids: Vec<usize>,
    pub filters: Vec<ExpressionRef>,
    pub limit: Option<usize>,
}

impl PhysicalTableScan {
    pub fn new(table_name: String, schema: Vec<PhysicalColumn>) -> Self {
        let schema_len = schema.len();
        Self {
            table_name,
            schema,
            column_ids: (0..schema_len).collect(),
            filters: Vec::new(),
            limit: None,
        }
    }
}

/// Physical filter operator
#[derive(Debug, Clone)]
pub struct PhysicalFilter {
    pub input: Box<PhysicalPlan>,
    pub predicate: ExpressionRef,
}

impl PhysicalFilter {
    pub fn new(input: PhysicalPlan, predicate: ExpressionRef) -> Self {
        Self {
            input: Box::new(input),
            predicate,
        }
    }
}

/// Physical QUALIFY operator - filters rows based on window function results
/// Applied after window computation but before ORDER BY/LIMIT
#[derive(Debug, Clone)]
pub struct PhysicalQualify {
    pub input: Box<PhysicalPlan>,
    pub predicate: ExpressionRef,
}

impl PhysicalQualify {
    pub fn new(input: PhysicalPlan, predicate: ExpressionRef) -> Self {
        Self {
            input: Box::new(input),
            predicate,
        }
    }
}

/// Physical projection operator
#[derive(Debug, Clone)]
pub struct PhysicalProjection {
    pub input: Box<PhysicalPlan>,
    pub expressions: Vec<ExpressionRef>,
    pub schema: Vec<PhysicalColumn>,
}

impl PhysicalProjection {
    pub fn new(
        input: PhysicalPlan,
        expressions: Vec<ExpressionRef>,
        schema: Vec<PhysicalColumn>,
    ) -> Self {
        Self {
            input: Box::new(input),
            expressions,
            schema,
        }
    }
}

/// Physical limit operator
#[derive(Debug, Clone)]
pub struct PhysicalLimit {
    pub input: Box<PhysicalPlan>,
    pub limit: usize,
    pub offset: usize,
}

impl PhysicalLimit {
    pub fn new(input: PhysicalPlan, limit: usize, offset: usize) -> Self {
        Self {
            input: Box::new(input),
            limit,
            offset,
        }
    }
}

/// Physical sort operator
#[derive(Debug, Clone)]
pub struct PhysicalSort {
    pub input: Box<PhysicalPlan>,
    pub expressions: Vec<PhysicalSortExpression>,
}

#[derive(Debug, Clone)]
pub struct PhysicalSortExpression {
    pub expression: ExpressionRef,
    pub ascending: bool,
    pub nulls_first: bool,
}

impl PhysicalSort {
    pub fn new(input: PhysicalPlan, expressions: Vec<PhysicalSortExpression>) -> Self {
        Self {
            input: Box::new(input),
            expressions,
        }
    }
}

/// Physical aggregate operator
#[derive(Debug, Clone)]
pub struct PhysicalAggregate {
    pub input: Box<PhysicalPlan>,
    pub group_by: Vec<ExpressionRef>,
    pub aggregates: Vec<PhysicalAggregateExpression>,
    pub schema: Vec<PhysicalColumn>,
}

#[derive(Debug, Clone)]
pub struct PhysicalAggregateExpression {
    pub function_name: String,
    pub arguments: Vec<ExpressionRef>,
    pub distinct: bool,
    pub return_type: LogicalType,
}

impl PhysicalAggregate {
    pub fn new(
        input: PhysicalPlan,
        group_by: Vec<ExpressionRef>,
        aggregates: Vec<PhysicalAggregateExpression>,
        schema: Vec<PhysicalColumn>,
    ) -> Self {
        Self {
            input: Box::new(input),
            group_by,
            aggregates,
            schema,
        }
    }
}

/// Physical join operator
#[derive(Debug, Clone)]
pub struct PhysicalJoin {
    pub left: Box<PhysicalPlan>,
    pub right: Box<PhysicalPlan>,
    pub join_type: PhysicalJoinType,
    pub condition: Option<ExpressionRef>,
    pub schema: Vec<PhysicalColumn>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PhysicalJoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
    Semi,
    Anti,
}

impl PhysicalJoin {
    pub fn new(
        left: PhysicalPlan,
        right: PhysicalPlan,
        join_type: PhysicalJoinType,
        condition: Option<ExpressionRef>,
        schema: Vec<PhysicalColumn>,
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

/// Physical union operator
#[derive(Debug, Clone)]
pub struct PhysicalUnion {
    pub left: Box<PhysicalPlan>,
    pub right: Box<PhysicalPlan>,
    pub all: bool,  // true for UNION ALL, false for UNION DISTINCT
    pub schema: Vec<PhysicalColumn>,
}

impl PhysicalUnion {
    pub fn new(left: PhysicalPlan, right: PhysicalPlan, all: bool, schema: Vec<PhysicalColumn>) -> Self {
        Self {
            left: Box::new(left),
            right: Box::new(right),
            all,
            schema,
        }
    }
}

/// Physical intersect operator
#[derive(Debug, Clone)]
pub struct PhysicalIntersect {
    pub left: Box<PhysicalPlan>,
    pub right: Box<PhysicalPlan>,
    pub schema: Vec<PhysicalColumn>,
}

impl PhysicalIntersect {
    pub fn new(left: PhysicalPlan, right: PhysicalPlan, schema: Vec<PhysicalColumn>) -> Self {
        Self {
            left: Box::new(left),
            right: Box::new(right),
            schema,
        }
    }
}

/// Physical except operator
#[derive(Debug, Clone)]
pub struct PhysicalExcept {
    pub left: Box<PhysicalPlan>,
    pub right: Box<PhysicalPlan>,
    pub schema: Vec<PhysicalColumn>,
}

impl PhysicalExcept {
    pub fn new(left: PhysicalPlan, right: PhysicalPlan, schema: Vec<PhysicalColumn>) -> Self {
        Self {
            left: Box::new(left),
            right: Box::new(right),
            schema,
        }
    }
}

/// Physical hash aggregate operator
#[derive(Debug, Clone)]
pub struct PhysicalHashAggregate {
    pub input: Box<PhysicalPlan>,
    pub group_by: Vec<ExpressionRef>,
    pub aggregates: Vec<PhysicalAggregateExpression>,
    pub schema: Vec<PhysicalColumn>,
}

impl PhysicalHashAggregate {
    pub fn new(
        input: PhysicalPlan,
        group_by: Vec<ExpressionRef>,
        aggregates: Vec<PhysicalAggregateExpression>,
        schema: Vec<PhysicalColumn>,
    ) -> Self {
        Self {
            input: Box::new(input),
            group_by,
            aggregates,
            schema,
        }
    }
}

/// Physical sort merge join operator
#[derive(Debug, Clone)]
pub struct PhysicalSortMergeJoin {
    pub left: Box<PhysicalPlan>,
    pub right: Box<PhysicalPlan>,
    pub join_type: PhysicalJoinType,
    pub left_keys: Vec<ExpressionRef>,
    pub right_keys: Vec<ExpressionRef>,
    pub condition: Option<ExpressionRef>,
    pub schema: Vec<PhysicalColumn>,
}

impl PhysicalSortMergeJoin {
    pub fn new(
        left: PhysicalPlan,
        right: PhysicalPlan,
        join_type: PhysicalJoinType,
        left_keys: Vec<ExpressionRef>,
        right_keys: Vec<ExpressionRef>,
        condition: Option<ExpressionRef>,
        schema: Vec<PhysicalColumn>,
    ) -> Self {
        Self {
            left: Box::new(left),
            right: Box::new(right),
            join_type,
            left_keys,
            right_keys,
            condition,
            schema,
        }
    }
}

/// Physical hash join operator
#[derive(Debug, Clone)]
pub struct PhysicalHashJoin {
    pub left: Box<PhysicalPlan>,
    pub right: Box<PhysicalPlan>,
    pub join_type: PhysicalJoinType,
    pub left_keys: Vec<ExpressionRef>,
    pub right_keys: Vec<ExpressionRef>,
    pub condition: Option<ExpressionRef>,
    pub schema: Vec<PhysicalColumn>,
}

impl PhysicalHashJoin {
    pub fn new(
        left: PhysicalPlan,
        right: PhysicalPlan,
        join_type: PhysicalJoinType,
        left_keys: Vec<ExpressionRef>,
        right_keys: Vec<ExpressionRef>,
        condition: Option<ExpressionRef>,
        schema: Vec<PhysicalColumn>,
    ) -> Self {
        Self {
            left: Box::new(left),
            right: Box::new(right),
            join_type,
            left_keys,
            right_keys,
            condition,
            schema,
        }
    }
}

/// Physical insert operator
#[derive(Debug, Clone)]
pub struct PhysicalInsert {
    pub table_name: String,
    pub input: Box<PhysicalPlan>,
    pub column_names: Vec<String>,
}

impl PhysicalInsert {
    pub fn new(table_name: String, input: PhysicalPlan, column_names: Vec<String>) -> Self {
        Self {
            table_name,
            input: Box::new(input),
            column_names,
        }
    }
}

/// Physical update operator
#[derive(Debug, Clone)]
pub struct PhysicalUpdate {
    pub table_name: String,
    pub assignments: HashMap<String, ExpressionRef>,
    pub condition: Option<ExpressionRef>,
}

impl PhysicalUpdate {
    pub fn new(
        table_name: String,
        assignments: HashMap<String, ExpressionRef>,
        condition: Option<ExpressionRef>,
    ) -> Self {
        Self {
            table_name,
            assignments,
            condition,
        }
    }
}

/// Physical delete operator
#[derive(Debug, Clone)]
pub struct PhysicalDelete {
    pub table_name: String,
    pub condition: Option<ExpressionRef>,
}

impl PhysicalDelete {
    pub fn new(table_name: String, condition: Option<ExpressionRef>) -> Self {
        Self {
            table_name,
            condition,
        }
    }
}

/// Physical create table operator
#[derive(Debug, Clone)]
pub struct PhysicalCreateTable {
    pub table_name: String,
    pub schema: Vec<PhysicalColumn>,
    pub if_not_exists: bool,
}

impl PhysicalCreateTable {
    pub fn new(table_name: String, schema: Vec<PhysicalColumn>, if_not_exists: bool) -> Self {
        Self {
            table_name,
            schema,
            if_not_exists,
        }
    }
}

/// Physical drop table operator
#[derive(Debug, Clone)]
pub struct PhysicalDropTable {
    pub table_name: String,
    pub if_exists: bool,
}

impl PhysicalDropTable {
    pub fn new(table_name: String, if_exists: bool) -> Self {
        Self {
            table_name,
            if_exists,
        }
    }
}

/// Physical explain operator
#[derive(Debug, Clone)]
pub struct PhysicalExplain {
    pub input: Box<PhysicalPlan>,
    pub analyze: bool,
    pub verbose: bool,
}

impl PhysicalExplain {
    pub fn new(input: PhysicalPlan, analyze: bool, verbose: bool) -> Self {
        Self {
            input: Box::new(input),
            analyze,
            verbose,
        }
    }
}

/// Physical empty result operator
#[derive(Debug, Clone)]
pub struct PhysicalEmptyResult {
    pub schema: Vec<PhysicalColumn>,
}

impl PhysicalEmptyResult {
    pub fn new(schema: Vec<PhysicalColumn>) -> Self {
        Self { schema }
    }
}

/// Physical values operator (produces constant rows)
#[derive(Debug, Clone)]
pub struct PhysicalValues {
    /// List of rows, where each row is a list of bound expressions
    pub values: Vec<Vec<ExpressionRef>>,
    /// Schema of the values
    pub schema: Vec<PhysicalColumn>,
}

impl PhysicalValues {
    pub fn new(values: Vec<Vec<ExpressionRef>>, schema: Vec<PhysicalColumn>) -> Self {
        Self { values, schema }
    }
}

/// Physical PIVOT operator - transforms rows to columns
#[derive(Debug, Clone)]
pub struct PhysicalPivot {
    pub input: Box<PhysicalPlan>,
    /// Columns to pivot (create new columns for each distinct value)
    pub on_columns: Vec<ExpressionRef>,
    /// Aggregate expressions to compute for each pivot column
    pub using_values: Vec<PhysicalPivotValue>,
    /// Explicit pivot values (columns to create)
    pub in_values: Option<Vec<PhysicalPivotInValue>>,
    /// Columns to group by (determines output rows)
    pub group_by: Vec<ExpressionRef>,
    /// Output schema (computed based on pivot values and aggregates)
    pub schema: Vec<PhysicalColumn>,
}

/// Physical PIVOT value specification
#[derive(Debug, Clone)]
pub struct PhysicalPivotValue {
    pub expression: ExpressionRef,
    pub alias: Option<String>,
}

/// Physical PIVOT IN value
#[derive(Debug, Clone)]
pub struct PhysicalPivotInValue {
    pub value: ExpressionRef,
    pub alias: Option<String>,
}

impl PhysicalPivot {
    pub fn new(
        input: PhysicalPlan,
        on_columns: Vec<ExpressionRef>,
        using_values: Vec<PhysicalPivotValue>,
        in_values: Option<Vec<PhysicalPivotInValue>>,
        group_by: Vec<ExpressionRef>,
        schema: Vec<PhysicalColumn>,
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

/// Physical UNPIVOT operator - transforms columns to rows
#[derive(Debug, Clone)]
pub struct PhysicalUnpivot {
    pub input: Box<PhysicalPlan>,
    /// Columns to unpivot (stack into rows)
    pub on_columns: Vec<ExpressionRef>,
    /// Column name for the "name" column
    pub name_column: String,
    /// Column name(s) for the "value" column(s)
    pub value_columns: Vec<String>,
    /// Whether to include NULL values
    pub include_nulls: bool,
    /// Output schema
    pub schema: Vec<PhysicalColumn>,
}

impl PhysicalUnpivot {
    pub fn new(
        input: PhysicalPlan,
        on_columns: Vec<ExpressionRef>,
        name_column: String,
        value_columns: Vec<String>,
        include_nulls: bool,
        schema: Vec<PhysicalColumn>,
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

/// Physical RecursiveCTE operator - executes recursive CTEs with fixpoint iteration
#[derive(Debug, Clone)]
pub struct PhysicalRecursiveCTE {
    /// CTE name (for debugging)
    pub name: String,
    /// Base case plan (non-recursive)
    pub base_case: Box<PhysicalPlan>,
    /// Recursive case plan (references CTE)
    pub recursive_case: Box<PhysicalPlan>,
    /// Output schema
    pub schema: Vec<PhysicalColumn>,
}

impl PhysicalRecursiveCTE {
    pub fn new(
        name: String,
        base_case: PhysicalPlan,
        recursive_case: PhysicalPlan,
        schema: Vec<PhysicalColumn>,
    ) -> Self {
        Self {
            name,
            base_case: Box::new(base_case),
            recursive_case: Box::new(recursive_case),
            schema,
        }
    }
}

/// Execution operator trait
pub trait ExecutionOperator: Send + Sync {
    /// Execute the operator and return a stream of data chunks
    fn execute(&self) -> PrismDBResult<Box<dyn DataChunkStream>>;

    /// Get the schema of the output
    fn schema(&self) -> Vec<PhysicalColumn>;
}

/// Stream of data chunks
pub trait DataChunkStream: Iterator<Item = PrismDBResult<DataChunk>> + Send {}

/// Boxed data chunk stream
pub type BoxedDataChunkStream = Box<dyn DataChunkStream>;
