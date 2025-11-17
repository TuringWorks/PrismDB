//! Abstract Syntax Tree (AST) for SQL statements
//!
//! Defines the structure of parsed SQL statements.

use crate::types::LogicalType;
use std::collections::HashMap;

/// SQL statement types
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    CreateTable(CreateTableStatement),
    DropTable(DropTableStatement),
    AlterTable(AlterTableStatement),
    CreateView(CreateViewStatement),
    DropView(DropViewStatement),
    RefreshMaterializedView(RefreshMaterializedViewStatement),
    CreateIndex(CreateIndexStatement),
    DropIndex(DropIndexStatement),
    Begin(BeginStatement),
    Commit(CommitStatement),
    Rollback(RollbackStatement),
    Explain(ExplainStatement),
    Show(ShowStatement),
    Install(InstallStatement),
    Load(LoadStatement),
    Set(SetStatement),
    CreateSecret(CreateSecretStatement),
}

/// SELECT statement
#[derive(Debug, Clone, PartialEq)]
pub struct SelectStatement {
    pub with_clause: Option<WithClause>,  // Common Table Expressions (CTEs)
    pub distinct: bool,
    pub select_list: Vec<SelectItem>,
    pub from: Option<TableReference>,
    pub where_clause: Option<Box<Expression>>,
    pub group_by: Vec<Expression>,
    pub having: Option<Box<Expression>>,
    pub qualify: Option<Box<Expression>>, // QUALIFY clause for filtering window functions
    pub order_by: Vec<OrderByExpression>,
    pub limit: Option<LimitClause>,
    pub offset: Option<usize>,
    pub set_operations: Vec<SetOperation>,  // UNION, INTERSECT, EXCEPT
}

/// WITH clause (Common Table Expressions)
#[derive(Debug, Clone, PartialEq)]
pub struct WithClause {
    pub recursive: bool,
    pub ctes: Vec<CommonTableExpression>,
}

/// Common Table Expression (CTE)
#[derive(Debug, Clone, PartialEq)]
pub struct CommonTableExpression {
    pub name: String,
    pub columns: Vec<String>,  // Optional column names
    pub query: Box<SelectStatement>,
}

/// Set operation (UNION, INTERSECT, EXCEPT)
#[derive(Debug, Clone, PartialEq)]
pub struct SetOperation {
    pub op_type: SetOperationType,
    pub all: bool,  // For UNION ALL vs UNION
    pub query: Box<SelectStatement>,
}

/// Type of set operation
#[derive(Debug, Clone, PartialEq)]
pub enum SetOperationType {
    Union,
    Intersect,
    Except,
}

/// SELECT list item
#[derive(Debug, Clone, PartialEq)]
pub enum SelectItem {
    Expression(Expression),
    QualifiedWildcard(String), // table.*
    Wildcard,                  // *
    Alias(Box<Expression>, String),
}

/// Table reference
#[derive(Debug, Clone, PartialEq)]
pub enum TableReference {
    Table {
        name: String,
        alias: Option<String>,
    },
    Join {
        left: Box<TableReference>,
        join_type: JoinType,
        right: Box<TableReference>,
        condition: JoinCondition,
    },
    Subquery {
        subquery: Box<SelectStatement>,
        alias: String,
    },
    TableFunction {
        name: String,
        arguments: Vec<Expression>,
        alias: Option<String>,
    },
    Pivot {
        source: Box<TableReference>,
        pivot_spec: PivotSpec,
        alias: Option<String>,
    },
    Unpivot {
        source: Box<TableReference>,
        unpivot_spec: UnpivotSpec,
        alias: Option<String>,
    },
}

/// Join type
#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

/// Join condition
#[derive(Debug, Clone, PartialEq)]
pub enum JoinCondition {
    On(Expression),
    Using(Vec<String>),
}

/// ORDER BY expression
#[derive(Debug, Clone, PartialEq)]
pub struct OrderByExpression {
    pub expression: Expression,
    pub ascending: bool,
    pub nulls_first: bool,
}

/// LIMIT clause
#[derive(Debug, Clone, PartialEq)]
pub struct LimitClause {
    pub limit: usize,
    pub offset: Option<usize>,
}

/// INSERT statement
#[derive(Debug, Clone, PartialEq)]
pub struct InsertStatement {
    pub table_name: String,
    pub columns: Vec<String>,
    pub source: InsertSource,
    pub on_conflict: Option<OnConflict>,
}

/// INSERT source
#[derive(Debug, Clone, PartialEq)]
pub enum InsertSource {
    Values(Vec<Vec<Expression>>),
    Select(SelectStatement),
    DefaultValues,
}

/// ON CONFLICT clause
#[derive(Debug, Clone, PartialEq)]
pub enum OnConflict {
    DoNothing,
    DoUpdate {
        assignments: Vec<Assignment>,
        where_clause: Option<Expression>,
    },
}

/// UPDATE statement
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateStatement {
    pub table_name: String,
    pub assignments: Vec<Assignment>,
    pub where_clause: Option<Expression>,
}

/// DELETE statement
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteStatement {
    pub table_name: String,
    pub where_clause: Option<Expression>,
}

/// Assignment (SET column = value)
#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    pub column: String,
    pub value: Expression,
}

/// CREATE TABLE statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableStatement {
    pub table_name: String,
    pub columns: Vec<ColumnDefinition>,
    pub constraints: Vec<TableConstraint>,
    pub if_not_exists: bool,
}

/// Column definition
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: LogicalType,
    pub nullable: bool,
    pub default_value: Option<Expression>,
    pub constraints: Vec<ColumnConstraint>,
}

/// Column constraint
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnConstraint {
    PrimaryKey,
    Unique,
    NotNull,
    Check(Expression),
    Default(Expression),
    References { table: String, column: String },
    AutoIncrement,
}

/// Table constraint
#[derive(Debug, Clone, PartialEq)]
pub enum TableConstraint {
    PrimaryKey {
        columns: Vec<String>,
    },
    Unique {
        columns: Vec<String>,
        name: Option<String>,
    },
    ForeignKey {
        columns: Vec<String>,
        foreign_table: String,
        foreign_columns: Vec<String>,
        name: Option<String>,
    },
    Check {
        expression: Expression,
        name: Option<String>,
    },
}

/// DROP TABLE statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropTableStatement {
    pub table_name: String,
    pub if_exists: bool,
}

/// ALTER TABLE statement
#[derive(Debug, Clone, PartialEq)]
pub struct AlterTableStatement {
    pub table_name: String,
    pub operation: AlterTableOperation,
}

/// ALTER TABLE operation
#[derive(Debug, Clone, PartialEq)]
pub enum AlterTableOperation {
    AddColumn(ColumnDefinition),
    DropColumn { column_name: String },
    RenameColumn { old_name: String, new_name: String },
    RenameTable { new_name: String },
    AddConstraint(TableConstraint),
    DropConstraint { constraint_name: String },
}

/// CREATE VIEW statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateViewStatement {
    pub view_name: String,
    pub columns: Vec<String>,
    pub query: SelectStatement,
    pub or_replace: bool,
    pub if_not_exists: bool,
    pub materialized: bool,
    pub refresh_strategy: Option<ViewRefreshStrategy>,
}

/// Refresh strategy for materialized views
#[derive(Debug, Clone, PartialEq)]
pub enum ViewRefreshStrategy {
    Manual,
    OnCommit,
    OnDemand,
    Incremental,
}

/// DROP VIEW statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropViewStatement {
    pub view_name: String,
    pub if_exists: bool,
    pub materialized: bool,
}

/// REFRESH MATERIALIZED VIEW statement
#[derive(Debug, Clone, PartialEq)]
pub struct RefreshMaterializedViewStatement {
    pub view_name: String,
    pub concurrently: bool,
}

/// CREATE INDEX statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateIndexStatement {
    pub index_name: String,
    pub table_name: String,
    pub columns: Vec<String>,
    pub unique: bool,
    pub if_not_exists: bool,
}

/// DROP INDEX statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropIndexStatement {
    pub index_name: String,
    pub if_exists: bool,
}

/// BEGIN statement
#[derive(Debug, Clone, PartialEq)]
pub struct BeginStatement {
    pub transaction_mode: Option<TransactionMode>,
}

/// Transaction mode
#[derive(Debug, Clone, PartialEq)]
pub enum TransactionMode {
    ReadWrite,
    ReadOnly,
    Serializable,
    RepeatableRead,
    ReadCommitted,
}

/// COMMIT statement
#[derive(Debug, Clone, PartialEq)]
pub struct CommitStatement {
    pub chain: bool,
}

/// ROLLBACK statement
#[derive(Debug, Clone, PartialEq)]
pub struct RollbackStatement {
    pub savepoint: Option<String>,
    pub chain: bool,
}

/// EXPLAIN statement
#[derive(Debug, Clone, PartialEq)]
pub struct ExplainStatement {
    pub statement: Box<Statement>,
    pub analyze: bool,
    pub verbose: bool,
}

/// SHOW statement
#[derive(Debug, Clone, PartialEq)]
pub enum ShowStatement {
    Tables,
    Columns { table: String },
    Indexes { table: Option<String> },
    Variables,
    Databases,
    Schemas,
}

/// INSTALL statement (for installing extensions)
#[derive(Debug, Clone, PartialEq)]
pub struct InstallStatement {
    pub extension_name: String,
}

/// LOAD statement (for loading extensions)
#[derive(Debug, Clone, PartialEq)]
pub struct LoadStatement {
    pub extension_name: String,
}

/// SET statement (for configuration variables)
#[derive(Debug, Clone, PartialEq)]
pub struct SetStatement {
    pub variable: String,
    pub value: SetValue,
}

/// Value types for SET statement
#[derive(Debug, Clone, PartialEq)]
pub enum SetValue {
    String(String),
    Number(i64),
    Boolean(bool),
    Default,
}

/// CREATE SECRET statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateSecretStatement {
    pub or_replace: bool,
    pub name: String,
    pub secret_type: String,
    pub options: HashMap<String, String>,
}

/// Expression AST
#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    // Literals
    Literal(LiteralValue),
    ColumnReference {
        table: Option<String>,
        column: String,
    },
    Parameter(usize),
    FunctionCall {
        name: String,
        arguments: Vec<Expression>,
        distinct: bool,
    },
    AggregateFunction {
        name: String,
        arguments: Vec<Expression>,
        distinct: bool,
    },
    WindowFunction {
        name: String,
        arguments: Vec<Expression>,
        window_spec: WindowSpec,
    },
    Cast {
        expression: Box<Expression>,
        data_type: LogicalType,
    },
    Case {
        operand: Option<Box<Expression>>,
        conditions: Vec<Expression>,
        results: Vec<Expression>,
        else_result: Option<Box<Expression>>,
    },
    Between {
        expression: Box<Expression>,
        low: Box<Expression>,
        high: Box<Expression>,
        not: bool,
    },
    InList {
        expression: Box<Expression>,
        list: Vec<Expression>,
        not: bool,
    },
    InSubquery {
        expression: Box<Expression>,
        subquery: Box<SelectStatement>,
        not: bool,
    },
    Exists(Box<SelectStatement>),
    Subquery(Box<SelectStatement>),
    IsNull(Box<Expression>),
    IsNotNull(Box<Expression>),
    IsTrue(Box<Expression>),
    IsFalse(Box<Expression>),
    IsUnknown(Box<Expression>),
    IsNotTrue(Box<Expression>),
    IsNotFalse(Box<Expression>),
    IsNotUnknown(Box<Expression>),
    Like {
        expression: Box<Expression>,
        pattern: Box<Expression>,
        escape: Option<Box<Expression>>,
        case_insensitive: bool,
        not: bool,
    },
    BetweenSymmetric {
        expression: Box<Expression>,
        low: Box<Expression>,
        high: Box<Expression>,
        not: bool,
    },
    Binary {
        left: Box<Expression>,
        operator: BinaryOperator,
        right: Box<Expression>,
    },
    Unary {
        operator: UnaryOperator,
        expression: Box<Expression>,
    },
    QualifiedWildcard {
        table: String,
    },
    Wildcard,
}

/// Literal values
#[derive(Debug, Clone, PartialEq)]
pub enum LiteralValue {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
    Date(String),
    Time(String),
    Timestamp(String),
    Interval { value: String, field: String },
}

/// Binary operators
#[derive(Debug, Clone, PartialEq)]
pub enum BinaryOperator {
    // Arithmetic
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,

    // Comparison
    Equals,
    NotEquals,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,

    // Logical
    And,
    Or,

    // String
    Like,
    ILike,
    SimilarTo,

    // Other
    Is,
    IsNot,
    In,
    NotIn,
    Between,
    NotBetween,
}

/// Unary operators
#[derive(Debug, Clone, PartialEq)]
pub enum UnaryOperator {
    Plus,
    Minus,
    Not,
    IsNull,
    IsNotNull,
}

/// Window specification
#[derive(Debug, Clone, PartialEq)]
pub struct WindowSpec {
    pub partition_by: Vec<Expression>,
    pub order_by: Vec<OrderByExpression>,
    pub window_frame: Option<WindowFrame>,
}

/// Window frame
#[derive(Debug, Clone, PartialEq)]
pub struct WindowFrame {
    pub units: WindowFrameUnits,
    pub start_bound: WindowFrameBound,
    pub end_bound: Option<WindowFrameBound>,
}

/// Window frame units
#[derive(Debug, Clone, PartialEq)]
pub enum WindowFrameUnits {
    Rows,
    Range,
    Groups,
}

/// Window frame bound
#[derive(Debug, Clone, PartialEq)]
pub enum WindowFrameBound {
    CurrentRow,
    UnboundedPreceding,
    UnboundedFollowing,
    Preceding(usize),
    Following(usize),
}

/// PIVOT specification
/// Supports both simplified syntax (PIVOT dataset ON columns USING values)
/// and SQL Standard syntax (FROM dataset PIVOT (values FOR columns IN (in_list)))
#[derive(Debug, Clone, PartialEq)]
pub struct PivotSpec {
    /// Columns to pivot (create new columns for each distinct value)
    pub on_columns: Vec<Expression>,
    /// Aggregate expressions to compute for each pivot column
    /// Example: [SUM(revenue) AS total, AVG(revenue) AS average]
    pub using_values: Vec<PivotValue>,
    /// Optional: explicitly list which pivot values to create columns for
    /// If None, automatically detects all distinct values
    pub in_values: Option<Vec<PivotInValue>>,
    /// Columns to group by (determines output rows)
    pub group_by: Vec<Expression>,
}

/// PIVOT value specification (aggregate expression with optional alias)
#[derive(Debug, Clone, PartialEq)]
pub struct PivotValue {
    pub expression: Expression,
    pub alias: Option<String>,
}

/// PIVOT IN value (explicit column value specification)
#[derive(Debug, Clone, PartialEq)]
pub struct PivotInValue {
    /// The value from the ON column that should get its own column
    /// Example: In "FOR year IN (2000, 2010, 2020)", these are 2000, 2010, 2020
    pub value: Expression,
    /// Optional alias for the generated column
    /// Example: IN ((2000, 2001) AS '2000-2001', (2002, 2003) AS '2002-2003')
    pub alias: Option<String>,
}

/// UNPIVOT specification
/// Supports both simplified syntax (UNPIVOT dataset ON columns INTO NAME/VALUE)
/// and SQL Standard syntax (FROM dataset UNPIVOT [INCLUDE NULLS] (value FOR name IN (columns)))
#[derive(Debug, Clone, PartialEq)]
pub struct UnpivotSpec {
    /// Columns to unpivot (stack into rows)
    pub on_columns: Vec<Expression>,
    /// Column name for the "name" column (contains original column names)
    pub name_column: String,
    /// Column name(s) for the "value" column(s) (contains the values)
    /// Multiple value columns support partial unpivoting
    pub value_columns: Vec<String>,
    /// Whether to include NULL values (SQL Standard INCLUDE NULLS option)
    pub include_nulls: bool,
}

/// Query parameters
#[derive(Debug, Clone, Default)]
pub struct QueryParameters {
    pub parameters: HashMap<usize, LiteralValue>,
}

impl QueryParameters {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_parameter(&mut self, index: usize, value: LiteralValue) {
        self.parameters.insert(index, value);
    }

    pub fn get_parameter(&self, index: usize) -> Option<&LiteralValue> {
        self.parameters.get(&index)
    }
}

impl Expression {
    /// Evaluate the expression on a data chunk
    /// This is a stub implementation - full expression evaluation should be
    /// delegated to the expression module
    pub fn evaluate(
        &self,
        chunk: &crate::types::DataChunk,
    ) -> crate::common::error::PrismDBResult<crate::types::Vector> {
        // This is a simplified stub that always returns a boolean vector
        // In a real implementation, this would properly evaluate the expression
        use crate::common::error::PrismDBError;
        use crate::types::Vector;
        use crate::Value;

        // For now, return a simple boolean vector filled with true
        let size = chunk.len();
        let values: Vec<Value> = vec![Value::Boolean(true); size];

        Vector::from_values(&values).map_err(|e| {
            PrismDBError::Execution(format!(
                "Failed to create vector from expression evaluation: {:?}",
                e
            ))
        })
    }
}
