//! Core expression types for PrismDB

use crate::common::error::{PrismDBError, PrismDBResult};
use crate::types::{DataChunk, LogicalType, Value, Vector};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;

/// Expression reference type
pub type ExpressionRef = Arc<dyn Expression>;

/// Expression trait that all expressions must implement
pub trait Expression: std::fmt::Debug + Send + Sync {
    /// Get the return type of this expression
    fn return_type(&self) -> &LogicalType;

    /// Evaluate this expression on a data chunk
    /// Takes ExecutionContext for subquery evaluation
    fn evaluate(&self, chunk: &DataChunk, context: &crate::execution::ExecutionContext) -> PrismDBResult<Vector>;

    /// Evaluate this expression on a single row
    /// Takes ExecutionContext for subquery evaluation
    fn evaluate_row(&self, chunk: &DataChunk, row_idx: usize, context: &crate::execution::ExecutionContext) -> PrismDBResult<Value>;

    /// Check if this expression is deterministic
    fn is_deterministic(&self) -> bool;

    /// Check if this expression can return null
    fn is_nullable(&self) -> bool;

    /// Downcast to Any for type checking
    fn as_any(&self) -> &dyn std::any::Any;

    /// Get the children of this expression
    fn children(&self) -> Vec<ExpressionRef> {
        vec![]
    }
}

/// Expression enum that encompasses all expression types
#[derive(Debug, Clone)]
pub enum ExpressionEnum {
    Constant(ConstantExpression),
    ColumnRef(ColumnRefExpression),
    Function(FunctionExpression),
    Cast(CastExpression),
    Comparison(ComparisonExpression),
}

impl Expression for ExpressionEnum {
    fn return_type(&self) -> &LogicalType {
        match self {
            ExpressionEnum::Constant(expr) => expr.return_type(),
            ExpressionEnum::ColumnRef(expr) => expr.return_type(),
            ExpressionEnum::Function(expr) => expr.return_type(),
            ExpressionEnum::Cast(expr) => expr.return_type(),
            ExpressionEnum::Comparison(expr) => expr.return_type(),
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn evaluate(&self, chunk: &DataChunk, context: &crate::execution::ExecutionContext) -> PrismDBResult<Vector> {
        match self {
            ExpressionEnum::Constant(expr) => expr.evaluate(chunk, context),
            ExpressionEnum::ColumnRef(expr) => expr.evaluate(chunk, context),
            ExpressionEnum::Function(expr) => expr.evaluate(chunk, context),
            ExpressionEnum::Cast(expr) => expr.evaluate(chunk, context),
            ExpressionEnum::Comparison(expr) => expr.evaluate(chunk, context),
        }
    }

    fn evaluate_row(&self, chunk: &DataChunk, row_idx: usize, context: &crate::execution::ExecutionContext) -> PrismDBResult<Value> {
        match self {
            ExpressionEnum::Constant(expr) => expr.evaluate_row(chunk, row_idx, context),
            ExpressionEnum::ColumnRef(expr) => expr.evaluate_row(chunk, row_idx, context),
            ExpressionEnum::Function(expr) => expr.evaluate_row(chunk, row_idx, context),
            ExpressionEnum::Cast(expr) => expr.evaluate_row(chunk, row_idx, context),
            ExpressionEnum::Comparison(expr) => expr.evaluate_row(chunk, row_idx, context),
        }
    }

    fn is_deterministic(&self) -> bool {
        match self {
            ExpressionEnum::Constant(expr) => expr.is_deterministic(),
            ExpressionEnum::ColumnRef(expr) => expr.is_deterministic(),
            ExpressionEnum::Function(expr) => expr.is_deterministic(),
            ExpressionEnum::Cast(expr) => expr.is_deterministic(),
            ExpressionEnum::Comparison(expr) => expr.is_deterministic(),
        }
    }

    fn is_nullable(&self) -> bool {
        match self {
            ExpressionEnum::Constant(expr) => expr.is_nullable(),
            ExpressionEnum::ColumnRef(expr) => expr.is_nullable(),
            ExpressionEnum::Function(expr) => expr.is_nullable(),
            ExpressionEnum::Cast(expr) => expr.is_nullable(),
            ExpressionEnum::Comparison(expr) => expr.is_nullable(),
        }
    }

    fn children(&self) -> Vec<ExpressionRef> {
        match self {
            ExpressionEnum::Constant(_) => vec![],
            ExpressionEnum::ColumnRef(_) => vec![],
            ExpressionEnum::Function(expr) => expr.children(),
            ExpressionEnum::Cast(expr) => expr.children(),
            ExpressionEnum::Comparison(expr) => expr.children(),
        }
    }
}

/// Different types of expressions
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ExpressionType {
    // Constants
    Constant,
    Parameter,
    Null,

    // Column references
    ColumnRef,
    BoundColumnRef,
    BoundRef,

    // Operators
    Operator,
    BoundOperator,
    Comparison,
    BoundComparison,
    Conjunction,
    BoundConjunction,

    // Functions
    Function,
    BoundFunction,
    Aggregate,
    BoundAggregate,
    Window,
    BoundWindow,

    // Subqueries
    Subquery,
    Exists,
    NotExists,
    ScalarSubquery,
    InSubquery,
    NotInSubquery,

    // Case expressions
    Case,
    BoundCase,

    // Cast expressions
    Cast,
    BoundCast,

    // Other
    Between,
    BoundBetween,
    IsNull,
    IsNotNull,
    Default,
    RowNumber,
    Star,
    TableStar,
}

/// Base expression struct
#[derive(Debug, Clone)]
pub struct BaseExpression {
    pub expression_type: ExpressionType,
    pub return_type: LogicalType,
    pub alias: Option<String>,
}

impl BaseExpression {
    pub fn new(expression_type: ExpressionType, return_type: LogicalType) -> Self {
        Self {
            expression_type,
            return_type,
            alias: None,
        }
    }

    pub fn with_alias(mut self, alias: String) -> Self {
        self.alias = Some(alias);
        self
    }
}

/// Constant value expression
#[derive(Debug, Clone)]
pub struct ConstantExpression {
    base: BaseExpression,
    value: Value,
}

impl ConstantExpression {
    pub fn new(value: Value) -> PrismDBResult<Self> {
        let return_type = value.get_type().clone();
        Ok(Self {
            base: BaseExpression::new(ExpressionType::Constant, return_type),
            value,
        })
    }

    pub fn value(&self) -> &Value {
        &self.value
    }
}

impl Expression for ConstantExpression {
    fn return_type(&self) -> &LogicalType {
        &self.base.return_type
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn evaluate(&self, chunk: &DataChunk, _context: &crate::execution::ExecutionContext) -> PrismDBResult<Vector> {
        // Create a vector with the same value repeated for all rows
        let mut values = Vec::with_capacity(chunk.count());
        for _ in 0..chunk.count() {
            values.push(self.value.clone());
        }

        crate::types::Vector::from_values(&values)
    }

    fn evaluate_row(&self, _chunk: &DataChunk, _row_idx: usize, _context: &crate::execution::ExecutionContext) -> PrismDBResult<Value> {
        Ok(self.value.clone())
    }

    fn is_deterministic(&self) -> bool {
        true
    }

    fn is_nullable(&self) -> bool {
        self.value.is_null()
    }

    fn children(&self) -> Vec<ExpressionRef> {
        vec![]
    }
}

/// Column reference expression
#[derive(Debug, Clone)]
pub struct ColumnRefExpression {
    base: BaseExpression,
    column_index: usize,
    column_name: String,
}

impl ColumnRefExpression {
    pub fn new(column_index: usize, column_name: String, return_type: LogicalType) -> Self {
        Self {
            base: BaseExpression::new(ExpressionType::ColumnRef, return_type),
            column_index,
            column_name,
        }
    }

    pub fn column_index(&self) -> usize {
        self.column_index
    }

    pub fn column_name(&self) -> &str {
        &self.column_name
    }
}

impl Expression for ColumnRefExpression {
    fn return_type(&self) -> &LogicalType {
        &self.base.return_type
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn evaluate(&self, chunk: &DataChunk, _context: &crate::execution::ExecutionContext) -> PrismDBResult<Vector> {
        if self.column_index >= chunk.column_count() {
            return Err(PrismDBError::InvalidValue(format!(
                "Column index {} out of bounds (columns: {})",
                self.column_index,
                chunk.column_count()
            )));
        }

        // Extract column vector
        match chunk.get_vector(self.column_index) {
            Some(vector) => Ok(vector.clone()),
            None => Err(PrismDBError::InvalidValue(format!(
                "Column {} not found",
                self.column_index
            ))),
        }
    }

    fn evaluate_row(&self, chunk: &DataChunk, row_idx: usize, _context: &crate::execution::ExecutionContext) -> PrismDBResult<Value> {
        chunk.get_value(row_idx, self.column_index)
    }

    fn is_deterministic(&self) -> bool {
        true
    }

    fn is_nullable(&self) -> bool {
        true // Column references can be nullable depending on data
    }

    fn children(&self) -> Vec<ExpressionRef> {
        vec![]
    }
}

/// Function call expression
#[derive(Debug, Clone)]
pub struct FunctionExpression {
    base: BaseExpression,
    function_name: String,
    children: Vec<ExpressionRef>,
    is_aggregate: bool,
}

impl FunctionExpression {
    pub fn new(
        function_name: String,
        return_type: LogicalType,
        children: Vec<ExpressionRef>,
    ) -> Self {
        Self {
            base: BaseExpression::new(ExpressionType::Function, return_type),
            function_name,
            children,
            is_aggregate: false,
        }
    }

    pub fn aggregate(
        function_name: String,
        return_type: LogicalType,
        children: Vec<ExpressionRef>,
    ) -> Self {
        Self {
            base: BaseExpression::new(ExpressionType::Aggregate, return_type),
            function_name,
            children,
            is_aggregate: true,
        }
    }

    pub fn function_name(&self) -> &str {
        &self.function_name
    }

    pub fn is_aggregate(&self) -> bool {
        self.is_aggregate
    }
}

impl Expression for FunctionExpression {
    fn return_type(&self) -> &LogicalType {
        &self.base.return_type
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn evaluate(&self, chunk: &DataChunk, context: &crate::execution::ExecutionContext) -> PrismDBResult<Vector> {
        use crate::expression::function::evaluate_builtin_function;

        // Evaluate all child expressions
        let mut arg_vectors = Vec::new();
        for child in &self.children {
            let child_result = child.evaluate(chunk, context)?;
            arg_vectors.push(child_result);
        }

        // For each row in the chunk, evaluate the function
        let row_count = chunk.count();
        let mut result_values = Vec::with_capacity(row_count);

        for row_idx in 0..row_count {
            // Extract argument values for this row
            let mut arg_values = Vec::new();
            for arg_vector in &arg_vectors {
                let value = arg_vector.get_value(row_idx)?;
                arg_values.push(value);
            }

            // Evaluate the function for this row
            let result = evaluate_builtin_function(&self.function_name, &arg_values)?;
            result_values.push(result);
        }

        // Create result vector
        Vector::from_values(&result_values)
    }

    fn evaluate_row(&self, chunk: &DataChunk, row_idx: usize, context: &crate::execution::ExecutionContext) -> PrismDBResult<Value> {
        if self.children.is_empty() {
            return Err(PrismDBError::InvalidValue(
                "Function expression has no children".to_string(),
            ));
        }

        // For now, just return the first child's evaluation
        self.children[0].evaluate_row(chunk, row_idx, context)
    }

    fn is_deterministic(&self) -> bool {
        // Most functions are deterministic, but some like RANDOM() are not
        !matches!(
            self.function_name.to_uppercase().as_str(),
            "RANDOM" | "NOW" | "CURRENT_TIMESTAMP"
        )
    }

    fn is_nullable(&self) -> bool {
        // Functions can be nullable if any child is nullable
        self.children.iter().any(|child| child.is_nullable())
    }

    fn children(&self) -> Vec<ExpressionRef> {
        self.children.clone()
    }
}

/// Cast expression
#[derive(Debug, Clone)]
pub struct CastExpression {
    base: BaseExpression,
    child: ExpressionRef,
    try_cast: bool,
}

impl CastExpression {
    pub fn new(child: ExpressionRef, target_type: LogicalType, try_cast: bool) -> Self {
        Self {
            base: BaseExpression::new(ExpressionType::Cast, target_type),
            child,
            try_cast,
        }
    }

    pub fn child(&self) -> &dyn Expression {
        self.child.as_ref()
    }

    pub fn try_cast(&self) -> bool {
        self.try_cast
    }
}

impl Expression for CastExpression {
    fn return_type(&self) -> &LogicalType {
        &self.base.return_type
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn evaluate(&self, chunk: &DataChunk, context: &crate::execution::ExecutionContext) -> PrismDBResult<Vector> {
        let child_result = self.child.evaluate(chunk, context)?;
        // In a real implementation, we'd perform cast here
        // For now, just return child result
        Ok(child_result)
    }

    fn evaluate_row(&self, chunk: &DataChunk, row_idx: usize, context: &crate::execution::ExecutionContext) -> PrismDBResult<Value> {
        let child_value = self.child.evaluate_row(chunk, row_idx, context)?;
        // Perform cast
        if self.try_cast {
            child_value.cast_to(&self.base.return_type)
        } else {
            child_value.cast_to(&self.base.return_type)
        }
    }

    fn is_deterministic(&self) -> bool {
        self.child.is_deterministic()
    }

    fn is_nullable(&self) -> bool {
        self.child.is_nullable() || self.try_cast
    }

    fn children(&self) -> Vec<ExpressionRef> {
        vec![self.child.clone()]
    }
}

/// Comparison type enum
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ComparisonType {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    Like,
    NotLike,
    ILike,
    NotILike,
    In,
    NotIn,
    IsDistinctFrom,
    IsNotDistinctFrom,
}

/// Comparison expression
#[derive(Debug, Clone)]
pub struct ComparisonExpression {
    base: BaseExpression,
    left: ExpressionRef,
    right: ExpressionRef,
    comparison_type: ComparisonType,
}

impl ComparisonExpression {
    pub fn new(comparison_type: ComparisonType, left: ExpressionRef, right: ExpressionRef) -> Self {
        Self {
            base: BaseExpression::new(ExpressionType::Comparison, LogicalType::Boolean),
            comparison_type,
            left,
            right,
        }
    }

    pub fn comparison_type(&self) -> &ComparisonType {
        &self.comparison_type
    }

    pub fn left(&self) -> &dyn Expression {
        self.left.as_ref()
    }

    pub fn right(&self) -> &dyn Expression {
        self.right.as_ref()
    }

    pub fn left_ref(&self) -> &ExpressionRef {
        &self.left
    }

    pub fn right_ref(&self) -> &ExpressionRef {
        &self.right
    }
}

impl Expression for ComparisonExpression {
    fn return_type(&self) -> &LogicalType {
        &self.base.return_type
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn evaluate(&self, chunk: &DataChunk, context: &crate::execution::ExecutionContext) -> PrismDBResult<Vector> {
        let left_result = self.left.evaluate(chunk, context)?;
        let right_result = self.right.evaluate(chunk, context)?;

        // Perform comparison row by row
        let mut results = Vec::with_capacity(chunk.count());
        for row_idx in 0..chunk.count() {
            let left_value = left_result.get_value(row_idx)?;
            let right_value = right_result.get_value(row_idx)?;
            let result = self.compare_values(&left_value, &right_value)?;
            results.push(result);
        }

        crate::types::Vector::from_values(&results)
    }

    fn evaluate_row(&self, chunk: &DataChunk, row_idx: usize, context: &crate::execution::ExecutionContext) -> PrismDBResult<Value> {
        let left_value = self.left.evaluate_row(chunk, row_idx, context)?;
        let right_value = self.right.evaluate_row(chunk, row_idx, context)?;
        self.compare_values(&left_value, &right_value)
    }

    fn is_deterministic(&self) -> bool {
        self.left.is_deterministic() && self.right.is_deterministic()
    }

    fn is_nullable(&self) -> bool {
        self.left.is_nullable() || self.right.is_nullable()
    }

    fn children(&self) -> Vec<ExpressionRef> {
        vec![self.left.clone(), self.right.clone()]
    }
}

impl ComparisonExpression {
    fn compare_values(&self, left: &Value, right: &Value) -> PrismDBResult<Value> {
        let result = match self.comparison_type {
            ComparisonType::Equal => left.compare(right)? == std::cmp::Ordering::Equal,
            ComparisonType::NotEqual => left.compare(right)? != std::cmp::Ordering::Equal,
            ComparisonType::LessThan => left.compare(right)? == std::cmp::Ordering::Less,
            ComparisonType::LessThanOrEqual => {
                let cmp = left.compare(right)?;
                cmp == std::cmp::Ordering::Less || cmp == std::cmp::Ordering::Equal
            }
            ComparisonType::GreaterThan => left.compare(right)? == std::cmp::Ordering::Greater,
            ComparisonType::GreaterThanOrEqual => {
                let cmp = left.compare(right)?;
                cmp == std::cmp::Ordering::Greater || cmp == std::cmp::Ordering::Equal
            }
            // Simplified implementations for other comparison types
            ComparisonType::Like => self.like_comparison(left, right)?,
            ComparisonType::NotLike => !self.like_comparison(left, right)?,
            ComparisonType::ILike => self.ilike_comparison(left, right)?,
            ComparisonType::NotILike => !self.ilike_comparison(left, right)?,
            ComparisonType::In => self.in_comparison(left, right)?,
            ComparisonType::NotIn => !self.in_comparison(left, right)?,
            ComparisonType::IsDistinctFrom => self.is_distinct_from(left, right)?,
            ComparisonType::IsNotDistinctFrom => !self.is_distinct_from(left, right)?,
        };

        Ok(Value::Boolean(result))
    }

    fn like_comparison(&self, left: &Value, right: &Value) -> PrismDBResult<bool> {
        // Simplified LIKE implementation
        match (left, right) {
            (Value::Varchar(l), Value::Varchar(r)) => {
                // Convert SQL LIKE pattern to regex
                let pattern = r.replace('%', ".*").replace('_', ".");
                Ok(l.contains(&pattern))
            }
            _ => Ok(false),
        }
    }

    fn ilike_comparison(&self, left: &Value, right: &Value) -> PrismDBResult<bool> {
        // Case-insensitive LIKE
        match (left, right) {
            (Value::Varchar(l), Value::Varchar(r)) => {
                let pattern = r.to_lowercase().replace('%', ".*").replace('_', ".");
                Ok(l.to_lowercase().contains(&pattern))
            }
            _ => Ok(false),
        }
    }

    fn in_comparison(&self, left: &Value, right: &Value) -> PrismDBResult<bool> {
        // Simplified IN implementation
        match right {
            Value::List(values) => Ok(values.iter().any(|v| v == left)),
            _ => Ok(false),
        }
    }

    fn is_distinct_from(&self, left: &Value, right: &Value) -> PrismDBResult<bool> {
        // IS DISTINCT FROM treats NULL as distinct from NULL
        if left.is_null() && right.is_null() {
            Ok(false)
        } else if left.is_null() || right.is_null() {
            Ok(true)
        } else {
            Ok(left.compare(right)? != std::cmp::Ordering::Equal)
        }
    }
}

impl fmt::Display for ComparisonType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ComparisonType::Equal => write!(f, "="),
            ComparisonType::NotEqual => write!(f, "!="),
            ComparisonType::LessThan => write!(f, "<"),
            ComparisonType::LessThanOrEqual => write!(f, "<="),
            ComparisonType::GreaterThan => write!(f, ">"),
            ComparisonType::GreaterThanOrEqual => write!(f, ">="),
            ComparisonType::Like => write!(f, "LIKE"),
            ComparisonType::NotLike => write!(f, "NOT LIKE"),
            ComparisonType::ILike => write!(f, "ILIKE"),
            ComparisonType::NotILike => write!(f, "NOT ILIKE"),
            ComparisonType::In => write!(f, "IN"),
            ComparisonType::NotIn => write!(f, "NOT IN"),
            ComparisonType::IsDistinctFrom => write!(f, "IS DISTINCT FROM"),
            ComparisonType::IsNotDistinctFrom => write!(f, "IS NOT DISTINCT FROM"),
        }
    }
}

/// Subquery expression for scalar subqueries
pub struct SubqueryExpression {
    base: BaseExpression,
    subquery: crate::parser::ast::SelectStatement,
    #[allow(dead_code)]
    catalog: Option<Arc<std::sync::RwLock<crate::catalog::Catalog>>>,
    #[allow(dead_code)]
    transaction_manager: Option<Arc<crate::storage::transaction::TransactionManager>>,
    // Store CTE context from parent for subquery access
    ctes: std::collections::HashMap<String, crate::planner::LogicalPlan>,
    // Correlation tracking
    is_correlated: bool,
    outer_tables: Vec<String>,
}

impl std::fmt::Debug for SubqueryExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubqueryExpression")
            .field("base", &self.base)
            .field("subquery", &self.subquery)
            .field("ctes", &self.ctes.keys())
            .finish()
    }
}

impl SubqueryExpression {
    pub fn new(
        subquery: crate::parser::ast::SelectStatement,
        return_type: LogicalType,
        catalog: Option<Arc<std::sync::RwLock<crate::catalog::Catalog>>>,
        ctes: std::collections::HashMap<String, crate::planner::LogicalPlan>,
    ) -> Self {
        let (is_correlated, outer_tables) = Self::detect_correlation(&subquery);

        Self {
            base: BaseExpression::new(ExpressionType::Function, return_type),
            subquery,
            catalog,
            transaction_manager: None,
            ctes,
            is_correlated,
            outer_tables,
        }
    }

    pub fn new_with_context(
        subquery: crate::parser::ast::SelectStatement,
        return_type: LogicalType,
        catalog: Arc<std::sync::RwLock<crate::catalog::Catalog>>,
        transaction_manager: Arc<crate::storage::transaction::TransactionManager>,
        ctes: std::collections::HashMap<String, crate::planner::LogicalPlan>,
    ) -> Self {
        let (is_correlated, outer_tables) = Self::detect_correlation(&subquery);

        Self {
            base: BaseExpression::new(ExpressionType::Function, return_type),
            subquery,
            catalog: Some(catalog),
            transaction_manager: Some(transaction_manager),
            ctes,
            is_correlated,
            outer_tables,
        }
    }

    /// Detect if subquery references outer tables
    fn detect_correlation(subquery: &crate::parser::ast::SelectStatement) -> (bool, Vec<String>) {
        let mut outer_tables = std::collections::HashSet::new();

        if let Some(where_clause) = &subquery.where_clause {
            Self::collect_table_references(where_clause, &mut outer_tables);
        }

        let mut inner_tables = std::collections::HashSet::new();
        if let Some(from) = &subquery.from {
            Self::collect_inner_tables(from, &mut inner_tables);
        }

        let correlated_tables: Vec<String> = outer_tables.iter()
            .filter(|t| !inner_tables.contains(*t))
            .cloned()
            .collect();

        (!correlated_tables.is_empty(), correlated_tables)
    }

    fn collect_inner_tables(table_ref: &crate::parser::ast::TableReference, tables: &mut std::collections::HashSet<String>) {
        use crate::parser::ast::TableReference;
        match table_ref {
            TableReference::Table { name, alias } => {
                tables.insert(alias.as_ref().unwrap_or(name).clone());
            }
            TableReference::Join { left, right, .. } => {
                Self::collect_inner_tables(left, tables);
                Self::collect_inner_tables(right, tables);
            }
            TableReference::Subquery { alias, .. } => {
                tables.insert(alias.clone());
            }
            TableReference::Pivot { source, alias, .. } | TableReference::Unpivot { source, alias, .. } => {
                Self::collect_inner_tables(source, tables);
                if let Some(a) = alias {
                    tables.insert(a.clone());
                }
            }
            TableReference::TableFunction { alias, .. } => {
                if let Some(a) = alias {
                    tables.insert(a.clone());
                }
            }
        }
    }

    fn collect_table_references(expr: &crate::parser::ast::Expression, tables: &mut std::collections::HashSet<String>) {
        use crate::parser::ast::Expression;

        match expr {
            Expression::ColumnReference { table: Some(table), .. } => {
                tables.insert(table.clone());
            }
            Expression::Binary { left, right, .. } => {
                Self::collect_table_references(left, tables);
                Self::collect_table_references(right, tables);
            }
            Expression::Unary { expression, .. } => {
                Self::collect_table_references(expression, tables);
            }
            _ => {}
        }
    }

    fn substitute_outer_refs(
        expr: &mut crate::parser::ast::Expression,
        outer_values: &std::collections::HashMap<(String, String), Value>
    ) {
        use crate::parser::ast::Expression;

        match expr {
            Expression::ColumnReference { table: Some(table), column } => {
                if let Some(value) = outer_values.get(&(table.clone(), column.clone())) {
                    *expr = Expression::Literal(Self::value_to_ast_literal(value));
                }
            }
            Expression::Binary { left, right, .. } => {
                Self::substitute_outer_refs(left, outer_values);
                Self::substitute_outer_refs(right, outer_values);
            }
            Expression::Unary { expression, .. } => {
                Self::substitute_outer_refs(expression, outer_values);
            }
            _ => {}
        }
    }

    fn value_to_ast_literal(value: &Value) -> crate::parser::ast::LiteralValue {
        use crate::parser::ast::LiteralValue;
        match value {
            Value::Integer(i) => LiteralValue::Integer(*i as i64),
            Value::BigInt(i) => LiteralValue::Integer(*i as i64),
            Value::Float(f) => LiteralValue::Float(*f as f64),
            Value::Double(d) => LiteralValue::Float(*d),
            Value::Varchar(s) | Value::Char(s) => LiteralValue::String(s.clone()),
            Value::Boolean(b) => LiteralValue::Boolean(*b),
            Value::Null => LiteralValue::Null,
            _ => LiteralValue::Null,
        }
    }

    fn execute_subquery(&self, chunk: &DataChunk, row_idx: Option<usize>, context: &crate::execution::ExecutionContext) -> PrismDBResult<Value> {
        let catalog = context.catalog.clone();
        let _transaction_manager = context.transaction_manager.clone();

        let mut rewritten_subquery = self.subquery.clone();

        // Handle correlation: substitute outer references with values from current row
        if self.is_correlated && row_idx.is_some() {
            let row_idx = row_idx.unwrap();

            let catalog_lock = catalog.read().unwrap();
            let schema_ref = catalog_lock.get_schema("main")?;
            let schema_lock = schema_ref.read().unwrap();
            let all_tables = schema_lock.list_tables();

            for outer_table_alias in &self.outer_tables {
                let mut candidates = vec![outer_table_alias.clone()];

                // Add tables starting with same letter first (heuristic)
                let alias_first_char = outer_table_alias.chars().next().map(|c| c.to_lowercase().to_string());
                if let Some(first_char) = &alias_first_char {
                    for table in &all_tables {
                        if table.to_lowercase().starts_with(first_char) {
                            candidates.push(table.clone());
                        }
                    }
                }

                for table in &all_tables {
                    if !candidates.contains(table) {
                        candidates.push(table.clone());
                    }
                }

                for table_name in candidates {
                    if let Ok(table_ref) = schema_lock.get_table(&table_name) {
                        let table_lock = table_ref.read().unwrap();
                        let table_info = table_lock.get_table_info();

                        if table_info.columns.len() == chunk.column_count() {
                            let mut outer_values = std::collections::HashMap::new();
                            for (col_idx, col_info) in table_info.columns.iter().enumerate() {
                                if let Some(vector) = chunk.get_vector(col_idx) {
                                    let value = vector.get_value(row_idx)?;
                                    outer_values.insert((outer_table_alias.clone(), col_info.name.clone()), value);
                                }
                            }

                            if let Some(ref mut where_clause) = rewritten_subquery.where_clause {
                                Self::substitute_outer_refs(where_clause, &outer_values);
                            }

                            break;
                        }
                    }
                }
            }
        }

        let mut binder = crate::planner::Binder::new_with_catalog(catalog.clone());

        // Restore parent CTEs so subquery can reference them
        for (cte_name, cte_plan) in &self.ctes {
            binder.register_cte(cte_name.clone(), cte_plan.clone())?;
        }

        let logical_plan = binder.bind_select_statement(&rewritten_subquery)?;

        // Optimize the plan with catalog/transaction manager context
        let mut optimizer = crate::planner::QueryOptimizer::new()
            .with_context(context.catalog.clone(), context.transaction_manager.clone());
        let physical_plan = optimizer.optimize(logical_plan)?;

        // Execute the plan using the provided context
        let mut engine = crate::execution::ExecutionEngine::new(context.clone());
        let results = engine.execute_collect(physical_plan)?;

        // Extract scalar value from results
        if results.is_empty() {
            return Ok(Value::Null);
        }

        if results[0].len() == 0 {
            return Ok(Value::Null);
        }

        // Get the first row, first column value
        let first_chunk = &results[0];
        let first_vector = first_chunk.get_vector(0).ok_or_else(|| {
            PrismDBError::Execution("Subquery returned no columns".to_string())
        })?;

        first_vector.get_value(0)
    }
}

impl Expression for SubqueryExpression {
    fn return_type(&self) -> &LogicalType {
        &self.base.return_type
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn evaluate(&self, chunk: &DataChunk, context: &crate::execution::ExecutionContext) -> PrismDBResult<Vector> {
        if self.is_correlated {
            // Correlated subquery: execute once per row
            let mut values = Vec::with_capacity(chunk.count());
            for row_idx in 0..chunk.count() {
                let value = self.execute_subquery(chunk, Some(row_idx), context)?;
                values.push(value);
            }
            Vector::from_values(&values)
        } else {
            // Non-correlated subquery: execute once and replicate the result
            let value = self.execute_subquery(chunk, None, context)?;
            let values = vec![value; chunk.count()];
            Vector::from_values(&values)
        }
    }

    fn evaluate_row(&self, chunk: &DataChunk, row_idx: usize, context: &crate::execution::ExecutionContext) -> PrismDBResult<Value> {
        self.execute_subquery(chunk, Some(row_idx), context)
    }

    fn is_deterministic(&self) -> bool {
        false // Subqueries are generally not deterministic
    }

    fn is_nullable(&self) -> bool {
        true // Subqueries can return NULL
    }
}

/// EXISTS expression - returns true if subquery returns any rows
#[derive(Debug, Clone)]
pub struct ExistsExpression {
    base: BaseExpression,
    subquery: crate::parser::ast::SelectStatement,
    #[allow(dead_code)]
    catalog: Option<Arc<std::sync::RwLock<crate::catalog::Catalog>>>,
    #[allow(dead_code)]
    transaction_manager: Option<Arc<crate::storage::transaction::TransactionManager>>,
    ctes: std::collections::HashMap<String, crate::planner::LogicalPlan>,
    /// Whether this EXISTS contains correlated references to outer query
    is_correlated: bool,
    /// Names of outer tables that might be referenced
    outer_tables: Vec<String>,
}

impl ExistsExpression {
    pub fn new(
        subquery: crate::parser::ast::SelectStatement,
        catalog: Arc<std::sync::RwLock<crate::catalog::Catalog>>,
        transaction_manager: Arc<crate::storage::transaction::TransactionManager>,
        ctes: std::collections::HashMap<String, crate::planner::LogicalPlan>,
    ) -> Self {
        // Detect if subquery contains correlated references
        let (is_correlated, outer_tables) = Self::detect_correlation(&subquery);

        Self {
            base: BaseExpression::new(ExpressionType::Function, LogicalType::Boolean),
            subquery,
            catalog: Some(catalog),
            transaction_manager: Some(transaction_manager),
            ctes,
            is_correlated,
            outer_tables,
        }
    }

    /// Detect if subquery references outer tables (simple heuristic)
    /// Returns (is_correlated, list of potential outer table names)
    fn detect_correlation(subquery: &crate::parser::ast::SelectStatement) -> (bool, Vec<String>) {
        // Simple detection: look for column references with table qualifiers in WHERE clause
        let mut outer_tables = std::collections::HashSet::new();

        if let Some(where_clause) = &subquery.where_clause {
            Self::collect_table_references(where_clause, &mut outer_tables);
        }

        // Check if these tables are not in the FROM clause (meaning they're outer references)
        let mut inner_tables = std::collections::HashSet::new();
        if let Some(from) = &subquery.from {
            Self::collect_inner_tables(from, &mut inner_tables);
        }

        // Outer tables are those referenced but not defined in FROM
        let correlated_tables: Vec<String> = outer_tables.iter()
            .filter(|t| !inner_tables.contains(*t))
            .cloned()
            .collect();

        (!correlated_tables.is_empty(), correlated_tables)
    }

    /// Collect inner table names from TableReference recursively
    fn collect_inner_tables(table_ref: &crate::parser::ast::TableReference, tables: &mut std::collections::HashSet<String>) {
        use crate::parser::ast::TableReference;
        match table_ref {
            TableReference::Table { name, alias } => {
                tables.insert(alias.as_ref().unwrap_or(name).clone());
            }
            TableReference::Join { left, right, .. } => {
                Self::collect_inner_tables(left, tables);
                Self::collect_inner_tables(right, tables);
            }
            TableReference::Subquery { alias, .. } => {
                tables.insert(alias.clone());
            }
            TableReference::Pivot { source, alias, .. } | TableReference::Unpivot { source, alias, .. } => {
                Self::collect_inner_tables(source, tables);
                if let Some(a) = alias {
                    tables.insert(a.clone());
                }
            }
            TableReference::TableFunction { alias, .. } => {
                if let Some(a) = alias {
                    tables.insert(a.clone());
                }
            }
        }
    }

    fn collect_table_references(expr: &crate::parser::ast::Expression, tables: &mut std::collections::HashSet<String>) {
        use crate::parser::ast::Expression;

        match expr {
            Expression::ColumnReference { table: Some(table), .. } => {
                tables.insert(table.clone());
            }
            Expression::Binary { left, right, .. } => {
                Self::collect_table_references(left, tables);
                Self::collect_table_references(right, tables);
            }
            Expression::Unary { expression, .. } => {
                Self::collect_table_references(expression, tables);
            }
            _ => {}
        }
    }

    /// Substitute outer column references with constant values
    fn substitute_outer_refs(
        expr: &mut crate::parser::ast::Expression,
        outer_values: &std::collections::HashMap<(String, String), Value>
    ) {
        use crate::parser::ast::Expression;

        match expr {
            Expression::ColumnReference { table: Some(table), column } => {
                // Check if this is an outer reference
                if let Some(value) = outer_values.get(&(table.clone(), column.clone())) {
                    // Replace with constant
                    *expr = Expression::Literal(Self::value_to_ast_literal(value));
                }
            }
            Expression::Binary { left, right, .. } => {
                Self::substitute_outer_refs(left, outer_values);
                Self::substitute_outer_refs(right, outer_values);
            }
            Expression::Unary { expression, .. } => {
                Self::substitute_outer_refs(expression, outer_values);
            }
            _ => {}
        }
    }

    /// Convert a Value to an AST literal
    fn value_to_ast_literal(value: &Value) -> crate::parser::ast::LiteralValue {
        use crate::parser::ast::LiteralValue;
        match value {
            Value::Integer(i) => LiteralValue::Integer(*i as i64),
            Value::BigInt(i) => LiteralValue::Integer(*i as i64),
            Value::Float(f) => LiteralValue::Float(*f as f64),
            Value::Double(d) => LiteralValue::Float(*d),
            Value::Varchar(s) | Value::Char(s) => LiteralValue::String(s.clone()),
            Value::Boolean(b) => LiteralValue::Boolean(*b),
            Value::Null => LiteralValue::Null,
            _ => LiteralValue::Null, // Fallback for unsupported types
        }
    }

    fn execute_exists(
        &self,
        context: &crate::execution::ExecutionContext,
        outer_chunk: Option<&DataChunk>,
        outer_row_idx: Option<usize>
    ) -> PrismDBResult<Value> {
        // Use the provided catalog and transaction manager from context
        let catalog = context.catalog.clone();

        // If correlated, we need to rewrite the subquery to substitute outer values
        let mut rewritten_subquery = self.subquery.clone();

        if self.is_correlated && outer_chunk.is_some() && outer_row_idx.is_some() {
            let chunk = outer_chunk.unwrap();
            let row_idx = outer_row_idx.unwrap();

            // The outer_tables contains aliases (like "d"), but we need to find the actual table
            // The chunk contains the outer row data. We need to figure out which table it's from.
            // Strategy: Try each outer table name, and if it's an alias, try common table names

            let catalog_lock = catalog.read().unwrap();
            let schema_ref = catalog_lock.get_schema("main")?;
            let schema_lock = schema_ref.read().unwrap();

            // Get all table names from the schema to try
            let all_tables = schema_lock.list_tables();

            for outer_table_alias in &self.outer_tables {
                // Try the alias first, then try all actual table names
                // Heuristic: prefer tables that start with the same letter as the alias
                let mut candidates = vec![outer_table_alias.clone()];

                // Add tables starting with same letter first
                let alias_first_char = outer_table_alias.chars().next().map(|c| c.to_lowercase().to_string());
                if let Some(first_char) = &alias_first_char {
                    for table in &all_tables {
                        if table.to_lowercase().starts_with(first_char) {
                            candidates.push(table.clone());
                        }
                    }
                }

                // Then add remaining tables
                for table in &all_tables {
                    if !candidates.contains(table) {
                        candidates.push(table.clone());
                    }
                }

                for table_name in candidates {
                    if let Ok(table_ref) = schema_lock.get_table(&table_name) {
                        let table_lock = table_ref.read().unwrap();
                        let table_info = table_lock.get_table_info();

                        // Check if this table's column count matches the chunk
                        if table_info.columns.len() == chunk.column_count() {

                            // Build a map of (alias, column) -> value for this outer row
                            let mut outer_values = std::collections::HashMap::new();
                            for (col_idx, col_info) in table_info.columns.iter().enumerate() {
                                if let Some(vector) = chunk.get_vector(col_idx) {
                                    let value = vector.get_value(row_idx)?;
                                    outer_values.insert((outer_table_alias.clone(), col_info.name.clone()), value);
                                }
                            }

                            // Rewrite the WHERE clause to substitute outer references
                            if let Some(ref mut where_clause) = rewritten_subquery.where_clause {
                                Self::substitute_outer_refs(where_clause, &outer_values);
                            }

                            break; // Found the right table, move to next alias
                        }
                    }
                }
            }
        }

        let mut binder = crate::planner::Binder::new_with_catalog(catalog.clone());

        // Restore parent CTEs so subquery can reference them
        for (cte_name, cte_plan) in &self.ctes {
            binder.register_cte(cte_name.clone(), cte_plan.clone())?;
        }

        let logical_plan = binder.bind_select_statement(&rewritten_subquery)?;

        // Optimize the plan with catalog/transaction manager context
        let mut optimizer = crate::planner::QueryOptimizer::new()
            .with_context(context.catalog.clone(), context.transaction_manager.clone());
        let physical_plan = optimizer.optimize(logical_plan)?;

        // Execute the plan using the provided context
        let mut engine = crate::execution::ExecutionEngine::new(context.clone());
        let results = engine.execute_collect(physical_plan)?;

        // Return true if any rows exist, false otherwise
        let has_rows = !results.is_empty() && results[0].len() > 0;
        Ok(Value::Boolean(has_rows))
    }
}

impl Expression for ExistsExpression {
    fn return_type(&self) -> &LogicalType {
        &self.base.return_type
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn evaluate(&self, chunk: &DataChunk, context: &crate::execution::ExecutionContext) -> PrismDBResult<Vector> {
        if self.is_correlated {
            // Correlated EXISTS: execute once per row
            let mut values = Vec::with_capacity(chunk.count());
            for row_idx in 0..chunk.count() {
                let value = self.execute_exists(context, Some(chunk), Some(row_idx))?;
                values.push(value);
            }
            Vector::from_values(&values)
        } else {
            // Non-correlated EXISTS: execute once and replicate
            let value = self.execute_exists(context, None, None)?;
            let values = vec![value; chunk.count()];
            Vector::from_values(&values)
        }
    }

    fn evaluate_row(&self, chunk: &DataChunk, row_idx: usize, context: &crate::execution::ExecutionContext) -> PrismDBResult<Value> {
        // Execute with outer row context
        if self.is_correlated {
            self.execute_exists(context, Some(chunk), Some(row_idx))
        } else {
            self.execute_exists(context, None, None)
        }
    }

    fn is_deterministic(&self) -> bool {
        false // Subqueries are generally not deterministic
    }

    fn is_nullable(&self) -> bool {
        false // EXISTS always returns a boolean
    }
}

/// IN subquery expression - checks if value is in subquery results
#[derive(Debug, Clone)]
pub struct InSubqueryExpression {
    base: BaseExpression,
    expression: ExpressionRef,
    subquery: crate::parser::ast::SelectStatement,
    not: bool,
    #[allow(dead_code)]
    catalog: Option<Arc<std::sync::RwLock<crate::catalog::Catalog>>>,
    #[allow(dead_code)]
    transaction_manager: Option<Arc<crate::storage::transaction::TransactionManager>>,
    ctes: std::collections::HashMap<String, crate::planner::LogicalPlan>,
}

impl InSubqueryExpression {
    pub fn new(
        expression: ExpressionRef,
        subquery: crate::parser::ast::SelectStatement,
        not: bool,
        catalog: Arc<std::sync::RwLock<crate::catalog::Catalog>>,
        transaction_manager: Arc<crate::storage::transaction::TransactionManager>,
        ctes: std::collections::HashMap<String, crate::planner::LogicalPlan>,
    ) -> Self {
        Self {
            base: BaseExpression::new(ExpressionType::Function, LogicalType::Boolean),
            expression,
            subquery,
            not,
            catalog: Some(catalog),
            transaction_manager: Some(transaction_manager),
            ctes,
        }
    }

    fn execute_in_subquery(&self, value: &Value, context: &crate::execution::ExecutionContext) -> PrismDBResult<Value> {
        // Use the provided catalog and transaction manager from context
        let catalog = context.catalog.clone();

        let mut binder = crate::planner::Binder::new_with_catalog(catalog.clone());

        // Restore parent CTEs so subquery can reference them
        for (cte_name, cte_plan) in &self.ctes {
            binder.register_cte(cte_name.clone(), cte_plan.clone())?;
        }

        let logical_plan = binder.bind_select_statement(&self.subquery)?;

        // Optimize the plan with catalog/transaction manager context
        let mut optimizer = crate::planner::QueryOptimizer::new()
            .with_context(context.catalog.clone(), context.transaction_manager.clone());
        let physical_plan = optimizer.optimize(logical_plan)?;

        // Execute the plan using the provided context
        let mut engine = crate::execution::ExecutionEngine::new(context.clone());
        let results = engine.execute_collect(physical_plan)?;

        // Check if the value is in any of the results
        let mut found = false;
        for chunk in &results {
            if chunk.len() == 0 {
                continue;
            }
            let first_vector = chunk.get_vector(0).ok_or_else(|| {
                PrismDBError::Execution("IN subquery returned no columns".to_string())
            })?;

            for row_idx in 0..chunk.len() {
                let subquery_value = first_vector.get_value(row_idx)?;
                match value.compare(&subquery_value)? {
                    std::cmp::Ordering::Equal => {
                        found = true;
                        break;
                    }
                    _ => {}
                }
            }
            if found {
                break;
            }
        }

        // Apply NOT if necessary
        let result = if self.not { !found } else { found };
        Ok(Value::Boolean(result))
    }
}

impl Expression for InSubqueryExpression {
    fn return_type(&self) -> &LogicalType {
        &self.base.return_type
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn evaluate(&self, chunk: &DataChunk, context: &crate::execution::ExecutionContext) -> PrismDBResult<Vector> {
        // Evaluate the expression for each row and check if it's in the subquery
        let mut results = Vec::with_capacity(chunk.count());
        let expr_vector = self.expression.evaluate(chunk, context)?;

        for row_idx in 0..chunk.count() {
            let value = expr_vector.get_value(row_idx)?;
            let in_result = self.execute_in_subquery(&value, context)?;
            results.push(in_result);
        }

        Vector::from_values(&results)
    }

    fn evaluate_row(&self, chunk: &DataChunk, row_idx: usize, context: &crate::execution::ExecutionContext) -> PrismDBResult<Value> {
        let value = self.expression.evaluate_row(chunk, row_idx, context)?;
        self.execute_in_subquery(&value, context)
    }

    fn is_deterministic(&self) -> bool {
        false // Subqueries are generally not deterministic
    }

    fn is_nullable(&self) -> bool {
        false // IN always returns a boolean
    }

    fn children(&self) -> Vec<ExpressionRef> {
        vec![self.expression.clone()]
    }
}

/// CASE expression for conditional logic
/// Supports both simple CASE (with operand) and searched CASE (conditions only)
pub struct CaseExpression {
    base: BaseExpression,
    operand: Option<ExpressionRef>,
    conditions: Vec<ExpressionRef>,
    results: Vec<ExpressionRef>,
    else_result: Option<ExpressionRef>,
}

impl std::fmt::Debug for CaseExpression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CaseExpression")
            .field("operand", &self.operand)
            .field("conditions_count", &self.conditions.len())
            .field("results_count", &self.results.len())
            .field("has_else", &self.else_result.is_some())
            .field("return_type", &self.base.return_type)
            .finish()
    }
}

impl CaseExpression {
    pub fn new(
        operand: Option<ExpressionRef>,
        conditions: Vec<ExpressionRef>,
        results: Vec<ExpressionRef>,
        else_result: Option<ExpressionRef>,
        return_type: LogicalType,
    ) -> PrismDBResult<Self> {
        if conditions.len() != results.len() {
            return Err(PrismDBError::InvalidValue(
                "CASE expression must have same number of conditions and results".to_string(),
            ));
        }

        Ok(Self {
            base: BaseExpression::new(ExpressionType::Function, return_type),
            operand,
            conditions,
            results,
            else_result,
        })
    }
}

impl Expression for CaseExpression {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn return_type(&self) -> &LogicalType {
        &self.base.return_type
    }

    fn evaluate(&self, chunk: &DataChunk, context: &crate::execution::ExecutionContext) -> PrismDBResult<Vector> {
        let num_rows = chunk.len();
        let mut result_values = Vec::with_capacity(num_rows);

        for row_idx in 0..num_rows {
            let value = self.evaluate_row(chunk, row_idx, context)?;
            result_values.push(value);
        }

        Vector::from_values(&result_values)
    }

    fn children(&self) -> Vec<ExpressionRef> {
        let mut children = Vec::new();
        if let Some(ref op) = self.operand {
            children.push(op.clone());
        }
        for cond in &self.conditions {
            children.push(cond.clone());
        }
        for result in &self.results {
            children.push(result.clone());
        }
        if let Some(ref else_res) = self.else_result {
            children.push(else_res.clone());
        }
        children
    }

    fn evaluate_row(&self, chunk: &DataChunk, row_idx: usize, context: &crate::execution::ExecutionContext) -> PrismDBResult<Value> {
        // Evaluate operand if this is a simple CASE
        let operand_value = if let Some(ref op) = self.operand {
            Some(op.evaluate_row(chunk, row_idx, context)?)
        } else {
            None
        };

        // Evaluate each condition
        for (i, condition) in self.conditions.iter().enumerate() {
            let condition_value = condition.evaluate_row(chunk, row_idx, context)?;

            let matches = if let Some(ref op_val) = operand_value {
                // Simple CASE: compare operand with condition value
                match op_val.compare(&condition_value)? {
                    std::cmp::Ordering::Equal => true,
                    _ => false,
                }
            } else {
                // Searched CASE: evaluate condition as boolean
                match condition_value {
                    Value::Boolean(b) => b,
                    _ => false,
                }
            };

            if matches {
                return self.results[i].evaluate_row(chunk, row_idx, context);
            }
        }

        // No condition matched, return ELSE result or NULL
        if let Some(ref else_res) = self.else_result {
            else_res.evaluate_row(chunk, row_idx, context)
        } else {
            Ok(Value::Null)
        }
    }

    fn is_deterministic(&self) -> bool {
        // CASE is deterministic if all sub-expressions are deterministic
        let operand_det = self.operand.as_ref().map_or(true, |e| e.is_deterministic());
        let conds_det = self.conditions.iter().all(|e| e.is_deterministic());
        let results_det = self.results.iter().all(|e| e.is_deterministic());
        let else_det = self.else_result.as_ref().map_or(true, |e| e.is_deterministic());
        operand_det && conds_det && results_det && else_det
    }

    fn is_nullable(&self) -> bool {
        // CASE can return NULL if any result is nullable or if there's no ELSE clause
        if self.else_result.is_none() {
            return true;
        }
        self.results.iter().any(|e| e.is_nullable())
            || self.else_result.as_ref().map_or(false, |e| e.is_nullable())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Value;

    #[test]
    fn test_constant_expression() -> PrismDBResult<()> {
        let expr = ConstantExpression::new(Value::integer(42))?;
        assert_eq!(expr.return_type(), &LogicalType::Integer);
        assert!(expr.is_deterministic());
        assert!(!expr.is_nullable());
        Ok(())
    }

    #[test]
    fn test_column_ref_expression() {
        let expr = ColumnRefExpression::new(0, "id".to_string(), LogicalType::Integer);
        assert_eq!(expr.return_type(), &LogicalType::Integer);
        assert_eq!(expr.column_index(), 0);
        assert_eq!(expr.column_name(), "id");
        assert!(expr.is_deterministic());
    }

    #[test]
    fn test_comparison_expression() -> PrismDBResult<()> {
        let left = Arc::new(ConstantExpression::new(Value::integer(10))?) as ExpressionRef;
        let right = Arc::new(ConstantExpression::new(Value::integer(20))?) as ExpressionRef;
        let expr = ComparisonExpression::new(ComparisonType::LessThan, left, right);

        assert_eq!(expr.return_type(), &LogicalType::Boolean);
        assert!(expr.is_deterministic());
        assert!(!expr.is_nullable());
        Ok(())
    }

    #[test]
    fn test_cast_expression() -> PrismDBResult<()> {
        let child = Arc::new(ConstantExpression::new(Value::integer(42))?) as ExpressionRef;
        let expr = CastExpression::new(child, LogicalType::Varchar, false);

        assert_eq!(expr.return_type(), &LogicalType::Varchar);
        assert!(expr.is_deterministic());
        assert!(!expr.try_cast());
        Ok(())
    }
}
