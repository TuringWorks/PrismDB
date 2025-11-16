//! Expression Binding
//!
//! This module handles binding expressions to catalog objects and resolving
//! column references, function calls, and type conversions.

use crate::common::{PrismDBResult, error::PrismDBError};
use crate::expression::expression::{
    CastExpression, ColumnRefExpression, ComparisonExpression, ComparisonType, ConstantExpression,
    ExpressionRef, FunctionExpression,
};
use crate::parser::ast;
use crate::types::{LogicalType, Value};
use std::collections::HashMap;
use std::sync::Arc;

/// Expression binder context
#[derive(Debug, Clone)]
pub struct BinderContext {
    pub alias_map: HashMap<String, ColumnBinding>,
    pub column_bindings: Vec<ColumnBinding>,
    pub depth: usize,
}

/// Column binding information
#[derive(Debug, Clone)]
pub struct ColumnBinding {
    pub table_index: usize,
    pub column_index: usize,
    pub column_name: String,
    pub column_type: LogicalType,
}

impl ColumnBinding {
    pub fn new(
        table_index: usize,
        column_index: usize,
        column_name: String,
        column_type: LogicalType,
    ) -> Self {
        Self {
            table_index,
            column_index,
            column_name,
            column_type,
        }
    }
}

/// Expression binder
pub struct ExpressionBinder {
    context: BinderContext,
    catalog: Option<Arc<std::sync::RwLock<crate::catalog::Catalog>>>,
    transaction_manager: Option<Arc<crate::storage::transaction::TransactionManager>>,
    ctes: std::collections::HashMap<String, crate::planner::LogicalPlan>,
}

impl ExpressionBinder {
    pub fn new(context: BinderContext) -> Self {
        Self {
            context,
            catalog: None,
            transaction_manager: None,
            ctes: std::collections::HashMap::new(),
        }
    }

    pub fn new_with_catalog(
        context: BinderContext,
        catalog: Arc<std::sync::RwLock<crate::catalog::Catalog>>,
    ) -> Self {
        Self {
            context,
            catalog: Some(catalog),
            transaction_manager: None,
            ctes: std::collections::HashMap::new(),
        }
    }

    pub fn new_with_context(
        context: BinderContext,
        catalog: Arc<std::sync::RwLock<crate::catalog::Catalog>>,
        transaction_manager: Arc<crate::storage::transaction::TransactionManager>,
    ) -> Self {
        Self {
            context,
            catalog: Some(catalog),
            transaction_manager: Some(transaction_manager),
            ctes: std::collections::HashMap::new(),
        }
    }

    pub fn new_with_ctes(
        context: BinderContext,
        catalog: Arc<std::sync::RwLock<crate::catalog::Catalog>>,
        transaction_manager: Arc<crate::storage::transaction::TransactionManager>,
        ctes: std::collections::HashMap<String, crate::planner::LogicalPlan>,
    ) -> Self {
        Self {
            context,
            catalog: Some(catalog),
            transaction_manager: Some(transaction_manager),
            ctes,
        }
    }

    /// Check if a function name is an aggregate function
    fn is_aggregate_function_name(name: &str) -> bool {
        matches!(
            name.to_uppercase().as_str(),
            "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "STDDEV" | "VARIANCE" | "STRING_AGG"
        )
    }

    /// Bind a parser AST expression to an execution expression
    pub fn bind_expression(&self, expr: &ast::Expression) -> PrismDBResult<ExpressionRef> {
        use crate::common::error::PrismDBError;

        match expr {
            ast::Expression::Literal(literal) => self.bind_literal(literal),
            ast::Expression::ColumnReference { table, column } => {
                self.bind_column_ref(table.as_deref(), column)
            }
            ast::Expression::FunctionCall {
                name,
                arguments,
                distinct: _,
            } => {
                // Auto-detect if this is actually an aggregate function
                let is_agg = Self::is_aggregate_function_name(name);
                self.bind_function_call(name, arguments, is_agg)
            }
            ast::Expression::AggregateFunction {
                name,
                arguments,
                distinct: _,
            } => self.bind_function_call(name, arguments, true),
            ast::Expression::Cast {
                expression,
                data_type,
            } => self.bind_cast(expression, data_type),
            ast::Expression::Binary {
                left,
                operator,
                right,
            } => self.bind_binary_op(left, operator, right),
            ast::Expression::Unary {
                operator,
                expression,
            } => self.bind_unary_op(operator, expression),
            ast::Expression::Wildcard => {
                // Wildcard is typically handled at a higher level (in SELECT list expansion)
                // For now, return a placeholder constant
                // TODO: This should be handled properly in the context where it's used
                let constant = ConstantExpression::new(Value::Integer(1))?;
                Ok(Arc::new(constant))
            }
            ast::Expression::QualifiedWildcard { table: _ } => {
                // QualifiedWildcard is typically handled at a higher level
                // For now, return a placeholder constant
                let constant = ConstantExpression::new(Value::Integer(1))?;
                Ok(Arc::new(constant))
            }
            ast::Expression::Subquery(subquery) => {
                use crate::expression::expression::SubqueryExpression;

                // Create a subquery expression
                // TODO: Infer return type from subquery schema
                let return_type = LogicalType::Double; // Use DOUBLE for aggregate results

                let subquery_expr = if let (Some(catalog), Some(tm)) = (&self.catalog, &self.transaction_manager) {
                    SubqueryExpression::new_with_context(
                        (**subquery).clone(),
                        return_type,
                        catalog.clone(),
                        tm.clone(),
                        self.ctes.clone(),
                    )
                } else {
                    SubqueryExpression::new(
                        (**subquery).clone(),
                        return_type,
                        self.catalog.clone(),
                        self.ctes.clone(),
                    )
                };

                Ok(Arc::new(subquery_expr))
            }
            ast::Expression::Exists(subquery) => {
                // EXISTS subqueries return a boolean indicating if the subquery returns any rows
                self.bind_exists(subquery)
            }
            ast::Expression::InSubquery {
                expression,
                subquery,
                not,
            } => {
                // IN subqueries check if an expression is in the result set of a subquery
                self.bind_in_subquery(expression, subquery, *not)
            }
            ast::Expression::IsNull(expression) => {
                // Bind IS NULL expression as a function call
                self.bind_is_null(expression, false)
            }
            ast::Expression::IsNotNull(expression) => {
                // Bind IS NOT NULL expression as a function call
                self.bind_is_null(expression, true)
            }
            ast::Expression::Case {
                operand,
                conditions,
                results,
                else_result,
            } => self.bind_case(operand, conditions, results, else_result),
            _ => Err(PrismDBError::NotImplemented(format!(
                "Binding not implemented for expression: {:?}",
                expr
            ))),
        }
    }

    /// Bind a literal value
    fn bind_literal(&self, literal: &ast::LiteralValue) -> PrismDBResult<ExpressionRef> {
        let value = match literal {
            ast::LiteralValue::Null => Value::Null,
            ast::LiteralValue::Boolean(b) => Value::Boolean(*b),
            ast::LiteralValue::Integer(i) => Value::Integer(*i as i32),
            ast::LiteralValue::Float(f) => Value::Double(*f),
            ast::LiteralValue::String(s) => Value::Varchar(s.clone()),
            ast::LiteralValue::Date(d) => Value::Varchar(d.clone()), // TODO: proper date handling
            ast::LiteralValue::Time(t) => Value::Varchar(t.clone()), // TODO: proper time handling
            ast::LiteralValue::Timestamp(ts) => Value::Varchar(ts.clone()), // TODO: proper timestamp handling
            ast::LiteralValue::Interval { value, field } => {
                Value::Varchar(format!("{} {}", value, field))
            } // TODO: proper interval handling
        };

        let constant = ConstantExpression::new(value)?;
        Ok(Arc::new(constant))
    }

    /// Bind a column reference
    fn bind_column_ref(&self, table: Option<&str>, column: &str) -> PrismDBResult<ExpressionRef> {
        // Try qualified name first if table is specified
        let column_to_lookup = if let Some(table_name) = table {
            format!("{}.{}", table_name, column)
        } else {
            column.to_string()
        };

        let binding = match self.bind_column_reference(&column_to_lookup) {
            Ok(b) => b,
            Err(_) if table.is_some() => {
                // If qualified lookup failed, try unqualified
                self.bind_column_reference(column)?
            }
            Err(e) => return Err(e),
        };

        let col_ref = ColumnRefExpression::new(
            binding.column_index,
            binding.column_name.clone(),
            binding.column_type.clone(),
        );
        Ok(Arc::new(col_ref))
    }

    /// Bind a function call
    fn bind_function_call(
        &self,
        name: &str,
        arguments: &[ast::Expression],
        is_aggregate: bool,
    ) -> PrismDBResult<ExpressionRef> {
        // Handle special case: COUNT(*) where * is a wildcard
        if is_aggregate && name.to_uppercase() == "COUNT" && arguments.len() == 1 {
            if matches!(arguments[0], ast::Expression::Wildcard) {
                // COUNT(*) - special case, no arguments needed
                let return_type = LogicalType::Integer;
                let func_expr = FunctionExpression::new("COUNT".to_string(), return_type, vec![]);
                return Ok(Arc::new(func_expr));
            }
        }

        // Bind all arguments first
        let mut bound_args = Vec::new();
        let mut arg_types = Vec::new();

        for arg in arguments {
            let bound_arg = self.bind_expression(arg)?;
            arg_types.push(bound_arg.return_type().clone());
            bound_args.push(bound_arg);
        }

        // Determine return type
        let return_type = if is_aggregate {
            self.bind_aggregate_function(name, &arg_types)?
        } else {
            self.bind_function(name, &arg_types)?
        };

        let func_expr = FunctionExpression::new(name.to_string(), return_type, bound_args);

        Ok(Arc::new(func_expr))
    }

    /// Bind a cast expression
    fn bind_cast(
        &self,
        expression: &ast::Expression,
        target_type: &LogicalType,
    ) -> PrismDBResult<ExpressionRef> {
        let bound_expr = self.bind_expression(expression)?;

        // Check if cast is valid
        self.check_cast_validity(bound_expr.return_type(), target_type)?;

        let cast_expr = CastExpression::new(bound_expr, target_type.clone(), false);
        Ok(Arc::new(cast_expr))
    }

    /// Bind a binary operation
    fn bind_binary_op(
        &self,
        left: &ast::Expression,
        op: &ast::BinaryOperator,
        right: &ast::Expression,
    ) -> PrismDBResult<ExpressionRef> {
        let bound_left = self.bind_expression(left)?;
        let bound_right = self.bind_expression(right)?;

        // Handle comparison operators with ComparisonExpression
        match op {
            ast::BinaryOperator::Equals => {
                let comp_expr =
                    ComparisonExpression::new(ComparisonType::Equal, bound_left, bound_right);
                Ok(Arc::new(comp_expr))
            }
            ast::BinaryOperator::NotEquals => {
                let comp_expr =
                    ComparisonExpression::new(ComparisonType::NotEqual, bound_left, bound_right);
                Ok(Arc::new(comp_expr))
            }
            ast::BinaryOperator::LessThan => {
                let comp_expr =
                    ComparisonExpression::new(ComparisonType::LessThan, bound_left, bound_right);
                Ok(Arc::new(comp_expr))
            }
            ast::BinaryOperator::LessThanOrEqual => {
                let comp_expr = ComparisonExpression::new(
                    ComparisonType::LessThanOrEqual,
                    bound_left,
                    bound_right,
                );
                Ok(Arc::new(comp_expr))
            }
            ast::BinaryOperator::GreaterThan => {
                let comp_expr =
                    ComparisonExpression::new(ComparisonType::GreaterThan, bound_left, bound_right);
                Ok(Arc::new(comp_expr))
            }
            ast::BinaryOperator::GreaterThanOrEqual => {
                let comp_expr = ComparisonExpression::new(
                    ComparisonType::GreaterThanOrEqual,
                    bound_left,
                    bound_right,
                );
                Ok(Arc::new(comp_expr))
            }
            // Arithmetic and logical operators as function calls
            ast::BinaryOperator::Add => {
                let return_type = TypeInference::infer_binary_type(
                    bound_left.return_type(),
                    bound_right.return_type(),
                )?;
                let func_expr = FunctionExpression::new(
                    "ADD".to_string(),
                    return_type,
                    vec![bound_left, bound_right],
                );
                Ok(Arc::new(func_expr))
            }
            ast::BinaryOperator::Subtract => {
                let return_type = TypeInference::infer_binary_type(
                    bound_left.return_type(),
                    bound_right.return_type(),
                )?;
                let func_expr = FunctionExpression::new(
                    "SUBTRACT".to_string(),
                    return_type,
                    vec![bound_left, bound_right],
                );
                Ok(Arc::new(func_expr))
            }
            ast::BinaryOperator::Multiply => {
                let return_type = TypeInference::infer_binary_type(
                    bound_left.return_type(),
                    bound_right.return_type(),
                )?;
                let func_expr = FunctionExpression::new(
                    "MULTIPLY".to_string(),
                    return_type,
                    vec![bound_left, bound_right],
                );
                Ok(Arc::new(func_expr))
            }
            ast::BinaryOperator::Divide => {
                let return_type = TypeInference::infer_binary_type(
                    bound_left.return_type(),
                    bound_right.return_type(),
                )?;
                let func_expr = FunctionExpression::new(
                    "DIVIDE".to_string(),
                    return_type,
                    vec![bound_left, bound_right],
                );
                Ok(Arc::new(func_expr))
            }
            ast::BinaryOperator::Modulo => {
                let return_type = TypeInference::infer_binary_type(
                    bound_left.return_type(),
                    bound_right.return_type(),
                )?;
                let func_expr = FunctionExpression::new(
                    "MODULO".to_string(),
                    return_type,
                    vec![bound_left, bound_right],
                );
                Ok(Arc::new(func_expr))
            }
            ast::BinaryOperator::And => {
                let func_expr = FunctionExpression::new(
                    "AND".to_string(),
                    LogicalType::Boolean,
                    vec![bound_left, bound_right],
                );
                Ok(Arc::new(func_expr))
            }
            ast::BinaryOperator::Or => {
                let func_expr = FunctionExpression::new(
                    "OR".to_string(),
                    LogicalType::Boolean,
                    vec![bound_left, bound_right],
                );
                Ok(Arc::new(func_expr))
            }
            ast::BinaryOperator::Like => {
                let func_expr = FunctionExpression::new(
                    "LIKE".to_string(),
                    LogicalType::Boolean,
                    vec![bound_left, bound_right],
                );
                Ok(Arc::new(func_expr))
            }
            _ => Err(crate::common::error::PrismDBError::NotImplemented(format!(
                "Binary operator {:?} not implemented",
                op
            ))),
        }
    }

    /// Bind a unary operation
    fn bind_unary_op(
        &self,
        op: &ast::UnaryOperator,
        expression: &ast::Expression,
    ) -> PrismDBResult<ExpressionRef> {
        use crate::common::error::PrismDBError;

        let bound_expr = self.bind_expression(expression)?;

        match op {
            ast::UnaryOperator::Not => {
                // NOT operation - create a function call
                let func_expr = FunctionExpression::new(
                    "NOT".to_string(),
                    LogicalType::Boolean,
                    vec![bound_expr],
                );
                Ok(Arc::new(func_expr))
            }
            ast::UnaryOperator::Minus => {
                // Negate - create a function call
                let return_type = bound_expr.return_type().clone();
                let func_expr =
                    FunctionExpression::new("NEGATE".to_string(), return_type, vec![bound_expr]);
                Ok(Arc::new(func_expr))
            }
            _ => Err(PrismDBError::NotImplemented(format!(
                "Unary operator {:?} not implemented",
                op
            ))),
        }
    }

    /// Bind IS NULL / IS NOT NULL expression
    fn bind_is_null(
        &self,
        expression: &ast::Expression,
        is_not_null: bool,
    ) -> PrismDBResult<ExpressionRef> {
        use crate::expression::expression::FunctionExpression;

        let bound_expr = self.bind_expression(expression)?;

        // Create IS_NULL or IS_NOT_NULL function
        let function_name = if is_not_null { "IS_NOT_NULL" } else { "IS_NULL" };
        let func_expr = FunctionExpression::new(
            function_name.to_string(),
            LogicalType::Boolean,
            vec![bound_expr],
        );
        Ok(Arc::new(func_expr))
    }

    /// Bind an expression to a column reference
    pub fn bind_column_reference(&self, column_name: &str) -> PrismDBResult<ColumnBinding> {
        // First try exact match
        if let Some(binding) = self.context.alias_map.get(column_name) {
            return Ok(binding.clone());
        }

        // Then try to find exact match in column bindings
        for binding in &self.context.column_bindings {
            if binding.column_name == column_name {
                return Ok(binding.clone());
            }
        }

        // If no exact match, try to match unqualified name against qualified columns
        // For example, "age" should match "users.age" or "u.age"
        let mut matches = Vec::new();
        for binding in &self.context.column_bindings {
            // Check if this is a qualified column name (contains '.')
            if let Some(dot_pos) = binding.column_name.rfind('.') {
                let unqualified = &binding.column_name[dot_pos + 1..];
                if unqualified == column_name {
                    matches.push(binding.clone());
                }
            }
        }

        // If we found exactly one match, return it
        if matches.len() == 1 {
            return Ok(matches[0].clone());
        } else if matches.len() > 1 {
            return Err(crate::common::error::PrismDBError::InvalidValue(format!(
                "Column '{}' is ambiguous",
                column_name
            )));
        }

        Err(crate::common::error::PrismDBError::InvalidValue(format!(
            "Column '{}' not found",
            column_name
        )))
    }

    /// Bind a function call
    pub fn bind_function(
        &self,
        function_name: &str,
        args: &[LogicalType],
    ) -> PrismDBResult<LogicalType> {
        // This would look up the function in the catalog
        // For now, return a basic implementation
        match function_name.to_uppercase().as_str() {
            "ABS" => {
                if args.len() != 1 {
                    return Err(crate::common::error::PrismDBError::InvalidValue(
                        "ABS requires exactly 1 argument".to_string(),
                    ));
                }
                Ok(args[0].clone())
            }
            "LENGTH" => {
                if args.len() != 1 {
                    return Err(crate::common::error::PrismDBError::InvalidValue(
                        "LENGTH requires exactly 1 argument".to_string(),
                    ));
                }
                if !args[0].is_string() {
                    return Err(crate::common::error::PrismDBError::InvalidValue(
                        "LENGTH requires string argument".to_string(),
                    ));
                }
                Ok(LogicalType::Integer)
            }
            "SUBSTRING" => {
                if args.len() < 2 || args.len() > 3 {
                    return Err(crate::common::error::PrismDBError::InvalidValue(
                        "SUBSTRING requires 2 or 3 arguments".to_string(),
                    ));
                }
                if !args[0].is_string() {
                    return Err(crate::common::error::PrismDBError::InvalidValue(
                        "SUBSTRING first argument must be string".to_string(),
                    ));
                }
                Ok(LogicalType::Varchar)
            }
            "COALESCE" => {
                if args.is_empty() {
                    return Err(crate::common::error::PrismDBError::InvalidValue(
                        "COALESCE requires at least 1 argument".to_string(),
                    ));
                }
                // Return the type of the first non-Invalid argument
                for arg_type in args {
                    if *arg_type != LogicalType::Invalid {
                        return Ok(arg_type.clone());
                    }
                }
                // If all args are Invalid, return Integer as fallback
                Ok(LogicalType::Integer)
            }
            "NULLIF" => {
                if args.len() != 2 {
                    return Err(crate::common::error::PrismDBError::InvalidValue(
                        "NULLIF requires exactly 2 arguments".to_string(),
                    ));
                }
                Ok(args[0].clone())
            }
            "IS_NULL" | "IS_NOT_NULL" => {
                if args.len() != 1 {
                    return Err(crate::common::error::PrismDBError::InvalidValue(format!(
                        "{} requires exactly 1 argument",
                        function_name
                    )));
                }
                // IS NULL/IS NOT NULL always returns BOOLEAN
                Ok(LogicalType::Boolean)
            }
            "CONCAT" => {
                if args.is_empty() {
                    return Err(crate::common::error::PrismDBError::InvalidValue(
                        "CONCAT requires at least 1 argument".to_string(),
                    ));
                }
                // CONCAT returns VARCHAR
                Ok(LogicalType::Varchar)
            }
            _ => Err(crate::common::error::PrismDBError::InvalidValue(format!(
                "Unknown function: {}",
                function_name
            ))),
        }
    }

    /// Bind an aggregate function
    pub fn bind_aggregate_function(
        &self,
        function_name: &str,
        args: &[LogicalType],
    ) -> PrismDBResult<LogicalType> {
        match function_name.to_uppercase().as_str() {
            "COUNT" => {
                if args.len() > 1 {
                    return Err(crate::common::error::PrismDBError::InvalidValue(
                        "COUNT accepts at most 1 argument".to_string(),
                    ));
                }
                Ok(LogicalType::Integer)
            }
            "SUM" => {
                if args.len() != 1 {
                    return Err(crate::common::error::PrismDBError::InvalidValue(
                        "SUM requires exactly 1 argument".to_string(),
                    ));
                }
                if !args[0].is_numeric() {
                    return Err(crate::common::error::PrismDBError::InvalidValue(
                        "SUM requires numeric argument".to_string(),
                    ));
                }
                // Return the widest numeric type
                match args[0] {
                    LogicalType::HugeInt => Ok(LogicalType::HugeInt),
                    LogicalType::Double => Ok(LogicalType::Double),
                    LogicalType::Float => Ok(LogicalType::Double),
                    LogicalType::BigInt => Ok(LogicalType::BigInt),
                    LogicalType::Integer => Ok(LogicalType::BigInt),
                    LogicalType::SmallInt => Ok(LogicalType::BigInt),
                    LogicalType::TinyInt => Ok(LogicalType::BigInt),
                    _ => Ok(LogicalType::Double),
                }
            }
            "AVG" => {
                if args.len() != 1 {
                    return Err(crate::common::error::PrismDBError::InvalidValue(
                        "AVG requires exactly 1 argument".to_string(),
                    ));
                }
                if !args[0].is_numeric() {
                    return Err(crate::common::error::PrismDBError::InvalidValue(
                        "AVG requires numeric argument".to_string(),
                    ));
                }
                Ok(LogicalType::Double)
            }
            "MIN" | "MAX" => {
                if args.len() != 1 {
                    return Err(crate::common::error::PrismDBError::InvalidValue(format!(
                        "{} requires exactly 1 argument",
                        function_name
                    )));
                }
                Ok(args[0].clone())
            }
            _ => Err(crate::common::error::PrismDBError::InvalidValue(format!(
                "Unknown aggregate function: {}",
                function_name
            ))),
        }
    }

    /// Bind a window function
    pub fn bind_window_function(
        &self,
        function_name: &str,
        args: &[LogicalType],
    ) -> PrismDBResult<LogicalType> {
        match function_name.to_uppercase().as_str() {
            "ROW_NUMBER" | "RANK" | "DENSE_RANK" => {
                if !args.is_empty() {
                    return Err(crate::common::error::PrismDBError::InvalidValue(format!(
                        "{} takes no arguments",
                        function_name
                    )));
                }
                Ok(LogicalType::BigInt)
            }
            "PERCENT_RANK" | "CUME_DIST" => {
                if !args.is_empty() {
                    return Err(crate::common::error::PrismDBError::InvalidValue(format!(
                        "{} takes no arguments",
                        function_name
                    )));
                }
                Ok(LogicalType::Double)
            }
            "NTILE" => {
                if args.len() != 1 {
                    return Err(crate::common::error::PrismDBError::InvalidValue(
                        "NTILE requires exactly 1 argument".to_string(),
                    ));
                }
                if !args[0].is_integral() {
                    return Err(crate::common::error::PrismDBError::InvalidValue(
                        "NTILE requires integer argument".to_string(),
                    ));
                }
                Ok(LogicalType::BigInt)
            }
            "LAG" | "LEAD" => {
                if args.len() < 1 || args.len() > 3 {
                    return Err(crate::common::error::PrismDBError::InvalidValue(format!(
                        "{} requires 1 to 3 arguments",
                        function_name
                    )));
                }
                Ok(args[0].clone())
            }
            "FIRST_VALUE" | "LAST_VALUE" => {
                if args.len() != 1 {
                    return Err(crate::common::error::PrismDBError::InvalidValue(format!(
                        "{} requires exactly 1 argument",
                        function_name
                    )));
                }
                Ok(args[0].clone())
            }
            "NTH_VALUE" => {
                if args.len() != 2 {
                    return Err(crate::common::error::PrismDBError::InvalidValue(
                        "NTH_VALUE requires exactly 2 arguments".to_string(),
                    ));
                }
                if !args[1].is_integral() {
                    return Err(crate::common::error::PrismDBError::InvalidValue(
                        "NTH_VALUE second argument must be integer".to_string(),
                    ));
                }
                Ok(args[0].clone())
            }
            _ => Err(crate::common::error::PrismDBError::InvalidValue(format!(
                "Unknown window function: {}",
                function_name
            ))),
        }
    }

    /// Check if a cast operation is valid
    pub fn check_cast_validity(
        &self,
        from_type: &LogicalType,
        to_type: &LogicalType,
    ) -> PrismDBResult<bool> {
        // Check if cast is valid
        if from_type == to_type {
            return Ok(true);
        }

        // Basic cast compatibility rules
        match (from_type, to_type) {
            // Numeric casts
            (LogicalType::TinyInt, LogicalType::SmallInt) => Ok(true),
            (LogicalType::TinyInt, LogicalType::Integer) => Ok(true),
            (LogicalType::TinyInt, LogicalType::BigInt) => Ok(true),
            (LogicalType::TinyInt, LogicalType::HugeInt) => Ok(true),
            (LogicalType::TinyInt, LogicalType::Float) => Ok(true),
            (LogicalType::TinyInt, LogicalType::Double) => Ok(true),

            (LogicalType::SmallInt, LogicalType::TinyInt) => Ok(true),
            (LogicalType::SmallInt, LogicalType::Integer) => Ok(true),
            (LogicalType::SmallInt, LogicalType::BigInt) => Ok(true),
            (LogicalType::SmallInt, LogicalType::HugeInt) => Ok(true),
            (LogicalType::SmallInt, LogicalType::Float) => Ok(true),
            (LogicalType::SmallInt, LogicalType::Double) => Ok(true),

            (LogicalType::Integer, LogicalType::TinyInt) => Ok(true),
            (LogicalType::Integer, LogicalType::SmallInt) => Ok(true),
            (LogicalType::Integer, LogicalType::BigInt) => Ok(true),
            (LogicalType::Integer, LogicalType::HugeInt) => Ok(true),
            (LogicalType::Integer, LogicalType::Float) => Ok(true),
            (LogicalType::Integer, LogicalType::Double) => Ok(true),

            (LogicalType::BigInt, LogicalType::TinyInt) => Ok(true),
            (LogicalType::BigInt, LogicalType::SmallInt) => Ok(true),
            (LogicalType::BigInt, LogicalType::Integer) => Ok(true),
            (LogicalType::BigInt, LogicalType::HugeInt) => Ok(true),
            (LogicalType::BigInt, LogicalType::Float) => Ok(true),
            (LogicalType::BigInt, LogicalType::Double) => Ok(true),

            // String casts
            (LogicalType::Varchar, _) => Ok(true),
            (_, LogicalType::Varchar) => Ok(true),

            // Date/Time casts
            (LogicalType::Date, LogicalType::Timestamp) => Ok(true),
            (LogicalType::Time, LogicalType::Timestamp) => Ok(true),
            (LogicalType::Timestamp, LogicalType::Date) => Ok(true),
            (LogicalType::Timestamp, LogicalType::Time) => Ok(true),

            _ => Err(crate::common::error::PrismDBError::InvalidValue(format!(
                "Cannot cast from {:?} to {:?}",
                from_type, to_type
            ))),
        }
    }

    /// Resolve implicit cast
    pub fn resolve_implicit_cast(
        &self,
        from_type: &LogicalType,
        to_type: &LogicalType,
    ) -> PrismDBResult<bool> {
        // Implicit casts are more restrictive than explicit casts
        match (from_type, to_type) {
            // Widening numeric casts
            (LogicalType::TinyInt, LogicalType::SmallInt) => Ok(true),
            (LogicalType::TinyInt, LogicalType::Integer) => Ok(true),
            (LogicalType::TinyInt, LogicalType::BigInt) => Ok(true),
            (LogicalType::TinyInt, LogicalType::HugeInt) => Ok(true),
            (LogicalType::TinyInt, LogicalType::Float) => Ok(true),
            (LogicalType::TinyInt, LogicalType::Double) => Ok(true),

            (LogicalType::SmallInt, LogicalType::Integer) => Ok(true),
            (LogicalType::SmallInt, LogicalType::BigInt) => Ok(true),
            (LogicalType::SmallInt, LogicalType::HugeInt) => Ok(true),
            (LogicalType::SmallInt, LogicalType::Float) => Ok(true),
            (LogicalType::SmallInt, LogicalType::Double) => Ok(true),

            (LogicalType::Integer, LogicalType::BigInt) => Ok(true),
            (LogicalType::Integer, LogicalType::HugeInt) => Ok(true),
            (LogicalType::Integer, LogicalType::Float) => Ok(true),
            (LogicalType::Integer, LogicalType::Double) => Ok(true),

            (LogicalType::BigInt, LogicalType::HugeInt) => Ok(true),
            (LogicalType::BigInt, LogicalType::Float) => Ok(true),
            (LogicalType::BigInt, LogicalType::Double) => Ok(true),

            (LogicalType::Float, LogicalType::Double) => Ok(true),

            _ => Ok(false),
        }
    }

    /// Bind a CASE expression
    /// Supports both simple CASE (with operand) and searched CASE (conditions only)
    fn bind_case(
        &self,
        operand: &Option<Box<ast::Expression>>,
        conditions: &[ast::Expression],
        results: &[ast::Expression],
        else_result: &Option<Box<ast::Expression>>,
    ) -> PrismDBResult<ExpressionRef> {
        use crate::expression::expression::CaseExpression;

        // Bind the operand if it exists (simple CASE)
        let bound_operand = if let Some(op) = operand {
            Some(self.bind_expression(op)?)
        } else {
            None
        };

        // Bind all conditions
        let mut bound_conditions = Vec::new();
        for cond in conditions {
            bound_conditions.push(self.bind_expression(cond)?);
        }

        // Bind all results
        let mut bound_results = Vec::new();
        for result in results {
            bound_results.push(self.bind_expression(result)?);
        }

        // Bind the ELSE result if it exists
        let bound_else = if let Some(else_expr) = else_result {
            Some(self.bind_expression(else_expr)?)
        } else {
            None
        };

        // Infer return type from the first result expression
        let return_type = if !bound_results.is_empty() {
            bound_results[0].return_type().clone()
        } else {
            LogicalType::Varchar // Default to VARCHAR if no results
        };

        let case_expr = CaseExpression::new(
            bound_operand,
            bound_conditions,
            bound_results,
            bound_else,
            return_type,
        )?;

        Ok(Arc::new(case_expr))
    }

    /// Bind EXISTS subquery expression
    fn bind_exists(&self, subquery: &Box<ast::SelectStatement>) -> PrismDBResult<ExpressionRef> {
        use crate::expression::expression::ExistsExpression;

        // Get catalog and transaction manager from binder context
        let catalog = self.catalog.clone().ok_or_else(|| {
            PrismDBError::Execution("Cannot bind EXISTS subquery without catalog".to_string())
        })?;

        let transaction_manager = self.transaction_manager.clone().ok_or_else(|| {
            PrismDBError::Execution("Cannot bind EXISTS subquery without transaction manager".to_string())
        })?;

        // Create EXISTS expression that will execute the subquery and check if any rows exist
        let exists_expr = ExistsExpression::new(
            (**subquery).clone(),
            catalog,
            transaction_manager,
            self.ctes.clone(),
        );

        Ok(Arc::new(exists_expr))
    }

    /// Bind IN subquery expression
    fn bind_in_subquery(
        &self,
        expression: &ast::Expression,
        subquery: &Box<ast::SelectStatement>,
        not: bool,
    ) -> PrismDBResult<ExpressionRef> {
        use crate::expression::expression::InSubqueryExpression;

        // Bind the left-hand side expression
        let bound_expr = self.bind_expression(expression)?;

        // Get catalog and transaction manager from binder context
        let catalog = self.catalog.clone().ok_or_else(|| {
            PrismDBError::Execution("Cannot bind IN subquery without catalog".to_string())
        })?;

        let transaction_manager = self.transaction_manager.clone().ok_or_else(|| {
            PrismDBError::Execution("Cannot bind IN subquery without transaction manager".to_string())
        })?;

        // Create IN subquery expression
        let in_expr = InSubqueryExpression::new(
            bound_expr,
            (**subquery).clone(),
            not,
            catalog,
            transaction_manager,
            self.ctes.clone(),
        );

        Ok(Arc::new(in_expr))
    }
}

/// Type inference utilities
pub struct TypeInference;

impl TypeInference {
    /// Infer common type for binary operations
    pub fn infer_binary_type(left: &LogicalType, right: &LogicalType) -> PrismDBResult<LogicalType> {
        if left == right {
            return Ok(left.clone());
        }

        // Numeric type promotion
        match (left, right) {
            // If either is double, result is double
            (LogicalType::Double, _) | (_, LogicalType::Double) => Ok(LogicalType::Double),

            // If either is float, result is double (promote)
            (LogicalType::Float, _) | (_, LogicalType::Float) => Ok(LogicalType::Double),

            // If either is hugeint, result is hugeint
            (LogicalType::HugeInt, _) | (_, LogicalType::HugeInt) => Ok(LogicalType::HugeInt),

            // If either is bigint, result is bigint
            (LogicalType::BigInt, _) | (_, LogicalType::BigInt) => Ok(LogicalType::BigInt),

            // If either is integer, result is integer
            (LogicalType::Integer, _) | (_, LogicalType::Integer) => Ok(LogicalType::Integer),

            // If either is smallint, result is smallint
            (LogicalType::SmallInt, _) | (_, LogicalType::SmallInt) => Ok(LogicalType::SmallInt),

            // Otherwise, use tinyint
            _ => Ok(LogicalType::TinyInt),
        }
    }

    /// Infer return type for comparison operations
    pub fn infer_comparison_type(
        _left: &LogicalType,
        _right: &LogicalType,
    ) -> PrismDBResult<LogicalType> {
        // Comparisons always return boolean
        Ok(LogicalType::Boolean)
    }

    /// Infer return type for logical operations
    pub fn infer_logical_type(
        _left: &LogicalType,
        _right: &LogicalType,
    ) -> PrismDBResult<LogicalType> {
        // Logical operations always return boolean
        Ok(LogicalType::Boolean)
    }
}
