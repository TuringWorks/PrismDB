//! SQL Parser
//!
//! Parses tokens into AST statements.

use crate::common::error::{PrismDBError, PrismDBResult};
use crate::parser::ast::*;
use crate::parser::keywords::Keyword;
use crate::parser::tokenizer::{Token, TokenType};
use crate::types::LogicalType;
use std::collections::HashMap;

/// SQL parser
pub struct Parser {
    tokens: Vec<Token>,
    position: usize,
}

impl Parser {
    /// Create a new parser with the given tokens
    pub fn new(tokens: Vec<Token>) -> Self {
        Self {
            tokens,
            position: 0,
        }
    }

    /// Parse a single statement
    pub fn parse_statement(&mut self) -> PrismDBResult<Statement> {
        let statement = self.parse_statement_internal()?;

        // Skip any semicolons
        while self.consume_token(&TokenType::Semicolon).is_ok() {
            // Continue consuming semicolons
        }

        // Expect EOF or end of statement
        if !self.current_token().is_eof() {
            return Err(PrismDBError::Parse(
                "Unexpected token after statement".to_string(),
            ));
        }

        Ok(statement)
    }

    /// Parse multiple statements
    pub fn parse_statements(&mut self) -> PrismDBResult<Vec<Statement>> {
        let mut statements = Vec::new();

        while !self.current_token().is_eof() {
            // Skip empty statements (just semicolons)
            if self.current_token().token_type == TokenType::Semicolon {
                let _ = self.consume_token(&TokenType::Semicolon);
                continue;
            }

            let statement = self.parse_statement_internal()?;
            statements.push(statement);

            // Skip semicolon if present
            let _ = self.consume_token(&TokenType::Semicolon);
        }

        Ok(statements)
    }

    /// Parse a single statement (internal)
    fn parse_statement_internal(&mut self) -> PrismDBResult<Statement> {
        match &self.current_token().token_type {
            TokenType::Keyword(Keyword::With) => {
                let select = self.parse_query()?;
                Ok(Statement::Select(select))
            }
            TokenType::Keyword(Keyword::Select) => {
                let select = self.parse_query()?;
                Ok(Statement::Select(select))
            }
            TokenType::Keyword(Keyword::Insert) => {
                let insert = self.parse_insert_statement()?;
                Ok(Statement::Insert(insert))
            }
            TokenType::Keyword(Keyword::Update) => {
                let update = self.parse_update_statement()?;
                Ok(Statement::Update(update))
            }
            TokenType::Keyword(Keyword::Delete) => {
                let delete = self.parse_delete_statement()?;
                Ok(Statement::Delete(delete))
            }
            TokenType::Keyword(Keyword::Create) => self.parse_create_statement(),
            TokenType::Keyword(Keyword::Drop) => self.parse_drop_statement(),
            TokenType::Keyword(Keyword::Refresh) => {
                let refresh = self.parse_refresh_materialized_view_statement()?;
                Ok(Statement::RefreshMaterializedView(refresh))
            }
            TokenType::Keyword(Keyword::Alter) => {
                let alter = self.parse_alter_table_statement()?;
                Ok(Statement::AlterTable(alter))
            }
            TokenType::Keyword(Keyword::Begin) | TokenType::Keyword(Keyword::Start) => {
                let begin = self.parse_begin_statement()?;
                Ok(Statement::Begin(begin))
            }
            TokenType::Keyword(Keyword::Commit) => {
                let commit = self.parse_commit_statement()?;
                Ok(Statement::Commit(commit))
            }
            TokenType::Keyword(Keyword::Rollback) => {
                let rollback = self.parse_rollback_statement()?;
                Ok(Statement::Rollback(rollback))
            }
            TokenType::Keyword(Keyword::Explain) => {
                let explain = self.parse_explain_statement()?;
                Ok(Statement::Explain(explain))
            }
            TokenType::Keyword(Keyword::Show) => {
                let show = self.parse_show_statement()?;
                Ok(Statement::Show(show))
            }
            TokenType::Keyword(Keyword::Install) => {
                let install = self.parse_install_statement()?;
                Ok(Statement::Install(install))
            }
            TokenType::Keyword(Keyword::Load) => {
                let load = self.parse_load_statement()?;
                Ok(Statement::Load(load))
            }
            TokenType::Keyword(Keyword::Set) => {
                let set = self.parse_set_statement()?;
                Ok(Statement::Set(set))
            }
            _ => Err(PrismDBError::Parse(format!(
                "Unexpected token: {:?}",
                self.current_token()
            ))),
        }
    }

    /// Parse a complete query (WITH clause + SELECT + set operations)
    fn parse_query(&mut self) -> PrismDBResult<SelectStatement> {
        // Parse optional WITH clause
        let with_clause = if self.consume_keyword(Keyword::With).is_ok() {
            Some(self.parse_with_clause()?)
        } else {
            None
        };

        // Parse the main SELECT statement
        let mut select = self.parse_select_statement()?;
        select.with_clause = with_clause;

        // Parse set operations (UNION, INTERSECT, EXCEPT)
        let set_operations = self.parse_set_operations()?;
        select.set_operations = set_operations;

        Ok(select)
    }

    /// Parse WITH clause (Common Table Expressions)
    fn parse_with_clause(&mut self) -> PrismDBResult<WithClause> {
        let recursive = self.consume_keyword(Keyword::Recursive).is_ok();
        let mut ctes = Vec::new();

        loop {
            // Parse CTE name
            let name = self.consume_identifier()?;

            // Parse optional column list
            let columns = if self.consume_token(&TokenType::LeftParen).is_ok() {
                let cols = self.parse_identifier_list()?;
                self.consume_token(&TokenType::RightParen)?;
                cols
            } else {
                Vec::new()
            };

            // Parse AS (subquery)
            self.consume_keyword(Keyword::As)?;
            self.consume_token(&TokenType::LeftParen)?;

            // Parse SELECT with set operations (but without WITH clause)
            let mut query = self.parse_select_statement()?;
            let set_operations = self.parse_set_operations()?;
            query.set_operations = set_operations;

            self.consume_token(&TokenType::RightParen)?;
            let query = Box::new(query);

            ctes.push(CommonTableExpression {
                name,
                columns,
                query,
            });

            // Check for more CTEs
            if self.consume_token(&TokenType::Comma).is_err() {
                break;
            }
        }

        Ok(WithClause { recursive, ctes })
    }

    /// Parse set operations (UNION, INTERSECT, EXCEPT)
    fn parse_set_operations(&mut self) -> PrismDBResult<Vec<SetOperation>> {
        let mut operations = Vec::new();

        loop {
            let op_type = match &self.current_token().token_type {
                TokenType::Keyword(Keyword::Union) => {
                    self.consume_keyword(Keyword::Union)?;
                    SetOperationType::Union
                }
                TokenType::Keyword(Keyword::Intersect) => {
                    self.consume_keyword(Keyword::Intersect)?;
                    SetOperationType::Intersect
                }
                TokenType::Keyword(Keyword::Except) => {
                    self.consume_keyword(Keyword::Except)?;
                    SetOperationType::Except
                }
                _ => break, // No more set operations
            };

            // Check for ALL keyword
            let all = self.consume_keyword(Keyword::All).is_ok();

            // Parse the next SELECT statement (without WITH clause)
            let query = Box::new(self.parse_select_statement()?);

            operations.push(SetOperation {
                op_type,
                all,
                query,
            });
        }

        Ok(operations)
    }

    /// Parse SELECT statement
    fn parse_select_statement(&mut self) -> PrismDBResult<SelectStatement> {
        self.consume_keyword(Keyword::Select)?;

        let distinct = self.consume_keyword(Keyword::Distinct).is_ok();
        if !distinct {
            let _ = self.consume_keyword(Keyword::All);
        }

        let select_list = self.parse_select_list()?;

        let from = if self.consume_keyword(Keyword::From).is_ok() {
            Some(self.parse_table_reference()?)
        } else {
            None
        };

        let where_clause = if self.consume_keyword(Keyword::Where).is_ok() {
            Some(Box::new(self.parse_expression()?))
        } else {
            None
        };

        let mut group_by = Vec::new();
        if self.consume_keyword(Keyword::Group).is_ok() {
            self.consume_keyword(Keyword::By)?;
            group_by = self.parse_expression_list()?;
        }

        let having = if self.consume_keyword(Keyword::Having).is_ok() {
            Some(Box::new(self.parse_expression()?))
        } else {
            None
        };

        // QUALIFY clause for filtering on window functions (PrismDB extension)
        let qualify = if self.consume_keyword(Keyword::Qualify).is_ok() {
            Some(Box::new(self.parse_expression()?))
        } else {
            None
        };

        let mut order_by = Vec::new();
        if self.consume_keyword(Keyword::Order).is_ok() {
            self.consume_keyword(Keyword::By)?;
            order_by = self.parse_order_by_list()?;
        }

        let limit = if self.consume_keyword(Keyword::Limit).is_ok() {
            let limit_value = self.parse_literal_integer()?;
            let offset = if self.consume_keyword(Keyword::Offset).is_ok() {
                Some(self.parse_literal_integer()?)
            } else {
                None
            };
            Some(LimitClause {
                limit: limit_value,
                offset,
            })
        } else {
            None
        };

        let offset = if limit.is_none() && self.consume_keyword(Keyword::Offset).is_ok() {
            Some(self.parse_literal_integer()?)
        } else {
            None
        };

        Ok(SelectStatement {
            with_clause: None,  // TODO: Parse WITH clause
            distinct,
            select_list,
            from,
            where_clause,
            group_by,
            having,
            qualify,
            order_by,
            limit,
            offset,
            set_operations: Vec::new(),  // TODO: Parse set operations
        })
    }

    /// Parse SELECT list
    fn parse_select_list(&mut self) -> PrismDBResult<Vec<SelectItem>> {
        let mut items = Vec::new();

        loop {
            let item = if self.current_token().token_type == TokenType::Star {
                let _ = self.consume_token(&TokenType::Star);
                SelectItem::Wildcard
            } else if self.current_token().token_type == TokenType::Keyword(Keyword::From) {
                // Handle qualified wildcard like table.*
                let table_name = self.consume_identifier()?;
                self.consume_token(&TokenType::Dot)?;
                self.consume_token(&TokenType::Star)?;
                SelectItem::QualifiedWildcard(table_name)
            } else {
                let expression = self.parse_expression()?;

                if self.consume_keyword(Keyword::As).is_ok()
                    || (matches!(self.current_token().token_type, TokenType::Identifier(_))
                        && self.peek_token().token_type != TokenType::Comma
                        && !self.peek_token().is_eof())
                {
                    let alias = self.consume_identifier()?;
                    SelectItem::Alias(Box::new(expression), alias)
                } else {
                    SelectItem::Expression(expression)
                }
            };

            items.push(item);

            if self.consume_token(&TokenType::Comma).is_err() {
                break;
            }
        }

        Ok(items)
    }

    /// Parse table reference
    fn parse_table_reference(&mut self) -> PrismDBResult<TableReference> {
        let mut left = self.parse_table_factor()?;

        while self.is_join_keyword() {
            let join_type = self.parse_join_type()?;
            self.consume_keyword(Keyword::Join)?;
            let right = self.parse_table_factor()?;
            let condition = self.parse_join_condition()?;

            left = TableReference::Join {
                left: Box::new(left),
                join_type,
                right: Box::new(right),
                condition,
            };
        }

        Ok(left)
    }

    /// Parse table factor
    fn parse_table_factor(&mut self) -> PrismDBResult<TableReference> {
        let base_table = if self.current_token().token_type == TokenType::LeftParen {
            let _ = self.consume_token(&TokenType::LeftParen);

            // Check if it's a subquery
            if self.current_token().token_type == TokenType::Keyword(Keyword::Select) {
                // Parse SELECT with set operations (but without WITH clause)
                let mut subquery = self.parse_select_statement()?;
                let set_operations = self.parse_set_operations()?;
                subquery.set_operations = set_operations;

                self.consume_token(&TokenType::RightParen)?;

                let alias = if self.consume_keyword(Keyword::As).is_ok() {
                    self.consume_identifier()?
                } else if matches!(self.current_token().token_type, TokenType::Identifier(_)) {
                    self.consume_identifier()?
                } else {
                    return Err(PrismDBError::Parse("Subquery requires alias".to_string()));
                };

                TableReference::Subquery {
                    subquery: Box::new(subquery),
                    alias,
                }
            } else {
                // It's a parenthesized table reference
                let table_ref = self.parse_table_reference()?;
                self.consume_token(&TokenType::RightParen)?;
                table_ref
            }
        } else {
            let name = self.consume_identifier()?;

            // Check if it's a table function call (identifier followed by left paren)
            if self.current_token().token_type == TokenType::LeftParen {
                // Parse table function: function_name(arg1, arg2, ...)
                self.consume_token(&TokenType::LeftParen)?;

                let mut arguments = Vec::new();
                if self.current_token().token_type != TokenType::RightParen {
                    loop {
                        arguments.push(self.parse_expression()?);
                        if self.consume_token(&TokenType::Comma).is_err() {
                            break;
                        }
                    }
                }

                self.consume_token(&TokenType::RightParen)?;

                let alias = if self.consume_keyword(Keyword::As).is_ok() {
                    Some(self.consume_identifier()?)
                } else if matches!(self.current_token().token_type, TokenType::Identifier(_))
                    && !self.is_join_keyword()
                {
                    Some(self.consume_identifier()?)
                } else {
                    None
                };

                TableReference::TableFunction {
                    name,
                    arguments,
                    alias,
                }
            } else {
                // Regular table
                let alias = if self.consume_keyword(Keyword::As).is_ok() {
                    Some(self.consume_identifier()?)
                } else if matches!(self.current_token().token_type, TokenType::Identifier(_))
                    && !self.is_join_keyword()
                    && self.peek_token().token_type != TokenType::Semicolon
                    && !self.peek_token().is_eof()
                {
                    Some(self.consume_identifier()?)
                } else {
                    None
                };

                TableReference::Table { name, alias }
            }
        };

        // Check for PIVOT or UNPIVOT after base table/subquery
        if self.consume_keyword(Keyword::Pivot).is_ok() {
            let pivot_spec = self.parse_pivot_spec()?;
            let alias = if self.consume_keyword(Keyword::As).is_ok() {
                Some(self.consume_identifier()?)
            } else if matches!(self.current_token().token_type, TokenType::Identifier(_)) {
                Some(self.consume_identifier()?)
            } else {
                None
            };
            Ok(TableReference::Pivot {
                source: Box::new(base_table),
                pivot_spec,
                alias,
            })
        } else if self.consume_keyword(Keyword::Unpivot).is_ok() {
            let unpivot_spec = self.parse_unpivot_spec()?;
            let alias = if self.consume_keyword(Keyword::As).is_ok() {
                Some(self.consume_identifier()?)
            } else if matches!(self.current_token().token_type, TokenType::Identifier(_)) {
                Some(self.consume_identifier()?)
            } else {
                None
            };
            Ok(TableReference::Unpivot {
                source: Box::new(base_table),
                unpivot_spec,
                alias,
            })
        } else {
            Ok(base_table)
        }
    }

    /// Parse join type
    fn parse_join_type(&mut self) -> PrismDBResult<JoinType> {
        if self.consume_keyword(Keyword::Inner).is_ok() {
            Ok(JoinType::Inner)
        } else if self.consume_keyword(Keyword::Left).is_ok() {
            let _ = self.consume_keyword(Keyword::Outer);
            Ok(JoinType::Left)
        } else if self.consume_keyword(Keyword::Right).is_ok() {
            let _ = self.consume_keyword(Keyword::Outer);
            Ok(JoinType::Right)
        } else if self.consume_keyword(Keyword::Full).is_ok() {
            let _ = self.consume_keyword(Keyword::Outer);
            Ok(JoinType::Full)
        } else if self.consume_keyword(Keyword::Cross).is_ok() {
            Ok(JoinType::Cross)
        } else {
            Ok(JoinType::Inner) // Default to INNER JOIN
        }
    }

    /// Parse join condition
    fn parse_join_condition(&mut self) -> PrismDBResult<JoinCondition> {
        if self.consume_keyword(Keyword::On).is_ok() {
            let expression = self.parse_expression()?;
            Ok(JoinCondition::On(expression))
        } else if self.consume_keyword(Keyword::Using).is_ok() {
            self.consume_token(&TokenType::LeftParen)?;
            let mut columns = Vec::new();
            loop {
                columns.push(self.consume_identifier()?);
                if self.consume_token(&TokenType::Comma).is_err() {
                    break;
                }
            }
            self.consume_token(&TokenType::RightParen)?;
            Ok(JoinCondition::Using(columns))
        } else {
            Err(PrismDBError::Parse(
                "Expected ON or USING for join condition".to_string(),
            ))
        }
    }

    /// Parse ORDER BY list
    fn parse_order_by_list(&mut self) -> PrismDBResult<Vec<OrderByExpression>> {
        let mut expressions = Vec::new();

        loop {
            let expression = self.parse_expression()?;

            let ascending = if self.consume_keyword(Keyword::Asc).is_ok() {
                true
            } else if self.consume_keyword(Keyword::Desc).is_ok() {
                false
            } else {
                true // Default to ASC
            };

            let nulls_first = if self.consume_keyword(Keyword::Nulls).is_ok() {
                self.consume_keyword(Keyword::First).is_ok()
            } else {
                false // Default
            };

            expressions.push(OrderByExpression {
                expression,
                ascending,
                nulls_first,
            });

            if self.consume_token(&TokenType::Comma).is_err() {
                break;
            }
        }

        Ok(expressions)
    }

    /// Parse expression
    fn parse_expression(&mut self) -> PrismDBResult<Expression> {
        self.parse_or_expression()
    }

    /// Parse OR expression
    fn parse_or_expression(&mut self) -> PrismDBResult<Expression> {
        let mut left = self.parse_and_expression()?;

        while self.consume_keyword(Keyword::Or).is_ok() {
            let right = self.parse_and_expression()?;
            left = Expression::Binary {
                left: Box::new(left),
                operator: BinaryOperator::Or,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    /// Parse AND expression
    fn parse_and_expression(&mut self) -> PrismDBResult<Expression> {
        let mut left = self.parse_not_expression()?;

        while self.consume_keyword(Keyword::And).is_ok() {
            let right = self.parse_not_expression()?;
            left = Expression::Binary {
                left: Box::new(left),
                operator: BinaryOperator::And,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    /// Parse NOT expression
    fn parse_not_expression(&mut self) -> PrismDBResult<Expression> {
        if self.consume_keyword(Keyword::Not).is_ok() {
            let expression = self.parse_not_expression()?;
            Ok(Expression::Unary {
                operator: UnaryOperator::Not,
                expression: Box::new(expression),
            })
        } else {
            self.parse_comparison_expression()
        }
    }

    /// Parse comparison expression
    fn parse_comparison_expression(&mut self) -> PrismDBResult<Expression> {
        let mut left = self.parse_additive_expression()?;

        loop {
            let operator = match &self.current_token().token_type {
                TokenType::Equals => {
                    let _ = self.consume_token(&TokenType::Equals);
                    Some(BinaryOperator::Equals)
                }
                TokenType::NotEquals => {
                    let _ = self.consume_token(&TokenType::NotEquals);
                    Some(BinaryOperator::NotEquals)
                }
                TokenType::LessThan => {
                    let _ = self.consume_token(&TokenType::LessThan);
                    Some(BinaryOperator::LessThan)
                }
                TokenType::LessThanOrEqual => {
                    let _ = self.consume_token(&TokenType::LessThanOrEqual);
                    Some(BinaryOperator::LessThanOrEqual)
                }
                TokenType::GreaterThan => {
                    let _ = self.consume_token(&TokenType::GreaterThan);
                    Some(BinaryOperator::GreaterThan)
                }
                TokenType::GreaterThanOrEqual => {
                    let _ = self.consume_token(&TokenType::GreaterThanOrEqual);
                    Some(BinaryOperator::GreaterThanOrEqual)
                }
                TokenType::Keyword(Keyword::Like) => {
                    let _ = self.consume_keyword(Keyword::Like);
                    Some(BinaryOperator::Like)
                }
                TokenType::Keyword(Keyword::In) => {
                    let _ = self.consume_keyword(Keyword::In);
                    return self.parse_in_expression(left);
                }
                TokenType::Keyword(Keyword::Between) => {
                    let _ = self.consume_keyword(Keyword::Between);
                    return self.parse_between_expression(left, false);
                }
                TokenType::Keyword(Keyword::Is) => {
                    let _ = self.consume_keyword(Keyword::Is);
                    return self.parse_is_expression(left);
                }
                _ => None,
            };

            if let Some(op) = operator {
                let right = self.parse_additive_expression()?;
                left = Expression::Binary {
                    left: Box::new(left),
                    operator: op,
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }

        Ok(left)
    }

    /// Parse additive expression
    fn parse_additive_expression(&mut self) -> PrismDBResult<Expression> {
        let mut left = self.parse_multiplicative_expression()?;

        loop {
            let operator = match &self.current_token().token_type {
                TokenType::Plus => {
                    let _ = self.consume_token(&TokenType::Plus);
                    Some(BinaryOperator::Add)
                }
                TokenType::Minus => {
                    let _ = self.consume_token(&TokenType::Minus);
                    Some(BinaryOperator::Subtract)
                }
                _ => None,
            };

            if let Some(op) = operator {
                let right = self.parse_multiplicative_expression()?;
                left = Expression::Binary {
                    left: Box::new(left),
                    operator: op,
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }

        Ok(left)
    }

    /// Parse multiplicative expression
    fn parse_multiplicative_expression(&mut self) -> PrismDBResult<Expression> {
        let mut left = self.parse_unary_expression()?;

        loop {
            let operator = match &self.current_token().token_type {
                TokenType::Star => {
                    let _ = self.consume_token(&TokenType::Star);
                    Some(BinaryOperator::Multiply)
                }
                TokenType::Divide => {
                    let _ = self.consume_token(&TokenType::Divide);
                    Some(BinaryOperator::Divide)
                }
                TokenType::Modulo => {
                    let _ = self.consume_token(&TokenType::Modulo);
                    Some(BinaryOperator::Modulo)
                }
                _ => None,
            };

            if let Some(op) = operator {
                let right = self.parse_unary_expression()?;
                left = Expression::Binary {
                    left: Box::new(left),
                    operator: op,
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }

        Ok(left)
    }

    /// Parse unary expression
    fn parse_unary_expression(&mut self) -> PrismDBResult<Expression> {
        match &self.current_token().token_type {
            TokenType::Plus => {
                let _ = self.consume_token(&TokenType::Plus);
                let expression = self.parse_unary_expression()?;
                Ok(Expression::Unary {
                    operator: UnaryOperator::Plus,
                    expression: Box::new(expression),
                })
            }
            TokenType::Minus => {
                let _ = self.consume_token(&TokenType::Minus);
                let expression = self.parse_unary_expression()?;
                Ok(Expression::Unary {
                    operator: UnaryOperator::Minus,
                    expression: Box::new(expression),
                })
            }
            _ => self.parse_primary_expression(),
        }
    }

    /// Parse primary expression
    fn parse_primary_expression(&mut self) -> PrismDBResult<Expression> {
        match &self.current_token().token_type {
            TokenType::StringLiteral(_) => {
                let value = self.consume_string_literal()?;
                Ok(Expression::Literal(LiteralValue::String(value)))
            }
            TokenType::NumericLiteral(_) => {
                let value = self.consume_numeric_literal()?;
                if value.contains('.') {
                    Ok(Expression::Literal(LiteralValue::Float(
                        value.parse().unwrap(),
                    )))
                } else {
                    Ok(Expression::Literal(LiteralValue::Integer(
                        value.parse().unwrap(),
                    )))
                }
            }
            TokenType::Keyword(Keyword::True) => {
                let _ = self.consume_keyword(Keyword::True);
                Ok(Expression::Literal(LiteralValue::Boolean(true)))
            }
            TokenType::Keyword(Keyword::False) => {
                let _ = self.consume_keyword(Keyword::False);
                Ok(Expression::Literal(LiteralValue::Boolean(false)))
            }
            TokenType::Keyword(Keyword::Null) => {
                let _ = self.consume_keyword(Keyword::Null);
                Ok(Expression::Literal(LiteralValue::Null))
            }
            // Handle CASE expression
            TokenType::Keyword(Keyword::Case) => {
                self.parse_case_expression()
            }
            // Handle EXISTS subquery
            TokenType::Keyword(Keyword::Exists) => {
                self.consume_keyword(Keyword::Exists)?;
                self.consume_token(&TokenType::LeftParen)?;
                let subquery = Box::new(self.parse_query()?);
                self.consume_token(&TokenType::RightParen)?;
                Ok(Expression::Exists(subquery))
            }
            // Handle aggregate function keywords (COUNT, SUM, AVG, etc.)
            TokenType::Keyword(kw) if self.is_aggregate_keyword(kw) => {
                let func_name = self.current_token().text.clone();
                self.position += 1; // Consume the keyword

                // Must be followed by left paren
                if self.current_token().token_type == TokenType::LeftParen {
                    self.parse_function_call(func_name)
                } else {
                    Err(PrismDBError::Parse(format!(
                        "Expected '(' after aggregate function {}",
                        func_name
                    )))
                }
            }
            // Handle scalar function keywords (COALESCE, NULLIF, LENGTH, etc.)
            TokenType::Keyword(kw) if self.is_scalar_function_keyword(kw) => {
                let func_name = self.current_token().text.clone();
                self.position += 1; // Consume the keyword

                // Must be followed by left paren
                if self.current_token().token_type == TokenType::LeftParen {
                    self.parse_function_call(func_name)
                } else {
                    Err(PrismDBError::Parse(format!(
                        "Expected '(' after function {}",
                        func_name
                    )))
                }
            }
            // Handle non-reserved keywords as identifiers (e.g., column names like "temp")
            TokenType::Keyword(kw)
                if !self.is_aggregate_keyword(kw) && !self.is_scalar_function_keyword(kw) =>
            {
                let identifier = self.consume_identifier()?;

                // Check if it's a function call
                if self.current_token().token_type == TokenType::LeftParen {
                    self.parse_function_call(identifier)
                } else {
                    // Check if it's a qualified column reference
                    if self.current_token().token_type == TokenType::Dot {
                        self.consume_token(&TokenType::Dot)?;
                        let column = self.consume_identifier()?;
                        Ok(Expression::ColumnReference {
                            table: Some(identifier),
                            column,
                        })
                    } else {
                        Ok(Expression::ColumnReference {
                            table: None,
                            column: identifier,
                        })
                    }
                }
            }
            TokenType::Identifier(_) => {
                let identifier = self.consume_identifier()?;

                // Check if it's a function call
                if self.current_token().token_type == TokenType::LeftParen {
                    self.parse_function_call(identifier)
                } else {
                    // Check if it's a qualified column reference
                    if self.current_token().token_type == TokenType::Dot {
                        self.consume_token(&TokenType::Dot)?;
                        let column = self.consume_identifier()?;
                        Ok(Expression::ColumnReference {
                            table: Some(identifier),
                            column,
                        })
                    } else {
                        Ok(Expression::ColumnReference {
                            table: None,
                            column: identifier,
                        })
                    }
                }
            }
            TokenType::LeftParen => {
                let _ = self.consume_token(&TokenType::LeftParen);

                // Check if this is a subquery (starts with SELECT or WITH)
                if matches!(
                    &self.current_token().token_type,
                    TokenType::Keyword(Keyword::Select) | TokenType::Keyword(Keyword::With)
                ) {
                    let subquery = Box::new(self.parse_query()?);
                    self.consume_token(&TokenType::RightParen)?;
                    Ok(Expression::Subquery(subquery))
                } else {
                    // Regular parenthesized expression
                    let expression = self.parse_expression()?;
                    self.consume_token(&TokenType::RightParen)?;
                    Ok(expression)
                }
            }
            _ => Err(PrismDBError::Parse(format!(
                "Unexpected token in expression: {:?}",
                self.current_token()
            ))),
        }
    }

    /// Check if a keyword is an aggregate function keyword
    fn is_aggregate_keyword(&self, kw: &Keyword) -> bool {
        matches!(
            kw,
            Keyword::Count
                | Keyword::Sum
                | Keyword::Avg
                | Keyword::Min
                | Keyword::Max
                | Keyword::StdDev
                | Keyword::Variance
        )
    }

    /// Check if a keyword is a scalar function that should be parsed as a function call
    fn is_scalar_function_keyword(&self, kw: &Keyword) -> bool {
        matches!(
            kw,
            Keyword::Coalesce
                | Keyword::NullIf
                | Keyword::Length
                | Keyword::Upper
                | Keyword::Lower
                | Keyword::Trim
                | Keyword::Substring
                | Keyword::Cast
                | Keyword::Concat
        )
    }

    /// Parse function call
    fn parse_function_call(&mut self, name: String) -> PrismDBResult<Expression> {
        let _ = self.consume_token(&TokenType::LeftParen);

        let distinct = self.consume_keyword(Keyword::Distinct).is_ok();

        let mut arguments = Vec::new();
        if self.current_token().token_type != TokenType::RightParen {
            // Special case for COUNT(*) - star is a valid argument
            if self.current_token().token_type == TokenType::Star {
                // Create a wildcard expression for star
                arguments.push(Expression::Wildcard);
                self.position += 1; // Consume the star
            } else {
                arguments = self.parse_expression_list()?;
            }
        }

        self.consume_token(&TokenType::RightParen)?;

        // Check for OVER clause (window function)
        if self.consume_keyword(Keyword::Over).is_ok() {
            let window_spec = self.parse_window_spec()?;
            return Ok(Expression::WindowFunction {
                name,
                arguments,
                window_spec,
            });
        }

        // Check if it's an aggregate function
        let is_aggregate = match name.to_uppercase().as_str() {
            "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" | "STDDEV" | "VARIANCE" => true,
            _ => false,
        };

        if is_aggregate {
            Ok(Expression::AggregateFunction {
                name,
                arguments,
                distinct,
            })
        } else {
            Ok(Expression::FunctionCall {
                name,
                arguments,
                distinct,
            })
        }
    }

    /// Parse CASE expression
    /// Supports both simple CASE and searched CASE:
    /// Simple: CASE expr WHEN value1 THEN result1 ... ELSE default END
    /// Searched: CASE WHEN condition1 THEN result1 ... ELSE default END
    fn parse_case_expression(&mut self) -> PrismDBResult<Expression> {
        self.consume_keyword(Keyword::Case)?;

        // Check if this is a simple CASE (has an operand) or searched CASE
        let operand = if !matches!(&self.current_token().token_type, TokenType::Keyword(Keyword::When)) {
            // Simple CASE - next token is the operand expression
            Some(Box::new(self.parse_expression()?))
        } else {
            None
        };

        // Parse WHEN ... THEN ... clauses
        let mut conditions = Vec::new();
        let mut results = Vec::new();

        while self.consume_keyword(Keyword::When).is_ok() {
            // Parse the condition (or value for simple CASE)
            let condition = self.parse_expression()?;
            conditions.push(condition);

            // Consume THEN
            self.consume_keyword(Keyword::Then)?;

            // Parse the result expression
            let result = self.parse_expression()?;
            results.push(result);
        }

        // Check for ELSE clause
        let else_result = if self.consume_keyword(Keyword::Else).is_ok() {
            Some(Box::new(self.parse_expression()?))
        } else {
            None
        };

        // Consume END keyword
        self.consume_keyword(Keyword::End)?;

        Ok(Expression::Case {
            operand,
            conditions,
            results,
            else_result,
        })
    }

    /// Parse window specification (OVER clause)
    fn parse_window_spec(&mut self) -> PrismDBResult<WindowSpec> {
        self.consume_token(&TokenType::LeftParen)?;

        // Parse PARTITION BY
        let partition_by = if self.consume_keyword(Keyword::Partition).is_ok() {
            self.consume_keyword(Keyword::By)?;
            self.parse_expression_list()?
        } else {
            Vec::new()
        };

        // Parse ORDER BY
        let order_by = if self.consume_keyword(Keyword::Order).is_ok() {
            self.consume_keyword(Keyword::By)?;
            self.parse_order_by_list()?
        } else {
            Vec::new()
        };

        // Parse window frame (ROWS/RANGE/GROUPS BETWEEN)
        let window_frame = self.parse_window_frame()?;

        self.consume_token(&TokenType::RightParen)?;

        Ok(WindowSpec {
            partition_by,
            order_by,
            window_frame,
        })
    }

    /// Parse window frame specification
    fn parse_window_frame(&mut self) -> PrismDBResult<Option<WindowFrame>> {
        // Check for frame units (ROWS, RANGE, or GROUPS)
        let units = if self.consume_keyword(Keyword::Rows).is_ok() {
            WindowFrameUnits::Rows
        } else if self.consume_keyword(Keyword::Range).is_ok() {
            WindowFrameUnits::Range
        } else if self.consume_keyword(Keyword::Groups).is_ok() {
            WindowFrameUnits::Groups
        } else {
            // No frame specification - use default
            return Ok(None);
        };

        // Parse BETWEEN start_bound AND end_bound
        // or just a single bound (which becomes start bound, end defaults to CURRENT ROW)
        let (start_bound, end_bound) = if self.consume_keyword(Keyword::Between).is_ok() {
            let start = self.parse_window_frame_bound()?;
            self.consume_keyword(Keyword::And)?;
            let end = self.parse_window_frame_bound()?;
            (start, Some(end))
        } else {
            // Single bound syntax (e.g., "ROWS 5 PRECEDING")
            let bound = self.parse_window_frame_bound()?;
            (bound, None)
        };

        Ok(Some(WindowFrame {
            units,
            start_bound,
            end_bound,
        }))
    }

    /// Parse window frame bound
    fn parse_window_frame_bound(&mut self) -> PrismDBResult<WindowFrameBound> {
        if self.consume_keyword(Keyword::Current).is_ok() {
            self.consume_keyword(Keyword::Row)?;
            Ok(WindowFrameBound::CurrentRow)
        } else if self.consume_keyword(Keyword::Unbounded).is_ok() {
            if self.consume_keyword(Keyword::Preceding).is_ok() {
                Ok(WindowFrameBound::UnboundedPreceding)
            } else if self.consume_keyword(Keyword::Following).is_ok() {
                Ok(WindowFrameBound::UnboundedFollowing)
            } else {
                Err(PrismDBError::Parse(
                    "Expected PRECEDING or FOLLOWING after UNBOUNDED".to_string(),
                ))
            }
        } else {
            // Parse numeric offset
            let offset = match &self.current_token().token_type {
                TokenType::NumericLiteral(n) => {
                    let value = n.parse::<usize>().map_err(|_| {
                        PrismDBError::Parse(format!("Invalid frame offset: {}", n))
                    })?;
                    self.position += 1;
                    value
                }
                _ => {
                    return Err(PrismDBError::Parse(format!(
                        "Expected number for frame offset, got: {:?}",
                        self.current_token()
                    )));
                }
            };

            if self.consume_keyword(Keyword::Preceding).is_ok() {
                Ok(WindowFrameBound::Preceding(offset))
            } else if self.consume_keyword(Keyword::Following).is_ok() {
                Ok(WindowFrameBound::Following(offset))
            } else {
                Err(PrismDBError::Parse(
                    "Expected PRECEDING or FOLLOWING after offset".to_string(),
                ))
            }
        }
    }

    /// Parse IN expression
    fn parse_in_expression(&mut self, left: Expression) -> PrismDBResult<Expression> {
        if self.current_token().token_type == TokenType::LeftParen {
            let _ = self.consume_token(&TokenType::LeftParen);

            if matches!(
                &self.current_token().token_type,
                TokenType::Keyword(Keyword::Select) | TokenType::Keyword(Keyword::With)
            ) {
                // Subquery
                let subquery = self.parse_query()?;
                self.consume_token(&TokenType::RightParen)?;
                Ok(Expression::InSubquery {
                    expression: Box::new(left),
                    subquery: Box::new(subquery),
                    not: false,
                })
            } else {
                // List of values
                let list = self.parse_expression_list()?;
                self.consume_token(&TokenType::RightParen)?;
                Ok(Expression::InList {
                    expression: Box::new(left),
                    list,
                    not: false,
                })
            }
        } else {
            Err(PrismDBError::Parse("Expected '(' after IN".to_string()))
        }
    }

    /// Parse BETWEEN expression
    fn parse_between_expression(
        &mut self,
        left: Expression,
        not: bool,
    ) -> PrismDBResult<Expression> {
        let low = self.parse_expression()?;
        self.consume_keyword(Keyword::And)?;
        let high = self.parse_expression()?;

        Ok(Expression::Between {
            expression: Box::new(left),
            low: Box::new(low),
            high: Box::new(high),
            not,
        })
    }

    /// Parse IS expression
    fn parse_is_expression(&mut self, left: Expression) -> PrismDBResult<Expression> {
        if self.consume_keyword(Keyword::Null).is_ok() {
            Ok(Expression::IsNull(Box::new(left)))
        } else if self.consume_keyword(Keyword::Not).is_ok() {
            if self.consume_keyword(Keyword::Null).is_ok() {
                Ok(Expression::IsNotNull(Box::new(left)))
            } else {
                Err(PrismDBError::Parse("Expected NULL after IS NOT".to_string()))
            }
        } else {
            Err(PrismDBError::Parse("Expected NULL after IS".to_string()))
        }
    }

    /// Parse expression list
    fn parse_expression_list(&mut self) -> PrismDBResult<Vec<Expression>> {
        let mut expressions = Vec::new();

        loop {
            expressions.push(self.parse_expression()?);
            if self.consume_token(&TokenType::Comma).is_err() {
                break;
            }
        }

        Ok(expressions)
    }

    /// Parse INSERT statement
    fn parse_insert_statement(&mut self) -> PrismDBResult<InsertStatement> {
        self.consume_keyword(Keyword::Insert)?;
        self.consume_keyword(Keyword::Into)?;

        let table_name = self.consume_identifier()?;

        let mut columns = Vec::new();
        if self.current_token().token_type == TokenType::LeftParen {
            let _ = self.consume_token(&TokenType::LeftParen);
            loop {
                columns.push(self.consume_identifier()?);
                if self.consume_token(&TokenType::Comma).is_err() {
                    break;
                }
            }
            self.consume_token(&TokenType::RightParen)?;
        }

        let source = if self.consume_keyword(Keyword::Values).is_ok() {
            let mut values = Vec::new();
            loop {
                self.consume_token(&TokenType::LeftParen)?;
                let mut row = Vec::new();
                loop {
                    row.push(self.parse_expression()?);
                    if self.consume_token(&TokenType::Comma).is_err() {
                        break;
                    }
                }
                self.consume_token(&TokenType::RightParen)?;
                values.push(row);

                if self.consume_token(&TokenType::Comma).is_err() {
                    break;
                }
            }
            InsertSource::Values(values)
        } else if self.current_token().token_type == TokenType::Keyword(Keyword::Select) {
            let select = self.parse_select_statement()?;
            InsertSource::Select(select)
        } else if self.consume_keyword(Keyword::Default).is_ok() {
            self.consume_keyword(Keyword::Values)?;
            InsertSource::DefaultValues
        } else {
            return Err(PrismDBError::Parse(
                "Expected VALUES, SELECT, or DEFAULT VALUES".to_string(),
            ));
        };

        Ok(InsertStatement {
            table_name,
            columns,
            source,
            on_conflict: None,
        })
    }

    /// Parse UPDATE statement
    fn parse_update_statement(&mut self) -> PrismDBResult<UpdateStatement> {
        self.consume_keyword(Keyword::Update)?;

        let table_name = self.consume_identifier()?;

        self.consume_keyword(Keyword::Set)?;

        let mut assignments = Vec::new();
        loop {
            let column = self.consume_identifier()?;
            self.consume_token(&TokenType::Equals)?;
            let value = self.parse_expression()?;
            assignments.push(Assignment { column, value });

            if self.consume_token(&TokenType::Comma).is_err() {
                break;
            }
        }

        let where_clause = if self.consume_keyword(Keyword::Where).is_ok() {
            Some(self.parse_expression()?)
        } else {
            None
        };

        Ok(UpdateStatement {
            table_name,
            assignments,
            where_clause,
        })
    }

    /// Parse DELETE statement
    fn parse_delete_statement(&mut self) -> PrismDBResult<DeleteStatement> {
        self.consume_keyword(Keyword::Delete)?;
        self.consume_keyword(Keyword::From)?;

        let table_name = self.consume_identifier()?;

        let where_clause = if self.consume_keyword(Keyword::Where).is_ok() {
            Some(self.parse_expression()?)
        } else {
            None
        };

        Ok(DeleteStatement {
            table_name,
            where_clause,
        })
    }

    /// Parse CREATE statement
    fn parse_create_statement(&mut self) -> PrismDBResult<Statement> {
        self.consume_keyword(Keyword::Create)?;

        // Check for OR REPLACE
        let or_replace = if self.consume_keyword(Keyword::Or).is_ok() {
            self.consume_keyword(Keyword::Replace)?;
            true
        } else {
            false
        };

        match &self.current_token().token_type {
            TokenType::Keyword(Keyword::Table) => {
                let table = self.parse_create_table_statement()?;
                Ok(Statement::CreateTable(table))
            }
            TokenType::Keyword(Keyword::View) => {
                let view = self.parse_create_view_statement()?;
                Ok(Statement::CreateView(view))
            }
            TokenType::Keyword(Keyword::Index) => {
                let index = self.parse_create_index_statement()?;
                Ok(Statement::CreateIndex(index))
            }
            TokenType::Identifier(_) if or_replace => {
                // This might be CREATE OR REPLACE SECRET
                let identifier = self.consume_identifier()?;
                if identifier.to_uppercase() == "SECRET" {
                    let secret = self.parse_create_secret_body(or_replace)?;
                    Ok(Statement::CreateSecret(secret))
                } else {
                    Err(PrismDBError::Parse(format!(
                        "Unexpected identifier '{}' after CREATE OR REPLACE",
                        identifier
                    )))
                }
            }
            _ => Err(PrismDBError::Parse(
                "Expected TABLE, VIEW, INDEX, or SECRET after CREATE".to_string(),
            )),
        }
    }

    /// Parse CREATE TABLE statement
    fn parse_create_table_statement(&mut self) -> PrismDBResult<CreateTableStatement> {
        let if_not_exists = self.consume_keyword(Keyword::If).is_ok()
            && self.consume_keyword(Keyword::Not).is_ok()
            && self.consume_keyword(Keyword::Exists).is_ok();

        self.consume_keyword(Keyword::Table)?;

        let table_name = self.consume_identifier()?;

        self.consume_token(&TokenType::LeftParen)?;

        let mut columns = Vec::new();
        let mut constraints = Vec::new();

        loop {
            if self.current_token().token_type == TokenType::Keyword(Keyword::Primary)
                || self.current_token().token_type == TokenType::Keyword(Keyword::Foreign)
                || self.current_token().token_type == TokenType::Keyword(Keyword::Unique)
                || self.current_token().token_type == TokenType::Keyword(Keyword::Check)
            {
                constraints.push(self.parse_table_constraint()?);
            } else {
                columns.push(self.parse_column_definition()?);
            }

            if self.consume_token(&TokenType::Comma).is_err() {
                break;
            }
        }

        self.consume_token(&TokenType::RightParen)?;

        Ok(CreateTableStatement {
            table_name,
            columns,
            constraints,
            if_not_exists,
        })
    }

    /// Parse column definition
    fn parse_column_definition(&mut self) -> PrismDBResult<ColumnDefinition> {
        let name = self.consume_identifier()?;
        let data_type = self.parse_data_type()?;

        let mut nullable = true;
        let mut default_value = None;
        let mut constraints = Vec::new();

        loop {
            if self.consume_keyword(Keyword::Not).is_ok() {
                self.consume_keyword(Keyword::Null)?;
                nullable = false;
                constraints.push(ColumnConstraint::NotNull);
            } else if self.consume_keyword(Keyword::Null).is_ok() {
                nullable = true;
            } else if self.consume_keyword(Keyword::Default).is_ok() {
                default_value = Some(self.parse_expression()?);
            } else if self.consume_keyword(Keyword::Primary).is_ok() {
                self.consume_keyword(Keyword::Key)?;
                constraints.push(ColumnConstraint::PrimaryKey);
            } else if self.consume_keyword(Keyword::Unique).is_ok() {
                constraints.push(ColumnConstraint::Unique);
            } else if self.consume_keyword(Keyword::Check).is_ok() {
                self.consume_token(&TokenType::LeftParen)?;
                let expression = self.parse_expression()?;
                self.consume_token(&TokenType::RightParen)?;
                constraints.push(ColumnConstraint::Check(expression));
            } else if self.consume_keyword(Keyword::References).is_ok() {
                let table = self.consume_identifier()?;
                self.consume_token(&TokenType::LeftParen)?;
                let column = self.consume_identifier()?;
                self.consume_token(&TokenType::RightParen)?;
                constraints.push(ColumnConstraint::References { table, column });
            } else {
                break;
            }
        }

        Ok(ColumnDefinition {
            name,
            data_type,
            nullable,
            default_value,
            constraints,
        })
    }

    /// Parse data type
    fn parse_data_type(&mut self) -> PrismDBResult<LogicalType> {
        match &self.current_token().token_type {
            TokenType::Keyword(Keyword::Integer) | TokenType::Keyword(Keyword::Int) => {
                let _ = self.consume_keyword(Keyword::Integer);
                Ok(LogicalType::Integer)
            }
            TokenType::Keyword(Keyword::BigInt) => {
                let _ = self.consume_keyword(Keyword::BigInt);
                Ok(LogicalType::BigInt)
            }
            TokenType::Keyword(Keyword::SmallInt) => {
                let _ = self.consume_keyword(Keyword::SmallInt);
                Ok(LogicalType::SmallInt)
            }
            TokenType::Keyword(Keyword::TinyInt) => {
                let _ = self.consume_keyword(Keyword::TinyInt);
                Ok(LogicalType::TinyInt)
            }
            TokenType::Keyword(Keyword::Decimal) | TokenType::Keyword(Keyword::Numeric) => {
                let _ = self.consume_keyword(Keyword::Decimal);

                // Check if precision and scale are specified: DECIMAL(precision, scale)
                if self.current_token().token_type == TokenType::LeftParen {
                    self.consume_token(&TokenType::LeftParen)?;

                    // Parse precision
                    let precision_str = self.consume_numeric_literal()?;
                    let precision = precision_str.parse::<u8>().map_err(|_| {
                        PrismDBError::Parse(format!("Invalid precision for DECIMAL: {}", precision_str))
                    })?;

                    // Parse comma
                    self.consume_token(&TokenType::Comma)?;

                    // Parse scale
                    let scale_str = self.consume_numeric_literal()?;
                    let scale = scale_str.parse::<u8>().map_err(|_| {
                        PrismDBError::Parse(format!("Invalid scale for DECIMAL: {}", scale_str))
                    })?;

                    self.consume_token(&TokenType::RightParen)?;

                    Ok(LogicalType::Decimal { precision, scale })
                } else {
                    // Default to DECIMAL(10, 2) if no parameters specified
                    Ok(LogicalType::Decimal {
                        precision: 10,
                        scale: 2,
                    })
                }
            }
            TokenType::Keyword(Keyword::Real) | TokenType::Keyword(Keyword::Float) => {
                let _ = self.consume_keyword(Keyword::Real);
                Ok(LogicalType::Float)
            }
            TokenType::Keyword(Keyword::Double) => {
                let _ = self.consume_keyword(Keyword::Double);
                Ok(LogicalType::Double)
            }
            TokenType::Keyword(Keyword::Varchar) => {
                let _ = self.consume_keyword(Keyword::Varchar);
                Ok(LogicalType::Varchar)
            }
            TokenType::Keyword(Keyword::Char) => {
                let _ = self.consume_keyword(Keyword::Char);
                Ok(LogicalType::Char { length: 1 })
            }
            TokenType::Keyword(Keyword::Text) => {
                let _ = self.consume_keyword(Keyword::Text);
                Ok(LogicalType::Text)
            }
            TokenType::Keyword(Keyword::Blob) => {
                let _ = self.consume_keyword(Keyword::Blob);
                Ok(LogicalType::Blob)
            }
            TokenType::Keyword(Keyword::Boolean) | TokenType::Keyword(Keyword::Bool) => {
                let _ = self.consume_keyword(Keyword::Boolean);
                Ok(LogicalType::Boolean)
            }
            TokenType::Keyword(Keyword::Date) => {
                let _ = self.consume_keyword(Keyword::Date);
                Ok(LogicalType::Date)
            }
            TokenType::Keyword(Keyword::Time) => {
                let _ = self.consume_keyword(Keyword::Time);
                Ok(LogicalType::Time)
            }
            TokenType::Keyword(Keyword::Timestamp) => {
                let _ = self.consume_keyword(Keyword::Timestamp);
                Ok(LogicalType::Timestamp)
            }
            _ => Err(PrismDBError::Parse(format!(
                "Unknown data type: {:?}",
                self.current_token()
            ))),
        }
    }

    /// Parse table constraint
    fn parse_table_constraint(&mut self) -> PrismDBResult<TableConstraint> {
        if self.consume_keyword(Keyword::Primary).is_ok() {
            self.consume_keyword(Keyword::Key)?;
            self.consume_token(&TokenType::LeftParen)?;
            let mut columns = Vec::new();
            loop {
                columns.push(self.consume_identifier()?);
                if self.consume_token(&TokenType::Comma).is_err() {
                    break;
                }
            }
            self.consume_token(&TokenType::RightParen)?;
            Ok(TableConstraint::PrimaryKey { columns })
        } else if self.consume_keyword(Keyword::Unique).is_ok() {
            self.consume_token(&TokenType::LeftParen)?;
            let mut columns = Vec::new();
            loop {
                columns.push(self.consume_identifier()?);
                if self.consume_token(&TokenType::Comma).is_err() {
                    break;
                }
            }
            self.consume_token(&TokenType::RightParen)?;
            Ok(TableConstraint::Unique {
                columns,
                name: None,
            })
        } else if self.consume_keyword(Keyword::Foreign).is_ok() {
            self.consume_keyword(Keyword::Key)?;
            self.consume_token(&TokenType::LeftParen)?;
            let mut columns = Vec::new();
            loop {
                columns.push(self.consume_identifier()?);
                if self.consume_token(&TokenType::Comma).is_err() {
                    break;
                }
            }
            self.consume_token(&TokenType::RightParen)?;
            self.consume_keyword(Keyword::References)?;
            let foreign_table = self.consume_identifier()?;
            self.consume_token(&TokenType::LeftParen)?;
            let mut foreign_columns = Vec::new();
            loop {
                foreign_columns.push(self.consume_identifier()?);
                if self.consume_token(&TokenType::Comma).is_err() {
                    break;
                }
            }
            self.consume_token(&TokenType::RightParen)?;
            Ok(TableConstraint::ForeignKey {
                columns,
                foreign_table,
                foreign_columns,
                name: None,
            })
        } else if self.consume_keyword(Keyword::Check).is_ok() {
            self.consume_token(&TokenType::LeftParen)?;
            let expression = self.parse_expression()?;
            self.consume_token(&TokenType::RightParen)?;
            Ok(TableConstraint::Check {
                expression,
                name: None,
            })
        } else {
            Err(PrismDBError::Parse("Expected table constraint".to_string()))
        }
    }

    /// Parse DROP statement
    fn parse_drop_statement(&mut self) -> PrismDBResult<Statement> {
        self.consume_keyword(Keyword::Drop)?;

        match &self.current_token().token_type {
            TokenType::Keyword(Keyword::Table) => {
                let table = self.parse_drop_table_statement()?;
                Ok(Statement::DropTable(table))
            }
            TokenType::Keyword(Keyword::View) => {
                let view = self.parse_drop_view_statement()?;
                Ok(Statement::DropView(view))
            }
            TokenType::Keyword(Keyword::Index) => {
                let index = self.parse_drop_index_statement()?;
                Ok(Statement::DropIndex(index))
            }
            _ => Err(PrismDBError::Parse(
                "Expected TABLE, VIEW, or INDEX after DROP".to_string(),
            )),
        }
    }

    /// Parse DROP TABLE statement
    fn parse_drop_table_statement(&mut self) -> PrismDBResult<DropTableStatement> {
        let if_exists = self.consume_keyword(Keyword::If).is_ok()
            && self.consume_keyword(Keyword::Exists).is_ok();

        self.consume_keyword(Keyword::Table)?;
        let table_name = self.consume_identifier()?;

        Ok(DropTableStatement {
            table_name,
            if_exists,
        })
    }

    /// Parse DROP [MATERIALIZED] VIEW statement
    fn parse_drop_view_statement(&mut self) -> PrismDBResult<DropViewStatement> {
        // Check for MATERIALIZED keyword
        let materialized = self.consume_keyword(Keyword::Materialized).is_ok();

        let if_exists = self.consume_keyword(Keyword::If).is_ok()
            && self.consume_keyword(Keyword::Exists).is_ok();

        self.consume_keyword(Keyword::View)?;
        let view_name = self.consume_identifier()?;

        Ok(DropViewStatement {
            view_name,
            if_exists,
            materialized,
        })
    }

    /// Parse DROP INDEX statement
    fn parse_drop_index_statement(&mut self) -> PrismDBResult<DropIndexStatement> {
        let if_exists = self.consume_keyword(Keyword::If).is_ok()
            && self.consume_keyword(Keyword::Exists).is_ok();

        self.consume_keyword(Keyword::Index)?;
        let index_name = self.consume_identifier()?;

        Ok(DropIndexStatement {
            index_name,
            if_exists,
        })
    }

    /// Parse ALTER TABLE statement
    fn parse_alter_table_statement(&mut self) -> PrismDBResult<AlterTableStatement> {
        self.consume_keyword(Keyword::Alter)?;
        self.consume_keyword(Keyword::Table)?;

        let table_name = self.consume_identifier()?;

        let operation = if self.consume_keyword(Keyword::Add).is_ok() {
            if self.consume_keyword(Keyword::Column).is_ok() {
                let column = self.parse_column_definition()?;
                AlterTableOperation::AddColumn(column)
            } else if self.consume_keyword(Keyword::Constraint).is_ok() {
                let constraint = self.parse_table_constraint()?;
                AlterTableOperation::AddConstraint(constraint)
            } else {
                return Err(PrismDBError::Parse(
                    "Expected COLUMN or CONSTRAINT after ADD".to_string(),
                ));
            }
        } else if self.consume_keyword(Keyword::Drop).is_ok() {
            if self.consume_keyword(Keyword::Column).is_ok() {
                let column_name = self.consume_identifier()?;
                AlterTableOperation::DropColumn { column_name }
            } else if self.consume_keyword(Keyword::Constraint).is_ok() {
                let constraint_name = self.consume_identifier()?;
                AlterTableOperation::DropConstraint { constraint_name }
            } else {
                return Err(PrismDBError::Parse(
                    "Expected COLUMN or CONSTRAINT after DROP".to_string(),
                ));
            }
        } else if self.consume_keyword(Keyword::Rename).is_ok() {
            if self.consume_keyword(Keyword::Column).is_ok() {
                let old_name = self.consume_identifier()?;
                self.consume_keyword(Keyword::To)?;
                let new_name = self.consume_identifier()?;
                AlterTableOperation::RenameColumn { old_name, new_name }
            } else {
                self.consume_keyword(Keyword::To)?;
                let new_name = self.consume_identifier()?;
                AlterTableOperation::RenameTable { new_name }
            }
        } else {
            return Err(PrismDBError::Parse(
                "Expected ADD, DROP, or RENAME after ALTER TABLE".to_string(),
            ));
        };

        Ok(AlterTableStatement {
            table_name,
            operation,
        })
    }

    /// Parse CREATE [MATERIALIZED] VIEW statement
    fn parse_create_view_statement(&mut self) -> PrismDBResult<CreateViewStatement> {
        let or_replace = self.consume_keyword(Keyword::Or).is_ok()
            && self.consume_keyword(Keyword::Replace).is_ok();

        // Check for MATERIALIZED keyword
        let materialized = self.consume_keyword(Keyword::Materialized).is_ok();

        let if_not_exists = self.consume_keyword(Keyword::If).is_ok()
            && self.consume_keyword(Keyword::Not).is_ok()
            && self.consume_keyword(Keyword::Exists).is_ok();

        self.consume_keyword(Keyword::View)?;

        let view_name = self.consume_identifier()?;

        let mut columns = Vec::new();
        if self.current_token().token_type == TokenType::LeftParen {
            self.consume_token(&TokenType::LeftParen)?;
            loop {
                columns.push(self.consume_identifier()?);
                if self.consume_token(&TokenType::Comma).is_err() {
                    break;
                }
            }
            self.consume_token(&TokenType::RightParen)?;
        }

        // Parse refresh strategy for materialized views
        let refresh_strategy = if materialized {
            self.parse_refresh_strategy()?
        } else {
            None
        };

        self.consume_keyword(Keyword::As)?;
        let query = self.parse_select_statement()?;

        Ok(CreateViewStatement {
            view_name,
            columns,
            query,
            or_replace,
            if_not_exists,
            materialized,
            refresh_strategy,
        })
    }

    /// Parse refresh strategy for materialized views
    fn parse_refresh_strategy(&mut self) -> PrismDBResult<Option<ViewRefreshStrategy>> {
        use crate::parser::ast::ViewRefreshStrategy;

        // Check for WITH clause (for future WITH [NO] DATA support)
        // For now, we skip this and default to Manual refresh
        // Actual refresh happens separately via REFRESH command

        // For now, default to Manual refresh strategy
        // Future: parse REFRESH ON COMMIT, etc.
        Ok(Some(ViewRefreshStrategy::Manual))
    }

    /// Parse CREATE INDEX statement
    fn parse_create_index_statement(&mut self) -> PrismDBResult<CreateIndexStatement> {
        let unique = self.consume_keyword(Keyword::Unique).is_ok();

        let if_not_exists = self.consume_keyword(Keyword::If).is_ok()
            && self.consume_keyword(Keyword::Not).is_ok()
            && self.consume_keyword(Keyword::Exists).is_ok();

        self.consume_keyword(Keyword::Index)?;

        let index_name = self.consume_identifier()?;

        self.consume_keyword(Keyword::On)?;
        let table_name = self.consume_identifier()?;

        self.consume_token(&TokenType::LeftParen)?;
        let mut columns = Vec::new();
        loop {
            columns.push(self.consume_identifier()?);
            if self.consume_token(&TokenType::Comma).is_err() {
                break;
            }
        }
        self.consume_token(&TokenType::RightParen)?;

        Ok(CreateIndexStatement {
            index_name,
            table_name,
            columns,
            unique,
            if_not_exists,
        })
    }

    /// Parse BEGIN statement
    fn parse_begin_statement(&mut self) -> PrismDBResult<BeginStatement> {
        self.consume_keyword(Keyword::Begin)?;

        let transaction_mode = if self.consume_keyword(Keyword::Transaction).is_ok() {
            if self.consume_keyword(Keyword::Read).is_ok() {
                if self.consume_keyword(Keyword::Only).is_ok() {
                    Some(TransactionMode::ReadOnly)
                } else if self.consume_keyword(Keyword::Write).is_ok() {
                    Some(TransactionMode::ReadWrite)
                } else {
                    None
                }
            } else if self.consume_keyword(Keyword::Serializable).is_ok() {
                Some(TransactionMode::Serializable)
            } else if self.consume_keyword(Keyword::Repeatable).is_ok() {
                self.consume_keyword(Keyword::Read)?;
                Some(TransactionMode::RepeatableRead)
            } else if self.consume_keyword(Keyword::Read).is_ok() {
                self.consume_keyword(Keyword::Committed)?;
                Some(TransactionMode::ReadCommitted)
            } else {
                None
            }
        } else {
            None
        };

        Ok(BeginStatement { transaction_mode })
    }

    /// Parse COMMIT statement
    fn parse_commit_statement(&mut self) -> PrismDBResult<CommitStatement> {
        self.consume_keyword(Keyword::Commit)?;
        let chain = self.consume_keyword(Keyword::And).is_ok()
            && self.consume_keyword(Keyword::Chain).is_ok();

        Ok(CommitStatement { chain })
    }

    /// Parse ROLLBACK statement
    fn parse_rollback_statement(&mut self) -> PrismDBResult<RollbackStatement> {
        self.consume_keyword(Keyword::Rollback)?;

        let savepoint = if self.consume_keyword(Keyword::To).is_ok() {
            self.consume_keyword(Keyword::Savepoint)?;
            Some(self.consume_identifier()?)
        } else {
            None
        };

        let chain = self.consume_keyword(Keyword::And).is_ok()
            && self.consume_keyword(Keyword::Chain).is_ok();

        Ok(RollbackStatement { savepoint, chain })
    }

    /// Parse EXPLAIN statement
    fn parse_explain_statement(&mut self) -> PrismDBResult<ExplainStatement> {
        self.consume_keyword(Keyword::Explain)?;

        let analyze = self.consume_keyword(Keyword::Analyze).is_ok();
        let verbose = self.consume_keyword(Keyword::Verbose).is_ok();

        let statement = self.parse_statement_internal()?;

        Ok(ExplainStatement {
            statement: Box::new(statement),
            analyze,
            verbose,
        })
    }

    /// Parse SHOW statement
    fn parse_show_statement(&mut self) -> PrismDBResult<ShowStatement> {
        self.consume_keyword(Keyword::Show)?;

        match &self.current_token().token_type {
            TokenType::Keyword(Keyword::Tables) => {
                let _ = self.consume_keyword(Keyword::Tables);
                Ok(ShowStatement::Tables)
            }
            TokenType::Keyword(Keyword::Columns) => {
                let _ = self.consume_keyword(Keyword::Columns);
                self.consume_keyword(Keyword::From)?;
                let table = self.consume_identifier()?;
                Ok(ShowStatement::Columns { table })
            }
            TokenType::Keyword(Keyword::Indexes) => {
                let _ = self.consume_keyword(Keyword::Indexes);
                let table = if self.consume_keyword(Keyword::From).is_ok() {
                    Some(self.consume_identifier()?)
                } else {
                    None
                };
                Ok(ShowStatement::Indexes { table })
            }
            TokenType::Keyword(Keyword::Variables) => {
                let _ = self.consume_keyword(Keyword::Variables);
                Ok(ShowStatement::Variables)
            }
            TokenType::Keyword(Keyword::Databases) => {
                let _ = self.consume_keyword(Keyword::Databases);
                Ok(ShowStatement::Databases)
            }
            TokenType::Keyword(Keyword::Schemas) => {
                let _ = self.consume_keyword(Keyword::Schemas);
                Ok(ShowStatement::Schemas)
            }
            _ => Err(PrismDBError::Parse(
                "Expected TABLES, COLUMNS, INDEXES, VARIABLES, DATABASES, or SCHEMAS after SHOW"
                    .to_string(),
            )),
        }
    }

    fn parse_install_statement(&mut self) -> PrismDBResult<InstallStatement> {
        self.consume_keyword(Keyword::Install)?;
        let extension_name = self.consume_identifier()?;
        Ok(InstallStatement { extension_name })
    }

    fn parse_load_statement(&mut self) -> PrismDBResult<LoadStatement> {
        self.consume_keyword(Keyword::Load)?;
        let extension_name = self.consume_identifier()?;
        Ok(LoadStatement { extension_name })
    }

    fn parse_set_statement(&mut self) -> PrismDBResult<SetStatement> {
        self.consume_keyword(Keyword::Set)?;
        let variable = self.consume_identifier()?;

        // Consume = or TO
        if self.consume_token(&TokenType::Equals).is_err() {
            self.consume_keyword(Keyword::To)?;
        }

        // Parse the value
        let value = match &self.current_token().token_type {
            TokenType::StringLiteral(s) => {
                let val = s.clone();
                self.position += 1;
                SetValue::String(val)
            }
            TokenType::NumericLiteral(n) => {
                let val = n.parse::<i64>().unwrap_or(0);
                self.position += 1;
                SetValue::Number(val)
            }
            TokenType::Keyword(Keyword::True) => {
                self.position += 1;
                SetValue::Boolean(true)
            }
            TokenType::Keyword(Keyword::False) => {
                self.position += 1;
                SetValue::Boolean(false)
            }
            TokenType::Keyword(Keyword::Default) => {
                self.position += 1;
                SetValue::Default
            }
            TokenType::Identifier(s) => {
                // Treat identifiers as strings (for values like 'path', 'us-east-1', etc.)
                let val = s.clone();
                self.position += 1;
                SetValue::String(val)
            }
            _ => {
                return Err(PrismDBError::Parse(format!(
                    "Expected value after SET {}, got {:?}",
                    variable,
                    self.current_token()
                )));
            }
        };

        Ok(SetStatement { variable, value })
    }

    fn parse_create_secret_body(&mut self, or_replace: bool) -> PrismDBResult<CreateSecretStatement> {
        // Expect: secret_name (
        let name = self.consume_identifier()?;
        self.consume_token(&TokenType::LeftParen)?;

        let mut options = HashMap::new();

        // Parse options: KEY value, KEY value, ...
        loop {
            if self.current_token().token_type == TokenType::RightParen {
                break;
            }

            // Parse option key (identifier or keyword)
            let key = match &self.current_token().token_type {
                TokenType::Identifier(s) => {
                    let k = s.clone();
                    self.position += 1;
                    k
                }
                TokenType::Keyword(kw) => {
                    let k = format!("{:?}", kw).to_uppercase();
                    self.position += 1;
                    k
                }
                _ => {
                    return Err(PrismDBError::Parse(format!(
                        "Expected option key in CREATE SECRET, got {:?}",
                        self.current_token()
                    )));
                }
            };

            // Parse option value (string literal or identifier)
            let value = match &self.current_token().token_type {
                TokenType::StringLiteral(s) => {
                    let v = s.clone();
                    self.position += 1;
                    v
                }
                TokenType::Identifier(s) => {
                    let v = s.clone();
                    self.position += 1;
                    v
                }
                TokenType::Keyword(Keyword::True) => {
                    self.position += 1;
                    "true".to_string()
                }
                TokenType::Keyword(Keyword::False) => {
                    self.position += 1;
                    "false".to_string()
                }
                _ => {
                    return Err(PrismDBError::Parse(format!(
                        "Expected option value in CREATE SECRET, got {:?}",
                        self.current_token()
                    )));
                }
            };

            options.insert(key.to_lowercase(), value);

            // Consume optional comma
            if self.consume_token(&TokenType::Comma).is_err() {
                break;
            }
        }

        self.consume_token(&TokenType::RightParen)?;

        // Extract TYPE from options (required)
        let secret_type = options
            .get("type")
            .cloned()
            .ok_or_else(|| PrismDBError::Parse("CREATE SECRET requires TYPE option".to_string()))?;

        Ok(CreateSecretStatement {
            or_replace,
            name,
            secret_type,
            options,
        })
    }

    // Helper methods

    fn current_token(&self) -> &Token {
        &self.tokens[self.position]
    }

    fn peek_token(&self) -> &Token {
        if self.position + 1 < self.tokens.len() {
            &self.tokens[self.position + 1]
        } else {
            &self.tokens[self.tokens.len() - 1] // EOF token
        }
    }

    fn consume_token(&mut self, token_type: &TokenType) -> PrismDBResult<&Token> {
        if self.current_token().token_type == *token_type {
            let token = &self.tokens[self.position];
            self.position += 1;
            Ok(token)
        } else {
            Err(PrismDBError::Parse(format!(
                "Expected token '{:?}', found '{}'",
                token_type,
                self.current_token().text
            )))
        }
    }

    fn consume_keyword(&mut self, keyword: Keyword) -> PrismDBResult<&Token> {
        if self.current_token().is_keyword(keyword) {
            let token = &self.tokens[self.position];
            self.position += 1;
            Ok(token)
        } else {
            Err(PrismDBError::Parse(format!(
                "Expected keyword '{}', found '{}'",
                keyword,
                self.current_token().text
            )))
        }
    }

    fn consume_identifier(&mut self) -> PrismDBResult<String> {
        match &self.current_token().token_type {
            TokenType::Identifier(name) => {
                let name = name.clone();
                self.position += 1;
                Ok(name)
            }
            TokenType::Keyword(_) => {
                // Allow keywords to be used as identifiers in unambiguous contexts
                let name = self.current_token().text.clone();
                self.position += 1;
                Ok(name)
            }
            _ => Err(PrismDBError::Parse(format!(
                "Expected identifier, got: {:?}",
                self.current_token()
            ))),
        }
    }

    /// Consume an identifier or keyword (for use in aliases where keywords are allowed)
    fn consume_identifier_or_keyword(&mut self) -> PrismDBResult<String> {
        match &self.current_token().token_type {
            TokenType::Identifier(name) => {
                let name = name.clone();
                self.position += 1;
                Ok(name)
            }
            TokenType::Keyword(_) => {
                // Allow keywords to be used as identifiers (for aliases)
                let name = self.current_token().text.clone();
                self.position += 1;
                Ok(name)
            }
            _ => Err(PrismDBError::Parse(format!(
                "Expected identifier or keyword, got: {:?}",
                self.current_token()
            ))),
        }
    }

    /// Parse a comma-separated list of identifiers
    fn parse_identifier_list(&mut self) -> PrismDBResult<Vec<String>> {
        let mut identifiers = Vec::new();
        loop {
            identifiers.push(self.consume_identifier()?);
            if self.consume_token(&TokenType::Comma).is_err() {
                break;
            }
        }
        Ok(identifiers)
    }

    fn consume_string_literal(&mut self) -> PrismDBResult<String> {
        match &self.current_token().token_type {
            TokenType::StringLiteral(value) => {
                let value = value.clone();
                self.position += 1;
                Ok(value)
            }
            _ => Err(PrismDBError::Parse(format!(
                "Expected string literal, got: {:?}",
                self.current_token()
            ))),
        }
    }

    fn consume_numeric_literal(&mut self) -> PrismDBResult<String> {
        match &self.current_token().token_type {
            TokenType::NumericLiteral(value) => {
                let value = value.clone();
                self.position += 1;
                Ok(value)
            }
            _ => Err(PrismDBError::Parse(format!(
                "Expected numeric literal, got: {:?}",
                self.current_token()
            ))),
        }
    }

    fn parse_literal_integer(&mut self) -> PrismDBResult<usize> {
        let value = self.consume_numeric_literal()?;
        value
            .parse()
            .map_err(|_| PrismDBError::Parse(format!("Expected integer, got: {}", value)))
    }

    fn is_join_keyword(&self) -> bool {
        matches!(
            self.current_token().token_type,
            TokenType::Keyword(Keyword::Join)
                | TokenType::Keyword(Keyword::Inner)
                | TokenType::Keyword(Keyword::Left)
                | TokenType::Keyword(Keyword::Right)
                | TokenType::Keyword(Keyword::Full)
                | TokenType::Keyword(Keyword::Cross)
        )
    }

    /// Parse PIVOT specification
    /// Syntax: PIVOT ( aggregate_list FOR column_list IN ( value_list ) [GROUP BY group_list] )
    fn parse_pivot_spec(&mut self) -> PrismDBResult<PivotSpec> {
        use crate::parser::ast::{PivotInValue, PivotSpec, PivotValue};

        self.consume_token(&TokenType::LeftParen)?;

        // Parse aggregate expressions (USING clause / values)
        let mut using_values = Vec::new();
        loop {
            let expr = self.parse_expression()?;
            let alias = if self.consume_keyword(Keyword::As).is_ok() {
                Some(self.consume_identifier_or_keyword()?)
            } else {
                None
            };
            using_values.push(PivotValue {
                expression: expr,
                alias,
            });

            if self.consume_token(&TokenType::Comma).is_err() {
                break;
            }
        }

        // Parse FOR clause (column list, not full expressions to avoid IN confusion)
        self.consume_keyword(Keyword::For)?;
        let mut on_columns = Vec::new();
        loop {
            let col_name = self.consume_identifier()?;
            on_columns.push(Expression::ColumnReference {
                table: None,
                column: col_name,
            });
            if self.consume_token(&TokenType::Comma).is_err() {
                break;
            }
        }

        // Parse IN clause
        self.consume_keyword(Keyword::In)?;
        self.consume_token(&TokenType::LeftParen)?;

        let mut in_values = Vec::new();
        loop {
            let value = self.parse_expression()?;
            let alias = if self.consume_keyword(Keyword::As).is_ok() {
                Some(self.consume_identifier_or_keyword()?)
            } else {
                None
            };
            in_values.push(PivotInValue { value, alias });

            if self.consume_token(&TokenType::Comma).is_err() {
                break;
            }
        }
        self.consume_token(&TokenType::RightParen)?;

        // Parse optional GROUP BY clause
        let group_by = if self.consume_keyword(Keyword::Group).is_ok() {
            self.consume_keyword(Keyword::By)?;
            self.parse_expression_list()?
        } else {
            Vec::new()
        };

        self.consume_token(&TokenType::RightParen)?;

        Ok(PivotSpec {
            on_columns,
            using_values,
            in_values: Some(in_values),
            group_by,
        })
    }

    /// Parse UNPIVOT specification
    /// Syntax: UNPIVOT [INCLUDE NULLS] ( value_column FOR name_column IN ( column_list ) )
    fn parse_unpivot_spec(&mut self) -> PrismDBResult<UnpivotSpec> {
        use crate::parser::ast::UnpivotSpec;

        // Check for INCLUDE NULLS option
        let include_nulls = if self.consume_keyword(Keyword::Include).is_ok() {
            self.consume_keyword(Keyword::Nulls)?;
            true
        } else {
            false
        };

        self.consume_token(&TokenType::LeftParen)?;

        // Parse value column(s)
        let mut value_columns = Vec::new();
        loop {
            let col = self.consume_identifier()?;
            value_columns.push(col);

            if self.consume_token(&TokenType::Comma).is_err() {
                break;
            }
        }

        // Parse FOR clause
        self.consume_keyword(Keyword::For)?;
        let name_column = self.consume_identifier()?;

        // Parse IN clause
        self.consume_keyword(Keyword::In)?;
        self.consume_token(&TokenType::LeftParen)?;

        let mut on_columns = Vec::new();
        loop {
            let col = self.parse_expression()?;
            on_columns.push(col);

            if self.consume_token(&TokenType::Comma).is_err() {
                break;
            }
        }
        self.consume_token(&TokenType::RightParen)?;

        self.consume_token(&TokenType::RightParen)?;

        Ok(UnpivotSpec {
            on_columns,
            name_column,
            value_columns,
            include_nulls,
        })
    }

    /// Parse REFRESH MATERIALIZED VIEW statement
    fn parse_refresh_materialized_view_statement(&mut self) -> PrismDBResult<RefreshMaterializedViewStatement> {
        use crate::parser::ast::RefreshMaterializedViewStatement;

        self.consume_keyword(Keyword::Refresh)?;
        self.consume_keyword(Keyword::Materialized)?;
        self.consume_keyword(Keyword::View)?;

        // Check for CONCURRENTLY option
        let concurrently = self.consume_keyword(Keyword::Concurrently).is_ok();

        let view_name = self.consume_identifier()?;

        Ok(RefreshMaterializedViewStatement {
            view_name,
            concurrently,
        })
    }
}
