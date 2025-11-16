//! SQL Parser for DuckDB
//!
//! This module provides SQL parsing capabilities that convert SQL strings into
//! structured query representations that can be planned and executed.

pub mod ast;
pub mod keywords;
pub mod parser;
pub mod tokenizer;

pub use ast::*;
pub use keywords::*;
pub use parser::*;
pub use tokenizer::*;

use crate::common::error::PrismDBResult;

/// Main parser interface
pub struct SqlParser {
    tokenizer: Tokenizer,
}

impl SqlParser {
    /// Create a new SQL parser
    pub fn new() -> Self {
        Self {
            tokenizer: Tokenizer::new(),
        }
    }

    /// Parse a SQL query string into a statement
    pub fn parse(&mut self, sql: &str) -> PrismDBResult<Statement> {
        let tokens = self.tokenizer.tokenize(sql)?;
        let mut parser = Parser::new(tokens);
        parser.parse_statement()
    }

    /// Parse multiple SQL statements
    pub fn parse_multiple(&mut self, sql: &str) -> PrismDBResult<Vec<Statement>> {
        let tokens = self.tokenizer.tokenize(sql)?;
        let mut parser = Parser::new(tokens);
        parser.parse_statements()
    }
}

impl Default for SqlParser {
    fn default() -> Self {
        Self::new()
    }
}

/// Parse a single SQL statement (convenience function)
pub fn parse_sql(sql: &str) -> PrismDBResult<Statement> {
    let mut parser = SqlParser::new();
    parser.parse(sql)
}

/// Parse multiple SQL statements (convenience function)
pub fn parse_sql_multiple(sql: &str) -> PrismDBResult<Vec<Statement>> {
    let mut parser = SqlParser::new();
    parser.parse_multiple(sql)
}
