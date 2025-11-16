//! SQL Tokenizer
//!
//! Breaks SQL strings into individual tokens for parsing.

use crate::common::error::{PrismDBError, PrismDBResult};
use crate::parser::keywords::Keyword;
use std::iter::Peekable;
use std::str::Chars;

/// SQL token types
#[derive(Debug, Clone, PartialEq)]
pub enum TokenType {
    // Literals
    Identifier(String),
    StringLiteral(String),
    NumericLiteral(String),
    BooleanLiteral(bool),
    NullLiteral,

    // Keywords
    Keyword(Keyword),

    // Operators
    Plus,               // +
    Minus,              // -
    Multiply,           // *
    Divide,             // /
    Modulo,             // %
    Equals,             // =
    NotEquals,          // != or <>
    LessThan,           // <
    GreaterThan,        // >
    LessThanOrEqual,    // <=
    GreaterThanOrEqual, // >=
    And,                // AND
    Or,                 // OR
    Not,                // NOT
    Like,               // LIKE
    In,                 // IN
    Is,                 // IS
    Between,            // BETWEEN

    // Punctuation
    LeftParen,    // (
    RightParen,   // )
    Comma,        // ,
    Dot,          // .
    Semicolon,    // ;
    Colon,        // :
    QuestionMark, // ?

    // Special
    Star, // *
    EOF,
    Whitespace,
}

/// SQL token with position information
#[derive(Debug, Clone)]
pub struct Token {
    pub token_type: TokenType,
    pub text: String,
    pub line: usize,
    pub column: usize,
}

impl Token {
    pub fn new(token_type: TokenType, text: String, line: usize, column: usize) -> Self {
        Self {
            token_type,
            text,
            line,
            column,
        }
    }

    pub fn is_eof(&self) -> bool {
        matches!(self.token_type, TokenType::EOF)
    }

    pub fn is_keyword(&self, keyword: Keyword) -> bool {
        matches!(self.token_type, TokenType::Keyword(k) if k == keyword)
    }
}

/// SQL tokenizer
pub struct Tokenizer {
    keywords: std::collections::HashMap<String, Keyword>,
}

impl Tokenizer {
    pub fn new() -> Self {
        let mut keywords = std::collections::HashMap::new();

        // Initialize keyword map
        for keyword in Keyword::all() {
            keywords.insert(keyword.to_string().to_uppercase(), *keyword);
        }

        Self { keywords }
    }

    /// Tokenize a SQL string into tokens
    pub fn tokenize(&self, sql: &str) -> PrismDBResult<Vec<Token>> {
        let mut tokens = Vec::new();
        let mut chars = sql.chars().peekable();
        let mut line = 1;
        let mut column = 1;

        while let Some(&ch) = chars.peek() {
            if ch.is_whitespace() {
                self.consume_whitespace(&mut chars, &mut line, &mut column);
                continue;
            }

            let start_line = line;
            let start_column = column;

            match ch {
                '\'' => {
                    let (text, new_line, new_column) =
                        self.consume_string(&mut chars, line, column)?;
                    line = new_line;
                    column = new_column;
                    tokens.push(Token::new(
                        TokenType::StringLiteral(text),
                        String::new(),
                        start_line,
                        start_column,
                    ));
                }
                '"' => {
                    // Double quotes for quoted identifiers (e.g., "column name", "0", "NULL")
                    let (text, new_line, new_column) =
                        self.consume_quoted_identifier(&mut chars, line, column)?;
                    line = new_line;
                    column = new_column;
                    tokens.push(Token::new(
                        TokenType::Identifier(text),
                        String::new(),
                        start_line,
                        start_column,
                    ));
                }
                '0'..='9' => {
                    let (text, new_line, new_column) =
                        self.consume_number(&mut chars, line, column)?;
                    line = new_line;
                    column = new_column;
                    tokens.push(Token::new(
                        TokenType::NumericLiteral(text),
                        String::new(),
                        start_line,
                        start_column,
                    ));
                }
                '(' => {
                    chars.next();
                    column += 1;
                    tokens.push(Token::new(
                        TokenType::LeftParen,
                        "(".to_string(),
                        start_line,
                        start_column,
                    ));
                }
                ')' => {
                    chars.next();
                    column += 1;
                    tokens.push(Token::new(
                        TokenType::RightParen,
                        ")".to_string(),
                        start_line,
                        start_column,
                    ));
                }
                ',' => {
                    chars.next();
                    column += 1;
                    tokens.push(Token::new(
                        TokenType::Comma,
                        ",".to_string(),
                        start_line,
                        start_column,
                    ));
                }
                '.' => {
                    chars.next();
                    column += 1;
                    tokens.push(Token::new(
                        TokenType::Dot,
                        ".".to_string(),
                        start_line,
                        start_column,
                    ));
                }
                ';' => {
                    chars.next();
                    column += 1;
                    tokens.push(Token::new(
                        TokenType::Semicolon,
                        ";".to_string(),
                        start_line,
                        start_column,
                    ));
                }
                ':' => {
                    chars.next();
                    column += 1;
                    tokens.push(Token::new(
                        TokenType::Colon,
                        ":".to_string(),
                        start_line,
                        start_column,
                    ));
                }
                '?' => {
                    chars.next();
                    column += 1;
                    tokens.push(Token::new(
                        TokenType::QuestionMark,
                        "?".to_string(),
                        start_line,
                        start_column,
                    ));
                }
                '*' => {
                    chars.next();
                    column += 1;
                    tokens.push(Token::new(
                        TokenType::Star,
                        "*".to_string(),
                        start_line,
                        start_column,
                    ));
                }
                '+' => {
                    chars.next();
                    column += 1;
                    tokens.push(Token::new(
                        TokenType::Plus,
                        "+".to_string(),
                        start_line,
                        start_column,
                    ));
                }
                '-' => {
                    chars.next();
                    column += 1;
                    tokens.push(Token::new(
                        TokenType::Minus,
                        "-".to_string(),
                        start_line,
                        start_column,
                    ));
                }
                '/' => {
                    chars.next();
                    column += 1;
                    tokens.push(Token::new(
                        TokenType::Divide,
                        "/".to_string(),
                        start_line,
                        start_column,
                    ));
                }
                '%' => {
                    chars.next();
                    column += 1;
                    tokens.push(Token::new(
                        TokenType::Modulo,
                        "%".to_string(),
                        start_line,
                        start_column,
                    ));
                }
                '=' => {
                    chars.next();
                    column += 1;
                    tokens.push(Token::new(
                        TokenType::Equals,
                        "=".to_string(),
                        start_line,
                        start_column,
                    ));
                }
                '!' => {
                    chars.next();
                    column += 1;
                    if let Some(&'=') = chars.peek() {
                        chars.next();
                        column += 1;
                        tokens.push(Token::new(
                            TokenType::NotEquals,
                            "!=".to_string(),
                            start_line,
                            start_column,
                        ));
                    } else {
                        return Err(PrismDBError::Parse("Unexpected '!' character".to_string()));
                    }
                }
                '<' => {
                    chars.next();
                    column += 1;
                    if let Some(&'=') = chars.peek() {
                        chars.next();
                        column += 1;
                        tokens.push(Token::new(
                            TokenType::LessThanOrEqual,
                            "<=".to_string(),
                            start_line,
                            start_column,
                        ));
                    } else if let Some(&'>') = chars.peek() {
                        chars.next();
                        column += 1;
                        tokens.push(Token::new(
                            TokenType::NotEquals,
                            "<>".to_string(),
                            start_line,
                            start_column,
                        ));
                    } else {
                        tokens.push(Token::new(
                            TokenType::LessThan,
                            "<".to_string(),
                            start_line,
                            start_column,
                        ));
                    }
                }
                '>' => {
                    chars.next();
                    column += 1;
                    if let Some(&'=') = chars.peek() {
                        chars.next();
                        column += 1;
                        tokens.push(Token::new(
                            TokenType::GreaterThanOrEqual,
                            ">=".to_string(),
                            start_line,
                            start_column,
                        ));
                    } else {
                        tokens.push(Token::new(
                            TokenType::GreaterThan,
                            ">".to_string(),
                            start_line,
                            start_column,
                        ));
                    }
                }
                _ if self.is_identifier_start(ch) => {
                    let (text, new_line, new_column) =
                        self.consume_identifier(&mut chars, line, column)?;
                    line = new_line;
                    column = new_column;

                    // Check if it's a keyword
                    let upper_text = text.to_uppercase();
                    if let Some(&keyword) = self.keywords.get(&upper_text) {
                        tokens.push(Token::new(
                            TokenType::Keyword(keyword),
                            text,
                            start_line,
                            start_column,
                        ));
                    } else {
                        tokens.push(Token::new(
                            TokenType::Identifier(text),
                            String::new(),
                            start_line,
                            start_column,
                        ));
                    }
                }
                _ => {
                    return Err(PrismDBError::Parse(format!("Unexpected character: {}", ch)));
                }
            }
        }

        // Add EOF token
        tokens.push(Token::new(TokenType::EOF, String::new(), line, column));

        Ok(tokens)
    }

    fn consume_whitespace(
        &self,
        chars: &mut Peekable<Chars>,
        line: &mut usize,
        column: &mut usize,
    ) {
        while let Some(&ch) = chars.peek() {
            if ch.is_whitespace() {
                chars.next();
                if ch == '\n' {
                    *line += 1;
                    *column = 1;
                } else {
                    *column += 1;
                }
            } else {
                break;
            }
        }
    }

    fn consume_string(
        &self,
        chars: &mut Peekable<Chars>,
        mut line: usize,
        mut column: usize,
    ) -> PrismDBResult<(String, usize, usize)> {
        chars.next(); // Consume opening quote
        column += 1;

        let mut result = String::new();
        let mut escaped = false;

        while let Some(&ch) = chars.peek() {
            chars.next();
            column += 1;

            if escaped {
                match ch {
                    'n' => result.push('\n'),
                    't' => result.push('\t'),
                    'r' => result.push('\r'),
                    '\\' => result.push('\\'),
                    '\'' => result.push('\''),
                    '"' => result.push('"'),
                    _ => result.push(ch),
                }
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '\'' {
                break;
            } else if ch == '\n' {
                line += 1;
                column = 1;
                result.push(ch);
            } else {
                result.push(ch);
            }
        }

        if escaped {
            return Err(PrismDBError::Parse(
                "Unterminated escape sequence in string literal".to_string(),
            ));
        }

        Ok((result, line, column))
    }

    fn consume_quoted_identifier(
        &self,
        chars: &mut Peekable<Chars>,
        mut line: usize,
        mut column: usize,
    ) -> PrismDBResult<(String, usize, usize)> {
        chars.next(); // Consume opening double quote
        column += 1;

        let mut result = String::new();
        let mut escaped = false;

        while let Some(&ch) = chars.peek() {
            chars.next();
            column += 1;

            if escaped {
                match ch {
                    'n' => result.push('\n'),
                    't' => result.push('\t'),
                    'r' => result.push('\r'),
                    '\\' => result.push('\\'),
                    '\'' => result.push('\''),
                    '"' => result.push('"'),
                    _ => result.push(ch),
                }
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                // Check for double double-quote (SQL escape for double quote)
                if chars.peek() == Some(&'"') {
                    chars.next();
                    column += 1;
                    result.push('"');
                } else {
                    break;
                }
            } else if ch == '\n' {
                line += 1;
                column = 1;
                result.push(ch);
            } else {
                result.push(ch);
            }
        }

        if escaped {
            return Err(PrismDBError::Parse(
                "Unterminated escape sequence in quoted identifier".to_string(),
            ));
        }

        Ok((result, line, column))
    }

    fn consume_number(
        &self,
        chars: &mut Peekable<Chars>,
        line: usize,
        mut column: usize,
    ) -> PrismDBResult<(String, usize, usize)> {
        let mut result = String::new();

        // Integer part
        while let Some(&ch) = chars.peek() {
            if ch.is_ascii_digit() {
                result.push(ch);
                chars.next();
                column += 1;
            } else {
                break;
            }
        }

        // Fractional part
        if let Some(&'.') = chars.peek() {
            result.push('.');
            chars.next();
            column += 1;

            while let Some(&ch) = chars.peek() {
                if ch.is_ascii_digit() {
                    result.push(ch);
                    chars.next();
                    column += 1;
                } else {
                    break;
                }
            }
        }

        // Exponent part
        if let Some(&'e') | Some(&'E') = chars.peek() {
            result.push(chars.next().unwrap());
            column += 1;

            if let Some(&'+') | Some(&'-') = chars.peek() {
                result.push(chars.next().unwrap());
                column += 1;
            }

            while let Some(&ch) = chars.peek() {
                if ch.is_ascii_digit() {
                    result.push(ch);
                    chars.next();
                    column += 1;
                } else {
                    break;
                }
            }
        }

        Ok((result, line, column))
    }

    fn consume_identifier(
        &self,
        chars: &mut Peekable<Chars>,
        line: usize,
        mut column: usize,
    ) -> PrismDBResult<(String, usize, usize)> {
        let mut result = String::new();

        while let Some(&ch) = chars.peek() {
            if self.is_identifier_char(ch) {
                result.push(ch);
                chars.next();
                column += 1;
            } else {
                break;
            }
        }

        Ok((result, line, column))
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_ascii_alphabetic() || ch == '_'
    }

    fn is_identifier_char(&self, ch: char) -> bool {
        ch.is_ascii_alphanumeric() || ch == '_' || ch == '$'
    }
}

impl Default for Tokenizer {
    fn default() -> Self {
        Self::new()
    }
}
