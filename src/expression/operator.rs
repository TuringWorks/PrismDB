//! Operator definitions and implementations for DuckDB expressions

use crate::common::error::{PrismDBError, PrismDBResult};
use crate::types::Value;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Different types of operators
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OperatorType {
    // Arithmetic operators
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,

    // Bitwise operators
    BitwiseAnd,
    BitwiseOr,
    BitwiseXor,
    BitwiseLeftShift,
    BitwiseRightShift,

    // Logical operators
    And,
    Or,
    Not,

    // Comparison operators (handled in expression module)
    // Equal, NotEqual, LessThan, etc.

    // String operators
    Concat,
    Like,
    ILike,
    RegexpMatch,

    // Other operators
    Coalesce,
    NullIf,
    IsNull,
    IsNotNull,
}

/// Operator information and metadata
#[derive(Debug, Clone)]
pub struct OperatorInfo {
    pub operator_type: OperatorType,
    pub name: String,
    pub symbol: String,
    pub is_commutative: bool,
    pub is_associative: bool,
    pub precedence: u8,
    pub is_left_associative: bool,
}

impl OperatorInfo {
    pub fn new(
        operator_type: OperatorType,
        name: String,
        symbol: String,
        is_commutative: bool,
        is_associative: bool,
        precedence: u8,
        is_left_associative: bool,
    ) -> Self {
        Self {
            operator_type,
            name,
            symbol,
            is_commutative,
            is_associative,
            precedence,
            is_left_associative,
        }
    }
}

/// Get operator information for a given operator type
pub fn get_operator_info(operator_type: &OperatorType) -> OperatorInfo {
    match operator_type {
        OperatorType::Add => OperatorInfo::new(
            OperatorType::Add,
            "add".to_string(),
            "+".to_string(),
            true,
            true,
            10,
            true,
        ),
        OperatorType::Subtract => OperatorInfo::new(
            OperatorType::Subtract,
            "subtract".to_string(),
            "-".to_string(),
            false,
            false,
            10,
            true,
        ),
        OperatorType::Multiply => OperatorInfo::new(
            OperatorType::Multiply,
            "multiply".to_string(),
            "*".to_string(),
            true,
            true,
            20,
            true,
        ),
        OperatorType::Divide => OperatorInfo::new(
            OperatorType::Divide,
            "divide".to_string(),
            "/".to_string(),
            false,
            false,
            20,
            true,
        ),
        OperatorType::Modulo => OperatorInfo::new(
            OperatorType::Modulo,
            "modulo".to_string(),
            "%".to_string(),
            false,
            false,
            20,
            true,
        ),
        OperatorType::BitwiseAnd => OperatorInfo::new(
            OperatorType::BitwiseAnd,
            "bitwise_and".to_string(),
            "&".to_string(),
            true,
            true,
            6,
            true,
        ),
        OperatorType::BitwiseOr => OperatorInfo::new(
            OperatorType::BitwiseOr,
            "bitwise_or".to_string(),
            "|".to_string(),
            true,
            true,
            4,
            true,
        ),
        OperatorType::BitwiseXor => OperatorInfo::new(
            OperatorType::BitwiseXor,
            "bitwise_xor".to_string(),
            "^".to_string(),
            true,
            true,
            5,
            true,
        ),
        OperatorType::BitwiseLeftShift => OperatorInfo::new(
            OperatorType::BitwiseLeftShift,
            "bitwise_left_shift".to_string(),
            "<<".to_string(),
            false,
            false,
            7,
            true,
        ),
        OperatorType::BitwiseRightShift => OperatorInfo::new(
            OperatorType::BitwiseRightShift,
            "bitwise_right_shift".to_string(),
            ">>".to_string(),
            false,
            false,
            7,
            true,
        ),
        OperatorType::And => OperatorInfo::new(
            OperatorType::And,
            "and".to_string(),
            "AND".to_string(),
            true,
            true,
            2,
            true,
        ),
        OperatorType::Or => OperatorInfo::new(
            OperatorType::Or,
            "or".to_string(),
            "OR".to_string(),
            true,
            true,
            1,
            true,
        ),
        OperatorType::Not => OperatorInfo::new(
            OperatorType::Not,
            "not".to_string(),
            "NOT".to_string(),
            false,
            false,
            3,
            false, // NOT is right-associative
        ),
        OperatorType::Concat => OperatorInfo::new(
            OperatorType::Concat,
            "concat".to_string(),
            "||".to_string(),
            false,
            false,
            8,
            true,
        ),
        OperatorType::Like => OperatorInfo::new(
            OperatorType::Like,
            "like".to_string(),
            "LIKE".to_string(),
            false,
            false,
            9,
            true,
        ),
        OperatorType::ILike => OperatorInfo::new(
            OperatorType::ILike,
            "ilike".to_string(),
            "ILIKE".to_string(),
            false,
            false,
            9,
            true,
        ),
        OperatorType::RegexpMatch => OperatorInfo::new(
            OperatorType::RegexpMatch,
            "regexp_match".to_string(),
            "~".to_string(),
            false,
            false,
            9,
            true,
        ),
        OperatorType::Coalesce => OperatorInfo::new(
            OperatorType::Coalesce,
            "coalesce".to_string(),
            "COALESCE".to_string(),
            false,
            false,
            0,
            true,
        ),
        OperatorType::NullIf => OperatorInfo::new(
            OperatorType::NullIf,
            "nullif".to_string(),
            "NULLIF".to_string(),
            false,
            false,
            0,
            true,
        ),
        OperatorType::IsNull => OperatorInfo::new(
            OperatorType::IsNull,
            "is_null".to_string(),
            "IS NULL".to_string(),
            false,
            false,
            0,
            true,
        ),
        OperatorType::IsNotNull => OperatorInfo::new(
            OperatorType::IsNotNull,
            "is_not_null".to_string(),
            "IS NOT NULL".to_string(),
            false,
            false,
            0,
            true,
        ),
    }
}

/// Evaluate a binary operator on two values
pub fn evaluate_binary_operator(
    operator_type: &OperatorType,
    left: &Value,
    right: &Value,
) -> PrismDBResult<Value> {
    match operator_type {
        OperatorType::Add => evaluate_add(left, right),
        OperatorType::Subtract => evaluate_subtract(left, right),
        OperatorType::Multiply => evaluate_multiply(left, right),
        OperatorType::Divide => evaluate_divide(left, right),
        OperatorType::Modulo => evaluate_modulo(left, right),
        OperatorType::BitwiseAnd => evaluate_bitwise_and(left, right),
        OperatorType::BitwiseOr => evaluate_bitwise_or(left, right),
        OperatorType::BitwiseXor => evaluate_bitwise_xor(left, right),
        OperatorType::BitwiseLeftShift => evaluate_bitwise_left_shift(left, right),
        OperatorType::BitwiseRightShift => evaluate_bitwise_right_shift(left, right),
        OperatorType::And => evaluate_and(left, right),
        OperatorType::Or => evaluate_or(left, right),
        OperatorType::Concat => evaluate_concat(left, right),
        OperatorType::Like => evaluate_like(left, right),
        OperatorType::ILike => evaluate_ilike(left, right),
        OperatorType::RegexpMatch => evaluate_regexp_match(left, right),
        OperatorType::Coalesce => evaluate_coalesce(left, right),
        OperatorType::NullIf => evaluate_nullif(left, right),
        _ => Err(PrismDBError::InvalidType(format!(
            "Binary operator {:?} not implemented",
            operator_type
        ))),
    }
}

/// Evaluate a unary operator on a value
pub fn evaluate_unary_operator(
    operator_type: &OperatorType,
    operand: &Value,
) -> PrismDBResult<Value> {
    match operator_type {
        OperatorType::Not => evaluate_not(operand),
        OperatorType::IsNull => evaluate_is_null(operand),
        OperatorType::IsNotNull => evaluate_is_not_null(operand),
        _ => Err(PrismDBError::InvalidType(format!(
            "Unary operator {:?} not implemented",
            operator_type
        ))),
    }
}

// Arithmetic operators
fn evaluate_add(left: &Value, right: &Value) -> PrismDBResult<Value> {
    match (left, right) {
        (Value::Integer(l), Value::Integer(r)) => Ok(Value::Integer(l + r)),
        (Value::BigInt(l), Value::BigInt(r)) => Ok(Value::BigInt(l + r)),
        (Value::Float(l), Value::Float(r)) => Ok(Value::Float(l + r)),
        (Value::Double(l), Value::Double(r)) => Ok(Value::Double(l + r)),
        (Value::Varchar(l), Value::Varchar(r)) => Ok(Value::Varchar(format!("{}{}", l, r))),
        _ => Err(PrismDBError::Type(format!(
            "Cannot add {} and {}",
            left.get_type(),
            right.get_type()
        ))),
    }
}

fn evaluate_subtract(left: &Value, right: &Value) -> PrismDBResult<Value> {
    match (left, right) {
        (Value::Integer(l), Value::Integer(r)) => Ok(Value::Integer(l - r)),
        (Value::BigInt(l), Value::BigInt(r)) => Ok(Value::BigInt(l - r)),
        (Value::Float(l), Value::Float(r)) => Ok(Value::Float(l - r)),
        (Value::Double(l), Value::Double(r)) => Ok(Value::Double(l - r)),
        _ => Err(PrismDBError::Type(format!(
            "Cannot subtract {} from {}",
            right.get_type(),
            left.get_type()
        ))),
    }
}

fn evaluate_multiply(left: &Value, right: &Value) -> PrismDBResult<Value> {
    match (left, right) {
        (Value::Integer(l), Value::Integer(r)) => Ok(Value::Integer(l * r)),
        (Value::BigInt(l), Value::BigInt(r)) => Ok(Value::BigInt(l * r)),
        (Value::Float(l), Value::Float(r)) => Ok(Value::Float(l * r)),
        (Value::Double(l), Value::Double(r)) => Ok(Value::Double(l * r)),
        _ => Err(PrismDBError::Type(format!(
            "Cannot multiply {} and {}",
            left.get_type(),
            right.get_type()
        ))),
    }
}

fn evaluate_divide(left: &Value, right: &Value) -> PrismDBResult<Value> {
    match (left, right) {
        (Value::Integer(l), Value::Integer(r)) => {
            if *r == 0 {
                return Err(PrismDBError::Execution("Division by zero".to_string()));
            }
            Ok(Value::Integer(l / r))
        }
        (Value::BigInt(l), Value::BigInt(r)) => {
            if *r == 0 {
                return Err(PrismDBError::Execution("Division by zero".to_string()));
            }
            Ok(Value::BigInt(l / r))
        }
        (Value::Float(l), Value::Float(r)) => {
            if *r == 0.0 {
                return Err(PrismDBError::Execution("Division by zero".to_string()));
            }
            Ok(Value::Float(l / r))
        }
        (Value::Double(l), Value::Double(r)) => {
            if *r == 0.0 {
                return Err(PrismDBError::Execution("Division by zero".to_string()));
            }
            Ok(Value::Double(l / r))
        }
        _ => Err(PrismDBError::Type(format!(
            "Cannot divide {} by {}",
            left.get_type(),
            right.get_type()
        ))),
    }
}

fn evaluate_modulo(left: &Value, right: &Value) -> PrismDBResult<Value> {
    match (left, right) {
        (Value::Integer(l), Value::Integer(r)) => {
            if *r == 0 {
                return Err(PrismDBError::Execution("Modulo by zero".to_string()));
            }
            Ok(Value::Integer(l % r))
        }
        (Value::BigInt(l), Value::BigInt(r)) => {
            if *r == 0 {
                return Err(PrismDBError::Execution("Modulo by zero".to_string()));
            }
            Ok(Value::BigInt(l % r))
        }
        _ => Err(PrismDBError::Type(format!(
            "Cannot compute {} modulo {}",
            left.get_type(),
            right.get_type()
        ))),
    }
}

// Bitwise operators
fn evaluate_bitwise_and(left: &Value, right: &Value) -> PrismDBResult<Value> {
    match (left, right) {
        (Value::Integer(l), Value::Integer(r)) => Ok(Value::Integer(l & r)),
        (Value::BigInt(l), Value::BigInt(r)) => Ok(Value::BigInt(l & r)),
        _ => Err(PrismDBError::Type(format!(
            "Cannot compute bitwise AND of {} and {}",
            left.get_type(),
            right.get_type()
        ))),
    }
}

fn evaluate_bitwise_or(left: &Value, right: &Value) -> PrismDBResult<Value> {
    match (left, right) {
        (Value::Integer(l), Value::Integer(r)) => Ok(Value::Integer(l | r)),
        (Value::BigInt(l), Value::BigInt(r)) => Ok(Value::BigInt(l | r)),
        _ => Err(PrismDBError::Type(format!(
            "Cannot compute bitwise OR of {} and {}",
            left.get_type(),
            right.get_type()
        ))),
    }
}

fn evaluate_bitwise_xor(left: &Value, right: &Value) -> PrismDBResult<Value> {
    match (left, right) {
        (Value::Integer(l), Value::Integer(r)) => Ok(Value::Integer(l ^ r)),
        (Value::BigInt(l), Value::BigInt(r)) => Ok(Value::BigInt(l ^ r)),
        _ => Err(PrismDBError::Type(format!(
            "Cannot compute bitwise XOR of {} and {}",
            left.get_type(),
            right.get_type()
        ))),
    }
}

fn evaluate_bitwise_left_shift(left: &Value, right: &Value) -> PrismDBResult<Value> {
    match (left, right) {
        (Value::Integer(l), Value::Integer(r)) => Ok(Value::Integer(l << r)),
        (Value::BigInt(l), Value::BigInt(r)) => Ok(Value::BigInt(l << r)),
        _ => Err(PrismDBError::Type(format!(
            "Cannot left shift {} by {}",
            left.get_type(),
            right.get_type()
        ))),
    }
}

fn evaluate_bitwise_right_shift(left: &Value, right: &Value) -> PrismDBResult<Value> {
    match (left, right) {
        (Value::Integer(l), Value::Integer(r)) => Ok(Value::Integer(l >> r)),
        (Value::BigInt(l), Value::BigInt(r)) => Ok(Value::BigInt(l >> r)),
        _ => Err(PrismDBError::Type(format!(
            "Cannot right shift {} by {}",
            left.get_type(),
            right.get_type()
        ))),
    }
}

// Logical operators
fn evaluate_and(left: &Value, right: &Value) -> PrismDBResult<Value> {
    match (left, right) {
        (Value::Boolean(l), Value::Boolean(r)) => Ok(Value::Boolean(*l && *r)),
        _ => Err(PrismDBError::Type(format!(
            "Cannot compute AND of {} and {}",
            left.get_type(),
            right.get_type()
        ))),
    }
}

fn evaluate_or(left: &Value, right: &Value) -> PrismDBResult<Value> {
    match (left, right) {
        (Value::Boolean(l), Value::Boolean(r)) => Ok(Value::Boolean(*l || *r)),
        _ => Err(PrismDBError::Type(format!(
            "Cannot compute OR of {} and {}",
            left.get_type(),
            right.get_type()
        ))),
    }
}

fn evaluate_not(operand: &Value) -> PrismDBResult<Value> {
    match operand {
        Value::Boolean(v) => Ok(Value::Boolean(!v)),
        _ => Err(PrismDBError::Type(format!(
            "Cannot compute NOT of {}",
            operand.get_type()
        ))),
    }
}

// String operators
fn evaluate_concat(left: &Value, right: &Value) -> PrismDBResult<Value> {
    match (left, right) {
        (Value::Varchar(l), Value::Varchar(r)) => Ok(Value::Varchar(format!("{}{}", l, r))),
        _ => Err(PrismDBError::Type(format!(
            "Cannot concatenate {} and {}",
            left.get_type(),
            right.get_type()
        ))),
    }
}

fn evaluate_like(left: &Value, right: &Value) -> PrismDBResult<Value> {
    match (left, right) {
        (Value::Varchar(l), Value::Varchar(r)) => {
            let pattern = r.replace('%', ".*").replace('_', ".");
            Ok(Value::Boolean(l.matches(&pattern).next().is_some()))
        }
        _ => Err(PrismDBError::Type(format!(
            "Cannot compute LIKE of {} and {}",
            left.get_type(),
            right.get_type()
        ))),
    }
}

fn evaluate_ilike(left: &Value, right: &Value) -> PrismDBResult<Value> {
    match (left, right) {
        (Value::Varchar(l), Value::Varchar(r)) => {
            let pattern = r.to_lowercase().replace('%', ".*").replace('_', ".");
            Ok(Value::Boolean(
                l.to_lowercase().matches(&pattern).next().is_some(),
            ))
        }
        _ => Err(PrismDBError::Type(format!(
            "Cannot compute ILIKE of {} and {}",
            left.get_type(),
            right.get_type()
        ))),
    }
}

fn evaluate_regexp_match(left: &Value, right: &Value) -> PrismDBResult<Value> {
    match (left, right) {
        (Value::Varchar(l), Value::Varchar(r)) => {
            let regex = regex::Regex::new(r)
                .map_err(|e| PrismDBError::Execution(format!("Invalid regex pattern: {}", e)))?;
            Ok(Value::Boolean(regex.is_match(l)))
        }
        _ => Err(PrismDBError::Type(format!(
            "Cannot compute regexp match of {} and {}",
            left.get_type(),
            right.get_type()
        ))),
    }
}

// Special operators
fn evaluate_coalesce(left: &Value, right: &Value) -> PrismDBResult<Value> {
    if !left.is_null() {
        Ok(left.clone())
    } else {
        Ok(right.clone())
    }
}

fn evaluate_nullif(left: &Value, right: &Value) -> PrismDBResult<Value> {
    if left == right {
        Ok(Value::Null)
    } else {
        Ok(left.clone())
    }
}

fn evaluate_is_null(operand: &Value) -> PrismDBResult<Value> {
    Ok(Value::Boolean(operand.is_null()))
}

fn evaluate_is_not_null(operand: &Value) -> PrismDBResult<Value> {
    Ok(Value::Boolean(!operand.is_null()))
}

impl fmt::Display for OperatorType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let info = get_operator_info(self);
        write!(f, "{}", info.symbol)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Value;

    #[test]
    fn test_arithmetic_operators() -> PrismDBResult<()> {
        let left = Value::integer(10);
        let right = Value::integer(5);

        assert_eq!(
            evaluate_binary_operator(&OperatorType::Add, &left, &right)?,
            Value::integer(15)
        );
        assert_eq!(
            evaluate_binary_operator(&OperatorType::Subtract, &left, &right)?,
            Value::integer(5)
        );
        assert_eq!(
            evaluate_binary_operator(&OperatorType::Multiply, &left, &right)?,
            Value::integer(50)
        );
        assert_eq!(
            evaluate_binary_operator(&OperatorType::Divide, &left, &right)?,
            Value::integer(2)
        );
        assert_eq!(
            evaluate_binary_operator(&OperatorType::Modulo, &left, &right)?,
            Value::integer(0)
        );

        Ok(())
    }

    #[test]
    fn test_logical_operators() -> PrismDBResult<()> {
        let left = Value::boolean(true);
        let right = Value::boolean(false);

        assert_eq!(
            evaluate_binary_operator(&OperatorType::And, &left, &right)?,
            Value::boolean(false)
        );
        assert_eq!(
            evaluate_binary_operator(&OperatorType::Or, &left, &right)?,
            Value::boolean(true)
        );
        assert_eq!(
            evaluate_unary_operator(&OperatorType::Not, &left)?,
            Value::boolean(false)
        );

        Ok(())
    }

    #[test]
    fn test_string_operators() -> PrismDBResult<()> {
        let left = Value::varchar("hello".to_string());
        let right = Value::varchar("world".to_string());

        assert_eq!(
            evaluate_binary_operator(&OperatorType::Concat, &left, &right)?,
            Value::varchar("helloworld".to_string())
        );

        Ok(())
    }

    #[test]
    fn test_operator_info() {
        let add_info = get_operator_info(&OperatorType::Add);
        assert_eq!(add_info.symbol, "+");
        assert!(add_info.is_commutative);
        assert!(add_info.is_associative);
        assert_eq!(add_info.precedence, 10);

        let subtract_info = get_operator_info(&OperatorType::Subtract);
        assert_eq!(subtract_info.symbol, "-");
        assert!(!subtract_info.is_commutative);
        assert!(!subtract_info.is_associative);
    }
}
