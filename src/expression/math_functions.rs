//! Mathematical Functions
//!
//! This module implements PrismDB's mathematical functions for 100% compatibility.
//! Includes: trigonometry, logarithms, rounding, statistical, and more.

use crate::common::error::{PrismDBError, PrismDBResult};
use crate::types::Value;
use std::f64::consts::{E, PI};

/// ABS - Absolute value
pub fn abs(value: &Value) -> PrismDBResult<Value> {
    match value {
        Value::TinyInt(v) => Ok(Value::TinyInt(v.abs())),
        Value::SmallInt(v) => Ok(Value::SmallInt(v.abs())),
        Value::Integer(v) => Ok(Value::Integer(v.abs())),
        Value::BigInt(v) => Ok(Value::BigInt(v.abs())),
        Value::HugeInt { high, low } => {
            // Handle 128-bit integer absolute value
            if *high < 0 {
                // Negate the 128-bit value
                let neg_low = (!low).wrapping_add(1);
                let neg_high = !high + if neg_low == 0 && *low != 0 { 1 } else { 0 };
                Ok(Value::HugeInt {
                    high: neg_high,
                    low: neg_low,
                })
            } else {
                Ok(Value::HugeInt {
                    high: *high,
                    low: *low,
                })
            }
        }
        Value::Float(v) => Ok(Value::Float(v.abs())),
        Value::Double(v) => Ok(Value::Double(v.abs())),
        Value::Decimal {
            value,
            scale,
            precision,
        } => Ok(Value::Decimal {
            value: value.abs(),
            scale: *scale,
            precision: *precision,
        }),
        Value::Null => Ok(Value::Null),
        _ => Err(PrismDBError::Type(format!(
            "ABS not supported for {:?}",
            value
        ))),
    }
}

/// SIGN - Sign of a number (-1, 0, or 1)
pub fn sign(value: &Value) -> PrismDBResult<Value> {
    match value {
        Value::TinyInt(v) => Ok(Value::TinyInt(if *v < 0 {
            -1
        } else if *v > 0 {
            1
        } else {
            0
        })),
        Value::SmallInt(v) => Ok(Value::SmallInt(if *v < 0 {
            -1
        } else if *v > 0 {
            1
        } else {
            0
        })),
        Value::Integer(v) => Ok(Value::Integer(if *v < 0 {
            -1
        } else if *v > 0 {
            1
        } else {
            0
        })),
        Value::BigInt(v) => Ok(Value::BigInt(if *v < 0 {
            -1
        } else if *v > 0 {
            1
        } else {
            0
        })),
        Value::Float(v) => Ok(Value::Float(if *v < 0.0 {
            -1.0
        } else if *v > 0.0 {
            1.0
        } else {
            0.0
        })),
        Value::Double(v) => Ok(Value::Double(if *v < 0.0 {
            -1.0
        } else if *v > 0.0 {
            1.0
        } else {
            0.0
        })),
        Value::Null => Ok(Value::Null),
        _ => Err(PrismDBError::Type(format!(
            "SIGN not supported for {:?}",
            value
        ))),
    }
}

/// SQRT - Square root
pub fn sqrt(value: &Value) -> PrismDBResult<Value> {
    match value {
        Value::Integer(v) => Ok(Value::Double((*v as f64).sqrt())),
        Value::BigInt(v) => Ok(Value::Double((*v as f64).sqrt())),
        Value::Float(v) => Ok(Value::Float(v.sqrt())),
        Value::Double(v) => Ok(Value::Double(v.sqrt())),
        Value::Null => Ok(Value::Null),
        _ => Err(PrismDBError::Type(format!(
            "SQRT not supported for {:?}",
            value
        ))),
    }
}

/// POW/POWER - Raise to power
pub fn power(base: &Value, exponent: &Value) -> PrismDBResult<Value> {
    let base_f64 = match base {
        Value::Integer(v) => *v as f64,
        Value::BigInt(v) => *v as f64,
        Value::Float(v) => *v as f64,
        Value::Double(v) => *v,
        Value::Null => return Ok(Value::Null),
        _ => return Err(PrismDBError::Type("POWER base must be numeric".to_string())),
    };

    let exp_f64 = match exponent {
        Value::Integer(v) => *v as f64,
        Value::BigInt(v) => *v as f64,
        Value::Float(v) => *v as f64,
        Value::Double(v) => *v,
        Value::Null => return Ok(Value::Null),
        _ => {
            return Err(PrismDBError::Type(
                "POWER exponent must be numeric".to_string(),
            ))
        }
    };

    Ok(Value::Double(base_f64.powf(exp_f64)))
}

/// EXP - Exponential (e^x)
pub fn exp(value: &Value) -> PrismDBResult<Value> {
    match value {
        Value::Integer(v) => Ok(Value::Double(E.powf(*v as f64))),
        Value::BigInt(v) => Ok(Value::Double(E.powf(*v as f64))),
        Value::Float(v) => Ok(Value::Float(E.powf(*v as f64) as f32)),
        Value::Double(v) => Ok(Value::Double(E.powf(*v))),
        Value::Null => Ok(Value::Null),
        _ => Err(PrismDBError::Type(format!(
            "EXP not supported for {:?}",
            value
        ))),
    }
}

/// LN - Natural logarithm (base e)
pub fn ln(value: &Value) -> PrismDBResult<Value> {
    match value {
        Value::Integer(v) => Ok(Value::Double((*v as f64).ln())),
        Value::BigInt(v) => Ok(Value::Double((*v as f64).ln())),
        Value::Float(v) => Ok(Value::Float(v.ln())),
        Value::Double(v) => Ok(Value::Double(v.ln())),
        Value::Null => Ok(Value::Null),
        _ => Err(PrismDBError::Type(format!(
            "LN not supported for {:?}",
            value
        ))),
    }
}

/// LOG - Logarithm (base 10)
pub fn log10(value: &Value) -> PrismDBResult<Value> {
    match value {
        Value::Integer(v) => Ok(Value::Double((*v as f64).log10())),
        Value::BigInt(v) => Ok(Value::Double((*v as f64).log10())),
        Value::Float(v) => Ok(Value::Float(v.log10())),
        Value::Double(v) => Ok(Value::Double(v.log10())),
        Value::Null => Ok(Value::Null),
        _ => Err(PrismDBError::Type(format!(
            "LOG10 not supported for {:?}",
            value
        ))),
    }
}

/// LOG2 - Logarithm (base 2)
pub fn log2(value: &Value) -> PrismDBResult<Value> {
    match value {
        Value::Integer(v) => Ok(Value::Double((*v as f64).log2())),
        Value::BigInt(v) => Ok(Value::Double((*v as f64).log2())),
        Value::Float(v) => Ok(Value::Float(v.log2())),
        Value::Double(v) => Ok(Value::Double(v.log2())),
        Value::Null => Ok(Value::Null),
        _ => Err(PrismDBError::Type(format!(
            "LOG2 not supported for {:?}",
            value
        ))),
    }
}

/// CEIL/CEILING - Round up to nearest integer
pub fn ceil(value: &Value) -> PrismDBResult<Value> {
    match value {
        Value::Integer(v) => Ok(Value::Integer(*v)),
        Value::BigInt(v) => Ok(Value::BigInt(*v)),
        Value::Float(v) => Ok(Value::Float(v.ceil())),
        Value::Double(v) => Ok(Value::Double(v.ceil())),
        Value::Decimal { value, scale, .. } => {
            // Convert decimal to double, apply ceil
            let double_val = (*value as f64) / 10f64.powi(*scale as i32);
            Ok(Value::Double(double_val.ceil()))
        }
        Value::Null => Ok(Value::Null),
        _ => Err(PrismDBError::Type(format!(
            "CEIL not supported for {:?}",
            value
        ))),
    }
}

/// FLOOR - Round down to nearest integer
pub fn floor(value: &Value) -> PrismDBResult<Value> {
    match value {
        Value::Integer(v) => Ok(Value::Integer(*v)),
        Value::BigInt(v) => Ok(Value::BigInt(*v)),
        Value::Float(v) => Ok(Value::Float(v.floor())),
        Value::Double(v) => Ok(Value::Double(v.floor())),
        Value::Decimal { value, scale, .. } => {
            // Convert decimal to double, apply floor
            let double_val = (*value as f64) / 10f64.powi(*scale as i32);
            Ok(Value::Double(double_val.floor()))
        }
        Value::Null => Ok(Value::Null),
        _ => Err(PrismDBError::Type(format!(
            "FLOOR not supported for {:?}",
            value
        ))),
    }
}

/// ROUND - Round to nearest integer or specified decimal places
pub fn round(value: &Value, decimals: Option<i32>) -> PrismDBResult<Value> {
    let places = decimals.unwrap_or(0);

    match value {
        Value::Float(v) => {
            let multiplier = 10f32.powi(places);
            Ok(Value::Float((v * multiplier).round() / multiplier))
        }
        Value::Double(v) => {
            let multiplier = 10f64.powi(places);
            Ok(Value::Double((v * multiplier).round() / multiplier))
        }
        Value::Decimal { value, scale, .. } => {
            // Convert decimal to double, apply round
            let double_val = (*value as f64) / 10f64.powi(*scale as i32);
            let multiplier = 10f64.powi(places);
            Ok(Value::Double(
                (double_val * multiplier).round() / multiplier,
            ))
        }
        Value::Integer(v) => Ok(Value::Integer(*v)),
        Value::BigInt(v) => Ok(Value::BigInt(*v)),
        Value::Null => Ok(Value::Null),
        _ => Err(PrismDBError::Type(format!(
            "ROUND not supported for {:?}",
            value
        ))),
    }
}

/// TRUNC/TRUNCATE - Truncate to integer or specified decimal places
pub fn trunc(value: &Value, decimals: Option<i32>) -> PrismDBResult<Value> {
    let places = decimals.unwrap_or(0);

    match value {
        Value::Float(v) => {
            let multiplier = 10f32.powi(places);
            Ok(Value::Float((v * multiplier).trunc() / multiplier))
        }
        Value::Double(v) => {
            let multiplier = 10f64.powi(places);
            Ok(Value::Double((v * multiplier).trunc() / multiplier))
        }
        Value::Decimal { value, scale, .. } => {
            // Convert decimal to double, apply trunc
            let double_val = (*value as f64) / 10f64.powi(*scale as i32);
            let multiplier = 10f64.powi(places);
            Ok(Value::Double(
                (double_val * multiplier).trunc() / multiplier,
            ))
        }
        Value::Integer(v) => Ok(Value::Integer(*v)),
        Value::BigInt(v) => Ok(Value::BigInt(*v)),
        Value::Null => Ok(Value::Null),
        _ => Err(PrismDBError::Type(format!(
            "TRUNC not supported for {:?}",
            value
        ))),
    }
}

// Trigonometric functions

/// SIN - Sine
pub fn sin(value: &Value) -> PrismDBResult<Value> {
    match value {
        Value::Integer(v) => Ok(Value::Double((*v as f64).sin())),
        Value::BigInt(v) => Ok(Value::Double((*v as f64).sin())),
        Value::Float(v) => Ok(Value::Float(v.sin())),
        Value::Double(v) => Ok(Value::Double(v.sin())),
        Value::Null => Ok(Value::Null),
        _ => Err(PrismDBError::Type(format!(
            "SIN not supported for {:?}",
            value
        ))),
    }
}

/// COS - Cosine
pub fn cos(value: &Value) -> PrismDBResult<Value> {
    match value {
        Value::Integer(v) => Ok(Value::Double((*v as f64).cos())),
        Value::BigInt(v) => Ok(Value::Double((*v as f64).cos())),
        Value::Float(v) => Ok(Value::Float(v.cos())),
        Value::Double(v) => Ok(Value::Double(v.cos())),
        Value::Null => Ok(Value::Null),
        _ => Err(PrismDBError::Type(format!(
            "COS not supported for {:?}",
            value
        ))),
    }
}

/// TAN - Tangent
pub fn tan(value: &Value) -> PrismDBResult<Value> {
    match value {
        Value::Integer(v) => Ok(Value::Double((*v as f64).tan())),
        Value::BigInt(v) => Ok(Value::Double((*v as f64).tan())),
        Value::Float(v) => Ok(Value::Float(v.tan())),
        Value::Double(v) => Ok(Value::Double(v.tan())),
        Value::Null => Ok(Value::Null),
        _ => Err(PrismDBError::Type(format!(
            "TAN not supported for {:?}",
            value
        ))),
    }
}

/// ASIN - Arc sine
pub fn asin(value: &Value) -> PrismDBResult<Value> {
    match value {
        Value::Integer(v) => Ok(Value::Double((*v as f64).asin())),
        Value::BigInt(v) => Ok(Value::Double((*v as f64).asin())),
        Value::Float(v) => Ok(Value::Float(v.asin())),
        Value::Double(v) => Ok(Value::Double(v.asin())),
        Value::Null => Ok(Value::Null),
        _ => Err(PrismDBError::Type(format!(
            "ASIN not supported for {:?}",
            value
        ))),
    }
}

/// ACOS - Arc cosine
pub fn acos(value: &Value) -> PrismDBResult<Value> {
    match value {
        Value::Integer(v) => Ok(Value::Double((*v as f64).acos())),
        Value::BigInt(v) => Ok(Value::Double((*v as f64).acos())),
        Value::Float(v) => Ok(Value::Float(v.acos())),
        Value::Double(v) => Ok(Value::Double(v.acos())),
        Value::Null => Ok(Value::Null),
        _ => Err(PrismDBError::Type(format!(
            "ACOS not supported for {:?}",
            value
        ))),
    }
}

/// ATAN - Arc tangent
pub fn atan(value: &Value) -> PrismDBResult<Value> {
    match value {
        Value::Integer(v) => Ok(Value::Double((*v as f64).atan())),
        Value::BigInt(v) => Ok(Value::Double((*v as f64).atan())),
        Value::Float(v) => Ok(Value::Float(v.atan())),
        Value::Double(v) => Ok(Value::Double(v.atan())),
        Value::Null => Ok(Value::Null),
        _ => Err(PrismDBError::Type(format!(
            "ATAN not supported for {:?}",
            value
        ))),
    }
}

/// ATAN2 - Arc tangent of y/x
pub fn atan2(y: &Value, x: &Value) -> PrismDBResult<Value> {
    let y_f64 = match y {
        Value::Integer(v) => *v as f64,
        Value::BigInt(v) => *v as f64,
        Value::Float(v) => *v as f64,
        Value::Double(v) => *v,
        Value::Null => return Ok(Value::Null),
        _ => return Err(PrismDBError::Type("ATAN2 y must be numeric".to_string())),
    };

    let x_f64 = match x {
        Value::Integer(v) => *v as f64,
        Value::BigInt(v) => *v as f64,
        Value::Float(v) => *v as f64,
        Value::Double(v) => *v,
        Value::Null => return Ok(Value::Null),
        _ => return Err(PrismDBError::Type("ATAN2 x must be numeric".to_string())),
    };

    Ok(Value::Double(y_f64.atan2(x_f64)))
}

/// PI - Mathematical constant π
pub fn pi() -> Value {
    Value::Double(PI)
}

/// DEGREES - Convert radians to degrees
pub fn degrees(value: &Value) -> PrismDBResult<Value> {
    match value {
        Value::Integer(v) => Ok(Value::Double((*v as f64).to_degrees())),
        Value::BigInt(v) => Ok(Value::Double((*v as f64).to_degrees())),
        Value::Float(v) => Ok(Value::Float(v.to_degrees())),
        Value::Double(v) => Ok(Value::Double(v.to_degrees())),
        Value::Null => Ok(Value::Null),
        _ => Err(PrismDBError::Type(format!(
            "DEGREES not supported for {:?}",
            value
        ))),
    }
}

/// RADIANS - Convert degrees to radians
pub fn radians(value: &Value) -> PrismDBResult<Value> {
    match value {
        Value::Integer(v) => Ok(Value::Double((*v as f64).to_radians())),
        Value::BigInt(v) => Ok(Value::Double((*v as f64).to_radians())),
        Value::Float(v) => Ok(Value::Float(v.to_radians())),
        Value::Double(v) => Ok(Value::Double(v.to_radians())),
        Value::Null => Ok(Value::Null),
        _ => Err(PrismDBError::Type(format!(
            "RADIANS not supported for {:?}",
            value
        ))),
    }
}

/// RANDOM - Generate random number between 0 and 1
pub fn random() -> Value {
    use rand::Rng;
    let mut rng = rand::rng();
    Value::Double(rng.random())
}

/// MOD - Modulo operation
pub fn mod_op(dividend: &Value, divisor: &Value) -> PrismDBResult<Value> {
    match (dividend, divisor) {
        (Value::Integer(a), Value::Integer(b)) => {
            if *b == 0 {
                return Err(PrismDBError::Execution("Division by zero".to_string()));
            }
            Ok(Value::Integer(a % b))
        }
        (Value::BigInt(a), Value::BigInt(b)) => {
            if *b == 0 {
                return Err(PrismDBError::Execution("Division by zero".to_string()));
            }
            Ok(Value::BigInt(a % b))
        }
        (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a % b)),
        (Value::Double(a), Value::Double(b)) => Ok(Value::Double(a % b)),
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => Err(PrismDBError::Type(
            "MOD requires numeric arguments".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_abs() {
        assert_eq!(abs(&Value::Integer(-5)).unwrap(), Value::Integer(5));
        assert_eq!(abs(&Value::Double(-3.14)).unwrap(), Value::Double(3.14));
    }

    #[test]
    fn test_sqrt() {
        assert_eq!(sqrt(&Value::Integer(16)).unwrap(), Value::Double(4.0));
        assert_eq!(sqrt(&Value::Double(25.0)).unwrap(), Value::Double(5.0));
    }

    #[test]
    fn test_power() {
        assert_eq!(
            power(&Value::Integer(2), &Value::Integer(3)).unwrap(),
            Value::Double(8.0)
        );
        assert_eq!(
            power(&Value::Double(2.0), &Value::Double(0.5)).unwrap(),
            Value::Double(2.0f64.sqrt())
        );
    }

    #[test]
    fn test_trigonometric() {
        let result = sin(&Value::Double(0.0)).unwrap();
        assert!(matches!(result, Value::Double(v) if v.abs() < 0.0001));

        let result = cos(&Value::Double(0.0)).unwrap();
        assert_eq!(result, Value::Double(1.0));
    }

    #[test]
    fn test_rounding() {
        assert_eq!(ceil(&Value::Double(3.2)).unwrap(), Value::Double(4.0));
        assert_eq!(floor(&Value::Double(3.8)).unwrap(), Value::Double(3.0));
        assert_eq!(
            round(&Value::Double(3.456), Some(2)).unwrap(),
            Value::Double(3.46)
        );
    }
}
