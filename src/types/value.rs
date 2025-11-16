use crate::common::error::{PrismDBError, PrismDBResult};
use crate::types::logical_type::LogicalType;
use crate::types::physical_type::PhysicalType;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;

/// Represents a single value in DuckDB with type information
/// Values are the fundamental unit of data in the system
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    /// Null value (type is stored separately)
    Null,
    /// Boolean value
    Boolean(bool),
    /// 8-bit signed integer
    TinyInt(i8),
    /// 16-bit signed integer
    SmallInt(i16),
    /// 32-bit signed integer
    Integer(i32),
    /// 64-bit signed integer
    BigInt(i64),
    /// 128-bit signed integer (as two i64 values for high and low parts)
    HugeInt { high: i64, low: i64 },
    /// 32-bit floating point
    Float(f32),
    /// 64-bit double precision
    Double(f64),
    /// String value
    Varchar(String),
    /// Fixed length character string
    Char(String),
    /// Decimal value (stored as integer with scale)
    Decimal {
        value: i128,
        scale: u8,
        precision: u8,
    },
    /// Date value (days since 1970-01-01)
    Date(i32),
    /// Time value (microseconds since midnight)
    Time(i64),
    /// Timestamp value (microseconds since 1970-01-01 00:00:00 UTC)
    Timestamp(i64),
    /// Interval value
    Interval { months: i32, days: i32, micros: i64 },
    /// UUID value (stored as two u64 values)
    UUID { high: u64, low: u64 },
    /// JSON value
    JSON(String),
    /// Binary data
    Blob(Vec<u8>),
    /// List value
    List(Vec<Value>),
    /// Struct value with field values
    Struct(Vec<(String, Value)>),
    /// Map value (key-value pairs)
    Map(Vec<(Value, Value)>),
    /// Union value with tag and value
    Union { tag: usize, value: Box<Value> },
}

impl Value {
    /// Create a null value with the specified type
    pub fn null(_type: LogicalType) -> Self {
        Value::Null
    }

    /// Check if this value is null
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Get the logical type of this value
    pub fn get_type(&self) -> LogicalType {
        match self {
            Value::Null => LogicalType::Invalid, // Null needs external type info
            Value::Boolean(_) => LogicalType::Boolean,
            Value::TinyInt(_) => LogicalType::TinyInt,
            Value::SmallInt(_) => LogicalType::SmallInt,
            Value::Integer(_) => LogicalType::Integer,
            Value::BigInt(_) => LogicalType::BigInt,
            Value::HugeInt { .. } => LogicalType::HugeInt,
            Value::Float(_) => LogicalType::Float,
            Value::Double(_) => LogicalType::Double,
            Value::Varchar(_) => LogicalType::Varchar,
            Value::Char(_) => LogicalType::Char { length: 1 }, // Default length
            Value::Decimal {
                precision, scale, ..
            } => LogicalType::Decimal {
                precision: *precision,
                scale: *scale,
            },
            Value::Date(_) => LogicalType::Date,
            Value::Time(_) => LogicalType::Time,
            Value::Timestamp(_) => LogicalType::Timestamp,
            Value::Interval { .. } => LogicalType::Interval,
            Value::UUID { .. } => LogicalType::UUID,
            Value::JSON(_) => LogicalType::JSON,
            Value::Blob(_) => LogicalType::Blob,
            Value::List(values) => {
                if values.is_empty() {
                    LogicalType::List(Box::new(LogicalType::Invalid))
                } else {
                    LogicalType::List(Box::new(values[0].get_type()))
                }
            }
            Value::Struct(fields) => {
                let field_types: Vec<(String, LogicalType)> = fields
                    .iter()
                    .map(|(name, value)| (name.clone(), value.get_type()))
                    .collect();
                LogicalType::Struct(field_types)
            }
            Value::Map(pairs) => {
                if pairs.is_empty() {
                    LogicalType::Map {
                        key_type: Box::new(LogicalType::Invalid),
                        value_type: Box::new(LogicalType::Invalid),
                    }
                } else {
                    LogicalType::Map {
                        key_type: Box::new(pairs[0].0.get_type()),
                        value_type: Box::new(pairs[0].1.get_type()),
                    }
                }
            }
            Value::Union { value, .. } => LogicalType::Union(vec![value.get_type()]),
        }
    }

    /// Get the physical type of this value
    pub fn get_physical_type(&self) -> PhysicalType {
        self.get_type().get_physical_type()
    }

    /// Try to extract a boolean value
    pub fn try_as_boolean(&self) -> PrismDBResult<bool> {
        match self {
            Value::Boolean(value) => Ok(*value),
            Value::Null => Err(PrismDBError::InvalidValue(
                "Cannot extract boolean from NULL".to_string(),
            )),
            _ => Err(PrismDBError::InvalidType(format!(
                "Cannot extract boolean from {}",
                self.get_type()
            ))),
        }
    }

    /// Try to extract an i32 value
    pub fn try_as_i32(&self) -> PrismDBResult<i32> {
        match self {
            Value::Integer(value) => Ok(*value),
            Value::TinyInt(value) => Ok(*value as i32),
            Value::SmallInt(value) => Ok(*value as i32),
            Value::Null => Err(PrismDBError::InvalidValue(
                "Cannot extract i32 from NULL".to_string(),
            )),
            _ => Err(PrismDBError::InvalidType(format!(
                "Cannot extract i32 from {}",
                self.get_type()
            ))),
        }
    }

    /// Try to extract an i64 value
    pub fn try_as_i64(&self) -> PrismDBResult<i64> {
        match self {
            Value::BigInt(value) => Ok(*value),
            Value::Integer(value) => Ok(*value as i64),
            Value::SmallInt(value) => Ok(*value as i64),
            Value::TinyInt(value) => Ok(*value as i64),
            Value::Date(value) => Ok(*value as i64),
            Value::Time(value) => Ok(*value),
            Value::Timestamp(value) => Ok(*value),
            Value::Null => Err(PrismDBError::InvalidValue(
                "Cannot extract i64 from NULL".to_string(),
            )),
            _ => Err(PrismDBError::InvalidType(format!(
                "Cannot extract i64 from {}",
                self.get_type()
            ))),
        }
    }

    /// Try to extract an f64 value
    pub fn try_as_f64(&self) -> PrismDBResult<f64> {
        match self {
            Value::Double(value) => Ok(*value),
            Value::Float(value) => Ok(*value as f64),
            Value::BigInt(value) => Ok(*value as f64),
            Value::Integer(value) => Ok(*value as f64),
            Value::SmallInt(value) => Ok(*value as f64),
            Value::TinyInt(value) => Ok(*value as f64),
            Value::Null => Err(PrismDBError::InvalidValue(
                "Cannot extract f64 from NULL".to_string(),
            )),
            _ => Err(PrismDBError::InvalidType(format!(
                "Cannot extract f64 from {}",
                self.get_type()
            ))),
        }
    }

    /// Try to extract a string value
    pub fn try_as_string(&self) -> PrismDBResult<String> {
        match self {
            Value::Varchar(value) => Ok(value.clone()),
            Value::Char(value) => Ok(value.clone()),
            Value::JSON(value) => Ok(value.clone()),
            Value::Null => Err(PrismDBError::InvalidValue(
                "Cannot extract string from NULL".to_string(),
            )),
            _ => Err(PrismDBError::InvalidType(format!(
                "Cannot extract string from {}",
                self.get_type()
            ))),
        }
    }

    /// Try to extract a list value
    pub fn try_as_list(&self) -> PrismDBResult<Vec<Value>> {
        match self {
            Value::List(values) => Ok(values.clone()),
            Value::Null => Err(PrismDBError::InvalidValue(
                "Cannot extract list from NULL".to_string(),
            )),
            _ => Err(PrismDBError::InvalidType(format!(
                "Cannot extract list from {}",
                self.get_type()
            ))),
        }
    }

    /// Create a boolean value
    pub fn boolean(value: bool) -> Self {
        Value::Boolean(value)
    }

    /// Create an integer value
    pub fn integer(value: i32) -> Self {
        Value::Integer(value)
    }

    /// Create a big integer value
    pub fn bigint(value: i64) -> Self {
        Value::BigInt(value)
    }

    /// Create a double value
    pub fn double(value: f64) -> Self {
        Value::Double(value)
    }

    /// Create a string value
    pub fn varchar(value: String) -> Self {
        Value::Varchar(value)
    }

    /// Create a date value
    pub fn date(value: i32) -> Self {
        Value::Date(value)
    }

    /// Create a timestamp value
    pub fn timestamp(value: i64) -> Self {
        Value::Timestamp(value)
    }

    /// Create a list value
    pub fn list(values: Vec<Value>) -> Self {
        Value::List(values)
    }

    /// Create a struct value
    pub fn struct_(fields: Vec<(String, Value)>) -> Self {
        Value::Struct(fields)
    }

    /// Cast this value to a target type
    pub fn cast_to(&self, target_type: &LogicalType) -> PrismDBResult<Value> {
        if self.is_null() {
            return Ok(Value::Null);
        }

        // If types are the same, return a clone
        if &self.get_type() == target_type {
            return Ok(self.clone());
        }

        match (&self.get_type(), target_type) {
            // Numeric casting
            (LogicalType::Integer, LogicalType::Double) => {
                Ok(Value::Double(self.try_as_i32()? as f64))
            }
            (LogicalType::Double, LogicalType::Integer) => {
                Ok(Value::Integer(self.try_as_f64()? as i32))
            }
            (LogicalType::TinyInt, LogicalType::Integer) => Ok(Value::Integer(self.try_as_i32()?)),
            (LogicalType::Integer, LogicalType::BigInt) => Ok(Value::BigInt(self.try_as_i64()?)),

            // String casting
            (LogicalType::Integer, LogicalType::Varchar) => {
                Ok(Value::Varchar(self.try_as_i32()?.to_string()))
            }
            (LogicalType::Double, LogicalType::Varchar) => {
                Ok(Value::Varchar(self.try_as_f64()?.to_string()))
            }
            (LogicalType::Boolean, LogicalType::Varchar) => {
                Ok(Value::Varchar(self.try_as_boolean()?.to_string()))
            }

            // From string casting
            (LogicalType::Varchar, LogicalType::Integer) => {
                let string_val = self.try_as_string()?;
                Ok(Value::Integer(string_val.parse().map_err(|_| {
                    PrismDBError::InvalidValue(format!("Cannot cast '{}' to INTEGER", string_val))
                })?))
            }
            (LogicalType::Varchar, LogicalType::Double) => {
                let string_val = self.try_as_string()?;
                Ok(Value::Double(string_val.parse().map_err(|_| {
                    PrismDBError::InvalidValue(format!("Cannot cast '{}' to DOUBLE", string_val))
                })?))
            }
            (LogicalType::Varchar, LogicalType::Boolean) => {
                let string_val = self.try_as_string()?;
                let lower = string_val.to_lowercase();
                match lower.as_str() {
                    "true" | "1" | "t" | "yes" | "y" => Ok(Value::Boolean(true)),
                    "false" | "0" | "f" | "no" | "n" => Ok(Value::Boolean(false)),
                    _ => Err(PrismDBError::InvalidValue(format!(
                        "Cannot cast '{}' to BOOLEAN",
                        string_val
                    ))),
                }
            }

            _ => Err(PrismDBError::InvalidType(format!(
                "Cannot cast from {} to {}",
                self.get_type(),
                target_type
            ))),
        }
    }

    /// Compare two values for ordering
    pub fn compare(&self, other: &Value) -> PrismDBResult<Ordering> {
        match (self, other) {
            (Value::Null, Value::Null) => Ok(Ordering::Equal),
            (Value::Null, _) | (_, Value::Null) => {
                // In SQL, NULL compared to anything is NULL (unknown)
                // For our purposes, we'll treat NULL as less than any value
                match (self.is_null(), other.is_null()) {
                    (true, false) => Ok(Ordering::Less),
                    (false, true) => Ok(Ordering::Greater),
                    _ => Ok(Ordering::Equal),
                }
            }
            (Value::Boolean(a), Value::Boolean(b)) => Ok(a.cmp(b)),
            (Value::TinyInt(a), Value::TinyInt(b)) => Ok(a.cmp(b)),
            (Value::SmallInt(a), Value::SmallInt(b)) => Ok(a.cmp(b)),
            (Value::Integer(a), Value::Integer(b)) => Ok(a.cmp(b)),
            (Value::BigInt(a), Value::BigInt(b)) => Ok(a.cmp(b)),
            (Value::Float(a), Value::Float(b)) => a
                .partial_cmp(b)
                .ok_or_else(|| PrismDBError::InvalidValue("Cannot compare NaN values".to_string())),
            (Value::Double(a), Value::Double(b)) => a
                .partial_cmp(b)
                .ok_or_else(|| PrismDBError::InvalidValue("Cannot compare NaN values".to_string())),
            (Value::Varchar(a), Value::Varchar(b)) => Ok(a.cmp(b)),
            (Value::Date(a), Value::Date(b)) => Ok(a.cmp(b)),
            (Value::Time(a), Value::Time(b)) => Ok(a.cmp(b)),
            (Value::Timestamp(a), Value::Timestamp(b)) => Ok(a.cmp(b)),

            // Numeric type coercion - compare different numeric types
            // Integer vs Double
            (Value::TinyInt(a), Value::Double(b)) => (*a as f64)
                .partial_cmp(b)
                .ok_or_else(|| PrismDBError::InvalidValue("Cannot compare NaN values".to_string())),
            (Value::SmallInt(a), Value::Double(b)) => (*a as f64)
                .partial_cmp(b)
                .ok_or_else(|| PrismDBError::InvalidValue("Cannot compare NaN values".to_string())),
            (Value::Integer(a), Value::Double(b)) => (*a as f64)
                .partial_cmp(b)
                .ok_or_else(|| PrismDBError::InvalidValue("Cannot compare NaN values".to_string())),
            (Value::BigInt(a), Value::Double(b)) => (*a as f64)
                .partial_cmp(b)
                .ok_or_else(|| PrismDBError::InvalidValue("Cannot compare NaN values".to_string())),

            // Double vs Integer (reverse)
            (Value::Double(a), Value::TinyInt(b)) => a
                .partial_cmp(&(*b as f64))
                .ok_or_else(|| PrismDBError::InvalidValue("Cannot compare NaN values".to_string())),
            (Value::Double(a), Value::SmallInt(b)) => a
                .partial_cmp(&(*b as f64))
                .ok_or_else(|| PrismDBError::InvalidValue("Cannot compare NaN values".to_string())),
            (Value::Double(a), Value::Integer(b)) => a
                .partial_cmp(&(*b as f64))
                .ok_or_else(|| PrismDBError::InvalidValue("Cannot compare NaN values".to_string())),
            (Value::Double(a), Value::BigInt(b)) => a
                .partial_cmp(&(*b as f64))
                .ok_or_else(|| PrismDBError::InvalidValue("Cannot compare NaN values".to_string())),

            // Integer vs Float
            (Value::TinyInt(a), Value::Float(b)) => (*a as f32)
                .partial_cmp(b)
                .ok_or_else(|| PrismDBError::InvalidValue("Cannot compare NaN values".to_string())),
            (Value::SmallInt(a), Value::Float(b)) => (*a as f32)
                .partial_cmp(b)
                .ok_or_else(|| PrismDBError::InvalidValue("Cannot compare NaN values".to_string())),
            (Value::Integer(a), Value::Float(b)) => (*a as f32)
                .partial_cmp(b)
                .ok_or_else(|| PrismDBError::InvalidValue("Cannot compare NaN values".to_string())),

            // Float vs Integer (reverse)
            (Value::Float(a), Value::TinyInt(b)) => a
                .partial_cmp(&(*b as f32))
                .ok_or_else(|| PrismDBError::InvalidValue("Cannot compare NaN values".to_string())),
            (Value::Float(a), Value::SmallInt(b)) => a
                .partial_cmp(&(*b as f32))
                .ok_or_else(|| PrismDBError::InvalidValue("Cannot compare NaN values".to_string())),
            (Value::Float(a), Value::Integer(b)) => a
                .partial_cmp(&(*b as f32))
                .ok_or_else(|| PrismDBError::InvalidValue("Cannot compare NaN values".to_string())),

            // Different integer widths - cast to wider type
            (Value::TinyInt(a), Value::SmallInt(b)) => Ok((*a as i16).cmp(b)),
            (Value::TinyInt(a), Value::Integer(b)) => Ok((*a as i32).cmp(b)),
            (Value::TinyInt(a), Value::BigInt(b)) => Ok((*a as i64).cmp(b)),
            (Value::SmallInt(a), Value::TinyInt(b)) => Ok(a.cmp(&(*b as i16))),
            (Value::SmallInt(a), Value::Integer(b)) => Ok((*a as i32).cmp(b)),
            (Value::SmallInt(a), Value::BigInt(b)) => Ok((*a as i64).cmp(b)),
            (Value::Integer(a), Value::TinyInt(b)) => Ok(a.cmp(&(*b as i32))),
            (Value::Integer(a), Value::SmallInt(b)) => Ok(a.cmp(&(*b as i32))),
            (Value::Integer(a), Value::BigInt(b)) => Ok((*a as i64).cmp(b)),
            (Value::BigInt(a), Value::TinyInt(b)) => Ok(a.cmp(&(*b as i64))),
            (Value::BigInt(a), Value::SmallInt(b)) => Ok(a.cmp(&(*b as i64))),
            (Value::BigInt(a), Value::Integer(b)) => Ok(a.cmp(&(*b as i64))),

            // DECIMAL vs DECIMAL - normalize to same scale
            (Value::Decimal { value: a, scale: scale_a, .. }, Value::Decimal { value: b, scale: scale_b, .. }) => {
                if scale_a == scale_b {
                    Ok(a.cmp(b))
                } else if scale_a < scale_b {
                    let multiplier = 10_i128.pow((scale_b - scale_a) as u32);
                    Ok((a * multiplier).cmp(b))
                } else {
                    let multiplier = 10_i128.pow((scale_a - scale_b) as u32);
                    Ok(a.cmp(&(b * multiplier)))
                }
            }

            // DECIMAL vs INTEGER types - convert integer to DECIMAL scale
            (Value::Decimal { value: a, scale, .. }, Value::TinyInt(b)) => {
                let b_scaled = (*b as i128) * 10_i128.pow(*scale as u32);
                Ok(a.cmp(&b_scaled))
            }
            (Value::Decimal { value: a, scale, .. }, Value::SmallInt(b)) => {
                let b_scaled = (*b as i128) * 10_i128.pow(*scale as u32);
                Ok(a.cmp(&b_scaled))
            }
            (Value::Decimal { value: a, scale, .. }, Value::Integer(b)) => {
                let b_scaled = (*b as i128) * 10_i128.pow(*scale as u32);
                Ok(a.cmp(&b_scaled))
            }
            (Value::Decimal { value: a, scale, .. }, Value::BigInt(b)) => {
                let b_scaled = (*b as i128) * 10_i128.pow(*scale as u32);
                Ok(a.cmp(&b_scaled))
            }

            // INTEGER types vs DECIMAL (reverse)
            (Value::TinyInt(a), Value::Decimal { value: b, scale, .. }) => {
                let a_scaled = (*a as i128) * 10_i128.pow(*scale as u32);
                Ok(a_scaled.cmp(b))
            }
            (Value::SmallInt(a), Value::Decimal { value: b, scale, .. }) => {
                let a_scaled = (*a as i128) * 10_i128.pow(*scale as u32);
                Ok(a_scaled.cmp(b))
            }
            (Value::Integer(a), Value::Decimal { value: b, scale, .. }) => {
                let a_scaled = (*a as i128) * 10_i128.pow(*scale as u32);
                Ok(a_scaled.cmp(b))
            }
            (Value::BigInt(a), Value::Decimal { value: b, scale, .. }) => {
                let a_scaled = (*a as i128) * 10_i128.pow(*scale as u32);
                Ok(a_scaled.cmp(b))
            }

            // DECIMAL vs DOUBLE/FLOAT - convert to f64
            (Value::Decimal { value: a, scale, .. }, Value::Double(b)) => {
                let a_as_f64 = (*a as f64) / 10_f64.powi(*scale as i32);
                a_as_f64
                    .partial_cmp(b)
                    .ok_or_else(|| PrismDBError::InvalidValue("Cannot compare NaN values".to_string()))
            }
            (Value::Decimal { value: a, scale, .. }, Value::Float(b)) => {
                let a_as_f32 = (*a as f32) / 10_f32.powi(*scale as i32);
                a_as_f32
                    .partial_cmp(b)
                    .ok_or_else(|| PrismDBError::InvalidValue("Cannot compare NaN values".to_string()))
            }

            // DOUBLE/FLOAT vs DECIMAL (reverse)
            (Value::Double(a), Value::Decimal { value: b, scale, .. }) => {
                let b_as_f64 = (*b as f64) / 10_f64.powi(*scale as i32);
                a
                    .partial_cmp(&b_as_f64)
                    .ok_or_else(|| PrismDBError::InvalidValue("Cannot compare NaN values".to_string()))
            }
            (Value::Float(a), Value::Decimal { value: b, scale, .. }) => {
                let b_as_f32 = (*b as f32) / 10_f32.powi(*scale as i32);
                a
                    .partial_cmp(&b_as_f32)
                    .ok_or_else(|| PrismDBError::InvalidValue("Cannot compare NaN values".to_string()))
            }

            _ => Err(PrismDBError::InvalidType(format!(
                "Cannot compare {} and {}",
                self.get_type(),
                other.get_type()
            ))),
        }
    }

    /// Get the size of this value in bytes (approximate)
    pub fn get_size(&self) -> usize {
        match self {
            Value::Null => 0,
            Value::Boolean(_) => 1,
            Value::TinyInt(_) => 1,
            Value::SmallInt(_) => 2,
            Value::Integer(_) => 4,
            Value::BigInt(_) => 8,
            Value::HugeInt { .. } => 16,
            Value::Float(_) => 4,
            Value::Double(_) => 8,
            Value::Varchar(s) => s.len(),
            Value::Char(s) => s.len(),
            Value::Decimal { .. } => 16,
            Value::Date(_) => 4,
            Value::Time(_) => 8,
            Value::Timestamp(_) => 8,
            Value::Interval { .. } => 16,
            Value::UUID { .. } => 16,
            Value::JSON(s) => s.len(),
            Value::Blob(data) => data.len(),
            Value::List(values) => values.iter().map(|v| v.get_size()).sum(),
            Value::Struct(fields) => fields.iter().map(|(_, v)| v.get_size()).sum(),
            Value::Map(pairs) => pairs.iter().map(|(k, v)| k.get_size() + v.get_size()).sum(),
            Value::Union { value, .. } => value.get_size(),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Boolean(value) => write!(f, "{}", value),
            Value::TinyInt(value) => write!(f, "{}", value),
            Value::SmallInt(value) => write!(f, "{}", value),
            Value::Integer(value) => write!(f, "{}", value),
            Value::BigInt(value) => write!(f, "{}", value),
            Value::HugeInt { high, low } => write!(f, "{}{}", high, low),
            Value::Float(value) => write!(f, "{}", value),
            Value::Double(value) => write!(f, "{}", value),
            Value::Varchar(value) => write!(f, "'{}'", value),
            Value::Char(value) => write!(f, "'{}'", value),
            Value::Decimal { value, scale, .. } => {
                let divisor = 10_i128.pow(*scale as u32);
                let integer_part = value / divisor;
                let fractional_part = (value % divisor).abs();
                write!(
                    f,
                    "{}.{:0width$}",
                    integer_part,
                    fractional_part,
                    width = *scale as usize
                )
            }
            Value::Date(value) => write!(f, "DATE({})", value),
            Value::Time(value) => write!(f, "TIME({})", value),
            Value::Timestamp(value) => write!(f, "TIMESTAMP({})", value),
            Value::Interval {
                months,
                days,
                micros,
            } => {
                write!(
                    f,
                    "INTERVAL {} months {} days {} micros",
                    months, days, micros
                )
            }
            Value::UUID { high, low } => write!(f, "UUID({:016x}{:016x})", high, low),
            Value::JSON(value) => write!(f, "{}", value),
            Value::Blob(data) => write!(f, "BLOB({:?})", data),
            Value::List(values) => {
                write!(f, "[")?;
                for (i, value) in values.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", value)?;
                }
                write!(f, "]")
            }
            Value::Struct(fields) => {
                write!(f, "{{")?;
                for (i, (name, value)) in fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}: {}", name, value)?;
                }
                write!(f, "}}")
            }
            Value::Map(pairs) => {
                write!(f, "{{")?;
                for (i, (key, value)) in pairs.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}: {}", key, value)?;
                }
                write!(f, "}}")
            }
            Value::Union { tag, value } => write!(f, "UNION[{}]: {}", tag, value),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_creation() {
        let bool_val = Value::boolean(true);
        assert_eq!(bool_val.try_as_boolean().unwrap(), true);

        let int_val = Value::integer(42);
        assert_eq!(int_val.try_as_i32().unwrap(), 42);

        let double_val = Value::double(3.14);
        assert!((double_val.try_as_f64().unwrap() - 3.14).abs() < f64::EPSILON);
    }

    #[test]
    fn test_value_casting() {
        let int_val = Value::integer(42);
        let double_val = int_val.cast_to(&LogicalType::Double).unwrap();
        assert_eq!(double_val.try_as_f64().unwrap(), 42.0);

        let string_val = Value::varchar("123".to_string());
        let int_from_str = string_val.cast_to(&LogicalType::Integer).unwrap();
        assert_eq!(int_from_str.try_as_i32().unwrap(), 123);
    }

    #[test]
    fn test_value_comparison() {
        let int1 = Value::integer(10);
        let int2 = Value::integer(20);
        assert_eq!(int1.compare(&int2).unwrap(), Ordering::Less);

        let str1 = Value::varchar("apple".to_string());
        let str2 = Value::varchar("banana".to_string());
        assert_eq!(str1.compare(&str2).unwrap(), Ordering::Less);
    }

    #[test]
    fn test_nested_values() {
        let list_val = Value::list(vec![
            Value::integer(1),
            Value::integer(2),
            Value::integer(3),
        ]);
        let extracted_list = list_val.try_as_list().unwrap();
        assert_eq!(extracted_list.len(), 3);
        assert_eq!(extracted_list[1].try_as_i32().unwrap(), 2);
    }

    #[test]
    fn test_null_values() {
        let null_val = Value::null(LogicalType::Integer);
        assert!(null_val.is_null());
        assert!(null_val.try_as_i32().is_err());
    }
}
