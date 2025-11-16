use crate::common::error::{PrismDBError, PrismDBResult};
use crate::types::physical_type::PhysicalType;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Logical types represent the SQL-level types that users interact with
/// These are mapped to physical types for storage and computation
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum LogicalType {
    /// NULL type
    Null,
    /// Boolean type (TRUE/FALSE)
    Boolean,
    /// 8-bit signed integer
    TinyInt,
    /// 16-bit signed integer
    SmallInt,
    /// 32-bit signed integer
    Integer,
    /// 64-bit signed integer
    BigInt,
    /// 128-bit signed integer
    HugeInt,
    /// 32-bit floating point
    Float,
    /// 64-bit double precision
    Double,
    /// Variable length string
    Varchar,
    /// Fixed length character string
    Char { length: usize },
    /// Text string (alias for VARCHAR)
    Text,
    /// Decimal with precision and scale
    Decimal { precision: u8, scale: u8 },
    /// Date value (days since 1970-01-01)
    Date,
    /// Time value (microseconds since midnight)
    Time,
    /// Timestamp value (microseconds since 1970-01-01 00:00:00 UTC)
    Timestamp,
    /// Interval type
    Interval,
    /// UUID type
    UUID,
    /// JSON type
    JSON,
    /// Binary large object
    Blob,
    /// List/array type with element type
    List(Box<LogicalType>),
    /// Struct type with named fields
    Struct(Vec<(String, LogicalType)>),
    /// Map type with key and value types
    Map {
        key_type: Box<LogicalType>,
        value_type: Box<LogicalType>,
    },
    /// Union type with member types
    Union(Vec<LogicalType>),
    /// Enum type with possible values
    Enum { name: String, values: Vec<String> },
    /// Invalid/unknown type
    Invalid,
}

impl LogicalType {
    /// Get the corresponding physical type for this logical type
    pub fn get_physical_type(&self) -> PhysicalType {
        match self {
            LogicalType::Boolean => PhysicalType::Bool,
            LogicalType::TinyInt => PhysicalType::Int8,
            LogicalType::SmallInt => PhysicalType::Int16,
            LogicalType::Integer => PhysicalType::Int32,
            LogicalType::BigInt => PhysicalType::Int64,
            LogicalType::HugeInt => PhysicalType::Int128,
            LogicalType::Float => PhysicalType::Float,
            LogicalType::Double => PhysicalType::Double,
            LogicalType::Varchar | LogicalType::Char { .. } | LogicalType::Text => {
                PhysicalType::Varchar
            }
            LogicalType::Decimal { precision, scale } => PhysicalType::Decimal {
                precision: *precision,
                scale: *scale,
            },
            LogicalType::Date => PhysicalType::Date,
            LogicalType::Time => PhysicalType::Time,
            LogicalType::Timestamp => PhysicalType::Timestamp,
            LogicalType::Interval => PhysicalType::Interval,
            LogicalType::UUID => PhysicalType::UUID,
            LogicalType::JSON => PhysicalType::JSON,
            LogicalType::Blob => PhysicalType::Blob,
            LogicalType::List(_) => PhysicalType::List,
            LogicalType::Struct(_) => PhysicalType::Struct,
            LogicalType::Map { .. } => PhysicalType::Map,
            LogicalType::Union(_) => PhysicalType::Union,
            LogicalType::Enum { .. } => PhysicalType::Varchar, // Enums stored as strings
            LogicalType::Null => PhysicalType::Invalid,        // NULL uses Invalid as physical type
            LogicalType::Invalid => PhysicalType::Invalid,
        }
    }

    /// Check if this type is numeric
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            LogicalType::TinyInt
                | LogicalType::SmallInt
                | LogicalType::Integer
                | LogicalType::BigInt
                | LogicalType::HugeInt
                | LogicalType::Float
                | LogicalType::Double
                | LogicalType::Decimal { .. }
        )
    }

    /// Check if this type is integral (no fractional part)
    pub fn is_integral(&self) -> bool {
        matches!(
            self,
            LogicalType::TinyInt
                | LogicalType::SmallInt
                | LogicalType::Integer
                | LogicalType::BigInt
                | LogicalType::HugeInt
        )
    }

    /// Check if this type is floating point
    pub fn is_floating_point(&self) -> bool {
        matches!(self, LogicalType::Float | LogicalType::Double)
    }

    /// Check if this type is a string type
    pub fn is_string(&self) -> bool {
        matches!(self, LogicalType::Varchar | LogicalType::Char { .. })
    }

    /// Check if this type is temporal (date/time related)
    pub fn is_temporal(&self) -> bool {
        matches!(
            self,
            LogicalType::Date | LogicalType::Time | LogicalType::Timestamp | LogicalType::Interval
        )
    }

    /// Check if this type is a nested type (contains other types)
    pub fn is_nested(&self) -> bool {
        matches!(
            self,
            LogicalType::List(_)
                | LogicalType::Struct(_)
                | LogicalType::Map { .. }
                | LogicalType::Union(_)
        )
    }

    /// Get the maximum size for this type (for fixed-size types)
    pub fn get_max_size(&self) -> Option<usize> {
        match self {
            LogicalType::Boolean => Some(1),
            LogicalType::TinyInt => Some(1),
            LogicalType::SmallInt => Some(2),
            LogicalType::Integer => Some(4),
            LogicalType::BigInt => Some(8),
            LogicalType::HugeInt => Some(16),
            LogicalType::Float => Some(4),
            LogicalType::Double => Some(8),
            LogicalType::Char { length } => Some(*length),
            LogicalType::Date => Some(4),
            LogicalType::Time => Some(8),
            LogicalType::Timestamp => Some(8),
            LogicalType::UUID => Some(16),
            _ => None, // Variable size types
        }
    }

    /// Validate if a decimal precision and scale are valid
    pub fn validate_decimal(precision: u8, scale: u8) -> PrismDBResult<()> {
        if precision == 0 || precision > 38 {
            return Err(PrismDBError::InvalidType(format!(
                "Decimal precision must be between 1 and 38, got {}",
                precision
            )));
        }
        if scale > precision {
            return Err(PrismDBError::InvalidType(format!(
                "Decimal scale ({}) cannot be greater than precision ({})",
                scale, precision
            )));
        }
        Ok(())
    }

    /// Create a decimal type with validation
    pub fn decimal(precision: u8, scale: u8) -> PrismDBResult<LogicalType> {
        Self::validate_decimal(precision, scale)?;
        Ok(LogicalType::Decimal { precision, scale })
    }

    /// Get the default string collation
    pub fn default_collation() -> &'static str {
        "en_US"
    }

    /// Check if this type can be implicitly cast to another type
    pub fn can_implicitly_cast_to(&self, target: &LogicalType) -> bool {
        use LogicalType::*;

        match (self, target) {
            // Same types
            (a, b) if a == b => true,

            // Numeric promotions
            (TinyInt, SmallInt | Integer | BigInt | HugeInt | Float | Double | Decimal { .. }) => {
                true
            }
            (SmallInt, Integer | BigInt | HugeInt | Float | Double | Decimal { .. }) => true,
            (Integer, BigInt | HugeInt | Float | Double | Decimal { .. }) => true,
            (BigInt, HugeInt | Float | Double | Decimal { .. }) => true,
            (HugeInt, Float | Double | Decimal { .. }) => true,
            (Float, Double) => true,

            // String conversions
            (Varchar, Char { .. }) => true,
            (Char { .. }, Varchar) => true,

            // Date/Time to timestamp
            (Date, Timestamp) => true,
            (Time, Timestamp) => true,

            // Any type to string (with some restrictions)
            (_, Varchar) => !matches!(target, List(_) | Struct(_) | Map { .. } | Union(_)),

            _ => false,
        }
    }
}

impl fmt::Display for LogicalType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogicalType::Boolean => write!(f, "BOOLEAN"),
            LogicalType::TinyInt => write!(f, "TINYINT"),
            LogicalType::SmallInt => write!(f, "SMALLINT"),
            LogicalType::Integer => write!(f, "INTEGER"),
            LogicalType::BigInt => write!(f, "BIGINT"),
            LogicalType::HugeInt => write!(f, "HUGEINT"),
            LogicalType::Float => write!(f, "FLOAT"),
            LogicalType::Double => write!(f, "DOUBLE"),
            LogicalType::Varchar | LogicalType::Text => write!(f, "VARCHAR"),
            LogicalType::Char { length } => write!(f, "CHAR({})", length),
            LogicalType::Decimal { precision, scale } => {
                write!(f, "DECIMAL({},{})", precision, scale)
            }
            LogicalType::Date => write!(f, "DATE"),
            LogicalType::Time => write!(f, "TIME"),
            LogicalType::Timestamp => write!(f, "TIMESTAMP"),
            LogicalType::Interval => write!(f, "INTERVAL"),
            LogicalType::UUID => write!(f, "UUID"),
            LogicalType::JSON => write!(f, "JSON"),
            LogicalType::Blob => write!(f, "BLOB"),
            LogicalType::List(element_type) => write!(f, "{}[]", element_type),
            LogicalType::Struct(fields) => {
                write!(f, "STRUCT(")?;
                for (i, (name, field_type)) in fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{} {}", name, field_type)?;
                }
                write!(f, ")")
            }
            LogicalType::Map {
                key_type,
                value_type,
            } => {
                write!(f, "MAP({}, {})", key_type, value_type)
            }
            LogicalType::Union(types) => {
                write!(f, "UNION(")?;
                for (i, union_type) in types.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", union_type)?;
                }
                write!(f, ")")
            }
            LogicalType::Enum { name, .. } => write!(f, "ENUM({})", name),
            LogicalType::Null => write!(f, "NULL"),
            LogicalType::Invalid => write!(f, "INVALID"),
        }
    }
}

/// Type alias for common use
pub type TypeId = LogicalType;

/// Type system utilities
pub struct TypeUtils;

impl TypeUtils {
    /// Get the smallest type that can hold both types
    pub fn get_max_type(type1: &LogicalType, type2: &LogicalType) -> PrismDBResult<LogicalType> {
        use LogicalType::*;

        if type1 == type2 {
            return Ok(type1.clone());
        }

        match (type1, type2) {
            // Numeric type promotion
            (TinyInt, SmallInt | Integer | BigInt | HugeInt | Float | Double | Decimal { .. }) => {
                Ok(type2.clone())
            }
            (SmallInt, TinyInt | Integer | BigInt | HugeInt | Float | Double | Decimal { .. }) => {
                Ok(type2.clone())
            }
            (Integer, TinyInt | SmallInt | BigInt | HugeInt | Float | Double | Decimal { .. }) => {
                Ok(type2.clone())
            }
            (BigInt, TinyInt | SmallInt | Integer | HugeInt | Float | Double | Decimal { .. }) => {
                Ok(type2.clone())
            }
            (HugeInt, TinyInt | SmallInt | Integer | BigInt | Float | Double | Decimal { .. }) => {
                Ok(type2.clone())
            }
            (Float, Double) => Ok(Double),
            (Double, Float) => Ok(Double),

            // String types
            (Varchar, Char { .. }) => Ok(Varchar),
            (Char { .. }, Varchar) => Ok(Varchar),

            // Date/Time to timestamp
            (Date, Time) => Ok(Timestamp),
            (Time, Date) => Ok(Timestamp),

            _ => Err(PrismDBError::InvalidType(format!(
                "Cannot find common type between {} and {}",
                type1, type2
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_logical_type_creation() {
        let int_type = LogicalType::Integer;
        assert!(int_type.is_numeric());
        assert!(int_type.is_integral());
        assert!(!int_type.is_floating_point());

        let float_type = LogicalType::Float;
        assert!(float_type.is_numeric());
        assert!(!float_type.is_integral());
        assert!(float_type.is_floating_point());
    }

    #[test]
    fn test_decimal_validation() {
        assert!(LogicalType::decimal(10, 2).is_ok());
        assert!(LogicalType::decimal(0, 0).is_err());
        assert!(LogicalType::decimal(39, 10).is_err());
        assert!(LogicalType::decimal(10, 11).is_err());
    }

    #[test]
    fn test_implicit_casting() {
        let int_type = LogicalType::Integer;
        let double_type = LogicalType::Double;
        let varchar_type = LogicalType::Varchar;

        assert!(int_type.can_implicitly_cast_to(&double_type));
        assert!(!double_type.can_implicitly_cast_to(&int_type));
        assert!(int_type.can_implicitly_cast_to(&varchar_type));
    }

    #[test]
    fn test_type_promotion() {
        let tinyint_type = LogicalType::TinyInt;
        let bigint_type = LogicalType::BigInt;

        let max_type = TypeUtils::get_max_type(&tinyint_type, &bigint_type).unwrap();
        assert_eq!(max_type, LogicalType::BigInt);
    }

    #[test]
    fn test_nested_types() {
        let list_type = LogicalType::List(Box::new(LogicalType::Integer));
        assert!(list_type.is_nested());

        let struct_type = LogicalType::Struct(vec![
            ("id".to_string(), LogicalType::Integer),
            ("name".to_string(), LogicalType::Varchar),
        ]);
        assert!(struct_type.is_nested());

        let simple_type = LogicalType::Integer;
        assert!(!simple_type.is_nested());
    }
}
