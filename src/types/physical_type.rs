use serde::{Deserialize, Serialize};
use std::fmt;

/// Physical types represent how data is stored internally in DuckDB
/// These are the low-level representations used for memory layout and storage
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PhysicalType {
    /// 8-bit signed integer
    Int8,
    /// 16-bit signed integer
    Int16,
    /// 32-bit signed integer
    Int32,
    /// 64-bit signed integer
    Int64,
    /// 128-bit signed integer
    Int128,
    /// 32-bit floating point
    Float,
    /// 64-bit double precision
    Double,
    /// Variable length string
    Varchar,
    /// Boolean value
    Bool,
    /// Fixed length binary data
    FixedSizeBinary { width: usize },
    /// Nested list/array type
    List,
    /// Struct type with named fields
    Struct,
    /// Map type (key-value pairs)
    Map,
    /// Union type
    Union,
    /// Decimal type with precision and scale
    Decimal { precision: u8, scale: u8 },
    /// Date value (days since epoch)
    Date,
    /// Time value (microseconds since midnight)
    Time,
    /// Timestamp value (microseconds since epoch)
    Timestamp,
    /// Interval type
    Interval,
    /// UUID type
    UUID,
    /// JSON type
    JSON,
    /// Blob type
    Blob,
    /// Enum type
    Enum,
    /// Invalid/unknown type
    Invalid,
}

impl PhysicalType {
    /// Get the size of this physical type in bytes (for fixed-size types)
    pub fn get_size(&self) -> Option<usize> {
        match self {
            PhysicalType::Int8 => Some(1),
            PhysicalType::Int16 => Some(2),
            PhysicalType::Int32 => Some(4),
            PhysicalType::Int64 => Some(8),
            PhysicalType::Int128 => Some(16),
            PhysicalType::Float => Some(4),
            PhysicalType::Double => Some(8),
            PhysicalType::Bool => Some(1),
            PhysicalType::Decimal { .. } => Some(16), // DECIMAL stored as i128
            PhysicalType::FixedSizeBinary { width } => Some(*width),
            PhysicalType::Date => Some(4),
            PhysicalType::Time => Some(8),
            PhysicalType::Timestamp => Some(8),
            PhysicalType::UUID => Some(16),
            _ => None, // Variable size types
        }
    }

    /// Check if this type is numeric
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            PhysicalType::Int8
                | PhysicalType::Int16
                | PhysicalType::Int32
                | PhysicalType::Int64
                | PhysicalType::Int128
                | PhysicalType::Float
                | PhysicalType::Double
                | PhysicalType::Decimal { .. }
        )
    }

    /// Check if this type is variable length
    pub fn is_variable_size(&self) -> bool {
        matches!(
            self,
            PhysicalType::Varchar
                | PhysicalType::List
                | PhysicalType::Struct
                | PhysicalType::Map
                | PhysicalType::Union
                | PhysicalType::JSON
                | PhysicalType::Blob
                | PhysicalType::Enum
        )
    }

    /// Get the alignment requirement for this type
    pub fn get_alignment(&self) -> usize {
        match self {
            PhysicalType::Int8 | PhysicalType::Bool => 1,
            PhysicalType::Int16 => 2,
            PhysicalType::Int32 | PhysicalType::Float | PhysicalType::Date => 4,
            PhysicalType::Int64
            | PhysicalType::Double
            | PhysicalType::Time
            | PhysicalType::Timestamp => 8,
            PhysicalType::Int128 | PhysicalType::UUID => 16,
            PhysicalType::FixedSizeBinary { width } => *width.min(&8),
            _ => 8, // Default alignment for complex types
        }
    }
}

impl fmt::Display for PhysicalType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PhysicalType::Int8 => write!(f, "INT8"),
            PhysicalType::Int16 => write!(f, "INT16"),
            PhysicalType::Int32 => write!(f, "INT32"),
            PhysicalType::Int64 => write!(f, "INT64"),
            PhysicalType::Int128 => write!(f, "INT128"),
            PhysicalType::Float => write!(f, "FLOAT"),
            PhysicalType::Double => write!(f, "DOUBLE"),
            PhysicalType::Varchar => write!(f, "VARCHAR"),
            PhysicalType::Bool => write!(f, "BOOL"),
            PhysicalType::FixedSizeBinary { width } => write!(f, "FIXED_SIZE_BINARY[{}]", width),
            PhysicalType::List => write!(f, "LIST"),
            PhysicalType::Struct => write!(f, "STRUCT"),
            PhysicalType::Map => write!(f, "MAP"),
            PhysicalType::Union => write!(f, "UNION"),
            PhysicalType::Decimal { precision, scale } => {
                write!(f, "DECIMAL({},{})", precision, scale)
            }
            PhysicalType::Date => write!(f, "DATE"),
            PhysicalType::Time => write!(f, "TIME"),
            PhysicalType::Timestamp => write!(f, "TIMESTAMP"),
            PhysicalType::Interval => write!(f, "INTERVAL"),
            PhysicalType::UUID => write!(f, "UUID"),
            PhysicalType::JSON => write!(f, "JSON"),
            PhysicalType::Blob => write!(f, "BLOB"),
            PhysicalType::Enum => write!(f, "ENUM"),
            PhysicalType::Invalid => write!(f, "INVALID"),
        }
    }
}

/// Convert logical type to physical type
pub fn logical_to_physical_type(
    logical_type: &crate::types::logical_type::LogicalType,
) -> PhysicalType {
    use crate::types::logical_type::LogicalType;

    match logical_type {
        LogicalType::Boolean => PhysicalType::Bool,
        LogicalType::TinyInt => PhysicalType::Int8,
        LogicalType::SmallInt => PhysicalType::Int16,
        LogicalType::Integer => PhysicalType::Int32,
        LogicalType::BigInt => PhysicalType::Int64,
        LogicalType::HugeInt => PhysicalType::Int128,
        LogicalType::Float => PhysicalType::Float,
        LogicalType::Double => PhysicalType::Double,
        LogicalType::Varchar => PhysicalType::Varchar,
        LogicalType::Char { .. } => PhysicalType::Varchar,
        LogicalType::Text => PhysicalType::Varchar,
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
        LogicalType::Map {
            key_type: _,
            value_type: _,
        } => PhysicalType::Map,
        LogicalType::Union(_) => PhysicalType::Union,
        LogicalType::Enum { name: _, values: _ } => PhysicalType::Enum,
        LogicalType::Null => PhysicalType::Invalid, // NULL uses Invalid as physical type
        LogicalType::Invalid => PhysicalType::Invalid,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_physical_type_sizes() {
        assert_eq!(PhysicalType::Int8.get_size(), Some(1));
        assert_eq!(PhysicalType::Int32.get_size(), Some(4));
        assert_eq!(PhysicalType::Double.get_size(), Some(8));
        assert_eq!(PhysicalType::Varchar.get_size(), None);
    }

    #[test]
    fn test_numeric_types() {
        assert!(PhysicalType::Int32.is_numeric());
        assert!(PhysicalType::Float.is_numeric());
        assert!(!PhysicalType::Varchar.is_numeric());
        assert!(!PhysicalType::Bool.is_numeric());
    }

    #[test]
    fn test_variable_size_types() {
        assert!(PhysicalType::Varchar.is_variable_size());
        assert!(PhysicalType::List.is_variable_size());
        assert!(!PhysicalType::Int32.is_variable_size());
        assert!(!PhysicalType::Double.is_variable_size());
    }

    #[test]
    fn test_alignment() {
        assert_eq!(PhysicalType::Int8.get_alignment(), 1);
        assert_eq!(PhysicalType::Int16.get_alignment(), 2);
        assert_eq!(PhysicalType::Int32.get_alignment(), 4);
        assert_eq!(PhysicalType::Int64.get_alignment(), 8);
        assert_eq!(PhysicalType::Int128.get_alignment(), 16);
    }
}
