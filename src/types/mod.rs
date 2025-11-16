//! Type system module for DuckDB
//!
//! This module contains the core type system components:
//! - PhysicalType: Low-level storage representations
//! - LogicalType: SQL-level type abstractions  
//! - Value: Single value containers with type information
//! - Vector: Columnar data containers with validity masks
//! - DataChunk: Collections of vectors for batch processing

pub mod data_chunk;
pub mod logical_type;
pub mod physical_type;
pub mod value;
pub mod vector;

// Re-export main types for convenience
pub use data_chunk::{ColumnIterator, DataChunk, RowIterator};
pub use logical_type::{LogicalType, TypeUtils};
pub use physical_type::PhysicalType;
pub use value::Value;
pub use vector::{SelectionVector, ValidityMask, Vector};

/// Type system utilities and constants
pub mod utils {
    use super::*;

    /// Default chunk size for batch processing
    pub const DEFAULT_CHUNK_SIZE: usize = 1024;

    /// Maximum chunk size to prevent memory issues
    pub const MAX_CHUNK_SIZE: usize = 100_000;

    /// Get the default chunk size based on type
    pub fn get_default_chunk_size(logical_type: &LogicalType) -> usize {
        match logical_type {
            LogicalType::Struct(_)
            | LogicalType::List(_)
            | LogicalType::Map { .. }
            | LogicalType::Union(_) => {
                // Smaller chunks for complex types
                DEFAULT_CHUNK_SIZE / 4
            }
            LogicalType::Varchar | LogicalType::JSON | LogicalType::Blob => {
                // Medium chunks for variable-size types
                DEFAULT_CHUNK_SIZE / 2
            }
            _ => DEFAULT_CHUNK_SIZE,
        }
    }

    /// Estimate memory usage for a type
    pub fn estimate_type_size(logical_type: &LogicalType) -> usize {
        match logical_type {
            LogicalType::Boolean => 1,
            LogicalType::TinyInt => 1,
            LogicalType::SmallInt => 2,
            LogicalType::Integer | LogicalType::Date => 4,
            LogicalType::Float => 4,
            LogicalType::BigInt | LogicalType::Time | LogicalType::Timestamp => 8,
            LogicalType::Double => 8,
            LogicalType::HugeInt | LogicalType::UUID => 16,
            LogicalType::Decimal { .. } => 16,
            LogicalType::Varchar
            | LogicalType::Char { .. }
            | LogicalType::Text
            | LogicalType::JSON
            | LogicalType::Blob => {
                // Variable size - use average estimate
                32
            }
            LogicalType::List(_) => {
                // Pointer + overhead
                16
            }
            LogicalType::Struct(fields) => {
                // Sum of field sizes
                fields.iter().map(|(_, t)| estimate_type_size(t)).sum()
            }
            LogicalType::Map {
                key_type,
                value_type,
            } => estimate_type_size(key_type) + estimate_type_size(value_type) + 16,
            LogicalType::Union(types) => {
                // Tag + largest variant
                1 + types.iter().map(estimate_type_size).max().unwrap_or(0)
            }
            LogicalType::Enum { .. } => 4,
            LogicalType::Interval => 16,
            LogicalType::Null => 0, // NULL takes no space
            LogicalType::Invalid => 0,
        }
    }

    /// Check if a type is suitable for vectorized operations
    pub fn is_vectorizable(logical_type: &LogicalType) -> bool {
        !matches!(logical_type, LogicalType::Struct(_) | LogicalType::Union(_))
    }

    /// Get the alignment requirement for a type
    pub fn get_type_alignment(logical_type: &LogicalType) -> usize {
        logical_type.get_physical_type().get_alignment()
    }
}

#[cfg(test)]
mod tests {
    use super::utils::*;
    use super::*;

    #[test]
    fn test_type_utilities() {
        assert_eq!(
            get_default_chunk_size(&LogicalType::Integer),
            DEFAULT_CHUNK_SIZE
        );
        assert_eq!(
            get_default_chunk_size(&LogicalType::Struct(vec![])),
            DEFAULT_CHUNK_SIZE / 4
        );

        assert_eq!(estimate_type_size(&LogicalType::Integer), 4);
        assert_eq!(estimate_type_size(&LogicalType::Boolean), 1);
        assert_eq!(estimate_type_size(&LogicalType::Double), 8);

        assert!(is_vectorizable(&LogicalType::Integer));
        assert!(!is_vectorizable(&LogicalType::Struct(vec![])));
    }

    #[test]
    fn test_type_alignment() {
        assert_eq!(get_type_alignment(&LogicalType::Boolean), 1);
        assert_eq!(get_type_alignment(&LogicalType::Integer), 4);
        assert_eq!(get_type_alignment(&LogicalType::BigInt), 8);
        assert_eq!(get_type_alignment(&LogicalType::HugeInt), 16);
    }
}
