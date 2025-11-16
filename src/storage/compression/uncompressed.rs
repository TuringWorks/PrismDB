/// Uncompressed storage - passthrough "compression"
///
/// This is a fallback used when compression doesn't provide any benefit,
/// or when data is already highly entropic (e.g., encrypted data, random data).
///
/// Use cases:
/// - Random or encrypted data where compression would expand the data
/// - Small segments where compression overhead outweighs benefits
/// - Data types that don't compress well (e.g., already compressed images)
///
/// This provides a uniform interface while avoiding compression overhead.
use crate::storage::compression::traits::{
    CompressionError, CompressionFunction, CompressionResult,
};
use crate::storage::compression::types::{
    AnalyzeResult, CompressedSegment, CompressionMetadata, CompressionType, SelectionVector,
};
use crate::types::Value;

/// Uncompressed storage function
pub struct UncompressedStorage;

impl UncompressedStorage {
    /// Creates a new uncompressed storage instance
    pub fn new() -> Self {
        Self
    }

    /// Serializes values to bytes without compression
    fn serialize_values(data: &[Value]) -> CompressionResult<Vec<u8>> {
        // Simple serialization using bincode 2.0 serde API
        let config = bincode::config::standard();
        bincode::serde::encode_to_vec(data, config).map_err(|e| {
            CompressionError::CompressionFailed(format!("Serialization failed: {}", e))
        })
    }

    /// Deserializes values from bytes
    fn deserialize_values(bytes: &[u8]) -> CompressionResult<Vec<Value>> {
        let config = bincode::config::standard();
        bincode::serde::decode_from_slice(bytes, config)
            .map(|(values, _)| values)
            .map_err(|e| {
                CompressionError::DecompressionFailed(format!("Deserialization failed: {}", e))
            })
    }

    /// Estimates size of a value in bytes
    fn estimate_value_size(value: &Value) -> usize {
        match value {
            Value::Null => 1,
            Value::Boolean(_) => 2,
            Value::TinyInt(_) => 2,
            Value::SmallInt(_) => 3,
            Value::Integer(_) => 5,
            Value::BigInt(_) => 9,
            Value::HugeInt { .. } => 17,
            Value::Float(_) => 5,
            Value::Double(_) => 9,
            Value::Varchar(s) | Value::Char(s) => 5 + s.len(),
            Value::Date(_) => 5,
            Value::Time(_) => 9,
            Value::Timestamp(_) => 9,
            Value::Interval { .. } => 17,
            Value::Decimal { .. } => 18,
            _ => 32, // Conservative estimate
        }
    }
}

impl Default for UncompressedStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl CompressionFunction for UncompressedStorage {
    fn analyze(&self, data: &[Value]) -> CompressionResult<AnalyzeResult> {
        if data.is_empty() {
            return Ok(AnalyzeResult::new(CompressionType::Uncompressed, 0, 0));
        }

        // Estimate size
        let estimated_size: usize = data.iter().map(Self::estimate_value_size).sum();

        // Uncompressed has ~1:1 ratio (add minimal overhead for type markers)
        // Use a smaller overhead to keep ratio close to 1.0
        let serialized_size = estimated_size + data.len(); // 1 byte per value for type marker

        Ok(AnalyzeResult::new(
            CompressionType::Uncompressed,
            estimated_size,
            serialized_size,
        ))
    }

    fn compress(&self, data: &[Value]) -> CompressionResult<CompressedSegment> {
        if data.is_empty() {
            return Ok(CompressedSegment {
                compression_type: CompressionType::Uncompressed,
                data: Vec::new(),
                value_count: 0,
                null_bitmap: None,
                metadata: CompressionMetadata::Uncompressed,
            });
        }

        // Serialize without compression
        let serialized_data = Self::serialize_values(data)?;

        Ok(CompressedSegment {
            compression_type: CompressionType::Uncompressed,
            data: serialized_data,
            value_count: data.len(),
            null_bitmap: None,
            metadata: CompressionMetadata::Uncompressed,
        })
    }

    fn decompress(&self, segment: &CompressedSegment) -> CompressionResult<Vec<Value>> {
        if segment.value_count == 0 {
            return Ok(Vec::new());
        }

        // Deserialize
        let values = Self::deserialize_values(&segment.data)?;

        // Verify count
        if values.len() != segment.value_count {
            return Err(CompressionError::CorruptedData(format!(
                "Expected {} values, got {}",
                segment.value_count,
                values.len()
            )));
        }

        Ok(values)
    }

    fn scan(
        &self,
        segment: &CompressedSegment,
        selection: &SelectionVector,
    ) -> CompressionResult<Vec<Value>> {
        if segment.value_count == 0 || selection.is_empty() {
            return Ok(Vec::new());
        }

        // For uncompressed, we have to deserialize everything first
        let all_values = self.decompress(segment)?;

        // Extract selected values
        let mut values = Vec::with_capacity(selection.len());

        for &idx in &selection.indices {
            if idx >= all_values.len() {
                return Err(CompressionError::CorruptedData(
                    "Selection index out of bounds".to_string(),
                ));
            }
            values.push(all_values[idx].clone());
        }

        Ok(values)
    }

    fn name(&self) -> &'static str {
        "Uncompressed"
    }

    fn supports_type(&self, _value: &Value) -> bool {
        // Uncompressed supports all types
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_uncompressed_integers() {
        let storage = UncompressedStorage::new();

        let data = vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
            Value::Integer(4),
            Value::Integer(5),
        ];

        // Compress (actually just serialize)
        let segment = storage.compress(&data).unwrap();
        assert_eq!(segment.value_count, 5);
        assert_eq!(segment.compression_type, CompressionType::Uncompressed);

        // Decompress
        let decompressed = storage.decompress(&segment).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_uncompressed_strings() {
        let storage = UncompressedStorage::new();

        let data = vec![
            Value::Varchar("hello".to_string()),
            Value::Varchar("world".to_string()),
            Value::Varchar("test".to_string()),
        ];

        let segment = storage.compress(&data).unwrap();
        let decompressed = storage.decompress(&segment).unwrap();

        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_uncompressed_mixed_types() {
        let storage = UncompressedStorage::new();

        let data = vec![
            Value::Integer(42),
            Value::Varchar("test".to_string()),
            Value::Double(3.14),
            Value::Boolean(true),
            Value::Null,
        ];

        let segment = storage.compress(&data).unwrap();
        let decompressed = storage.decompress(&segment).unwrap();

        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_uncompressed_scan_selection() {
        let storage = UncompressedStorage::new();

        let data = vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
            Value::Integer(4),
            Value::Integer(5),
        ];

        let segment = storage.compress(&data).unwrap();

        // Scan with selection
        let selection = SelectionVector::new(vec![0, 2, 4]);
        let scanned = storage.scan(&segment, &selection).unwrap();

        assert_eq!(scanned.len(), 3);
        assert_eq!(scanned[0], Value::Integer(1));
        assert_eq!(scanned[1], Value::Integer(3));
        assert_eq!(scanned[2], Value::Integer(5));
    }

    #[test]
    fn test_uncompressed_analyze() {
        let storage = UncompressedStorage::new();

        let data = vec![Value::Integer(1); 100];

        let result = storage.analyze(&data).unwrap();
        assert_eq!(result.compression_type, CompressionType::Uncompressed);

        // Compression ratio should be close to 1.0 (may be slightly worse)
        assert!(result.compression_ratio >= 0.8);
        assert!(result.compression_ratio <= 1.2);
    }

    #[test]
    fn test_uncompressed_empty() {
        let storage = UncompressedStorage::new();

        let data: Vec<Value> = Vec::new();
        let segment = storage.compress(&data).unwrap();
        let decompressed = storage.decompress(&segment).unwrap();

        assert_eq!(decompressed.len(), 0);
    }
}
