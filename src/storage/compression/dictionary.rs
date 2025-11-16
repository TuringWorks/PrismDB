/// Dictionary compression implementation
///
/// Dictionary encoding maps values to integer indices, storing the unique values
/// in a dictionary and replacing each occurrence with its index.
///
/// Best for:
/// - Low-cardinality strings (e.g., country codes, status values)
/// - Categorical data (e.g., product categories)
/// - Enum-like columns
///
/// Compression ratio:
/// - Low cardinality (< 256 unique): 10-50x
/// - Medium cardinality (256-65536): 2-10x
/// - High cardinality (> 65536): 1-2x or may expand
use crate::storage::compression::traits::{
    CompressionError, CompressionFunction, CompressionResult,
};
use crate::storage::compression::types::{
    AnalyzeResult, CompressedSegment, CompressionMetadata, CompressionType, SelectionVector,
};
use crate::types::{LogicalType, Value};
use std::collections::HashMap;

/// Dictionary compression function
pub struct DictionaryCompression;

impl DictionaryCompression {
    /// Creates a new dictionary compression instance
    pub fn new() -> Self {
        Self
    }

    /// Determines optimal index width based on dictionary size
    fn select_index_width(dict_size: usize) -> u8 {
        if dict_size < 256 {
            1 // u8 (0-255)
        } else if dict_size < 65536 {
            2 // u16 (256-65535)
        } else {
            4 // u32 (65536+)
        }
    }

    /// Serializes dictionary values to bytes
    fn serialize_dictionary(dict: &[Value]) -> CompressionResult<Vec<u8>> {
        let mut bytes = Vec::new();

        // Write dictionary size
        bytes.extend_from_slice(&(dict.len() as u32).to_le_bytes());

        // Write each value
        for value in dict {
            match value {
                Value::Varchar(s) | Value::Char(s) => {
                    // Write length + string bytes
                    bytes.extend_from_slice(&(s.len() as u32).to_le_bytes());
                    bytes.extend_from_slice(s.as_bytes());
                }
                Value::TinyInt(i) => {
                    bytes.push(6); // Type marker
                    bytes.push(*i as u8);
                }
                Value::SmallInt(i) => {
                    bytes.push(7); // Type marker
                    bytes.extend_from_slice(&i.to_le_bytes());
                }
                Value::Integer(i) => {
                    bytes.push(0); // Type marker for Integer
                    bytes.extend_from_slice(&i.to_le_bytes());
                }
                Value::BigInt(i) => {
                    bytes.push(1); // Type marker for BigInt
                    bytes.extend_from_slice(&i.to_le_bytes());
                }
                Value::Float(f) => {
                    bytes.push(8); // Type marker for Float
                    bytes.extend_from_slice(&f.to_le_bytes());
                }
                Value::Double(d) => {
                    bytes.push(2); // Type marker for Double
                    bytes.extend_from_slice(&d.to_le_bytes());
                }
                Value::Boolean(b) => {
                    bytes.push(3); // Type marker for Boolean
                    bytes.push(if *b { 1 } else { 0 });
                }
                Value::Date(d) => {
                    bytes.push(4); // Type marker for Date
                    bytes.extend_from_slice(&d.to_le_bytes());
                }
                Value::Time(t) => {
                    bytes.push(9); // Type marker for Time
                    bytes.extend_from_slice(&t.to_le_bytes());
                }
                Value::Timestamp(ts) => {
                    bytes.push(5); // Type marker for Timestamp
                    bytes.extend_from_slice(&ts.to_le_bytes());
                }
                Value::Null => {
                    // Nulls are handled by null bitmap, shouldn't be in dictionary
                    return Err(CompressionError::InvalidMetadata(
                        "Null values should not be in dictionary".to_string(),
                    ));
                }
                // Unsupported types for now
                _ => {
                    return Err(CompressionError::Incompatible(format!(
                        "Unsupported value type for dictionary compression: {:?}",
                        value
                    )));
                }
            }
        }

        Ok(bytes)
    }

    /// Deserializes dictionary values from bytes
    fn deserialize_dictionary(
        bytes: &[u8],
        logical_type: &LogicalType,
    ) -> CompressionResult<Vec<Value>> {
        let mut offset = 0;
        let mut dict = Vec::new();

        // Read dictionary size
        if bytes.len() < 4 {
            return Err(CompressionError::CorruptedData(
                "Dictionary data too short".to_string(),
            ));
        }
        let dict_size = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
        offset += 4;

        // Read each value
        for _ in 0..dict_size {
            match logical_type {
                LogicalType::Varchar => {
                    if offset + 4 > bytes.len() {
                        return Err(CompressionError::CorruptedData(
                            "Incomplete string length".to_string(),
                        ));
                    }
                    let len = u32::from_le_bytes([
                        bytes[offset],
                        bytes[offset + 1],
                        bytes[offset + 2],
                        bytes[offset + 3],
                    ]) as usize;
                    offset += 4;

                    if offset + len > bytes.len() {
                        return Err(CompressionError::CorruptedData(
                            "Incomplete string data".to_string(),
                        ));
                    }
                    let s = String::from_utf8(bytes[offset..offset + len].to_vec())
                        .map_err(|e| CompressionError::CorruptedData(format!("Invalid UTF-8: {}", e)))?;
                    dict.push(Value::Varchar(s));
                    offset += len;
                }
                LogicalType::Integer => {
                    if offset + 5 > bytes.len() {
                        return Err(CompressionError::CorruptedData(
                            "Incomplete integer".to_string(),
                        ));
                    }
                    offset += 1; // Skip type marker
                    let i = i32::from_le_bytes([
                        bytes[offset],
                        bytes[offset + 1],
                        bytes[offset + 2],
                        bytes[offset + 3],
                    ]);
                    dict.push(Value::Integer(i));
                    offset += 4;
                }
                LogicalType::BigInt => {
                    if offset + 9 > bytes.len() {
                        return Err(CompressionError::CorruptedData(
                            "Incomplete bigint".to_string(),
                        ));
                    }
                    offset += 1; // Skip type marker
                    let i = i64::from_le_bytes([
                        bytes[offset],
                        bytes[offset + 1],
                        bytes[offset + 2],
                        bytes[offset + 3],
                        bytes[offset + 4],
                        bytes[offset + 5],
                        bytes[offset + 6],
                        bytes[offset + 7],
                    ]);
                    dict.push(Value::BigInt(i));
                    offset += 8;
                }
                LogicalType::Double => {
                    if offset + 9 > bytes.len() {
                        return Err(CompressionError::CorruptedData(
                            "Incomplete double".to_string(),
                        ));
                    }
                    offset += 1; // Skip type marker
                    let d = f64::from_le_bytes([
                        bytes[offset],
                        bytes[offset + 1],
                        bytes[offset + 2],
                        bytes[offset + 3],
                        bytes[offset + 4],
                        bytes[offset + 5],
                        bytes[offset + 6],
                        bytes[offset + 7],
                    ]);
                    dict.push(Value::Double(d));
                    offset += 8;
                }
                LogicalType::Boolean => {
                    if offset + 2 > bytes.len() {
                        return Err(CompressionError::CorruptedData(
                            "Incomplete boolean".to_string(),
                        ));
                    }
                    offset += 1; // Skip type marker
                    let b = bytes[offset] != 0;
                    dict.push(Value::Boolean(b));
                    offset += 1;
                }
                LogicalType::Date => {
                    if offset + 5 > bytes.len() {
                        return Err(CompressionError::CorruptedData(
                            "Incomplete date".to_string(),
                        ));
                    }
                    offset += 1; // Skip type marker
                    let d = i32::from_le_bytes([
                        bytes[offset],
                        bytes[offset + 1],
                        bytes[offset + 2],
                        bytes[offset + 3],
                    ]);
                    dict.push(Value::Date(d));
                    offset += 4;
                }
                LogicalType::Timestamp => {
                    if offset + 9 > bytes.len() {
                        return Err(CompressionError::CorruptedData(
                            "Incomplete timestamp".to_string(),
                        ));
                    }
                    offset += 1; // Skip type marker
                    let ts = i64::from_le_bytes([
                        bytes[offset],
                        bytes[offset + 1],
                        bytes[offset + 2],
                        bytes[offset + 3],
                        bytes[offset + 4],
                        bytes[offset + 5],
                        bytes[offset + 6],
                        bytes[offset + 7],
                    ]);
                    dict.push(Value::Timestamp(ts));
                    offset += 8;
                }
                _ => {
                    return Err(CompressionError::Incompatible(format!(
                        "Unsupported type for dictionary: {:?}",
                        logical_type
                    )))
                }
            }
        }

        Ok(dict)
    }

    /// Encodes indices based on width
    fn encode_indices(indices: &[u32], width: u8) -> Vec<u8> {
        let mut bytes = Vec::new();

        match width {
            1 => {
                for &idx in indices {
                    bytes.push(idx as u8);
                }
            }
            2 => {
                for &idx in indices {
                    bytes.extend_from_slice(&(idx as u16).to_le_bytes());
                }
            }
            4 => {
                for &idx in indices {
                    bytes.extend_from_slice(&idx.to_le_bytes());
                }
            }
            _ => unreachable!("Invalid index width"),
        }

        bytes
    }

    /// Decodes indices based on width
    fn decode_indices(bytes: &[u8], width: u8, count: usize) -> CompressionResult<Vec<u32>> {
        let mut indices = Vec::with_capacity(count);

        match width {
            1 => {
                for &byte in bytes.iter().take(count) {
                    indices.push(byte as u32);
                }
            }
            2 => {
                for chunk in bytes.chunks(2).take(count) {
                    if chunk.len() < 2 {
                        return Err(CompressionError::CorruptedData(
                            "Incomplete index data".to_string(),
                        ));
                    }
                    indices.push(u16::from_le_bytes([chunk[0], chunk[1]]) as u32);
                }
            }
            4 => {
                for chunk in bytes.chunks(4).take(count) {
                    if chunk.len() < 4 {
                        return Err(CompressionError::CorruptedData(
                            "Incomplete index data".to_string(),
                        ));
                    }
                    indices.push(u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
                }
            }
            _ => {
                return Err(CompressionError::InvalidMetadata(format!(
                    "Invalid index width: {}",
                    width
                )))
            }
        }

        Ok(indices)
    }

    /// Builds null bitmap from values
    fn build_null_bitmap(data: &[Value]) -> Option<Vec<u8>> {
        let has_nulls = data.iter().any(|v| matches!(v, Value::Null));
        if !has_nulls {
            return None;
        }

        let num_bytes = (data.len() + 7) / 8;
        let mut bitmap = vec![0u8; num_bytes];

        for (i, value) in data.iter().enumerate() {
            if matches!(value, Value::Null) {
                bitmap[i / 8] |= 1 << (i % 8);
            }
        }

        Some(bitmap)
    }

    /// Checks if value is null from bitmap
    fn is_null(bitmap: &Option<Vec<u8>>, index: usize) -> bool {
        bitmap
            .as_ref()
            .map(|b| (b[index / 8] & (1 << (index % 8))) != 0)
            .unwrap_or(false)
    }
}

impl Default for DictionaryCompression {
    fn default() -> Self {
        Self::new()
    }
}

impl CompressionFunction for DictionaryCompression {
    fn analyze(&self, data: &[Value]) -> CompressionResult<AnalyzeResult> {
        if data.is_empty() {
            return Ok(AnalyzeResult::new(
                CompressionType::Dictionary,
                0,
                0,
            ));
        }

        // Build dictionary to count unique values
        let mut unique_values: HashMap<String, Value> = HashMap::new();
        let mut null_count = 0;

        for value in data {
            if matches!(value, Value::Null) {
                null_count += 1;
            } else {
                let key = format!("{:?}", value); // Simple hash key
                unique_values.entry(key).or_insert_with(|| value.clone());
            }
        }

        let dict_size = unique_values.len();
        let index_width = Self::select_index_width(dict_size) as usize;

        // Helper function to estimate value size
        let estimate_value_size = |v: &Value| -> usize {
            match v {
                Value::Varchar(s) => s.len() + 4, // Length prefix + data
                Value::TinyInt(_) => 1,
                Value::SmallInt(_) => 2,
                Value::Integer(_) => 4,
                Value::BigInt(_) => 8,
                Value::HugeInt { .. } => 16,
                Value::Float(_) => 4,
                Value::Double(_) => 8,
                Value::Boolean(_) => 1,
                Value::Date(_) => 4,
                Value::Time(_) => 8,
                Value::Timestamp(_) => 8,
                Value::Interval { .. } => 16,
                Value::Null => 0,
                // Unsupported types for now - use conservative estimate
                _ => 32,
            }
        };

        // Estimate sizes
        let original_size: usize = data.iter().map(estimate_value_size).sum();

        // Dictionary size + indices
        // Calculate actual size of dictionary values, not debug strings
        let dict_bytes: usize = unique_values.values().map(|v| estimate_value_size(v) + 1).sum();
        let index_bytes = data.len() * index_width;
        let null_bitmap_bytes = if null_count > 0 { (data.len() + 7) / 8 } else { 0 };

        let estimated_size = dict_bytes + index_bytes + null_bitmap_bytes;

        Ok(AnalyzeResult::new(
            CompressionType::Dictionary,
            original_size,
            estimated_size,
        ))
    }

    fn compress(&self, data: &[Value]) -> CompressionResult<CompressedSegment> {
        if data.is_empty() {
            return Ok(CompressedSegment {
                compression_type: CompressionType::Dictionary,
                data: Vec::new(),
                value_count: 0,
                null_bitmap: None,
                metadata: CompressionMetadata::Dictionary {
                    index_width: 1,
                    dict_size: 0,
                    dict_data: Vec::new(),
                },
            });
        }

        // Build null bitmap first
        let null_bitmap = Self::build_null_bitmap(data);

        // Build dictionary (excluding nulls)
        let mut dict_map: HashMap<String, u32> = HashMap::new();
        let mut dict_values: Vec<Value> = Vec::new();

        for value in data {
            if !matches!(value, Value::Null) {
                let key = format!("{:?}", value);
                if !dict_map.contains_key(&key) {
                    dict_map.insert(key, dict_values.len() as u32);
                    dict_values.push(value.clone());
                }
            }
        }

        // Select index width
        let index_width = Self::select_index_width(dict_values.len());

        // Encode indices
        let indices: Vec<u32> = data
            .iter()
            .map(|v| {
                if matches!(v, Value::Null) {
                    0 // Dummy index for nulls (will be masked by null bitmap)
                } else {
                    let key = format!("{:?}", v);
                    *dict_map.get(&key).unwrap()
                }
            })
            .collect();

        let encoded_indices = Self::encode_indices(&indices, index_width);

        // Serialize dictionary
        let dict_data = Self::serialize_dictionary(&dict_values)?;

        Ok(CompressedSegment {
            compression_type: CompressionType::Dictionary,
            data: encoded_indices,
            value_count: data.len(),
            null_bitmap,
            metadata: CompressionMetadata::Dictionary {
                index_width,
                dict_size: dict_values.len() as u32,
                dict_data,
            },
        })
    }

    fn decompress(&self, segment: &CompressedSegment) -> CompressionResult<Vec<Value>> {
        if segment.value_count == 0 {
            return Ok(Vec::new());
        }

        // Extract metadata
        let (index_width, dict_data) = match &segment.metadata {
            CompressionMetadata::Dictionary {
                index_width,
                dict_data,
                ..
            } => (*index_width, dict_data),
            _ => {
                return Err(CompressionError::InvalidMetadata(
                    "Expected Dictionary metadata".to_string(),
                ))
            }
        };

        // Deserialize dictionary (need to infer type from first value)
        // For simplicity, assume Varchar for now (would need type info in real implementation)
        let dict = Self::deserialize_dictionary(dict_data, &LogicalType::Varchar)?;

        // Decode indices
        let indices = Self::decode_indices(&segment.data, index_width, segment.value_count)?;

        // Reconstruct values
        let mut values = Vec::with_capacity(segment.value_count);
        for (i, &idx) in indices.iter().enumerate() {
            if Self::is_null(&segment.null_bitmap, i) {
                values.push(Value::Null);
            } else {
                values.push(dict[idx as usize].clone());
            }
        }

        Ok(values)
    }

    fn scan(
        &self,
        segment: &CompressedSegment,
        selection: &SelectionVector,
    ) -> CompressionResult<Vec<Value>> {
        // Extract metadata
        let (index_width, dict_data) = match &segment.metadata {
            CompressionMetadata::Dictionary {
                index_width,
                dict_data,
                ..
            } => (*index_width, dict_data),
            _ => {
                return Err(CompressionError::InvalidMetadata(
                    "Expected Dictionary metadata".to_string(),
                ))
            }
        };

        // Deserialize dictionary
        let dict = Self::deserialize_dictionary(dict_data, &LogicalType::Varchar)?;

        // Decode only selected indices
        let mut values = Vec::with_capacity(selection.len());

        for &idx in &selection.indices {
            if idx >= segment.value_count {
                return Err(CompressionError::CorruptedData(
                    "Selection index out of bounds".to_string(),
                ));
            }

            if Self::is_null(&segment.null_bitmap, idx) {
                values.push(Value::Null);
            } else {
                // Decode single index
                let offset = idx * index_width as usize;
                let dict_idx = match index_width {
                    1 => segment.data[offset] as u32,
                    2 => u16::from_le_bytes([
                        segment.data[offset],
                        segment.data[offset + 1],
                    ]) as u32,
                    4 => u32::from_le_bytes([
                        segment.data[offset],
                        segment.data[offset + 1],
                        segment.data[offset + 2],
                        segment.data[offset + 3],
                    ]),
                    _ => {
                        return Err(CompressionError::InvalidMetadata(
                            "Invalid index width".to_string(),
                        ))
                    }
                };

                values.push(dict[dict_idx as usize].clone());
            }
        }

        Ok(values)
    }

    fn name(&self) -> &'static str {
        "Dictionary"
    }

    fn supports_type(&self, value: &Value) -> bool {
        matches!(
            value,
            Value::Varchar(_)
                | Value::Integer(_)
                | Value::BigInt(_)
                | Value::Double(_)
                | Value::Boolean(_)
                | Value::Date(_)
                | Value::Timestamp(_)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_select_index_width() {
        assert_eq!(DictionaryCompression::select_index_width(100), 1);
        assert_eq!(DictionaryCompression::select_index_width(256), 2);
        assert_eq!(DictionaryCompression::select_index_width(1000), 2);
        assert_eq!(DictionaryCompression::select_index_width(70000), 4);
    }

    #[test]
    fn test_dictionary_compression_strings() {
        let comp = DictionaryCompression::new();

        let data = vec![
            Value::Varchar("apple".to_string()),
            Value::Varchar("banana".to_string()),
            Value::Varchar("apple".to_string()),
            Value::Varchar("cherry".to_string()),
            Value::Varchar("banana".to_string()),
        ];

        // Compress
        let segment = comp.compress(&data).unwrap();
        assert_eq!(segment.value_count, 5);
        assert_eq!(segment.compression_type, CompressionType::Dictionary);

        // Decompress
        let decompressed = comp.decompress(&segment).unwrap();
        assert_eq!(decompressed.len(), 5);

        // Verify values match
        for (orig, decomp) in data.iter().zip(decompressed.iter()) {
            match (orig, decomp) {
                (Value::Varchar(s1), Value::Varchar(s2)) => assert_eq!(s1, s2),
                _ => panic!("Type mismatch"),
            }
        }
    }

    #[test]
    fn test_dictionary_compression_with_nulls() {
        let comp = DictionaryCompression::new();

        let data = vec![
            Value::Varchar("apple".to_string()),
            Value::Null,
            Value::Varchar("banana".to_string()),
            Value::Null,
            Value::Varchar("apple".to_string()),
        ];

        // Compress
        let segment = comp.compress(&data).unwrap();
        assert!(segment.null_bitmap.is_some());

        // Decompress
        let decompressed = comp.decompress(&segment).unwrap();
        assert_eq!(decompressed.len(), 5);
        assert!(matches!(decompressed[1], Value::Null));
        assert!(matches!(decompressed[3], Value::Null));
    }

    #[test]
    fn test_dictionary_scan_selection() {
        let comp = DictionaryCompression::new();

        let data = vec![
            Value::Varchar("apple".to_string()),
            Value::Varchar("banana".to_string()),
            Value::Varchar("cherry".to_string()),
            Value::Varchar("date".to_string()),
            Value::Varchar("elderberry".to_string()),
        ];

        let segment = comp.compress(&data).unwrap();

        // Scan with selection
        let selection = SelectionVector::new(vec![0, 2, 4]);
        let scanned = comp.scan(&segment, &selection).unwrap();

        assert_eq!(scanned.len(), 3);
        match (&scanned[0], &scanned[1], &scanned[2]) {
            (Value::Varchar(s1), Value::Varchar(s2), Value::Varchar(s3)) => {
                assert_eq!(s1, "apple");
                assert_eq!(s2, "cherry");
                assert_eq!(s3, "elderberry");
            }
            _ => panic!("Type mismatch"),
        }
    }

    #[test]
    fn test_dictionary_analyze() {
        let comp = DictionaryCompression::new();

        let data = vec![
            Value::Varchar("apple".to_string()),
            Value::Varchar("banana".to_string()),
            Value::Varchar("apple".to_string()),
        ];

        let result = comp.analyze(&data).unwrap();
        assert_eq!(result.compression_type, CompressionType::Dictionary);
        assert!(result.is_beneficial());
    }
}
