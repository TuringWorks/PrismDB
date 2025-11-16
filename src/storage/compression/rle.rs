/// RLE (Run-Length Encoding) compression implementation
///
/// RLE encodes consecutive runs of identical values as (value, count) pairs.
/// This is highly effective for sorted or repeated data.
///
/// Best for:
/// - Sorted columns (e.g., timestamp columns, sequential IDs)
/// - Columns with many repeated values (e.g., status flags, categories)
/// - Low-cardinality columns with clustering
///
/// Compression ratio:
/// - Sorted data: 100-1000x
/// - Repeated values: 10-100x
/// - Random data: 0.5-1x (may expand)
///
/// Algorithm:
/// 1. Scan data and count consecutive identical values
/// 2. Encode as sequence of (value, run_length) pairs
/// 3. For decompression, expand each run
/// 4. For scanning, binary search runs to find target indices
use crate::storage::compression::traits::{
    CompressionError, CompressionFunction, CompressionResult,
};
use crate::storage::compression::types::{
    AnalyzeResult, CompressedSegment, CompressionMetadata, CompressionType, SelectionVector,
};
use crate::types::Value;

/// RLE compression function
pub struct RLECompression;

/// A single run in RLE encoding
#[derive(Debug, Clone)]
struct Run {
    value: Value,
    count: u32,
}

impl RLECompression {
    /// Creates a new RLE compression instance
    pub fn new() -> Self {
        Self
    }

    /// Counts runs in the data
    fn count_runs(data: &[Value]) -> Vec<Run> {
        if data.is_empty() {
            return Vec::new();
        }

        let mut runs = Vec::new();
        let mut current_value = data[0].clone();
        let mut current_count = 1u32;

        for value in data.iter().skip(1) {
            if Self::values_equal(&current_value, value) {
                current_count += 1;
            } else {
                runs.push(Run {
                    value: current_value.clone(),
                    count: current_count,
                });
                current_value = value.clone();
                current_count = 1;
            }
        }

        // Push the last run
        runs.push(Run {
            value: current_value,
            count: current_count,
        });

        runs
    }

    /// Checks if two values are equal (handles nulls specially)
    fn values_equal(a: &Value, b: &Value) -> bool {
        match (a, b) {
            (Value::Null, Value::Null) => true,
            (Value::Null, _) | (_, Value::Null) => false,
            _ => a == b,
        }
    }

    /// Serializes a single value to bytes
    fn serialize_value(value: &Value) -> CompressionResult<Vec<u8>> {
        let mut bytes = Vec::new();

        match value {
            Value::Null => {
                bytes.push(0); // Type marker for Null
            }
            Value::Boolean(b) => {
                bytes.push(1); // Type marker
                bytes.push(if *b { 1 } else { 0 });
            }
            Value::TinyInt(i) => {
                bytes.push(2); // Type marker
                bytes.push(*i as u8);
            }
            Value::SmallInt(i) => {
                bytes.push(3); // Type marker
                bytes.extend_from_slice(&i.to_le_bytes());
            }
            Value::Integer(i) => {
                bytes.push(4); // Type marker
                bytes.extend_from_slice(&i.to_le_bytes());
            }
            Value::BigInt(i) => {
                bytes.push(5); // Type marker
                bytes.extend_from_slice(&i.to_le_bytes());
            }
            Value::Float(f) => {
                bytes.push(6); // Type marker
                bytes.extend_from_slice(&f.to_le_bytes());
            }
            Value::Double(d) => {
                bytes.push(7); // Type marker
                bytes.extend_from_slice(&d.to_le_bytes());
            }
            Value::Varchar(s) | Value::Char(s) => {
                bytes.push(8); // Type marker
                bytes.extend_from_slice(&(s.len() as u32).to_le_bytes());
                bytes.extend_from_slice(s.as_bytes());
            }
            Value::Date(d) => {
                bytes.push(9); // Type marker
                bytes.extend_from_slice(&d.to_le_bytes());
            }
            Value::Time(t) => {
                bytes.push(10); // Type marker
                bytes.extend_from_slice(&t.to_le_bytes());
            }
            Value::Timestamp(ts) => {
                bytes.push(11); // Type marker
                bytes.extend_from_slice(&ts.to_le_bytes());
            }
            _ => {
                return Err(CompressionError::Incompatible(format!(
                    "Unsupported value type for RLE: {:?}",
                    value
                )))
            }
        }

        Ok(bytes)
    }

    /// Deserializes a single value from bytes
    fn deserialize_value(bytes: &[u8], offset: &mut usize) -> CompressionResult<Value> {
        if *offset >= bytes.len() {
            return Err(CompressionError::CorruptedData(
                "Unexpected end of data".to_string(),
            ));
        }

        let type_marker = bytes[*offset];
        *offset += 1;

        match type_marker {
            0 => Ok(Value::Null),
            1 => {
                if *offset >= bytes.len() {
                    return Err(CompressionError::CorruptedData(
                        "Incomplete boolean".to_string(),
                    ));
                }
                let b = bytes[*offset] != 0;
                *offset += 1;
                Ok(Value::Boolean(b))
            }
            2 => {
                if *offset >= bytes.len() {
                    return Err(CompressionError::CorruptedData(
                        "Incomplete tinyint".to_string(),
                    ));
                }
                let i = bytes[*offset] as i8;
                *offset += 1;
                Ok(Value::TinyInt(i))
            }
            3 => {
                if *offset + 2 > bytes.len() {
                    return Err(CompressionError::CorruptedData(
                        "Incomplete smallint".to_string(),
                    ));
                }
                let i = i16::from_le_bytes([bytes[*offset], bytes[*offset + 1]]);
                *offset += 2;
                Ok(Value::SmallInt(i))
            }
            4 => {
                if *offset + 4 > bytes.len() {
                    return Err(CompressionError::CorruptedData(
                        "Incomplete integer".to_string(),
                    ));
                }
                let i = i32::from_le_bytes([
                    bytes[*offset],
                    bytes[*offset + 1],
                    bytes[*offset + 2],
                    bytes[*offset + 3],
                ]);
                *offset += 4;
                Ok(Value::Integer(i))
            }
            5 => {
                if *offset + 8 > bytes.len() {
                    return Err(CompressionError::CorruptedData(
                        "Incomplete bigint".to_string(),
                    ));
                }
                let i = i64::from_le_bytes([
                    bytes[*offset],
                    bytes[*offset + 1],
                    bytes[*offset + 2],
                    bytes[*offset + 3],
                    bytes[*offset + 4],
                    bytes[*offset + 5],
                    bytes[*offset + 6],
                    bytes[*offset + 7],
                ]);
                *offset += 8;
                Ok(Value::BigInt(i))
            }
            6 => {
                if *offset + 4 > bytes.len() {
                    return Err(CompressionError::CorruptedData(
                        "Incomplete float".to_string(),
                    ));
                }
                let f = f32::from_le_bytes([
                    bytes[*offset],
                    bytes[*offset + 1],
                    bytes[*offset + 2],
                    bytes[*offset + 3],
                ]);
                *offset += 4;
                Ok(Value::Float(f))
            }
            7 => {
                if *offset + 8 > bytes.len() {
                    return Err(CompressionError::CorruptedData(
                        "Incomplete double".to_string(),
                    ));
                }
                let d = f64::from_le_bytes([
                    bytes[*offset],
                    bytes[*offset + 1],
                    bytes[*offset + 2],
                    bytes[*offset + 3],
                    bytes[*offset + 4],
                    bytes[*offset + 5],
                    bytes[*offset + 6],
                    bytes[*offset + 7],
                ]);
                *offset += 8;
                Ok(Value::Double(d))
            }
            8 => {
                if *offset + 4 > bytes.len() {
                    return Err(CompressionError::CorruptedData(
                        "Incomplete string length".to_string(),
                    ));
                }
                let len = u32::from_le_bytes([
                    bytes[*offset],
                    bytes[*offset + 1],
                    bytes[*offset + 2],
                    bytes[*offset + 3],
                ]) as usize;
                *offset += 4;

                if *offset + len > bytes.len() {
                    return Err(CompressionError::CorruptedData(
                        "Incomplete string data".to_string(),
                    ));
                }
                let s = String::from_utf8(bytes[*offset..*offset + len].to_vec()).map_err(
                    |e| CompressionError::CorruptedData(format!("Invalid UTF-8: {}", e)),
                )?;
                *offset += len;
                Ok(Value::Varchar(s))
            }
            9 => {
                if *offset + 4 > bytes.len() {
                    return Err(CompressionError::CorruptedData(
                        "Incomplete date".to_string(),
                    ));
                }
                let d = i32::from_le_bytes([
                    bytes[*offset],
                    bytes[*offset + 1],
                    bytes[*offset + 2],
                    bytes[*offset + 3],
                ]);
                *offset += 4;
                Ok(Value::Date(d))
            }
            10 => {
                if *offset + 8 > bytes.len() {
                    return Err(CompressionError::CorruptedData(
                        "Incomplete time".to_string(),
                    ));
                }
                let t = i64::from_le_bytes([
                    bytes[*offset],
                    bytes[*offset + 1],
                    bytes[*offset + 2],
                    bytes[*offset + 3],
                    bytes[*offset + 4],
                    bytes[*offset + 5],
                    bytes[*offset + 6],
                    bytes[*offset + 7],
                ]);
                *offset += 8;
                Ok(Value::Time(t))
            }
            11 => {
                if *offset + 8 > bytes.len() {
                    return Err(CompressionError::CorruptedData(
                        "Incomplete timestamp".to_string(),
                    ));
                }
                let ts = i64::from_le_bytes([
                    bytes[*offset],
                    bytes[*offset + 1],
                    bytes[*offset + 2],
                    bytes[*offset + 3],
                    bytes[*offset + 4],
                    bytes[*offset + 5],
                    bytes[*offset + 6],
                    bytes[*offset + 7],
                ]);
                *offset += 8;
                Ok(Value::Timestamp(ts))
            }
            _ => Err(CompressionError::CorruptedData(format!(
                "Invalid type marker: {}",
                type_marker
            ))),
        }
    }

    /// Serializes runs to bytes
    fn serialize_runs(runs: &[Run]) -> CompressionResult<Vec<u8>> {
        let mut bytes = Vec::new();

        // Write run count
        bytes.extend_from_slice(&(runs.len() as u32).to_le_bytes());

        // Write each run
        for run in runs {
            // Serialize value
            let value_bytes = Self::serialize_value(&run.value)?;
            bytes.extend_from_slice(&value_bytes);

            // Write run count
            bytes.extend_from_slice(&run.count.to_le_bytes());
        }

        Ok(bytes)
    }

    /// Deserializes runs from bytes
    fn deserialize_runs(bytes: &[u8]) -> CompressionResult<Vec<Run>> {
        let mut offset = 0;

        // Read run count
        if bytes.len() < 4 {
            return Err(CompressionError::CorruptedData(
                "Incomplete run count".to_string(),
            ));
        }
        let run_count = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
        offset += 4;

        let mut runs = Vec::with_capacity(run_count);

        // Read each run
        for _ in 0..run_count {
            let value = Self::deserialize_value(bytes, &mut offset)?;

            if offset + 4 > bytes.len() {
                return Err(CompressionError::CorruptedData(
                    "Incomplete run count".to_string(),
                ));
            }
            let count = u32::from_le_bytes([
                bytes[offset],
                bytes[offset + 1],
                bytes[offset + 2],
                bytes[offset + 3],
            ]);
            offset += 4;

            runs.push(Run { value, count });
        }

        Ok(runs)
    }

    /// Binary search to find the run containing a specific index
    fn find_run_for_index(runs: &[Run], index: usize) -> Option<(usize, usize)> {
        let mut cumulative_count = 0usize;

        for (run_idx, run) in runs.iter().enumerate() {
            let next_cumulative = cumulative_count + run.count as usize;
            if index < next_cumulative {
                // Found the run, return (run_index, offset_within_run)
                let offset_in_run = index - cumulative_count;
                return Some((run_idx, offset_in_run));
            }
            cumulative_count = next_cumulative;
        }

        None
    }

    /// Estimates value size for compression analysis
    fn estimate_value_size(value: &Value) -> usize {
        match value {
            Value::Null => 1,
            Value::Boolean(_) => 2,
            Value::TinyInt(_) => 2,
            Value::SmallInt(_) => 3,
            Value::Integer(_) => 5,
            Value::BigInt(_) => 9,
            Value::Float(_) => 5,
            Value::Double(_) => 9,
            Value::Varchar(s) | Value::Char(s) => 5 + s.len(),
            Value::Date(_) => 5,
            Value::Time(_) => 9,
            Value::Timestamp(_) => 9,
            _ => 32, // Conservative estimate for unsupported types
        }
    }
}

impl Default for RLECompression {
    fn default() -> Self {
        Self::new()
    }
}

impl CompressionFunction for RLECompression {
    fn analyze(&self, data: &[Value]) -> CompressionResult<AnalyzeResult> {
        if data.is_empty() {
            return Ok(AnalyzeResult::new(CompressionType::RLE, 0, 0));
        }

        // Count runs
        let runs = Self::count_runs(data);

        // Estimate original size
        let original_size: usize = data.iter().map(Self::estimate_value_size).sum();

        // Estimate compressed size
        let mut compressed_size = 4; // Run count (u32)

        for run in &runs {
            compressed_size += Self::estimate_value_size(&run.value); // Value
            compressed_size += 4; // Count (u32)
        }

        Ok(AnalyzeResult::new(
            CompressionType::RLE,
            original_size,
            compressed_size,
        ))
    }

    fn compress(&self, data: &[Value]) -> CompressionResult<CompressedSegment> {
        if data.is_empty() {
            return Ok(CompressedSegment {
                compression_type: CompressionType::RLE,
                data: Vec::new(),
                value_count: 0,
                null_bitmap: None,
                metadata: CompressionMetadata::RLE { run_count: 0 },
            });
        }

        // Count runs
        let runs = Self::count_runs(data);

        // Serialize runs
        let compressed_data = Self::serialize_runs(&runs)?;

        Ok(CompressedSegment {
            compression_type: CompressionType::RLE,
            data: compressed_data,
            value_count: data.len(),
            null_bitmap: None, // RLE encodes nulls as values
            metadata: CompressionMetadata::RLE {
                run_count: runs.len() as u32,
            },
        })
    }

    fn decompress(&self, segment: &CompressedSegment) -> CompressionResult<Vec<Value>> {
        if segment.value_count == 0 {
            return Ok(Vec::new());
        }

        // Deserialize runs
        let runs = Self::deserialize_runs(&segment.data)?;

        // Expand runs
        let mut values = Vec::with_capacity(segment.value_count);

        for run in runs {
            for _ in 0..run.count {
                values.push(run.value.clone());
            }
        }

        // Verify value count
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

        // Deserialize runs
        let runs = Self::deserialize_runs(&segment.data)?;

        // Extract selected values using binary search
        let mut values = Vec::with_capacity(selection.len());

        for &idx in &selection.indices {
            if idx >= segment.value_count {
                return Err(CompressionError::CorruptedData(
                    "Selection index out of bounds".to_string(),
                ));
            }

            // Find run containing this index
            if let Some((run_idx, _)) = Self::find_run_for_index(&runs, idx) {
                values.push(runs[run_idx].value.clone());
            } else {
                return Err(CompressionError::CorruptedData(format!(
                    "Could not find run for index {}",
                    idx
                )));
            }
        }

        Ok(values)
    }

    fn name(&self) -> &'static str {
        "RLE"
    }

    fn supports_type(&self, _value: &Value) -> bool {
        // RLE supports all types
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rle_sorted_integers() {
        let comp = RLECompression::new();

        // Sorted data with long runs
        let data = vec![
            Value::Integer(1),
            Value::Integer(1),
            Value::Integer(1),
            Value::Integer(1),
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(2),
            Value::Integer(2),
            Value::Integer(3),
            Value::Integer(3),
        ];

        // Compress
        let segment = comp.compress(&data).unwrap();
        assert_eq!(segment.value_count, 10);
        assert_eq!(segment.compression_type, CompressionType::RLE);

        // Should have 3 runs
        match &segment.metadata {
            CompressionMetadata::RLE { run_count } => assert_eq!(*run_count, 3),
            _ => panic!("Wrong metadata type"),
        }

        // Decompress
        let decompressed = comp.decompress(&segment).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_rle_repeated_strings() {
        let comp = RLECompression::new();

        let data = vec![
            Value::Varchar("apple".to_string()),
            Value::Varchar("apple".to_string()),
            Value::Varchar("apple".to_string()),
            Value::Varchar("banana".to_string()),
            Value::Varchar("banana".to_string()),
        ];

        let segment = comp.compress(&data).unwrap();
        let decompressed = comp.decompress(&segment).unwrap();

        // Verify compression
        match &segment.metadata {
            CompressionMetadata::RLE { run_count } => assert_eq!(*run_count, 2),
            _ => panic!("Wrong metadata type"),
        }

        // Verify values
        assert_eq!(decompressed.len(), 5);
        for (orig, decomp) in data.iter().zip(decompressed.iter()) {
            assert_eq!(orig, decomp);
        }
    }

    #[test]
    fn test_rle_with_nulls() {
        let comp = RLECompression::new();

        let data = vec![
            Value::Integer(1),
            Value::Integer(1),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Integer(2),
        ];

        let segment = comp.compress(&data).unwrap();
        let decompressed = comp.decompress(&segment).unwrap();

        // Should have 3 runs (1, NULL, 2)
        match &segment.metadata {
            CompressionMetadata::RLE { run_count } => assert_eq!(*run_count, 3),
            _ => panic!("Wrong metadata type"),
        }

        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_rle_scan_selection() {
        let comp = RLECompression::new();

        let data = vec![
            Value::Integer(1),
            Value::Integer(1),
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(2),
            Value::Integer(3),
            Value::Integer(3),
            Value::Integer(3),
        ];

        let segment = comp.compress(&data).unwrap();

        // Scan with selection
        let selection = SelectionVector::new(vec![0, 3, 7]);
        let scanned = comp.scan(&segment, &selection).unwrap();

        assert_eq!(scanned.len(), 3);
        assert_eq!(scanned[0], Value::Integer(1));
        assert_eq!(scanned[1], Value::Integer(2));
        assert_eq!(scanned[2], Value::Integer(3));
    }

    #[test]
    fn test_rle_analyze() {
        let comp = RLECompression::new();

        // Highly compressible data
        let data = vec![Value::Integer(42); 1000];

        let result = comp.analyze(&data).unwrap();
        assert_eq!(result.compression_type, CompressionType::RLE);

        // Should have huge compression ratio (1000 values → 1 run)
        assert!(result.compression_ratio > 100.0);
        assert!(result.is_beneficial());
    }

    #[test]
    fn test_rle_random_data() {
        let comp = RLECompression::new();

        // Random data (no runs)
        let data = vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
            Value::Integer(4),
            Value::Integer(5),
        ];

        let result = comp.analyze(&data).unwrap();

        // Should have low compression ratio (5 values → 5 runs)
        // May actually expand due to overhead
        assert!(result.compression_ratio <= 1.5);
    }

    #[test]
    fn test_rle_empty_data() {
        let comp = RLECompression::new();

        let data: Vec<Value> = Vec::new();
        let segment = comp.compress(&data).unwrap();
        let decompressed = comp.decompress(&segment).unwrap();

        assert_eq!(decompressed.len(), 0);
    }

    #[test]
    fn test_rle_single_value() {
        let comp = RLECompression::new();

        let data = vec![Value::Integer(42)];
        let segment = comp.compress(&data).unwrap();
        let decompressed = comp.decompress(&segment).unwrap();

        assert_eq!(decompressed, data);
        match &segment.metadata {
            CompressionMetadata::RLE { run_count } => assert_eq!(*run_count, 1),
            _ => panic!("Wrong metadata type"),
        }
    }

    #[test]
    fn test_rle_different_types() {
        let comp = RLECompression::new();

        // Test with different data types
        let data = vec![
            Value::Boolean(true),
            Value::Boolean(true),
            Value::Double(3.14),
            Value::Double(3.14),
            Value::Double(3.14),
        ];

        let segment = comp.compress(&data).unwrap();
        let decompressed = comp.decompress(&segment).unwrap();

        assert_eq!(decompressed, data);
        match &segment.metadata {
            CompressionMetadata::RLE { run_count } => assert_eq!(*run_count, 2),
            _ => panic!("Wrong metadata type"),
        }
    }
}
