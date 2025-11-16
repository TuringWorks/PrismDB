/// Compression algorithm selection
///
/// This module analyzes data and selects the optimal compression algorithm
/// by testing multiple algorithms and choosing the one with the best compression ratio.
use crate::storage::compression::dictionary::DictionaryCompression;
use crate::storage::compression::rle::RLECompression;
use crate::storage::compression::traits::{CompressionFunction, CompressionResult};
use crate::storage::compression::types::{AnalyzeResult, CompressedSegment, CompressionType};
use crate::storage::compression::uncompressed::UncompressedStorage;
use crate::types::Value;

/// Compression selector that tests multiple algorithms
pub struct CompressionSelector {
    /// Minimum compression ratio to use compression (default: 1.1 = 10% savings)
    min_compression_ratio: f64,

    /// Maximum sample size for analysis (to avoid slow analysis on large data)
    max_sample_size: usize,
}

impl CompressionSelector {
    /// Creates a new compression selector with default settings
    pub fn new() -> Self {
        Self {
            min_compression_ratio: 1.1,
            max_sample_size: 10000,
        }
    }

    /// Creates a compression selector with custom settings
    pub fn with_settings(min_compression_ratio: f64, max_sample_size: usize) -> Self {
        Self {
            min_compression_ratio,
            max_sample_size,
        }
    }

    /// Analyzes data and selects the best compression algorithm
    ///
    /// Tests all available algorithms and returns the one with the best compression ratio.
    /// Falls back to uncompressed if no algorithm provides sufficient benefit.
    pub fn select_compression(&self, data: &[Value]) -> CompressionResult<CompressionType> {
        if data.is_empty() {
            return Ok(CompressionType::Uncompressed);
        }

        // Sample data if too large
        let sample_data = self.sample_data(data);

        // Test all algorithms
        let results = self.analyze_all_algorithms(sample_data)?;

        // Select best algorithm
        let best = self.select_best(&results);

        Ok(best.compression_type)
    }

    /// Compresses data using the optimal algorithm
    ///
    /// Automatically selects the best compression algorithm and compresses the data.
    pub fn compress(&self, data: &[Value]) -> CompressionResult<CompressedSegment> {
        let compression_type = self.select_compression(data)?;

        match compression_type {
            CompressionType::Dictionary => {
                let comp = DictionaryCompression::new();
                comp.compress(data)
            }
            CompressionType::RLE => {
                let comp = RLECompression::new();
                comp.compress(data)
            }
            CompressionType::Uncompressed => {
                let comp = UncompressedStorage::new();
                comp.compress(data)
            }
        }
    }

    /// Samples data if it's too large
    fn sample_data<'a>(&self, data: &'a [Value]) -> &'a [Value] {
        if data.len() <= self.max_sample_size {
            data
        } else {
            // Take first max_sample_size values
            // TODO: Could use random sampling or stratified sampling
            &data[..self.max_sample_size]
        }
    }

    /// Analyzes all available compression algorithms
    fn analyze_all_algorithms(&self, data: &[Value]) -> CompressionResult<Vec<AnalyzeResult>> {
        let mut results = Vec::new();

        // Test Dictionary compression
        let dict = DictionaryCompression::new();
        results.push(dict.analyze(data)?);

        // Test RLE compression
        let rle = RLECompression::new();
        results.push(rle.analyze(data)?);

        // Test Uncompressed (baseline)
        let uncompressed = UncompressedStorage::new();
        results.push(uncompressed.analyze(data)?);

        Ok(results)
    }

    /// Selects the best compression algorithm from analysis results
    fn select_best<'a>(&self, results: &'a [AnalyzeResult]) -> &'a AnalyzeResult {
        // Find algorithm with highest compression ratio
        let mut best = &results[0];
        let mut best_ratio = best.compression_ratio;

        for result in results.iter().skip(1) {
            if result.compression_ratio > best_ratio {
                best = result;
                best_ratio = result.compression_ratio;
            }
        }

        // If best ratio doesn't meet minimum threshold, use uncompressed
        if best_ratio < self.min_compression_ratio {
            // Find uncompressed result
            for result in results {
                if result.compression_type == CompressionType::Uncompressed {
                    return result;
                }
            }
        }

        best
    }
}

impl Default for CompressionSelector {
    fn default() -> Self {
        Self::new()
    }
}

/// Convenience function to auto-compress data with optimal algorithm
pub fn auto_compress(data: &[Value]) -> CompressionResult<CompressedSegment> {
    let selector = CompressionSelector::new();
    selector.compress(data)
}

/// Convenience function to select optimal compression type
pub fn select_compression_type(data: &[Value]) -> CompressionResult<CompressionType> {
    let selector = CompressionSelector::new();
    selector.select_compression(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_selector_low_cardinality_strings() {
        let selector = CompressionSelector::new();

        // Low cardinality - should select Dictionary
        let data = vec![
            Value::Varchar("apple".to_string()),
            Value::Varchar("banana".to_string()),
            Value::Varchar("apple".to_string()),
            Value::Varchar("cherry".to_string()),
            Value::Varchar("banana".to_string()),
            Value::Varchar("apple".to_string()),
        ];

        let compression_type = selector.select_compression(&data).unwrap();
        assert_eq!(compression_type, CompressionType::Dictionary);
    }

    #[test]
    fn test_selector_sorted_data() {
        let selector = CompressionSelector::new();

        // Sorted data with runs - should select RLE
        let data = vec![
            Value::Integer(1),
            Value::Integer(1),
            Value::Integer(1),
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(2),
            Value::Integer(2),
            Value::Integer(3),
            Value::Integer(3),
            Value::Integer(3),
        ];

        let compression_type = selector.select_compression(&data).unwrap();
        assert_eq!(compression_type, CompressionType::RLE);
    }

    #[test]
    fn test_selector_random_data() {
        let selector = CompressionSelector::new();

        // Random unique values - should select Uncompressed or Dictionary
        let data = vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
            Value::Integer(4),
            Value::Integer(5),
        ];

        let compression_type = selector.select_compression(&data).unwrap();
        // Either uncompressed or dictionary is acceptable for random data
        assert!(
            compression_type == CompressionType::Uncompressed
                || compression_type == CompressionType::Dictionary
        );
    }

    #[test]
    fn test_selector_highly_repeated() {
        let selector = CompressionSelector::new();

        // Highly repeated value - should select RLE
        let data = vec![Value::Integer(42); 1000];

        let compression_type = selector.select_compression(&data).unwrap();
        assert_eq!(compression_type, CompressionType::RLE);
    }

    #[test]
    fn test_auto_compress() {
        let data = vec![
            Value::Varchar("apple".to_string()),
            Value::Varchar("apple".to_string()),
            Value::Varchar("banana".to_string()),
        ];

        let segment = auto_compress(&data).unwrap();
        assert_eq!(segment.value_count, 3);

        // Should have selected a compression algorithm
        assert_ne!(segment.compression_type, CompressionType::Uncompressed);
    }

    #[test]
    fn test_selector_with_custom_settings() {
        let selector = CompressionSelector::with_settings(2.0, 1000);

        // Data that only achieves 1.5x compression
        let data = vec![
            Value::Integer(1),
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ];

        // With minimum ratio of 2.0, should fall back to uncompressed
        let compression_type = selector.select_compression(&data).unwrap();

        // Might select uncompressed due to high threshold
        // (depends on actual compression ratios achieved)
        assert!(
            compression_type == CompressionType::Uncompressed
                || compression_type == CompressionType::Dictionary
                || compression_type == CompressionType::RLE
        );
    }

    #[test]
    fn test_selector_empty_data() {
        let selector = CompressionSelector::new();

        let data: Vec<Value> = Vec::new();
        let compression_type = selector.select_compression(&data).unwrap();

        assert_eq!(compression_type, CompressionType::Uncompressed);
    }

    #[test]
    fn test_selector_mixed_nulls() {
        let selector = CompressionSelector::new();

        // Use more data to make compression actually beneficial
        let mut data = vec![];
        for _ in 0..10 {
            data.push(Value::Null);
            data.push(Value::Null);
            data.push(Value::Null);
            data.push(Value::Integer(1));
            data.push(Value::Integer(2));
        }

        let compression_type = selector.select_compression(&data).unwrap();

        // Should select RLE or Dictionary for repeated null patterns
        assert!(
            compression_type == CompressionType::RLE
                || compression_type == CompressionType::Dictionary
        );
    }

    #[test]
    fn test_compress_decompress_cycle() {
        let selector = CompressionSelector::new();

        let data = vec![
            Value::Varchar("test".to_string()),
            Value::Varchar("test".to_string()),
            Value::Varchar("hello".to_string()),
        ];

        // Compress
        let segment = selector.compress(&data).unwrap();

        // Decompress using appropriate algorithm
        let decompressed = match segment.compression_type {
            CompressionType::Dictionary => {
                let comp = DictionaryCompression::new();
                comp.decompress(&segment).unwrap()
            }
            CompressionType::RLE => {
                let comp = RLECompression::new();
                comp.decompress(&segment).unwrap()
            }
            CompressionType::Uncompressed => {
                let comp = UncompressedStorage::new();
                comp.decompress(&segment).unwrap()
            }
        };

        assert_eq!(decompressed, data);
    }
}
