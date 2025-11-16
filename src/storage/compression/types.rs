/// Compression types supported by the storage engine
///
/// DuckDBRS implements a subset of DuckDB's compression algorithms,
/// focusing on the most impactful ones for typical workloads.
use serde::{Deserialize, Serialize};

/// Compression algorithm identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CompressionType {
    /// No compression - direct value storage
    Uncompressed,

    /// Dictionary encoding - maps values to integer indices
    /// Best for: Low-cardinality strings, categorical data
    /// Compression ratio: 10-50x for low cardinality, 2-5x for high
    Dictionary,

    /// Run-Length Encoding - stores (value, count) pairs
    /// Best for: Sorted data, repeated values
    /// Compression ratio: 100-1000x for sorted, 10-100x for repeated
    RLE,

    // Future compression algorithms:
    // BitPacking,      // Integer compression with SIMD
    // FSST,            // Fast Static Symbol Table for strings
    // Zstd,            // General-purpose compression
    // ALP,             // Adaptive Lossless floating-Point
    // Chimp,           // Time series compression
}

impl CompressionType {
    /// Returns human-readable name
    pub fn name(&self) -> &'static str {
        match self {
            CompressionType::Uncompressed => "Uncompressed",
            CompressionType::Dictionary => "Dictionary",
            CompressionType::RLE => "RLE",
        }
    }

    /// Returns whether this compression type is lossless
    pub fn is_lossless(&self) -> bool {
        true // All implemented compressions are lossless
    }
}

/// Result of compression analysis
#[derive(Debug, Clone)]
pub struct AnalyzeResult {
    /// Recommended compression type
    pub compression_type: CompressionType,

    /// Estimated compressed size in bytes
    pub estimated_size: usize,

    /// Estimated compression ratio (original_size / compressed_size)
    pub compression_ratio: f64,

    /// Confidence in the estimate (0.0 = low, 1.0 = high)
    pub confidence: f64,
}

impl AnalyzeResult {
    /// Creates a new analyze result
    pub fn new(
        compression_type: CompressionType,
        original_size: usize,
        estimated_size: usize,
    ) -> Self {
        let compression_ratio = if estimated_size > 0 {
            original_size as f64 / estimated_size as f64
        } else {
            1.0
        };

        Self {
            compression_type,
            estimated_size,
            compression_ratio,
            confidence: 1.0,
        }
    }

    /// Returns whether compression is beneficial (ratio > 1.0)
    pub fn is_beneficial(&self) -> bool {
        self.compression_ratio > 1.0
    }
}

/// Compressed segment data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressedSegment {
    /// Compression type used
    pub compression_type: CompressionType,

    /// Compressed data bytes
    pub data: Vec<u8>,

    /// Number of values in the segment
    pub value_count: usize,

    /// Null bitmap (1 bit per value, None if no nulls)
    pub null_bitmap: Option<Vec<u8>>,

    /// Compression metadata (algorithm-specific)
    pub metadata: CompressionMetadata,
}

impl CompressedSegment {
    /// Returns the total size of the compressed segment in bytes
    pub fn total_size(&self) -> usize {
        self.data.len()
            + self.null_bitmap.as_ref().map(|b| b.len()).unwrap_or(0)
            + self.metadata.size()
            + std::mem::size_of::<Self>()
    }
}

/// Compression metadata (algorithm-specific)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CompressionMetadata {
    /// No metadata for uncompressed
    Uncompressed,

    /// Dictionary compression metadata
    Dictionary {
        /// Dictionary index width (1, 2, or 4 bytes)
        index_width: u8,

        /// Number of unique values in dictionary
        dict_size: u32,

        /// Dictionary data (serialized)
        dict_data: Vec<u8>,
    },

    /// RLE compression metadata
    RLE {
        /// Number of runs
        run_count: u32,
    },
}

impl CompressionMetadata {
    /// Returns the size of the metadata in bytes
    pub fn size(&self) -> usize {
        match self {
            CompressionMetadata::Uncompressed => 0,
            CompressionMetadata::Dictionary { dict_data, .. } => {
                std::mem::size_of::<u8>() + std::mem::size_of::<u32>() + dict_data.len()
            }
            CompressionMetadata::RLE { .. } => std::mem::size_of::<u32>(),
        }
    }
}

/// Selection vector for filtered scans
#[derive(Debug, Clone)]
pub struct SelectionVector {
    /// Selected row indices
    pub indices: Vec<usize>,
}

impl SelectionVector {
    /// Creates a new selection vector
    pub fn new(indices: Vec<usize>) -> Self {
        Self { indices }
    }

    /// Creates a selection vector for all rows
    pub fn all(count: usize) -> Self {
        Self {
            indices: (0..count).collect(),
        }
    }

    /// Returns the number of selected rows
    pub fn len(&self) -> usize {
        self.indices.len()
    }

    /// Returns whether the selection is empty
    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compression_type_name() {
        assert_eq!(CompressionType::Uncompressed.name(), "Uncompressed");
        assert_eq!(CompressionType::Dictionary.name(), "Dictionary");
        assert_eq!(CompressionType::RLE.name(), "RLE");
    }

    #[test]
    fn test_analyze_result() {
        let result = AnalyzeResult::new(
            CompressionType::Dictionary,
            1000,
            100,
        );

        assert_eq!(result.compression_type, CompressionType::Dictionary);
        assert_eq!(result.estimated_size, 100);
        assert_eq!(result.compression_ratio, 10.0);
        assert!(result.is_beneficial());
    }

    #[test]
    fn test_analyze_result_no_benefit() {
        let result = AnalyzeResult::new(
            CompressionType::Uncompressed,
            1000,
            1200,
        );

        assert_eq!(result.compression_ratio, 1000.0 / 1200.0);
        assert!(!result.is_beneficial());
    }

    #[test]
    fn test_selection_vector() {
        let sel = SelectionVector::all(100);
        assert_eq!(sel.len(), 100);
        assert!(!sel.is_empty());

        let sel = SelectionVector::new(vec![1, 3, 5]);
        assert_eq!(sel.len(), 3);
        assert_eq!(sel.indices, vec![1, 3, 5]);
    }
}
