/// Compression function trait
///
/// All compression algorithms implement this trait to provide a uniform interface
/// for compression, decompression, and analysis.
use crate::storage::compression::types::{
    AnalyzeResult, CompressedSegment, SelectionVector,
};
use crate::types::Value;
use std::error::Error;
use std::fmt;

/// Compression error type
#[derive(Debug, Clone)]
pub enum CompressionError {
    /// Data cannot be compressed with this algorithm
    Incompatible(String),

    /// Compression failed
    CompressionFailed(String),

    /// Decompression failed
    DecompressionFailed(String),

    /// Invalid compression metadata
    InvalidMetadata(String),

    /// Data corruption detected
    CorruptedData(String),
}

impl fmt::Display for CompressionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompressionError::Incompatible(msg) => write!(f, "Incompatible data: {}", msg),
            CompressionError::CompressionFailed(msg) => write!(f, "Compression failed: {}", msg),
            CompressionError::DecompressionFailed(msg) => {
                write!(f, "Decompression failed: {}", msg)
            }
            CompressionError::InvalidMetadata(msg) => write!(f, "Invalid metadata: {}", msg),
            CompressionError::CorruptedData(msg) => write!(f, "Corrupted data: {}", msg),
        }
    }
}

impl Error for CompressionError {}

/// Result type for compression operations
pub type CompressionResult<T> = Result<T, CompressionError>;

/// Compression function trait
///
/// Defines the interface that all compression algorithms must implement.
pub trait CompressionFunction: Send + Sync {
    /// Analyzes data to determine if this compression is suitable
    ///
    /// # Arguments
    /// * `data` - Input data values
    ///
    /// # Returns
    /// Analysis result with estimated compression ratio
    fn analyze(&self, data: &[Value]) -> CompressionResult<AnalyzeResult>;

    /// Compresses data into a compressed segment
    ///
    /// # Arguments
    /// * `data` - Input data values
    ///
    /// # Returns
    /// Compressed segment with metadata
    fn compress(&self, data: &[Value]) -> CompressionResult<CompressedSegment>;

    /// Decompresses an entire segment
    ///
    /// # Arguments
    /// * `segment` - Compressed segment
    ///
    /// # Returns
    /// Decompressed values
    fn decompress(&self, segment: &CompressedSegment) -> CompressionResult<Vec<Value>>;

    /// Scans segment with optional selection vector
    ///
    /// This is an optimized path that can avoid decompressing the entire segment
    /// if only specific values are needed.
    ///
    /// # Arguments
    /// * `segment` - Compressed segment
    /// * `selection` - Indices of values to extract
    ///
    /// # Returns
    /// Selected decompressed values
    fn scan(
        &self,
        segment: &CompressedSegment,
        selection: &SelectionVector,
    ) -> CompressionResult<Vec<Value>>;

    /// Returns the name of this compression algorithm
    fn name(&self) -> &'static str;

    /// Returns whether this algorithm supports the given data type
    fn supports_type(&self, value: &Value) -> bool;
}

/// Helper trait for compression statistics
pub trait CompressionStats {
    /// Returns the uncompressed size
    fn uncompressed_size(&self) -> usize;

    /// Returns the compressed size
    fn compressed_size(&self) -> usize;

    /// Returns the compression ratio
    fn compression_ratio(&self) -> f64 {
        if self.compressed_size() > 0 {
            self.uncompressed_size() as f64 / self.compressed_size() as f64
        } else {
            1.0
        }
    }

    /// Returns the space savings as a percentage
    fn space_savings(&self) -> f64 {
        if self.uncompressed_size() > 0 {
            (1.0 - (self.compressed_size() as f64 / self.uncompressed_size() as f64)) * 100.0
        } else {
            0.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockStats {
        uncompressed: usize,
        compressed: usize,
    }

    impl CompressionStats for MockStats {
        fn uncompressed_size(&self) -> usize {
            self.uncompressed
        }

        fn compressed_size(&self) -> usize {
            self.compressed
        }
    }

    #[test]
    fn test_compression_ratio() {
        let stats = MockStats {
            uncompressed: 1000,
            compressed: 100,
        };
        assert_eq!(stats.compression_ratio(), 10.0);
    }

    #[test]
    fn test_space_savings() {
        let stats = MockStats {
            uncompressed: 1000,
            compressed: 100,
        };
        assert_eq!(stats.space_savings(), 90.0);

        let stats2 = MockStats {
            uncompressed: 1000,
            compressed: 800,
        };
        // Use approximate equality due to floating point precision
        let savings = stats2.space_savings();
        assert!((savings - 20.0).abs() < 0.0001);
    }

    #[test]
    fn test_compression_error_display() {
        let err = CompressionError::Incompatible("test".to_string());
        assert_eq!(format!("{}", err), "Incompatible data: test");

        let err = CompressionError::CompressionFailed("failed".to_string());
        assert_eq!(format!("{}", err), "Compression failed: failed");
    }
}
