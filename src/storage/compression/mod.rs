/// Compression module for PrismDB storage engine
///
/// This module provides compression algorithms for columnar data storage.
/// Compression is critical for reducing storage size and improving I/O performance.
///
/// ## Supported Compression Algorithms:
///
/// - **Dictionary**: Maps values to integer indices (10-50x for low cardinality)
/// - **RLE**: Run-length encoding for sorted/repeated data (100-1000x for sorted)
/// - **Uncompressed**: Fallback when compression doesn't help
///
/// ## Automatic Compression Selection:
///
/// Use `CompressionSelector` or `auto_compress()` to automatically choose the best algorithm.
///
/// ## Future Algorithms:
///
/// - BitPacking: Integer compression with SIMD
/// - FSST: Fast Static Symbol Table for strings
/// - Zstd: General-purpose compression
/// - ALP: Adaptive Lossless floating-Point
/// - Chimp: Time series compression
///
/// ## Usage Example:
///
/// ```ignore
/// use prismdb::storage::compression::*;
///
/// // Manual compression with specific algorithm
/// let comp = DictionaryCompression::new();
/// let data = vec![Value::Varchar("apple".to_string()), ...];
/// let result = comp.analyze(&data)?;
///
/// if result.is_beneficial() {
///     let segment = comp.compress(&data)?;
///     let values = comp.decompress(&segment)?;
/// }
///
/// // Automatic compression (recommended)
/// let segment = auto_compress(&data)?;
/// ```

pub mod analyze;
pub mod dictionary;
pub mod rle;
pub mod traits;
pub mod types;
pub mod uncompressed;

// Future modules:
// pub mod bitpacking;
// pub mod fsst;
// pub mod zstd;
// pub mod alp;
// pub mod chimp;

pub use analyze::{auto_compress, select_compression_type, CompressionSelector};
pub use dictionary::DictionaryCompression;
pub use rle::RLECompression;
pub use traits::{CompressionError, CompressionFunction, CompressionResult, CompressionStats};
pub use types::{
    AnalyzeResult, CompressedSegment, CompressionMetadata, CompressionType, SelectionVector,
};
pub use uncompressed::UncompressedStorage;
