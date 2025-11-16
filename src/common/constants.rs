//! Constants used throughout PrismDB

/// Default vector size for processing
pub const STANDARD_VECTOR_SIZE: usize = 2048;

/// Invalid index constant
pub const INVALID_INDEX: usize = usize::MAX;

/// Invalid column constant
pub const INVALID_COLUMN: usize = usize::MAX;

/// Page size for storage (typically 4KB)
pub const STORAGE_PAGE_SIZE: usize = 4096;

/// Block size for storage (typically 256KB)
pub const STORAGE_BLOCK_SIZE: usize = 262144;

/// Maximum string length
pub const MAX_STRING_LENGTH: usize = 1 << 30; // 1GB

/// Maximum list length
pub const MAX_LIST_LENGTH: usize = 1 << 30; // 1GB

/// Maximum array size
pub const MAX_ARRAY_SIZE: usize = 1 << 20; // 1M elements

/// Maximum blob size
pub const MAX_BLOB_SIZE: usize = 1 << 30; // 1GB

/// Default memory limit for buffer manager (1GB)
pub const DEFAULT_MEMORY_LIMIT: usize = 1024 * 1024 * 1024;

/// Maximum threads for parallel execution
pub const MAX_THREADS: usize = 64;

/// File format version
pub const STORAGE_FORMAT_VERSION: u32 = 1;

/// Magic number for PrismDB files
pub const STORAGE_MAGIC: &[u8; 4] = b"PRSM";

/// Extension file suffix
pub const EXTENSION_SUFFIX: &str = ".prismdb_extension";

/// Default database file name
pub const DEFAULT_DATABASE_FILE: &str = "prismdb.db";

/// In-memory database identifier
pub const IN_MEMORY_DATABASE: &str = ":memory:";
