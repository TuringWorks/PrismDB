//! Block Manager for DuckDB Storage
//!
//! This module provides block-based disk I/O following DuckDB's design:
//! - Fixed-size blocks (default 256KB)
//! - Block allocation and deallocation
//! - Reading and writing blocks to disk
//! - Free list management

use crate::common::error::{PrismDBError, PrismDBResult};
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

/// Block size (256KB like DuckDB)
pub const BLOCK_SIZE: usize = 262144;

/// Block ID type
pub type BlockId = u64;

/// Block header for metadata
#[derive(Debug, Clone)]
pub struct BlockHeader {
    /// Block ID
    pub block_id: BlockId,
    /// Block type
    pub block_type: BlockType,
    /// Number of rows in this block (for data blocks)
    pub row_count: usize,
    /// Next block ID (for linked blocks)
    pub next_block_id: Option<BlockId>,
}

/// Types of blocks
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockType {
    /// Free block
    Free,
    /// Data block (table data)
    Data,
    /// Index block
    Index,
    /// Metadata block
    Metadata,
    /// Overflow block (for large values)
    Overflow,
}

impl BlockHeader {
    pub fn new(block_id: BlockId, block_type: BlockType) -> Self {
        Self {
            block_id,
            block_type,
            row_count: 0,
            next_block_id: None,
        }
    }

    /// Serialize header to bytes (first 64 bytes of block)
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(64);
        bytes.extend_from_slice(&self.block_id.to_le_bytes());
        bytes.push(self.block_type as u8);
        bytes.extend_from_slice(&self.row_count.to_le_bytes());
        bytes.extend_from_slice(&self.next_block_id.unwrap_or(0).to_le_bytes());
        bytes.resize(64, 0); // Pad to 64 bytes
        bytes
    }

    /// Deserialize header from bytes
    pub fn from_bytes(bytes: &[u8]) -> PrismDBResult<Self> {
        if bytes.len() < 64 {
            return Err(PrismDBError::Storage(
                "Invalid block header size".to_string(),
            ));
        }

        let block_id = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let block_type = match bytes[8] {
            0 => BlockType::Free,
            1 => BlockType::Data,
            2 => BlockType::Index,
            3 => BlockType::Metadata,
            4 => BlockType::Overflow,
            _ => return Err(PrismDBError::Storage("Invalid block type".to_string())),
        };
        let row_count = usize::from_le_bytes(bytes[9..17].try_into().unwrap());
        let next_block_id_raw = u64::from_le_bytes(bytes[17..25].try_into().unwrap());
        let next_block_id = if next_block_id_raw == 0 {
            None
        } else {
            Some(next_block_id_raw)
        };

        Ok(Self {
            block_id,
            block_type,
            row_count,
            next_block_id,
        })
    }
}

/// Block data
#[derive(Debug, Clone)]
pub struct Block {
    /// Block header
    pub header: BlockHeader,
    /// Block data (excluding header)
    pub data: Vec<u8>,
}

impl Block {
    pub fn new(block_id: BlockId, block_type: BlockType) -> Self {
        Self {
            header: BlockHeader::new(block_id, block_type),
            data: vec![0u8; BLOCK_SIZE - 64], // Reserve 64 bytes for header
        }
    }

    /// Serialize block to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = self.header.to_bytes();
        bytes.extend_from_slice(&self.data);
        bytes.resize(BLOCK_SIZE, 0); // Ensure exact block size
        bytes
    }

    /// Deserialize block from bytes
    pub fn from_bytes(bytes: &[u8]) -> PrismDBResult<Self> {
        if bytes.len() != BLOCK_SIZE {
            return Err(PrismDBError::Storage(format!(
                "Invalid block size: expected {}, got {}",
                BLOCK_SIZE,
                bytes.len()
            )));
        }

        let header = BlockHeader::from_bytes(&bytes[0..64])?;
        let data = bytes[64..].to_vec();

        Ok(Self { header, data })
    }
}

/// Block manager for disk I/O
pub struct BlockManager {
    /// Database file path
    file_path: PathBuf,
    /// File handle
    file: Arc<RwLock<File>>,
    /// Free list (available block IDs)
    free_list: Arc<RwLock<HashSet<BlockId>>>,
    /// Next block ID
    next_block_id: Arc<RwLock<BlockId>>,
    /// Total number of blocks
    total_blocks: Arc<RwLock<u64>>,
}

impl BlockManager {
    /// Create a new block manager
    pub fn new<P: AsRef<Path>>(file_path: P) -> PrismDBResult<Self> {
        let file_path = file_path.as_ref().to_path_buf();

        // Create parent directory if it doesn't exist
        if let Some(parent) = file_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| PrismDBError::Storage(format!("Failed to create directory: {}", e)))?;
        }

        // Open or create the file
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&file_path)
            .map_err(|e| PrismDBError::Storage(format!("Failed to open database file: {}", e)))?;

        // Get file size to determine number of blocks
        let file_size = file
            .metadata()
            .map_err(|e| PrismDBError::Storage(format!("Failed to get file metadata: {}", e)))?
            .len();
        let total_blocks = file_size / BLOCK_SIZE as u64;

        Ok(Self {
            file_path,
            file: Arc::new(RwLock::new(file)),
            free_list: Arc::new(RwLock::new(HashSet::new())),
            next_block_id: Arc::new(RwLock::new(total_blocks)),
            total_blocks: Arc::new(RwLock::new(total_blocks)),
        })
    }

    /// Allocate a new block
    pub fn allocate_block(&self, block_type: BlockType) -> PrismDBResult<BlockId> {
        // Try to reuse a free block first
        let mut free_list = self.free_list.write().unwrap();
        if let Some(&block_id) = free_list.iter().next() {
            free_list.remove(&block_id);
            return Ok(block_id);
        }
        drop(free_list);

        // Allocate a new block
        let mut next_id = self.next_block_id.write().unwrap();
        let block_id = *next_id;
        *next_id += 1;

        let mut total = self.total_blocks.write().unwrap();
        *total += 1;

        // Initialize the block
        let block = Block::new(block_id, block_type);
        self.write_block(block_id, &block)?;

        Ok(block_id)
    }

    /// Free a block
    pub fn free_block(&self, block_id: BlockId) -> PrismDBResult<()> {
        let mut free_list = self.free_list.write().unwrap();
        free_list.insert(block_id);
        Ok(())
    }

    /// Read a block from disk
    pub fn read_block(&self, block_id: BlockId) -> PrismDBResult<Block> {
        let mut file = self.file.write().unwrap();

        // Seek to block position
        let offset = block_id * BLOCK_SIZE as u64;
        file.seek(SeekFrom::Start(offset)).map_err(|e| {
            PrismDBError::Storage(format!("Failed to seek to block {}: {}", block_id, e))
        })?;

        // Read block data
        let mut buffer = vec![0u8; BLOCK_SIZE];
        file.read_exact(&mut buffer).map_err(|e| {
            PrismDBError::Storage(format!("Failed to read block {}: {}", block_id, e))
        })?;

        Block::from_bytes(&buffer)
    }

    /// Write a block to disk
    pub fn write_block(&self, block_id: BlockId, block: &Block) -> PrismDBResult<()> {
        let mut file = self.file.write().unwrap();

        // Seek to block position
        let offset = block_id * BLOCK_SIZE as u64;
        file.seek(SeekFrom::Start(offset)).map_err(|e| {
            PrismDBError::Storage(format!("Failed to seek to block {}: {}", block_id, e))
        })?;

        // Write block data
        let bytes = block.to_bytes();
        file.write_all(&bytes).map_err(|e| {
            PrismDBError::Storage(format!("Failed to write block {}: {}", block_id, e))
        })?;

        // Flush to ensure data is written
        file.flush().map_err(|e| {
            PrismDBError::Storage(format!("Failed to flush block {}: {}", block_id, e))
        })?;

        Ok(())
    }

    /// Get total number of blocks
    pub fn get_total_blocks(&self) -> u64 {
        *self.total_blocks.read().unwrap()
    }

    /// Get file path
    pub fn get_file_path(&self) -> &Path {
        &self.file_path
    }

    /// Sync all data to disk
    pub fn sync(&self) -> PrismDBResult<()> {
        let file = self.file.write().unwrap();
        file.sync_all()
            .map_err(|e| PrismDBError::Storage(format!("Failed to sync database file: {}", e)))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_block_header_serialization() {
        let mut header = BlockHeader::new(42, BlockType::Data);
        header.row_count = 100;
        header.next_block_id = Some(43);

        let bytes = header.to_bytes();
        let deserialized = BlockHeader::from_bytes(&bytes).unwrap();

        assert_eq!(header.block_id, deserialized.block_id);
        assert_eq!(header.block_type, deserialized.block_type);
        assert_eq!(header.row_count, deserialized.row_count);
        assert_eq!(header.next_block_id, deserialized.next_block_id);
    }

    #[test]
    fn test_block_manager_basic() -> PrismDBResult<()> {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        let manager = BlockManager::new(&db_path)?;

        // Allocate a block
        let block_id = manager.allocate_block(BlockType::Data)?;
        assert_eq!(block_id, 0);

        // Write data to block
        let mut block = Block::new(block_id, BlockType::Data);
        block.data[0..10].copy_from_slice(b"test data!");
        manager.write_block(block_id, &block)?;

        // Read block back
        let read_block = manager.read_block(block_id)?;
        assert_eq!(&read_block.data[0..10], b"test data!");

        Ok(())
    }

    #[test]
    fn test_block_manager_free_reuse() -> PrismDBResult<()> {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        let manager = BlockManager::new(&db_path)?;

        // Allocate two blocks
        let block_id_1 = manager.allocate_block(BlockType::Data)?;
        let _block_id_2 = manager.allocate_block(BlockType::Data)?;

        // Free the first block
        manager.free_block(block_id_1)?;

        // Allocate another block - should reuse freed block
        let block_id_3 = manager.allocate_block(BlockType::Data)?;
        assert_eq!(block_id_3, block_id_1);

        Ok(())
    }
}
