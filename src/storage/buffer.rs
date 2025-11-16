//! Buffer management for DuckDB storage engine
//!
//! This module provides:
//! - Memory buffer pool management
//! - Page allocation and deallocation
//! - Buffer caching strategies
//! - Memory usage tracking

use crate::common::error::{PrismDBError, PrismDBResult};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

/// Buffer configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BufferConfig {
    /// Page size in bytes
    pub page_size: usize,
    /// Maximum memory usage in bytes
    pub max_memory: usize,
    /// Buffer pool size
    pub pool_size: usize,
    /// Whether to use direct I/O
    pub use_direct_io: bool,
}

impl BufferConfig {
    pub fn new(max_memory: usize, pool_size: usize) -> Self {
        Self {
            page_size: 4096, // 4KB pages
            max_memory,
            pool_size,
            use_direct_io: false,
        }
    }
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            page_size: 4096,                // 4KB pages
            max_memory: 1024 * 1024 * 1024, // 1GB
            pool_size: 1000,
            use_direct_io: false,
        }
    }
}

/// Memory buffer
#[derive(Debug, Clone)]
pub struct MemoryBuffer {
    /// Buffer data
    pub data: Vec<u8>,
    /// Current position
    pub position: usize,
    /// Capacity
    pub capacity: usize,
    /// Whether buffer is dirty
    pub is_dirty: bool,
}

impl MemoryBuffer {
    /// Create a new memory buffer
    pub fn new(capacity: usize) -> Self {
        Self {
            data: vec![0u8; capacity],
            position: 0,
            capacity,
            is_dirty: false,
        }
    }

    /// Get buffer size
    pub fn len(&self) -> usize {
        self.position
    }

    /// Get buffer capacity
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Check if buffer is empty
    pub fn is_empty(&self) -> bool {
        self.position == 0
    }

    /// Check if buffer is full
    pub fn is_full(&self) -> bool {
        self.position >= self.capacity
    }

    /// Write data to buffer
    pub fn write(&mut self, data: &[u8]) -> PrismDBResult<usize> {
        if self.position + data.len() > self.capacity {
            return Err(PrismDBError::InvalidValue("Buffer overflow".to_string()));
        }

        let write_start = self.position;
        self.data[write_start..write_start + data.len()].copy_from_slice(data);
        self.position += data.len();
        self.is_dirty = true;

        Ok(data.len())
    }

    /// Read data from buffer
    pub fn read(&self, offset: usize, length: usize) -> PrismDBResult<&[u8]> {
        if offset + length > self.position {
            return Err(PrismDBError::InvalidValue(format!(
                "Read out of bounds: offset={}, length={}, position={}",
                offset, length, self.position
            )));
        }

        Ok(&self.data[offset..offset + length])
    }

    /// Clear the buffer
    pub fn clear(&mut self) {
        self.position = 0;
        self.is_dirty = false;
        for byte in self.data.iter_mut() {
            *byte = 0;
        }
    }

    /// Reset buffer position
    pub fn reset(&mut self) {
        self.position = 0;
    }

    /// Mark buffer as dirty
    pub fn mark_dirty(&mut self) {
        self.is_dirty = true;
    }

    /// Mark buffer as clean
    pub fn mark_clean(&mut self) {
        self.is_dirty = false;
    }

    /// Get remaining capacity
    pub fn remaining_capacity(&self) -> usize {
        self.capacity - self.position
    }

    /// Get slice of written data
    pub fn as_slice(&self) -> &[u8] {
        &self.data[..self.position]
    }

    /// Get mutable slice of buffer
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.data[..self.capacity]
    }
}

/// Buffer pool for managing memory buffers
#[derive(Debug)]
pub struct BufferPool {
    /// Available buffers
    available: VecDeque<MemoryBuffer>,
    /// Used buffers
    used: Vec<MemoryBuffer>,
    /// Configuration
    config: BufferConfig,
    /// Total allocated memory
    total_allocated: usize,
}

impl BufferPool {
    /// Create a new buffer pool
    pub fn new(config: BufferConfig) -> Self {
        Self {
            available: VecDeque::with_capacity(config.pool_size),
            used: Vec::with_capacity(config.pool_size),
            config,
            total_allocated: 0,
        }
    }

    /// Get a buffer from the pool
    pub fn get_buffer(&mut self, size: usize) -> PrismDBResult<MemoryBuffer> {
        // Check if we have an available buffer of sufficient size
        if let Some(mut buffer) = self
            .available
            .iter()
            .position(|b| b.capacity() >= size)
            .map(|pos| self.available.remove(pos).unwrap())
        {
            // Clear and reuse the buffer
            buffer.clear();
            self.used.push(buffer.clone());
            Ok(buffer)
        } else {
            // Create a new buffer
            self.allocate_new_buffer(size)
        }
    }

    /// Return a buffer to the pool
    pub fn return_buffer(&mut self, _buffer: MemoryBuffer) {
        // For now, just decrement used count
        // In a real implementation, we'd track buffers more carefully
        if !self.used.is_empty() {
            self.used.pop();
        }
    }

    /// Allocate a new buffer
    fn allocate_new_buffer(&mut self, size: usize) -> PrismDBResult<MemoryBuffer> {
        if self.total_allocated + size > self.config.max_memory {
            return Err(PrismDBError::InvalidValue(
                "Memory limit exceeded".to_string(),
            ));
        }

        let buffer = MemoryBuffer::new(size);
        self.total_allocated += buffer.capacity();
        self.used.push(buffer.clone());

        Ok(buffer)
    }

    /// Get memory usage statistics
    pub fn get_memory_usage(&self) -> MemoryUsage {
        MemoryUsage {
            total_allocated: self.total_allocated,
            used_buffers: self.used.len(),
            available_buffers: self.available.len(),
            total_buffers: self.used.len() + self.available.len(),
        }
    }

    /// Cleanup unused buffers
    pub fn cleanup(&mut self) {
        // Remove buffers that haven't been used recently
        // For now, just keep a minimum number of available buffers
        while self.available.len() > self.config.pool_size / 2 {
            self.available.pop_front();
        }
    }
}

/// Memory usage statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryUsage {
    /// Total allocated memory in bytes
    pub total_allocated: usize,
    /// Number of used buffers
    pub used_buffers: usize,
    /// Number of available buffers
    pub available_buffers: usize,
    /// Total number of buffers
    pub total_buffers: usize,
}

impl MemoryUsage {
    /// Get memory utilization ratio
    pub fn utilization(&self) -> f64 {
        if self.total_allocated == 0 {
            0.0
        } else {
            self.total_allocated as f64 / self.total_allocated as f64
        }
    }
}

/// Page buffer for disk I/O
#[derive(Debug, Clone)]
pub struct PageBuffer {
    /// Page data
    pub data: Vec<u8>,
    /// Page ID
    pub page_id: u64,
    /// Whether page is dirty
    pub is_dirty: bool,
    /// Page size
    pub page_size: usize,
}

impl PageBuffer {
    /// Create a new page buffer
    pub fn new(page_id: u64, page_size: usize) -> Self {
        Self {
            data: vec![0u8; page_size],
            page_id,
            is_dirty: false,
            page_size,
        }
    }

    /// Write data to page
    pub fn write(&mut self, offset: usize, data: &[u8]) -> PrismDBResult<()> {
        if offset + data.len() > self.page_size {
            return Err(PrismDBError::InvalidValue("Page overflow".to_string()));
        }

        self.data[offset..offset + data.len()].copy_from_slice(data);
        self.is_dirty = true;
        Ok(())
    }

    /// Read data from page
    pub fn read(&self, offset: usize, length: usize) -> PrismDBResult<&[u8]> {
        if offset + length > self.page_size {
            return Err(PrismDBError::InvalidValue("Read out of bounds".to_string()));
        }

        Ok(&self.data[offset..offset + length])
    }

    /// Clear the page
    pub fn clear(&mut self) {
        for byte in self.data.iter_mut() {
            *byte = 0;
        }
        self.is_dirty = false;
    }

    /// Get page data slice
    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }

    /// Get mutable page data slice
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.data
    }

    /// Mark page as dirty
    pub fn mark_dirty(&mut self) {
        self.is_dirty = true;
    }

    /// Mark page as clean
    pub fn mark_clean(&mut self) {
        self.is_dirty = false;
    }
}

/// Buffer manager for coordinating all buffer operations
#[derive(Debug)]
pub struct BufferManager {
    /// Buffer pool
    pool: Arc<Mutex<BufferPool>>,
    /// Configuration
    config: BufferConfig,
    /// Page cache
    page_cache: Arc<Mutex<std::collections::HashMap<u64, PageBuffer>>>,
}

impl BufferManager {
    /// Create a new buffer manager
    pub fn new(config: BufferConfig) -> Self {
        Self {
            pool: Arc::new(Mutex::new(BufferPool::new(config.clone()))),
            config,
            page_cache: Arc::new(Mutex::new(std::collections::HashMap::new())),
        }
    }

    /// Get a memory buffer
    pub fn get_memory_buffer(&self, size: usize) -> PrismDBResult<MemoryBuffer> {
        let mut pool = self.pool.lock().unwrap();
        pool.get_buffer(size)
    }

    /// Return a memory buffer
    pub fn return_memory_buffer(&self, buffer: MemoryBuffer) {
        let mut pool = self.pool.lock().unwrap();
        pool.return_buffer(buffer);
    }

    /// Get a page buffer
    pub fn get_page_buffer(&self, page_id: u64) -> PrismDBResult<PageBuffer> {
        let mut cache = self.page_cache.lock().unwrap();

        if let Some(page) = cache.get(&page_id) {
            Ok(page.clone())
        } else {
            let page = PageBuffer::new(page_id, self.config.page_size);
            cache.insert(page_id, page.clone());
            Ok(page)
        }
    }

    /// Flush dirty pages
    pub fn flush_dirty_pages(&self) -> PrismDBResult<Vec<u64>> {
        let cache = self.page_cache.lock().unwrap();
        let mut dirty_pages = Vec::new();

        for (page_id, page) in cache.iter() {
            if page.is_dirty {
                dirty_pages.push(*page_id);
            }
        }

        Ok(dirty_pages)
    }

    /// Get memory usage statistics
    pub fn get_memory_usage(&self) -> PrismDBResult<MemoryUsage> {
        let pool = self.pool.lock().unwrap();
        Ok(pool.get_memory_usage())
    }

    /// Cleanup unused resources
    pub fn cleanup(&mut self) -> PrismDBResult<()> {
        let mut pool = self.pool.lock().unwrap();
        pool.cleanup();

        let mut cache = self.page_cache.lock().unwrap();
        cache.clear();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_buffer() -> PrismDBResult<()> {
        let mut buffer = MemoryBuffer::new(100);

        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.capacity(), 100);
        assert!(buffer.is_empty());
        assert!(!buffer.is_full());

        let data = b"Hello, World!";
        let written = buffer.write(data)?;
        assert_eq!(written, 13);
        assert_eq!(buffer.len(), 13);
        assert!(!buffer.is_empty());

        let read_data = buffer.read(0, 13)?;
        assert_eq!(read_data, data);

        Ok(())
    }

    #[test]
    fn test_buffer_pool() -> PrismDBResult<()> {
        let config = BufferConfig::default();
        let mut pool = BufferPool::new(config);

        let buffer1 = pool.get_buffer(50)?;
        let _buffer2 = pool.get_buffer(75)?;

        assert_eq!(pool.get_memory_usage().used_buffers, 2);

        pool.return_buffer(buffer1);
        assert_eq!(pool.get_memory_usage().used_buffers, 1);

        Ok(())
    }

    #[test]
    fn test_page_buffer() -> PrismDBResult<()> {
        let mut page = PageBuffer::new(1, 4096);

        assert_eq!(page.page_id, 1);
        assert_eq!(page.page_size, 4096);
        assert!(!page.is_dirty);

        let data = b"test data";
        page.write(0, data)?;
        assert!(page.is_dirty);

        let read_data = page.read(0, data.len())?;
        assert_eq!(read_data, data);

        Ok(())
    }
}
