//! Memory allocation utilities

use crate::common::error::Result;
use std::alloc::{GlobalAlloc, Layout, System};
use std::ptr::NonNull;

/// Custom allocator for DuckDB memory management
pub struct DuckDBAllocator;

unsafe impl GlobalAlloc for DuckDBAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        System.alloc(layout)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        System.realloc(ptr, layout, new_size)
    }
}

/// Arena allocator for temporary allocations
pub struct ArenaAllocator {
    chunks: Vec<Vec<u8>>,
    current_chunk: usize,
    current_offset: usize,
    chunk_size: usize,
}

impl ArenaAllocator {
    pub fn new(chunk_size: usize) -> Self {
        Self {
            chunks: Vec::new(),
            current_chunk: 0,
            current_offset: 0,
            chunk_size,
        }
    }

    pub fn allocate(&mut self, size: usize, align: usize) -> Result<NonNull<u8>> {
        let aligned_size = (size + align - 1) & !(align - 1);

        if self.current_chunk >= self.chunks.len() {
            self.chunks
                .push(vec![0u8; self.chunk_size.max(aligned_size)]);
            self.current_chunk = self.chunks.len() - 1;
            self.current_offset = 0;
        }

        let chunk = &mut self.chunks[self.current_chunk];
        if self.current_offset + aligned_size > chunk.len() {
            // Need new chunk
            self.chunks
                .push(vec![0u8; self.chunk_size.max(aligned_size)]);
            self.current_chunk = self.chunks.len() - 1;
            self.current_offset = 0;
        }

        let chunk = &mut self.chunks[self.current_chunk];
        let ptr = unsafe {
            let base_ptr = chunk.as_mut_ptr().add(self.current_offset);
            NonNull::new_unchecked(base_ptr as *mut u8)
        };

        self.current_offset += aligned_size;
        Ok(ptr)
    }

    pub fn reset(&mut self) {
        self.current_chunk = 0;
        self.current_offset = 0;
        // Keep first chunk, drop others
        self.chunks.truncate(1);
        if !self.chunks.is_empty() {
            self.chunks[0].fill(0);
        }
    }

    pub fn used_memory(&self) -> usize {
        self.current_chunk * self.chunk_size + self.current_offset
    }
}

impl Default for ArenaAllocator {
    fn default() -> Self {
        Self::new(4096) // 4KB default chunk size
    }
}

/// Buffer pool for managing reusable memory buffers
pub struct BufferPool {
    buffers: Vec<Vec<u8>>,
    available: Vec<usize>,
    buffer_size: usize,
}

impl BufferPool {
    pub fn new(buffer_size: usize, initial_count: usize) -> Self {
        let mut buffers = Vec::with_capacity(initial_count);
        let mut available = Vec::with_capacity(initial_count);

        for _ in 0..initial_count {
            buffers.push(vec![0u8; buffer_size]);
            available.push(buffers.len() - 1);
        }

        Self {
            buffers,
            available,
            buffer_size,
        }
    }

    pub fn acquire(&mut self) -> Option<Vec<u8>> {
        if let Some(index) = self.available.pop() {
            let buffer = std::mem::replace(&mut self.buffers[index], Vec::new());
            Some(buffer)
        } else {
            let buffer = vec![0u8; self.buffer_size];
            self.buffers.push(buffer.clone());
            Some(buffer)
        }
    }

    pub fn release(&mut self, mut buffer: Vec<u8>) {
        if buffer.len() == self.buffer_size {
            buffer.clear();
            self.available.push(self.buffers.len());
            self.buffers.push(buffer);
        }
        // If buffer size doesn't match, just drop it
    }

    pub fn available_count(&self) -> usize {
        self.available.len()
    }

    pub fn total_count(&self) -> usize {
        self.buffers.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arena_allocator() {
        let mut arena = ArenaAllocator::new(1024);

        let ptr1 = arena.allocate(100, 8).unwrap();
        let ptr2 = arena.allocate(200, 8).unwrap();

        assert_ne!(ptr1.as_ptr(), ptr2.as_ptr());
        assert_eq!(arena.used_memory(), 304); // 104 + 200 (aligned)

        arena.reset();
        assert_eq!(arena.used_memory(), 0);
    }

    #[test]
    fn test_buffer_pool() {
        let mut pool = BufferPool::new(4096, 2);

        assert_eq!(pool.available_count(), 2);

        let buffer1 = pool.acquire().unwrap();
        assert_eq!(pool.available_count(), 1);

        let _buffer2 = pool.acquire().unwrap();
        assert_eq!(pool.available_count(), 0);

        let _buffer3 = pool.acquire().unwrap(); // Should create new buffer
        assert_eq!(pool.available_count(), 0);
        assert_eq!(pool.total_count(), 3);

        pool.release(buffer1);
        assert_eq!(pool.available_count(), 1);
    }
}
