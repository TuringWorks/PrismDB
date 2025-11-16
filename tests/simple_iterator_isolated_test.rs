// Simple test to isolate IteratorStream behavior
use std::vec::Vec;

// Mock the basic types we need
#[derive(Debug, Clone)]
struct MockDataChunk {
    rows: usize,
}

impl MockDataChunk {
    fn new(rows: usize) -> Self {
        Self { rows }
    }
    
    fn count(&self) -> usize {
        self.rows
    }
}

// Simple iterator stream implementation
struct SimpleIteratorStream {
    chunks: Vec<MockDataChunk>,
    index: usize,
}

impl SimpleIteratorStream {
    fn new(chunks: Vec<MockDataChunk>) -> Self {
        Self { chunks, index: 0 }
    }
}

impl Iterator for SimpleIteratorStream {
    type Item = MockDataChunk;
    
    fn next(&mut self) -> Option<Self::Item> {
        println!("SimpleIteratorStream::next() called - index: {}, chunks.len(): {}", self.index, self.chunks.len());
        if self.index < self.chunks.len() {
            let chunk = self.chunks[self.index].clone();
            self.index += 1;
            println!("SimpleIteratorStream::next() returning chunk with {} rows, new index: {}", chunk.count(), self.index);
            Some(chunk)
        } else {
            println!("SimpleIteratorStream::next() returning None - end of stream");
            None
        }
    }
}

#[test]
fn test_simple_iterator() {
    let chunks = vec![
        MockDataChunk::new(2),
        MockDataChunk::new(3),
    ];
    
    let mut stream = SimpleIteratorStream::new(chunks);
    let mut count = 0;
    
    for chunk in &mut stream {
        count += 1;
        println!("Got chunk {} with {} rows", count, chunk.count());
        
        if count > 5 {
            println!("Breaking to prevent infinite loop");
            break;
        }
    }
    
    println!("Iterator completed after {} chunks", count);
    assert_eq!(count, 2, "Should have exactly 2 chunks");
}