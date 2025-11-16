use crate::common::error::{PrismDBError, PrismDBResult};
use crate::types::logical_type::LogicalType;
use crate::types::value::Value;
use crate::types::vector::Vector;
use std::fmt;

/// A DataChunk represents a collection of vectors (columns) for batch processing
/// This is the fundamental unit of data processing in DuckDB
#[derive(Debug, Clone)]
pub struct DataChunk {
    /// The vectors (columns) in this chunk
    vectors: Vec<Vector>,
    /// The number of rows in this chunk
    count: usize,
    /// The capacity of this chunk
    capacity: usize,
}

impl DataChunk {
    /// Create a new empty data chunk
    pub fn new() -> Self {
        Self {
            vectors: Vec::new(),
            count: 0,
            capacity: 0,
        }
    }

    /// Create a new data chunk with specified number of rows
    pub fn with_rows(row_count: usize) -> Self {
        Self {
            vectors: Vec::new(),
            count: row_count,
            capacity: row_count,
        }
    }

    /// Create a new data chunk with specified capacity (for backward compatibility)
    pub fn new_with_capacity(capacity: usize) -> Self {
        Self {
            vectors: Vec::new(),
            count: 0,
            capacity,
        }
    }

    /// Create a new data chunk with the specified types and capacity
    pub fn with_capacity(types: Vec<LogicalType>, capacity: usize) -> Self {
        let vectors: Vec<Vector> = types
            .into_iter()
            .map(|logical_type| Vector::new(logical_type, capacity))
            .collect();

        Self {
            vectors,
            count: 0,
            capacity,
        }
    }

    /// Create a data chunk from a slice of vectors
    pub fn from_vectors(vectors: Vec<Vector>) -> PrismDBResult<Self> {
        if vectors.is_empty() {
            return Ok(Self::new());
        }

        // All vectors should have the same count
        let count = vectors[0].count();
        for (i, vector) in vectors.iter().enumerate() {
            if vector.count() != count {
                return Err(PrismDBError::InvalidValue(format!(
                    "Vector {} has count {}, expected {}",
                    i,
                    vector.count(),
                    count
                )));
            }
        }

        let capacity = vectors.iter().map(|v| v.capacity()).max().unwrap_or(0);

        Ok(Self {
            vectors,
            count,
            capacity,
        })
    }

    /// Get the number of vectors (columns) in this chunk
    pub fn column_count(&self) -> usize {
        self.vectors.len()
    }

    /// Get the number of rows in this chunk
    pub fn count(&self) -> usize {
        self.count
    }

    /// Get the number of rows in this chunk (alias for count)
    pub fn len(&self) -> usize {
        self.count
    }

    /// Get the capacity of this chunk
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Check if this chunk is empty
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Get a reference to a vector at the specified column index
    pub fn get_vector(&self, column_index: usize) -> Option<&Vector> {
        self.vectors.get(column_index)
    }

    /// Get a mutable reference to a vector at the specified column index
    pub fn get_vector_mut(&mut self, column_index: usize) -> Option<&mut Vector> {
        self.vectors.get_mut(column_index)
    }

    /// Get all vectors in this chunk
    pub fn get_vectors(&self) -> &[Vector] {
        &self.vectors
    }

    /// Get mutable references to all vectors in this chunk
    pub fn get_vectors_mut(&mut self) -> &mut [Vector] {
        &mut self.vectors
    }

    /// Set a vector at the specified column index
    pub fn set_vector(&mut self, column_index: usize, vector: Vector) -> PrismDBResult<()> {
        if column_index >= self.vectors.len() {
            self.vectors
                .resize(column_index + 1, Vector::new(LogicalType::Integer, 0));
        }
        self.vectors[column_index] = vector;
        Ok(())
    }

    /// Get the types of all vectors in this chunk
    pub fn get_types(&self) -> Vec<LogicalType> {
        self.vectors.iter().map(|v| v.get_type().clone()).collect()
    }

    /// Add a new vector to this chunk
    pub fn add_vector(&mut self, vector: Vector) -> PrismDBResult<()> {
        if self.vectors.is_empty() {
            self.count = vector.count();
            self.capacity = vector.capacity();
        } else if vector.count() != self.count {
            return Err(PrismDBError::InvalidValue(format!(
                "Vector has count {}, expected {}",
                vector.count(),
                self.count
            )));
        }

        self.vectors.push(vector);
        Ok(())
    }

    /// Remove a vector from this chunk
    pub fn remove_vector(&mut self, column_index: usize) -> Option<Vector> {
        if column_index < self.vectors.len() {
            Some(self.vectors.remove(column_index))
        } else {
            None
        }
    }

    /// Resize this chunk to the specified number of rows
    pub fn resize(&mut self, new_count: usize) -> PrismDBResult<()> {
        if new_count > self.capacity {
            self.reserve(new_count)?;
        }

        for vector in &mut self.vectors {
            vector.resize(new_count)?;
        }

        self.count = new_count;
        Ok(())
    }

    /// Reserve capacity for additional rows
    pub fn reserve(&mut self, new_capacity: usize) -> PrismDBResult<()> {
        if new_capacity <= self.capacity {
            return Ok(());
        }

        for vector in &mut self.vectors {
            vector.reserve(new_capacity)?;
        }

        self.capacity = new_capacity;
        Ok(())
    }

    /// Append a row to this chunk
    pub fn append_row(&mut self, values: Vec<Value>) -> PrismDBResult<()> {
        if values.len() != self.vectors.len() {
            return Err(PrismDBError::InvalidValue(format!(
                "Expected {} values, got {}",
                self.vectors.len(),
                values.len()
            )));
        }

        if self.count >= self.capacity {
            self.reserve(if self.capacity == 0 {
                1024
            } else {
                self.capacity * 2
            })?;
        }

        for (i, value) in values.iter().enumerate() {
            self.vectors[i].push(value)?;
        }

        self.count += 1;
        Ok(())
    }

    /// Append another data chunk to this one
    pub fn append_chunk(&mut self, other: &DataChunk) -> PrismDBResult<()> {
        if self.vectors.len() != other.vectors.len() {
            return Err(PrismDBError::InvalidValue(format!(
                "Chunk column count mismatch: {} vs {}",
                self.vectors.len(),
                other.vectors.len()
            )));
        }

        // Verify type compatibility
        for (i, (self_vector, other_vector)) in
            self.vectors.iter().zip(other.vectors.iter()).enumerate()
        {
            if self_vector.get_type() != other_vector.get_type() {
                return Err(PrismDBError::InvalidType(format!(
                    "Column {} type mismatch: {} vs {}",
                    i,
                    self_vector.get_type(),
                    other_vector.get_type()
                )));
            }
        }

        // Reserve space if needed
        let new_count = self.count + other.count();
        if new_count > self.capacity {
            self.reserve(new_count)?;
        }

        // Copy data from other chunk
        for (self_vector, other_vector) in self.vectors.iter_mut().zip(other.vectors.iter()) {
            for row in 0..other.count() {
                let value = other_vector.get_value(row)?;
                self_vector.push(&value)?;
            }
        }

        self.count = new_count;
        Ok(())
    }

    /// Get a value at a specific row and column
    pub fn get_value(&self, row: usize, column: usize) -> PrismDBResult<Value> {
        if row >= self.count {
            return Err(PrismDBError::InvalidValue(format!(
                "Row index {} out of bounds (count: {})",
                row, self.count
            )));
        }

        if column >= self.vectors.len() {
            return Err(PrismDBError::InvalidValue(format!(
                "Column index {} out of bounds (columns: {})",
                column,
                self.vectors.len()
            )));
        }

        self.vectors[column].get_value(row)
    }

    /// Set a value at a specific row and column
    pub fn set_value(&mut self, row: usize, column: usize, value: &Value) -> PrismDBResult<()> {
        if row >= self.capacity {
            return Err(PrismDBError::InvalidValue(format!(
                "Row index {} out of bounds (capacity: {})",
                row, self.capacity
            )));
        }

        if column >= self.vectors.len() {
            return Err(PrismDBError::InvalidValue(format!(
                "Column index {} out of bounds (columns: {})",
                column,
                self.vectors.len()
            )));
        }

        self.vectors[column].set_value(row, value)?;

        // Update count if this is a new row
        if row >= self.count {
            self.count = row + 1;
        }

        Ok(())
    }

    /// Check if a value at a specific position is null
    pub fn is_null(&self, row: usize, column: usize) -> bool {
        if row >= self.count || column >= self.vectors.len() {
            return true;
        }
        self.vectors[column].is_null(row)
    }

    /// Check if a value at a specific position is valid
    pub fn is_valid(&self, row: usize, column: usize) -> bool {
        if row >= self.count || column >= self.vectors.len() {
            return false;
        }
        self.vectors[column].is_valid(row)
    }

    /// Get the number of null values in a specific column
    pub fn null_count(&self, column: usize) -> usize {
        if column >= self.vectors.len() {
            return 0;
        }
        self.vectors[column].null_count()
    }

    /// Get the number of valid values in a specific column
    pub fn valid_count(&self, column: usize) -> usize {
        if column >= self.vectors.len() {
            return 0;
        }
        self.vectors[column].valid_count()
    }

    /// Clear all data from this chunk
    pub fn clear(&mut self) {
        for vector in &mut self.vectors {
            vector.clear();
        }
        self.count = 0;
    }

    /// Reset this chunk to be empty but keep the structure
    pub fn reset(&mut self) {
        self.clear();
    }

    /// Create a new chunk with the same structure but no data
    pub fn clone_empty(&self) -> Self {
        let types = self.get_types();
        Self::with_capacity(types, self.capacity)
    }

    /// Slice this chunk to get a range of rows [start, start+len)
    pub fn slice_range(&self, start: usize, len: usize) -> PrismDBResult<DataChunk> {
        if start >= self.count {
            return Ok(DataChunk::new());
        }

        let end = (start + len).min(self.count);
        let actual_len = end - start;

        if actual_len == 0 {
            return Ok(DataChunk::new());
        }

        let mut sliced_vectors = Vec::new();
        for vector in &self.vectors {
            let mut new_vector = Vector::new(vector.get_type().clone(), actual_len);
            for i in 0..actual_len {
                let value = vector.get_value(start + i)?;
                new_vector.push(&value)?;
            }
            sliced_vectors.push(new_vector);
        }

        DataChunk::from_vectors(sliced_vectors)
    }

    /// Slice this chunk using a SelectionVector (DuckDB-faithful)
    /// This is the zero-copy filtering mechanism - creates a new chunk
    /// with only the selected rows
    pub fn slice(&self, selection: &crate::types::SelectionVector) -> PrismDBResult<DataChunk> {
        if selection.is_empty() {
            return Ok(DataChunk::new());
        }

        let mut sliced_vectors = Vec::new();
        for vector in &self.vectors {
            let mut new_vector = Vector::new(vector.get_type().clone(), selection.count());
            for i in 0..selection.count() {
                let row_index = selection.get_index(i);
                if row_index < self.count {
                    let value = vector.get_value(row_index)?;
                    new_vector.push(&value)?;
                }
            }
            sliced_vectors.push(new_vector);
        }

        DataChunk::from_vectors(sliced_vectors)
    }

    /// Filter this chunk based on a selection vector (slice of indices)
    pub fn filter(&self, selection: &[usize]) -> PrismDBResult<DataChunk> {
        let mut filtered_vectors = Vec::new();
        for vector in &self.vectors {
            let mut new_vector = Vector::new(vector.get_type().clone(), selection.len());
            for &row_index in selection {
                if row_index < self.count {
                    let value = vector.get_value(row_index)?;
                    new_vector.push(&value)?;
                }
            }
            filtered_vectors.push(new_vector);
        }

        DataChunk::from_vectors(filtered_vectors)
    }

    /// Get an iterator over rows in this chunk
    pub fn row_iter(&self) -> RowIterator<'_> {
        RowIterator {
            chunk: self,
            row: 0,
        }
    }

    /// Get an iterator over columns in this chunk
    pub fn column_iter(&self) -> ColumnIterator<'_> {
        ColumnIterator {
            chunk: self,
            column: 0,
        }
    }

    /// Convert this chunk to a vector of rows (each row is a vector of values)
    pub fn to_rows(&self) -> PrismDBResult<Vec<Vec<Value>>> {
        let mut rows = Vec::with_capacity(self.count);
        for row in 0..self.count {
            let mut row_values = Vec::with_capacity(self.vectors.len());
            for col in 0..self.vectors.len() {
                row_values.push(self.get_value(row, col)?);
            }
            rows.push(row_values);
        }
        Ok(rows)
    }

    /// Convert this chunk to a vector of columns (each column is a vector of values)
    pub fn to_columns(&self) -> PrismDBResult<Vec<Vec<Value>>> {
        let mut columns = Vec::with_capacity(self.vectors.len());
        for col in 0..self.vectors.len() {
            let mut column_values = Vec::with_capacity(self.count);
            for row in 0..self.count {
                column_values.push(self.get_value(row, col)?);
            }
            columns.push(column_values);
        }
        Ok(columns)
    }

    /// Get the total size of this chunk in bytes (approximate)
    pub fn get_size(&self) -> usize {
        self.vectors
            .iter()
            .map(|v| v.count() * v.get_type().get_max_size().unwrap_or(8))
            .sum()
    }

    /// Verify the integrity of this chunk
    pub fn verify(&self) -> PrismDBResult<()> {
        // Check that all vectors have the same count
        for (i, vector) in self.vectors.iter().enumerate() {
            if vector.count() != self.count {
                return Err(PrismDBError::InvalidValue(format!(
                    "Vector {} has count {}, expected {}",
                    i,
                    vector.count(),
                    self.count
                )));
            }
        }

        // Check that count doesn't exceed capacity
        if self.count > self.capacity {
            return Err(PrismDBError::InvalidValue(format!(
                "Count {} exceeds capacity {}",
                self.count, self.capacity
            )));
        }

        Ok(())
    }
}

impl Default for DataChunk {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for DataChunk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "DataChunk {{")?;
        writeln!(f, "  rows: {}, columns: {}", self.count, self.vectors.len())?;

        for (i, vector) in self.vectors.iter().enumerate() {
            writeln!(
                f,
                "  column {}: {} ({} rows)",
                i,
                vector.get_type(),
                vector.count()
            )?;
        }

        write!(f, "}}")
    }
}

/// Iterator for rows in a DataChunk
pub struct RowIterator<'a> {
    chunk: &'a DataChunk,
    row: usize,
}

impl<'a> Iterator for RowIterator<'a> {
    type Item = PrismDBResult<Vec<Value>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.row >= self.chunk.count {
            None
        } else {
            let mut row_values = Vec::with_capacity(self.chunk.vectors.len());
            for col in 0..self.chunk.vectors.len() {
                match self.chunk.get_value(self.row, col) {
                    Ok(value) => row_values.push(value),
                    Err(e) => return Some(Err(e)),
                }
            }
            self.row += 1;
            Some(Ok(row_values))
        }
    }
}

/// Iterator for columns in a DataChunk
pub struct ColumnIterator<'a> {
    chunk: &'a DataChunk,
    column: usize,
}

impl<'a> Iterator for ColumnIterator<'a> {
    type Item = &'a Vector;

    fn next(&mut self) -> Option<Self::Item> {
        if self.column >= self.chunk.vectors.len() {
            None
        } else {
            let column = &self.chunk.vectors[self.column];
            self.column += 1;
            Some(column)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_chunk_creation() {
        let types = vec![
            LogicalType::Integer,
            LogicalType::Varchar,
            LogicalType::Boolean,
        ];
        let chunk = DataChunk::with_capacity(types, 100);

        assert_eq!(chunk.column_count(), 3);
        assert_eq!(chunk.capacity(), 100);
        assert_eq!(chunk.count(), 0);
        assert!(chunk.is_empty());
    }

    #[test]
    fn test_data_chunk_from_vectors() -> PrismDBResult<()> {
        let int_vector = Vector::from_values(&[Value::integer(1), Value::integer(2)])?;
        let str_vector = Vector::from_values(&[
            Value::varchar("a".to_string()),
            Value::varchar("b".to_string()),
        ])?;

        let chunk = DataChunk::from_vectors(vec![int_vector, str_vector])?;

        assert_eq!(chunk.column_count(), 2);
        assert_eq!(chunk.count(), 2);
        assert_eq!(chunk.get_value(0, 0)?, Value::integer(1));
        assert_eq!(chunk.get_value(1, 1)?, Value::varchar("b".to_string()));

        Ok(())
    }

    #[test]
    fn test_data_chunk_append_row() -> PrismDBResult<()> {
        let types = vec![LogicalType::Integer, LogicalType::Boolean];
        let mut chunk = DataChunk::with_capacity(types, 10);

        chunk.append_row(vec![Value::integer(1), Value::boolean(true)])?;
        chunk.append_row(vec![Value::integer(2), Value::boolean(false)])?;

        assert_eq!(chunk.count(), 2);
        assert_eq!(chunk.get_value(0, 0)?, Value::integer(1));
        assert_eq!(chunk.get_value(1, 1)?, Value::boolean(false));

        Ok(())
    }

    #[test]
    fn test_data_chunk_slice() -> PrismDBResult<()> {
        let values = vec![
            Value::integer(1),
            Value::integer(2),
            Value::integer(3),
            Value::integer(4),
        ];
        let vector = Vector::from_values(&values)?;
        let chunk = DataChunk::from_vectors(vec![vector])?;

        let sliced = chunk.slice_range(1, 2)?;

        assert_eq!(sliced.count(), 2);
        assert_eq!(sliced.get_value(0, 0)?, Value::integer(2));
        assert_eq!(sliced.get_value(1, 0)?, Value::integer(3));

        Ok(())
    }

    #[test]
    fn test_data_chunk_filter() -> PrismDBResult<()> {
        let values = vec![
            Value::integer(1),
            Value::integer(2),
            Value::integer(3),
            Value::integer(4),
        ];
        let vector = Vector::from_values(&values)?;
        let chunk = DataChunk::from_vectors(vec![vector])?;

        let selection = vec![0, 2, 3]; // Select rows 0, 2, 3
        let filtered = chunk.filter(&selection)?;

        assert_eq!(filtered.count(), 3);
        assert_eq!(filtered.get_value(0, 0)?, Value::integer(1));
        assert_eq!(filtered.get_value(1, 0)?, Value::integer(3));
        assert_eq!(filtered.get_value(2, 0)?, Value::integer(4));

        Ok(())
    }

    #[test]
    fn test_data_chunk_iterators() -> PrismDBResult<()> {
        let chunk = DataChunk::from_vectors(vec![
            Vector::from_values(&[Value::integer(1), Value::integer(2)])?,
            Vector::from_values(&[Value::boolean(true), Value::boolean(false)])?,
        ])?;

        // Test row iterator
        let rows: PrismDBResult<Vec<Vec<Value>>> = chunk.row_iter().collect();
        let rows = rows?;
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], vec![Value::integer(1), Value::boolean(true)]);
        assert_eq!(rows[1], vec![Value::integer(2), Value::boolean(false)]);

        // Test column iterator
        let columns: Vec<&Vector> = chunk.column_iter().collect();
        assert_eq!(columns.len(), 2);

        Ok(())
    }

    #[test]
    fn test_data_chunk_null_handling() -> PrismDBResult<()> {
        let values = vec![Value::integer(1), Value::Null, Value::integer(3)];
        let vector = Vector::from_values(&values)?;
        let chunk = DataChunk::from_vectors(vec![vector])?;

        assert_eq!(chunk.null_count(0), 1);
        assert_eq!(chunk.valid_count(0), 2);
        assert!(chunk.is_null(1, 0));
        assert!(!chunk.is_null(0, 0));
        assert!(chunk.is_valid(0, 0));
        assert!(!chunk.is_valid(1, 0));

        Ok(())
    }
}
