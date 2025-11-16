use crate::common::error::{PrismDBError, PrismDBResult};
use crate::types::logical_type::LogicalType;
use crate::types::physical_type::PhysicalType;
use crate::types::value::Value;
use serde::{Deserialize, Serialize};

/// A validity mask for tracking null values in a vector
/// Uses a bitset for efficient storage
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ValidityMask {
    /// Bitset where each bit represents whether the corresponding value is valid (1) or null (0)
    data: Vec<u64>,
    /// Number of entries in the mask
    count: usize,
}

impl ValidityMask {
    /// Create a new validity mask with the specified capacity
    pub fn new(count: usize) -> Self {
        let data_size = (count + 63) / 64; // Round up to 64-bit boundaries
        Self {
            data: vec![0u64; data_size],
            count,
        }
    }

    /// Create a validity mask where all entries are valid
    pub fn all_valid(count: usize) -> Self {
        let data_size = (count + 63) / 64;
        Self {
            data: vec![u64::MAX; data_size],
            count,
        }
    }

    /// Create a validity mask where all entries are null
    pub fn all_null(count: usize) -> Self {
        Self {
            data: vec![0u64; (count + 63) / 64],
            count,
        }
    }

    /// Set the validity of a specific entry
    pub fn set_valid(&mut self, index: usize, valid: bool) {
        if index >= self.count {
            return;
        }
        let word_index = index / 64;
        let bit_index = index % 64;

        if valid {
            self.data[word_index] |= 1u64 << bit_index;
        } else {
            self.data[word_index] &= !(1u64 << bit_index);
        }
    }

    /// Set a specific entry as invalid (null)
    pub fn set_invalid(&mut self, index: usize) {
        self.set_valid(index, false);
    }

    /// Check if a specific entry is valid
    pub fn is_valid(&self, index: usize) -> bool {
        if index >= self.count {
            return false;
        }
        let word_index = index / 64;
        let bit_index = index % 64;
        (self.data[word_index] & (1u64 << bit_index)) != 0
    }

    /// Check if a specific entry is null
    pub fn is_null(&self, index: usize) -> bool {
        !self.is_valid(index)
    }

    /// Get the number of entries in the mask
    pub fn count(&self) -> usize {
        self.count
    }

    /// Count the number of valid entries
    pub fn valid_count(&self) -> usize {
        (0..self.count).filter(|&i| self.is_valid(i)).count()
    }

    /// Count the number of null entries
    pub fn null_count(&self) -> usize {
        self.count - self.valid_count()
    }

    /// Resize the validity mask
    pub fn resize(&mut self, new_count: usize) {
        let new_data_size = (new_count + 63) / 64;
        self.data.resize(new_data_size, 0);
        self.count = new_count;
    }

    /// Get an iterator over the validity bits
    pub fn iter(&self) -> ValidityIterator<'_> {
        ValidityIterator {
            mask: self,
            index: 0,
        }
    }
}

/// Iterator for validity mask
pub struct ValidityIterator<'a> {
    mask: &'a ValidityMask,
    index: usize,
}

impl<'a> Iterator for ValidityIterator<'a> {
    type Item = bool;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.mask.count {
            None
        } else {
            let result = self.mask.is_valid(self.index);
            self.index += 1;
            Some(result)
        }
    }
}

/// A selection vector for filtering and reordering data
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SelectionVector {
    /// Indices into the data
    data: Vec<usize>,
    /// Number of valid entries
    count: usize,
}

impl SelectionVector {
    /// Create a new empty selection vector with capacity
    pub fn new(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
            count: 0,
        }
    }

    /// Create a selection vector with sequential indices [0, 1, 2, ..., count-1]
    /// This is the most common case for unfiltered data
    pub fn sequential(count: usize) -> Self {
        Self {
            data: (0..count).collect(),
            count,
        }
    }

    /// Create a selection vector from existing indices
    pub fn from_indices(indices: Vec<usize>) -> Self {
        let count = indices.len();
        Self {
            data: indices,
            count,
        }
    }

    /// Get the index at a specific position (DuckDB-style API)
    #[inline]
    pub fn get_index(&self, position: usize) -> usize {
        debug_assert!(
            position < self.count,
            "Index {} out of bounds (count: {})",
            position,
            self.count
        );
        self.data[position]
    }

    /// Set the index at a specific position
    #[inline]
    pub fn set_index(&mut self, position: usize, index: usize) {
        if position >= self.data.len() {
            self.data.resize(position + 1, 0);
        }
        self.data[position] = index;
        if position >= self.count {
            self.count = position + 1;
        }
    }

    /// Append an index to the selection vector
    #[inline]
    pub fn append(&mut self, index: usize) {
        self.data.push(index);
        self.count += 1;
    }

    /// Get the number of entries
    #[inline]
    pub fn count(&self) -> usize {
        self.count
    }

    /// Check if empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Reset to empty
    pub fn reset(&mut self) {
        self.count = 0;
        self.data.clear();
    }

    /// Set the count (used when filling indices manually)
    pub fn set_count(&mut self, count: usize) {
        debug_assert!(count <= self.data.len(), "Count exceeds data length");
        self.count = count;
    }

    /// Resize the selection vector
    pub fn resize(&mut self, new_count: usize) {
        self.data.resize(new_count, 0);
        self.count = new_count;
    }

    /// Reserve capacity
    pub fn reserve(&mut self, additional: usize) {
        self.data.reserve(additional);
    }

    /// Get a slice of the indices
    pub fn as_slice(&self) -> &[usize] {
        &self.data[..self.count]
    }

    /// Create a new SelectionVector by slicing this one with another
    /// This implements composition: result[i] = self[other[i]]
    /// This is a key DuckDB optimization for chaining filters
    pub fn slice(&self, other: &SelectionVector) -> SelectionVector {
        let mut result = SelectionVector::new(other.count());

        for i in 0..other.count() {
            let idx = other.get_index(i);
            if idx < self.count {
                result.append(self.get_index(idx));
            }
        }

        result
    }

    /// Slice this selection vector in place
    /// Modifies self to contain self[other[i]] for each i
    pub fn slice_in_place(&mut self, other: &SelectionVector) {
        let mut new_data = Vec::with_capacity(other.count());

        for i in 0..other.count() {
            let idx = other.get_index(i);
            if idx < self.count {
                new_data.push(self.get_index(idx));
            }
        }

        self.data = new_data;
        self.count = self.data.len();
    }

    /// Verify that all indices are within bounds (debug assertions only)
    #[cfg(debug_assertions)]
    pub fn verify(&self, vector_size: usize) -> PrismDBResult<()> {
        for i in 0..self.count {
            let idx = self.data[i];
            if idx >= vector_size {
                return Err(PrismDBError::Internal(format!(
                    "SelectionVector index {} is out of bounds (size: {})",
                    idx, vector_size
                )));
            }
        }
        Ok(())
    }

    /// No-op verify in release mode for performance
    #[cfg(not(debug_assertions))]
    #[inline]
    pub fn verify(&self, _vector_size: usize) -> PrismDBResult<()> {
        Ok(())
    }

    /// Sort the indices
    pub fn sort(&mut self) {
        self.data[0..self.count].sort_unstable();
    }
}

/// A Vector represents columnar data in DuckDB
/// It contains the actual data, a validity mask, and optionally a selection vector
#[derive(Debug, Clone)]
pub struct Vector {
    /// The logical type of this vector
    logical_type: LogicalType,
    /// The physical type for storage
    physical_type: PhysicalType,
    /// The actual data (stored as raw bytes)
    data: Vec<u8>,
    /// Validity mask for null values
    validity: ValidityMask,
    /// Optional selection vector for filtering
    selection: Option<SelectionVector>,
    /// Number of entries in the vector
    count: usize,
    /// Capacity of the vector
    capacity: usize,
}

impl Vector {
    /// Create a new vector with the specified type and capacity
    pub fn new(logical_type: LogicalType, capacity: usize) -> Self {
        let physical_type = logical_type.get_physical_type();
        let element_size = physical_type.get_size().unwrap_or(0); // Variable size types handled separately

        Self {
            logical_type,
            physical_type,
            data: vec![0u8; element_size * capacity],
            validity: ValidityMask::all_valid(capacity),
            selection: None,
            count: 0,
            capacity,
        }
    }

    /// Try to coerce a value to a target type
    /// This enables automatic type conversion for compatible types (e.g., DOUBLE -> DECIMAL)
    fn try_coerce_value(value: &Value, target_type: &LogicalType) -> PrismDBResult<Value> {
        use crate::types::Value;

        match (value, target_type) {
            // DECIMAL type coercion
            (Value::Double(d), LogicalType::Decimal { precision, scale }) => {
                // Convert DOUBLE to DECIMAL by multiplying by 10^scale and rounding
                let multiplier = 10_f64.powi(*scale as i32);
                let scaled_value = (d * multiplier).round() as i128;
                Ok(Value::Decimal {
                    value: scaled_value,
                    precision: *precision,
                    scale: *scale,
                })
            }
            (Value::Float(f), LogicalType::Decimal { precision, scale }) => {
                // Convert FLOAT to DECIMAL
                let multiplier = 10_f64.powi(*scale as i32);
                let scaled_value = ((*f as f64) * multiplier).round() as i128;
                Ok(Value::Decimal {
                    value: scaled_value,
                    precision: *precision,
                    scale: *scale,
                })
            }
            (Value::Integer(i), LogicalType::Decimal { precision, scale }) => {
                // Convert INTEGER to DECIMAL
                let multiplier = 10_i128.pow(*scale as u32);
                let scaled_value = (*i as i128) * multiplier;
                Ok(Value::Decimal {
                    value: scaled_value,
                    precision: *precision,
                    scale: *scale,
                })
            }
            (Value::BigInt(i), LogicalType::Decimal { precision, scale }) => {
                // Convert BIGINT to DECIMAL
                let multiplier = 10_i128.pow(*scale as u32);
                let scaled_value = (*i as i128) * multiplier;
                Ok(Value::Decimal {
                    value: scaled_value,
                    precision: *precision,
                    scale: *scale,
                })
            }
            (Value::SmallInt(i), LogicalType::Decimal { precision, scale }) => {
                // Convert SMALLINT to DECIMAL
                let multiplier = 10_i128.pow(*scale as u32);
                let scaled_value = (*i as i128) * multiplier;
                Ok(Value::Decimal {
                    value: scaled_value,
                    precision: *precision,
                    scale: *scale,
                })
            }
            (Value::TinyInt(i), LogicalType::Decimal { precision, scale }) => {
                // Convert TINYINT to DECIMAL
                let multiplier = 10_i128.pow(*scale as u32);
                let scaled_value = (*i as i128) * multiplier;
                Ok(Value::Decimal {
                    value: scaled_value,
                    precision: *precision,
                    scale: *scale,
                })
            }
            // If no coercion is available, return error
            _ => Err(PrismDBError::InvalidType(format!(
                "Cannot coerce value type {} to {}",
                value.get_type(),
                target_type
            ))),
        }
    }

    /// Create a vector from a slice of values
    pub fn from_values(values: &[Value]) -> PrismDBResult<Self> {
        if values.is_empty() {
            return Err(PrismDBError::InvalidValue(
                "Cannot create vector from empty values".to_string(),
            ));
        }

        // Find first non-NULL value to determine type
        // NULL values have type Invalid, so we need to skip them
        let logical_type = values
            .iter()
            .find(|v| !v.is_null())
            .map(|v| v.get_type())
            .unwrap_or(LogicalType::Invalid);
        let physical_type = logical_type.get_physical_type();
        let element_size = physical_type.get_size().unwrap_or(0);

        // For variable-size types, estimate space needed
        let data_size = if element_size == 0 {
            // Variable size type - estimate space needed
            match logical_type {
                LogicalType::Varchar | LogicalType::Char { .. } => {
                    // Calculate actual space needed: 4 bytes for length + actual string length
                    // Add some padding for each string
                    let total_string_bytes: usize = values
                        .iter()
                        .map(|v| match v {
                            Value::Varchar(s) | Value::Char(s) => 4 + s.len(),
                            _ => 4,
                        })
                        .sum();
                    // Round up to ensure enough space
                    ((total_string_bytes / 8) + 1) * 8
                }
                _ => 1024, // Default allocation
            }
        } else {
            element_size * values.len()
        };

        let mut vector = Self {
            logical_type: logical_type.clone(),
            physical_type,
            data: vec![0u8; data_size],
            validity: ValidityMask::new(values.len()),
            selection: None,
            count: values.len(),
            capacity: values.len(),
        };

        for (i, value) in values.iter().enumerate() {
            if value.is_null() {
                vector.validity.set_valid(i, false);
            } else {
                // Try type coercion if types don't match exactly
                let coerced_value = if value.get_type() != logical_type {
                    Self::try_coerce_value(value, &logical_type)?
                } else {
                    value.clone()
                };
                vector.set_value(i, &coerced_value)?;
            }
        }

        Ok(vector)
    }

    /// Get the logical type of this vector
    pub fn get_type(&self) -> &LogicalType {
        &self.logical_type
    }

    /// Get the physical type of this vector
    pub fn get_physical_type(&self) -> &PhysicalType {
        &self.physical_type
    }

    /// Get the validity mask for this vector
    pub fn get_validity_mask(&self) -> &ValidityMask {
        &self.validity
    }

    /// Get the number of entries in the vector
    pub fn count(&self) -> usize {
        self.count
    }

    /// Get the capacity of the vector
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Check if the vector is empty
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Resize the vector
    pub fn resize(&mut self, new_count: usize) -> PrismDBResult<()> {
        if new_count > self.capacity {
            self.reserve(new_count)?;
        }

        self.validity.resize(new_count);
        self.count = new_count;
        Ok(())
    }

    /// Reserve capacity for additional entries
    pub fn reserve(&mut self, new_capacity: usize) -> PrismDBResult<()> {
        if new_capacity <= self.capacity {
            return Ok(());
        }

        let element_size = self.physical_type.get_size().unwrap_or(0);
        if element_size > 0 {
            self.data.resize(element_size * new_capacity, 0);
        } else {
            // For variable-size types (VARCHAR, etc.), allocate reasonable space
            // Estimate: 16 bytes average string length + 4 bytes length prefix
            let estimated_size = new_capacity * 20;
            if estimated_size > self.data.len() {
                self.data.resize(estimated_size, 0);
            }
        }

        self.validity.resize(new_capacity);
        self.capacity = new_capacity;
        Ok(())
    }

    /// Set a value at a specific index
    pub fn set_value(&mut self, index: usize, value: &Value) -> PrismDBResult<()> {
        if index >= self.capacity {
            return Err(PrismDBError::InvalidValue(format!(
                "Index {} exceeds vector capacity {}",
                index, self.capacity
            )));
        }

        if value.is_null() {
            self.validity.set_valid(index, false);
            return Ok(());
        }

        // Try type coercion if types don't match exactly
        let coerced_value = if value.get_type() != self.logical_type {
            Self::try_coerce_value(value, &self.logical_type)?
        } else {
            value.clone()
        };

        self.validity.set_valid(index, true);

        // Use the coerced value for storage
        let value = &coerced_value;

        // Store the value based on its type
        match value {
            Value::Boolean(v) => self.store_numeric(index, *v as u64),
            Value::TinyInt(v) => self.store_numeric(index, *v as u64),
            Value::SmallInt(v) => self.store_numeric(index, *v as u64),
            Value::Integer(v) => self.store_numeric(index, *v as u64),
            Value::BigInt(v) => self.store_numeric(index, *v as u64),
            Value::Float(v) => self.store_float(index, *v),
            Value::Double(v) => self.store_double(index, *v),
            Value::Decimal { value, .. } => {
                // Store DECIMAL as i128 (using 16 bytes)
                let bytes = value.to_le_bytes();
                let offset = index * 16;
                if offset + 16 <= self.data.len() {
                    self.data[offset..offset + 16].copy_from_slice(&bytes);
                }
            }
            Value::Varchar(s) | Value::Char(s) => self.store_string(index, s),
            Value::Date(v) => self.store_numeric(index, *v as u64),
            Value::Time(v) => self.store_numeric(index, *v as u64),
            Value::Timestamp(v) => self.store_numeric(index, *v as u64),
            _ => {
                return Err(PrismDBError::InvalidType(format!(
                    "Unsupported value type for vector storage: {:?}",
                    value
                )))
            }
        }

        Ok(())
    }

    /// Store a numeric value
    #[allow(dead_code)]
    fn store_numeric(&mut self, index: usize, value: u64) {
        let element_size = self.physical_type.get_size().unwrap_or(0);
        if element_size == 0 {
            return;
        }

        let offset = index * element_size;
        match element_size {
            1 => self.data[offset] = value as u8,
            2 => {
                let bytes = (value as u16).to_le_bytes();
                self.data[offset..offset + 2].copy_from_slice(&bytes);
            }
            4 => {
                let bytes = (value as u32).to_le_bytes();
                self.data[offset..offset + 4].copy_from_slice(&bytes);
            }
            8 => {
                let bytes = value.to_le_bytes();
                self.data[offset..offset + 8].copy_from_slice(&bytes);
            }
            _ => {}
        }
    }

    /// Store a float value
    #[allow(dead_code)]
    fn store_float(&mut self, index: usize, value: f32) {
        let element_size = self.physical_type.get_size().unwrap_or(0);
        if element_size != 4 {
            return;
        }

        let offset = index * element_size;
        let bytes = value.to_le_bytes();
        self.data[offset..offset + 4].copy_from_slice(&bytes);
    }

    /// Store a double value
    #[allow(dead_code)]
    fn store_double(&mut self, index: usize, value: f64) {
        let element_size = self.physical_type.get_size().unwrap_or(0);
        if element_size != 8 {
            return;
        }

        let offset = index * element_size;
        let bytes = value.to_le_bytes();
        self.data[offset..offset + 8].copy_from_slice(&bytes);
    }

    /// Store a tinyint value
    #[allow(dead_code)]
    fn store_tinyint(&mut self, index: usize, value: i8) {
        let element_size = self.physical_type.get_size().unwrap_or(0);
        if element_size != 1 {
            return;
        }

        let offset = index * element_size;
        self.data[offset] = value as u8;
    }

    /// Store a smallint value
    #[allow(dead_code)]
    fn store_smallint(&mut self, index: usize, value: i16) {
        let element_size = self.physical_type.get_size().unwrap_or(0);
        if element_size != 2 {
            return;
        }

        let offset = index * element_size;
        let bytes = value.to_le_bytes();
        self.data[offset..offset + 2].copy_from_slice(&bytes);
    }

    /// Store a hugeint value
    #[allow(dead_code)]
    fn store_hugeint(&mut self, index: usize, value: i128) {
        let element_size = self.physical_type.get_size().unwrap_or(0);
        if element_size != 16 {
            return;
        }

        let offset = index * element_size;
        let bytes = value.to_le_bytes();
        self.data[offset..offset + 16].copy_from_slice(&bytes);
    }

    /// Store a string value (simplified - stores length + data sequentially)
    #[allow(dead_code)]
    fn store_string(&mut self, index: usize, string: &str) {
        // Calculate offset by summing sizes of all previous strings
        let mut offset = 0;
        for i in 0..index {
            if self.validity.is_valid(i) {
                // Skip previous strings to find our offset
                if offset + 4 <= self.data.len() {
                    let mut len_bytes = [0u8; 4];
                    len_bytes.copy_from_slice(&self.data[offset..offset + 4]);
                    let prev_len = u32::from_le_bytes(len_bytes) as usize;
                    offset += 4 + prev_len;
                }
            }
        }

        let string_bytes = string.as_bytes();
        let required_space = 4 + string_bytes.len();

        // Grow buffer if needed
        if offset + required_space > self.data.len() {
            let new_size = (offset + required_space).max(self.data.len() * 2);
            self.data.resize(new_size, 0);
        }

        // Store length as u32
        let len_bytes = (string_bytes.len() as u32).to_le_bytes();
        self.data[offset..offset + 4].copy_from_slice(&len_bytes);

        // Store actual string data
        self.data[offset + 4..offset + 4 + string_bytes.len()].copy_from_slice(string_bytes);
    }

    /// Get a value at a specific index
    pub fn get_value(&self, index: usize) -> PrismDBResult<Value> {
        if index >= self.count {
            return Err(PrismDBError::InvalidValue(format!(
                "Index {} exceeds vector count {}",
                index, self.count
            )));
        }

        if !self.validity.is_valid(index) {
            return Ok(Value::Null);
        }

        // Extract value based on type
        match &self.logical_type {
            LogicalType::Boolean => Ok(Value::Boolean(self.extract_numeric(index) != 0)),
            LogicalType::TinyInt => Ok(Value::TinyInt(self.extract_numeric(index) as i8)),
            LogicalType::SmallInt => Ok(Value::SmallInt(self.extract_numeric(index) as i16)),
            LogicalType::Integer => Ok(Value::Integer(self.extract_numeric(index) as i32)),
            LogicalType::BigInt => Ok(Value::BigInt(self.extract_numeric(index) as i64)),
            LogicalType::Float => Ok(Value::Float(self.extract_float(index))),
            LogicalType::Double => Ok(Value::Double(self.extract_double(index))),
            LogicalType::Decimal { precision, scale } => {
                // Extract DECIMAL from 16-byte i128 storage
                let offset = index * 16;
                let mut bytes = [0u8; 16];
                if offset + 16 <= self.data.len() {
                    bytes.copy_from_slice(&self.data[offset..offset + 16]);
                }
                let value = i128::from_le_bytes(bytes);
                Ok(Value::Decimal {
                    value,
                    precision: *precision,
                    scale: *scale,
                })
            }
            LogicalType::Varchar => Ok(Value::Varchar(self.extract_string(index)?)),
            LogicalType::Char { .. } => Ok(Value::Char(self.extract_string(index)?)),
            LogicalType::Date => Ok(Value::Date(self.extract_numeric(index) as i32)),
            LogicalType::Time => Ok(Value::Time(self.extract_numeric(index) as i64)),
            LogicalType::Timestamp => Ok(Value::Timestamp(self.extract_numeric(index) as i64)),
            _ => Err(PrismDBError::InvalidType(format!(
                "Unsupported vector type for value extraction: {:?}",
                self.logical_type
            ))),
        }
    }

    /// Extract a numeric value
    fn extract_numeric(&self, index: usize) -> u64 {
        let element_size = self.physical_type.get_size().unwrap_or(0);
        if element_size == 0 {
            return 0;
        }

        let offset = index * element_size;
        match element_size {
            1 => self.data[offset] as u64,
            2 => {
                let mut bytes = [0u8; 2];
                bytes.copy_from_slice(&self.data[offset..offset + 2]);
                u16::from_le_bytes(bytes) as u64
            }
            4 => {
                let mut bytes = [0u8; 4];
                bytes.copy_from_slice(&self.data[offset..offset + 4]);
                u32::from_le_bytes(bytes) as u64
            }
            8 => {
                let mut bytes = [0u8; 8];
                bytes.copy_from_slice(&self.data[offset..offset + 8]);
                u64::from_le_bytes(bytes)
            }
            _ => 0,
        }
    }

    /// Extract a float value
    fn extract_float(&self, index: usize) -> f32 {
        let element_size = self.physical_type.get_size().unwrap_or(0);
        if element_size != 4 {
            return 0.0;
        }

        let offset = index * element_size;
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(&self.data[offset..offset + 4]);
        f32::from_le_bytes(bytes)
    }

    /// Extract a double value
    fn extract_double(&self, index: usize) -> f64 {
        let element_size = self.physical_type.get_size().unwrap_or(0);
        if element_size != 8 {
            return 0.0;
        }

        let offset = index * element_size;
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&self.data[offset..offset + 8]);
        f64::from_le_bytes(bytes)
    }

    /// Extract a string value
    fn extract_string(&self, index: usize) -> PrismDBResult<String> {
        // Calculate offset by summing sizes of all previous strings
        let mut offset = 0;
        for i in 0..index {
            if self.validity.is_valid(i) {
                // Skip previous strings to find our offset
                if offset + 4 <= self.data.len() {
                    let mut len_bytes = [0u8; 4];
                    len_bytes.copy_from_slice(&self.data[offset..offset + 4]);
                    let prev_len = u32::from_le_bytes(len_bytes) as usize;
                    offset += 4 + prev_len;
                }
            }
        }

        if offset + 4 > self.data.len() {
            return Ok(String::new());
        }

        // Extract length
        let mut len_bytes = [0u8; 4];
        len_bytes.copy_from_slice(&self.data[offset..offset + 4]);
        let len = u32::from_le_bytes(len_bytes) as usize;

        // Extract string data
        if len > 0 && offset + 4 + len <= self.data.len() {
            let string_bytes = &self.data[offset + 4..offset + 4 + len];
            Ok(String::from_utf8_lossy(string_bytes).to_string())
        } else {
            Ok(String::new())
        }
    }

    /// Get the validity mask
    pub fn get_validity(&self) -> &ValidityMask {
        &self.validity
    }

    /// Get a mutable reference to the validity mask
    pub fn get_validity_mut(&mut self) -> &mut ValidityMask {
        &mut self.validity
    }

    /// Get the selection vector
    pub fn get_selection(&self) -> Option<&SelectionVector> {
        self.selection.as_ref()
    }

    /// Set the selection vector
    pub fn set_selection(&mut self, selection: Option<SelectionVector>) {
        self.selection = selection;
    }

    /// Check if a specific entry is null
    pub fn is_null(&self, index: usize) -> bool {
        self.validity.is_null(index)
    }

    /// Check if a specific entry is valid
    pub fn is_valid(&self, index: usize) -> bool {
        self.validity.is_valid(index)
    }

    /// Get the number of null values
    pub fn null_count(&self) -> usize {
        self.validity.null_count()
    }

    /// Get the number of valid values
    pub fn valid_count(&self) -> usize {
        self.validity.valid_count()
    }

    /// Append a value to the vector
    pub fn push(&mut self, value: &Value) -> PrismDBResult<()> {
        if self.count >= self.capacity {
            self.reserve(self.capacity * 2)?;
        }

        self.set_value(self.count, value)?;
        self.count += 1;
        Ok(())
    }

    /// Clear all values from the vector
    pub fn clear(&mut self) {
        self.count = 0;
        self.validity = ValidityMask::all_valid(self.capacity);
    }

    /// Get the number of entries in the vector
    pub fn len(&self) -> usize {
        self.count
    }

    /// Push a null value to the vector
    pub fn push_null(&mut self) -> PrismDBResult<()> {
        if self.count >= self.capacity {
            self.reserve(self.capacity * 2)?;
        }

        self.validity.set_valid(self.count, false);
        self.count += 1;
        Ok(())
    }

    /// Push a value to the vector (alias for push)
    pub fn push_value(&mut self, value: &Value) -> PrismDBResult<()> {
        self.push(value)
    }

    /// Get an iterator over the values in this vector
    pub fn iter(&self) -> VectorIterator<'_> {
        VectorIterator {
            vector: self,
            index: 0,
        }
    }

    /// Store an integer value
    #[allow(dead_code)]
    fn store_integer(&mut self, index: usize, value: i32) {
        let element_size = self.physical_type.get_size().unwrap_or(0);
        if element_size != 4 {
            return;
        }

        let offset = index * element_size;
        let bytes = value.to_le_bytes();
        self.data[offset..offset + 4].copy_from_slice(&bytes);
    }

    /// Store a bigint value
    #[allow(dead_code)]
    fn store_bigint(&mut self, index: usize, value: i64) {
        let element_size = self.physical_type.get_size().unwrap_or(0);
        if element_size != 8 {
            return;
        }

        let offset = index * element_size;
        let bytes = value.to_le_bytes();
        self.data[offset..offset + 8].copy_from_slice(&bytes);
    }

    /// Store a boolean value
    #[allow(dead_code)]
    fn store_boolean(&mut self, index: usize, value: bool) {
        let element_size = self.physical_type.get_size().unwrap_or(0);
        if element_size != 1 {
            return;
        }

        let offset = index * element_size;
        self.data[offset] = if value { 1 } else { 0 };
    }
}

/// Iterator for Vector values
pub struct VectorIterator<'a> {
    vector: &'a Vector,
    index: usize,
}

impl<'a> Iterator for VectorIterator<'a> {
    type Item = PrismDBResult<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.vector.count {
            None
        } else {
            let result = self.vector.get_value(self.index);
            self.index += 1;
            Some(result)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validity_mask() {
        let mut mask = ValidityMask::new(10);

        // Initially all invalid
        assert_eq!(mask.null_count(), 10);
        assert_eq!(mask.valid_count(), 0);

        // Set some valid entries
        mask.set_valid(0, true);
        mask.set_valid(3, true);
        mask.set_valid(7, true);

        assert_eq!(mask.valid_count(), 3);
        assert_eq!(mask.null_count(), 7);
        assert!(mask.is_valid(0));
        assert!(mask.is_valid(3));
        assert!(mask.is_valid(7));
        assert!(!mask.is_valid(1));
    }

    #[test]
    fn test_selection_vector() {
        let indices = vec![2, 5, 1, 8];
        let sel_vec = SelectionVector::from_indices(indices);

        assert_eq!(sel_vec.count(), 4);
        assert_eq!(sel_vec.get_index(0), 2);
        assert_eq!(sel_vec.get_index(1), 5);
        assert_eq!(sel_vec.get_index(3), 8);
        // Note: get_index(4) would panic in debug mode or be UB in release mode
        // so we don't test out-of-bounds access
    }

    #[test]
    fn test_vector_creation() {
        let vector = Vector::new(LogicalType::Integer, 100);

        assert_eq!(vector.get_type(), &LogicalType::Integer);
        assert_eq!(vector.capacity(), 100);
        assert_eq!(vector.count(), 0);
        assert!(vector.is_empty());
    }

    #[test]
    fn test_vector_from_values() -> PrismDBResult<()> {
        let values = vec![Value::integer(1), Value::integer(2), Value::integer(3)];

        let vector = Vector::from_values(&values)?;

        assert_eq!(vector.count(), 3);
        assert_eq!(vector.get_type(), &LogicalType::Integer);

        for (i, expected) in values.iter().enumerate() {
            let actual = vector.get_value(i)?;
            assert_eq!(actual, *expected);
        }

        Ok(())
    }

    #[test]
    fn test_vector_null_values() -> PrismDBResult<()> {
        let values = vec![
            Value::integer(1),
            Value::Null,
            Value::integer(3),
            Value::Null,
        ];

        let vector = Vector::from_values(&values)?;

        assert_eq!(vector.count(), 4);
        assert_eq!(vector.null_count(), 2);
        assert_eq!(vector.valid_count(), 2);

        assert!(vector.is_valid(0));
        assert!(vector.is_null(1));
        assert!(vector.is_valid(2));
        assert!(vector.is_null(3));

        Ok(())
    }

    #[test]
    fn test_vector_push() -> PrismDBResult<()> {
        let mut vector = Vector::new(LogicalType::Integer, 2);

        vector.push(&Value::integer(10))?;
        vector.push(&Value::integer(20))?;
        vector.push(&Value::integer(30))?; // Should trigger resize

        assert_eq!(vector.count(), 3);
        assert_eq!(vector.get_value(0)?, Value::integer(10));
        assert_eq!(vector.get_value(1)?, Value::integer(20));
        assert_eq!(vector.get_value(2)?, Value::integer(30));

        Ok(())
    }

    #[test]
    fn test_vector_iterator() -> PrismDBResult<()> {
        let values = vec![Value::integer(1), Value::integer(2), Value::integer(3)];

        let vector = Vector::from_values(&values)?;
        let collected: PrismDBResult<Vec<Value>> = vector.iter().collect();
        let collected = collected?;

        assert_eq!(collected, values);

        Ok(())
    }
}
