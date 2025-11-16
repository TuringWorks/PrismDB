//! Helper utilities and common functions

use crate::common::error::{PrismDBError, Result};
use std::marker::Copy;

/// Type alias for indices used throughout the codebase
pub type IdxType = usize;

/// Type alias for optional values
pub type Optional<T> = Option<T>;

/// Type alias for shared pointers
pub type SharedPtr<T> = std::sync::Arc<T>;

/// Type alias for unique pointers
pub type UniquePtr<T> = Box<T>;

/// Type alias for buffer pointers
pub type BufferPtr<T> = Box<T>;

/// Helper function to create a shared pointer
pub fn make_shared<T>(value: T) -> SharedPtr<T> {
    std::sync::Arc::new(value)
}

/// Helper function to create a unique pointer
pub fn make_unique<T>(value: T) -> UniquePtr<T> {
    Box::new(value)
}

/// Helper function to create a buffer pointer
pub fn make_buffer<T>(value: T) -> BufferPtr<T> {
    Box::new(value)
}

/// Macro for unreachable code paths
#[macro_export]
macro_rules! unreachable {
    () => {
        unreachable!("This code path should be unreachable")
    };
    ($msg:expr) => {
        unreachable!($msg)
    };
    ($fmt:expr, $($arg:tt)*) => {
        unreachable!(format!($fmt, $($arg)*))
    };
}

/// Macro for asserting conditions in debug mode
#[macro_export]
macro_rules! debug_assert {
    ($cond:expr) => {
        debug_assert!($cond, "Assertion failed: {}", stringify!($cond))
    };
    ($cond:expr, $msg:expr) => {
        debug_assert!($cond, $msg)
    };
    ($cond:expr, $fmt:expr, $($arg:tt)*) => {
        debug_assert!($cond, format!($fmt, $($arg)*))
    };
}

/// Helper function to align values to a boundary
pub fn align_value(value: usize, alignment: usize) -> usize {
    (value + alignment - 1) & !(alignment - 1)
}

/// Helper function to check if a value is aligned
pub fn is_aligned(value: usize, alignment: usize) -> bool {
    value & (alignment - 1) == 0
}

/// Helper function to divide and round up
pub fn div_round_up(dividend: usize, divisor: usize) -> usize {
    (dividend + divisor - 1) / divisor
}

/// Helper function to get the next power of two
pub fn next_power_of_two(mut n: usize) -> usize {
    if n <= 1 {
        return 1;
    }

    n -= 1;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    n |= n >> 32;
    n + 1
}

/// Helper function to count leading zeros
pub fn count_leading_zeros(mut x: u64) -> u32 {
    if x == 0 {
        return 64;
    }

    let mut count = 0;
    while x & 0x8000000000000000 == 0 {
        count += 1;
        x <<= 1;
    }
    count
}

/// Helper function to count trailing zeros
pub fn count_trailing_zeros(mut x: u64) -> u32 {
    if x == 0 {
        return 64;
    }

    let mut count = 0;
    while x & 1 == 0 {
        count += 1;
        x >>= 1;
    }
    count
}

/// Helper function to swap bytes for primitive types
pub fn swap_bytes<T: Copy>(value: T) -> T {
    unsafe { std::mem::transmute_copy(&value) }
}

/// Helper function to read bytes as a type
pub fn read_bytes<T>(bytes: &[u8]) -> Result<T> {
    if bytes.len() < std::mem::size_of::<T>() {
        return Err(PrismDBError::InvalidArgument(
            "Insufficient bytes to read type".to_string(),
        ));
    }

    unsafe {
        let ptr = bytes.as_ptr() as *const T;
        Ok(std::ptr::read_unaligned(ptr))
    }
}

/// Helper function to write type as bytes
pub fn write_bytes<T: Copy>(value: &T, bytes: &mut [u8]) -> Result<()> {
    if bytes.len() < std::mem::size_of::<T>() {
        return Err(PrismDBError::InvalidArgument(
            "Insufficient bytes to write type".to_string(),
        ));
    }

    unsafe {
        let ptr = bytes.as_mut_ptr() as *mut T;
        std::ptr::write_unaligned(ptr, *value);
    }
    Ok(())
}

/// Helper function to hash bytes
pub fn hash_bytes(bytes: &[u8]) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    bytes.hash(&mut hasher);
    hasher.finish()
}

/// Helper function to compare slices
pub fn compare_slices<T: Ord>(a: &[T], b: &[T]) -> std::cmp::Ordering {
    a.iter().cmp(b.iter())
}

/// Helper function to find in slice
pub fn find_in_slice<T: PartialEq>(slice: &[T], item: &T) -> Option<usize> {
    slice.iter().position(|x| x == item)
}

/// Helper function to check if slice contains item
pub fn slice_contains<T: PartialEq>(slice: &[T], item: &T) -> bool {
    slice.iter().any(|x| x == item)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_align_value() {
        assert_eq!(align_value(10, 8), 16);
        assert_eq!(align_value(16, 8), 16);
        assert_eq!(align_value(17, 8), 24);
    }

    #[test]
    fn test_is_aligned() {
        assert!(is_aligned(16, 8));
        assert!(!is_aligned(10, 8));
        assert!(is_aligned(24, 8));
    }

    #[test]
    fn test_div_round_up() {
        assert_eq!(div_round_up(10, 3), 4);
        assert_eq!(div_round_up(9, 3), 3);
        assert_eq!(div_round_up(0, 3), 0);
    }

    #[test]
    fn test_next_power_of_two() {
        assert_eq!(next_power_of_two(1), 1);
        assert_eq!(next_power_of_two(2), 2);
        assert_eq!(next_power_of_two(3), 4);
        assert_eq!(next_power_of_two(5), 8);
        assert_eq!(next_power_of_two(17), 32);
    }

    #[test]
    fn test_count_leading_zeros() {
        assert_eq!(count_leading_zeros(0), 64);
        assert_eq!(count_leading_zeros(1), 63);
        assert_eq!(count_leading_zeros(2), 62);
        assert_eq!(count_leading_zeros(8), 60);
    }

    #[test]
    fn test_count_trailing_zeros() {
        assert_eq!(count_trailing_zeros(0), 64);
        assert_eq!(count_trailing_zeros(1), 0);
        assert_eq!(count_trailing_zeros(2), 1);
        assert_eq!(count_trailing_zeros(8), 3);
    }
}
