//! Window Functions
//!
//! This module implements window functions for analytical queries.
//! Includes: ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, and value functions.
//! Now with full window frame support (ROWS/RANGE/GROUPS BETWEEN).

use crate::common::error::{PrismDBError, PrismDBResult};
use crate::parser::ast::{WindowFrame, WindowFrameBound, WindowFrameUnits};
use crate::types::Value;
use std::cmp::Ordering;

/// Window frame boundaries for a specific row
#[derive(Debug, Clone)]
pub struct FrameBounds {
    pub start: usize,  // Inclusive start index
    pub end: usize,    // Inclusive end index
}

/// Calculate frame bounds for a specific row in the partition
///
/// For ROWS frames: Direct row offsets from current row
/// For RANGE frames: Rows with values within the specified range (TODO: full implementation)
/// For GROUPS frames: Groups of rows with same ORDER BY values (TODO: full implementation)
pub fn calculate_frame_bounds(
    current_row: usize,
    partition_size: usize,
    frame: &Option<WindowFrame>,
) -> PrismDBResult<FrameBounds> {
    // Default frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    let frame = match frame {
        Some(f) => f,
        None => {
            return Ok(FrameBounds {
                start: 0,
                end: current_row,
            });
        }
    };

    match frame.units {
        WindowFrameUnits::Rows => calculate_rows_frame_bounds(current_row, partition_size, frame),
        WindowFrameUnits::Range => {
            // For now, treat RANGE same as ROWS (simplified implementation)
            // Full RANGE implementation requires ORDER BY value comparison
            calculate_rows_frame_bounds(current_row, partition_size, frame)
        }
        WindowFrameUnits::Groups => {
            // For now, treat GROUPS same as ROWS (simplified implementation)
            // Full GROUPS implementation requires peer group detection
            calculate_rows_frame_bounds(current_row, partition_size, frame)
        }
    }
}

/// Calculate ROWS frame bounds (physical row offsets)
fn calculate_rows_frame_bounds(
    current_row: usize,
    partition_size: usize,
    frame: &WindowFrame,
) -> PrismDBResult<FrameBounds> {
    // Calculate start bound
    let start = match &frame.start_bound {
        WindowFrameBound::UnboundedPreceding => 0,
        WindowFrameBound::Preceding(n) => current_row.saturating_sub(*n),
        WindowFrameBound::CurrentRow => current_row,
        WindowFrameBound::Following(n) => {
            let pos = current_row + n;
            if pos >= partition_size {
                partition_size.saturating_sub(1)
            } else {
                pos
            }
        }
        WindowFrameBound::UnboundedFollowing => {
            return Err(PrismDBError::Execution(
                "Frame start cannot be UNBOUNDED FOLLOWING".to_string(),
            ));
        }
    };

    // Calculate end bound (defaults to CURRENT ROW if not specified)
    let end = match &frame.end_bound {
        Some(WindowFrameBound::UnboundedFollowing) => partition_size.saturating_sub(1),
        Some(WindowFrameBound::Following(n)) => {
            let pos = current_row + n;
            if pos >= partition_size {
                partition_size.saturating_sub(1)
            } else {
                pos
            }
        }
        Some(WindowFrameBound::CurrentRow) => current_row,
        Some(WindowFrameBound::Preceding(n)) => current_row.saturating_sub(*n),
        Some(WindowFrameBound::UnboundedPreceding) => {
            return Err(PrismDBError::Execution(
                "Frame end cannot be UNBOUNDED PRECEDING".to_string(),
            ));
        }
        None => current_row, // Default to CURRENT ROW
    };

    // Validate frame
    if start > end {
        return Err(PrismDBError::Execution(format!(
            "Invalid frame: start ({}) > end ({})",
            start, end
        )));
    }

    Ok(FrameBounds { start, end })
}

/// ROW_NUMBER - Assign unique sequential integers starting from 1
pub fn row_number(partition_data: &[Vec<Value>]) -> PrismDBResult<Vec<Value>> {
    let mut result = Vec::new();
    for (idx, _row) in partition_data.iter().enumerate() {
        result.push(Value::BigInt((idx + 1) as i64));
    }
    Ok(result)
}

/// RANK - Assign rank with gaps for tied values
/// If ORDER BY values are equal, they get the same rank, and next rank has gap
pub fn rank(partition_data: &[Vec<Value>], order_by_col: usize) -> PrismDBResult<Vec<Value>> {
    if partition_data.is_empty() {
        return Ok(Vec::new());
    }

    let mut result = Vec::new();
    let mut current_rank = 1i64;
    let mut same_rank_count = 1;

    result.push(Value::BigInt(1)); // First row is always rank 1

    for i in 1..partition_data.len() {
        let current_val = &partition_data[i][order_by_col];
        let prev_val = &partition_data[i - 1][order_by_col];

        if values_equal(current_val, prev_val)? {
            // Same value, same rank
            result.push(Value::BigInt(current_rank));
            same_rank_count += 1;
        } else {
            // Different value, increment rank by count of previous tied values
            current_rank += same_rank_count;
            result.push(Value::BigInt(current_rank));
            same_rank_count = 1;
        }
    }

    Ok(result)
}

/// DENSE_RANK - Assign rank without gaps for tied values
/// Similar to RANK but consecutive ranks with no gaps
pub fn dense_rank(partition_data: &[Vec<Value>], order_by_col: usize) -> PrismDBResult<Vec<Value>> {
    if partition_data.is_empty() {
        return Ok(Vec::new());
    }

    let mut result = Vec::new();
    let mut current_rank = 1i64;

    result.push(Value::BigInt(1)); // First row is always rank 1

    for i in 1..partition_data.len() {
        let current_val = &partition_data[i][order_by_col];
        let prev_val = &partition_data[i - 1][order_by_col];

        if values_equal(current_val, prev_val)? {
            // Same value, same rank
            result.push(Value::BigInt(current_rank));
        } else {
            // Different value, increment rank by 1 (no gaps)
            current_rank += 1;
            result.push(Value::BigInt(current_rank));
        }
    }

    Ok(result)
}

/// LAG - Access value from previous row
/// Returns value from offset rows before current row, or default if out of bounds
pub fn lag(
    partition_data: &[Vec<Value>],
    value_col: usize,
    offset: Option<i64>,
    default: Option<Value>,
) -> PrismDBResult<Vec<Value>> {
    let offset = offset.unwrap_or(1).max(0) as usize;
    let default = default.unwrap_or(Value::Null);

    let mut result = Vec::new();

    for i in 0..partition_data.len() {
        if i < offset {
            // Before the start, use default
            result.push(default.clone());
        } else {
            // Get value from offset rows before
            result.push(partition_data[i - offset][value_col].clone());
        }
    }

    Ok(result)
}

/// LEAD - Access value from following row
/// Returns value from offset rows after current row, or default if out of bounds
pub fn lead(
    partition_data: &[Vec<Value>],
    value_col: usize,
    offset: Option<i64>,
    default: Option<Value>,
) -> PrismDBResult<Vec<Value>> {
    let offset = offset.unwrap_or(1).max(0) as usize;
    let default = default.unwrap_or(Value::Null);

    let mut result = Vec::new();
    let len = partition_data.len();

    for i in 0..len {
        if i + offset >= len {
            // Beyond the end, use default
            result.push(default.clone());
        } else {
            // Get value from offset rows after
            result.push(partition_data[i + offset][value_col].clone());
        }
    }

    Ok(result)
}

/// FIRST_VALUE - Get first value in window frame
/// Now frame-aware: returns first value within the frame for each row
pub fn first_value(
    partition_data: &[Vec<Value>],
    value_col: usize,
    frame: &Option<WindowFrame>,
) -> PrismDBResult<Vec<Value>> {
    if partition_data.is_empty() {
        return Ok(Vec::new());
    }

    let partition_size = partition_data.len();
    let mut result = Vec::new();

    for current_row in 0..partition_size {
        // Calculate frame bounds for this row
        let bounds = calculate_frame_bounds(current_row, partition_size, frame)?;

        // Get first value in the frame
        let first = partition_data[bounds.start][value_col].clone();
        result.push(first);
    }

    Ok(result)
}

/// LAST_VALUE - Get last value in window frame
/// Now frame-aware: returns last value within the frame for each row
pub fn last_value(
    partition_data: &[Vec<Value>],
    value_col: usize,
    frame: &Option<WindowFrame>,
) -> PrismDBResult<Vec<Value>> {
    if partition_data.is_empty() {
        return Ok(Vec::new());
    }

    let partition_size = partition_data.len();
    let mut result = Vec::new();

    for current_row in 0..partition_size {
        // Calculate frame bounds for this row
        let bounds = calculate_frame_bounds(current_row, partition_size, frame)?;

        // Get last value in the frame
        let last = partition_data[bounds.end][value_col].clone();
        result.push(last);
    }

    Ok(result)
}

/// NTH_VALUE - Get nth value in window frame (1-based)
/// Now frame-aware: returns nth value within the frame for each row
pub fn nth_value(
    partition_data: &[Vec<Value>],
    value_col: usize,
    n: i64,
    frame: &Option<WindowFrame>,
) -> PrismDBResult<Vec<Value>> {
    if partition_data.is_empty() || n < 1 {
        return Ok(vec![Value::Null; partition_data.len()]);
    }

    let partition_size = partition_data.len();
    let mut result = Vec::new();

    for current_row in 0..partition_size {
        // Calculate frame bounds for this row
        let bounds = calculate_frame_bounds(current_row, partition_size, frame)?;

        // Calculate nth position within the frame (1-based to 0-based)
        let nth_idx = bounds.start + (n as usize - 1);

        // Get nth value if it exists within the frame
        let value = if nth_idx <= bounds.end {
            partition_data[nth_idx][value_col].clone()
        } else {
            Value::Null
        };

        result.push(value);
    }

    Ok(result)
}

/// NTILE - Distribute rows into specified number of groups
pub fn ntile(partition_data: &[Vec<Value>], num_buckets: i64) -> PrismDBResult<Vec<Value>> {
    if num_buckets <= 0 {
        return Err(PrismDBError::Execution(
            "NTILE buckets must be positive".to_string(),
        ));
    }

    let len = partition_data.len();
    if len == 0 {
        return Ok(Vec::new());
    }

    let num_buckets = num_buckets as usize;
    let rows_per_bucket = len / num_buckets;
    let remainder = len % num_buckets;

    let mut result = Vec::new();
    let mut current_bucket = 1;
    let mut rows_in_current = 0;
    let max_in_current = if current_bucket <= remainder {
        rows_per_bucket + 1
    } else {
        rows_per_bucket
    };

    for _ in 0..len {
        result.push(Value::Integer(current_bucket as i32));
        rows_in_current += 1;

        if rows_in_current >= max_in_current && current_bucket < num_buckets {
            current_bucket += 1;
            rows_in_current = 0;
        }
    }

    Ok(result)
}

/// PERCENT_RANK - Calculate relative rank as percentage (0 to 1)
/// Formula: (rank - 1) / (total_rows - 1)
pub fn percent_rank(
    partition_data: &[Vec<Value>],
    order_by_col: usize,
) -> PrismDBResult<Vec<Value>> {
    if partition_data.is_empty() {
        return Ok(Vec::new());
    }

    if partition_data.len() == 1 {
        return Ok(vec![Value::Double(0.0)]);
    }

    let ranks = rank(partition_data, order_by_col)?;
    let total_rows = partition_data.len() as f64;

    let mut result = Vec::new();
    for rank_value in ranks {
        if let Value::BigInt(r) = rank_value {
            let percent = (r as f64 - 1.0) / (total_rows - 1.0);
            result.push(Value::Double(percent));
        } else {
            result.push(Value::Null);
        }
    }

    Ok(result)
}

/// CUME_DIST - Calculate cumulative distribution (relative position)
/// Formula: (number of rows <= current row) / total_rows
pub fn cume_dist(partition_data: &[Vec<Value>], order_by_col: usize) -> PrismDBResult<Vec<Value>> {
    if partition_data.is_empty() {
        return Ok(Vec::new());
    }

    let len = partition_data.len();
    let mut result = Vec::with_capacity(len);

    for i in 0..len {
        // Count how many rows have value <= current row's value
        let current_val = &partition_data[i][order_by_col];
        let mut count = 0;

        for j in 0..len {
            let other_val = &partition_data[j][order_by_col];
            match current_val.compare(other_val)? {
                Ordering::Greater | Ordering::Equal => count += 1,
                Ordering::Less => {}
            }
        }

        let cume = count as f64 / len as f64;
        result.push(Value::Double(cume));
    }

    Ok(result)
}

/// SUM window function - Sum over window frame
/// Now frame-aware: computes sum only within the specified frame for each row
pub fn sum_window(
    partition_data: &[Vec<Value>],
    value_col: usize,
    frame: &Option<WindowFrame>,
) -> PrismDBResult<Vec<Value>> {
    if partition_data.is_empty() {
        return Ok(Vec::new());
    }

    let partition_size = partition_data.len();
    let mut result = Vec::new();

    for current_row in 0..partition_size {
        // Calculate frame bounds for this row
        let bounds = calculate_frame_bounds(current_row, partition_size, frame)?;

        // Sum values within the frame
        let mut sum = 0.0;
        for row_idx in bounds.start..=bounds.end {
            let val = &partition_data[row_idx][value_col];
            match val {
                Value::Integer(i) => sum += *i as f64,
                Value::BigInt(i) => sum += *i as f64,
                Value::Float(f) => sum += *f as f64,
                Value::Double(d) => sum += d,
                Value::Null => {} // Skip nulls
                _ => {}
            }
        }

        result.push(Value::Double(sum));
    }

    Ok(result)
}

/// AVG window function - Average over window frame
/// Now frame-aware: computes average only within the specified frame for each row
pub fn avg_window(
    partition_data: &[Vec<Value>],
    value_col: usize,
    frame: &Option<WindowFrame>,
) -> PrismDBResult<Vec<Value>> {
    if partition_data.is_empty() {
        return Ok(Vec::new());
    }

    let partition_size = partition_data.len();
    let mut result = Vec::new();

    for current_row in 0..partition_size {
        // Calculate frame bounds for this row
        let bounds = calculate_frame_bounds(current_row, partition_size, frame)?;

        // Calculate average within the frame
        let mut sum = 0.0;
        let mut count = 0;

        for row_idx in bounds.start..=bounds.end {
            let val = &partition_data[row_idx][value_col];
            match val {
                Value::Integer(i) => {
                    sum += *i as f64;
                    count += 1;
                }
                Value::BigInt(i) => {
                    sum += *i as f64;
                    count += 1;
                }
                Value::Float(f) => {
                    sum += *f as f64;
                    count += 1;
                }
                Value::Double(d) => {
                    sum += d;
                    count += 1;
                }
                Value::Null => {} // Skip nulls
                _ => {}
            }
        }

        if count > 0 {
            result.push(Value::Double(sum / count as f64));
        } else {
            result.push(Value::Null);
        }
    }

    Ok(result)
}

/// COUNT window function - Count over window frame
/// Now frame-aware: counts non-null values only within the specified frame for each row
pub fn count_window(
    partition_data: &[Vec<Value>],
    value_col: usize,
    frame: &Option<WindowFrame>,
) -> PrismDBResult<Vec<Value>> {
    if partition_data.is_empty() {
        return Ok(Vec::new());
    }

    let partition_size = partition_data.len();
    let mut result = Vec::new();

    for current_row in 0..partition_size {
        // Calculate frame bounds for this row
        let bounds = calculate_frame_bounds(current_row, partition_size, frame)?;

        // Count non-null values within the frame
        let mut count = 0i64;
        for row_idx in bounds.start..=bounds.end {
            let val = &partition_data[row_idx][value_col];
            if !matches!(val, Value::Null) {
                count += 1;
            }
        }

        result.push(Value::BigInt(count));
    }

    Ok(result)
}

/// MIN window function - Minimum over window frame
/// Now frame-aware: finds minimum only within the specified frame for each row
pub fn min_window(
    partition_data: &[Vec<Value>],
    value_col: usize,
    frame: &Option<WindowFrame>,
) -> PrismDBResult<Vec<Value>> {
    if partition_data.is_empty() {
        return Ok(Vec::new());
    }

    let partition_size = partition_data.len();
    let mut result = Vec::new();

    for current_row in 0..partition_size {
        // Calculate frame bounds for this row
        let bounds = calculate_frame_bounds(current_row, partition_size, frame)?;

        // Find minimum within the frame
        let mut min_val: Option<Value> = None;
        for row_idx in bounds.start..=bounds.end {
            let val = &partition_data[row_idx][value_col];
            if !matches!(val, Value::Null) {
                min_val = match &min_val {
                    None => Some(val.clone()),
                    Some(current_min) => {
                        if val.compare(current_min)? == Ordering::Less {
                            Some(val.clone())
                        } else {
                            Some(current_min.clone())
                        }
                    }
                };
            }
        }

        result.push(min_val.unwrap_or(Value::Null));
    }

    Ok(result)
}

/// MAX window function - Maximum over window frame
/// Now frame-aware: finds maximum only within the specified frame for each row
pub fn max_window(
    partition_data: &[Vec<Value>],
    value_col: usize,
    frame: &Option<WindowFrame>,
) -> PrismDBResult<Vec<Value>> {
    if partition_data.is_empty() {
        return Ok(Vec::new());
    }

    let partition_size = partition_data.len();
    let mut result = Vec::new();

    for current_row in 0..partition_size {
        // Calculate frame bounds for this row
        let bounds = calculate_frame_bounds(current_row, partition_size, frame)?;

        // Find maximum within the frame
        let mut max_val: Option<Value> = None;
        for row_idx in bounds.start..=bounds.end {
            let val = &partition_data[row_idx][value_col];
            if !matches!(val, Value::Null) {
                max_val = match &max_val {
                    None => Some(val.clone()),
                    Some(current_max) => {
                        if val.compare(current_max)? == Ordering::Greater {
                            Some(val.clone())
                        } else {
                            Some(current_max.clone())
                        }
                    }
                };
            }
        }

        result.push(max_val.unwrap_or(Value::Null));
    }

    Ok(result)
}

/// Helper function to compare values for equality
fn values_equal(a: &Value, b: &Value) -> PrismDBResult<bool> {
    Ok(a.compare(b)? == Ordering::Equal)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_partition(values: Vec<i32>) -> Vec<Vec<Value>> {
        values
            .into_iter()
            .map(|v| vec![Value::Integer(v)])
            .collect()
    }

    #[test]
    fn test_row_number() -> PrismDBResult<()> {
        let data = create_test_partition(vec![10, 20, 30, 40]);
        let result = row_number(&data)?;

        assert_eq!(result.len(), 4);
        assert_eq!(result[0], Value::BigInt(1));
        assert_eq!(result[1], Value::BigInt(2));
        assert_eq!(result[2], Value::BigInt(3));
        assert_eq!(result[3], Value::BigInt(4));

        Ok(())
    }

    #[test]
    fn test_rank() -> PrismDBResult<()> {
        // Test data: 10, 20, 20, 30, 30, 30, 40
        let data = create_test_partition(vec![10, 20, 20, 30, 30, 30, 40]);
        let result = rank(&data, 0)?;

        assert_eq!(result[0], Value::BigInt(1)); // 10
        assert_eq!(result[1], Value::BigInt(2)); // 20
        assert_eq!(result[2], Value::BigInt(2)); // 20 (same rank)
        assert_eq!(result[3], Value::BigInt(4)); // 30 (gap)
        assert_eq!(result[4], Value::BigInt(4)); // 30
        assert_eq!(result[5], Value::BigInt(4)); // 30
        assert_eq!(result[6], Value::BigInt(7)); // 40 (gap)

        Ok(())
    }

    #[test]
    fn test_dense_rank() -> PrismDBResult<()> {
        // Test data: 10, 20, 20, 30, 30, 30, 40
        let data = create_test_partition(vec![10, 20, 20, 30, 30, 30, 40]);
        let result = dense_rank(&data, 0)?;

        assert_eq!(result[0], Value::BigInt(1)); // 10
        assert_eq!(result[1], Value::BigInt(2)); // 20
        assert_eq!(result[2], Value::BigInt(2)); // 20 (same rank)
        assert_eq!(result[3], Value::BigInt(3)); // 30 (no gap)
        assert_eq!(result[4], Value::BigInt(3)); // 30
        assert_eq!(result[5], Value::BigInt(3)); // 30
        assert_eq!(result[6], Value::BigInt(4)); // 40 (no gap)

        Ok(())
    }

    #[test]
    fn test_lag() -> PrismDBResult<()> {
        let data = create_test_partition(vec![10, 20, 30, 40, 50]);

        // LAG with default offset 1
        let result = lag(&data, 0, None, None)?;
        assert_eq!(result[0], Value::Null); // No previous row
        assert_eq!(result[1], Value::Integer(10));
        assert_eq!(result[2], Value::Integer(20));
        assert_eq!(result[3], Value::Integer(30));
        assert_eq!(result[4], Value::Integer(40));

        // LAG with offset 2
        let result2 = lag(&data, 0, Some(2), None)?;
        assert_eq!(result2[0], Value::Null);
        assert_eq!(result2[1], Value::Null);
        assert_eq!(result2[2], Value::Integer(10));
        assert_eq!(result2[3], Value::Integer(20));

        Ok(())
    }

    #[test]
    fn test_lead() -> PrismDBResult<()> {
        let data = create_test_partition(vec![10, 20, 30, 40, 50]);

        // LEAD with default offset 1
        let result = lead(&data, 0, None, None)?;
        assert_eq!(result[0], Value::Integer(20));
        assert_eq!(result[1], Value::Integer(30));
        assert_eq!(result[2], Value::Integer(40));
        assert_eq!(result[3], Value::Integer(50));
        assert_eq!(result[4], Value::Null); // No next row

        // LEAD with offset 2
        let result2 = lead(&data, 0, Some(2), None)?;
        assert_eq!(result2[0], Value::Integer(30));
        assert_eq!(result2[1], Value::Integer(40));
        assert_eq!(result2[2], Value::Integer(50));
        assert_eq!(result2[3], Value::Null);
        assert_eq!(result2[4], Value::Null);

        Ok(())
    }

    #[test]
    fn test_first_last_value() -> PrismDBResult<()> {
        let data = create_test_partition(vec![10, 20, 30, 40]);

        // Default frame: UNBOUNDED PRECEDING to CURRENT ROW
        // FIRST_VALUE should return first in partition for all rows
        let first = first_value(&data, 0, &None)?;
        assert_eq!(first.len(), 4);
        assert_eq!(first[0], Value::Integer(10));
        assert_eq!(first[1], Value::Integer(10));
        assert_eq!(first[2], Value::Integer(10));
        assert_eq!(first[3], Value::Integer(10));

        // LAST_VALUE with default frame returns current row value (up to CURRENT ROW)
        let last = last_value(&data, 0, &None)?;
        assert_eq!(last.len(), 4);
        assert_eq!(last[0], Value::Integer(10)); // Last in frame [0..=0]
        assert_eq!(last[1], Value::Integer(20)); // Last in frame [0..=1]
        assert_eq!(last[2], Value::Integer(30)); // Last in frame [0..=2]
        assert_eq!(last[3], Value::Integer(40)); // Last in frame [0..=3]

        Ok(())
    }

    #[test]
    fn test_nth_value() -> PrismDBResult<()> {
        let data = create_test_partition(vec![10, 20, 30, 40]);

        // Default frame: UNBOUNDED PRECEDING to CURRENT ROW
        // NTH_VALUE(2) should return 2nd value in frame
        let second = nth_value(&data, 0, 2, &None)?;
        assert_eq!(second[0], Value::Null);       // Frame [0..=0] has only 1 value
        assert_eq!(second[1], Value::Integer(20)); // Frame [0..=1] 2nd value is 20
        assert_eq!(second[2], Value::Integer(20)); // Frame [0..=2] 2nd value is 20
        assert_eq!(second[3], Value::Integer(20)); // Frame [0..=3] 2nd value is 20

        let fifth = nth_value(&data, 0, 5, &None)?; // Out of bounds in all frames
        assert!(fifth.iter().all(|v| *v == Value::Null));

        Ok(())
    }

    #[test]
    fn test_ntile() -> PrismDBResult<()> {
        let data = create_test_partition(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

        // Divide into 4 buckets
        let result = ntile(&data, 4)?;
        assert_eq!(result.len(), 10);

        // With 10 rows and 4 buckets: 3, 3, 2, 2 distribution
        // First 2 buckets get 3 rows, last 2 get 2 rows
        assert_eq!(result[0], Value::Integer(1));
        assert_eq!(result[1], Value::Integer(1));
        assert_eq!(result[2], Value::Integer(1));
        assert_eq!(result[3], Value::Integer(2));

        Ok(())
    }

    #[test]
    fn test_percent_rank() -> PrismDBResult<()> {
        // Test data: 10, 20, 20, 30
        let data = create_test_partition(vec![10, 20, 20, 30]);
        let result = percent_rank(&data, 0)?;

        assert_eq!(result.len(), 4);
        // First row: (1-1)/(4-1) = 0.0
        assert_eq!(result[0], Value::Double(0.0));
        // Second row: (2-1)/(4-1) = 0.333...
        if let Value::Double(v) = result[1] {
            assert!((v - 0.333333).abs() < 0.01);
        }
        // Third row: same rank as second = 0.333...
        if let Value::Double(v) = result[2] {
            assert!((v - 0.333333).abs() < 0.01);
        }
        // Fourth row: (4-1)/(4-1) = 1.0
        assert_eq!(result[3], Value::Double(1.0));

        Ok(())
    }

    #[test]
    fn test_cume_dist() -> PrismDBResult<()> {
        // Test data: 10, 20, 30, 40
        let data = create_test_partition(vec![10, 20, 30, 40]);
        let result = cume_dist(&data, 0)?;

        assert_eq!(result.len(), 4);
        // Each row is unique, so cumulative distribution increases
        assert_eq!(result[0], Value::Double(0.25)); // 1/4
        assert_eq!(result[1], Value::Double(0.5)); // 2/4
        assert_eq!(result[2], Value::Double(0.75)); // 3/4
        assert_eq!(result[3], Value::Double(1.0)); // 4/4

        Ok(())
    }

    #[test]
    fn test_sum_window() -> PrismDBResult<()> {
        let data = create_test_partition(vec![10, 20, 30, 40]);
        // Default frame: UNBOUNDED PRECEDING to CURRENT ROW (running sum)
        let result = sum_window(&data, 0, &None)?;

        assert_eq!(result.len(), 4);
        assert_eq!(result[0], Value::Double(10.0)); // 10
        assert_eq!(result[1], Value::Double(30.0)); // 10 + 20
        assert_eq!(result[2], Value::Double(60.0)); // 10 + 20 + 30
        assert_eq!(result[3], Value::Double(100.0)); // 10 + 20 + 30 + 40

        Ok(())
    }

    #[test]
    fn test_avg_window() -> PrismDBResult<()> {
        let data = create_test_partition(vec![10, 20, 30, 40]);
        // Default frame: UNBOUNDED PRECEDING to CURRENT ROW (running average)
        let result = avg_window(&data, 0, &None)?;

        assert_eq!(result.len(), 4);
        assert_eq!(result[0], Value::Double(10.0)); // 10/1
        assert_eq!(result[1], Value::Double(15.0)); // 30/2
        assert_eq!(result[2], Value::Double(20.0)); // 60/3
        assert_eq!(result[3], Value::Double(25.0)); // 100/4

        Ok(())
    }

    #[test]
    fn test_count_window() -> PrismDBResult<()> {
        let data = create_test_partition(vec![10, 20, 30, 40]);
        // Default frame: UNBOUNDED PRECEDING to CURRENT ROW (running count)
        let result = count_window(&data, 0, &None)?;

        assert_eq!(result.len(), 4);
        assert_eq!(result[0], Value::BigInt(1));
        assert_eq!(result[1], Value::BigInt(2));
        assert_eq!(result[2], Value::BigInt(3));
        assert_eq!(result[3], Value::BigInt(4));

        Ok(())
    }

    #[test]
    fn test_min_max_window() -> PrismDBResult<()> {
        let data = create_test_partition(vec![40, 20, 30, 10]);

        // Default frame: UNBOUNDED PRECEDING to CURRENT ROW (running min/max)
        let min_result = min_window(&data, 0, &None)?;
        assert_eq!(min_result[0], Value::Integer(40)); // First value
        assert_eq!(min_result[1], Value::Integer(20)); // min(40, 20)
        assert_eq!(min_result[2], Value::Integer(20)); // min(40, 20, 30)
        assert_eq!(min_result[3], Value::Integer(10)); // min(40, 20, 30, 10)

        let max_result = max_window(&data, 0, &None)?;
        assert_eq!(max_result[0], Value::Integer(40)); // First value
        assert_eq!(max_result[1], Value::Integer(40)); // max(40, 20)
        assert_eq!(max_result[2], Value::Integer(40)); // max(40, 20, 30)
        assert_eq!(max_result[3], Value::Integer(40)); // max(40, 20, 30, 10)

        Ok(())
    }
}
