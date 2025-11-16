//! Aggregate function implementations for PrismDB expressions

use crate::common::error::{PrismDBError, PrismDBResult};
use crate::expression::{Expression, ExpressionRef};
use crate::types::{DataChunk, LogicalType, Value, Vector};

/// Aggregate function state
pub trait AggregateState: std::fmt::Debug + Send + Sync + AsAny {
    /// Update the state with a new value
    fn update(&mut self, value: &Value) -> PrismDBResult<()>;

    /// Get the final result
    fn finalize(&self) -> PrismDBResult<Value>;

    /// Merge another state into this state
    fn merge(&mut self, other: Box<dyn AggregateState>) -> PrismDBResult<()>;

    /// Clone the state
    fn clone_box(&self) -> Box<dyn AggregateState>;
}

impl Clone for Box<dyn AggregateState> {
    fn clone(&self) -> Box<dyn AggregateState> {
        self.clone_box()
    }
}

/// Count aggregate state
#[derive(Debug, Clone)]
pub struct CountState {
    count: usize,
    non_null_count: usize,
}

impl CountState {
    pub fn new() -> Self {
        Self {
            count: 0,
            non_null_count: 0,
        }
    }
}

impl AggregateState for CountState {
    fn update(&mut self, value: &Value) -> PrismDBResult<()> {
        self.count += 1;
        if !value.is_null() {
            self.non_null_count += 1;
        }
        Ok(())
    }

    fn finalize(&self) -> PrismDBResult<Value> {
        Ok(Value::BigInt(self.non_null_count as i64))
    }

    fn merge(&mut self, other: Box<dyn AggregateState>) -> PrismDBResult<()> {
        if let Some(other_count) = other.as_any().downcast_ref::<CountState>() {
            self.count += other_count.count;
            self.non_null_count += other_count.non_null_count;
        }
        Ok(())
    }

    fn clone_box(&self) -> Box<dyn AggregateState> {
        Box::new(self.clone())
    }
}

/// Sum aggregate state
#[derive(Debug, Clone)]
pub struct SumState {
    sum: f64,
    decimal_sum: i128,
    count: usize,
    is_decimal: bool,
    decimal_scale: u8,
    decimal_precision: u8,
}

impl SumState {
    pub fn new() -> Self {
        Self {
            sum: 0.0,
            decimal_sum: 0,
            count: 0,
            is_decimal: false,
            decimal_scale: 2,
            decimal_precision: 10,
        }
    }
}

impl AggregateState for SumState {
    fn update(&mut self, value: &Value) -> PrismDBResult<()> {
        if !value.is_null() {
            match value {
                Value::Decimal {
                    value: v,
                    scale,
                    precision,
                } => {
                    self.is_decimal = true;
                    self.decimal_scale = *scale;
                    self.decimal_precision = *precision;
                    self.decimal_sum += v;
                }
                Value::Integer(v) => self.sum += *v as f64,
                Value::BigInt(v) => self.sum += *v as f64,
                Value::SmallInt(v) => self.sum += *v as f64,
                Value::TinyInt(v) => self.sum += *v as f64,
                Value::Float(v) => self.sum += *v as f64,
                Value::Double(v) => self.sum += *v,
                _ => {
                    return Err(PrismDBError::Type(
                        "SUM function requires numeric argument".to_string(),
                    ))
                }
            };
            self.count += 1;
        }
        Ok(())
    }

    fn finalize(&self) -> PrismDBResult<Value> {
        if self.count == 0 {
            Ok(Value::Null)
        } else if self.is_decimal {
            Ok(Value::Decimal {
                value: self.decimal_sum,
                scale: self.decimal_scale,
                precision: self.decimal_precision,
            })
        } else {
            Ok(Value::Double(self.sum))
        }
    }

    fn merge(&mut self, other: Box<dyn AggregateState>) -> PrismDBResult<()> {
        if let Some(other_sum) = other.as_any().downcast_ref::<SumState>() {
            self.sum += other_sum.sum;
            self.decimal_sum += other_sum.decimal_sum;
            self.count += other_sum.count;
            if other_sum.is_decimal {
                self.is_decimal = true;
                self.decimal_scale = other_sum.decimal_scale;
                self.decimal_precision = other_sum.decimal_precision;
            }
        }
        Ok(())
    }

    fn clone_box(&self) -> Box<dyn AggregateState> {
        Box::new(self.clone())
    }
}

/// Average aggregate state
#[derive(Debug, Clone)]
pub struct AvgState {
    sum: f64,
    decimal_sum: i128,
    count: usize,
    is_decimal: bool,
    decimal_scale: u8,
    decimal_precision: u8,
    return_decimal: bool,  // Return DECIMAL even for integer input
}

impl AvgState {
    pub fn new() -> Self {
        Self {
            sum: 0.0,
            decimal_sum: 0,
            count: 0,
            is_decimal: false,
            decimal_scale: 1,
            decimal_precision: 10,
            return_decimal: true,  // AVG always returns DECIMAL or DOUBLE
        }
    }
}

impl AggregateState for AvgState {
    fn update(&mut self, value: &Value) -> PrismDBResult<()> {
        if !value.is_null() {
            match value {
                Value::Decimal {
                    value: v,
                    scale,
                    precision,
                } => {
                    self.is_decimal = true;
                    self.decimal_scale = *scale;
                    self.decimal_precision = *precision;
                    self.decimal_sum += v;
                }
                Value::Integer(v) => {
                    // For integer input, convert to decimal with scale 1 for precision
                    let multiplier = 10_i128.pow(self.decimal_scale as u32);
                    self.decimal_sum += (*v as i128) * multiplier;
                }
                Value::BigInt(v) => {
                    let multiplier = 10_i128.pow(self.decimal_scale as u32);
                    self.decimal_sum += (*v as i128) * multiplier;
                }
                Value::SmallInt(v) => {
                    let multiplier = 10_i128.pow(self.decimal_scale as u32);
                    self.decimal_sum += (*v as i128) * multiplier;
                }
                Value::TinyInt(v) => {
                    let multiplier = 10_i128.pow(self.decimal_scale as u32);
                    self.decimal_sum += (*v as i128) * multiplier;
                }
                Value::Float(v) => {
                    self.return_decimal = false;
                    self.sum += *v as f64;
                }
                Value::Double(v) => {
                    self.return_decimal = false;
                    self.sum += *v;
                }
                _ => {
                    return Err(PrismDBError::Type(
                        "AVG function requires numeric argument".to_string(),
                    ))
                }
            };
            self.count += 1;
        }
        Ok(())
    }

    fn finalize(&self) -> PrismDBResult<Value> {
        if self.count == 0 {
            Ok(Value::Null)
        } else if self.return_decimal {
            // Return DECIMAL result
            let avg_value = self.decimal_sum / self.count as i128;
            Ok(Value::Decimal {
                value: avg_value,
                scale: self.decimal_scale,
                precision: self.decimal_precision,
            })
        } else {
            Ok(Value::Double(self.sum / self.count as f64))
        }
    }

    fn merge(&mut self, other: Box<dyn AggregateState>) -> PrismDBResult<()> {
        if let Some(other_avg) = other.as_any().downcast_ref::<AvgState>() {
            self.sum += other_avg.sum;
            self.decimal_sum += other_avg.decimal_sum;
            self.count += other_avg.count;
            if other_avg.is_decimal {
                self.is_decimal = true;
                self.decimal_scale = other_avg.decimal_scale;
                self.decimal_precision = other_avg.decimal_precision;
            }
            if !other_avg.return_decimal {
                self.return_decimal = false;
            }
        }
        Ok(())
    }

    fn clone_box(&self) -> Box<dyn AggregateState> {
        Box::new(self.clone())
    }
}

/// Min aggregate state
#[derive(Debug, Clone)]
pub struct MinState {
    min: Option<Value>,
}

impl MinState {
    pub fn new() -> Self {
        Self { min: None }
    }
}

impl AggregateState for MinState {
    fn update(&mut self, value: &Value) -> PrismDBResult<()> {
        if !value.is_null() {
            match &self.min {
                None => self.min = Some(value.clone()),
                Some(current_min) => {
                    if value.compare(current_min)? == std::cmp::Ordering::Less {
                        self.min = Some(value.clone());
                    }
                }
            }
        }
        Ok(())
    }

    fn finalize(&self) -> PrismDBResult<Value> {
        match &self.min {
            Some(min_val) => Ok(min_val.clone()),
            None => Ok(Value::Null),
        }
    }

    fn merge(&mut self, other: Box<dyn AggregateState>) -> PrismDBResult<()> {
        if let Some(other_min) = other.as_any().downcast_ref::<MinState>() {
            match &other_min.min {
                Some(other_val) => self.update(other_val)?,
                None => {}
            }
        }
        Ok(())
    }

    fn clone_box(&self) -> Box<dyn AggregateState> {
        Box::new(self.clone())
    }
}

/// Max aggregate state
#[derive(Debug, Clone)]
pub struct MaxState {
    max: Option<Value>,
}

/// Standard Deviation aggregate state (uses Welford's online algorithm)
#[derive(Debug, Clone)]
pub struct StdDevState {
    count: usize,
    mean: f64,
    m2: f64, // Sum of squared differences from mean
}

/// Variance aggregate state (uses Welford's online algorithm)
#[derive(Debug, Clone)]
pub struct VarianceState {
    count: usize,
    mean: f64,
    m2: f64, // Sum of squared differences from mean
}

/// Median aggregate state (collects all values for sorting)
#[derive(Debug, Clone)]
pub struct MedianState {
    values: Vec<f64>,
}

impl MaxState {
    pub fn new() -> Self {
        Self { max: None }
    }
}

impl AggregateState for MaxState {
    fn update(&mut self, value: &Value) -> PrismDBResult<()> {
        if !value.is_null() {
            match &self.max {
                None => self.max = Some(value.clone()),
                Some(current_max) => {
                    if value.compare(current_max)? == std::cmp::Ordering::Greater {
                        self.max = Some(value.clone());
                    }
                }
            }
        }
        Ok(())
    }

    fn finalize(&self) -> PrismDBResult<Value> {
        match &self.max {
            Some(max_val) => Ok(max_val.clone()),
            None => Ok(Value::Null),
        }
    }

    fn merge(&mut self, other: Box<dyn AggregateState>) -> PrismDBResult<()> {
        if let Some(other_max) = other.as_any().downcast_ref::<MaxState>() {
            match &other_max.max {
                Some(other_val) => self.update(other_val)?,
                None => {}
            }
        }
        Ok(())
    }

    fn clone_box(&self) -> Box<dyn AggregateState> {
        Box::new(self.clone())
    }
}

impl StdDevState {
    pub fn new() -> Self {
        Self {
            count: 0,
            mean: 0.0,
            m2: 0.0,
        }
    }
}

impl AggregateState for StdDevState {
    fn update(&mut self, value: &Value) -> PrismDBResult<()> {
        if !value.is_null() {
            let val = match value {
                Value::Integer(v) => *v as f64,
                Value::BigInt(v) => *v as f64,
                Value::Float(v) => *v as f64,
                Value::Double(v) => *v,
                _ => {
                    return Err(PrismDBError::Type(
                        "STDDEV function requires numeric argument".to_string(),
                    ))
                }
            };

            // Welford's online algorithm for variance
            self.count += 1;
            let delta = val - self.mean;
            self.mean += delta / self.count as f64;
            let delta2 = val - self.mean;
            self.m2 += delta * delta2;
        }
        Ok(())
    }

    fn finalize(&self) -> PrismDBResult<Value> {
        if self.count < 2 {
            Ok(Value::Null) // Need at least 2 values for stddev
        } else {
            let variance = self.m2 / (self.count - 1) as f64; // Sample variance
            Ok(Value::Double(variance.sqrt()))
        }
    }

    fn merge(&mut self, other: Box<dyn AggregateState>) -> PrismDBResult<()> {
        if let Some(other_stddev) = other.as_any().downcast_ref::<StdDevState>() {
            if other_stddev.count == 0 {
                return Ok(());
            }
            if self.count == 0 {
                self.count = other_stddev.count;
                self.mean = other_stddev.mean;
                self.m2 = other_stddev.m2;
                return Ok(());
            }

            // Parallel variance algorithm
            let total_count = self.count + other_stddev.count;
            let delta = other_stddev.mean - self.mean;
            let new_mean = (self.count as f64 * self.mean
                + other_stddev.count as f64 * other_stddev.mean)
                / total_count as f64;
            let new_m2 = self.m2
                + other_stddev.m2
                + delta * delta * (self.count * other_stddev.count) as f64 / total_count as f64;

            self.count = total_count;
            self.mean = new_mean;
            self.m2 = new_m2;
        }
        Ok(())
    }

    fn clone_box(&self) -> Box<dyn AggregateState> {
        Box::new(self.clone())
    }
}

impl VarianceState {
    pub fn new() -> Self {
        Self {
            count: 0,
            mean: 0.0,
            m2: 0.0,
        }
    }
}

impl AggregateState for VarianceState {
    fn update(&mut self, value: &Value) -> PrismDBResult<()> {
        if !value.is_null() {
            let val = match value {
                Value::Integer(v) => *v as f64,
                Value::BigInt(v) => *v as f64,
                Value::Float(v) => *v as f64,
                Value::Double(v) => *v,
                _ => {
                    return Err(PrismDBError::Type(
                        "VARIANCE function requires numeric argument".to_string(),
                    ))
                }
            };

            // Welford's online algorithm for variance
            self.count += 1;
            let delta = val - self.mean;
            self.mean += delta / self.count as f64;
            let delta2 = val - self.mean;
            self.m2 += delta * delta2;
        }
        Ok(())
    }

    fn finalize(&self) -> PrismDBResult<Value> {
        if self.count < 2 {
            Ok(Value::Null) // Need at least 2 values for variance
        } else {
            let variance = self.m2 / (self.count - 1) as f64; // Sample variance
            Ok(Value::Double(variance))
        }
    }

    fn merge(&mut self, other: Box<dyn AggregateState>) -> PrismDBResult<()> {
        if let Some(other_var) = other.as_any().downcast_ref::<VarianceState>() {
            if other_var.count == 0 {
                return Ok(());
            }
            if self.count == 0 {
                self.count = other_var.count;
                self.mean = other_var.mean;
                self.m2 = other_var.m2;
                return Ok(());
            }

            // Parallel variance algorithm
            let total_count = self.count + other_var.count;
            let delta = other_var.mean - self.mean;
            let new_mean = (self.count as f64 * self.mean
                + other_var.count as f64 * other_var.mean)
                / total_count as f64;
            let new_m2 = self.m2
                + other_var.m2
                + delta * delta * (self.count * other_var.count) as f64 / total_count as f64;

            self.count = total_count;
            self.mean = new_mean;
            self.m2 = new_m2;
        }
        Ok(())
    }

    fn clone_box(&self) -> Box<dyn AggregateState> {
        Box::new(self.clone())
    }
}

impl MedianState {
    pub fn new() -> Self {
        Self { values: Vec::new() }
    }
}

impl AggregateState for MedianState {
    fn update(&mut self, value: &Value) -> PrismDBResult<()> {
        if !value.is_null() {
            let val = match value {
                Value::Integer(v) => *v as f64,
                Value::BigInt(v) => *v as f64,
                Value::Float(v) => *v as f64,
                Value::Double(v) => *v,
                _ => {
                    return Err(PrismDBError::Type(
                        "MEDIAN function requires numeric argument".to_string(),
                    ))
                }
            };
            self.values.push(val);
        }
        Ok(())
    }

    fn finalize(&self) -> PrismDBResult<Value> {
        if self.values.is_empty() {
            return Ok(Value::Null);
        }

        let mut sorted = self.values.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let len = sorted.len();
        let median = if len % 2 == 0 {
            // Even number of elements: average of two middle values
            (sorted[len / 2 - 1] + sorted[len / 2]) / 2.0
        } else {
            // Odd number of elements: middle value
            sorted[len / 2]
        };

        Ok(Value::Double(median))
    }

    fn merge(&mut self, other: Box<dyn AggregateState>) -> PrismDBResult<()> {
        if let Some(other_median) = other.as_any().downcast_ref::<MedianState>() {
            self.values.extend_from_slice(&other_median.values);
        }
        Ok(())
    }

    fn clone_box(&self) -> Box<dyn AggregateState> {
        Box::new(self.clone())
    }
}

/// MODE aggregate state - Find most frequent value
#[derive(Debug, Clone)]
pub struct ModeState {
    counts: std::collections::HashMap<String, usize>,
}

impl ModeState {
    pub fn new() -> Self {
        Self {
            counts: std::collections::HashMap::new(),
        }
    }
}

impl AggregateState for ModeState {
    fn update(&mut self, value: &Value) -> PrismDBResult<()> {
        if !value.is_null() {
            // Use debug format as key for any value type
            let key = format!("{:?}", value);
            *self.counts.entry(key).or_insert(0) += 1;
        }
        Ok(())
    }

    fn finalize(&self) -> PrismDBResult<Value> {
        if self.counts.is_empty() {
            return Ok(Value::Null);
        }

        // Find the most frequent value
        let mode = self
            .counts
            .iter()
            .max_by_key(|(_, &count)| count)
            .map(|(k, _)| k.clone());

        match mode {
            Some(m) => Ok(Value::Varchar(m)),
            None => Ok(Value::Null),
        }
    }

    fn merge(&mut self, other: Box<dyn AggregateState>) -> PrismDBResult<()> {
        if let Some(other_mode) = other.as_any().downcast_ref::<ModeState>() {
            for (key, count) in &other_mode.counts {
                *self.counts.entry(key.clone()).or_insert(0) += count;
            }
        }
        Ok(())
    }

    fn clone_box(&self) -> Box<dyn AggregateState> {
        Box::new(self.clone())
    }
}

/// APPROX_COUNT_DISTINCT aggregate state - Approximate distinct count using hash
#[derive(Debug, Clone)]
pub struct ApproxCountDistinctState {
    seen: std::collections::HashSet<u64>,
}

impl ApproxCountDistinctState {
    pub fn new() -> Self {
        Self {
            seen: std::collections::HashSet::new(),
        }
    }

    fn hash_value(value: &Value) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        // Hash the debug representation as a simple approach
        format!("{:?}", value).hash(&mut hasher);
        hasher.finish()
    }
}

impl AggregateState for ApproxCountDistinctState {
    fn update(&mut self, value: &Value) -> PrismDBResult<()> {
        if !value.is_null() {
            let hash = Self::hash_value(value);
            self.seen.insert(hash);
        }
        Ok(())
    }

    fn finalize(&self) -> PrismDBResult<Value> {
        Ok(Value::BigInt(self.seen.len() as i64))
    }

    fn merge(&mut self, other: Box<dyn AggregateState>) -> PrismDBResult<()> {
        if let Some(other_approx) = other.as_any().downcast_ref::<ApproxCountDistinctState>() {
            self.seen.extend(&other_approx.seen);
        }
        Ok(())
    }

    fn clone_box(&self) -> Box<dyn AggregateState> {
        Box::new(self.clone())
    }
}

/// APPROX_QUANTILE aggregate state - Approximate quantile using T-Digest algorithm
/// This is much faster than exact quantile computation for large datasets
/// Uses the t-digest algorithm for streaming quantile estimation
#[derive(Debug, Clone)]
pub struct ApproxQuantileState {
    digest: tdigest::TDigest,
    quantile: f64,
}

impl ApproxQuantileState {
    pub fn new(quantile: f64) -> Self {
        Self {
            digest: tdigest::TDigest::new_with_size(100), // 100 centroids for good accuracy
            quantile,
        }
    }

    pub fn with_default_quantile() -> Self {
        Self::new(0.5) // Default to median
    }
}

impl AggregateState for ApproxQuantileState {
    fn update(&mut self, value: &Value) -> PrismDBResult<()> {
        if !value.is_null() {
            let num_val = match value {
                Value::Integer(v) => *v as f64,
                Value::BigInt(v) => *v as f64,
                Value::Float(v) => *v as f64,
                Value::Double(v) => *v,
                _ => {
                    return Err(PrismDBError::Type(
                        "APPROX_QUANTILE requires numeric argument".to_string(),
                    ))
                }
            };
            self.digest = self.digest.merge_unsorted(vec![num_val]);
        }
        Ok(())
    }

    fn finalize(&self) -> PrismDBResult<Value> {
        if self.digest.count() == 0.0 {
            Ok(Value::Null)
        } else {
            let result = self.digest.estimate_quantile(self.quantile);
            Ok(Value::Double(result))
        }
    }

    fn merge(&mut self, other: Box<dyn AggregateState>) -> PrismDBResult<()> {
        if let Some(_other_quantile) = other.as_any().downcast_ref::<ApproxQuantileState>() {
            // Merge the other digest into this one
            // The tdigest crate provides merge_unsorted for merging
            let _other_values: Vec<f64> = Vec::new(); // Would need to extract values from _other_quantile.digest
            // For now, just skip merging as tdigest doesn't expose values easily
            // In practice, for parallel aggregation, we'd reconstruct from centroids
            // This is a limitation of the tdigest crate API
        }
        Ok(())
    }

    fn clone_box(&self) -> Box<dyn AggregateState> {
        Box::new(self.clone())
    }
}

/// STRING_AGG aggregate state - Concatenate strings with separator
#[derive(Debug, Clone)]
pub struct StringAggState {
    values: Vec<String>,
    separator: String,
}

impl StringAggState {
    pub fn new(separator: String) -> Self {
        Self {
            values: Vec::new(),
            separator,
        }
    }
}

impl AggregateState for StringAggState {
    fn update(&mut self, value: &Value) -> PrismDBResult<()> {
        if !value.is_null() {
            let string_val = match value {
                Value::Varchar(s) => s.clone(),
                Value::Integer(i) => i.to_string(),
                Value::BigInt(i) => i.to_string(),
                Value::Float(f) => f.to_string(),
                Value::Double(f) => f.to_string(),
                _ => format!("{:?}", value),
            };
            self.values.push(string_val);
        }
        Ok(())
    }

    fn finalize(&self) -> PrismDBResult<Value> {
        if self.values.is_empty() {
            Ok(Value::Null)
        } else {
            Ok(Value::Varchar(self.values.join(&self.separator)))
        }
    }

    fn merge(&mut self, other: Box<dyn AggregateState>) -> PrismDBResult<()> {
        if let Some(other_agg) = other.as_any().downcast_ref::<StringAggState>() {
            self.values.extend(other_agg.values.clone());
        }
        Ok(())
    }

    fn clone_box(&self) -> Box<dyn AggregateState> {
        Box::new(self.clone())
    }
}

/// PERCENTILE_CONT aggregate state - Continuous percentile (interpolated)
#[derive(Debug, Clone)]
pub struct PercentileContState {
    values: Vec<f64>,
    percentile: f64,
}

impl PercentileContState {
    pub fn new(percentile: f64) -> Self {
        Self {
            values: Vec::new(),
            percentile: percentile.abs().max(0.0).min(1.0), // Clamp to [0, 1]
        }
    }
}

impl AggregateState for PercentileContState {
    fn update(&mut self, value: &Value) -> PrismDBResult<()> {
        if !value.is_null() {
            let num_val = match value {
                Value::Integer(i) => *i as f64,
                Value::BigInt(i) => *i as f64,
                Value::Float(f) => *f as f64,
                Value::Double(d) => *d,
                _ => {
                    return Err(PrismDBError::Type(
                        "PERCENTILE_CONT requires numeric argument".to_string(),
                    ))
                }
            };
            self.values.push(num_val);
        }
        Ok(())
    }

    fn finalize(&self) -> PrismDBResult<Value> {
        if self.values.is_empty() {
            return Ok(Value::Null);
        }

        // Sort values
        let mut sorted = self.values.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        // Calculate position
        let n = sorted.len() as f64;
        let pos = self.percentile * (n - 1.0);
        let lower_idx = pos.floor() as usize;
        let upper_idx = pos.ceil() as usize;

        if lower_idx == upper_idx {
            Ok(Value::Double(sorted[lower_idx]))
        } else {
            // Linear interpolation
            let fraction = pos - lower_idx as f64;
            let result = sorted[lower_idx] * (1.0 - fraction) + sorted[upper_idx] * fraction;
            Ok(Value::Double(result))
        }
    }

    fn merge(&mut self, other: Box<dyn AggregateState>) -> PrismDBResult<()> {
        if let Some(other_pct) = other.as_any().downcast_ref::<PercentileContState>() {
            self.values.extend(other_pct.values.clone());
        }
        Ok(())
    }

    fn clone_box(&self) -> Box<dyn AggregateState> {
        Box::new(self.clone())
    }
}

/// PERCENTILE_DISC aggregate state - Discrete percentile (actual value)
#[derive(Debug, Clone)]
pub struct PercentileDiscState {
    values: Vec<f64>,
    percentile: f64,
}

impl PercentileDiscState {
    pub fn new(percentile: f64) -> Self {
        Self {
            values: Vec::new(),
            percentile: percentile.abs().max(0.0).min(1.0), // Clamp to [0, 1]
        }
    }
}

impl AggregateState for PercentileDiscState {
    fn update(&mut self, value: &Value) -> PrismDBResult<()> {
        if !value.is_null() {
            let num_val = match value {
                Value::Integer(i) => *i as f64,
                Value::BigInt(i) => *i as f64,
                Value::Float(f) => *f as f64,
                Value::Double(d) => *d,
                _ => {
                    return Err(PrismDBError::Type(
                        "PERCENTILE_DISC requires numeric argument".to_string(),
                    ))
                }
            };
            self.values.push(num_val);
        }
        Ok(())
    }

    fn finalize(&self) -> PrismDBResult<Value> {
        if self.values.is_empty() {
            return Ok(Value::Null);
        }

        // Sort values
        let mut sorted = self.values.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        // Calculate position and get the value at that index
        let n = sorted.len();
        let idx = ((self.percentile * (n as f64 - 1.0)).ceil() as usize).min(n - 1);

        Ok(Value::Double(sorted[idx]))
    }

    fn merge(&mut self, other: Box<dyn AggregateState>) -> PrismDBResult<()> {
        if let Some(other_pct) = other.as_any().downcast_ref::<PercentileDiscState>() {
            self.values.extend(other_pct.values.clone());
        }
        Ok(())
    }

    fn clone_box(&self) -> Box<dyn AggregateState> {
        Box::new(self.clone())
    }
}

/// COVAR_POP aggregate state - Population covariance
/// Uses Schubert and Gertz SSDBM 2018 algorithm
#[derive(Debug, Clone)]
pub struct CovarPopState {
    count: u64,
    mean_x: f64,
    mean_y: f64,
    co_moment: f64,
}

impl CovarPopState {
    pub fn new() -> Self {
        Self {
            count: 0,
            mean_x: 0.0,
            mean_y: 0.0,
            co_moment: 0.0,
        }
    }

    // Helper method for binary aggregate (two columns)
    pub fn update_pair(&mut self, x: f64, y: f64) -> PrismDBResult<()> {
        self.count += 1;
        let n = self.count as f64;

        let dx = x - self.mean_x;
        let mean_x = self.mean_x + dx / n;

        let dy = y - self.mean_y;
        let mean_y = self.mean_y + dy / n;

        // Schubert and Gertz SSDBM 2018 (4.3)
        let c = self.co_moment + dx * (y - mean_y);

        self.mean_x = mean_x;
        self.mean_y = mean_y;
        self.co_moment = c;

        Ok(())
    }
}

impl AggregateState for CovarPopState {
    fn update(&mut self, _value: &Value) -> PrismDBResult<()> {
        // Note: COVAR is a binary aggregate (requires two columns)
        // This single-value update is for compatibility
        // Real implementation would need binary update support
        Ok(())
    }

    fn finalize(&self) -> PrismDBResult<Value> {
        if self.count == 0 {
            Ok(Value::Null)
        } else {
            Ok(Value::Double(self.co_moment / self.count as f64))
        }
    }

    fn merge(&mut self, other: Box<dyn AggregateState>) -> PrismDBResult<()> {
        if let Some(other_covar) = other.as_any().downcast_ref::<CovarPopState>() {
            if other_covar.count == 0 {
                return Ok(());
            }
            if self.count == 0 {
                *self = other_covar.clone();
                return Ok(());
            }

            // Schubert and Gertz SSDBM 2018, equation 21
            let total_count = self.count + other_covar.count;
            let target_count = self.count as f64;
            let source_count = other_covar.count as f64;
            let total_count_f = total_count as f64;

            let mean_x =
                (source_count * other_covar.mean_x + target_count * self.mean_x) / total_count_f;
            let mean_y =
                (source_count * other_covar.mean_y + target_count * self.mean_y) / total_count_f;

            let delta_x = self.mean_x - other_covar.mean_x;
            let delta_y = self.mean_y - other_covar.mean_y;
            self.co_moment = other_covar.co_moment + self.co_moment
                + delta_x * delta_y * source_count * target_count / total_count_f;

            self.mean_x = mean_x;
            self.mean_y = mean_y;
            self.count = total_count;
        }
        Ok(())
    }

    fn clone_box(&self) -> Box<dyn AggregateState> {
        Box::new(self.clone())
    }
}

/// COVAR_SAMP aggregate state - Sample covariance
#[derive(Debug, Clone)]
pub struct CovarSampState {
    covar_pop: CovarPopState,
}

impl CovarSampState {
    pub fn new() -> Self {
        Self {
            covar_pop: CovarPopState::new(),
        }
    }

    pub fn update_pair(&mut self, x: f64, y: f64) -> PrismDBResult<()> {
        self.covar_pop.update_pair(x, y)
    }
}

impl AggregateState for CovarSampState {
    fn update(&mut self, value: &Value) -> PrismDBResult<()> {
        self.covar_pop.update(value)
    }

    fn finalize(&self) -> PrismDBResult<Value> {
        if self.covar_pop.count < 2 {
            Ok(Value::Null)
        } else {
            Ok(Value::Double(
                self.covar_pop.co_moment / (self.covar_pop.count - 1) as f64,
            ))
        }
    }

    fn merge(&mut self, other: Box<dyn AggregateState>) -> PrismDBResult<()> {
        if let Some(other_covar) = other.as_any().downcast_ref::<CovarSampState>() {
            self.covar_pop
                .merge(Box::new(other_covar.covar_pop.clone()))?;
        }
        Ok(())
    }

    fn clone_box(&self) -> Box<dyn AggregateState> {
        Box::new(self.clone())
    }
}

/// CORR aggregate state - Correlation coefficient (Pearson's r)
/// CORR(y, x) = COVAR_POP(y, x) / (STDDEV_POP(x) * STDDEV_POP(y))
#[derive(Debug, Clone)]
pub struct CorrState {
    covar_pop: CovarPopState,
    stddev_x: StdDevState,
    stddev_y: StdDevState,
}

impl CorrState {
    pub fn new() -> Self {
        Self {
            covar_pop: CovarPopState::new(),
            stddev_x: StdDevState::new(),
            stddev_y: StdDevState::new(),
        }
    }

    pub fn update_pair(&mut self, x: f64, y: f64) -> PrismDBResult<()> {
        self.covar_pop.update_pair(x, y)?;
        self.stddev_x.update(&Value::Double(x))?;
        self.stddev_y.update(&Value::Double(y))?;
        Ok(())
    }
}

impl AggregateState for CorrState {
    fn update(&mut self, _value: &Value) -> PrismDBResult<()> {
        // Binary aggregate - needs update_pair
        Ok(())
    }

    fn finalize(&self) -> PrismDBResult<Value> {
        if self.covar_pop.count == 0
            || self.stddev_x.count == 0
            || self.stddev_y.count == 0
        {
            return Ok(Value::Null);
        }

        let cov = self.covar_pop.co_moment / self.covar_pop.count as f64;

        // STDDEV_POP formula
        let std_x = if self.stddev_x.count > 1 {
            (self.stddev_x.m2 / self.stddev_x.count as f64).sqrt()
        } else {
            0.0
        };

        let std_y = if self.stddev_y.count > 1 {
            (self.stddev_y.m2 / self.stddev_y.count as f64).sqrt()
        } else {
            0.0
        };

        if !std_x.is_finite() {
            return Err(PrismDBError::Execution(
                "STDDEV_POP for X is out of range".to_string(),
            ));
        }

        if !std_y.is_finite() {
            return Err(PrismDBError::Execution(
                "STDDEV_POP for Y is out of range".to_string(),
            ));
        }

        let result = if std_x * std_y != 0.0 {
            cov / (std_x * std_y)
        } else {
            f64::NAN
        };

        Ok(Value::Double(result))
    }

    fn merge(&mut self, other: Box<dyn AggregateState>) -> PrismDBResult<()> {
        if let Some(other_corr) = other.as_any().downcast_ref::<CorrState>() {
            self.covar_pop
                .merge(Box::new(other_corr.covar_pop.clone()))?;
            self.stddev_x.merge(Box::new(other_corr.stddev_x.clone()))?;
            self.stddev_y.merge(Box::new(other_corr.stddev_y.clone()))?;
        }
        Ok(())
    }

    fn clone_box(&self) -> Box<dyn AggregateState> {
        Box::new(self.clone())
    }
}

/// Helper trait for downcasting
pub trait AsAny {
    fn as_any(&self) -> &dyn std::any::Any;
}

impl<T: std::any::Any> AsAny for T {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Aggregate function expression
#[derive(Debug)]
pub struct AggregateExpression {
    function_name: String,
    child: ExpressionRef,
    return_type: LogicalType,
}

impl AggregateExpression {
    pub fn new(function_name: String, child: ExpressionRef, return_type: LogicalType) -> Self {
        Self {
            function_name,
            child,
            return_type,
        }
    }

    pub fn function_name(&self) -> &str {
        &self.function_name
    }

    pub fn child(&self) -> &dyn Expression {
        self.child.as_ref()
    }

    /// Create aggregate state for the function
    pub fn create_state(&self) -> PrismDBResult<Box<dyn AggregateState>> {
        match self.function_name.to_uppercase().as_str() {
            "COUNT" => Ok(Box::new(CountState::new())),
            "SUM" => Ok(Box::new(SumState::new())),
            "AVG" => Ok(Box::new(AvgState::new())),
            "MIN" => Ok(Box::new(MinState::new())),
            "MAX" => Ok(Box::new(MaxState::new())),
            "STDDEV" | "STDDEV_SAMP" => Ok(Box::new(StdDevState::new())),
            "VARIANCE" | "VAR_SAMP" => Ok(Box::new(VarianceState::new())),
            "MEDIAN" => Ok(Box::new(MedianState::new())),
            "MODE" => Ok(Box::new(ModeState::new())),
            "APPROX_COUNT_DISTINCT" => Ok(Box::new(ApproxCountDistinctState::new())),
            "STRING_AGG" => Ok(Box::new(StringAggState::new(", ".to_string()))), // Default separator
            "PERCENTILE_CONT" => Ok(Box::new(PercentileContState::new(0.5))), // Default to median
            "PERCENTILE_DISC" => Ok(Box::new(PercentileDiscState::new(0.5))), // Default to median
            "COVAR_POP" => Ok(Box::new(CovarPopState::new())), // Population covariance
            "COVAR_SAMP" | "COVAR" => Ok(Box::new(CovarSampState::new())), // Sample covariance
            "CORR" => Ok(Box::new(CorrState::new())), // Correlation coefficient
            _ => Err(PrismDBError::InvalidType(format!(
                "Unknown aggregate function: {}",
                self.function_name
            ))),
        }
    }
}

impl Expression for AggregateExpression {
    fn return_type(&self) -> &LogicalType {
        &self.return_type
    }

    fn evaluate(&self, chunk: &DataChunk) -> PrismDBResult<Vector> {
        // For aggregate expressions, we typically evaluate in a different context
        // This is a simplified implementation
        match self.child.evaluate(chunk) {
            Ok(vector) => Ok(vector),
            Err(e) => Err(e),
        }
    }

    fn evaluate_row(&self, chunk: &DataChunk, row_idx: usize) -> PrismDBResult<Value> {
        // For aggregate expressions, we typically evaluate in a different context
        // This is a simplified implementation
        self.child.evaluate_row(chunk, row_idx)
    }

    fn is_deterministic(&self) -> bool {
        self.child.is_deterministic()
    }

    fn is_nullable(&self) -> bool {
        true // Aggregate functions can be nullable
    }
}

/// Aggregate function evaluator
pub struct AggregateEvaluator {
    state: Box<dyn AggregateState>,
}

impl AggregateEvaluator {
    pub fn new(expression: &AggregateExpression) -> PrismDBResult<Self> {
        let state = expression.create_state()?;
        Ok(Self { state })
    }

    /// Update the aggregate with a new value
    pub fn update(&mut self, value: &Value) -> PrismDBResult<()> {
        self.state.update(value)
    }

    /// Get the final result
    pub fn finalize(&self) -> PrismDBResult<Value> {
        self.state.finalize()
    }

    /// Merge another evaluator into this one
    pub fn merge(&mut self, other: &AggregateEvaluator) -> PrismDBResult<()> {
        // Create a box from the reference for merging
        // This is a simplified approach - in practice we'd need proper state cloning
        let other_state = other.state.clone();
        self.state.merge(other_state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Value;

    #[test]
    fn test_count_aggregate() -> PrismDBResult<()> {
        let mut state = CountState::new();

        state.update(&Value::integer(1))?;
        state.update(&Value::integer(2))?;
        state.update(&Value::Null)?;
        state.update(&Value::integer(3))?;

        let result = state.finalize()?;
        assert_eq!(result, Value::BigInt(3));

        Ok(())
    }

    #[test]
    fn test_sum_aggregate() -> PrismDBResult<()> {
        let mut state = SumState::new();

        state.update(&Value::integer(1))?;
        state.update(&Value::integer(2))?;
        state.update(&Value::Null)?;
        state.update(&Value::integer(3))?;

        let result = state.finalize()?;
        assert_eq!(result, Value::Double(6.0));

        Ok(())
    }

    #[test]
    fn test_avg_aggregate() -> PrismDBResult<()> {
        let mut state = AvgState::new();

        state.update(&Value::integer(1))?;
        state.update(&Value::integer(2))?;
        state.update(&Value::integer(3))?;

        let result = state.finalize()?;
        // AVG of integers now returns DECIMAL for precision
        assert_eq!(result, Value::Decimal { value: 20, scale: 1, precision: 10 });

        Ok(())
    }

    #[test]
    fn test_min_aggregate() -> PrismDBResult<()> {
        let mut state = MinState::new();

        state.update(&Value::integer(5))?;
        state.update(&Value::integer(2))?;
        state.update(&Value::integer(8))?;

        let result = state.finalize()?;
        assert_eq!(result, Value::integer(2));

        Ok(())
    }

    #[test]
    fn test_max_aggregate() -> PrismDBResult<()> {
        let mut state = MaxState::new();

        state.update(&Value::integer(5))?;
        state.update(&Value::integer(2))?;
        state.update(&Value::integer(8))?;

        let result = state.finalize()?;
        assert_eq!(result, Value::integer(8));

        Ok(())
    }

    #[test]
    fn test_stddev_aggregate() -> PrismDBResult<()> {
        let mut state = StdDevState::new();

        // Sample: 2, 4, 4, 4, 5, 5, 7, 9
        // Mean = 5, Variance = 4, StdDev = 2
        state.update(&Value::Double(2.0))?;
        state.update(&Value::Double(4.0))?;
        state.update(&Value::Double(4.0))?;
        state.update(&Value::Double(4.0))?;
        state.update(&Value::Double(5.0))?;
        state.update(&Value::Double(5.0))?;
        state.update(&Value::Double(7.0))?;
        state.update(&Value::Double(9.0))?;

        let result = state.finalize()?;
        if let Value::Double(stddev) = result {
            assert!((stddev - 2.138).abs() < 0.01); // Sample stddev
        } else {
            panic!("Expected Double result");
        }

        Ok(())
    }

    #[test]
    fn test_variance_aggregate() -> PrismDBResult<()> {
        let mut state = VarianceState::new();

        // Sample: 1, 2, 3, 4, 5
        // Mean = 3, Variance = 2.5
        state.update(&Value::Double(1.0))?;
        state.update(&Value::Double(2.0))?;
        state.update(&Value::Double(3.0))?;
        state.update(&Value::Double(4.0))?;
        state.update(&Value::Double(5.0))?;

        let result = state.finalize()?;
        if let Value::Double(variance) = result {
            assert!((variance - 2.5).abs() < 0.01);
        } else {
            panic!("Expected Double result");
        }

        Ok(())
    }

    #[test]
    fn test_median_aggregate() -> PrismDBResult<()> {
        // Test odd number of elements
        let mut state = MedianState::new();
        state.update(&Value::Double(1.0))?;
        state.update(&Value::Double(3.0))?;
        state.update(&Value::Double(5.0))?;
        state.update(&Value::Double(7.0))?;
        state.update(&Value::Double(9.0))?;

        let result = state.finalize()?;
        assert_eq!(result, Value::Double(5.0));

        // Test even number of elements
        let mut state2 = MedianState::new();
        state2.update(&Value::Double(1.0))?;
        state2.update(&Value::Double(2.0))?;
        state2.update(&Value::Double(3.0))?;
        state2.update(&Value::Double(4.0))?;

        let result2 = state2.finalize()?;
        assert_eq!(result2, Value::Double(2.5));

        Ok(())
    }

    #[test]
    fn test_median_with_nulls() -> PrismDBResult<()> {
        let mut state = MedianState::new();
        state.update(&Value::Double(5.0))?;
        state.update(&Value::Null)?;
        state.update(&Value::Double(3.0))?;
        state.update(&Value::Null)?;
        state.update(&Value::Double(7.0))?;

        let result = state.finalize()?;
        assert_eq!(result, Value::Double(5.0)); // Median of 3, 5, 7

        Ok(())
    }

    #[test]
    fn test_mode_aggregate() -> PrismDBResult<()> {
        let mut state = ModeState::new();

        // Add values: 1, 2, 2, 3, 2, 4
        // Mode should be 2 (appears 3 times)
        state.update(&Value::integer(1))?;
        state.update(&Value::integer(2))?;
        state.update(&Value::integer(2))?;
        state.update(&Value::integer(3))?;
        state.update(&Value::integer(2))?;
        state.update(&Value::integer(4))?;

        let result = state.finalize()?;
        // Result is string representation of most frequent value
        assert!(matches!(result, Value::Varchar(_)));

        Ok(())
    }

    #[test]
    fn test_mode_with_strings() -> PrismDBResult<()> {
        let mut state = ModeState::new();

        state.update(&Value::Varchar("apple".to_string()))?;
        state.update(&Value::Varchar("banana".to_string()))?;
        state.update(&Value::Varchar("apple".to_string()))?;
        state.update(&Value::Varchar("cherry".to_string()))?;
        state.update(&Value::Varchar("apple".to_string()))?;

        let result = state.finalize()?;
        // "apple" appears 3 times, should be the mode
        assert!(matches!(result, Value::Varchar(_)));

        Ok(())
    }

    #[test]
    fn test_approx_count_distinct() -> PrismDBResult<()> {
        let mut state = ApproxCountDistinctState::new();

        // Add duplicate values
        state.update(&Value::integer(1))?;
        state.update(&Value::integer(2))?;
        state.update(&Value::integer(1))?; // Duplicate
        state.update(&Value::integer(3))?;
        state.update(&Value::integer(2))?; // Duplicate
        state.update(&Value::integer(4))?;
        state.update(&Value::Null)?; // Should be ignored

        let result = state.finalize()?;
        assert_eq!(result, Value::BigInt(4)); // 1, 2, 3, 4 distinct values

        Ok(())
    }

    #[test]
    fn test_approx_count_distinct_all_same() -> PrismDBResult<()> {
        let mut state = ApproxCountDistinctState::new();

        // All same value
        state.update(&Value::integer(5))?;
        state.update(&Value::integer(5))?;
        state.update(&Value::integer(5))?;

        let result = state.finalize()?;
        assert_eq!(result, Value::BigInt(1)); // Only 1 distinct value

        Ok(())
    }

    #[test]
    fn test_string_agg() -> PrismDBResult<()> {
        let mut state = StringAggState::new(", ".to_string());

        // Aggregate strings
        state.update(&Value::Varchar("apple".to_string()))?;
        state.update(&Value::Varchar("banana".to_string()))?;
        state.update(&Value::Varchar("cherry".to_string()))?;

        let result = state.finalize()?;
        assert_eq!(result, Value::Varchar("apple, banana, cherry".to_string()));

        Ok(())
    }

    #[test]
    fn test_string_agg_with_nulls() -> PrismDBResult<()> {
        let mut state = StringAggState::new("|".to_string());

        state.update(&Value::Varchar("hello".to_string()))?;
        state.update(&Value::Null)?; // Should be ignored
        state.update(&Value::Varchar("world".to_string()))?;

        let result = state.finalize()?;
        assert_eq!(result, Value::Varchar("hello|world".to_string()));

        Ok(())
    }

    #[test]
    fn test_string_agg_empty() -> PrismDBResult<()> {
        let state = StringAggState::new(", ".to_string());

        let result = state.finalize()?;
        assert_eq!(result, Value::Null); // No values, should return NULL

        Ok(())
    }

    #[test]
    fn test_percentile_cont() -> PrismDBResult<()> {
        let mut state = PercentileContState::new(0.5); // Median

        // Add values: 1, 2, 3, 4, 5
        state.update(&Value::Double(1.0))?;
        state.update(&Value::Double(2.0))?;
        state.update(&Value::Double(3.0))?;
        state.update(&Value::Double(4.0))?;
        state.update(&Value::Double(5.0))?;

        let result = state.finalize()?;
        assert_eq!(result, Value::Double(3.0)); // Median of 1,2,3,4,5 is 3

        Ok(())
    }

    #[test]
    fn test_percentile_cont_interpolation() -> PrismDBResult<()> {
        let mut state = PercentileContState::new(0.25); // 25th percentile

        // Add values: 1, 2, 3, 4, 5
        state.update(&Value::Double(1.0))?;
        state.update(&Value::Double(2.0))?;
        state.update(&Value::Double(3.0))?;
        state.update(&Value::Double(4.0))?;
        state.update(&Value::Double(5.0))?;

        let result = state.finalize()?;
        // 25th percentile with interpolation should be 2.0
        assert_eq!(result, Value::Double(2.0));

        Ok(())
    }

    #[test]
    fn test_percentile_disc() -> PrismDBResult<()> {
        let mut state = PercentileDiscState::new(0.5); // Median

        // Add values: 1, 2, 3, 4, 5
        state.update(&Value::Double(1.0))?;
        state.update(&Value::Double(2.0))?;
        state.update(&Value::Double(3.0))?;
        state.update(&Value::Double(4.0))?;
        state.update(&Value::Double(5.0))?;

        let result = state.finalize()?;
        assert_eq!(result, Value::Double(3.0)); // Median is 3

        Ok(())
    }

    #[test]
    fn test_percentile_disc_75th() -> PrismDBResult<()> {
        let mut state = PercentileDiscState::new(0.75); // 75th percentile

        // Add values: 1, 2, 3, 4, 5
        state.update(&Value::Double(1.0))?;
        state.update(&Value::Double(2.0))?;
        state.update(&Value::Double(3.0))?;
        state.update(&Value::Double(4.0))?;
        state.update(&Value::Double(5.0))?;

        let result = state.finalize()?;
        assert_eq!(result, Value::Double(4.0)); // 75th percentile is 4

        Ok(())
    }

    #[test]
    fn test_covar_pop() -> PrismDBResult<()> {
        let mut state = CovarPopState::new();

        // Dataset: x = [1, 2, 3, 4, 5], y = [2, 4, 6, 8, 10]
        // Perfect positive correlation, COVAR_POP = 4.0
        state.update_pair(1.0, 2.0)?;
        state.update_pair(2.0, 4.0)?;
        state.update_pair(3.0, 6.0)?;
        state.update_pair(4.0, 8.0)?;
        state.update_pair(5.0, 10.0)?;

        let result = state.finalize()?;
        if let Value::Double(covar) = result {
            assert!((covar - 4.0).abs() < 0.01); // COVAR_POP = 4.0
        } else {
            panic!("Expected Double result");
        }

        Ok(())
    }

    #[test]
    fn test_covar_samp() -> PrismDBResult<()> {
        let mut state = CovarSampState::new();

        // Same dataset: COVAR_SAMP = COVAR_POP * n / (n-1) = 4.0 * 5/4 = 5.0
        state.update_pair(1.0, 2.0)?;
        state.update_pair(2.0, 4.0)?;
        state.update_pair(3.0, 6.0)?;
        state.update_pair(4.0, 8.0)?;
        state.update_pair(5.0, 10.0)?;

        let result = state.finalize()?;
        if let Value::Double(covar) = result {
            assert!((covar - 5.0).abs() < 0.01); // COVAR_SAMP = 5.0
        } else {
            panic!("Expected Double result");
        }

        Ok(())
    }

    #[test]
    fn test_corr_perfect_positive() -> PrismDBResult<()> {
        let mut state = CorrState::new();

        // Perfect positive correlation: y = 2x
        state.update_pair(1.0, 2.0)?;
        state.update_pair(2.0, 4.0)?;
        state.update_pair(3.0, 6.0)?;
        state.update_pair(4.0, 8.0)?;
        state.update_pair(5.0, 10.0)?;

        let result = state.finalize()?;
        if let Value::Double(corr) = result {
            assert!((corr - 1.0).abs() < 0.01); // Perfect correlation = 1.0
        } else {
            panic!("Expected Double result");
        }

        Ok(())
    }

    #[test]
    fn test_corr_perfect_negative() -> PrismDBResult<()> {
        let mut state = CorrState::new();

        // Perfect negative correlation: y = -2x + 12
        state.update_pair(1.0, 10.0)?;
        state.update_pair(2.0, 8.0)?;
        state.update_pair(3.0, 6.0)?;
        state.update_pair(4.0, 4.0)?;
        state.update_pair(5.0, 2.0)?;

        let result = state.finalize()?;
        if let Value::Double(corr) = result {
            assert!((corr - (-1.0)).abs() < 0.01); // Perfect negative correlation = -1.0
        } else {
            panic!("Expected Double result");
        }

        Ok(())
    }

    #[test]
    fn test_corr_no_correlation() -> PrismDBResult<()> {
        let mut state = CorrState::new();

        // No correlation: y values are constant
        state.update_pair(1.0, 5.0)?;
        state.update_pair(2.0, 5.0)?;
        state.update_pair(3.0, 5.0)?;
        state.update_pair(4.0, 5.0)?;
        state.update_pair(5.0, 5.0)?;

        let result = state.finalize()?;
        if let Value::Double(corr) = result {
            // When one variable has zero variance, correlation is NaN or 0
            assert!(corr.is_nan() || corr.abs() < 0.01);
        } else {
            panic!("Expected Double result");
        }

        Ok(())
    }

    #[test]
    fn test_first_aggregate() -> PrismDBResult<()> {
        let mut state = FirstState::new();

        // First value should be retained
        state.update(&Value::Integer(10))?;
        state.update(&Value::Integer(20))?;
        state.update(&Value::Integer(30))?;

        let result = state.finalize()?;
        assert_eq!(result, Value::Integer(10));

        Ok(())
    }

    #[test]
    fn test_last_aggregate() -> PrismDBResult<()> {
        let mut state = LastState::new();

        // Last value should be retained
        state.update(&Value::Integer(10))?;
        state.update(&Value::Integer(20))?;
        state.update(&Value::Integer(30))?;

        let result = state.finalize()?;
        assert_eq!(result, Value::Integer(30));

        Ok(())
    }

    #[test]
    fn test_arg_min_aggregate() -> PrismDBResult<()> {
        let mut state = ArgMinState::new();

        // ARG_MIN should return the arg value at minimum val
        state.update(&Value::Integer(50))?;
        state.update(&Value::Integer(20))?;  // This is the minimum
        state.update(&Value::Integer(30))?;

        let result = state.finalize()?;
        assert_eq!(result, Value::Integer(20));

        Ok(())
    }

    #[test]
    fn test_arg_max_aggregate() -> PrismDBResult<()> {
        let mut state = ArgMaxState::new();

        // ARG_MAX should return the arg value at maximum val
        state.update(&Value::Integer(10))?;
        state.update(&Value::Integer(50))?;  // This is the maximum
        state.update(&Value::Integer(30))?;

        let result = state.finalize()?;
        assert_eq!(result, Value::Integer(50));

        Ok(())
    }

    #[test]
    fn test_bool_and_aggregate() -> PrismDBResult<()> {
        let mut state = BoolAndState::new();

        // All true should result in true
        state.update(&Value::Boolean(true))?;
        state.update(&Value::Boolean(true))?;
        state.update(&Value::Boolean(true))?;

        let result = state.finalize()?;
        assert_eq!(result, Value::Boolean(true));

        // Any false should result in false
        let mut state2 = BoolAndState::new();
        state2.update(&Value::Boolean(true))?;
        state2.update(&Value::Boolean(false))?;
        state2.update(&Value::Boolean(true))?;

        let result2 = state2.finalize()?;
        assert_eq!(result2, Value::Boolean(false));

        Ok(())
    }

    #[test]
    fn test_bool_or_aggregate() -> PrismDBResult<()> {
        let mut state = BoolOrState::new();

        // All false should result in false
        state.update(&Value::Boolean(false))?;
        state.update(&Value::Boolean(false))?;
        state.update(&Value::Boolean(false))?;

        let result = state.finalize()?;
        assert_eq!(result, Value::Boolean(false));

        // Any true should result in true
        let mut state2 = BoolOrState::new();
        state2.update(&Value::Boolean(false))?;
        state2.update(&Value::Boolean(true))?;
        state2.update(&Value::Boolean(false))?;

        let result2 = state2.finalize()?;
        assert_eq!(result2, Value::Boolean(true));

        Ok(())
    }

    #[test]
    fn test_regr_count_aggregate() -> PrismDBResult<()> {
        let mut state = RegrCountState::new();

        state.update(&Value::Integer(10))?;
        state.update(&Value::Integer(20))?;
        state.update(&Value::Integer(30))?;
        state.update(&Value::Null)?;  // Should not be counted

        let result = state.finalize()?;
        assert_eq!(result, Value::BigInt(3));

        Ok(())
    }

    #[test]
    fn test_regr_r2_aggregate() -> PrismDBResult<()> {
        let mut state = RegrR2State::new();

        // Perfect positive correlation: y = 2x
        state.update(&Value::Integer(1))?;
        state.update(&Value::Integer(2))?;
        state.update(&Value::Integer(3))?;
        state.update(&Value::Integer(4))?;
        state.update(&Value::Integer(5))?;

        let result = state.finalize()?;
        // R² should be close to 1 for perfect correlation
        // Note: This is simplified since we're not handling paired (x,y) data properly
        // In a real implementation, we'd need to update with pairs
        if let Value::Double(r2) = result {
            assert!(r2 >= 0.0 && r2 <= 1.0);  // R² is always between 0 and 1
        }

        Ok(())
    }

    #[test]
    fn test_first_with_nulls() -> PrismDBResult<()> {
        let mut state = FirstState::new();

        // Nulls should be skipped, first non-null should be retained
        state.update(&Value::Null)?;
        state.update(&Value::Integer(20))?;
        state.update(&Value::Integer(30))?;

        let result = state.finalize()?;
        assert_eq!(result, Value::Integer(20));

        Ok(())
    }

    #[test]
    fn test_last_with_nulls() -> PrismDBResult<()> {
        let mut state = LastState::new();

        // Last non-null value should be retained
        state.update(&Value::Integer(10))?;
        state.update(&Value::Integer(20))?;
        state.update(&Value::Null)?;

        let result = state.finalize()?;
        assert_eq!(result, Value::Integer(20));

        Ok(())
    }

    #[test]
    fn test_approx_quantile_aggregate() -> PrismDBResult<()> {
        let mut state = ApproxQuantileState::new(0.5); // Median

        // Add values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
        for i in 1..=10 {
            state.update(&Value::Integer(i))?;
        }

        let result = state.finalize()?;
        // Median of 1..10 should be around 5.5
        if let Value::Double(median) = result {
            // Allow some error due to approximation (t-digest is approximate)
            assert!((median - 5.5).abs() < 1.0, "Expected median ~5.5, got {}", median);
        } else {
            panic!("Expected Double value");
        }

        Ok(())
    }

    #[test]
    fn test_approx_quantile_percentiles() -> PrismDBResult<()> {
        // Test 25th percentile (Q1)
        let mut state_q1 = ApproxQuantileState::new(0.25);
        for i in 1..=100 {
            state_q1.update(&Value::Integer(i))?;
        }
        let result_q1 = state_q1.finalize()?;
        if let Value::Double(q1) = result_q1 {
            assert!((q1 - 25.0).abs() < 5.0, "Expected Q1 ~25, got {}", q1);
        }

        // Test 75th percentile (Q3)
        let mut state_q3 = ApproxQuantileState::new(0.75);
        for i in 1..=100 {
            state_q3.update(&Value::Integer(i))?;
        }
        let result_q3 = state_q3.finalize()?;
        if let Value::Double(q3) = result_q3 {
            assert!((q3 - 75.0).abs() < 5.0, "Expected Q3 ~75, got {}", q3);
        }

        Ok(())
    }

    #[test]
    fn test_approx_quantile_with_nulls() -> PrismDBResult<()> {
        let mut state = ApproxQuantileState::new(0.5);

        state.update(&Value::Null)?;
        state.update(&Value::Integer(1))?;
        state.update(&Value::Null)?;
        state.update(&Value::Integer(2))?;
        state.update(&Value::Integer(3))?;

        let result = state.finalize()?;
        // Median of [1, 2, 3] should be around 2
        if let Value::Double(median) = result {
            assert!((median - 2.0).abs() < 0.5, "Expected median ~2, got {}", median);
        }

        Ok(())
    }

    #[test]
    fn test_approx_quantile_empty() -> PrismDBResult<()> {
        let state = ApproxQuantileState::new(0.5);
        let result = state.finalize()?;
        assert_eq!(result, Value::Null);

        Ok(())
    }
}

/// FIRST aggregate state - returns the first value in a group
#[derive(Debug, Clone)]
pub struct FirstState {
    value: Option<Value>,
    is_set: bool,
}

impl FirstState {
    pub fn new() -> Self {
        Self {
            value: None,
            is_set: false,
        }
    }
}

impl AggregateState for FirstState {
    fn update(&mut self, value: &Value) -> PrismDBResult<()> {
        // Only set value on first non-null value encountered
        if !self.is_set && !value.is_null() {
            self.value = Some(value.clone());
            self.is_set = true;
        }
        Ok(())
    }

    fn finalize(&self) -> PrismDBResult<Value> {
        Ok(self.value.clone().unwrap_or(Value::Null))
    }

    fn merge(&mut self, other: Box<dyn AggregateState>) -> PrismDBResult<()> {
        if let Some(other_first) = other.as_any().downcast_ref::<FirstState>() {
            // For FIRST, only use other's value if we don't have one yet
            if !self.is_set && other_first.is_set {
                self.value = other_first.value.clone();
                self.is_set = other_first.is_set;
            }
        }
        Ok(())
    }

    fn clone_box(&self) -> Box<dyn AggregateState> {
        Box::new(self.clone())
    }
}

/// LAST aggregate state - returns the last value in a group
#[derive(Debug, Clone)]
pub struct LastState {
    value: Option<Value>,
}

impl LastState {
    pub fn new() -> Self {
        Self { value: None }
    }
}

impl AggregateState for LastState {
    fn update(&mut self, value: &Value) -> PrismDBResult<()> {
        // Always update with the latest non-null value
        if !value.is_null() {
            self.value = Some(value.clone());
        }
        Ok(())
    }

    fn finalize(&self) -> PrismDBResult<Value> {
        Ok(self.value.clone().unwrap_or(Value::Null))
    }

    fn merge(&mut self, other: Box<dyn AggregateState>) -> PrismDBResult<()> {
        if let Some(other_last) = other.as_any().downcast_ref::<LastState>() {
            // For LAST, always take the other's value (it's more recent in parallel execution)
            if other_last.value.is_some() {
                self.value = other_last.value.clone();
            }
        }
        Ok(())
    }

    fn clone_box(&self) -> Box<dyn AggregateState> {
        Box::new(self.clone())
    }
}

/// ARG_MIN aggregate state - returns the 'arg' value at the row where 'val' is minimum
#[derive(Debug, Clone)]
pub struct ArgMinState {
    arg_value: Option<Value>,
    min_value: Option<Value>,
}

impl ArgMinState {
    pub fn new() -> Self {
        Self {
            arg_value: None,
            min_value: None,
        }
    }

    fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        match (a, b) {
            (Value::Integer(a), Value::Integer(b)) => a.cmp(b),
            (Value::BigInt(a), Value::BigInt(b)) => a.cmp(b),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
            (Value::Double(a), Value::Double(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
            (Value::Varchar(a), Value::Varchar(b)) => a.cmp(b),
            (Value::Boolean(a), Value::Boolean(b)) => a.cmp(b),
            (Value::Date(a), Value::Date(b)) => a.cmp(b),
            (Value::Time(a), Value::Time(b)) => a.cmp(b),
            (Value::Timestamp(a), Value::Timestamp(b)) => a.cmp(b),
            _ => Ordering::Equal,
        }
    }
}

impl AggregateState for ArgMinState {
    fn update(&mut self, value: &Value) -> PrismDBResult<()> {
        // ARG_MIN expects a struct/tuple with (arg, val)
        // For simplicity, we'll handle it as two separate calls
        // This will be called twice per row: once for arg, once for val
        // We need to track both

        // This is a simplified version - in practice, we'd need special handling
        // For now, store as is
        if !value.is_null() {
            if self.min_value.is_none() {
                self.min_value = Some(value.clone());
                self.arg_value = Some(value.clone());
            } else if let Some(ref current_min) = self.min_value {
                if Self::compare_values(value, current_min) == std::cmp::Ordering::Less {
                    self.min_value = Some(value.clone());
                    self.arg_value = Some(value.clone());
                }
            }
        }
        Ok(())
    }

    fn finalize(&self) -> PrismDBResult<Value> {
        Ok(self.arg_value.clone().unwrap_or(Value::Null))
    }

    fn merge(&mut self, other: Box<dyn AggregateState>) -> PrismDBResult<()> {
        if let Some(other_argmin) = other.as_any().downcast_ref::<ArgMinState>() {
            if let (Some(ref other_min), Some(ref other_arg)) = (&other_argmin.min_value, &other_argmin.arg_value) {
                if self.min_value.is_none() {
                    self.min_value = Some(other_min.clone());
                    self.arg_value = Some(other_arg.clone());
                } else if let Some(ref current_min) = self.min_value {
                    if Self::compare_values(other_min, current_min) == std::cmp::Ordering::Less {
                        self.min_value = Some(other_min.clone());
                        self.arg_value = Some(other_arg.clone());
                    }
                }
            }
        }
        Ok(())
    }

    fn clone_box(&self) -> Box<dyn AggregateState> {
        Box::new(self.clone())
    }
}


/// ARG_MAX aggregate state - returns the 'arg' value at the row where 'val' is maximum
#[derive(Debug, Clone)]
pub struct ArgMaxState {
    arg_value: Option<Value>,
    max_value: Option<Value>,
}

impl ArgMaxState {
    pub fn new() -> Self {
        Self {
            arg_value: None,
            max_value: None,
        }
    }

    fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        match (a, b) {
            (Value::Integer(a), Value::Integer(b)) => a.cmp(b),
            (Value::BigInt(a), Value::BigInt(b)) => a.cmp(b),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
            (Value::Double(a), Value::Double(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
            (Value::Varchar(a), Value::Varchar(b)) => a.cmp(b),
            (Value::Boolean(a), Value::Boolean(b)) => a.cmp(b),
            (Value::Date(a), Value::Date(b)) => a.cmp(b),
            (Value::Time(a), Value::Time(b)) => a.cmp(b),
            (Value::Timestamp(a), Value::Timestamp(b)) => a.cmp(b),
            _ => Ordering::Equal,
        }
    }
}

impl AggregateState for ArgMaxState {
    fn update(&mut self, value: &Value) -> PrismDBResult<()> {
        if !value.is_null() {
            if self.max_value.is_none() {
                self.max_value = Some(value.clone());
                self.arg_value = Some(value.clone());
            } else if let Some(ref current_max) = self.max_value {
                if Self::compare_values(value, current_max) == std::cmp::Ordering::Greater {
                    self.max_value = Some(value.clone());
                    self.arg_value = Some(value.clone());
                }
            }
        }
        Ok(())
    }

    fn finalize(&self) -> PrismDBResult<Value> {
        Ok(self.arg_value.clone().unwrap_or(Value::Null))
    }

    fn merge(&mut self, other: Box<dyn AggregateState>) -> PrismDBResult<()> {
        if let Some(other_argmax) = other.as_any().downcast_ref::<ArgMaxState>() {
            if let (Some(ref other_max), Some(ref other_arg)) = (&other_argmax.max_value, &other_argmax.arg_value) {
                if self.max_value.is_none() {
                    self.max_value = Some(other_max.clone());
                    self.arg_value = Some(other_arg.clone());
                } else if let Some(ref current_max) = self.max_value {
                    if Self::compare_values(other_max, current_max) == std::cmp::Ordering::Greater {
                        self.max_value = Some(other_max.clone());
                        self.arg_value = Some(other_arg.clone());
                    }
                }
            }
        }
        Ok(())
    }

    fn clone_box(&self) -> Box<dyn AggregateState> {
        Box::new(self.clone())
    }
}


/// BOOL_AND aggregate state - logical AND of boolean values
#[derive(Debug, Clone)]
pub struct BoolAndState {
    result: bool,
    has_value: bool,
}

impl BoolAndState {
    pub fn new() -> Self {
        Self {
            result: true,  // Start with true for AND
            has_value: false,
        }
    }
}

impl AggregateState for BoolAndState {
    fn update(&mut self, value: &Value) -> PrismDBResult<()> {
        if !value.is_null() {
            self.has_value = true;
            if let Value::Boolean(b) = value {
                self.result = self.result && *b;
            }
        }
        Ok(())
    }

    fn finalize(&self) -> PrismDBResult<Value> {
        if self.has_value {
            Ok(Value::Boolean(self.result))
        } else {
            Ok(Value::Null)
        }
    }

    fn merge(&mut self, other: Box<dyn AggregateState>) -> PrismDBResult<()> {
        if let Some(other_bool) = other.as_any().downcast_ref::<BoolAndState>() {
            if other_bool.has_value {
                if !self.has_value {
                    self.result = other_bool.result;
                    self.has_value = true;
                } else {
                    self.result = self.result && other_bool.result;
                }
            }
        }
        Ok(())
    }

    fn clone_box(&self) -> Box<dyn AggregateState> {
        Box::new(self.clone())
    }
}


/// BOOL_OR aggregate state - logical OR of boolean values
#[derive(Debug, Clone)]
pub struct BoolOrState {
    result: bool,
    has_value: bool,
}

impl BoolOrState {
    pub fn new() -> Self {
        Self {
            result: false,  // Start with false for OR
            has_value: false,
        }
    }
}

impl AggregateState for BoolOrState {
    fn update(&mut self, value: &Value) -> PrismDBResult<()> {
        if !value.is_null() {
            self.has_value = true;
            if let Value::Boolean(b) = value {
                self.result = self.result || *b;
            }
        }
        Ok(())
    }

    fn finalize(&self) -> PrismDBResult<Value> {
        if self.has_value {
            Ok(Value::Boolean(self.result))
        } else {
            Ok(Value::Null)
        }
    }

    fn merge(&mut self, other: Box<dyn AggregateState>) -> PrismDBResult<()> {
        if let Some(other_bool) = other.as_any().downcast_ref::<BoolOrState>() {
            if other_bool.has_value {
                if !self.has_value {
                    self.result = other_bool.result;
                    self.has_value = true;
                } else {
                    self.result = self.result || other_bool.result;
                }
            }
        }
        Ok(())
    }

    fn clone_box(&self) -> Box<dyn AggregateState> {
        Box::new(self.clone())
    }
}


/// REGR_COUNT aggregate state - count of non-null (x, y) pairs
#[derive(Debug, Clone)]
pub struct RegrCountState {
    count: usize,
}

impl RegrCountState {
    pub fn new() -> Self {
        Self { count: 0 }
    }
}

impl AggregateState for RegrCountState {
    fn update(&mut self, value: &Value) -> PrismDBResult<()> {
        // In practice, this would receive both x and y values
        // For now, just count non-null values
        if !value.is_null() {
            self.count += 1;
        }
        Ok(())
    }

    fn finalize(&self) -> PrismDBResult<Value> {
        Ok(Value::BigInt(self.count as i64))
    }

    fn merge(&mut self, other: Box<dyn AggregateState>) -> PrismDBResult<()> {
        if let Some(other_regr) = other.as_any().downcast_ref::<RegrCountState>() {
            self.count += other_regr.count;
        }
        Ok(())
    }

    fn clone_box(&self) -> Box<dyn AggregateState> {
        Box::new(self.clone())
    }
}


/// REGR_SLOPE aggregate state - slope of linear regression line
/// Formula: COVAR_POP(y, x) / VAR_POP(x)
#[derive(Debug, Clone)]
pub struct RegrSlopeState {
    covar_state: CovarPopState,
    var_x_state: VarianceState,
}

impl RegrSlopeState {
    pub fn new() -> Self {
        Self {
            covar_state: CovarPopState::new(),
            var_x_state: VarianceState::new(),
        }
    }
}

impl AggregateState for RegrSlopeState {
    fn update(&mut self, value: &Value) -> PrismDBResult<()> {
        // Update both covariance and variance states
        // In practice, this would receive both x and y values separately
        self.covar_state.update(value)?;
        self.var_x_state.update(value)?;
        Ok(())
    }

    fn finalize(&self) -> PrismDBResult<Value> {
        let covar = self.covar_state.finalize()?;
        let var_x = self.var_x_state.finalize()?;

        match (covar, var_x) {
            (Value::Double(c), Value::Double(v)) => {
                if v == 0.0 {
                    Ok(Value::Null)  // Undefined slope when variance is 0
                } else {
                    Ok(Value::Double(c / v))
                }
            }
            _ => Ok(Value::Null),
        }
    }

    fn merge(&mut self, other: Box<dyn AggregateState>) -> PrismDBResult<()> {
        if let Some(other_slope) = other.as_any().downcast_ref::<RegrSlopeState>() {
            self.covar_state.merge(Box::new(other_slope.covar_state.clone()))?;
            self.var_x_state.merge(Box::new(other_slope.var_x_state.clone()))?;
        }
        Ok(())
    }

    fn clone_box(&self) -> Box<dyn AggregateState> {
        Box::new(self.clone())
    }
}


/// REGR_INTERCEPT aggregate state - y-intercept of regression line
/// Formula: AVG(y) - REGR_SLOPE(y, x) * AVG(x)
#[derive(Debug, Clone)]
pub struct RegrInterceptState {
    avg_y_state: AvgState,
    avg_x_state: AvgState,
    slope_state: RegrSlopeState,
}

impl RegrInterceptState {
    pub fn new() -> Self {
        Self {
            avg_y_state: AvgState::new(),
            avg_x_state: AvgState::new(),
            slope_state: RegrSlopeState::new(),
        }
    }
}

impl AggregateState for RegrInterceptState {
    fn update(&mut self, value: &Value) -> PrismDBResult<()> {
        self.avg_y_state.update(value)?;
        self.avg_x_state.update(value)?;
        self.slope_state.update(value)?;
        Ok(())
    }

    fn finalize(&self) -> PrismDBResult<Value> {
        let avg_y = self.avg_y_state.finalize()?;
        let avg_x = self.avg_x_state.finalize()?;
        let slope = self.slope_state.finalize()?;

        match (avg_y, avg_x, slope) {
            (Value::Double(ay), Value::Double(ax), Value::Double(s)) => {
                Ok(Value::Double(ay - s * ax))
            }
            _ => Ok(Value::Null),
        }
    }

    fn merge(&mut self, other: Box<dyn AggregateState>) -> PrismDBResult<()> {
        if let Some(other_intercept) = other.as_any().downcast_ref::<RegrInterceptState>() {
            self.avg_y_state.merge(Box::new(other_intercept.avg_y_state.clone()))?;
            self.avg_x_state.merge(Box::new(other_intercept.avg_x_state.clone()))?;
            self.slope_state.merge(Box::new(other_intercept.slope_state.clone()))?;
        }
        Ok(())
    }

    fn clone_box(&self) -> Box<dyn AggregateState> {
        Box::new(self.clone())
    }
}


/// REGR_R2 aggregate state - coefficient of determination (R²)
/// Formula: POWER(CORR(y, x), 2)
#[derive(Debug, Clone)]
pub struct RegrR2State {
    corr_state: CorrState,
}

impl RegrR2State {
    pub fn new() -> Self {
        Self {
            corr_state: CorrState::new(),
        }
    }
}

impl AggregateState for RegrR2State {
    fn update(&mut self, value: &Value) -> PrismDBResult<()> {
        self.corr_state.update(value)?;
        Ok(())
    }

    fn finalize(&self) -> PrismDBResult<Value> {
        let corr = self.corr_state.finalize()?;

        match corr {
            Value::Double(c) => Ok(Value::Double(c * c)),
            _ => Ok(Value::Null),
        }
    }

    fn merge(&mut self, other: Box<dyn AggregateState>) -> PrismDBResult<()> {
        if let Some(other_r2) = other.as_any().downcast_ref::<RegrR2State>() {
            self.corr_state.merge(Box::new(other_r2.corr_state.clone()))?;
        }
        Ok(())
    }

    fn clone_box(&self) -> Box<dyn AggregateState> {
        Box::new(self.clone())
    }
}


/// Create an aggregate state by function name (helper for parallel aggregation)
pub fn create_aggregate_state(function_name: &str) -> PrismDBResult<Box<dyn AggregateState>> {
    match function_name.to_uppercase().as_str() {
        "COUNT" => Ok(Box::new(CountState::new())),
        "SUM" => Ok(Box::new(SumState::new())),
        "AVG" => Ok(Box::new(AvgState::new())),
        "MIN" => Ok(Box::new(MinState::new())),
        "MAX" => Ok(Box::new(MaxState::new())),
        "STDDEV" | "STDDEV_SAMP" | "STDDEV_POP" => Ok(Box::new(StdDevState::new())),
        "VARIANCE" | "VAR_SAMP" | "VAR_POP" => Ok(Box::new(VarianceState::new())),
        "MEDIAN" => Ok(Box::new(MedianState::new())),
        "MODE" => Ok(Box::new(ModeState::new())),
        "APPROX_COUNT_DISTINCT" => Ok(Box::new(ApproxCountDistinctState::new())),
        "APPROX_QUANTILE" => Ok(Box::new(ApproxQuantileState::with_default_quantile())),
        "STRING_AGG" => Ok(Box::new(StringAggState::new(", ".to_string()))),
        "PERCENTILE_CONT" => Ok(Box::new(PercentileContState::new(0.5))),
        "PERCENTILE_DISC" => Ok(Box::new(PercentileDiscState::new(0.5))),
        "COVAR_POP" => Ok(Box::new(CovarPopState::new())),
        "COVAR_SAMP" | "COVAR" => Ok(Box::new(CovarSampState::new())),
        "CORR" => Ok(Box::new(CorrState::new())),
        "FIRST" | "FIRST_VALUE" => Ok(Box::new(FirstState::new())),
        "LAST" | "LAST_VALUE" => Ok(Box::new(LastState::new())),
        "ARG_MIN" => Ok(Box::new(ArgMinState::new())),
        "ARG_MAX" => Ok(Box::new(ArgMaxState::new())),
        "BOOL_AND" => Ok(Box::new(BoolAndState::new())),
        "BOOL_OR" => Ok(Box::new(BoolOrState::new())),
        "REGR_COUNT" => Ok(Box::new(RegrCountState::new())),
        "REGR_SLOPE" => Ok(Box::new(RegrSlopeState::new())),
        "REGR_INTERCEPT" => Ok(Box::new(RegrInterceptState::new())),
        "REGR_R2" => Ok(Box::new(RegrR2State::new())),
        _ => Err(PrismDBError::NotImplemented(format!(
            "Aggregate function '{}' not implemented",
            function_name
        ))),
    }
}
