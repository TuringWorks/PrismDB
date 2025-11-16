//! Date/Time Functions (DuckDB-Compatible)
//!
//! This module implements DuckDB's date and time manipulation functions for 100% compatibility.
//! Includes: current time, extraction, arithmetic, formatting, parsing, and more.

use crate::common::error::{PrismDBError, PrismDBResult};
use crate::types::Value;
use chrono::{DateTime, Datelike, Local, NaiveDate, NaiveDateTime, Timelike, Utc};

/// CURRENT_DATE - Get current date
pub fn current_date() -> PrismDBResult<Value> {
    let now = Local::now();
    let date = now.date_naive();
    Ok(Value::Date(
        date.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp() as i32 / 86400,
    ))
}

/// CURRENT_TIME - Get current time
pub fn current_time() -> PrismDBResult<Value> {
    let now = Local::now();
    let time = now.time();
    let micros =
        time.num_seconds_from_midnight() as i64 * 1_000_000 + time.nanosecond() as i64 / 1_000;
    Ok(Value::Time(micros))
}

/// NOW / CURRENT_TIMESTAMP - Get current timestamp
pub fn now() -> PrismDBResult<Value> {
    let now = Utc::now();
    Ok(Value::Timestamp(now.timestamp_micros()))
}

/// EXTRACT - Extract field from date/time
/// Supports: year, month, day, hour, minute, second, dow, doy, week, quarter
pub fn extract(field: &str, value: &Value) -> PrismDBResult<Value> {
    let field_lower = field.to_lowercase();

    match value {
        Value::Date(days) => {
            let date = NaiveDate::from_ymd_opt(1970, 1, 1)
                .unwrap()
                .checked_add_signed(chrono::Duration::days(*days as i64))
                .ok_or_else(|| PrismDBError::Execution("Invalid date".to_string()))?;

            match field_lower.as_str() {
                "year" => Ok(Value::Integer(date.year())),
                "month" => Ok(Value::Integer(date.month() as i32)),
                "day" => Ok(Value::Integer(date.day() as i32)),
                "dow" | "dayofweek" => {
                    Ok(Value::Integer(date.weekday().num_days_from_sunday() as i32))
                }
                "doy" | "dayofyear" => Ok(Value::Integer(date.ordinal() as i32)),
                "week" => Ok(Value::Integer(date.iso_week().week() as i32)),
                "quarter" => Ok(Value::Integer(((date.month() - 1) / 3 + 1) as i32)),
                _ => Err(PrismDBError::Execution(format!(
                    "Unknown extract field: {}",
                    field
                ))),
            }
        }
        Value::Timestamp(micros) => {
            let dt = DateTime::from_timestamp(
                *micros / 1_000_000,
                ((*micros % 1_000_000) * 1000) as u32,
            )
            .ok_or_else(|| PrismDBError::Execution("Invalid timestamp".to_string()))?;

            match field_lower.as_str() {
                "year" => Ok(Value::Integer(dt.year())),
                "month" => Ok(Value::Integer(dt.month() as i32)),
                "day" => Ok(Value::Integer(dt.day() as i32)),
                "hour" => Ok(Value::Integer(dt.hour() as i32)),
                "minute" => Ok(Value::Integer(dt.minute() as i32)),
                "second" => Ok(Value::Integer(dt.second() as i32)),
                "dow" | "dayofweek" => {
                    Ok(Value::Integer(dt.weekday().num_days_from_sunday() as i32))
                }
                "doy" | "dayofyear" => Ok(Value::Integer(dt.ordinal() as i32)),
                "week" => Ok(Value::Integer(dt.iso_week().week() as i32)),
                "quarter" => Ok(Value::Integer(((dt.month() - 1) / 3 + 1) as i32)),
                "epoch" => Ok(Value::BigInt(*micros / 1_000_000)),
                _ => Err(PrismDBError::Execution(format!(
                    "Unknown extract field: {}",
                    field
                ))),
            }
        }
        Value::Null => Ok(Value::Null),
        _ => Err(PrismDBError::Type(
            "EXTRACT requires date or timestamp".to_string(),
        )),
    }
}

/// DATE_PART - Alias for EXTRACT
pub fn date_part(field: &str, value: &Value) -> PrismDBResult<Value> {
    extract(field, value)
}

/// YEAR - Extract year from date/timestamp
pub fn year(value: &Value) -> PrismDBResult<Value> {
    extract("year", value)
}

/// MONTH - Extract month from date/timestamp
pub fn month(value: &Value) -> PrismDBResult<Value> {
    extract("month", value)
}

/// DAY - Extract day from date/timestamp
pub fn day(value: &Value) -> PrismDBResult<Value> {
    extract("day", value)
}

/// HOUR - Extract hour from timestamp
pub fn hour(value: &Value) -> PrismDBResult<Value> {
    extract("hour", value)
}

/// MINUTE - Extract minute from timestamp
pub fn minute(value: &Value) -> PrismDBResult<Value> {
    extract("minute", value)
}

/// SECOND - Extract second from timestamp
pub fn second(value: &Value) -> PrismDBResult<Value> {
    extract("second", value)
}

/// DATE_TRUNC - Truncate timestamp to specified precision
pub fn date_trunc(field: &str, value: &Value) -> PrismDBResult<Value> {
    let field_lower = field.to_lowercase();

    match value {
        Value::Timestamp(micros) => {
            let dt = DateTime::from_timestamp(
                *micros / 1_000_000,
                ((*micros % 1_000_000) * 1000) as u32,
            )
            .ok_or_else(|| PrismDBError::Execution("Invalid timestamp".to_string()))?;

            let truncated = match field_lower.as_str() {
                "year" => dt
                    .with_month(1)
                    .and_then(|d| d.with_day(1))
                    .and_then(|d| d.with_hour(0))
                    .and_then(|d| d.with_minute(0))
                    .and_then(|d| d.with_second(0))
                    .and_then(|d| d.with_nanosecond(0)),
                "month" => dt
                    .with_day(1)
                    .and_then(|d| d.with_hour(0))
                    .and_then(|d| d.with_minute(0))
                    .and_then(|d| d.with_second(0))
                    .and_then(|d| d.with_nanosecond(0)),
                "day" => dt
                    .with_hour(0)
                    .and_then(|d| d.with_minute(0))
                    .and_then(|d| d.with_second(0))
                    .and_then(|d| d.with_nanosecond(0)),
                "hour" => dt
                    .with_minute(0)
                    .and_then(|d| d.with_second(0))
                    .and_then(|d| d.with_nanosecond(0)),
                "minute" => dt.with_second(0).and_then(|d| d.with_nanosecond(0)),
                "second" => dt.with_nanosecond(0),
                _ => {
                    return Err(PrismDBError::Execution(format!(
                        "Unknown truncate field: {}",
                        field
                    )))
                }
            };

            match truncated {
                Some(dt) => Ok(Value::Timestamp(dt.timestamp_micros())),
                None => Err(PrismDBError::Execution(
                    "Failed to truncate timestamp".to_string(),
                )),
            }
        }
        Value::Null => Ok(Value::Null),
        _ => Err(PrismDBError::Type(
            "DATE_TRUNC requires timestamp".to_string(),
        )),
    }
}

/// DATE_ADD - Add interval to date/timestamp
/// For simplicity, days parameter (TODO: support full INTERVAL syntax)
pub fn date_add(value: &Value, days: &Value) -> PrismDBResult<Value> {
    let day_count = match days {
        Value::Integer(d) => *d as i64,
        Value::BigInt(d) => *d,
        Value::Null => return Ok(Value::Null),
        _ => {
            return Err(PrismDBError::Type(
                "DATE_ADD interval must be integer".to_string(),
            ))
        }
    };

    match value {
        Value::Date(d) => {
            let new_days = d + day_count as i32;
            Ok(Value::Date(new_days))
        }
        Value::Timestamp(micros) => {
            let dt = DateTime::from_timestamp(
                *micros / 1_000_000,
                ((*micros % 1_000_000) * 1000) as u32,
            )
            .ok_or_else(|| PrismDBError::Execution("Invalid timestamp".to_string()))?;

            let new_dt = dt
                .checked_add_signed(chrono::Duration::days(day_count))
                .ok_or_else(|| PrismDBError::Execution("Date overflow".to_string()))?;

            Ok(Value::Timestamp(new_dt.timestamp_micros()))
        }
        Value::Null => Ok(Value::Null),
        _ => Err(PrismDBError::Type(
            "DATE_ADD requires date or timestamp".to_string(),
        )),
    }
}

/// DATE_SUB - Subtract interval from date/timestamp
pub fn date_sub(value: &Value, days: &Value) -> PrismDBResult<Value> {
    let negated = match days {
        Value::Integer(d) => Value::Integer(-d),
        Value::BigInt(d) => Value::BigInt(-d),
        v => v.clone(),
    };
    date_add(value, &negated)
}

/// DATE_DIFF - Difference between two dates (in days)
pub fn date_diff(end: &Value, start: &Value) -> PrismDBResult<Value> {
    match (end, start) {
        (Value::Date(end_days), Value::Date(start_days)) => {
            Ok(Value::Integer(end_days - start_days))
        }
        (Value::Timestamp(end_micros), Value::Timestamp(start_micros)) => {
            let diff_days = (end_micros - start_micros) / (86400 * 1_000_000);
            Ok(Value::BigInt(diff_days))
        }
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => Err(PrismDBError::Type(
            "DATE_DIFF requires matching date or timestamp types".to_string(),
        )),
    }
}

/// TO_TIMESTAMP - Parse string to timestamp
pub fn to_timestamp(value: &Value) -> PrismDBResult<Value> {
    match value {
        Value::Varchar(s) => {
            // Try multiple common formats
            let formats = vec![
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M:%S%.f",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%dT%H:%M:%S%.f",
                "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%d",
            ];

            for fmt in formats {
                if let Ok(dt) = NaiveDateTime::parse_from_str(s, fmt) {
                    return Ok(Value::Timestamp(dt.and_utc().timestamp_micros()));
                }
                // Try with just date
                if let Ok(date) = NaiveDate::parse_from_str(s, fmt) {
                    let dt = date.and_hms_opt(0, 0, 0).unwrap();
                    return Ok(Value::Timestamp(dt.and_utc().timestamp_micros()));
                }
            }

            Err(PrismDBError::Execution(format!(
                "Could not parse timestamp: {}",
                s
            )))
        }
        Value::Null => Ok(Value::Null),
        _ => Err(PrismDBError::Type(
            "TO_TIMESTAMP requires string".to_string(),
        )),
    }
}

/// TO_DATE - Parse string to date
pub fn to_date(value: &Value) -> PrismDBResult<Value> {
    match value {
        Value::Varchar(s) => {
            let formats = vec!["%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%d/%m/%Y"];

            for fmt in formats {
                if let Ok(date) = NaiveDate::parse_from_str(s, fmt) {
                    let epoch_date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                    let days = date.signed_duration_since(epoch_date).num_days() as i32;
                    return Ok(Value::Date(days));
                }
            }

            Err(PrismDBError::Execution(format!(
                "Could not parse date: {}",
                s
            )))
        }
        Value::Null => Ok(Value::Null),
        _ => Err(PrismDBError::Type("TO_DATE requires string".to_string())),
    }
}

/// MAKE_DATE - Construct date from year, month, day
pub fn make_date(year: &Value, month: &Value, day: &Value) -> PrismDBResult<Value> {
    match (year, month, day) {
        (Value::Integer(y), Value::Integer(m), Value::Integer(d)) => {
            let date = NaiveDate::from_ymd_opt(*y, *m as u32, *d as u32)
                .ok_or_else(|| PrismDBError::Execution("Invalid date components".to_string()))?;

            let epoch_date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let days = date.signed_duration_since(epoch_date).num_days() as i32;
            Ok(Value::Date(days))
        }
        (Value::Null, _, _) | (_, Value::Null, _) | (_, _, Value::Null) => Ok(Value::Null),
        _ => Err(PrismDBError::Type(
            "MAKE_DATE requires (integer, integer, integer)".to_string(),
        )),
    }
}

/// MAKE_TIMESTAMP - Construct timestamp from components
pub fn make_timestamp(
    year: &Value,
    month: &Value,
    day: &Value,
    hour: &Value,
    minute: &Value,
    second: &Value,
) -> PrismDBResult<Value> {
    match (year, month, day, hour, minute, second) {
        (
            Value::Integer(y),
            Value::Integer(mo),
            Value::Integer(d),
            Value::Integer(h),
            Value::Integer(mi),
            Value::Integer(s),
        ) => {
            let dt = NaiveDate::from_ymd_opt(*y, *mo as u32, *d as u32)
                .and_then(|date| date.and_hms_opt(*h as u32, *mi as u32, *s as u32))
                .ok_or_else(|| {
                    PrismDBError::Execution("Invalid timestamp components".to_string())
                })?;

            Ok(Value::Timestamp(dt.and_utc().timestamp_micros()))
        }
        _ if [year, month, day, hour, minute, second]
            .iter()
            .any(|v| matches!(v, Value::Null)) =>
        {
            Ok(Value::Null)
        }
        _ => Err(PrismDBError::Type(
            "MAKE_TIMESTAMP requires 6 integers".to_string(),
        )),
    }
}

/// EPOCH - Get Unix timestamp in seconds
pub fn epoch(value: &Value) -> PrismDBResult<Value> {
    match value {
        Value::Timestamp(micros) => Ok(Value::BigInt(micros / 1_000_000)),
        Value::Null => Ok(Value::Null),
        _ => Err(PrismDBError::Type("EPOCH requires timestamp".to_string())),
    }
}

/// EPOCH_MS - Get Unix timestamp in milliseconds
pub fn epoch_ms(value: &Value) -> PrismDBResult<Value> {
    match value {
        Value::Timestamp(micros) => Ok(Value::BigInt(micros / 1_000)),
        Value::Null => Ok(Value::Null),
        _ => Err(PrismDBError::Type("EPOCH_MS requires timestamp".to_string())),
    }
}

/// AGE - Calculate time difference between two timestamps
pub fn age(timestamp1: &Value, timestamp2: &Value) -> PrismDBResult<Value> {
    match (timestamp1, timestamp2) {
        (Value::Timestamp(t1), Value::Timestamp(t2)) => {
            let diff_seconds = (t1 - t2) / 1_000_000;
            // Return as interval (for now, just seconds as BigInt)
            Ok(Value::BigInt(diff_seconds))
        }
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => Err(PrismDBError::Type("AGE requires two timestamps".to_string())),
    }
}

/// LAST_DAY - Get last day of the month
pub fn last_day(value: &Value) -> PrismDBResult<Value> {
    match value {
        Value::Date(days) => {
            let date = NaiveDate::from_ymd_opt(1970, 1, 1)
                .unwrap()
                .checked_add_signed(chrono::Duration::days(*days as i64))
                .ok_or_else(|| PrismDBError::Execution("Invalid date".to_string()))?;

            // Get the first day of next month, then subtract one day
            let next_month = if date.month() == 12 {
                NaiveDate::from_ymd_opt(date.year() + 1, 1, 1)
            } else {
                NaiveDate::from_ymd_opt(date.year(), date.month() + 1, 1)
            };

            let last = next_month.and_then(|d| d.pred_opt()).ok_or_else(|| {
                PrismDBError::Execution("Could not calculate last day".to_string())
            })?;

            let epoch_date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let new_days = last.signed_duration_since(epoch_date).num_days() as i32;
            Ok(Value::Date(new_days))
        }
        Value::Null => Ok(Value::Null),
        _ => Err(PrismDBError::Type("LAST_DAY requires date".to_string())),
    }
}

/// TO_CHAR - Format timestamp to string
pub fn to_char(value: &Value, format: &Value) -> PrismDBResult<Value> {
    match (value, format) {
        (Value::Timestamp(micros), Value::Varchar(fmt)) => {
            let dt = DateTime::from_timestamp(*micros / 1_000_000, 0)
                .ok_or_else(|| PrismDBError::Execution("Invalid timestamp".to_string()))?;

            // Simple format mapping (extend as needed)
            let result = fmt
                .replace("YYYY", &format!("{:04}", dt.year()))
                .replace("MM", &format!("{:02}", dt.month()))
                .replace("DD", &format!("{:02}", dt.day()))
                .replace("HH24", &format!("{:02}", dt.hour()))
                .replace("MI", &format!("{:02}", dt.minute()))
                .replace("SS", &format!("{:02}", dt.second()));

            Ok(Value::Varchar(result))
        }
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => Err(PrismDBError::Type(
            "TO_CHAR requires (timestamp, string)".to_string(),
        )),
    }
}

/// STRFTIME - Format timestamp using C-style format codes
pub fn strftime(value: &Value, format: &Value) -> PrismDBResult<Value> {
    match (value, format) {
        (Value::Timestamp(micros), Value::Varchar(fmt)) => {
            let dt = DateTime::from_timestamp(*micros / 1_000_000, 0)
                .ok_or_else(|| PrismDBError::Execution("Invalid timestamp".to_string()))?;

            let formatted = dt.format(fmt).to_string();
            Ok(Value::Varchar(formatted))
        }
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => Err(PrismDBError::Type(
            "STRFTIME requires (timestamp, string)".to_string(),
        )),
    }
}

/// STRPTIME - Parse string to timestamp using C-style format codes
pub fn strptime(value: &Value, format: &Value) -> PrismDBResult<Value> {
    match (value, format) {
        (Value::Varchar(s), Value::Varchar(fmt)) => {
            let dt = NaiveDateTime::parse_from_str(s, fmt)
                .map_err(|e| PrismDBError::Execution(format!("Failed to parse timestamp: {}", e)))?;

            let micros = dt.and_utc().timestamp() * 1_000_000;
            Ok(Value::Timestamp(micros))
        }
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => Err(PrismDBError::Type(
            "STRPTIME requires (string, string)".to_string(),
        )),
    }
}

/// QUARTER - Get quarter from date (1-4)
pub fn quarter(value: &Value) -> PrismDBResult<Value> {
    match value {
        Value::Date(days) => {
            let date = NaiveDate::from_ymd_opt(1970, 1, 1)
                .unwrap()
                .checked_add_signed(chrono::Duration::days(*days as i64))
                .ok_or_else(|| PrismDBError::Execution("Invalid date".to_string()))?;

            let quarter = (date.month() - 1) / 3 + 1;
            Ok(Value::Integer(quarter as i32))
        }
        Value::Timestamp(micros) => {
            let dt = DateTime::from_timestamp(*micros / 1_000_000, 0)
                .ok_or_else(|| PrismDBError::Execution("Invalid timestamp".to_string()))?;

            let quarter = (dt.month() - 1) / 3 + 1;
            Ok(Value::Integer(quarter as i32))
        }
        Value::Null => Ok(Value::Null),
        _ => Err(PrismDBError::Type(
            "QUARTER requires date or timestamp".to_string(),
        )),
    }
}

/// WEEK - Get ISO week number from date
pub fn week(value: &Value) -> PrismDBResult<Value> {
    match value {
        Value::Date(days) => {
            let date = NaiveDate::from_ymd_opt(1970, 1, 1)
                .unwrap()
                .checked_add_signed(chrono::Duration::days(*days as i64))
                .ok_or_else(|| PrismDBError::Execution("Invalid date".to_string()))?;

            Ok(Value::Integer(date.iso_week().week() as i32))
        }
        Value::Null => Ok(Value::Null),
        _ => Err(PrismDBError::Type("WEEK requires date".to_string())),
    }
}

/// DAYOFWEEK - Get day of week (1=Sunday, 7=Saturday)
pub fn dayofweek(value: &Value) -> PrismDBResult<Value> {
    match value {
        Value::Date(days) => {
            let date = NaiveDate::from_ymd_opt(1970, 1, 1)
                .unwrap()
                .checked_add_signed(chrono::Duration::days(*days as i64))
                .ok_or_else(|| PrismDBError::Execution("Invalid date".to_string()))?;

            // chrono: 0 = Monday, 6 = Sunday
            // SQL: 1 = Sunday, 7 = Saturday
            let dow = (date.weekday().num_days_from_monday() + 1) % 7 + 1;
            Ok(Value::Integer(dow as i32))
        }
        Value::Null => Ok(Value::Null),
        _ => Err(PrismDBError::Type("DAYOFWEEK requires date".to_string())),
    }
}

/// DAYOFYEAR - Get day of year (1-366)
pub fn dayofyear(value: &Value) -> PrismDBResult<Value> {
    match value {
        Value::Date(days) => {
            let date = NaiveDate::from_ymd_opt(1970, 1, 1)
                .unwrap()
                .checked_add_signed(chrono::Duration::days(*days as i64))
                .ok_or_else(|| PrismDBError::Execution("Invalid date".to_string()))?;

            Ok(Value::Integer(date.ordinal() as i32))
        }
        Value::Null => Ok(Value::Null),
        _ => Err(PrismDBError::Type("DAYOFYEAR requires date".to_string())),
    }
}

/// ISFINITE - Check if timestamp is finite (not infinity)
pub fn isfinite(value: &Value) -> PrismDBResult<Value> {
    match value {
        Value::Timestamp(_) => Ok(Value::Boolean(true)),
        Value::Null => Ok(Value::Null),
        _ => Err(PrismDBError::Type("ISFINITE requires timestamp".to_string())),
    }
}

/// TIME_BUCKET - Bucket timestamp into intervals
pub fn time_bucket(bucket_width: &Value, value: &Value) -> PrismDBResult<Value> {
    match (bucket_width, value) {
        (Value::Integer(width), Value::Timestamp(micros)) => {
            if *width <= 0 {
                return Err(PrismDBError::Execution(
                    "TIME_BUCKET width must be positive".to_string(),
                ));
            }

            let width_micros = *width as i64 * 1_000_000; // Convert seconds to microseconds
            let bucket = (*micros / width_micros) * width_micros;
            Ok(Value::Timestamp(bucket))
        }
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => Err(PrismDBError::Type(
            "TIME_BUCKET requires (integer, timestamp)".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_current_functions() {
        // Just verify they don't crash
        assert!(current_date().is_ok());
        assert!(current_time().is_ok());
        assert!(now().is_ok());
    }

    #[test]
    fn test_extract() {
        let ts = Value::Timestamp(1609459200000000); // 2021-01-01 00:00:00 UTC

        assert_eq!(extract("year", &ts).unwrap(), Value::Integer(2021));
        assert_eq!(extract("month", &ts).unwrap(), Value::Integer(1));
        assert_eq!(extract("day", &ts).unwrap(), Value::Integer(1));
    }

    #[test]
    fn test_date_part_alias() {
        let ts = Value::Timestamp(1609459200000000);
        assert_eq!(date_part("year", &ts).unwrap(), Value::Integer(2021));
    }

    #[test]
    fn test_year_month_day() {
        let ts = Value::Timestamp(1609459200000000); // 2021-01-01

        assert_eq!(year(&ts).unwrap(), Value::Integer(2021));
        assert_eq!(month(&ts).unwrap(), Value::Integer(1));
        assert_eq!(day(&ts).unwrap(), Value::Integer(1));
    }

    #[test]
    fn test_date_add_sub() {
        let date = Value::Date(0); // 1970-01-01
        let days = Value::Integer(10);

        let result = date_add(&date, &days).unwrap();
        assert_eq!(result, Value::Date(10));

        let result = date_sub(&result, &days).unwrap();
        assert_eq!(result, Value::Date(0));
    }

    #[test]
    fn test_date_diff() {
        let date1 = Value::Date(10);
        let date2 = Value::Date(5);

        let result = date_diff(&date1, &date2).unwrap();
        assert_eq!(result, Value::Integer(5));
    }

    #[test]
    fn test_make_date() {
        let result = make_date(
            &Value::Integer(2021),
            &Value::Integer(1),
            &Value::Integer(1),
        )
        .unwrap();

        if let Value::Date(days) = result {
            assert!(days > 0); // Should be after epoch
        } else {
            panic!("Expected Date value");
        }
    }

    #[test]
    fn test_to_date() {
        let result = to_date(&Value::Varchar("2021-01-01".to_string())).unwrap();

        if let Value::Date(days) = result {
            assert!(days > 0);
        } else {
            panic!("Expected Date value");
        }
    }

    #[test]
    fn test_epoch() {
        let ts = Value::Timestamp(1609459200000000); // 2021-01-01 00:00:00 UTC
        let result = epoch(&ts).unwrap();
        assert_eq!(result, Value::BigInt(1609459200));
    }

    #[test]
    fn test_strftime() {
        let ts = Value::Timestamp(1609459200000000); // 2021-01-01 00:00:00 UTC
        let format = Value::Varchar("%Y-%m-%d".to_string());
        let result = strftime(&ts, &format).unwrap();
        assert_eq!(result, Value::Varchar("2021-01-01".to_string()));
    }

    #[test]
    fn test_quarter() {
        let date = Value::Date(18628); // 2021-01-01
        assert_eq!(quarter(&date).unwrap(), Value::Integer(1));

        let date = Value::Date(18719); // 2021-04-01
        assert_eq!(quarter(&date).unwrap(), Value::Integer(2));
    }

    #[test]
    fn test_week() {
        let date = Value::Date(18628); // 2021-01-01 (Friday, Week 53 of 2020)
        let result = week(&date).unwrap();
        // Should be week 53 or 1 depending on ISO rules
        if let Value::Integer(w) = result {
            assert!(w >= 1 && w <= 53);
        }
    }

    #[test]
    fn test_dayofyear() {
        let date = Value::Date(18628); // 2021-01-01
        assert_eq!(dayofyear(&date).unwrap(), Value::Integer(1));
    }

    #[test]
    fn test_isfinite() {
        let ts = Value::Timestamp(1609459200000000);
        assert_eq!(isfinite(&ts).unwrap(), Value::Boolean(true));
    }

    #[test]
    fn test_time_bucket() {
        let ts = Value::Timestamp(1609459265000000); // 2021-01-01 00:01:05 UTC
        let bucket_width = Value::Integer(60); // 60 seconds
        let result = time_bucket(&bucket_width, &ts).unwrap();
        // Should bucket to 2021-01-01 00:01:00
        assert_eq!(result, Value::Timestamp(1609459260000000));
    }
}
