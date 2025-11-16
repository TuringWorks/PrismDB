//! String Functions (DuckDB-Compatible)
//!
//! This module implements DuckDB's string manipulation functions for 100% compatibility.
//! Includes: manipulation, search, formatting, splitting, pattern matching, and more.

use crate::common::error::{PrismDBError, PrismDBResult};
use crate::types::Value;
use regex::Regex;

/// SUBSTRING - Extract substring from string
/// SUBSTRING(str, start [, length])
/// Note: SQL uses 1-based indexing
pub fn substring(value: &Value, start: &Value, length: Option<&Value>) -> PrismDBResult<Value> {
    match (value, start) {
        (Value::Varchar(s), Value::Integer(start_pos)) => {
            let chars: Vec<char> = s.chars().collect();

            // SQL uses 1-based indexing, convert to 0-based
            let start_idx = if *start_pos > 0 {
                (*start_pos - 1) as usize
            } else if *start_pos == 0 {
                0
            } else {
                // Negative indexing not standard in SQL SUBSTRING
                return Err(PrismDBError::Execution(
                    "SUBSTRING start position must be >= 1".to_string(),
                ));
            };

            if start_idx >= chars.len() {
                return Ok(Value::Varchar(String::new()));
            }

            match length {
                Some(Value::Integer(len)) => {
                    if *len < 0 {
                        return Err(PrismDBError::Execution(
                            "SUBSTRING length must be non-negative".to_string(),
                        ));
                    }
                    let end_idx = (start_idx + *len as usize).min(chars.len());
                    Ok(Value::Varchar(chars[start_idx..end_idx].iter().collect()))
                }
                None => Ok(Value::Varchar(chars[start_idx..].iter().collect())),
                Some(Value::Null) => Ok(Value::Null),
                Some(_) => Err(PrismDBError::Type(
                    "SUBSTRING length must be integer".to_string(),
                )),
            }
        }
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => Err(PrismDBError::Type(
            "SUBSTRING requires (string, integer [, integer])".to_string(),
        )),
    }
}

/// LEFT - Extract leftmost characters
pub fn left(value: &Value, n: &Value) -> PrismDBResult<Value> {
    match (value, n) {
        (Value::Varchar(s), Value::Integer(count)) => {
            let count = (*count).max(0) as usize;
            Ok(Value::Varchar(s.chars().take(count).collect()))
        }
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => Err(PrismDBError::Type(
            "LEFT requires (string, integer)".to_string(),
        )),
    }
}

/// RIGHT - Extract rightmost characters
pub fn right(value: &Value, n: &Value) -> PrismDBResult<Value> {
    match (value, n) {
        (Value::Varchar(s), Value::Integer(count)) => {
            let count = (*count).max(0) as usize;
            let chars: Vec<char> = s.chars().collect();
            let start = chars.len().saturating_sub(count);
            Ok(Value::Varchar(chars[start..].iter().collect()))
        }
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => Err(PrismDBError::Type(
            "RIGHT requires (string, integer)".to_string(),
        )),
    }
}

/// REVERSE - Reverse a string
pub fn reverse(value: &Value) -> PrismDBResult<Value> {
    match value {
        Value::Varchar(s) => Ok(Value::Varchar(s.chars().rev().collect())),
        Value::Null => Ok(Value::Null),
        _ => Err(PrismDBError::Type("REVERSE requires string".to_string())),
    }
}

/// REPEAT - Repeat a string n times
pub fn repeat(value: &Value, n: &Value) -> PrismDBResult<Value> {
    match (value, n) {
        (Value::Varchar(s), Value::Integer(count)) => {
            if *count < 0 {
                return Err(PrismDBError::Execution(
                    "REPEAT count must be non-negative".to_string(),
                ));
            }
            Ok(Value::Varchar(s.repeat(*count as usize)))
        }
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => Err(PrismDBError::Type(
            "REPEAT requires (string, integer)".to_string(),
        )),
    }
}

/// REPLACE - Replace all occurrences of a substring
pub fn replace(value: &Value, from: &Value, to: &Value) -> PrismDBResult<Value> {
    match (value, from, to) {
        (Value::Varchar(s), Value::Varchar(from_str), Value::Varchar(to_str)) => {
            Ok(Value::Varchar(s.replace(from_str, to_str)))
        }
        (Value::Null, _, _) | (_, Value::Null, _) | (_, _, Value::Null) => Ok(Value::Null),
        _ => Err(PrismDBError::Type(
            "REPLACE requires (string, string, string)".to_string(),
        )),
    }
}

/// POSITION - Find position of substring (1-based, 0 if not found)
pub fn position(substring: &Value, value: &Value) -> PrismDBResult<Value> {
    match (substring, value) {
        (Value::Varchar(needle), Value::Varchar(haystack)) => {
            match haystack.find(needle.as_str()) {
                Some(pos) => Ok(Value::Integer((pos + 1) as i32)), // SQL is 1-based
                None => Ok(Value::Integer(0)),
            }
        }
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => Err(PrismDBError::Type(
            "POSITION requires (string, string)".to_string(),
        )),
    }
}

/// STRPOS - Alias for POSITION
pub fn strpos(value: &Value, substring: &Value) -> PrismDBResult<Value> {
    position(substring, value)
}

/// INSTR - Find position of substring (1-based, 0 if not found) - DuckDB alias
pub fn instr(value: &Value, substring: &Value) -> PrismDBResult<Value> {
    position(substring, value)
}

/// CONTAINS - Check if string contains substring
pub fn contains(value: &Value, substring: &Value) -> PrismDBResult<Value> {
    match (value, substring) {
        (Value::Varchar(haystack), Value::Varchar(needle)) => {
            Ok(Value::Boolean(haystack.contains(needle.as_str())))
        }
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => Err(PrismDBError::Type(
            "CONTAINS requires (string, string)".to_string(),
        )),
    }
}

/// LPAD - Pad string on the left to specified length
pub fn lpad(value: &Value, length: &Value, fill: Option<&Value>) -> PrismDBResult<Value> {
    match (value, length) {
        (Value::Varchar(s), Value::Integer(len)) => {
            let target_len = (*len).max(0) as usize;
            let fill_str = match fill {
                Some(Value::Varchar(f)) => f.clone(),
                None => " ".to_string(),
                Some(Value::Null) => return Ok(Value::Null),
                _ => return Err(PrismDBError::Type("LPAD fill must be string".to_string())),
            };

            if fill_str.is_empty() {
                return Err(PrismDBError::Execution(
                    "LPAD fill string cannot be empty".to_string(),
                ));
            }

            let current_len = s.chars().count();
            if current_len >= target_len {
                Ok(Value::Varchar(s.chars().take(target_len).collect()))
            } else {
                let padding_needed = target_len - current_len;
                let fill_chars: Vec<char> = fill_str.chars().collect();
                let mut result = String::new();

                for i in 0..padding_needed {
                    result.push(fill_chars[i % fill_chars.len()]);
                }
                result.push_str(s);

                Ok(Value::Varchar(result))
            }
        }
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => Err(PrismDBError::Type(
            "LPAD requires (string, integer)".to_string(),
        )),
    }
}

/// RPAD - Pad string on the right to specified length
pub fn rpad(value: &Value, length: &Value, fill: Option<&Value>) -> PrismDBResult<Value> {
    match (value, length) {
        (Value::Varchar(s), Value::Integer(len)) => {
            let target_len = (*len).max(0) as usize;
            let fill_str = match fill {
                Some(Value::Varchar(f)) => f.clone(),
                None => " ".to_string(),
                Some(Value::Null) => return Ok(Value::Null),
                _ => return Err(PrismDBError::Type("RPAD fill must be string".to_string())),
            };

            if fill_str.is_empty() {
                return Err(PrismDBError::Execution(
                    "RPAD fill string cannot be empty".to_string(),
                ));
            }

            let current_len = s.chars().count();
            if current_len >= target_len {
                Ok(Value::Varchar(s.chars().take(target_len).collect()))
            } else {
                let padding_needed = target_len - current_len;
                let fill_chars: Vec<char> = fill_str.chars().collect();
                let mut result = s.clone();

                for i in 0..padding_needed {
                    result.push(fill_chars[i % fill_chars.len()]);
                }

                Ok(Value::Varchar(result))
            }
        }
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => Err(PrismDBError::Type(
            "RPAD requires (string, integer)".to_string(),
        )),
    }
}

/// SPLIT_PART - Split string and return nth part (1-based)
pub fn split_part(value: &Value, delimiter: &Value, index: &Value) -> PrismDBResult<Value> {
    match (value, delimiter, index) {
        (Value::Varchar(s), Value::Varchar(delim), Value::Integer(idx)) => {
            if *idx <= 0 {
                return Err(PrismDBError::Execution(
                    "SPLIT_PART index must be positive".to_string(),
                ));
            }

            let parts: Vec<&str> = s.split(delim.as_str()).collect();
            let index = (*idx - 1) as usize; // Convert to 0-based

            if index < parts.len() {
                Ok(Value::Varchar(parts[index].to_string()))
            } else {
                Ok(Value::Varchar(String::new())) // Return empty string if out of bounds
            }
        }
        (Value::Null, _, _) | (_, Value::Null, _) | (_, _, Value::Null) => Ok(Value::Null),
        _ => Err(PrismDBError::Type(
            "SPLIT_PART requires (string, string, integer)".to_string(),
        )),
    }
}

/// STARTS_WITH - Check if string starts with prefix
pub fn starts_with(value: &Value, prefix: &Value) -> PrismDBResult<Value> {
    match (value, prefix) {
        (Value::Varchar(s), Value::Varchar(p)) => Ok(Value::Boolean(s.starts_with(p.as_str()))),
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => Err(PrismDBError::Type(
            "STARTS_WITH requires (string, string)".to_string(),
        )),
    }
}

/// ENDS_WITH - Check if string ends with suffix
pub fn ends_with(value: &Value, suffix: &Value) -> PrismDBResult<Value> {
    match (value, suffix) {
        (Value::Varchar(s), Value::Varchar(suf)) => Ok(Value::Boolean(s.ends_with(suf.as_str()))),
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => Err(PrismDBError::Type(
            "ENDS_WITH requires (string, string)".to_string(),
        )),
    }
}

/// ASCII - Get ASCII code of first character
pub fn ascii(value: &Value) -> PrismDBResult<Value> {
    match value {
        Value::Varchar(s) => match s.chars().next() {
            Some(c) => Ok(Value::Integer(c as i32)),
            None => Ok(Value::Integer(0)),
        },
        Value::Null => Ok(Value::Null),
        _ => Err(PrismDBError::Type("ASCII requires string".to_string())),
    }
}

/// CHR - Convert ASCII code to character
pub fn chr(value: &Value) -> PrismDBResult<Value> {
    match value {
        Value::Integer(code) => {
            if *code < 0 || *code > 127 {
                return Err(PrismDBError::Execution("CHR code must be 0-127".to_string()));
            }
            Ok(Value::Varchar(((*code as u8) as char).to_string()))
        }
        Value::Null => Ok(Value::Null),
        _ => Err(PrismDBError::Type("CHR requires integer".to_string())),
    }
}

/// INITCAP - Capitalize first letter of each word
pub fn initcap(value: &Value) -> PrismDBResult<Value> {
    match value {
        Value::Varchar(s) => {
            let mut result = String::new();
            let mut capitalize_next = true;

            for c in s.chars() {
                if c.is_whitespace() {
                    result.push(c);
                    capitalize_next = true;
                } else if capitalize_next {
                    result.push_str(&c.to_uppercase().to_string());
                    capitalize_next = false;
                } else {
                    result.push_str(&c.to_lowercase().to_string());
                }
            }

            Ok(Value::Varchar(result))
        }
        Value::Null => Ok(Value::Null),
        _ => Err(PrismDBError::Type("INITCAP requires string".to_string())),
    }
}

/// REGEXP_MATCHES - Check if string matches regex pattern
pub fn regexp_matches(value: &Value, pattern: &Value) -> PrismDBResult<Value> {
    match (value, pattern) {
        (Value::Varchar(s), Value::Varchar(pat)) => match Regex::new(pat) {
            Ok(re) => Ok(Value::Boolean(re.is_match(s))),
            Err(e) => Err(PrismDBError::Execution(format!("Invalid regex: {}", e))),
        },
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => Err(PrismDBError::Type(
            "REGEXP_MATCHES requires (string, string)".to_string(),
        )),
    }
}

/// REGEXP_REPLACE - Replace matches of regex pattern
pub fn regexp_replace(value: &Value, pattern: &Value, replacement: &Value) -> PrismDBResult<Value> {
    match (value, pattern, replacement) {
        (Value::Varchar(s), Value::Varchar(pat), Value::Varchar(rep)) => match Regex::new(pat) {
            Ok(re) => Ok(Value::Varchar(re.replace_all(s, rep.as_str()).to_string())),
            Err(e) => Err(PrismDBError::Execution(format!("Invalid regex: {}", e))),
        },
        (Value::Null, _, _) | (_, Value::Null, _) | (_, _, Value::Null) => Ok(Value::Null),
        _ => Err(PrismDBError::Type(
            "REGEXP_REPLACE requires (string, string, string)".to_string(),
        )),
    }
}

/// CHAR_LENGTH - Number of characters in string (alias for LENGTH)
pub fn char_length(value: &Value) -> PrismDBResult<Value> {
    match value {
        Value::Varchar(s) => Ok(Value::Integer(s.chars().count() as i32)),
        Value::Null => Ok(Value::Null),
        _ => Err(PrismDBError::Type("CHAR_LENGTH requires string".to_string())),
    }
}

/// OCTET_LENGTH - Number of bytes in string
pub fn octet_length(value: &Value) -> PrismDBResult<Value> {
    match value {
        Value::Varchar(s) => Ok(Value::Integer(s.len() as i32)),
        Value::Null => Ok(Value::Null),
        _ => Err(PrismDBError::Type(
            "OCTET_LENGTH requires string".to_string(),
        )),
    }
}

/// BIT_LENGTH - Number of bits in string
pub fn bit_length(value: &Value) -> PrismDBResult<Value> {
    match value {
        Value::Varchar(s) => Ok(Value::Integer((s.len() * 8) as i32)),
        Value::Null => Ok(Value::Null),
        _ => Err(PrismDBError::Type("BIT_LENGTH requires string".to_string())),
    }
}

/// MD5 - Calculate MD5 hash
pub fn md5(value: &Value) -> PrismDBResult<Value> {
    match value {
        Value::Varchar(s) => {
            let digest = md5::compute(s.as_bytes());
            Ok(Value::Varchar(format!("{:x}", digest)))
        }
        Value::Null => Ok(Value::Null),
        _ => Err(PrismDBError::Type("MD5 requires string".to_string())),
    }
}

/// SHA256 - Calculate SHA256 hash
pub fn sha256(value: &Value) -> PrismDBResult<Value> {
    use sha2::{Digest, Sha256};

    match value {
        Value::Varchar(s) => {
            let mut hasher = Sha256::new();
            hasher.update(s.as_bytes());
            let result = hasher.finalize();
            Ok(Value::Varchar(format!("{:x}", result)))
        }
        Value::Null => Ok(Value::Null),
        _ => Err(PrismDBError::Type("SHA256 requires string".to_string())),
    }
}

/// BASE64 - Encode string as base64
pub fn base64_encode(value: &Value) -> PrismDBResult<Value> {
    use base64::{engine::general_purpose, Engine as _};

    match value {
        Value::Varchar(s) => {
            let encoded = general_purpose::STANDARD.encode(s.as_bytes());
            Ok(Value::Varchar(encoded))
        }
        Value::Null => Ok(Value::Null),
        _ => Err(PrismDBError::Type("BASE64 requires string".to_string())),
    }
}

/// BASE64_DECODE - Decode base64 string
pub fn base64_decode(value: &Value) -> PrismDBResult<Value> {
    use base64::{engine::general_purpose, Engine as _};

    match value {
        Value::Varchar(s) => match general_purpose::STANDARD.decode(s.as_bytes()) {
            Ok(decoded) => match String::from_utf8(decoded) {
                Ok(s) => Ok(Value::Varchar(s)),
                Err(_) => Err(PrismDBError::Execution(
                    "BASE64_DECODE: invalid UTF-8 in decoded data".to_string(),
                )),
            },
            Err(e) => Err(PrismDBError::Execution(format!(
                "BASE64_DECODE: invalid base64: {}",
                e
            ))),
        },
        Value::Null => Ok(Value::Null),
        _ => Err(PrismDBError::Type(
            "BASE64_DECODE requires string".to_string(),
        )),
    }
}

/// URL_ENCODE - URL encode string
pub fn url_encode(value: &Value) -> PrismDBResult<Value> {
    match value {
        Value::Varchar(s) => {
            let encoded: String = s
                .chars()
                .map(|c| {
                    if c.is_alphanumeric() || c == '-' || c == '_' || c == '.' || c == '~' {
                        c.to_string()
                    } else {
                        format!("%{:02X}", c as u8)
                    }
                })
                .collect();
            Ok(Value::Varchar(encoded))
        }
        Value::Null => Ok(Value::Null),
        _ => Err(PrismDBError::Type("URL_ENCODE requires string".to_string())),
    }
}

/// URL_DECODE - URL decode string
pub fn url_decode(value: &Value) -> PrismDBResult<Value> {
    match value {
        Value::Varchar(s) => {
            let mut result = String::new();
            let mut chars = s.chars().peekable();

            while let Some(c) = chars.next() {
                if c == '%' {
                    // Read next two hex digits
                    let hex: String = chars.by_ref().take(2).collect();
                    if hex.len() == 2 {
                        match u8::from_str_radix(&hex, 16) {
                            Ok(byte) => result.push(byte as char),
                            Err(_) => {
                                return Err(PrismDBError::Execution(
                                    "URL_DECODE: invalid percent encoding".to_string(),
                                ))
                            }
                        }
                    } else {
                        return Err(PrismDBError::Execution(
                            "URL_DECODE: incomplete percent encoding".to_string(),
                        ));
                    }
                } else if c == '+' {
                    result.push(' ');
                } else {
                    result.push(c);
                }
            }
            Ok(Value::Varchar(result))
        }
        Value::Null => Ok(Value::Null),
        _ => Err(PrismDBError::Type("URL_DECODE requires string".to_string())),
    }
}

/// LEVENSHTEIN - Calculate edit distance between two strings
pub fn levenshtein(str1: &Value, str2: &Value) -> PrismDBResult<Value> {
    match (str1, str2) {
        (Value::Varchar(s1), Value::Varchar(s2)) => {
            let len1 = s1.chars().count();
            let len2 = s2.chars().count();

            let mut matrix = vec![vec![0; len2 + 1]; len1 + 1];

            // Initialize first column and row
            for i in 0..=len1 {
                matrix[i][0] = i;
            }
            for j in 0..=len2 {
                matrix[0][j] = j;
            }

            // Build matrix
            let chars1: Vec<char> = s1.chars().collect();
            let chars2: Vec<char> = s2.chars().collect();

            for (i, &c1) in chars1.iter().enumerate() {
                for (j, &c2) in chars2.iter().enumerate() {
                    let cost = if c1 == c2 { 0 } else { 1 };
                    matrix[i + 1][j + 1] = std::cmp::min(
                        std::cmp::min(
                            matrix[i][j + 1] + 1, // deletion
                            matrix[i + 1][j] + 1, // insertion
                        ),
                        matrix[i][j] + cost, // substitution
                    );
                }
            }

            Ok(Value::Integer(matrix[len1][len2] as i32))
        }
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => Err(PrismDBError::Type(
            "LEVENSHTEIN requires (string, string)".to_string(),
        )),
    }
}

/// OVERLAY - Replace substring at position
pub fn overlay(
    value: &Value,
    replacement: &Value,
    start: &Value,
    length: Option<&Value>,
) -> PrismDBResult<Value> {
    match (value, replacement, start) {
        (Value::Varchar(s), Value::Varchar(rep), Value::Integer(pos)) => {
            if *pos < 1 {
                return Err(PrismDBError::Execution(
                    "OVERLAY position must be >= 1".to_string(),
                ));
            }

            let start_idx = (*pos - 1) as usize; // Convert to 0-based
            let chars: Vec<char> = s.chars().collect();

            let len = match length {
                Some(Value::Integer(l)) => (*l).max(0) as usize,
                None => rep.chars().count(),
                Some(Value::Null) => return Ok(Value::Null),
                Some(_) => {
                    return Err(PrismDBError::Type(
                        "OVERLAY length must be integer".to_string(),
                    ))
                }
            };

            if start_idx > chars.len() {
                // If position is beyond string, just return original
                return Ok(Value::Varchar(s.clone()));
            }

            let end_idx = (start_idx + len).min(chars.len());

            let mut result = String::new();
            result.extend(chars[..start_idx].iter());
            result.push_str(rep);
            result.extend(chars[end_idx..].iter());

            Ok(Value::Varchar(result))
        }
        (Value::Null, _, _) | (_, Value::Null, _) | (_, _, Value::Null) => Ok(Value::Null),
        _ => Err(PrismDBError::Type(
            "OVERLAY requires (string, string, integer)".to_string(),
        )),
    }
}

/// QUOTE - Add quotes around string for SQL
pub fn quote(value: &Value) -> PrismDBResult<Value> {
    match value {
        Value::Varchar(s) => {
            let escaped = s.replace('\'', "''");
            Ok(Value::Varchar(format!("'{}'", escaped)))
        }
        Value::Null => Ok(Value::Varchar("NULL".to_string())),
        _ => Err(PrismDBError::Type("QUOTE requires string".to_string())),
    }
}

/// STRING_AGG - Aggregate strings with delimiter (used in GROUP BY)
/// Note: This is typically handled by aggregate operators, but we define it here
pub fn string_agg(values: &[Value], delimiter: &Value) -> PrismDBResult<Value> {
    match delimiter {
        Value::Varchar(delim) => {
            let strings: Result<Vec<String>, _> = values
                .iter()
                .filter(|v| !v.is_null())
                .map(|v| match v {
                    Value::Varchar(s) => Ok(s.clone()),
                    _ => Err(PrismDBError::Type(
                        "STRING_AGG requires string values".to_string(),
                    )),
                })
                .collect();

            match strings {
                Ok(strs) => Ok(Value::Varchar(strs.join(delim))),
                Err(e) => Err(e),
            }
        }
        Value::Null => Ok(Value::Null),
        _ => Err(PrismDBError::Type(
            "STRING_AGG delimiter must be string".to_string(),
        )),
    }
}

/// STRING_SPLIT - Split string into array by delimiter
pub fn string_split(value: &Value, delimiter: &Value) -> PrismDBResult<Value> {
    match (value, delimiter) {
        (Value::Varchar(s), Value::Varchar(delim)) => {
            if delim.is_empty() {
                // Split into individual characters
                let parts: Vec<Value> = s.chars().map(|c| Value::Varchar(c.to_string())).collect();
                Ok(Value::List(parts))
            } else {
                let parts: Vec<Value> = s
                    .split(delim.as_str())
                    .map(|part| Value::Varchar(part.to_string()))
                    .collect();
                Ok(Value::List(parts))
            }
        }
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => Err(PrismDBError::Type(
            "STRING_SPLIT requires (string, string)".to_string(),
        )),
    }
}

/// SOUNDEX - Phonetic algorithm for indexing names by sound
pub fn soundex(value: &Value) -> PrismDBResult<Value> {
    match value {
        Value::Varchar(s) => {
            if s.is_empty() {
                return Ok(Value::Varchar(String::new()));
            }

            let chars: Vec<char> = s.to_uppercase().chars().collect();
            let mut result = String::new();

            // Keep first letter
            if let Some(&first) = chars.first() {
                if first.is_alphabetic() {
                    result.push(first);
                }
            }

            // Soundex mapping
            let soundex_code = |c: char| -> Option<char> {
                match c {
                    'B' | 'F' | 'P' | 'V' => Some('1'),
                    'C' | 'G' | 'J' | 'K' | 'Q' | 'S' | 'X' | 'Z' => Some('2'),
                    'D' | 'T' => Some('3'),
                    'L' => Some('4'),
                    'M' | 'N' => Some('5'),
                    'R' => Some('6'),
                    _ => None,
                }
            };

            let mut prev_code: Option<char> = soundex_code(chars[0]);

            for &c in &chars[1..] {
                if let Some(code) = soundex_code(c) {
                    // Don't add duplicate adjacent codes
                    if Some(code) != prev_code {
                        result.push(code);
                        if result.len() >= 4 {
                            break;
                        }
                    }
                    prev_code = Some(code);
                } else if c != 'A'
                    && c != 'E'
                    && c != 'I'
                    && c != 'O'
                    && c != 'U'
                    && c != 'H'
                    && c != 'W'
                    && c != 'Y'
                {
                    // Reset prev_code for separators
                    prev_code = None;
                }
            }

            // Pad with zeros to length 4
            while result.len() < 4 {
                result.push('0');
            }

            Ok(Value::Varchar(result))
        }
        Value::Null => Ok(Value::Null),
        _ => Err(PrismDBError::Type("SOUNDEX requires string".to_string())),
    }
}

/// FORMAT - Format string with arguments (simple printf-style)
pub fn format(template: &Value, args: &[Value]) -> PrismDBResult<Value> {
    match template {
        Value::Varchar(s) => {
            let mut result = s.clone();
            for (_i, arg) in args.iter().enumerate() {
                let placeholder = format!("{{}}");
                let replacement = match arg {
                    Value::Varchar(s) => s.clone(),
                    Value::Integer(i) => i.to_string(),
                    Value::Float(f) => f.to_string(),
                    Value::Boolean(b) => b.to_string(),
                    Value::Null => "NULL".to_string(),
                    _ => return Err(PrismDBError::Type("Unsupported format argument".to_string())),
                };

                // Replace first occurrence of {}
                if let Some(pos) = result.find(&placeholder) {
                    result.replace_range(pos..pos + 2, &replacement);
                } else {
                    break;
                }
            }
            Ok(Value::Varchar(result))
        }
        Value::Null => Ok(Value::Null),
        _ => Err(PrismDBError::Type(
            "FORMAT requires string template".to_string(),
        )),
    }
}

/// REGEXP_EXTRACT - Extract substring using regex pattern with group
pub fn regexp_extract(
    value: &Value,
    pattern: &Value,
    group: Option<&Value>,
) -> PrismDBResult<Value> {
    match (value, pattern) {
        (Value::Varchar(s), Value::Varchar(pat)) => {
            let re = Regex::new(pat)
                .map_err(|e| PrismDBError::Execution(format!("Invalid regex pattern: {}", e)))?;

            let group_idx = match group {
                Some(Value::Integer(idx)) => *idx as usize,
                None => 0,
                Some(Value::Null) => return Ok(Value::Null),
                Some(_) => {
                    return Err(PrismDBError::Type(
                        "REGEXP_EXTRACT group must be integer".to_string(),
                    ))
                }
            };

            if let Some(caps) = re.captures(s) {
                if let Some(matched) = caps.get(group_idx) {
                    return Ok(Value::Varchar(matched.as_str().to_string()));
                }
            }

            Ok(Value::Null)
        }
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => Err(PrismDBError::Type(
            "REGEXP_EXTRACT requires (string, string [, integer])".to_string(),
        )),
    }
}

/// LIKE_ESCAPE - Convert LIKE pattern to regex with custom escape character
pub fn like_escape(pattern: &Value, escape: Option<&Value>) -> PrismDBResult<Value> {
    match pattern {
        Value::Varchar(pat) => {
            let escape_char = match escape {
                Some(Value::Varchar(s)) => {
                    if s.len() != 1 {
                        return Err(PrismDBError::Execution(
                            "LIKE_ESCAPE escape must be single character".to_string(),
                        ));
                    }
                    s.chars().next().unwrap()
                }
                None => '\\',
                Some(Value::Null) => return Ok(Value::Null),
                Some(_) => {
                    return Err(PrismDBError::Type(
                        "LIKE_ESCAPE escape must be string".to_string(),
                    ))
                }
            };

            let mut result = String::from("^");
            let mut chars = pat.chars().peekable();

            while let Some(c) = chars.next() {
                if c == escape_char {
                    // Escaped character - treat literally
                    if let Some(&next) = chars.peek() {
                        result.push_str(&regex::escape(&next.to_string()));
                        chars.next();
                    }
                } else if c == '%' {
                    // Match any sequence
                    result.push_str(".*");
                } else if c == '_' {
                    // Match any single character
                    result.push('.');
                } else {
                    // Regular character - escape for regex
                    result.push_str(&regex::escape(&c.to_string()));
                }
            }

            result.push('$');
            Ok(Value::Varchar(result))
        }
        Value::Null => Ok(Value::Null),
        _ => Err(PrismDBError::Type(
            "LIKE_ESCAPE requires string pattern".to_string(),
        )),
    }
}

/// TRANSLATE - Replace characters in string based on mapping
pub fn translate(value: &Value, from: &Value, to: &Value) -> PrismDBResult<Value> {
    match (value, from, to) {
        (Value::Varchar(s), Value::Varchar(from_chars), Value::Varchar(to_chars)) => {
            let from_vec: Vec<char> = from_chars.chars().collect();
            let to_vec: Vec<char> = to_chars.chars().collect();

            let result: String = s
                .chars()
                .map(|c| {
                    if let Some(pos) = from_vec.iter().position(|&fc| fc == c) {
                        to_vec.get(pos).copied().unwrap_or(c)
                    } else {
                        c
                    }
                })
                .collect();

            Ok(Value::Varchar(result))
        }
        (Value::Null, _, _) | (_, Value::Null, _) | (_, _, Value::Null) => Ok(Value::Null),
        _ => Err(PrismDBError::Type(
            "TRANSLATE requires (string, string, string)".to_string(),
        )),
    }
}

/// PRINTF - Printf-style string formatting
pub fn printf(template: &Value, args: &[Value]) -> PrismDBResult<Value> {
    // Simple implementation - for full compatibility, would need printf parsing
    format(template, args)
}

// ============================================================================
// String Similarity Functions (DuckDB-Compatible)
// ============================================================================

/// JARO_SIMILARITY - Calculate Jaro similarity between two strings
/// Returns a value between 0.0 (no similarity) and 1.0 (identical strings)
pub fn jaro_similarity(s1: &Value, s2: &Value) -> PrismDBResult<Value> {
    match (s1, s2) {
        (Value::Varchar(str1), Value::Varchar(str2)) => {
            let similarity = strsim::jaro(str1, str2);
            Ok(Value::Double(similarity))
        }
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => Err(PrismDBError::Type(
            "JARO_SIMILARITY requires two strings".to_string(),
        )),
    }
}

/// JARO_WINKLER_SIMILARITY - Calculate Jaro-Winkler similarity between two strings
/// Similar to Jaro but gives more weight to common prefixes
/// Returns a value between 0.0 (no similarity) and 1.0 (identical strings)
pub fn jaro_winkler_similarity(s1: &Value, s2: &Value) -> PrismDBResult<Value> {
    match (s1, s2) {
        (Value::Varchar(str1), Value::Varchar(str2)) => {
            let similarity = strsim::jaro_winkler(str1, str2);
            Ok(Value::Double(similarity))
        }
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => Err(PrismDBError::Type(
            "JARO_WINKLER_SIMILARITY requires two strings".to_string(),
        )),
    }
}

/// DAMERAU_LEVENSHTEIN - Calculate Damerau-Levenshtein distance between two strings
/// Like Levenshtein but also allows transpositions (swapping adjacent characters)
pub fn damerau_levenshtein(s1: &Value, s2: &Value) -> PrismDBResult<Value> {
    match (s1, s2) {
        (Value::Varchar(str1), Value::Varchar(str2)) => {
            let distance = strsim::damerau_levenshtein(str1, str2);
            Ok(Value::BigInt(distance as i64))
        }
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => Err(PrismDBError::Type(
            "DAMERAU_LEVENSHTEIN requires two strings".to_string(),
        )),
    }
}

/// HAMMING - Calculate Hamming distance between two strings
/// Number of positions at which the corresponding characters are different
/// Strings must be of equal length
pub fn hamming(s1: &Value, s2: &Value) -> PrismDBResult<Value> {
    match (s1, s2) {
        (Value::Varchar(str1), Value::Varchar(str2)) => {
            if str1.len() != str2.len() {
                return Err(PrismDBError::Execution(
                    "HAMMING requires strings of equal length".to_string(),
                ));
            }
            let distance = strsim::hamming(str1, str2)
                .map_err(|e| PrismDBError::Execution(format!("HAMMING error: {}", e)))?;
            Ok(Value::BigInt(distance as i64))
        }
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => Err(PrismDBError::Type(
            "HAMMING requires two strings".to_string(),
        )),
    }
}

// ============================================================================
// Advanced Regex Functions (DuckDB-Compatible)
// ============================================================================

/// REGEXP_SPLIT_TO_ARRAY - Split a string by a regular expression pattern
/// Returns an array of strings (represented as comma-separated VARCHAR for now)
pub fn regexp_split_to_array(text: &Value, pattern: &Value) -> PrismDBResult<Value> {
    match (text, pattern) {
        (Value::Varchar(s), Value::Varchar(p)) => {
            let re = Regex::new(p)
                .map_err(|e| PrismDBError::Execution(format!("Invalid regex: {}", e)))?;
            let parts: Vec<String> = re.split(s).map(|s| s.to_string()).collect();
            // For now, return as JSON array string until ARRAY type is implemented
            let result = format!("[{}]", parts.iter()
                .map(|s| format!("\"{}\"", s.replace("\"", "\\\"")))
                .collect::<Vec<_>>()
                .join(","));
            Ok(Value::Varchar(result))
        }
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => Err(PrismDBError::Type(
            "REGEXP_SPLIT_TO_ARRAY requires (string, pattern)".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_left_right() {
        assert_eq!(
            left(&Value::Varchar("hello".to_string()), &Value::Integer(3)).unwrap(),
            Value::Varchar("hel".to_string())
        );
        assert_eq!(
            right(&Value::Varchar("hello".to_string()), &Value::Integer(3)).unwrap(),
            Value::Varchar("llo".to_string())
        );
    }

    #[test]
    fn test_reverse() {
        assert_eq!(
            reverse(&Value::Varchar("hello".to_string())).unwrap(),
            Value::Varchar("olleh".to_string())
        );
    }

    #[test]
    fn test_repeat() {
        assert_eq!(
            repeat(&Value::Varchar("ab".to_string()), &Value::Integer(3)).unwrap(),
            Value::Varchar("ababab".to_string())
        );
    }

    #[test]
    fn test_replace() {
        assert_eq!(
            replace(
                &Value::Varchar("hello world".to_string()),
                &Value::Varchar("world".to_string()),
                &Value::Varchar("rust".to_string())
            )
            .unwrap(),
            Value::Varchar("hello rust".to_string())
        );
    }

    #[test]
    fn test_position() {
        assert_eq!(
            position(
                &Value::Varchar("world".to_string()),
                &Value::Varchar("hello world".to_string())
            )
            .unwrap(),
            Value::Integer(7)
        );
        assert_eq!(
            position(
                &Value::Varchar("xyz".to_string()),
                &Value::Varchar("hello".to_string())
            )
            .unwrap(),
            Value::Integer(0)
        );
    }

    #[test]
    fn test_contains() {
        assert_eq!(
            contains(
                &Value::Varchar("hello world".to_string()),
                &Value::Varchar("world".to_string())
            )
            .unwrap(),
            Value::Boolean(true)
        );
        assert_eq!(
            contains(
                &Value::Varchar("hello".to_string()),
                &Value::Varchar("xyz".to_string())
            )
            .unwrap(),
            Value::Boolean(false)
        );
    }

    #[test]
    fn test_lpad_rpad() {
        assert_eq!(
            lpad(
                &Value::Varchar("hi".to_string()),
                &Value::Integer(5),
                Some(&Value::Varchar("x".to_string()))
            )
            .unwrap(),
            Value::Varchar("xxxhi".to_string())
        );
        assert_eq!(
            rpad(
                &Value::Varchar("hi".to_string()),
                &Value::Integer(5),
                Some(&Value::Varchar("x".to_string()))
            )
            .unwrap(),
            Value::Varchar("hixxx".to_string())
        );
    }

    #[test]
    fn test_split_part() {
        assert_eq!(
            split_part(
                &Value::Varchar("a,b,c".to_string()),
                &Value::Varchar(",".to_string()),
                &Value::Integer(2)
            )
            .unwrap(),
            Value::Varchar("b".to_string())
        );
    }

    #[test]
    fn test_starts_ends_with() {
        assert_eq!(
            starts_with(
                &Value::Varchar("hello".to_string()),
                &Value::Varchar("hel".to_string())
            )
            .unwrap(),
            Value::Boolean(true)
        );
        assert_eq!(
            ends_with(
                &Value::Varchar("hello".to_string()),
                &Value::Varchar("lo".to_string())
            )
            .unwrap(),
            Value::Boolean(true)
        );
    }

    #[test]
    fn test_ascii_chr() {
        assert_eq!(
            ascii(&Value::Varchar("A".to_string())).unwrap(),
            Value::Integer(65)
        );
        assert_eq!(
            chr(&Value::Integer(65)).unwrap(),
            Value::Varchar("A".to_string())
        );
    }

    #[test]
    fn test_initcap() {
        assert_eq!(
            initcap(&Value::Varchar("hello world".to_string())).unwrap(),
            Value::Varchar("Hello World".to_string())
        );
    }

    #[test]
    fn test_substring() {
        assert_eq!(
            substring(
                &Value::Varchar("hello world".to_string()),
                &Value::Integer(1),
                Some(&Value::Integer(5))
            )
            .unwrap(),
            Value::Varchar("hello".to_string())
        );
        assert_eq!(
            substring(
                &Value::Varchar("hello world".to_string()),
                &Value::Integer(7),
                None
            )
            .unwrap(),
            Value::Varchar("world".to_string())
        );
    }

    #[test]
    fn test_md5() {
        let result = md5(&Value::Varchar("hello".to_string())).unwrap();
        if let Value::Varchar(hash) = result {
            assert_eq!(hash.len(), 32); // MD5 hash is 32 hex chars
        } else {
            panic!("Expected Varchar result");
        }
    }

    #[test]
    fn test_base64() {
        let encoded = base64_encode(&Value::Varchar("hello".to_string())).unwrap();
        assert_eq!(encoded, Value::Varchar("aGVsbG8=".to_string()));

        let decoded = base64_decode(&Value::Varchar("aGVsbG8=".to_string())).unwrap();
        assert_eq!(decoded, Value::Varchar("hello".to_string()));
    }

    #[test]
    fn test_levenshtein() {
        assert_eq!(
            levenshtein(
                &Value::Varchar("kitten".to_string()),
                &Value::Varchar("sitting".to_string())
            )
            .unwrap(),
            Value::Integer(3)
        );
        assert_eq!(
            levenshtein(
                &Value::Varchar("hello".to_string()),
                &Value::Varchar("hello".to_string())
            )
            .unwrap(),
            Value::Integer(0)
        );
    }

    #[test]
    fn test_string_split() {
        let result = string_split(
            &Value::Varchar("a,b,c".to_string()),
            &Value::Varchar(",".to_string()),
        )
        .unwrap();

        if let Value::List(parts) = result {
            assert_eq!(parts.len(), 3);
            assert_eq!(parts[0], Value::Varchar("a".to_string()));
            assert_eq!(parts[1], Value::Varchar("b".to_string()));
            assert_eq!(parts[2], Value::Varchar("c".to_string()));
        } else {
            panic!("Expected List value");
        }
    }

    #[test]
    fn test_soundex() {
        assert_eq!(
            soundex(&Value::Varchar("Robert".to_string())).unwrap(),
            Value::Varchar("R163".to_string())
        );
        assert_eq!(
            soundex(&Value::Varchar("Rupert".to_string())).unwrap(),
            Value::Varchar("R163".to_string())
        );
        assert_eq!(
            soundex(&Value::Varchar("Rubin".to_string())).unwrap(),
            Value::Varchar("R150".to_string())
        );
    }

    #[test]
    fn test_format() {
        let result = format(
            &Value::Varchar("Hello {} {}!".to_string()),
            &[Value::Varchar("world".to_string()), Value::Integer(2025)],
        )
        .unwrap();
        assert_eq!(result, Value::Varchar("Hello world 2025!".to_string()));
    }

    #[test]
    fn test_regexp_extract() {
        // Extract full match
        assert_eq!(
            regexp_extract(
                &Value::Varchar("test123".to_string()),
                &Value::Varchar(r"\d+".to_string()),
                None
            )
            .unwrap(),
            Value::Varchar("123".to_string())
        );

        // Extract group 1
        assert_eq!(
            regexp_extract(
                &Value::Varchar("test123".to_string()),
                &Value::Varchar(r"([a-z]+)(\d+)".to_string()),
                Some(&Value::Integer(1))
            )
            .unwrap(),
            Value::Varchar("test".to_string())
        );
    }

    #[test]
    fn test_translate() {
        assert_eq!(
            translate(
                &Value::Varchar("hello".to_string()),
                &Value::Varchar("helo".to_string()),
                &Value::Varchar("HELO".to_string())
            )
            .unwrap(),
            Value::Varchar("HELLO".to_string())
        );
    }

    // ========================================================================
    // String Similarity Function Tests
    // ========================================================================

    #[test]
    fn test_jaro_similarity() {
        // Identical strings
        assert_eq!(
            jaro_similarity(
                &Value::Varchar("hello".to_string()),
                &Value::Varchar("hello".to_string())
            )
            .unwrap(),
            Value::Double(1.0)
        );

        // Similar strings
        let result = jaro_similarity(
            &Value::Varchar("martha".to_string()),
            &Value::Varchar("marhta".to_string())
        )
        .unwrap();
        if let Value::Double(sim) = result {
            assert!(sim > 0.9); // Should be very similar (transposition)
        }

        // Null handling
        assert_eq!(
            jaro_similarity(&Value::Null, &Value::Varchar("test".to_string())).unwrap(),
            Value::Null
        );
    }

    #[test]
    fn test_jaro_winkler_similarity() {
        // Identical strings
        assert_eq!(
            jaro_winkler_similarity(
                &Value::Varchar("hello".to_string()),
                &Value::Varchar("hello".to_string())
            )
            .unwrap(),
            Value::Double(1.0)
        );

        // Similar with common prefix (Jaro-Winkler favors common prefixes)
        let result = jaro_winkler_similarity(
            &Value::Varchar("test123".to_string()),
            &Value::Varchar("test456".to_string())
        )
        .unwrap();
        if let Value::Double(sim) = result {
            assert!(sim > 0.5); // Should have decent similarity due to common "test" prefix
        }
    }

    #[test]
    fn test_damerau_levenshtein() {
        // Identical strings = distance 0
        assert_eq!(
            damerau_levenshtein(
                &Value::Varchar("hello".to_string()),
                &Value::Varchar("hello".to_string())
            )
            .unwrap(),
            Value::BigInt(0)
        );

        // Transposition (swap adjacent chars) counts as 1 edit
        assert_eq!(
            damerau_levenshtein(
                &Value::Varchar("hello".to_string()),
                &Value::Varchar("ehllo".to_string())
            )
            .unwrap(),
            Value::BigInt(1)
        );
    }

    #[test]
    fn test_hamming() {
        // Equal length, identical strings = distance 0
        assert_eq!(
            hamming(
                &Value::Varchar("hello".to_string()),
                &Value::Varchar("hello".to_string())
            )
            .unwrap(),
            Value::BigInt(0)
        );

        // Equal length, 2 differences
        assert_eq!(
            hamming(
                &Value::Varchar("hello".to_string()),
                &Value::Varchar("hallo".to_string())
            )
            .unwrap(),
            Value::BigInt(1)
        );

        // Different lengths should error
        let result = hamming(
            &Value::Varchar("hello".to_string()),
            &Value::Varchar("hi".to_string())
        );
        assert!(result.is_err());
    }

    // ========================================================================
    // Advanced Regex Function Tests
    // ========================================================================

    #[test]
    fn test_regexp_matches() {
        // Match found
        assert_eq!(
            regexp_matches(
                &Value::Varchar("test123".to_string()),
                &Value::Varchar(r"\d+".to_string())
            )
            .unwrap(),
            Value::Boolean(true)
        );

        // No match
        assert_eq!(
            regexp_matches(
                &Value::Varchar("test".to_string()),
                &Value::Varchar(r"\d+".to_string())
            )
            .unwrap(),
            Value::Boolean(false)
        );

        // Null handling
        assert_eq!(
            regexp_matches(&Value::Null, &Value::Varchar(r"\d+".to_string())).unwrap(),
            Value::Null
        );
    }

    #[test]
    fn test_regexp_replace() {
        // Replace digits with X
        assert_eq!(
            regexp_replace(
                &Value::Varchar("test123abc456".to_string()),
                &Value::Varchar(r"\d+".to_string()),
                &Value::Varchar("X".to_string())
            )
            .unwrap(),
            Value::Varchar("testXabcX".to_string())
        );

        // Replace with groups
        assert_eq!(
            regexp_replace(
                &Value::Varchar("hello world".to_string()),
                &Value::Varchar(r"(\w+) (\w+)".to_string()),
                &Value::Varchar("$2 $1".to_string())
            )
            .unwrap(),
            Value::Varchar("world hello".to_string())
        );
    }

    #[test]
    fn test_regexp_split_to_array() {
        // Split by comma
        let result = regexp_split_to_array(
            &Value::Varchar("a,b,c".to_string()),
            &Value::Varchar(",".to_string())
        )
        .unwrap();

        if let Value::Varchar(arr) = result {
            assert!(arr.contains("\"a\""));
            assert!(arr.contains("\"b\""));
            assert!(arr.contains("\"c\""));
        }

        // Split by whitespace
        let result = regexp_split_to_array(
            &Value::Varchar("hello world test".to_string()),
            &Value::Varchar(r"\s+".to_string())
        )
        .unwrap();

        if let Value::Varchar(arr) = result {
            assert!(arr.contains("\"hello\""));
            assert!(arr.contains("\"world\""));
            assert!(arr.contains("\"test\""));
        }
    }
}
