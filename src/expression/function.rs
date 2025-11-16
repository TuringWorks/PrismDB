//! Function definitions and implementations for PrismDB expressions

use crate::common::error::{PrismDBError, PrismDBResult};
use crate::types::{LogicalType, Value};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Function types
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FunctionType {
    // Scalar functions
    Scalar,
    // Aggregate functions
    Aggregate,
    // Window functions
    Window,
}

/// Function classification
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FunctionClassification {
    // Mathematical functions
    Mathematical,
    // String functions
    String,
    // Date/time functions
    DateTime,
    // Type conversion functions
    TypeConversion,
    // Conditional functions
    Conditional,
    // System functions
    System,
    // Other
    Other,
}

/// Function metadata
#[derive(Debug, Clone)]
pub struct FunctionInfo {
    pub name: String,
    pub function_type: FunctionType,
    pub classification: FunctionClassification,
    pub return_type: LogicalType,
    pub argument_types: Vec<LogicalType>,
    pub is_variadic: bool,
    pub is_nullable: bool,
    pub is_deterministic: bool,
}

impl FunctionInfo {
    pub fn new(
        name: String,
        function_type: FunctionType,
        classification: FunctionClassification,
        return_type: LogicalType,
        argument_types: Vec<LogicalType>,
    ) -> Self {
        Self {
            name,
            function_type,
            classification,
            return_type,
            argument_types,
            is_variadic: false,
            is_nullable: true,
            is_deterministic: true,
        }
    }

    pub fn variadic(mut self) -> Self {
        self.is_variadic = true;
        self
    }

    pub fn non_nullable(mut self) -> Self {
        self.is_nullable = false;
        self
    }

    pub fn non_deterministic(mut self) -> Self {
        self.is_deterministic = false;
        self
    }
}

/// Built-in function registry
pub struct FunctionRegistry {
    functions: HashMap<String, Vec<FunctionInfo>>,
}

impl FunctionRegistry {
    pub fn new() -> Self {
        let mut registry = Self {
            functions: HashMap::new(),
        };
        registry.register_builtin_functions();
        registry
    }

    /// Register a function
    pub fn register_function(&mut self, function_info: FunctionInfo) {
        let name = function_info.name.to_uppercase();
        self.functions
            .entry(name)
            .or_insert_with(Vec::new)
            .push(function_info);
    }

    /// Get function by name and argument types
    pub fn get_function(
        &self,
        name: &str,
        argument_types: &[LogicalType],
    ) -> Option<&FunctionInfo> {
        let name = name.to_uppercase();
        if let Some(functions) = self.functions.get(&name) {
            // Find exact match first
            for function in functions {
                if function.argument_types == argument_types {
                    return Some(function);
                }
            }

            // Find variadic match
            for function in functions {
                if function.is_variadic {
                    return Some(function);
                }
            }
        }
        None
    }

    /// List all functions
    pub fn list_functions(&self) -> Vec<&str> {
        self.functions.keys().map(|s| s.as_str()).collect()
    }

    /// Register built-in functions
    fn register_builtin_functions(&mut self) {
        self.register_mathematical_functions();
        self.register_string_functions();
        self.register_conditional_functions();
        self.register_system_functions();
    }

    fn register_mathematical_functions(&mut self) {
        // Basic Math Functions

        // ABS - Register multiple overloads for different numeric types
        self.register_function(FunctionInfo::new(
            "abs".to_string(),
            FunctionType::Scalar,
            FunctionClassification::Mathematical,
            LogicalType::Integer,
            vec![LogicalType::Integer],
        ));

        self.register_function(FunctionInfo::new(
            "abs".to_string(),
            FunctionType::Scalar,
            FunctionClassification::Mathematical,
            LogicalType::BigInt,
            vec![LogicalType::BigInt],
        ));

        self.register_function(FunctionInfo::new(
            "abs".to_string(),
            FunctionType::Scalar,
            FunctionClassification::Mathematical,
            LogicalType::Double,
            vec![LogicalType::Double],
        ));

        // SIGN
        self.register_function(FunctionInfo::new(
            "sign".to_string(),
            FunctionType::Scalar,
            FunctionClassification::Mathematical,
            LogicalType::Double,
            vec![LogicalType::Double],
        ));

        // SQRT
        self.register_function(FunctionInfo::new(
            "sqrt".to_string(),
            FunctionType::Scalar,
            FunctionClassification::Mathematical,
            LogicalType::Double,
            vec![LogicalType::Double],
        ));

        // POWER/POW
        self.register_function(FunctionInfo::new(
            "power".to_string(),
            FunctionType::Scalar,
            FunctionClassification::Mathematical,
            LogicalType::Double,
            vec![LogicalType::Double, LogicalType::Double],
        ));

        self.register_function(FunctionInfo::new(
            "pow".to_string(),
            FunctionType::Scalar,
            FunctionClassification::Mathematical,
            LogicalType::Double,
            vec![LogicalType::Double, LogicalType::Double],
        ));

        // EXP
        self.register_function(FunctionInfo::new(
            "exp".to_string(),
            FunctionType::Scalar,
            FunctionClassification::Mathematical,
            LogicalType::Double,
            vec![LogicalType::Double],
        ));

        // Logarithmic Functions

        // LN (natural log)
        self.register_function(FunctionInfo::new(
            "ln".to_string(),
            FunctionType::Scalar,
            FunctionClassification::Mathematical,
            LogicalType::Double,
            vec![LogicalType::Double],
        ));

        // LOG (base 10)
        self.register_function(FunctionInfo::new(
            "log".to_string(),
            FunctionType::Scalar,
            FunctionClassification::Mathematical,
            LogicalType::Double,
            vec![LogicalType::Double],
        ));

        // LOG10
        self.register_function(FunctionInfo::new(
            "log10".to_string(),
            FunctionType::Scalar,
            FunctionClassification::Mathematical,
            LogicalType::Double,
            vec![LogicalType::Double],
        ));

        // LOG2
        self.register_function(FunctionInfo::new(
            "log2".to_string(),
            FunctionType::Scalar,
            FunctionClassification::Mathematical,
            LogicalType::Double,
            vec![LogicalType::Double],
        ));

        // Rounding Functions

        // CEIL/CEILING
        self.register_function(FunctionInfo::new(
            "ceil".to_string(),
            FunctionType::Scalar,
            FunctionClassification::Mathematical,
            LogicalType::Double,
            vec![LogicalType::Double],
        ));

        self.register_function(FunctionInfo::new(
            "ceiling".to_string(),
            FunctionType::Scalar,
            FunctionClassification::Mathematical,
            LogicalType::Double,
            vec![LogicalType::Double],
        ));

        // FLOOR
        self.register_function(FunctionInfo::new(
            "floor".to_string(),
            FunctionType::Scalar,
            FunctionClassification::Mathematical,
            LogicalType::Double,
            vec![LogicalType::Double],
        ));

        // ROUND (1 argument)
        self.register_function(FunctionInfo::new(
            "round".to_string(),
            FunctionType::Scalar,
            FunctionClassification::Mathematical,
            LogicalType::Double,
            vec![LogicalType::Double],
        ));

        // ROUND (2 arguments - with precision)
        self.register_function(FunctionInfo::new(
            "round".to_string(),
            FunctionType::Scalar,
            FunctionClassification::Mathematical,
            LogicalType::Double,
            vec![LogicalType::Double, LogicalType::Integer],
        ));

        // TRUNC (1 argument)
        self.register_function(FunctionInfo::new(
            "trunc".to_string(),
            FunctionType::Scalar,
            FunctionClassification::Mathematical,
            LogicalType::Double,
            vec![LogicalType::Double],
        ));

        // TRUNC (2 arguments - with precision)
        self.register_function(FunctionInfo::new(
            "trunc".to_string(),
            FunctionType::Scalar,
            FunctionClassification::Mathematical,
            LogicalType::Double,
            vec![LogicalType::Double, LogicalType::Integer],
        ));

        // Trigonometric Functions

        // SIN
        self.register_function(FunctionInfo::new(
            "sin".to_string(),
            FunctionType::Scalar,
            FunctionClassification::Mathematical,
            LogicalType::Double,
            vec![LogicalType::Double],
        ));

        // COS
        self.register_function(FunctionInfo::new(
            "cos".to_string(),
            FunctionType::Scalar,
            FunctionClassification::Mathematical,
            LogicalType::Double,
            vec![LogicalType::Double],
        ));

        // TAN
        self.register_function(FunctionInfo::new(
            "tan".to_string(),
            FunctionType::Scalar,
            FunctionClassification::Mathematical,
            LogicalType::Double,
            vec![LogicalType::Double],
        ));

        // ASIN
        self.register_function(FunctionInfo::new(
            "asin".to_string(),
            FunctionType::Scalar,
            FunctionClassification::Mathematical,
            LogicalType::Double,
            vec![LogicalType::Double],
        ));

        // ACOS
        self.register_function(FunctionInfo::new(
            "acos".to_string(),
            FunctionType::Scalar,
            FunctionClassification::Mathematical,
            LogicalType::Double,
            vec![LogicalType::Double],
        ));

        // ATAN
        self.register_function(FunctionInfo::new(
            "atan".to_string(),
            FunctionType::Scalar,
            FunctionClassification::Mathematical,
            LogicalType::Double,
            vec![LogicalType::Double],
        ));

        // ATAN2
        self.register_function(FunctionInfo::new(
            "atan2".to_string(),
            FunctionType::Scalar,
            FunctionClassification::Mathematical,
            LogicalType::Double,
            vec![LogicalType::Double, LogicalType::Double],
        ));

        // Utility Functions

        // PI
        self.register_function(FunctionInfo::new(
            "pi".to_string(),
            FunctionType::Scalar,
            FunctionClassification::Mathematical,
            LogicalType::Double,
            vec![],
        ));

        // DEGREES
        self.register_function(FunctionInfo::new(
            "degrees".to_string(),
            FunctionType::Scalar,
            FunctionClassification::Mathematical,
            LogicalType::Double,
            vec![LogicalType::Double],
        ));

        // RADIANS
        self.register_function(FunctionInfo::new(
            "radians".to_string(),
            FunctionType::Scalar,
            FunctionClassification::Mathematical,
            LogicalType::Double,
            vec![LogicalType::Double],
        ));
    }

    fn register_string_functions(&mut self) {
        // LENGTH
        self.register_function(FunctionInfo::new(
            "length".to_string(),
            FunctionType::Scalar,
            FunctionClassification::String,
            LogicalType::Integer,
            vec![LogicalType::Varchar],
        ));

        // UPPER
        self.register_function(FunctionInfo::new(
            "upper".to_string(),
            FunctionType::Scalar,
            FunctionClassification::String,
            LogicalType::Varchar,
            vec![LogicalType::Varchar],
        ));

        // LOWER
        self.register_function(FunctionInfo::new(
            "lower".to_string(),
            FunctionType::Scalar,
            FunctionClassification::String,
            LogicalType::Varchar,
            vec![LogicalType::Varchar],
        ));

        // SUBSTRING
        self.register_function(FunctionInfo::new(
            "substring".to_string(),
            FunctionType::Scalar,
            FunctionClassification::String,
            LogicalType::Varchar,
            vec![
                LogicalType::Varchar,
                LogicalType::Integer,
                LogicalType::Integer,
            ],
        ));

        // CONCAT
        self.register_function(
            FunctionInfo::new(
                "concat".to_string(),
                FunctionType::Scalar,
                FunctionClassification::String,
                LogicalType::Varchar,
                vec![LogicalType::Varchar, LogicalType::Varchar],
            )
            .variadic(),
        );

        // TRIM
        self.register_function(FunctionInfo::new(
            "trim".to_string(),
            FunctionType::Scalar,
            FunctionClassification::String,
            LogicalType::Varchar,
            vec![LogicalType::Varchar],
        ));

        // LTRIM
        self.register_function(FunctionInfo::new(
            "ltrim".to_string(),
            FunctionType::Scalar,
            FunctionClassification::String,
            LogicalType::Varchar,
            vec![LogicalType::Varchar],
        ));

        // RTRIM
        self.register_function(FunctionInfo::new(
            "rtrim".to_string(),
            FunctionType::Scalar,
            FunctionClassification::String,
            LogicalType::Varchar,
            vec![LogicalType::Varchar],
        ));

        // LEFT
        self.register_function(FunctionInfo::new(
            "left".to_string(),
            FunctionType::Scalar,
            FunctionClassification::String,
            LogicalType::Varchar,
            vec![LogicalType::Varchar, LogicalType::Integer],
        ));

        // RIGHT
        self.register_function(FunctionInfo::new(
            "right".to_string(),
            FunctionType::Scalar,
            FunctionClassification::String,
            LogicalType::Varchar,
            vec![LogicalType::Varchar, LogicalType::Integer],
        ));

        // REVERSE
        self.register_function(FunctionInfo::new(
            "reverse".to_string(),
            FunctionType::Scalar,
            FunctionClassification::String,
            LogicalType::Varchar,
            vec![LogicalType::Varchar],
        ));

        // REPEAT
        self.register_function(FunctionInfo::new(
            "repeat".to_string(),
            FunctionType::Scalar,
            FunctionClassification::String,
            LogicalType::Varchar,
            vec![LogicalType::Varchar, LogicalType::Integer],
        ));

        // REPLACE
        self.register_function(FunctionInfo::new(
            "replace".to_string(),
            FunctionType::Scalar,
            FunctionClassification::String,
            LogicalType::Varchar,
            vec![
                LogicalType::Varchar,
                LogicalType::Varchar,
                LogicalType::Varchar,
            ],
        ));

        // POSITION
        self.register_function(FunctionInfo::new(
            "position".to_string(),
            FunctionType::Scalar,
            FunctionClassification::String,
            LogicalType::Integer,
            vec![LogicalType::Varchar, LogicalType::Varchar],
        ));

        // STRPOS
        self.register_function(FunctionInfo::new(
            "strpos".to_string(),
            FunctionType::Scalar,
            FunctionClassification::String,
            LogicalType::Integer,
            vec![LogicalType::Varchar, LogicalType::Varchar],
        ));

        // INSTR
        self.register_function(FunctionInfo::new(
            "instr".to_string(),
            FunctionType::Scalar,
            FunctionClassification::String,
            LogicalType::Integer,
            vec![LogicalType::Varchar, LogicalType::Varchar],
        ));

        // CONTAINS
        self.register_function(FunctionInfo::new(
            "contains".to_string(),
            FunctionType::Scalar,
            FunctionClassification::String,
            LogicalType::Boolean,
            vec![LogicalType::Varchar, LogicalType::Varchar],
        ));

        // LPAD (2 args)
        self.register_function(FunctionInfo::new(
            "lpad".to_string(),
            FunctionType::Scalar,
            FunctionClassification::String,
            LogicalType::Varchar,
            vec![LogicalType::Varchar, LogicalType::Integer],
        ));

        // LPAD (3 args)
        self.register_function(FunctionInfo::new(
            "lpad".to_string(),
            FunctionType::Scalar,
            FunctionClassification::String,
            LogicalType::Varchar,
            vec![
                LogicalType::Varchar,
                LogicalType::Integer,
                LogicalType::Varchar,
            ],
        ));

        // RPAD (2 args)
        self.register_function(FunctionInfo::new(
            "rpad".to_string(),
            FunctionType::Scalar,
            FunctionClassification::String,
            LogicalType::Varchar,
            vec![LogicalType::Varchar, LogicalType::Integer],
        ));

        // RPAD (3 args)
        self.register_function(FunctionInfo::new(
            "rpad".to_string(),
            FunctionType::Scalar,
            FunctionClassification::String,
            LogicalType::Varchar,
            vec![
                LogicalType::Varchar,
                LogicalType::Integer,
                LogicalType::Varchar,
            ],
        ));

        // SPLIT_PART
        self.register_function(FunctionInfo::new(
            "split_part".to_string(),
            FunctionType::Scalar,
            FunctionClassification::String,
            LogicalType::Varchar,
            vec![
                LogicalType::Varchar,
                LogicalType::Varchar,
                LogicalType::Integer,
            ],
        ));

        // STARTS_WITH
        self.register_function(FunctionInfo::new(
            "starts_with".to_string(),
            FunctionType::Scalar,
            FunctionClassification::String,
            LogicalType::Boolean,
            vec![LogicalType::Varchar, LogicalType::Varchar],
        ));

        // ENDS_WITH
        self.register_function(FunctionInfo::new(
            "ends_with".to_string(),
            FunctionType::Scalar,
            FunctionClassification::String,
            LogicalType::Boolean,
            vec![LogicalType::Varchar, LogicalType::Varchar],
        ));

        // ASCII
        self.register_function(FunctionInfo::new(
            "ascii".to_string(),
            FunctionType::Scalar,
            FunctionClassification::String,
            LogicalType::Integer,
            vec![LogicalType::Varchar],
        ));

        // CHR
        self.register_function(FunctionInfo::new(
            "chr".to_string(),
            FunctionType::Scalar,
            FunctionClassification::String,
            LogicalType::Varchar,
            vec![LogicalType::Integer],
        ));

        // INITCAP
        self.register_function(FunctionInfo::new(
            "initcap".to_string(),
            FunctionType::Scalar,
            FunctionClassification::String,
            LogicalType::Varchar,
            vec![LogicalType::Varchar],
        ));

        // REGEXP_MATCHES
        self.register_function(FunctionInfo::new(
            "regexp_matches".to_string(),
            FunctionType::Scalar,
            FunctionClassification::String,
            LogicalType::Boolean,
            vec![LogicalType::Varchar, LogicalType::Varchar],
        ));

        // REGEXP_REPLACE
        self.register_function(FunctionInfo::new(
            "regexp_replace".to_string(),
            FunctionType::Scalar,
            FunctionClassification::String,
            LogicalType::Varchar,
            vec![
                LogicalType::Varchar,
                LogicalType::Varchar,
                LogicalType::Varchar,
            ],
        ));

        // CHAR_LENGTH
        self.register_function(FunctionInfo::new(
            "char_length".to_string(),
            FunctionType::Scalar,
            FunctionClassification::String,
            LogicalType::Integer,
            vec![LogicalType::Varchar],
        ));

        // OCTET_LENGTH
        self.register_function(FunctionInfo::new(
            "octet_length".to_string(),
            FunctionType::Scalar,
            FunctionClassification::String,
            LogicalType::Integer,
            vec![LogicalType::Varchar],
        ));

        // BIT_LENGTH
        self.register_function(FunctionInfo::new(
            "bit_length".to_string(),
            FunctionType::Scalar,
            FunctionClassification::String,
            LogicalType::Integer,
            vec![LogicalType::Varchar],
        ));

        // OVERLAY (3 args)
        self.register_function(FunctionInfo::new(
            "overlay".to_string(),
            FunctionType::Scalar,
            FunctionClassification::String,
            LogicalType::Varchar,
            vec![
                LogicalType::Varchar,
                LogicalType::Varchar,
                LogicalType::Integer,
            ],
        ));

        // OVERLAY (4 args)
        self.register_function(FunctionInfo::new(
            "overlay".to_string(),
            FunctionType::Scalar,
            FunctionClassification::String,
            LogicalType::Varchar,
            vec![
                LogicalType::Varchar,
                LogicalType::Varchar,
                LogicalType::Integer,
                LogicalType::Integer,
            ],
        ));

        // QUOTE
        self.register_function(FunctionInfo::new(
            "quote".to_string(),
            FunctionType::Scalar,
            FunctionClassification::String,
            LogicalType::Varchar,
            vec![LogicalType::Varchar],
        ));
    }

    fn register_conditional_functions(&mut self) {
        // COALESCE
        self.register_function(
            FunctionInfo::new(
                "coalesce".to_string(),
                FunctionType::Scalar,
                FunctionClassification::Conditional,
                LogicalType::Varchar, // Will be inferred based on arguments
                vec![],
            )
            .variadic(),
        );

        // NULLIF
        self.register_function(FunctionInfo::new(
            "nullif".to_string(),
            FunctionType::Scalar,
            FunctionClassification::Conditional,
            LogicalType::Varchar, // Will be inferred based on arguments
            vec![LogicalType::Varchar, LogicalType::Varchar],
        ));

        // GREATEST
        self.register_function(
            FunctionInfo::new(
                "greatest".to_string(),
                FunctionType::Scalar,
                FunctionClassification::Conditional,
                LogicalType::Varchar, // Will be inferred based on arguments
                vec![],
            )
            .variadic(),
        );

        // LEAST
        self.register_function(
            FunctionInfo::new(
                "least".to_string(),
                FunctionType::Scalar,
                FunctionClassification::Conditional,
                LogicalType::Varchar, // Will be inferred based on arguments
                vec![],
            )
            .variadic(),
        );
    }

    fn register_system_functions(&mut self) {
        // CURRENT_DATE
        self.register_function(
            FunctionInfo::new(
                "current_date".to_string(),
                FunctionType::Scalar,
                FunctionClassification::System,
                LogicalType::Date,
                vec![],
            )
            .non_deterministic(),
        );

        // CURRENT_TIME
        self.register_function(
            FunctionInfo::new(
                "current_time".to_string(),
                FunctionType::Scalar,
                FunctionClassification::System,
                LogicalType::Time,
                vec![],
            )
            .non_deterministic(),
        );

        // CURRENT_TIMESTAMP
        self.register_function(
            FunctionInfo::new(
                "current_timestamp".to_string(),
                FunctionType::Scalar,
                FunctionClassification::System,
                LogicalType::Timestamp,
                vec![],
            )
            .non_deterministic(),
        );

        // RANDOM
        self.register_function(
            FunctionInfo::new(
                "random".to_string(),
                FunctionType::Scalar,
                FunctionClassification::System,
                LogicalType::Double,
                vec![],
            )
            .non_deterministic(),
        );

        // VERSION
        self.register_function(FunctionInfo::new(
            "version".to_string(),
            FunctionType::Scalar,
            FunctionClassification::System,
            LogicalType::Varchar,
            vec![],
        ));
    }
}

impl Default for FunctionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Evaluate a built-in function
pub fn evaluate_builtin_function(name: &str, arguments: &[Value]) -> PrismDBResult<Value> {
    use crate::expression::math_functions;
    use crate::expression::operator::{evaluate_binary_operator, OperatorType};

    match name.to_uppercase().as_str() {
        // Arithmetic operators
        "ADD" => {
            if arguments.len() != 2 {
                return Err(PrismDBError::InvalidArgument(
                    "ADD requires 2 arguments".to_string(),
                ));
            }
            evaluate_binary_operator(&OperatorType::Add, &arguments[0], &arguments[1])
        }
        "SUBTRACT" => {
            if arguments.len() != 2 {
                return Err(PrismDBError::InvalidArgument(
                    "SUBTRACT requires 2 arguments".to_string(),
                ));
            }
            evaluate_binary_operator(&OperatorType::Subtract, &arguments[0], &arguments[1])
        }
        "MULTIPLY" => {
            if arguments.len() != 2 {
                return Err(PrismDBError::InvalidArgument(
                    "MULTIPLY requires 2 arguments".to_string(),
                ));
            }
            evaluate_binary_operator(&OperatorType::Multiply, &arguments[0], &arguments[1])
        }
        "DIVIDE" => {
            if arguments.len() != 2 {
                return Err(PrismDBError::InvalidArgument(
                    "DIVIDE requires 2 arguments".to_string(),
                ));
            }
            evaluate_binary_operator(&OperatorType::Divide, &arguments[0], &arguments[1])
        }
        "MODULO" | "MOD" => {
            if arguments.len() != 2 {
                return Err(PrismDBError::InvalidArgument(
                    "MODULO requires 2 arguments".to_string(),
                ));
            }
            evaluate_binary_operator(&OperatorType::Modulo, &arguments[0], &arguments[1])
        }
        "AND" => {
            if arguments.len() != 2 {
                return Err(PrismDBError::InvalidArgument(
                    "AND requires 2 arguments".to_string(),
                ));
            }
            evaluate_binary_operator(&OperatorType::And, &arguments[0], &arguments[1])
        }
        "OR" => {
            if arguments.len() != 2 {
                return Err(PrismDBError::InvalidArgument(
                    "OR requires 2 arguments".to_string(),
                ));
            }
            evaluate_binary_operator(&OperatorType::Or, &arguments[0], &arguments[1])
        }
        "LIKE" => {
            if arguments.len() != 2 {
                return Err(PrismDBError::InvalidArgument(
                    "LIKE requires 2 arguments".to_string(),
                ));
            }
            evaluate_binary_operator(&OperatorType::Like, &arguments[0], &arguments[1])
        }
        // Mathematical functions - Basic
        "ABS" => {
            if arguments.len() != 1 {
                return Err(PrismDBError::InvalidArgument(
                    "ABS requires 1 argument".to_string(),
                ));
            }
            math_functions::abs(&arguments[0])
        }
        "SIGN" => {
            if arguments.len() != 1 {
                return Err(PrismDBError::InvalidArgument(
                    "SIGN requires 1 argument".to_string(),
                ));
            }
            math_functions::sign(&arguments[0])
        }
        "SQRT" => {
            if arguments.len() != 1 {
                return Err(PrismDBError::InvalidArgument(
                    "SQRT requires 1 argument".to_string(),
                ));
            }
            math_functions::sqrt(&arguments[0])
        }
        "POWER" | "POW" => {
            if arguments.len() != 2 {
                return Err(PrismDBError::InvalidArgument(
                    "POWER requires 2 arguments".to_string(),
                ));
            }
            math_functions::power(&arguments[0], &arguments[1])
        }
        "EXP" => {
            if arguments.len() != 1 {
                return Err(PrismDBError::InvalidArgument(
                    "EXP requires 1 argument".to_string(),
                ));
            }
            math_functions::exp(&arguments[0])
        }

        // Mathematical functions - Logarithmic
        "LN" => {
            if arguments.len() != 1 {
                return Err(PrismDBError::InvalidArgument(
                    "LN requires 1 argument".to_string(),
                ));
            }
            math_functions::ln(&arguments[0])
        }
        "LOG" | "LOG10" => {
            if arguments.len() != 1 {
                return Err(PrismDBError::InvalidArgument(
                    "LOG requires 1 argument".to_string(),
                ));
            }
            math_functions::log10(&arguments[0])
        }
        "LOG2" => {
            if arguments.len() != 1 {
                return Err(PrismDBError::InvalidArgument(
                    "LOG2 requires 1 argument".to_string(),
                ));
            }
            math_functions::log2(&arguments[0])
        }

        // Mathematical functions - Rounding
        "CEIL" | "CEILING" => {
            if arguments.len() != 1 {
                return Err(PrismDBError::InvalidArgument(
                    "CEIL requires 1 argument".to_string(),
                ));
            }
            math_functions::ceil(&arguments[0])
        }
        "FLOOR" => {
            if arguments.len() != 1 {
                return Err(PrismDBError::InvalidArgument(
                    "FLOOR requires 1 argument".to_string(),
                ));
            }
            math_functions::floor(&arguments[0])
        }
        "ROUND" => {
            if arguments.is_empty() || arguments.len() > 2 {
                return Err(PrismDBError::InvalidArgument(
                    "ROUND requires 1 or 2 arguments".to_string(),
                ));
            }
            let decimals = if arguments.len() == 2 {
                match &arguments[1] {
                    Value::Integer(d) => Some(*d),
                    _ => {
                        return Err(PrismDBError::Type(
                            "ROUND decimals must be integer".to_string(),
                        ))
                    }
                }
            } else {
                None
            };
            math_functions::round(&arguments[0], decimals)
        }
        "TRUNC" => {
            if arguments.is_empty() || arguments.len() > 2 {
                return Err(PrismDBError::InvalidArgument(
                    "TRUNC requires 1 or 2 arguments".to_string(),
                ));
            }
            let decimals = if arguments.len() == 2 {
                match &arguments[1] {
                    Value::Integer(d) => Some(*d),
                    _ => {
                        return Err(PrismDBError::Type(
                            "TRUNC decimals must be integer".to_string(),
                        ))
                    }
                }
            } else {
                None
            };
            math_functions::trunc(&arguments[0], decimals)
        }

        // Mathematical functions - Trigonometric
        "SIN" => {
            if arguments.len() != 1 {
                return Err(PrismDBError::InvalidArgument(
                    "SIN requires 1 argument".to_string(),
                ));
            }
            math_functions::sin(&arguments[0])
        }
        "COS" => {
            if arguments.len() != 1 {
                return Err(PrismDBError::InvalidArgument(
                    "COS requires 1 argument".to_string(),
                ));
            }
            math_functions::cos(&arguments[0])
        }
        "TAN" => {
            if arguments.len() != 1 {
                return Err(PrismDBError::InvalidArgument(
                    "TAN requires 1 argument".to_string(),
                ));
            }
            math_functions::tan(&arguments[0])
        }
        "ASIN" => {
            if arguments.len() != 1 {
                return Err(PrismDBError::InvalidArgument(
                    "ASIN requires 1 argument".to_string(),
                ));
            }
            math_functions::asin(&arguments[0])
        }
        "ACOS" => {
            if arguments.len() != 1 {
                return Err(PrismDBError::InvalidArgument(
                    "ACOS requires 1 argument".to_string(),
                ));
            }
            math_functions::acos(&arguments[0])
        }
        "ATAN" => {
            if arguments.len() != 1 {
                return Err(PrismDBError::InvalidArgument(
                    "ATAN requires 1 argument".to_string(),
                ));
            }
            math_functions::atan(&arguments[0])
        }
        "ATAN2" => {
            if arguments.len() != 2 {
                return Err(PrismDBError::InvalidArgument(
                    "ATAN2 requires 2 arguments".to_string(),
                ));
            }
            math_functions::atan2(&arguments[0], &arguments[1])
        }

        // Mathematical functions - Utility
        "PI" => {
            if !arguments.is_empty() {
                return Err(PrismDBError::InvalidArgument(
                    "PI takes no arguments".to_string(),
                ));
            }
            Ok(math_functions::pi())
        }
        "DEGREES" => {
            if arguments.len() != 1 {
                return Err(PrismDBError::InvalidArgument(
                    "DEGREES requires 1 argument".to_string(),
                ));
            }
            math_functions::degrees(&arguments[0])
        }
        "RADIANS" => {
            if arguments.len() != 1 {
                return Err(PrismDBError::InvalidArgument(
                    "RADIANS requires 1 argument".to_string(),
                ));
            }
            math_functions::radians(&arguments[0])
        }

        // String functions
        "LENGTH" | "CHAR_LENGTH" => evaluate_length(arguments),
        "UPPER" => evaluate_upper(arguments),
        "LOWER" => evaluate_lower(arguments),
        "SUBSTRING" => evaluate_substring(arguments),
        "CONCAT" => evaluate_concat(arguments),
        "TRIM" => evaluate_trim(arguments),
        "LTRIM" => evaluate_ltrim(arguments),
        "RTRIM" => evaluate_rtrim(arguments),

        // String manipulation functions
        "LEFT" => {
            use crate::expression::string_functions;
            if arguments.len() != 2 {
                return Err(PrismDBError::InvalidArgument(
                    "LEFT requires 2 arguments".to_string(),
                ));
            }
            string_functions::left(&arguments[0], &arguments[1])
        }
        "RIGHT" => {
            use crate::expression::string_functions;
            if arguments.len() != 2 {
                return Err(PrismDBError::InvalidArgument(
                    "RIGHT requires 2 arguments".to_string(),
                ));
            }
            string_functions::right(&arguments[0], &arguments[1])
        }
        "REVERSE" => {
            use crate::expression::string_functions;
            if arguments.len() != 1 {
                return Err(PrismDBError::InvalidArgument(
                    "REVERSE requires 1 argument".to_string(),
                ));
            }
            string_functions::reverse(&arguments[0])
        }
        "REPEAT" => {
            use crate::expression::string_functions;
            if arguments.len() != 2 {
                return Err(PrismDBError::InvalidArgument(
                    "REPEAT requires 2 arguments".to_string(),
                ));
            }
            string_functions::repeat(&arguments[0], &arguments[1])
        }
        "REPLACE" => {
            use crate::expression::string_functions;
            if arguments.len() != 3 {
                return Err(PrismDBError::InvalidArgument(
                    "REPLACE requires 3 arguments".to_string(),
                ));
            }
            string_functions::replace(&arguments[0], &arguments[1], &arguments[2])
        }

        // String search functions
        "POSITION" => {
            use crate::expression::string_functions;
            if arguments.len() != 2 {
                return Err(PrismDBError::InvalidArgument(
                    "POSITION requires 2 arguments".to_string(),
                ));
            }
            string_functions::position(&arguments[0], &arguments[1])
        }
        "STRPOS" => {
            use crate::expression::string_functions;
            if arguments.len() != 2 {
                return Err(PrismDBError::InvalidArgument(
                    "STRPOS requires 2 arguments".to_string(),
                ));
            }
            string_functions::strpos(&arguments[0], &arguments[1])
        }
        "INSTR" => {
            use crate::expression::string_functions;
            if arguments.len() != 2 {
                return Err(PrismDBError::InvalidArgument(
                    "INSTR requires 2 arguments".to_string(),
                ));
            }
            string_functions::instr(&arguments[0], &arguments[1])
        }
        "CONTAINS" => {
            use crate::expression::string_functions;
            if arguments.len() != 2 {
                return Err(PrismDBError::InvalidArgument(
                    "CONTAINS requires 2 arguments".to_string(),
                ));
            }
            string_functions::contains(&arguments[0], &arguments[1])
        }

        // String padding functions
        "LPAD" => {
            use crate::expression::string_functions;
            if arguments.len() < 2 || arguments.len() > 3 {
                return Err(PrismDBError::InvalidArgument(
                    "LPAD requires 2 or 3 arguments".to_string(),
                ));
            }
            let fill = if arguments.len() == 3 {
                Some(&arguments[2])
            } else {
                None
            };
            string_functions::lpad(&arguments[0], &arguments[1], fill)
        }
        "RPAD" => {
            use crate::expression::string_functions;
            if arguments.len() < 2 || arguments.len() > 3 {
                return Err(PrismDBError::InvalidArgument(
                    "RPAD requires 2 or 3 arguments".to_string(),
                ));
            }
            let fill = if arguments.len() == 3 {
                Some(&arguments[2])
            } else {
                None
            };
            string_functions::rpad(&arguments[0], &arguments[1], fill)
        }

        // String splitting
        "SPLIT_PART" => {
            use crate::expression::string_functions;
            if arguments.len() != 3 {
                return Err(PrismDBError::InvalidArgument(
                    "SPLIT_PART requires 3 arguments".to_string(),
                ));
            }
            string_functions::split_part(&arguments[0], &arguments[1], &arguments[2])
        }

        // String testing
        "STARTS_WITH" => {
            use crate::expression::string_functions;
            if arguments.len() != 2 {
                return Err(PrismDBError::InvalidArgument(
                    "STARTS_WITH requires 2 arguments".to_string(),
                ));
            }
            string_functions::starts_with(&arguments[0], &arguments[1])
        }
        "ENDS_WITH" => {
            use crate::expression::string_functions;
            if arguments.len() != 2 {
                return Err(PrismDBError::InvalidArgument(
                    "ENDS_WITH requires 2 arguments".to_string(),
                ));
            }
            string_functions::ends_with(&arguments[0], &arguments[1])
        }

        // Character conversion
        "ASCII" => {
            use crate::expression::string_functions;
            if arguments.len() != 1 {
                return Err(PrismDBError::InvalidArgument(
                    "ASCII requires 1 argument".to_string(),
                ));
            }
            string_functions::ascii(&arguments[0])
        }
        "CHR" => {
            use crate::expression::string_functions;
            if arguments.len() != 1 {
                return Err(PrismDBError::InvalidArgument(
                    "CHR requires 1 argument".to_string(),
                ));
            }
            string_functions::chr(&arguments[0])
        }

        // String formatting
        "INITCAP" => {
            use crate::expression::string_functions;
            if arguments.len() != 1 {
                return Err(PrismDBError::InvalidArgument(
                    "INITCAP requires 1 argument".to_string(),
                ));
            }
            string_functions::initcap(&arguments[0])
        }

        // Regular expressions
        "REGEXP_MATCHES" => {
            use crate::expression::string_functions;
            if arguments.len() != 2 {
                return Err(PrismDBError::InvalidArgument(
                    "REGEXP_MATCHES requires 2 arguments".to_string(),
                ));
            }
            string_functions::regexp_matches(&arguments[0], &arguments[1])
        }
        "REGEXP_REPLACE" => {
            use crate::expression::string_functions;
            if arguments.len() != 3 {
                return Err(PrismDBError::InvalidArgument(
                    "REGEXP_REPLACE requires 3 arguments".to_string(),
                ));
            }
            string_functions::regexp_replace(&arguments[0], &arguments[1], &arguments[2])
        }

        // String length functions
        "OCTET_LENGTH" => {
            use crate::expression::string_functions;
            if arguments.len() != 1 {
                return Err(PrismDBError::InvalidArgument(
                    "OCTET_LENGTH requires 1 argument".to_string(),
                ));
            }
            string_functions::octet_length(&arguments[0])
        }
        "BIT_LENGTH" => {
            use crate::expression::string_functions;
            if arguments.len() != 1 {
                return Err(PrismDBError::InvalidArgument(
                    "BIT_LENGTH requires 1 argument".to_string(),
                ));
            }
            string_functions::bit_length(&arguments[0])
        }

        // String overlay
        "OVERLAY" => {
            use crate::expression::string_functions;
            if arguments.len() < 3 || arguments.len() > 4 {
                return Err(PrismDBError::InvalidArgument(
                    "OVERLAY requires 3 or 4 arguments".to_string(),
                ));
            }
            let length = if arguments.len() == 4 {
                Some(&arguments[3])
            } else {
                None
            };
            string_functions::overlay(&arguments[0], &arguments[1], &arguments[2], length)
        }

        // String quoting
        "QUOTE" => {
            use crate::expression::string_functions;
            if arguments.len() != 1 {
                return Err(PrismDBError::InvalidArgument(
                    "QUOTE requires 1 argument".to_string(),
                ));
            }
            string_functions::quote(&arguments[0])
        }

        // Conditional functions
        "COALESCE" => evaluate_coalesce(arguments),
        "NULLIF" => evaluate_nullif(arguments),
        "GREATEST" => evaluate_greatest(arguments),
        "LEAST" => evaluate_least(arguments),
        "IS_NULL" => evaluate_is_null(arguments),
        "IS_NOT_NULL" => evaluate_is_not_null(arguments),

        // System functions
        "CURRENT_DATE" => evaluate_current_date(),
        "CURRENT_TIME" => evaluate_current_time(),
        "CURRENT_TIMESTAMP" => evaluate_current_timestamp(),
        "RANDOM" => {
            if !arguments.is_empty() {
                return Err(PrismDBError::InvalidArgument(
                    "RANDOM takes no arguments".to_string(),
                ));
            }
            Ok(math_functions::random())
        }
        "VERSION" => evaluate_version(),

        _ => Err(PrismDBError::InvalidType(format!(
            "Unknown function: {}",
            name
        ))),
    }
}

// Mathematical function implementations
#[allow(dead_code)]
fn evaluate_abs(arguments: &[Value]) -> PrismDBResult<Value> {
    if arguments.len() != 1 {
        return Err(PrismDBError::InvalidArgument(
            "ABS function requires exactly 1 argument".to_string(),
        ));
    }

    match &arguments[0] {
        Value::Integer(v) => Ok(Value::Integer(v.abs())),
        Value::BigInt(v) => Ok(Value::BigInt(v.abs())),
        Value::Float(v) => Ok(Value::Float(v.abs())),
        Value::Double(v) => Ok(Value::Double(v.abs())),
        _ => Err(PrismDBError::Type(
            "ABS function requires numeric argument".to_string(),
        )),
    }
}

#[allow(dead_code)]
fn evaluate_round(arguments: &[Value]) -> PrismDBResult<Value> {
    if arguments.len() != 1 {
        return Err(PrismDBError::InvalidArgument(
            "ROUND function requires exactly 1 argument".to_string(),
        ));
    }

    match &arguments[0] {
        Value::Float(v) => Ok(Value::Float(v.round())),
        Value::Double(v) => Ok(Value::Double(v.round())),
        _ => Err(PrismDBError::Type(
            "ROUND function requires numeric argument".to_string(),
        )),
    }
}

#[allow(dead_code)]
fn evaluate_ceil(arguments: &[Value]) -> PrismDBResult<Value> {
    if arguments.len() != 1 {
        return Err(PrismDBError::InvalidArgument(
            "CEIL function requires exactly 1 argument".to_string(),
        ));
    }

    match &arguments[0] {
        Value::Float(v) => Ok(Value::Float(v.ceil())),
        Value::Double(v) => Ok(Value::Double(v.ceil())),
        _ => Err(PrismDBError::Type(
            "CEIL function requires numeric argument".to_string(),
        )),
    }
}

#[allow(dead_code)]
fn evaluate_floor(arguments: &[Value]) -> PrismDBResult<Value> {
    if arguments.len() != 1 {
        return Err(PrismDBError::InvalidArgument(
            "FLOOR function requires exactly 1 argument".to_string(),
        ));
    }

    match &arguments[0] {
        Value::Float(v) => Ok(Value::Float(v.floor())),
        Value::Double(v) => Ok(Value::Double(v.floor())),
        _ => Err(PrismDBError::Type(
            "FLOOR function requires numeric argument".to_string(),
        )),
    }
}

#[allow(dead_code)]
fn evaluate_sqrt(arguments: &[Value]) -> PrismDBResult<Value> {
    if arguments.len() != 1 {
        return Err(PrismDBError::InvalidArgument(
            "SQRT function requires exactly 1 argument".to_string(),
        ));
    }

    match &arguments[0] {
        Value::Float(v) => {
            if *v < 0.0 {
                return Err(PrismDBError::Execution(
                    "SQRT function requires non-negative argument".to_string(),
                ));
            }
            Ok(Value::Float(v.sqrt()))
        }
        Value::Double(v) => {
            if *v < 0.0 {
                return Err(PrismDBError::Execution(
                    "SQRT function requires non-negative argument".to_string(),
                ));
            }
            Ok(Value::Double(v.sqrt()))
        }
        _ => Err(PrismDBError::Type(
            "SQRT function requires numeric argument".to_string(),
        )),
    }
}

#[allow(dead_code)]
fn evaluate_power(arguments: &[Value]) -> PrismDBResult<Value> {
    if arguments.len() != 2 {
        return Err(PrismDBError::InvalidArgument(
            "POWER function requires exactly 2 arguments".to_string(),
        ));
    }

    match (&arguments[0], &arguments[1]) {
        (Value::Float(base), Value::Float(exp)) => Ok(Value::Float(base.powf(*exp))),
        (Value::Double(base), Value::Double(exp)) => Ok(Value::Double(base.powf(*exp))),
        _ => Err(PrismDBError::Type(
            "POWER function requires numeric arguments".to_string(),
        )),
    }
}

// String function implementations
fn evaluate_length(arguments: &[Value]) -> PrismDBResult<Value> {
    if arguments.len() != 1 {
        return Err(PrismDBError::InvalidArgument(
            "LENGTH function requires exactly 1 argument".to_string(),
        ));
    }

    match &arguments[0] {
        Value::Varchar(s) => Ok(Value::Integer(s.len() as i32)),
        _ => Err(PrismDBError::Type(
            "LENGTH function requires string argument".to_string(),
        )),
    }
}

fn evaluate_upper(arguments: &[Value]) -> PrismDBResult<Value> {
    if arguments.len() != 1 {
        return Err(PrismDBError::InvalidArgument(
            "UPPER function requires exactly 1 argument".to_string(),
        ));
    }

    match &arguments[0] {
        Value::Varchar(s) => Ok(Value::Varchar(s.to_uppercase())),
        _ => Err(PrismDBError::Type(
            "UPPER function requires string argument".to_string(),
        )),
    }
}

fn evaluate_lower(arguments: &[Value]) -> PrismDBResult<Value> {
    if arguments.len() != 1 {
        return Err(PrismDBError::InvalidArgument(
            "LOWER function requires exactly 1 argument".to_string(),
        ));
    }

    match &arguments[0] {
        Value::Varchar(s) => Ok(Value::Varchar(s.to_lowercase())),
        _ => Err(PrismDBError::Type(
            "LOWER function requires string argument".to_string(),
        )),
    }
}

fn evaluate_substring(arguments: &[Value]) -> PrismDBResult<Value> {
    if arguments.len() != 3 {
        return Err(PrismDBError::InvalidArgument(
            "SUBSTRING function requires exactly 3 arguments".to_string(),
        ));
    }

    match (&arguments[0], &arguments[1], &arguments[2]) {
        (Value::Varchar(s), Value::Integer(start), Value::Integer(length)) => {
            let start_idx = (*start - 1) as usize; // SQL is 1-based
            let length = *length as usize;

            if start_idx >= s.len() {
                return Ok(Value::Varchar(String::new()));
            }

            let end_idx = (start_idx + length).min(s.len());
            Ok(Value::Varchar(s[start_idx..end_idx].to_string()))
        }
        _ => Err(PrismDBError::Type(
            "SUBSTRING function requires (string, integer, integer) arguments".to_string(),
        )),
    }
}

fn evaluate_concat(arguments: &[Value]) -> PrismDBResult<Value> {
    if arguments.is_empty() {
        return Err(PrismDBError::InvalidArgument(
            "CONCAT function requires at least 1 argument".to_string(),
        ));
    }

    let mut result = String::new();
    for arg in arguments {
        match arg {
            Value::Varchar(s) => result.push_str(s),
            _ => {
                return Err(PrismDBError::Type(
                    "CONCAT function requires string arguments".to_string(),
                ))
            }
        }
    }

    Ok(Value::Varchar(result))
}

fn evaluate_trim(arguments: &[Value]) -> PrismDBResult<Value> {
    if arguments.len() != 1 {
        return Err(PrismDBError::InvalidArgument(
            "TRIM function requires exactly 1 argument".to_string(),
        ));
    }

    match &arguments[0] {
        Value::Varchar(s) => Ok(Value::Varchar(s.trim().to_string())),
        _ => Err(PrismDBError::Type(
            "TRIM function requires string argument".to_string(),
        )),
    }
}

fn evaluate_ltrim(arguments: &[Value]) -> PrismDBResult<Value> {
    if arguments.len() != 1 {
        return Err(PrismDBError::InvalidArgument(
            "LTRIM function requires exactly 1 argument".to_string(),
        ));
    }

    match &arguments[0] {
        Value::Varchar(s) => Ok(Value::Varchar(s.trim_start().to_string())),
        _ => Err(PrismDBError::Type(
            "LTRIM function requires string argument".to_string(),
        )),
    }
}

fn evaluate_rtrim(arguments: &[Value]) -> PrismDBResult<Value> {
    if arguments.len() != 1 {
        return Err(PrismDBError::InvalidArgument(
            "RTRIM function requires exactly 1 argument".to_string(),
        ));
    }

    match &arguments[0] {
        Value::Varchar(s) => Ok(Value::Varchar(s.trim_end().to_string())),
        _ => Err(PrismDBError::Type(
            "RTRIM function requires string argument".to_string(),
        )),
    }
}

// Conditional function implementations
fn evaluate_coalesce(arguments: &[Value]) -> PrismDBResult<Value> {
    if arguments.is_empty() {
        return Err(PrismDBError::InvalidArgument(
            "COALESCE function requires at least 1 argument".to_string(),
        ));
    }

    for arg in arguments {
        if !arg.is_null() {
            return Ok(arg.clone());
        }
    }

    Ok(Value::Null)
}

fn evaluate_nullif(arguments: &[Value]) -> PrismDBResult<Value> {
    if arguments.len() != 2 {
        return Err(PrismDBError::InvalidArgument(
            "NULLIF function requires exactly 2 arguments".to_string(),
        ));
    }

    if arguments[0] == arguments[1] {
        Ok(Value::Null)
    } else {
        Ok(arguments[0].clone())
    }
}

fn evaluate_is_null(arguments: &[Value]) -> PrismDBResult<Value> {
    if arguments.len() != 1 {
        return Err(PrismDBError::InvalidArgument(
            "IS_NULL function requires exactly 1 argument".to_string(),
        ));
    }

    Ok(Value::Boolean(arguments[0].is_null()))
}

fn evaluate_is_not_null(arguments: &[Value]) -> PrismDBResult<Value> {
    if arguments.len() != 1 {
        return Err(PrismDBError::InvalidArgument(
            "IS_NOT_NULL function requires exactly 1 argument".to_string(),
        ));
    }

    Ok(Value::Boolean(!arguments[0].is_null()))
}

fn evaluate_greatest(arguments: &[Value]) -> PrismDBResult<Value> {
    if arguments.is_empty() {
        return Err(PrismDBError::InvalidArgument(
            "GREATEST function requires at least 1 argument".to_string(),
        ));
    }

    let mut greatest = &arguments[0];
    for arg in arguments.iter().skip(1) {
        if arg.compare(greatest)? == std::cmp::Ordering::Greater {
            greatest = arg;
        }
    }

    Ok(greatest.clone())
}

fn evaluate_least(arguments: &[Value]) -> PrismDBResult<Value> {
    if arguments.is_empty() {
        return Err(PrismDBError::InvalidArgument(
            "LEAST function requires at least 1 argument".to_string(),
        ));
    }

    let mut least = &arguments[0];
    for arg in arguments.iter().skip(1) {
        if arg.compare(least)? == std::cmp::Ordering::Less {
            least = arg;
        }
    }

    Ok(least.clone())
}

// System function implementations
fn evaluate_current_date() -> PrismDBResult<Value> {
    use chrono::{Datelike, Local};
    let now = Local::now();
    Ok(Value::Date(
        now.num_days_from_ce() as i32 - 719528, // Days since 1970-01-01
    ))
}

fn evaluate_current_time() -> PrismDBResult<Value> {
    use chrono::{Local, Timelike};
    let now = Local::now();
    let time_since_midnight = now.num_seconds_from_midnight() as i64 * 1_000_000; // Convert to microseconds
    Ok(Value::Time(time_since_midnight))
}

fn evaluate_current_timestamp() -> PrismDBResult<Value> {
    use chrono::{Local, Utc};
    let now = Local::now();
    let utc_now = now.with_timezone(&Utc);
    let timestamp = utc_now.timestamp_micros();
    Ok(Value::Timestamp(timestamp))
}

#[allow(dead_code)]
fn evaluate_random() -> PrismDBResult<Value> {
    use rand::Rng;
    let mut rng = rand::rng();
    Ok(Value::Double(rng.random::<f64>()))
}

fn evaluate_version() -> PrismDBResult<Value> {
    Ok(Value::Varchar("PrismDB v0.1.0".to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Value;

    #[test]
    fn test_mathematical_functions() -> PrismDBResult<()> {
        assert_eq!(evaluate_abs(&[Value::integer(-5)])?, Value::integer(5));
        assert_eq!(evaluate_round(&[Value::double(3.7)])?, Value::double(4.0));
        assert_eq!(evaluate_ceil(&[Value::double(3.2)])?, Value::double(4.0));
        assert_eq!(evaluate_floor(&[Value::double(3.8)])?, Value::double(3.0));
        assert_eq!(evaluate_sqrt(&[Value::double(16.0)])?, Value::double(4.0));
        assert_eq!(
            evaluate_power(&[Value::double(2.0), Value::double(3.0)])?,
            Value::double(8.0)
        );

        Ok(())
    }

    #[test]
    fn test_string_functions() -> PrismDBResult<()> {
        assert_eq!(
            evaluate_length(&[Value::varchar("hello".to_string())])?,
            Value::integer(5)
        );
        assert_eq!(
            evaluate_upper(&[Value::varchar("hello".to_string())])?,
            Value::varchar("HELLO".to_string())
        );
        assert_eq!(
            evaluate_lower(&[Value::varchar("HELLO".to_string())])?,
            Value::varchar("hello".to_string())
        );
        assert_eq!(
            evaluate_substring(&[
                Value::varchar("hello".to_string()),
                Value::integer(2),
                Value::integer(3)
            ])?,
            Value::varchar("ell".to_string())
        );
        assert_eq!(
            evaluate_concat(&[
                Value::varchar("hello".to_string()),
                Value::varchar("world".to_string())
            ])?,
            Value::varchar("helloworld".to_string())
        );

        Ok(())
    }

    #[test]
    fn test_conditional_functions() -> PrismDBResult<()> {
        assert_eq!(
            evaluate_coalesce(&[Value::Null, Value::integer(42)])?,
            Value::integer(42)
        );
        assert_eq!(
            evaluate_nullif(&[Value::integer(5), Value::integer(5)])?,
            Value::Null
        );
        assert_eq!(
            evaluate_greatest(&[Value::integer(1), Value::integer(3), Value::integer(2)])?,
            Value::integer(3)
        );
        assert_eq!(
            evaluate_least(&[Value::integer(1), Value::integer(3), Value::integer(2)])?,
            Value::integer(1)
        );

        Ok(())
    }

    #[test]
    fn test_function_registry() {
        let registry = FunctionRegistry::new();

        // Test function lookup
        let abs_func = registry.get_function("abs", &[LogicalType::Integer]);
        assert!(abs_func.is_some());
        assert_eq!(abs_func.unwrap().name, "abs");

        let concat_func =
            registry.get_function("concat", &[LogicalType::Varchar, LogicalType::Varchar]);
        assert!(concat_func.is_some());
        assert!(concat_func.unwrap().is_variadic);

        // Test non-existent function
        let nonexistent = registry.get_function("nonexistent", &[]);
        assert!(nonexistent.is_none());

        // Test function listing
        let functions = registry.list_functions();
        assert!(functions.contains(&"ABS"));
        assert!(functions.contains(&"CONCAT"));
        assert!(functions.contains(&"RANDOM"));
    }
}
