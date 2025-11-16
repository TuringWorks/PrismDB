//! Database implementation
//!
//! This module provides the main Database struct that ties together
//! all components: catalog, storage, transactions, parser, planner, and executor.

use crate::catalog::Catalog;
use crate::common::error::{PrismDBError, PrismDBResult};
use crate::execution::{CollectedResult, ExecutionContext, ExecutionEngine, ExecutionStats};
use crate::extensions::{ConfigManager, ExtensionManager, SecretsManager};
use crate::extensions::csv_reader::CsvReader;
use crate::extensions::file_reader::FileReader;
use crate::extensions::json_reader::JsonReader;
use crate::extensions::parquet_reader::ParquetReader;
use crate::extensions::sqlite_reader::SqliteReader;
use crate::parser::{tokenizer::Tokenizer, Parser, Statement, SetValue, TableReference, Expression, SelectStatement};
use crate::planner::{LogicalPlan, QueryOptimizer, QueryPlanner};
use crate::storage::{BlockManager, TransactionManager};
use crate::types::{DataChunk, LogicalType, Value};
use std::path::Path;
use std::sync::{Arc, RwLock};

/// Main database instance
pub struct Database {
    /// Catalog for schema/table management
    catalog: Arc<RwLock<Catalog>>,
    /// Transaction manager
    transaction_manager: Arc<TransactionManager>,
    /// Block manager for disk I/O (None for in-memory databases)
    block_manager: Option<Arc<BlockManager>>,
    /// Extension manager
    extension_manager: Arc<ExtensionManager>,
    /// Configuration manager
    config_manager: Arc<ConfigManager>,
    /// Secrets manager
    secrets_manager: Arc<SecretsManager>,
    /// Database configuration
    #[allow(dead_code)]
    config: DatabaseConfig,
}

impl Database {
    /// Create a new in-memory database
    pub fn new_in_memory() -> PrismDBResult<Self> {
        let catalog = Arc::new(RwLock::new(Catalog::new()));
        let transaction_manager = Arc::new(TransactionManager::new());
        let extension_manager = Arc::new(ExtensionManager::new());
        let config_manager = Arc::new(ConfigManager::new());
        let secrets_manager = Arc::new(SecretsManager::new());
        let config = DatabaseConfig::in_memory();

        Ok(Database {
            catalog,
            transaction_manager,
            block_manager: None,
            extension_manager,
            config_manager,
            secrets_manager,
            config,
        })
    }

    /// Open a database from a file
    pub fn open<P: AsRef<Path>>(path: P) -> PrismDBResult<Self> {
        let path_str = path.as_ref().to_string_lossy().to_string();
        let config = DatabaseConfig::from_file(path_str.clone());

        // Create or open the block manager
        let block_manager = BlockManager::new(&path_str)?;

        // Check if this is a new database or existing one
        let is_new = block_manager.get_total_blocks() == 0;

        let catalog = if is_new {
            // New database - create default catalog
            Arc::new(RwLock::new(Catalog::new()))
        } else {
            // Existing database - load catalog from disk
            // For now, create a new catalog
            // TODO: Implement catalog loading from blocks
            Arc::new(RwLock::new(Catalog::new()))
        };

        let transaction_manager = Arc::new(TransactionManager::new());

        Ok(Database {
            catalog,
            transaction_manager,
            block_manager: Some(Arc::new(block_manager)),
            extension_manager: Arc::new(ExtensionManager::new()),
            config_manager: Arc::new(ConfigManager::new()),
            secrets_manager: Arc::new(SecretsManager::new()),
            config,
        })
    }

    /// Sync database to disk (flush all changes)
    pub fn sync(&self) -> PrismDBResult<()> {
        if let Some(ref block_manager) = self.block_manager {
            block_manager.sync()?;
        }
        Ok(())
    }

    /// Check if this is a file-based database
    pub fn is_file_based(&self) -> bool {
        self.block_manager.is_some()
    }

    /// Get the database file path (if file-based)
    pub fn get_file_path(&self) -> Option<&Path> {
        self.block_manager.as_ref().map(|bm| bm.get_file_path())
    }

    /// Execute a SQL query and collect results
    pub fn execute_sql_collect(&self, sql: &str) -> PrismDBResult<QueryResult> {
        // Tokenize the SQL
        let tokenizer = Tokenizer::new();
        let tokens = tokenizer.tokenize(sql)?;

        // Parse the SQL
        let mut parser = Parser::new(tokens);
        let statements = parser.parse_statements()?;

        if statements.is_empty() {
            return Ok(QueryResult::empty());
        }

        // Execute all statements but return only the last result
        let mut last_result = QueryResult::empty();
        for (idx, statement) in statements.iter().enumerate() {
            let _is_last = idx == statements.len() - 1;

        // Handle special statements that don't require planning/execution
        match statement {
            Statement::Install(install) => {
                self.extension_manager.install(&install.extension_name)?;
                last_result = QueryResult::empty();
                continue;
            }
            Statement::Load(load) => {
                self.extension_manager.load(&load.extension_name)?;
                last_result = QueryResult::empty();
                continue;
            }
            Statement::Set(set) => {
                let value_str = match &set.value {
                    SetValue::String(s) => s.clone(),
                    SetValue::Number(n) => n.to_string(),
                    SetValue::Boolean(b) => b.to_string(),
                    SetValue::Default => "DEFAULT".to_string(),
                };
                self.config_manager.set(&set.variable, value_str);
                last_result = QueryResult::empty();
                continue;
            }
            Statement::CreateSecret(secret) => {
                self.secrets_manager.create_secret(
                    secret.name.clone(),
                    secret.secret_type.clone(),
                    secret.options.clone(),
                    secret.or_replace,
                )?;
                last_result = QueryResult::empty();
                continue;
            }
            Statement::Select(select) => {
                // Check if this is a simple table function call
                if let Some(result) = self.try_execute_table_function(select)? {
                    last_result = result;
                    continue;
                }
            }
            _ => {}
        }

        // Plan the query and extract CTEs
        let (logical_plan, ctes) = self.plan_statement(statement)?;

        // Execute the plan with CTEs (optimization happens inside execute_plan)
        last_result = self.execute_plan(logical_plan, ctes)?;
        }

        Ok(last_result)
    }

    /// Plan a SQL statement and return plan with CTEs
    fn plan_statement(&self, statement: &Statement) -> PrismDBResult<(LogicalPlan, std::collections::HashMap<String, LogicalPlan>)> {
        let mut planner = QueryPlanner::new_with_catalog(self.catalog.clone());
        let plan = planner.plan_statement(statement)?;
        let ctes = planner.get_ctes();
        Ok((plan, ctes))
    }

    /// Execute a logical plan
    fn execute_plan(&self, plan: LogicalPlan, ctes: std::collections::HashMap<String, LogicalPlan>) -> PrismDBResult<QueryResult> {
        // Optimize and convert to physical plan with catalog/transaction context and CTEs
        let mut optimizer = QueryOptimizer::new()
            .with_context(self.catalog.clone(), self.transaction_manager.clone())
            .with_ctes(ctes);
        let physical_plan = optimizer.optimize(plan)?;

        // Extract column metadata from physical plan
        let physical_columns = physical_plan.schema();
        let columns: Vec<ColumnMetadata> = physical_columns
            .iter()
            .map(|col| ColumnMetadata {
                name: col.name.clone(),
                data_type: col.data_type.clone(),
            })
            .collect();

        // Create execution context
        let context = ExecutionContext::new(self.transaction_manager.clone(), self.catalog.clone());

        // Execute the physical plan
        let mut engine = ExecutionEngine::new(context);
        let mut stream = engine.execute(physical_plan)?;

        // Collect results
        let mut total_rows = 0;
        let mut all_chunks = Vec::new();

        while let Some(chunk_result) = stream.next() {
            let chunk = chunk_result?;
            total_rows += chunk.len();
            all_chunks.push(chunk);
        }

        Ok(QueryResult {
            chunks: all_chunks,
            row_count: total_rows,
            columns,
        })
    }

    /// Try to execute a table function directly (bypassing planner)
    fn try_execute_table_function(&self, select: &SelectStatement) -> PrismDBResult<Option<QueryResult>> {
        // Check if this is a simple SELECT * FROM table_function(...) query
        if let Some(ref from) = select.from {
            if let TableReference::TableFunction { name, arguments, .. } = from {
                let func_name = name.to_lowercase();
                match func_name.as_str() {
                    "read_csv_auto" => {
                        return Ok(Some(self.execute_read_csv_auto(arguments)?));
                    }
                    "read_parquet" => {
                        return Ok(Some(self.execute_read_parquet(arguments)?));
                    }
                    "read_json_auto" => {
                        return Ok(Some(self.execute_read_json_auto(arguments)?));
                    }
                    "sqlite_scan" => {
                        return Ok(Some(self.execute_sqlite_scan(arguments)?));
                    }
                    _ => {}
                }
            }
        }
        Ok(None)
    }

    /// Execute read_csv_auto table function
    fn execute_read_csv_auto(&self, arguments: &[Expression]) -> PrismDBResult<QueryResult> {
        // Extract the URL argument
        if arguments.is_empty() {
            return Err(PrismDBError::InvalidArgument(
                "read_csv_auto requires at least one argument (file URL)".to_string()
            ));
        }

        let url = match &arguments[0] {
            Expression::Literal(crate::parser::LiteralValue::String(s)) => s.clone(),
            _ => {
                return Err(PrismDBError::InvalidArgument(
                    "read_csv_auto first argument must be a string URL".to_string()
                ));
            }
        };

        println!("Executing read_csv_auto('{}')", url);

        // Create file reader
        let file_reader = FileReader::new()?;

        // Get S3 configuration from secrets manager
        let s3_config = self.secrets_manager.get_s3_config(&self.config_manager);

        // Read the file
        let file_data = file_reader.read_file(&url, Some(&s3_config))?;

        // Parse CSV
        let csv_reader = CsvReader::new(file_data);
        let chunk = csv_reader.read()?;

        // Get column names
        let column_names = csv_reader.get_column_names()?;

        // Build column metadata
        let columns: Vec<ColumnMetadata> = column_names.iter()
            .map(|name| ColumnMetadata {
                name: name.clone(),
                data_type: LogicalType::Varchar, // For now, all VARCHAR
            })
            .collect();

        let row_count = chunk.len();

        Ok(QueryResult {
            chunks: vec![chunk],
            row_count,
            columns,
        })
    }

    /// Execute read_parquet table function
    fn execute_read_parquet(&self, arguments: &[Expression]) -> PrismDBResult<QueryResult> {
        // Extract the URL argument
        if arguments.is_empty() {
            return Err(PrismDBError::InvalidArgument(
                "read_parquet requires at least one argument (file URL)".to_string()
            ));
        }

        let url = match &arguments[0] {
            Expression::Literal(crate::parser::LiteralValue::String(s)) => s.clone(),
            _ => {
                return Err(PrismDBError::InvalidArgument(
                    "read_parquet first argument must be a string URL".to_string()
                ));
            }
        };

        println!("Executing read_parquet('{}')", url);

        // Create file reader
        let file_reader = FileReader::new()?;

        // Get S3 configuration from secrets manager
        let s3_config = self.secrets_manager.get_s3_config(&self.config_manager);

        // Read the file
        let file_data = file_reader.read_file(&url, Some(&s3_config))?;

        // Parse Parquet
        let parquet_reader = ParquetReader::new(file_data);
        let chunk = parquet_reader.read()?;

        // Get column names and types
        let column_names = parquet_reader.get_column_names()?;
        let column_types = parquet_reader.get_column_types()?;

        // Build column metadata
        let columns: Vec<ColumnMetadata> = column_names.iter()
            .zip(column_types.iter())
            .map(|(name, data_type)| ColumnMetadata {
                name: name.clone(),
                data_type: data_type.clone(),
            })
            .collect();

        let row_count = chunk.len();

        Ok(QueryResult {
            chunks: vec![chunk],
            row_count,
            columns,
        })
    }

    /// Execute read_json_auto table function
    fn execute_read_json_auto(&self, arguments: &[Expression]) -> PrismDBResult<QueryResult> {
        // Extract the URL argument
        if arguments.is_empty() {
            return Err(PrismDBError::InvalidArgument(
                "read_json_auto requires at least one argument (file URL)".to_string()
            ));
        }

        let url = match &arguments[0] {
            Expression::Literal(crate::parser::LiteralValue::String(s)) => s.clone(),
            _ => {
                return Err(PrismDBError::InvalidArgument(
                    "read_json_auto first argument must be a string URL".to_string()
                ));
            }
        };

        println!("Executing read_json_auto('{}')", url);

        // Create file reader
        let file_reader = FileReader::new()?;

        // Get S3 configuration from secrets manager
        let s3_config = self.secrets_manager.get_s3_config(&self.config_manager);

        // Read the file
        let file_data = file_reader.read_file(&url, Some(&s3_config))?;

        // Parse JSON
        let json_reader = JsonReader::new(file_data);
        let chunk = json_reader.read()?;

        // Get column names and types
        let column_names = json_reader.get_column_names()?;
        let column_types = json_reader.get_column_types()?;

        // Build column metadata
        let columns: Vec<ColumnMetadata> = column_names.iter()
            .zip(column_types.iter())
            .map(|(name, data_type)| ColumnMetadata {
                name: name.clone(),
                data_type: data_type.clone(),
            })
            .collect();

        let row_count = chunk.len();

        Ok(QueryResult {
            chunks: vec![chunk],
            row_count,
            columns,
        })
    }

    /// Execute sqlite_scan table function
    fn execute_sqlite_scan(&self, arguments: &[Expression]) -> PrismDBResult<QueryResult> {
        // Extract the URL and table name arguments
        if arguments.len() < 2 {
            return Err(PrismDBError::InvalidArgument(
                "sqlite_scan requires two arguments (file URL, table name)".to_string()
            ));
        }

        let url = match &arguments[0] {
            Expression::Literal(crate::parser::LiteralValue::String(s)) => s.clone(),
            _ => {
                return Err(PrismDBError::InvalidArgument(
                    "sqlite_scan first argument must be a string URL".to_string()
                ));
            }
        };

        let table_name = match &arguments[1] {
            Expression::Literal(crate::parser::LiteralValue::String(s)) => s.clone(),
            _ => {
                return Err(PrismDBError::InvalidArgument(
                    "sqlite_scan second argument must be a string table name".to_string()
                ));
            }
        };

        println!("Executing sqlite_scan('{}', '{}')", url, table_name);

        // Create file reader
        let file_reader = FileReader::new()?;

        // Get S3 configuration from secrets manager
        let s3_config = self.secrets_manager.get_s3_config(&self.config_manager);

        // Read the file
        let file_data = file_reader.read_file(&url, Some(&s3_config))?;

        // Create SQLite reader and read the table
        let sqlite_reader = SqliteReader::new(file_data);
        let chunk = sqlite_reader.read_table(&table_name)?;

        // Get column names and types
        let column_names = sqlite_reader.get_column_names(&table_name)?;
        let column_types = sqlite_reader.get_column_types(&table_name)?;

        // Build column metadata
        let columns: Vec<ColumnMetadata> = column_names.iter()
            .zip(column_types.iter())
            .map(|(name, data_type)| ColumnMetadata {
                name: name.clone(),
                data_type: data_type.clone(),
            })
            .collect();

        let row_count = chunk.len();

        Ok(QueryResult {
            chunks: vec![chunk],
            row_count,
            columns,
        })
    }

    /// Get the catalog (for testing/debugging)
    pub fn catalog(&self) -> Arc<RwLock<Catalog>> {
        self.catalog.clone()
    }

    /// Execute a SQL statement (convenience wrapper)
    pub fn execute(&mut self, sql: &str) -> PrismDBResult<QueryResult> {
        self.execute_sql_collect(sql)
    }

    /// Execute a query and return results (convenience wrapper)
    pub fn query(&self, sql: &str) -> PrismDBResult<QueryResult> {
        self.execute_sql_collect(sql)
    }

    /// Create a new database with configuration
    pub fn new(config: DatabaseConfig) -> PrismDBResult<Self> {
        if let Some(ref file_path) = config.file_path {
            // Create file-based database
            Self::open(file_path)
        } else {
            // Create in-memory database
            Self::new_in_memory()
        }
    }
}

/// Column metadata
#[derive(Debug, Clone)]
pub struct ColumnMetadata {
    pub name: String,
    pub data_type: LogicalType,
}

/// Query result containing data chunks
#[derive(Debug)]
pub struct QueryResult {
    /// Data chunks containing the results
    chunks: Vec<DataChunk>,
    /// Total number of rows
    row_count: usize,
    /// Column metadata
    pub columns: Vec<ColumnMetadata>,
}

impl QueryResult {
    /// Create an empty result
    pub fn empty() -> Self {
        QueryResult {
            chunks: Vec::new(),
            row_count: 0,
            columns: Vec::new(),
        }
    }

    /// Get the number of rows in the result
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// Get all chunks
    pub fn chunks(&self) -> &[DataChunk] {
        &self.chunks
    }

    /// Iterator over data chunks
    pub fn iter(&self) -> impl Iterator<Item = &DataChunk> + '_ {
        self.chunks.iter()
    }

    /// Get the first value from the result
    pub fn first_value(&self) -> Option<Value> {
        self.chunks.first()
            .and_then(|chunk| chunk.get_vector(0))
            .and_then(|vector| vector.get_value(0).ok())
    }

    /// Get the number of columns in the result
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Check if the result is empty (no rows)
    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    /// Collect all chunks into rows format
    pub fn collect(&self) -> PrismDBResult<CollectedResult> {
        let mut all_rows = Vec::new();

        for chunk in &self.chunks {
            for row_idx in 0..chunk.count() {
                let mut row = Vec::new();
                for col_idx in 0..chunk.column_count() {
                    let vector = chunk.get_vector(col_idx).ok_or_else(|| {
                        PrismDBError::InvalidValue(format!("Column {} not found", col_idx))
                    })?;
                    let value = vector.get_value(row_idx)?;
                    row.push(value);
                }
                all_rows.push(row);
            }
        }

        Ok(CollectedResult {
            rows: all_rows,
            stats: ExecutionStats {
                rows_processed: self.row_count,
                execution_time_ms: 0,
                memory_used_bytes: 0,
                operators_executed: 0,
            },
        })
    }

    /// Convert result to a formatted table string
    pub fn to_table_string(&self) -> String {
        if self.chunks.is_empty() {
            return String::new();
        }

        let mut output = String::new();

        // Get column information from first chunk
        let first_chunk = &self.chunks[0];
        let column_count = first_chunk.column_count();

        if column_count == 0 {
            return output;
        }

        // Get column names and types from metadata
        let mut column_names = Vec::new();
        let mut column_types = Vec::new();

        for i in 0..column_count {
            // Use actual column names from metadata if available
            if i < self.columns.len() {
                column_names.push(self.columns[i].name.clone());
                column_types.push(self.columns[i].data_type.clone());
            } else {
                // Fallback to default names if metadata is missing
                column_names.push(format!("column_{}", i));
                if let Some(vector) = first_chunk.get_vector(i) {
                    column_types.push(vector.get_type().clone());
                } else {
                    column_types.push(LogicalType::Varchar);
                }
            }
        }

        // Calculate column widths (accounting for both name and type)
        let mut column_widths: Vec<usize> = column_names.iter()
            .zip(&column_types)
            .map(|(name, col_type)| {
                let type_name = format_type_name(col_type);
                name.len().max(type_name.len())
            })
            .collect();

        // Check data widths
        for chunk in &self.chunks {
            for row_idx in 0..chunk.len() {
                for col_idx in 0..column_count {
                    if let Some(vector) = chunk.get_vector(col_idx) {
                        if let Ok(value) = vector.get_value(row_idx) {
                            let str_len = format_value(&value).len();
                            column_widths[col_idx] = column_widths[col_idx].max(str_len);
                        }
                    }
                }
            }
        }

        // Print header
        output.push('┌');
        for (i, width) in column_widths.iter().enumerate() {
            output.push_str(&"─".repeat(width + 2));
            if i < column_widths.len() - 1 {
                output.push('┬');
            }
        }
        output.push_str("┐\n");

        // Print column names
        output.push('│');
        for (i, (name, width)) in column_names.iter().zip(&column_widths).enumerate() {
            output.push_str(&format!(" {:width$} ", name, width = width));
            if i < column_names.len() - 1 {
                output.push('│');
            }
        }
        output.push_str("│\n");

        // Print column types
        output.push('│');
        for (i, (col_type, width)) in column_types.iter().zip(&column_widths).enumerate() {
            let type_name = format_type_name(col_type);
            output.push_str(&format!(" {:width$} ", type_name, width = width));
            if i < column_types.len() - 1 {
                output.push('│');
            }
        }
        output.push_str("│\n");

        // Print separator
        output.push('├');
        for (i, width) in column_widths.iter().enumerate() {
            output.push_str(&"─".repeat(width + 2));
            if i < column_widths.len() - 1 {
                output.push('┼');
            }
        }
        output.push_str("┤\n");

        // Print rows
        for chunk in &self.chunks {
            for row_idx in 0..chunk.len() {
                output.push('│');
                for col_idx in 0..column_count {
                    let value_str = if let Some(vector) = chunk.get_vector(col_idx) {
                        if let Ok(value) = vector.get_value(row_idx) {
                            format_value(&value)
                        } else {
                            "NULL".to_string()
                        }
                    } else {
                        "NULL".to_string()
                    };

                    output.push_str(&format!(
                        " {:width$} ",
                        value_str,
                        width = column_widths[col_idx]
                    ));
                    if col_idx < column_count - 1 {
                        output.push('│');
                    }
                }
                output.push_str("│\n");
            }
        }

        // Print footer
        output.push('└');
        for (i, width) in column_widths.iter().enumerate() {
            output.push_str(&"─".repeat(width + 2));
            if i < column_widths.len() - 1 {
                output.push('┴');
            }
        }
        output.push_str("┘\n");

        output
    }
}

/// Format a type name for display
fn format_type_name(data_type: &LogicalType) -> String {
    match data_type {
        LogicalType::Boolean => "bool".to_string(),
        LogicalType::TinyInt => "int8".to_string(),
        LogicalType::SmallInt => "int16".to_string(),
        LogicalType::Integer => "int32".to_string(),
        LogicalType::BigInt => "int64".to_string(),
        LogicalType::HugeInt => "int128".to_string(),
        LogicalType::Float => "float32".to_string(),
        LogicalType::Double => "float64".to_string(),
        LogicalType::Varchar => "varchar".to_string(),
        LogicalType::Text => "text".to_string(),
        LogicalType::Char { .. } => "char".to_string(),
        LogicalType::Date => "date".to_string(),
        LogicalType::Time => "time".to_string(),
        LogicalType::Timestamp => "timestamp".to_string(),
        LogicalType::Interval => "interval".to_string(),
        LogicalType::Decimal { .. } => "decimal".to_string(),
        LogicalType::UUID => "uuid".to_string(),
        LogicalType::JSON => "json".to_string(),
        LogicalType::Blob => "blob".to_string(),
        LogicalType::List(_) => "list".to_string(),
        LogicalType::Struct(_) => "struct".to_string(),
        LogicalType::Map { .. } => "map".to_string(),
        LogicalType::Union(_) => "union".to_string(),
        LogicalType::Enum { .. } => "enum".to_string(),
        LogicalType::Null => "null".to_string(),
        LogicalType::Invalid => "invalid".to_string(),
    }
}

/// Format a value for display
fn format_value(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Boolean(b) => b.to_string(),
        Value::TinyInt(i) => i.to_string(),
        Value::SmallInt(i) => i.to_string(),
        Value::Integer(i) => i.to_string(),
        Value::BigInt(i) => i.to_string(),
        Value::HugeInt { high, low } => format!("{}{}", high, low),
        Value::Float(f) => f.to_string(),
        Value::Double(d) => d.to_string(),
        Value::Varchar(s) => s.clone(),
        Value::Char(s) => s.clone(),
        Value::Date(d) => format!("DATE({})", d),
        Value::Time(t) => format!("TIME({})", t),
        Value::Timestamp(ts) => format!("TIMESTAMP({})", ts),
        Value::Interval {
            months,
            days,
            micros,
        } => format!("INTERVAL {} months {} days {} micros", months, days, micros),
        Value::Decimal { value, scale, .. } => {
            let divisor = 10_i128.pow(*scale as u32);
            let integer_part = value / divisor;
            let fractional_part = (value % divisor).abs();
            format!(
                "{}.{:0width$}",
                integer_part,
                fractional_part,
                width = *scale as usize
            )
        }
        Value::UUID { high, low } => format!("UUID({:016x}{:016x})", high, low),
        Value::JSON(s) => s.clone(),
        Value::Blob(b) => format!("<blob {} bytes>", b.len()),
        Value::List(items) => {
            let formatted: Vec<String> = items.iter().map(format_value).collect();
            format!("[{}]", formatted.join(", "))
        }
        Value::Struct(fields) => {
            let formatted: Vec<String> = fields
                .iter()
                .map(|(k, v)| format!("{}: {}", k, format_value(v)))
                .collect();
            format!("{{{}}}", formatted.join(", "))
        }
        Value::Map(pairs) => {
            let formatted: Vec<String> = pairs
                .iter()
                .map(|(k, v)| format!("{}: {}", format_value(k), format_value(v)))
                .collect();
            format!("{{{}}}", formatted.join(", "))
        }
        Value::Union { tag, value } => format!("UNION[{}]: {}", tag, format_value(value)),
    }
}

/// Database configuration
#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    /// Database file path (None for in-memory)
    pub file_path: Option<String>,
    /// Maximum memory for buffer pool
    pub max_memory: usize,
    /// Number of threads for parallel execution
    pub threads: usize,
    /// Enable query optimization
    pub enable_optimizer: bool,
    /// Enable write-ahead logging
    pub enable_wal: bool,
}

impl DatabaseConfig {
    /// Create a configuration for an in-memory database
    pub fn in_memory() -> Self {
        Self {
            file_path: None,
            ..Default::default()
        }
    }

    /// Create a configuration for a file-based database
    pub fn from_file(file_path: String) -> Self {
        Self {
            file_path: Some(file_path),
            ..Default::default()
        }
    }
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        DatabaseConfig {
            file_path: None,
            max_memory: 1024 * 1024 * 1024, // 1GB
            threads: num_cpus::get(),
            enable_optimizer: true,
            enable_wal: true,
        }
    }
}
