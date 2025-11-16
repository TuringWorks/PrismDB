//! PrismDB - Main Entry Point
//!
//! This is the main entry point for the PrismDB CLI application.

use clap::Parser;
use rustyline::completion::Completer;
use rustyline::error::ReadlineError;
use rustyline::highlight::{Highlighter, CmdKind};
use rustyline::hint::Hinter;
use rustyline::validate::Validator;
use rustyline::{Context, Editor, Helper};
use std::borrow::Cow;
use std::process;

use prism::Database;

/// SQL Syntax Highlighter for interactive mode
struct SqlHighlighter;

impl SqlHighlighter {
    fn is_keyword(word: &str) -> bool {
        matches!(
            word.to_uppercase().as_str(),
            "SELECT" | "FROM" | "WHERE" | "INSERT" | "UPDATE" | "DELETE" | "CREATE" | "DROP" |
            "TABLE" | "INDEX" | "VIEW" | "AS" | "JOIN" | "INNER" | "LEFT" | "RIGHT" | "OUTER" |
            "ON" | "AND" | "OR" | "NOT" | "IN" | "EXISTS" | "BETWEEN" | "LIKE" | "IS" | "NULL" |
            "ORDER" | "BY" | "GROUP" | "HAVING" | "LIMIT" | "OFFSET" | "UNION" | "INTERSECT" |
            "EXCEPT" | "WITH" | "DISTINCT" | "ALL" | "VALUES" | "SET" | "INTO" | "PRIMARY" |
            "KEY" | "FOREIGN" | "REFERENCES" | "UNIQUE" | "CHECK" | "DEFAULT" | "INTEGER" |
            "VARCHAR" | "TEXT" | "BOOLEAN" | "FLOAT" | "DOUBLE" | "DECIMAL" | "DATE" | "TIME" |
            "TIMESTAMP" | "BIGINT" | "SMALLINT" | "TINYINT" | "BLOB" | "JSON" | "EXPLAIN" |
            "ANALYZE" | "BEGIN" | "COMMIT" | "ROLLBACK" | "TRANSACTION" | "CASE" | "WHEN" |
            "THEN" | "ELSE" | "END" | "CAST" | "TRUE" | "FALSE" | "ASC" | "DESC" | "NULLS" |
            "FIRST" | "LAST" | "OVER" | "PARTITION" | "WINDOW" | "ROWS" | "RANGE" | "UNBOUNDED" |
            "PRECEDING" | "FOLLOWING" | "CURRENT" | "ROW"
        )
    }
}

impl Highlighter for SqlHighlighter {
    fn highlight<'l>(&self, line: &'l str, _pos: usize) -> Cow<'l, str> {
        let mut highlighted = String::new();
        let mut chars = line.chars().peekable();
        let mut current_word = String::new();
        let mut in_string = false;
        let mut string_char = ' ';

        // ANSI color codes
        const KEYWORD: &str = "\x1b[1;34m";  // Bold blue for keywords
        const STRING: &str = "\x1b[32m";      // Green for strings
        const NUMBER: &str = "\x1b[33m";      // Yellow for numbers
        const COMMENT: &str = "\x1b[90m";     // Gray for comments
        const RESET: &str = "\x1b[0m";        // Reset

        while let Some(ch) = chars.next() {
            // Handle strings
            if (ch == '\'' || ch == '"') && !in_string {
                if !current_word.is_empty() {
                    if Self::is_keyword(&current_word) {
                        highlighted.push_str(KEYWORD);
                        highlighted.push_str(&current_word);
                        highlighted.push_str(RESET);
                    } else if current_word.chars().all(|c| c.is_numeric() || c == '.') {
                        highlighted.push_str(NUMBER);
                        highlighted.push_str(&current_word);
                        highlighted.push_str(RESET);
                    } else {
                        highlighted.push_str(&current_word);
                    }
                    current_word.clear();
                }
                in_string = true;
                string_char = ch;
                highlighted.push_str(STRING);
                highlighted.push(ch);
            } else if in_string {
                highlighted.push(ch);
                if ch == string_char {
                    in_string = false;
                    highlighted.push_str(RESET);
                }
            }
            // Handle comments
            else if ch == '-' && chars.peek() == Some(&'-') {
                if !current_word.is_empty() {
                    if Self::is_keyword(&current_word) {
                        highlighted.push_str(KEYWORD);
                        highlighted.push_str(&current_word);
                        highlighted.push_str(RESET);
                    } else {
                        highlighted.push_str(&current_word);
                    }
                    current_word.clear();
                }
                highlighted.push_str(COMMENT);
                highlighted.push(ch);
                // Add rest of line as comment
                highlighted.extend(chars.by_ref());
                highlighted.push_str(RESET);
                break;
            }
            // Handle word boundaries
            else if ch.is_alphanumeric() || ch == '_' {
                current_word.push(ch);
            } else {
                if !current_word.is_empty() {
                    if Self::is_keyword(&current_word) {
                        highlighted.push_str(KEYWORD);
                        highlighted.push_str(&current_word);
                        highlighted.push_str(RESET);
                    } else if current_word.chars().all(|c| c.is_numeric() || c == '.') {
                        highlighted.push_str(NUMBER);
                        highlighted.push_str(&current_word);
                        highlighted.push_str(RESET);
                    } else {
                        highlighted.push_str(&current_word);
                    }
                    current_word.clear();
                }
                highlighted.push(ch);
            }
        }

        // Handle any remaining word
        if !current_word.is_empty() {
            if Self::is_keyword(&current_word) {
                highlighted.push_str(KEYWORD);
                highlighted.push_str(&current_word);
                highlighted.push_str(RESET);
            } else if current_word.chars().all(|c| c.is_numeric() || c == '.') {
                highlighted.push_str(NUMBER);
                highlighted.push_str(&current_word);
                highlighted.push_str(RESET);
            } else {
                highlighted.push_str(&current_word);
            }
        }

        Cow::Owned(highlighted)
    }

    fn highlight_char(&self, _line: &str, _pos: usize, _kind: CmdKind) -> bool {
        true
    }
}

impl Hinter for SqlHighlighter {
    type Hint = String;

    fn hint(&self, _line: &str, _pos: usize, _ctx: &Context<'_>) -> Option<String> {
        None
    }
}

impl Completer for SqlHighlighter {
    type Candidate = String;

    fn complete(
        &self,
        _line: &str,
        _pos: usize,
        _ctx: &Context<'_>,
    ) -> rustyline::Result<(usize, Vec<String>)> {
        Ok((0, vec![]))
    }
}

impl Validator for SqlHighlighter {}

impl Helper for SqlHighlighter {}

fn run_interactive_mode(database: &Database) -> Result<(), Box<dyn std::error::Error>> {
    println!("PrismDB v{}", env!("CARGO_PKG_VERSION"));
    println!("Enter '.help' for usage hints.");
    println!("Enter SQL statements terminated with a semicolon (;)");
    println!();

    let mut settings = Settings::default();
    let mut rl = Editor::new()?;
    rl.set_helper(Some(SqlHighlighter));

    let history_file = dirs::home_dir()
        .map(|mut path| {
            path.push(".prismdb_history");
            path
        })
        .unwrap_or_else(|| std::path::PathBuf::from(".prismdb_history"));

    // Load history if it exists
    let _ = rl.load_history(&history_file);

    let mut sql_buffer = String::new();

    loop {
        let prompt = if sql_buffer.is_empty() {
            "P> "
        } else {
            "  -> "
        };

        match rl.readline(prompt) {
            Ok(line) => {
                let trimmed = line.trim();

                // Handle special commands
                if sql_buffer.is_empty() && trimmed.starts_with('.') {
                    let _ = rl.add_history_entry(trimmed);
                    match handle_special_command(trimmed, database, &mut settings) {
                        Ok(should_exit) => {
                            if should_exit {
                                break;
                            }
                        }
                        Err(e) => eprintln!("Error: {}", e),
                    }
                    continue;
                }

                // Handle empty lines
                if trimmed.is_empty() {
                    continue;
                }

                // Accumulate SQL statement
                if !sql_buffer.is_empty() {
                    sql_buffer.push(' ');
                }
                sql_buffer.push_str(trimmed);

                // Check if statement is complete (ends with semicolon)
                if trimmed.ends_with(';') {
                    let _ = rl.add_history_entry(&sql_buffer);

                    // Execute the SQL statement
                    match execute_sql(database, &sql_buffer, &settings) {
                        Ok(()) => {}
                        Err(e) => eprintln!("Error: {}", e),
                    }

                    // Clear buffer for next statement
                    sql_buffer.clear();
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("^C");
                sql_buffer.clear();
            }
            Err(ReadlineError::Eof) => {
                println!("exit");
                break;
            }
            Err(err) => {
                eprintln!("Error: {:?}", err);
                break;
            }
        }
    }

    // Save history
    let _ = rl.save_history(&history_file);

    Ok(())
}

struct Settings {
    mode: OutputMode,
    headers: bool,
    timer: bool,
}

#[derive(Debug, Clone, Copy)]
enum OutputMode {
    Table,
    List,
    Csv,
    Line,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            mode: OutputMode::Table,
            headers: true,
            timer: true,
        }
    }
}

fn handle_special_command(
    command: &str,
    database: &Database,
    settings: &mut Settings,
) -> Result<bool, Box<dyn std::error::Error>> {
    let parts: Vec<&str> = command.split_whitespace().collect();
    let cmd = parts.first().unwrap_or(&"");

    match *cmd {
        ".exit" | ".quit" => {
            println!("Goodbye!");
            Ok(true) // Signal to exit
        }
        ".help" => {
            print_help();
            Ok(false)
        }
        ".tables" => {
            show_tables(database);
            Ok(false)
        }
        ".schema" => {
            let table_name = parts.get(1).copied();
            show_schema(database, table_name);
            Ok(false)
        }
        ".databases" => {
            show_databases(database);
            Ok(false)
        }
        ".version" => {
            println!("PrismDB v{}", env!("CARGO_PKG_VERSION"));
            Ok(false)
        }
        ".mode" => {
            if let Some(mode_str) = parts.get(1) {
                set_mode(settings, mode_str);
            } else {
                println!("Current mode: {:?}", settings.mode);
            }
            Ok(false)
        }
        ".headers" => {
            if let Some(setting) = parts.get(1) {
                settings.headers = setting.to_lowercase() == "on";
            }
            println!("Headers: {}", if settings.headers { "on" } else { "off" });
            Ok(false)
        }
        ".timer" => {
            if let Some(setting) = parts.get(1) {
                settings.timer = setting.to_lowercase() == "on";
            }
            println!("Timer: {}", if settings.timer { "on" } else { "off" });
            Ok(false)
        }
        ".show" => {
            show_settings(settings);
            Ok(false)
        }
        ".dump" => {
            dump_database(database, parts.get(1).copied());
            Ok(false)
        }
        _ => {
            println!("Unknown command: {}", cmd);
            println!("Type '.help' for list of available commands.");
            Ok(false)
        }
    }
}

fn show_tables(database: &Database) {
    let catalog = database.catalog();
    let catalog_lock = catalog.read().unwrap();

    match catalog_lock.list_tables("main") {
        Ok(tables) => {
            if tables.is_empty() {
                println!("No tables found");
            } else {
                for table in tables {
                    println!("{}", table);
                }
            }
        }
        Err(e) => eprintln!("Error listing tables: {}", e),
    }
}

fn show_schema(database: &Database, table_name: Option<&str>) {
    let catalog = database.catalog();
    let catalog_lock = catalog.read().unwrap();

    let tables = match catalog_lock.list_tables("main") {
        Ok(t) => t,
        Err(e) => {
            eprintln!("Error listing tables: {}", e);
            return;
        }
    };

    for table in tables {
        if let Some(filter) = table_name {
            if table != filter {
                continue;
            }
        }

        match catalog_lock.get_table("main", &table) {
            Ok(table_ref) => {
                let table_lock = table_ref.read().unwrap();
                let table_info = table_lock.get_table_info();
                println!("CREATE TABLE {} (", table);
                let columns: Vec<String> = table_info
                    .columns
                    .iter()
                    .map(|col| format!("  {} {}", col.name, format_type(&col.column_type)))
                    .collect();
                println!("{}", columns.join(",\n"));
                println!(");");
            }
            Err(e) => eprintln!("Error getting table '{}': {}", table, e),
        }
    }
}

fn show_databases(database: &Database) {
    if database.is_file_based() {
        if let Some(path) = database.get_file_path() {
            println!("main: {}", path.display());
            return;
        }
    }
    println!("main: :memory:");
}

fn set_mode(settings: &mut Settings, mode_str: &str) {
    settings.mode = match mode_str.to_lowercase().as_str() {
        "table" => OutputMode::Table,
        "list" => OutputMode::List,
        "csv" => OutputMode::Csv,
        "line" => OutputMode::Line,
        _ => {
            eprintln!("Invalid mode. Use: table, list, csv, or line");
            return;
        }
    };
    println!("Mode set to: {}", mode_str);
}

fn show_settings(settings: &Settings) {
    println!("     mode: {:?}", settings.mode);
    println!("  headers: {}", if settings.headers { "on" } else { "off" });
    println!("    timer: {}", if settings.timer { "on" } else { "off" });
}

fn dump_database(database: &Database, table_name: Option<&str>) {
    let catalog = database.catalog();
    let catalog_lock = catalog.read().unwrap();

    let tables = match catalog_lock.list_tables("main") {
        Ok(t) => t,
        Err(e) => {
            eprintln!("Error listing tables: {}", e);
            return;
        }
    };

    for table in tables {
        if let Some(filter) = table_name {
            if table != filter {
                continue;
            }
        }

        // Show CREATE statement
        show_schema(database, Some(&table));
        println!();

        // Show INSERT statements
        if let Ok(result) = database.query(&format!("SELECT * FROM {}", table)) {
            if let Ok(collected) = result.collect() {
                for row in &collected.rows {
                    let values: Vec<String> = row.iter()
                        .map(|v| format_value_sql(v))
                        .collect();
                    println!("INSERT INTO {} VALUES ({});", table, values.join(", "));
                }
            }
        }
        println!();
    }
}

fn format_type(ty: &prism::LogicalType) -> &str {
    use prism::LogicalType;
    match ty {
        LogicalType::Boolean => "BOOLEAN",
        LogicalType::TinyInt => "TINYINT",
        LogicalType::SmallInt => "SMALLINT",
        LogicalType::Integer => "INTEGER",
        LogicalType::BigInt => "BIGINT",
        LogicalType::Float => "FLOAT",
        LogicalType::Double => "DOUBLE",
        LogicalType::Varchar => "VARCHAR",
        LogicalType::Date => "DATE",
        LogicalType::Timestamp => "TIMESTAMP",
        _ => "UNKNOWN",
    }
}

fn format_value_sql(value: &prism::Value) -> String {
    use prism::Value;
    match value {
        Value::Null => "NULL".to_string(),
        Value::Varchar(s) | Value::Char(s) => format!("'{}'", s.replace("'", "''")),
        Value::Integer(i) => i.to_string(),
        Value::BigInt(i) => i.to_string(),
        Value::Boolean(b) => b.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Double(d) => d.to_string(),
        _ => format!("{:?}", value),
    }
}

fn print_help() {
    println!(r#"
.help                    Show this help message
.quit                    Exit this program
.exit                    Exit this program
.tables                  List all tables
.schema ?TABLE?          Show the CREATE statements (all tables or specific table)
.mode MODE               Set output mode (table, list, csv, line)
.headers on|off          Turn display of headers on or off
.timer on|off            Turn SQL timer on or off (default: on)
.databases               List database file path
.open FILE               Close current database and open FILE
.show                    Show current settings
.dump ?TABLE?            Dump database as SQL statements
.version                 Show version information

Output modes:
  table     - ASCII table (default)
  list      - Values delimited by "|"
  csv       - Comma-separated values
  line      - One value per line

SQL Statements:
  Type SQL statements terminated with a semicolon (;)
  Multi-line statements are supported

Keyboard Shortcuts:
  Ctrl+C                Cancel current statement
  Ctrl+D                Exit (same as .exit)
  Up/Down arrows        Navigate command history
"#);
}

fn execute_sql(database: &Database, sql: &str, settings: &Settings) -> Result<(), Box<dyn std::error::Error>> {
    let start_time = if settings.timer {
        Some(std::time::Instant::now())
    } else {
        None
    };

    match database.execute_sql_collect(sql) {
        Ok(result) => {
            // Check if this is a DML result (single column, single row with BigInt count)
            // DML operations (INSERT/UPDATE/DELETE) return a count, not actual data
            // They have empty column metadata (no schema) and a single chunk with one column
            let is_dml_result = result.columns.is_empty()
                && result.row_count() == 1
                && result.chunks().len() == 1
                && result.chunks()[0].column_count() == 1;

            // For DML results, extract the actual affected row count
            let affected_rows = if is_dml_result {
                // Get the count value from the result
                if let Some(first_value) = result.first_value() {
                    if let prism::Value::BigInt(count) = first_value {
                        count as usize
                    } else {
                        result.row_count()
                    }
                } else {
                    result.row_count()
                }
            } else {
                result.row_count()
            };

            // Only display the table for non-DML results
            if !is_dml_result && result.row_count() > 0 {
                println!("{}", result.to_table_string());
                println!();
            }

            if settings.timer {
                if let Some(start) = start_time {
                    let elapsed = start.elapsed();
                    println!(
                        "Query executed successfully ({} row{} in {:.3}s)",
                        affected_rows,
                        if affected_rows == 1 { "" } else { "s" },
                        elapsed.as_secs_f64()
                    );
                }
            } else {
                println!(
                    "Query executed successfully ({} row{})",
                    affected_rows,
                    if affected_rows == 1 { "" } else { "s" }
                );
            }

            Ok(())
        }
        Err(e) => Err(Box::new(e)),
    }
}

#[derive(Parser)]
#[command(name = "prism")]
#[command(about = "PrismDB - High Performance Analytical Database")]
#[command(version = env!("CARGO_PKG_VERSION"))]
struct Cli {
    /// Database file path (in-memory if not specified)
    #[arg(short, long)]
    database: Option<String>,

    /// SQL query to execute (non-interactive mode)
    #[arg(short, long)]
    query: Option<String>,

    /// Run in interactive mode (default if no query is provided)
    #[arg(short, long)]
    interactive: bool,

    /// Enable verbose output
    #[arg(short, long)]
    verbose: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    if cli.verbose {
        println!("PrismDB v{}", env!("CARGO_PKG_VERSION"));
    }

    // Initialize database
    let database = if let Some(path) = cli.database {
        Database::open(path)?
    } else {
        Database::new_in_memory()?
    };

    if let Some(query) = cli.query {
        // Execute query
        match database.execute_sql_collect(&query) {
            Ok(result) => {
                println!("Query executed successfully");
                println!("Rows: {}", result.row_count());
                if result.row_count() > 0 {
                    println!("{}", result.to_table_string());
                }
            }
            Err(e) => {
                eprintln!("Error executing query: {}", e);
                process::exit(1);
            }
        }
    } else {
        // Default to interactive mode
        run_interactive_mode(&database)?;
    }

    Ok(())
}
