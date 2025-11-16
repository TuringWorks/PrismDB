# PrismDB

<div align="center">

**A high-performance analytical database system written in Rust**

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org/)

</div>

## About

PrismDB is a columnar analytical database system written entirely in Rust, inspired by the [DuckDB](https://duckdb.org/) architecture. It's designed as an educational project to understand and implement modern database internals while leveraging Rust's safety and performance features.

**Note**: PrismDB is not file-format compatible with DuckDB. It's an independent implementation for learning and exploration purposes.

## Features

### Core Database Features
- ✅ **Columnar Storage**: Efficient column-oriented data storage with compression
- ✅ **SQL Support**: Comprehensive SQL query parsing and execution
- ✅ **ACID Transactions**: Full transaction support with MVCC
- ✅ **Query Optimization**: Cost-based query optimizer with multiple optimization passes
- ✅ **Vectorized Execution**: High-performance vectorized query execution engine

### Data Types
- Integer types (INT, BIGINT, SMALLINT, TINYINT)
- Floating-point types (REAL, DOUBLE, DECIMAL)
- String types (VARCHAR, CHAR, TEXT)
- Date/Time types (DATE, TIME, TIMESTAMP)
- Boolean type
- Nested types (STRUCT, LIST, MAP, ARRAY)

### SQL Features
- SELECT queries with WHERE, GROUP BY, HAVING, ORDER BY
- JOIN operations (INNER, LEFT, RIGHT, FULL, CROSS)
- Subqueries and Common Table Expressions (CTEs)
- Window functions
- Aggregate functions
- String and mathematical functions
- Date/time functions

### File Formats
- CSV (read/write)
- Parquet (read)
- JSON (read)
- SQLite (read)

### Advanced Features
- HTTP/HTTPS file reading
- S3 file reading with AWS Signature V4
- Data compression (LZ4, ZSTD, Snappy)
- Parallel query execution
- Statistics and query profiling

## Quick Start

### Installation

```bash
# Clone the repository
git clone https://github.com/YOUR_USERNAME/prismdb.git
cd prismdb

# Build the project
cargo build --release

# Run tests
cargo test
```

### Usage

#### As a Library

```rust
use prismdb::{Database, DatabaseConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create an in-memory database
    let db = Database::new(DatabaseConfig::in_memory())?;
    
    // Create a table
    db.execute_sql_collect("CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER)")?;
    
    // Insert data
    db.execute_sql_collect("INSERT INTO users VALUES (1, 'Alice', 30)")?;
    db.execute_sql_collect("INSERT INTO users VALUES (2, 'Bob', 25)")?;
    
    // Query data
    let result = db.execute_sql_collect("SELECT * FROM users WHERE age > 26")?;
    println!("{:?}", result);
    
    Ok(())
}
```

#### As a CLI

```bash
# Start interactive shell
./target/release/prism

# Execute a query directly with -q flag
./target/release/prism -q "SELECT * FROM read_csv_auto('data.csv')"

```

## Architecture

PrismDB follows a modular architecture:

- **Parser**: SQL tokenization and parsing
- **Binder**: Semantic analysis and name resolution
- **Planner**: Logical and physical query planning
- **Optimizer**: Query optimization with multiple passes
- **Executor**: Vectorized query execution engine
- **Storage**: Columnar storage with compression
- **Catalog**: Schema and metadata management
- **Transaction**: MVCC-based transaction management

For detailed architecture documentation, see [ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Performance

PrismDB uses several techniques to achieve high performance:

- Vectorized execution with SIMD operations
- Columnar data layout for cache efficiency
- Parallel query execution with work-stealing
- Adaptive compression based on data characteristics
- Zero-copy data access where possible

## Development

### Prerequisites

- Rust 1.70 or later
- Cargo

### Building

```bash
# Debug build
cargo build

# Release build with optimizations
cargo build --release

# Run tests
cargo test

# Run benchmarks
cargo bench
```

### Project Structure

```
prismdb/
├── src/
│   ├── catalog/      # Schema and metadata
│   ├── common/       # Common utilities and errors
│   ├── execution/    # Query execution engine
│   ├── expression/   # Expression evaluation
│   ├── extensions/   # File readers and extensions
│   ├── parser/       # SQL parser
│   ├── planner/      # Query planner and optimizer
│   ├── storage/      # Storage engine
│   └── types/        # Type system
├── tests/            # Integration tests
├── benches/          # Benchmarks
├── examples/         # Example programs
└── docs/             # Documentation
```

## Roadmap

- [ ] More SQL features (WINDOW, PIVOT/UNPIVOT)
- [ ] Additional file formats (ORC, Avro)
- [ ] Query result caching
- [ ] Materialized views
- [ ] User-defined functions (UDFs)
- [ ] Distributed query execution

## Contributing

Contributions are welcome! This is an educational project, so feel free to:

- Report bugs
- Suggest features
- Submit pull requests
- Improve documentation

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

PrismDB is inspired by [DuckDB](https://duckdb.org/), an excellent in-process SQL OLAP database. This project aims to understand and implement similar concepts in Rust while maintaining independence in implementation and design decisions.

## Disclaimer

PrismDB is an educational project and not intended for production use. For production analytical workloads, consider using [DuckDB](https://duckdb.org/) or other mature database systems.

---

**Made with ❤️ and Rust**
