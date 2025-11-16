# PrismDB Python Bindings

Python bindings for PrismDB - A high-performance analytical database written in Rust.

## Installation

### From Source

```bash
# Install maturin (build tool for Rust-based Python packages)
pip install maturin

# Build and install in development mode
maturin develop --features python

# Or build a wheel for distribution
maturin build --release --features python
```

### From PyPI (once published)

```bash
pip install prismdb
```

## Quick Start

```python
import prismdb

# Create an in-memory database
db = prismdb.connect()

# Create a table
db.execute("CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER)")

# Insert data
db.execute("INSERT INTO users VALUES (1, 'Alice', 30)")
db.execute("INSERT INTO users VALUES (2, 'Bob', 25)")

# Query data
result = db.execute("SELECT * FROM users WHERE age > 25")
for row in result:
    print(row)  # [1, 'Alice', 30]

# Close connection
db.close()
```

## Features

- **In-Memory & File-Based**: Create databases in memory or persist to disk
- **SQL Support**: Full SQL query support including JOINs, aggregates, window functions
- **DB-API 2.0 Compatible**: Familiar cursor-based API
- **High Performance**: Columnar storage with vectorized execution
- **Type Safety**: Strong type system with Python native type conversion

## Usage Examples

### File-Based Database

```python
import prismdb

# Create or open a file-based database
db = prismdb.connect('mydata.db')

db.execute("CREATE TABLE products (id INTEGER, name VARCHAR, price DOUBLE)")
db.execute("INSERT INTO products VALUES (1, 'Laptop', 999.99)")

db.close()

# Data persists - reopen later
db = prismdb.connect('mydata.db')
result = db.execute("SELECT * FROM products")
```

### Using Cursors

```python
db = prismdb.connect()
cursor = db.cursor()

cursor.execute("SELECT * FROM users")

# Fetch one row
row = cursor.fetchone()
print(row)

# Fetch multiple rows
rows = cursor.fetchmany(10)

# Fetch all remaining rows
all_rows = cursor.fetchall()

cursor.close()
```

### Context Manager

```python
# Automatic cleanup with context manager
with prismdb.connect() as db:
    db.execute("CREATE TABLE temp (value INTEGER)")
    db.execute("INSERT INTO temp VALUES (42)")
    result = db.execute("SELECT * FROM temp")
# Database is automatically closed
```

### Dictionary Conversion

```python
db = prismdb.connect()

db.execute("CREATE TABLE sales (region VARCHAR, amount DOUBLE)")
db.execute("INSERT INTO sales VALUES ('North', 1500)")
db.execute("INSERT INTO sales VALUES ('South', 2000)")

# Convert to dictionary with column names as keys
data = db.to_dict("SELECT * FROM sales")
print(data)
# {'region': ['North', 'South'], 'amount': [1500.0, 2000.0]}
```

### Aggregate Queries

```python
db = prismdb.connect()

db.execute("""
    CREATE TABLE orders (
        customer_id INTEGER,
        order_date DATE,
        amount DOUBLE
    )
""")

result = db.execute("""
    SELECT
        customer_id,
        COUNT(*) as num_orders,
        SUM(amount) as total_spent,
        AVG(amount) as avg_order
    FROM orders
    GROUP BY customer_id
    HAVING SUM(amount) > 1000
    ORDER BY total_spent DESC
""")
```

### String Functions

```python
result = db.execute("""
    SELECT
        UPPER('hello') as uppercase,
        LOWER('WORLD') as lowercase,
        TRIM('  text  ') as trimmed,
        REVERSE('prism') as reversed,
        LEFT('database', 4) as first_four,
        RIGHT('database', 4) as last_four
""")
```

### Iterator Protocol

```python
result = db.execute("SELECT * FROM large_table")

# Iterate over rows efficiently
for row in result:
    process(row)
```

## API Reference

### Connection Object

**`prismdb.connect(path=None)`**

Create a database connection.

- `path` (str, optional): Path to database file. If None, creates in-memory database.
- Returns: `Connection` object

**`Connection.execute(sql)`**

Execute a SQL query and return results.

- `sql` (str): SQL query to execute
- Returns: `QueryResult` object

**`Connection.cursor()`**

Create a cursor for executing queries.

- Returns: `Cursor` object

**`Connection.to_dict(sql)`**

Execute query and return results as a dictionary.

- `sql` (str): SQL query to execute
- Returns: dict with column names as keys

**`Connection.close()`**

Close the database connection.

### Cursor Object

**`Cursor.execute(sql, parameters=None)`**

Execute a SQL query.

- `sql` (str): SQL query
- `parameters` (tuple, optional): Query parameters (not yet implemented)

**`Cursor.fetchone()`**

Fetch the next row from the result set.

- Returns: list or None

**`Cursor.fetchmany(size=None)`**

Fetch multiple rows.

- `size` (int, optional): Number of rows to fetch
- Returns: list of rows

**`Cursor.fetchall()`**

Fetch all remaining rows.

- Returns: list of rows

**`Cursor.description`**

Get column descriptions (DB-API 2.0 compatible).

- Returns: list of tuples (name, type_code, ...)

**`Cursor.rowcount`**

Get number of rows affected/returned.

- Returns: int

**`Cursor.close()`**

Close the cursor.

### QueryResult Object

**`QueryResult.fetchone()`**

Fetch next row.

- Returns: list or None

**`QueryResult.fetchall()`**

Fetch all rows.

- Returns: list of rows

**`QueryResult.to_dict()`**

Convert to dictionary.

- Returns: dict

**`QueryResult.description`**

Column descriptions.

- Returns: list of tuples

**`QueryResult.rowcount`**

Number of rows.

- Returns: int

## Type Mapping

| PrismDB Type | Python Type |
|--------------|-------------|
| INTEGER      | int         |
| BIGINT       | int         |
| DOUBLE       | float       |
| DECIMAL      | float       |
| VARCHAR      | str         |
| TEXT         | str         |
| BOOLEAN      | bool        |
| DATE         | str         |
| TIME         | str         |
| TIMESTAMP    | str         |
| BLOB         | bytes       |
| NULL         | None        |

## Supported SQL Features

- **DDL**: CREATE TABLE, DROP TABLE
- **DML**: INSERT, UPDATE, DELETE
- **Queries**: SELECT with WHERE, GROUP BY, HAVING, ORDER BY, LIMIT
- **Joins**: INNER JOIN, LEFT JOIN, RIGHT JOIN, FULL OUTER JOIN
- **Subqueries**: Correlated and non-correlated
- **CTEs**: WITH clauses (Common Table Expressions)
- **Window Functions**: ROW_NUMBER, RANK, LAG, LEAD, etc.
- **Aggregates**: SUM, AVG, MIN, MAX, COUNT, etc.
- **String Functions**: UPPER, LOWER, TRIM, REVERSE, REPLACE, etc.
- **Math Functions**: ABS, ROUND, CEIL, FLOOR, etc.
- **Date Functions**: DATE_ADD, DATE_DIFF, EXTRACT, etc.

## Performance Tips

1. **Batch Inserts**: Use multiple VALUES clauses for better performance
   ```python
   db.execute("INSERT INTO table VALUES (1, 'a'), (2, 'b'), (3, 'c')")
   ```

2. **Use File-Based for Large Datasets**: In-memory is fast but limited by RAM
   ```python
   db = prismdb.connect('large_data.db')
   ```

3. **Leverage Columnar Storage**: PrismDB excels at analytical queries
   ```python
   # This is very fast
   db.execute("SELECT SUM(amount) FROM large_table WHERE category = 'X'")
   ```

4. **Create Appropriate Indexes**: For frequently queried columns (coming soon)

## Examples

See the `python_examples/` directory for complete working examples:

- `basic_usage.py` - Comprehensive usage examples
- `test_basic.py` - Test suite demonstrating all features

Run examples:

```bash
python python_examples/basic_usage.py
python python_examples/test_basic.py
```

## Requirements

- Python 3.8+
- Rust 1.70+ (for building from source)

## Building

```bash
# Development build
maturin develop --features python

# Release build
maturin build --release --features python

# With specific Python version
maturin build --release --features python --interpreter python3.11
```

## Testing

```bash
# Run Python tests
python python_examples/test_basic.py

# Run Rust tests
cargo test --features python
```

## Limitations

Current limitations (will be addressed in future releases):

- Parameterized queries not yet supported
- Date/Time types return strings (native Python datetime coming soon)
- No transaction control API (auto-commit mode only)
- No prepared statements (coming soon)

## Contributing

Contributions are welcome! Please see the main [README.md](README.md) for contribution guidelines.

## License

PrismDB is licensed under the MIT License. See [LICENSE](LICENSE) for details.

## Acknowledgments

PrismDB Python bindings are built with [PyO3](https://pyo3.rs/), the Rust bindings for Python.

## Support

- GitHub Issues: https://github.com/TuringWorks/PrismDB/issues
- Documentation: https://github.com/TuringWorks/PrismDB

## Changelog

### 0.1.0 (Initial Release)

- Core database functionality
- DB-API 2.0 compatible interface
- SQL query support
- In-memory and file-based databases
- String, math, and aggregate functions
- Context manager support
- Iterator protocol support
