# PrismDB Quick Start Guide

Get started with PrismDB Python bindings in minutes.

## One-Line Install

### macOS/Linux
```bash
curl -sSL https://raw.githubusercontent.com/TuringWorks/PrismDB/master/scripts/install.sh | bash
```

### Windows (PowerShell)
```powershell
iwr https://raw.githubusercontent.com/TuringWorks/PrismDB/master/scripts/install.bat -OutFile install.bat; .\install.bat
```

## Manual Install (3 Steps)

### 1. Install Dependencies
```bash
# macOS/Linux
pip3 install maturin

# Windows
pip install maturin
```

### 2. Clone and Build
```bash
git clone https://github.com/TuringWorks/PrismDB.git
cd PrismDB

# macOS/Linux
./scripts/build.sh

# Windows
scripts\build.bat
```

### 3. Install
```bash
# macOS/Linux
pip3 install target/wheels/prismdb-*.whl

# Windows
pip install target\wheels\prismdb-*.whl
```

## First Query (30 seconds)

```python
import prismdb

# Create database
db = prismdb.connect()

# Create table
db.execute("""
    CREATE TABLE users (
        id INTEGER,
        name VARCHAR,
        email VARCHAR
    )
""")

# Insert data
db.execute("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')")
db.execute("INSERT INTO users VALUES (2, 'Bob', 'bob@example.com')")

# Query
result = db.execute("SELECT * FROM users WHERE id > 0")
for row in result:
    print(row)

# Output:
# [1, 'Alice', 'alice@example.com']
# [2, 'Bob', 'bob@example.com']
```

## Common Use Cases

### Analytical Queries
```python
import prismdb

db = prismdb.connect()

db.execute("""
    CREATE TABLE sales (
        region VARCHAR,
        product VARCHAR,
        amount DOUBLE,
        quantity INTEGER
    )
""")

db.execute("INSERT INTO sales VALUES ('North', 'Laptop', 1500.00, 3)")
db.execute("INSERT INTO sales VALUES ('North', 'Mouse', 45.00, 10)")
db.execute("INSERT INTO sales VALUES ('South', 'Laptop', 2000.00, 4)")

# Aggregate query
result = db.execute("""
    SELECT
        region,
        SUM(amount) as total_revenue,
        AVG(amount) as avg_sale,
        COUNT(*) as num_sales
    FROM sales
    GROUP BY region
    ORDER BY total_revenue DESC
""")

for row in result:
    print(row)
```

### Using Cursor API
```python
import prismdb

db = prismdb.connect()
cursor = db.cursor()

cursor.execute("CREATE TABLE products (id INTEGER, name VARCHAR, price DOUBLE)")
cursor.execute("INSERT INTO products VALUES (1, 'Widget', 9.99)")
cursor.execute("INSERT INTO products VALUES (2, 'Gadget', 19.99)")

cursor.execute("SELECT * FROM products")

# Fetch one row at a time
row = cursor.fetchone()
while row:
    print(row)
    row = cursor.fetchone()

cursor.close()
```

### Dictionary Output
```python
import prismdb

db = prismdb.connect()

db.execute("CREATE TABLE employees (id INTEGER, name VARCHAR, salary DOUBLE)")
db.execute("INSERT INTO employees VALUES (1, 'Alice', 75000.0)")
db.execute("INSERT INTO employees VALUES (2, 'Bob', 65000.0)")

# Get results as dictionary
data = db.to_dict("SELECT * FROM employees ORDER BY id")

print(data)
# {'employees.id': [1, 2],
#  'employees.name': ['Alice', 'Bob'],
#  'employees.salary': [75000.0, 65000.0]}
```

### Context Manager
```python
import prismdb

# Auto-cleanup with context manager
with prismdb.connect() as db:
    db.execute("CREATE TABLE temp (value INTEGER)")
    db.execute("INSERT INTO temp VALUES (42)")

    result = db.execute("SELECT * FROM temp")
    print(list(result))  # [[42]]
# Database automatically closed
```

### File-Based Database
```python
import prismdb

# Create persistent database
db = prismdb.connect('mydata.db')

db.execute("CREATE TABLE persistent (id INTEGER, data VARCHAR)")
db.execute("INSERT INTO persistent VALUES (1, 'saved')")

db.close()

# Reopen later
db = prismdb.connect('mydata.db')
result = db.execute("SELECT * FROM persistent")
print(list(result))  # [[1, 'saved']]
```

## Running Examples

```bash
# Run all examples
python3 python_examples/basic_usage.py

# Run tests
python3 python_examples/test_basic.py
```

## Next Steps

- **Full Documentation:** [README_PYTHON.md](README_PYTHON.md)
- **Installation Guide:** [INSTALL.md](INSTALL.md)
- **Examples:** [python_examples/](python_examples/)
- **SQL Reference:** See main [README.md](README.md)

## Quick Reference

### Connection Methods
- `connect(path=None)` - Create database connection
- `execute(sql)` - Execute query and return results
- `cursor()` - Create cursor for DB-API 2.0 interface
- `to_dict(sql)` - Execute and return as dictionary
- `close()` - Close connection

### Cursor Methods
- `execute(sql)` - Execute query
- `fetchone()` - Fetch next row
- `fetchmany(size)` - Fetch multiple rows
- `fetchall()` - Fetch all rows
- `description` - Column descriptions
- `rowcount` - Number of rows
- `close()` - Close cursor

### QueryResult Methods
- `fetchone()` - Fetch next row
- `fetchall()` - Fetch all rows
- `to_dict()` - Convert to dictionary
- `description` - Column descriptions
- `rowcount` - Number of rows

### Supported SQL

**DDL:** CREATE TABLE, DROP TABLE
**DML:** INSERT, UPDATE, DELETE
**Queries:** SELECT, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT
**Joins:** INNER, LEFT, RIGHT, FULL OUTER
**Advanced:** CTEs (WITH), Subqueries, Window Functions

### Supported Functions

**Aggregates:** SUM, AVG, MIN, MAX, COUNT, STDDEV, VARIANCE
**String:** UPPER, LOWER, TRIM, REVERSE, LEFT, RIGHT, REPLACE
**Math:** ABS, ROUND, CEIL, FLOOR, SQRT, POWER
**Date:** DATE_ADD, DATE_DIFF, EXTRACT

## Getting Help

- **Issues:** https://github.com/TuringWorks/PrismDB/issues
- **Examples:** `python_examples/`
- **Tests:** `python_examples/test_basic.py`

## Performance Tips

1. Use release build for production: `maturin build --release`
2. Batch inserts: `INSERT INTO table VALUES (1,'a'), (2,'b'), (3,'c')`
3. Leverage columnar storage for analytics
4. Use file-based DB for large datasets

## License

MIT License - see [LICENSE](LICENSE)
