"""
Basic PrismDB Python usage examples
"""

import prismdb

def example_basic_queries():
    """Demonstrate basic query execution"""
    print("=== Basic Queries Example ===")

    # Create an in-memory database
    db = prismdb.connect()

    # Create a table
    db.execute("CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER)")
    print("Created users table")

    # Insert data
    db.execute("INSERT INTO users VALUES (1, 'Alice', 30)")
    db.execute("INSERT INTO users VALUES (2, 'Bob', 25)")
    db.execute("INSERT INTO users VALUES (3, 'Charlie', 35)")
    print("Inserted 3 users")

    # Query data
    result = db.execute("SELECT * FROM users")
    print("\nAll users:")
    for row in result:
        print(f"  {row}")

    # Filter query
    result = db.execute("SELECT name, age FROM users WHERE age > 25")
    print("\nUsers older than 25:")
    for row in result:
        print(f"  {row}")

    db.close()
    print("\n✓ Basic queries example completed\n")


def example_cursor_api():
    """Demonstrate cursor-based API"""
    print("=== Cursor API Example ===")

    db = prismdb.connect()

    # Create and populate table
    db.execute("CREATE TABLE products (id INTEGER, name VARCHAR, price DOUBLE)")
    db.execute("INSERT INTO products VALUES (1, 'Laptop', 999.99)")
    db.execute("INSERT INTO products VALUES (2, 'Mouse', 29.99)")
    db.execute("INSERT INTO products VALUES (3, 'Keyboard', 79.99)")

    # Use cursor
    cursor = db.cursor()
    cursor.execute("SELECT * FROM products ORDER BY price DESC")

    # Fetch one row at a time
    print("\nFetching one row at a time:")
    row = cursor.fetchone()
    while row:
        print(f"  {row}")
        row = cursor.fetchone()

    # Fetch all rows
    cursor.execute("SELECT name, price FROM products WHERE price < 100")
    rows = cursor.fetchall()
    print(f"\nProducts under $100: {len(rows)} items")
    for row in rows:
        print(f"  {row}")

    cursor.close()
    db.close()
    print("\n✓ Cursor API example completed\n")


def example_aggregates():
    """Demonstrate aggregate functions"""
    print("=== Aggregate Functions Example ===")

    db = prismdb.connect()

    # Create sales data
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
    db.execute("INSERT INTO sales VALUES ('South', 'Keyboard', 150.00, 5)")

    # Aggregate queries
    result = db.execute("""
        SELECT
            region,
            COUNT(*) as num_sales,
            SUM(amount) as total_revenue,
            AVG(amount) as avg_sale,
            MAX(quantity) as max_qty
        FROM sales
        GROUP BY region
        ORDER BY region
    """)

    print("\nSales by region:")
    for row in result:
        print(f"  {row}")

    db.close()
    print("\n✓ Aggregate functions example completed\n")


def example_to_dict():
    """Demonstrate dictionary conversion"""
    print("=== Dictionary Conversion Example ===")

    db = prismdb.connect()

    db.execute("CREATE TABLE employees (id INTEGER, name VARCHAR, salary DOUBLE)")
    db.execute("INSERT INTO employees VALUES (1, 'Alice', 75000.0)")
    db.execute("INSERT INTO employees VALUES (2, 'Bob', 65000.0)")
    db.execute("INSERT INTO employees VALUES (3, 'Charlie', 85000.0)")

    # Convert to dictionary
    data = db.to_dict("SELECT * FROM employees ORDER BY id")

    print("\nData as dictionary:")
    # Column names include table prefix
    keys = list(data.keys())
    for key in keys:
        print(f"  {key}: {data[key]}")

    db.close()
    print("\n✓ Dictionary conversion example completed\n")


def example_context_manager():
    """Demonstrate context manager usage"""
    print("=== Context Manager Example ===")

    # Using 'with' statement for automatic cleanup
    with prismdb.connect() as db:
        db.execute("CREATE TABLE temp (value INTEGER)")
        db.execute("INSERT INTO temp VALUES (42)")

        result = db.execute("SELECT * FROM temp")
        for row in result:
            print(f"  {row}")

    # Database is automatically closed
    print("\n✓ Context manager example completed\n")


def example_string_functions():
    """Demonstrate string functions"""
    print("=== String Functions Example ===")

    db = prismdb.connect()

    result = db.execute("""
        SELECT
            UPPER('hello') as upper_case,
            LOWER('WORLD') as lower_case,
            TRIM('  space  ') as trimmed,
            REVERSE('prismdb') as reversed,
            LEFT('database', 4) as left_part,
            RIGHT('database', 4) as right_part
    """)

    print("\nString function results:")
    for row in result:
        print(f"  {row}")

    db.close()
    print("\n✓ String functions example completed\n")


def example_file_based_db():
    """Demonstrate file-based database"""
    print("=== File-Based Database Example ===")

    import os
    import tempfile

    # Create temporary database file
    with tempfile.NamedTemporaryFile(delete=False, suffix='.db') as f:
        db_path = f.name

    try:
        # Create and populate database
        print(f"\nCreating file-based database at: {db_path}")
        db = prismdb.connect(db_path)
        db.execute("CREATE TABLE persistent (id INTEGER, value VARCHAR)")
        db.execute("INSERT INTO persistent VALUES (1, 'stored')")

        # Verify data is accessible
        result = db.execute("SELECT * FROM persistent")
        print("Data in current session:")
        for row in result:
            print(f"  {row}")

        db.close()
        print("\nNote: File persistence requires WAL/checkpoint implementation")
        print("      (currently in development)")

    finally:
        # Clean up
        if os.path.exists(db_path):
            os.unlink(db_path)

    print("\n✓ File-based database example completed\n")


if __name__ == "__main__":
    print("PrismDB Python Examples\n")
    print("=" * 50)

    example_basic_queries()
    example_cursor_api()
    example_aggregates()
    example_to_dict()
    example_context_manager()
    example_string_functions()
    example_file_based_db()

    print("=" * 50)
    print("\nAll examples completed successfully!")
