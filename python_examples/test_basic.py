"""
Basic tests for PrismDB Python bindings
"""

import prismdb
import sys


def test_connection():
    """Test database connection"""
    print("Testing connection...", end=" ")
    db = prismdb.connect()
    assert db is not None
    db.close()
    print("✓")


def test_create_table():
    """Test table creation"""
    print("Testing CREATE TABLE...", end=" ")
    db = prismdb.connect()
    db.execute("CREATE TABLE test (id INTEGER, name VARCHAR)")
    db.close()
    print("✓")


def test_insert():
    """Test INSERT operation"""
    print("Testing INSERT...", end=" ")
    db = prismdb.connect()
    db.execute("CREATE TABLE test (id INTEGER, name VARCHAR)")
    db.execute("INSERT INTO test VALUES (1, 'Alice')")
    db.execute("INSERT INTO test VALUES (2, 'Bob')")

    result = db.execute("SELECT COUNT(*) FROM test")
    row = result.fetchone()
    assert row[0] == 2, f"Expected 2 rows, got {row[0]}"
    db.close()
    print("✓")


def test_select():
    """Test SELECT query"""
    print("Testing SELECT...", end=" ")
    db = prismdb.connect()
    db.execute("CREATE TABLE test (id INTEGER, value DOUBLE)")
    db.execute("INSERT INTO test VALUES (1, 10.5)")
    db.execute("INSERT INTO test VALUES (2, 20.5)")

    result = db.execute("SELECT * FROM test ORDER BY id")
    rows = result.fetchall()

    assert len(rows) == 2, f"Expected 2 rows, got {len(rows)}"
    assert rows[0][0] == 1, f"Expected id=1, got {rows[0][0]}"
    db.close()
    print("✓")


def test_cursor():
    """Test cursor API"""
    print("Testing cursor API...", end=" ")
    db = prismdb.connect()
    db.execute("CREATE TABLE test (value INTEGER)")
    db.execute("INSERT INTO test VALUES (1)")
    db.execute("INSERT INTO test VALUES (2)")
    db.execute("INSERT INTO test VALUES (3)")

    cursor = db.cursor()
    cursor.execute("SELECT * FROM test")

    # Test fetchone
    row1 = cursor.fetchone()
    assert row1 is not None

    # Test fetchall
    cursor.execute("SELECT * FROM test")
    rows = cursor.fetchall()
    assert len(rows) == 3, f"Expected 3 rows, got {len(rows)}"

    cursor.close()
    db.close()
    print("✓")


def test_aggregates():
    """Test aggregate functions"""
    print("Testing aggregate functions...", end=" ")
    db = prismdb.connect()
    db.execute("CREATE TABLE numbers (value INTEGER)")
    db.execute("INSERT INTO numbers VALUES (10)")
    db.execute("INSERT INTO numbers VALUES (20)")
    db.execute("INSERT INTO numbers VALUES (30)")

    result = db.execute("SELECT SUM(value), AVG(value), MIN(value), MAX(value), COUNT(*) FROM numbers")
    row = result.fetchone()

    assert row[0] == 60, f"Expected SUM=60, got {row[0]}"
    assert row[2] == 10, f"Expected MIN=10, got {row[2]}"
    assert row[3] == 30, f"Expected MAX=30, got {row[3]}"
    assert row[4] == 3, f"Expected COUNT=3, got {row[4]}"

    db.close()
    print("✓")


def test_string_functions():
    """Test string functions"""
    print("Testing string functions...", end=" ")
    db = prismdb.connect()

    result = db.execute("SELECT UPPER('hello') as upper_test")
    row = result.fetchone()
    assert row[0] == 'HELLO', f"Expected 'HELLO', got {row[0]}"

    result = db.execute("SELECT LOWER('WORLD') as lower_test")
    row = result.fetchone()
    assert row[0] == 'world', f"Expected 'world', got {row[0]}"

    db.close()
    print("✓")


def test_to_dict():
    """Test to_dict conversion"""
    print("Testing to_dict...", end=" ")
    db = prismdb.connect()
    db.execute("CREATE TABLE test (id INTEGER, name VARCHAR)")
    db.execute("INSERT INTO test VALUES (1, 'Alice')")
    db.execute("INSERT INTO test VALUES (2, 'Bob')")

    data = db.to_dict("SELECT * FROM test ORDER BY id")

    # Column names include table prefix (test.id, test.name)
    keys = list(data.keys())
    assert len(keys) == 2, f"Expected 2 columns, got {len(keys)}"
    assert any('id' in k for k in keys), "Expected 'id' column in dict"
    assert any('name' in k for k in keys), "Expected 'name' column in dict"

    # Get the actual column names
    id_col = [k for k in keys if 'id' in k][0]
    name_col = [k for k in keys if 'name' in k][0]

    assert data[id_col] == [1, 2], f"Expected [1, 2], got {data[id_col]}"
    assert data[name_col] == ['Alice', 'Bob'], f"Expected ['Alice', 'Bob'], got {data[name_col]}"

    db.close()
    print("✓")


def test_context_manager():
    """Test context manager"""
    print("Testing context manager...", end=" ")
    with prismdb.connect() as db:
        db.execute("CREATE TABLE test (value INTEGER)")
        db.execute("INSERT INTO test VALUES (42)")
        result = db.execute("SELECT * FROM test")
        row = result.fetchone()
        assert row[0] == 42
    print("✓")


def test_iterator():
    """Test iterator protocol"""
    print("Testing iterator protocol...", end=" ")
    db = prismdb.connect()
    db.execute("CREATE TABLE test (value INTEGER)")
    db.execute("INSERT INTO test VALUES (1)")
    db.execute("INSERT INTO test VALUES (2)")
    db.execute("INSERT INTO test VALUES (3)")

    result = db.execute("SELECT * FROM test")
    count = 0
    for row in result:
        count += 1
        assert row[0] in [1, 2, 3]

    assert count == 3, f"Expected 3 iterations, got {count}"
    db.close()
    print("✓")


def run_all_tests():
    """Run all tests"""
    print("=" * 50)
    print("PrismDB Python Bindings - Test Suite")
    print("=" * 50)
    print()

    tests = [
        test_connection,
        test_create_table,
        test_insert,
        test_select,
        test_cursor,
        test_aggregates,
        test_string_functions,
        test_to_dict,
        test_context_manager,
        test_iterator,
    ]

    failed = []

    for test in tests:
        try:
            test()
        except Exception as e:
            print(f"✗ FAILED: {e}")
            failed.append((test.__name__, str(e)))

    print()
    print("=" * 50)

    if failed:
        print(f"FAILED: {len(failed)} test(s) failed")
        for name, error in failed:
            print(f"  - {name}: {error}")
        return False
    else:
        print(f"SUCCESS: All {len(tests)} tests passed!")
        return True


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
