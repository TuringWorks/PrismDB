use prismdb::PrismDBResult;
use prismdb::database::{Database, DatabaseConfig};

#[test]
fn test_constant_folding_optimization() -> PrismDBResult<()> {
    let db = Database::new(DatabaseConfig::in_memory())?;

    // Create a table
    db.execute_sql_collect("CREATE TABLE test (id INTEGER, value INTEGER)")?;
    db.execute_sql_collect("INSERT INTO test VALUES (1, 10)")?;
    db.execute_sql_collect("INSERT INTO test VALUES (2, 20)")?;
    db.execute_sql_collect("INSERT INTO test VALUES (3, 30)")?;

    // Query with constant expressions that should be folded at compile time
    // 5 + 3 should be folded to 8
    let result = db.execute_sql_collect("SELECT id, value FROM test WHERE id < (5 + 3)")?;
    assert_eq!(result.row_count(), 3); // All rows should match (id < 8)

    // 10 * 2 should be folded to 20
    let result = db.execute_sql_collect("SELECT id, value FROM test WHERE value <= (10 * 2)")?;
    assert_eq!(result.row_count(), 2); // Only first two rows

    // Nested constant expression: 2 * 3 - 5 should be folded to 1
    let result = db.execute_sql_collect("SELECT id FROM test WHERE id > (2 * 3 - 5)")?;
    assert_eq!(result.row_count(), 2); // id > 1, so id=2 and id=3 (2 rows)

    println!("✓ Constant folding optimization works");
    Ok(())
}

#[test]
fn test_filter_pushdown_optimization() -> PrismDBResult<()> {
    let db = Database::new(DatabaseConfig::in_memory())?;

    // Create a table
    db.execute_sql_collect("CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER)")?;
    db.execute_sql_collect("INSERT INTO users VALUES (1, 'Alice', 30)")?;
    db.execute_sql_collect("INSERT INTO users VALUES (2, 'Bob', 25)")?;
    db.execute_sql_collect("INSERT INTO users VALUES (3, 'Charlie', 35)")?;
    db.execute_sql_collect("INSERT INTO users VALUES (4, 'David', 28)")?;

    // Filter should be pushed down to table scan
    let result = db.execute_sql_collect("SELECT name FROM users WHERE age > 25")?;
    assert_eq!(result.row_count(), 3); // Alice, Charlie, David

    // Multiple filters
    let result = db.execute_sql_collect("SELECT name FROM users WHERE age > 25 AND age < 35")?;
    assert_eq!(result.row_count(), 2); // Alice, David

    println!("✓ Filter pushdown optimization works");
    Ok(())
}

#[test]
fn test_limit_pushdown_optimization() -> PrismDBResult<()> {
    let db = Database::new(DatabaseConfig::in_memory())?;

    // Create a table with more rows
    db.execute_sql_collect("CREATE TABLE numbers (n INTEGER)")?;
    for i in 1..=100 {
        db.execute_sql_collect(&format!("INSERT INTO numbers VALUES ({})", i))?;
    }

    // Limit should be pushed down to table scan
    let result = db.execute_sql_collect("SELECT n FROM numbers LIMIT 10")?;
    assert_eq!(result.row_count(), 10);

    // Limit with offset
    let result = db.execute_sql_collect("SELECT n FROM numbers LIMIT 5 OFFSET 10")?;
    assert_eq!(result.row_count(), 5);

    println!("✓ Limit pushdown optimization works");
    Ok(())
}

#[test]
fn test_combined_optimizations() -> PrismDBResult<()> {
    let db = Database::new(DatabaseConfig::in_memory())?;

    db.execute_sql_collect(
        "CREATE TABLE products (id INTEGER, name VARCHAR, price INTEGER, stock INTEGER)",
    )?;
    db.execute_sql_collect("INSERT INTO products VALUES (1, 'Laptop', 1000, 50)")?;
    db.execute_sql_collect("INSERT INTO products VALUES (2, 'Mouse', 25, 100)")?;
    db.execute_sql_collect("INSERT INTO products VALUES (3, 'Keyboard', 75, 80)")?;
    db.execute_sql_collect("INSERT INTO products VALUES (4, 'Monitor', 300, 40)")?;
    db.execute_sql_collect("INSERT INTO products VALUES (5, 'USB Cable', 10, 200)")?;

    // This query should benefit from:
    // 1. Constant folding: (20 + 5) -> 25
    // 2. Filter pushdown: price > 25 pushed to table scan
    // 3. Projection pushdown: only read name and price columns
    let result = db.execute_sql_collect(
        "SELECT name, price FROM products WHERE price > (20 + 5) AND stock > 30",
    )?;
    assert_eq!(result.row_count(), 3); // Laptop, Keyboard, Monitor

    // Test with arithmetic in projection and filter
    // Constant folding: 2 * 50 -> 100
    let result = db.execute_sql_collect(
        "SELECT name, price * 2 AS doubled FROM products WHERE stock > (2 * 50)",
    )?;
    assert_eq!(result.row_count(), 1); // Only USB Cable has stock > 100

    println!("✓ Combined optimizations work correctly");
    Ok(())
}

#[test]
fn test_optimizer_preserves_correctness() -> PrismDBResult<()> {
    let db = Database::new(DatabaseConfig::in_memory())?;

    db.execute_sql_collect("CREATE TABLE test (a INTEGER, b INTEGER, c INTEGER)")?;
    db.execute_sql_collect("INSERT INTO test VALUES (1, 2, 3)")?;
    db.execute_sql_collect("INSERT INTO test VALUES (4, 5, 6)")?;
    db.execute_sql_collect("INSERT INTO test VALUES (7, 8, 9)")?;

    // Complex query with multiple operations
    let result =
        db.execute_sql_collect("SELECT a, b * c AS product FROM test WHERE a + b > 5 AND c < 10")?;
    assert_eq!(result.row_count(), 2); // Rows 2 and 3

    // Query with all arithmetic operators
    let result = db.execute_sql_collect(
        "SELECT a * 2 AS doubled, b / 2 AS halved, c % 2 AS remainder FROM test",
    )?;
    assert_eq!(result.row_count(), 3);

    println!("✓ Optimizer preserves query correctness");
    Ok(())
}
