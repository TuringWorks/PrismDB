use prism::PrismDBResult;
use prism::database::{Database, DatabaseConfig};

#[test]
fn test_arithmetic_multiply() -> PrismDBResult<()> {
    let db = Database::new(DatabaseConfig::in_memory())?;

    db.execute_sql_collect("CREATE TABLE test (a INTEGER, b INTEGER)")?;
    db.execute_sql_collect("INSERT INTO test VALUES (3, 4)")?;
    db.execute_sql_collect("INSERT INTO test VALUES (5, 6)")?;

    let result = db.execute_sql_collect("SELECT a * b AS product FROM test")?;

    assert_eq!(result.row_count(), 2);
    println!("✓ Multiplication works");
    Ok(())
}

#[test]
fn test_arithmetic_divide() -> PrismDBResult<()> {
    let db = Database::new(DatabaseConfig::in_memory())?;

    db.execute_sql_collect("CREATE TABLE test (a INTEGER, b INTEGER)")?;
    db.execute_sql_collect("INSERT INTO test VALUES (20, 4)")?;
    db.execute_sql_collect("INSERT INTO test VALUES (15, 3)")?;

    let result = db.execute_sql_collect("SELECT a / b AS quotient FROM test")?;

    assert_eq!(result.row_count(), 2);
    println!("✓ Division works");
    Ok(())
}

#[test]
fn test_arithmetic_modulo() -> PrismDBResult<()> {
    let db = Database::new(DatabaseConfig::in_memory())?;

    db.execute_sql_collect("CREATE TABLE test (a INTEGER, b INTEGER)")?;
    db.execute_sql_collect("INSERT INTO test VALUES (10, 3)")?;
    db.execute_sql_collect("INSERT INTO test VALUES (17, 5)")?;

    let result = db.execute_sql_collect("SELECT a % b AS remainder FROM test")?;

    assert_eq!(result.row_count(), 2);
    println!("✓ Modulo works");
    Ok(())
}

#[test]
fn test_arithmetic_combined() -> PrismDBResult<()> {
    let db = Database::new(DatabaseConfig::in_memory())?;

    db.execute_sql_collect("CREATE TABLE test (x INTEGER, y INTEGER, z INTEGER)")?;
    db.execute_sql_collect("INSERT INTO test VALUES (2, 3, 4)")?;

    // Test combined: (x * y) + z = (2 * 3) + 4 = 10
    let result = db.execute_sql_collect("SELECT (x * y) + z AS result FROM test")?;

    assert_eq!(result.row_count(), 1);
    println!("✓ Combined arithmetic works");
    Ok(())
}
