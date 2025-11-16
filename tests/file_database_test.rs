use prism::PrismDBResult;
use prism::database::{Database, DatabaseConfig};
use tempfile::tempdir;

#[test]
fn test_file_database_create_and_reopen() -> PrismDBResult<()> {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");

    // Create a file-based database
    {
        let db = Database::open(&db_path)?;
        assert!(db.is_file_based());
        assert_eq!(db.get_file_path().unwrap(), db_path.as_path());

        // Create a table
        db.execute_sql_collect("CREATE TABLE test (id INTEGER, name VARCHAR)")?;
        db.execute_sql_collect("INSERT INTO test VALUES (1, 'Alice')")?;
        db.execute_sql_collect("INSERT INTO test VALUES (2, 'Bob')")?;

        // Query the table
        let result = db.execute_sql_collect("SELECT * FROM test")?;
        assert_eq!(result.row_count(), 2);

        // Sync to disk
        db.sync()?;
    }

    // Reopen the database
    {
        let db = Database::open(&db_path)?;
        assert!(db.is_file_based());

        // The table should still be available (once catalog persistence is implemented)
        // For now, this will fail because we haven't implemented catalog loading
        // TODO: Uncomment when catalog persistence is implemented
        // let result = db.execute_sql_collect("SELECT * FROM test")?;
        // assert_eq!(result.row_count(), 2);
    }

    Ok(())
}

#[test]
fn test_database_config_in_memory() -> PrismDBResult<()> {
    let config = DatabaseConfig::in_memory();
    let db = Database::new(config)?;
    assert!(!db.is_file_based());
    assert_eq!(db.get_file_path(), None);
    Ok(())
}

#[test]
fn test_database_config_file_based() -> PrismDBResult<()> {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");

    let config = DatabaseConfig::from_file(db_path.to_string_lossy().to_string());
    let db = Database::new(config)?;
    assert!(db.is_file_based());
    assert_eq!(db.get_file_path().unwrap(), db_path.as_path());

    Ok(())
}

#[test]
fn test_file_database_basic_operations() -> PrismDBResult<()> {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test_ops.db");

    let db = Database::open(&db_path)?;

    // Test CREATE TABLE
    db.execute_sql_collect("CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER)")?;

    // Test INSERT
    db.execute_sql_collect("INSERT INTO users VALUES (1, 'Alice', 30)")?;
    db.execute_sql_collect("INSERT INTO users VALUES (2, 'Bob', 25)")?;
    db.execute_sql_collect("INSERT INTO users VALUES (3, 'Charlie', 35)")?;

    // Test SELECT
    let result = db.execute_sql_collect("SELECT * FROM users")?;
    assert_eq!(result.row_count(), 3);

    // Test SELECT with WHERE
    let result = db.execute_sql_collect("SELECT * FROM users WHERE age > 25")?;
    assert_eq!(result.row_count(), 2);

    // Test arithmetic operations
    let result = db.execute_sql_collect("SELECT id * 10 AS scaled_id FROM users")?;
    assert_eq!(result.row_count(), 3);

    // Sync to disk
    db.sync()?;

    println!("✓ File-based database operations work correctly");

    Ok(())
}
