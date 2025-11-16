//! Integration tests for query execution
//! 
//! These tests verify end-to-end query execution functionality.
//! Currently simplified while advanced features are being implemented.

use prism::{Database, PrismDBResult};

#[test]
fn test_basic_query_execution() -> PrismDBResult<()> {
    let mut db = Database::new_in_memory()?;

    // Create table
    db.execute("CREATE TABLE test (id INTEGER, name VARCHAR)")?;

    // Insert data
    db.execute("INSERT INTO test VALUES (1, 'Alice')")?;

    // Select data
    let result = db.execute("SELECT * FROM test")?;

    assert!(result.row_count() > 0);
    Ok(())
}
