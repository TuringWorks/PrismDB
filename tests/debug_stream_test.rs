use prism::{Database, PrismDBResult};

#[test]
fn test_stream_behavior() -> PrismDBResult<()> {
    let mut db = Database::new_in_memory()?;

    // Create table and insert data
    db.execute("CREATE TABLE users (id INTEGER, name VARCHAR)")?;
    db.execute("INSERT INTO users VALUES (1, 'Alice')")?;
    db.execute("INSERT INTO users VALUES (2, 'Bob')")?;

    // Test SELECT with debug output
    println!("About to execute SELECT query");
    let result = db.execute("SELECT * FROM users");
    println!("SELECT query completed with result: {:?}", result);

    assert!(result.is_ok());
    Ok(())
}