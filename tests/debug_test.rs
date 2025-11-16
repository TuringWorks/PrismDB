use prism::Database;
use prism::PrismDBResult;

#[test]
fn test_basic_create() -> PrismDBResult<()> {
    println!("=== Starting Basic Create Test ===");
    
    let mut db = Database::new_in_memory()?;
    println!("✓ Database created");
    
    // Just test table creation - no data
    let result = db.execute("CREATE TABLE test (id INTEGER, name VARCHAR)")?;
    println!("✓ Table created successfully");
    println!("Rows processed: {}", result.row_count());
    
    Ok(())
}

#[test]
fn test_basic_insert() -> PrismDBResult<()> {
    println!("=== Starting Basic Insert Test ===");
    
    let mut db = Database::new_in_memory()?;
    println!("✓ Database created");
    
    // Create table
    db.execute("CREATE TABLE test (id INTEGER, name VARCHAR)")?;
    println!("✓ Table created");
    
    // Insert data
    let result = db.execute("INSERT INTO test VALUES (1, 'test')")?;
    println!("✓ Insert completed");
    println!("Rows processed: {}", result.row_count());
    
    Ok(())
}

#[test]
fn test_empty_select() -> PrismDBResult<()> {
    println!("=== Starting Empty Select Test ===");
    
    let mut db = Database::new_in_memory()?;
    println!("✓ Database created");
    
    // Create table
    db.execute("CREATE TABLE test (id INTEGER, name VARCHAR)")?;
    println!("✓ Table created");
    
    // Select from empty table
    println!("About to execute SELECT...");
    let result = db.execute("SELECT * FROM test")?;
    println!("✓ Select completed");
    println!("Rows processed: {}", result.row_count());
    
    Ok(())
}