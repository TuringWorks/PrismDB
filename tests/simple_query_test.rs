//! Simple query test to debug basic functionality

use prism::database::Database;
use prism::PrismDBResult;

#[test]
fn test_simple_database_creation() -> PrismDBResult<()> {
    println!("Starting simple database creation test");
    let _db = Database::new_in_memory()?;
    println!("Database created successfully");
    Ok(())
}

#[test]
fn test_simple_table_creation() -> PrismDBResult<()> {
    println!("Starting simple table creation test");
    let mut db = Database::new_in_memory()?;
    
    println!("Creating table...");
    let result = db.execute("CREATE TABLE test (id INTEGER, name VARCHAR)");
    println!("Table creation result: {:?}", result);
    
    Ok(())
}

#[test]
fn test_simple_insert() -> PrismDBResult<()> {
    println!("Starting simple insert test");
    let mut db = Database::new_in_memory()?;
    
    println!("Creating table...");
    db.execute("CREATE TABLE test (id INTEGER, name VARCHAR)")?;
    
    println!("Inserting data...");
    let result = db.execute("INSERT INTO test VALUES (1, 'test')");
    println!("Insert result: {:?}", result);
    
    Ok(())
}

#[test]
fn test_simple_select() -> PrismDBResult<()> {
    println!("Starting simple select test");
    
    println!("About to create database...");
    let mut db = Database::new_in_memory()?;
    println!("Database created successfully");
    
    println!("About to create table...");
    let create_result = db.execute("CREATE TABLE test (id INTEGER, name VARCHAR)");
    println!("Table creation result: {:?}", create_result);
    
    println!("About to execute SELECT...");
    let select_result = db.execute("SELECT * FROM test");
    println!("Select result: {:?}", select_result);
    
    println!("Test completed");
    Ok(())
}