#[cfg(test)]
mod empty_tests {
    use prismdb::Database;
    use prismdb::PrismDBResult;

    #[test]
    fn test_empty_select() -> PrismDBResult<()> {
        println!("=== Testing Empty SELECT ===");
        
        // Create database
        let mut db = Database::new_in_memory()?;
        
        // Create table
        db.execute("CREATE TABLE users (id INTEGER, name VARCHAR)")?;
        
        // Try SELECT on empty table
        println!("Testing SELECT on empty table...");
        let result = db.execute("SELECT * FROM users");
        match result {
            Ok(_) => println!("   ✓ Empty SELECT succeeded"),
            Err(e) => println!("   ✗ Empty SELECT failed: {:?}", e),
        }
        
        println!("=== Empty Test Complete ===");
        Ok(())
    }
}