//! Direct executor test to bypass Database wrapper

use prismdb::execution::{QueryExecutor, ExecutionContext};
use prismdb::catalog::Catalog;
use prismdb::storage::TransactionManager;
use prismdb::PrismDBResult;
use std::sync::{Arc, RwLock};

#[test]
fn test_direct_executor() -> PrismDBResult<()> {
    println!("=== Starting direct executor test ===");

    println!("1. Creating execution context...");
    let catalog = Arc::new(RwLock::new(Catalog::new()));
    let transaction_manager = Arc::new(TransactionManager::new());
    let context = ExecutionContext::new(transaction_manager, catalog);
    println!("   Execution context created");

    println!("2. Creating query executor...");
    let mut executor = QueryExecutor::new(context);
    println!("   Query executor created");

    println!("3. Creating table...");
    let result = executor.execute_sql("CREATE TABLE test (id INTEGER, name VARCHAR)")?;
    println!("   Table creation result: {:?}", result);

    println!("4. Executing SELECT...");
    let result = executor.execute_sql("SELECT * FROM test")?;
    println!("   Select result: {:?}", result);

    println!("=== Test completed successfully ===");
    Ok(())
}