//! Test to isolate WalManager issue

use prism::PrismDBResult;

#[test]
fn test_database_without_wal() -> PrismDBResult<()> {
    println!("Testing database creation without WAL...");

    // Try to create components manually without WAL
    let _catalog = prism::catalog::Catalog::new();
    println!("Catalog created successfully");

    let buffer_config = prism::storage::BufferConfig::new(1024 * 1024 * 1024, 1000);
    let _buffer_manager = prism::storage::BufferManager::new(buffer_config);
    println!("Buffer manager created successfully");

    let _transaction_manager = prism::storage::TransactionManager::new();
    println!("Transaction manager created successfully");

    // Skip WAL manager creation
    println!("All components except WAL created successfully");
    Ok(())
}