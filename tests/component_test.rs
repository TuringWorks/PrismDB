//! Test to isolate component initialization issues

use prismdb::PrismDBResult;

#[test]
fn test_catalog_only() -> PrismDBResult<()> {
    println!("Testing catalog creation only...");
    let _catalog = duckdb::catalog::Catalog::new();
    println!("Catalog created successfully");
    Ok(())
}

#[test]
fn test_buffer_manager_only() -> PrismDBResult<()> {
    println!("Testing buffer manager creation only...");
    let buffer_config = duckdb::storage::BufferConfig::new(1024 * 1024 * 1024, 1000);
    let _buffer_manager = duckdb::storage::BufferManager::new(buffer_config);
    println!("Buffer manager created successfully");
    Ok(())
}

#[test]
fn test_transaction_manager_only() -> PrismDBResult<()> {
    println!("Testing transaction manager creation only...");
    let _transaction_manager = duckdb::storage::TransactionManager::new();
    println!("Transaction manager created successfully");
    Ok(())
}

#[test]
fn test_wal_manager_only() -> PrismDBResult<()> {
    println!("Testing WAL manager creation only...");
    let wal_path = std::path::PathBuf::from("data");
    let _wal_manager = duckdb::storage::WalManager::new(wal_path)?;
    println!("WAL manager created successfully");
    Ok(())
}