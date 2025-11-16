//! Simple integration test for TableScan functionality
//!
//! This test verifies that TableScan can read data from storage at a low level

use prismdb::catalog::{Catalog, Schema};
use prismdb::execution::pipeline::TableScanSource;
use prismdb::execution::ExecutionContext;
use prismdb::planner::PhysicalTableScan;
use prismdb::storage::{
    BufferManager, ColumnInfo, TableInfo, TransactionManager, WalManager,
};
use prismdb::types::{LogicalType, Value};
use std::sync::{Arc, RwLock};

#[test]
fn test_table_scan_source_reads_data() -> Result<(), Box<dyn std::error::Error>> {
    // Create catalog
    let catalog = Catalog::new();

    // Create the "main" schema
    let mut schema = Schema::new("main".to_string());

    // Create table info
    let mut table_info = TableInfo::new("test_table".to_string());
    table_info.add_column(ColumnInfo::new("id".to_string(), LogicalType::Integer, 0))?;
    table_info.add_column(ColumnInfo::new("name".to_string(), LogicalType::Varchar, 1))?;

    // Create table and add to schema
    schema.create_table(&table_info)?;

    // Get the table and insert some test data
    {
        let table_arc = schema.get_table("test_table")?;
        let table = table_arc.read().unwrap();
        let table_data_arc = table.get_data();
        let mut table_data = table_data_arc.write().unwrap();

        // Insert 3 rows
        table_data.insert_row(&[Value::Integer(1), Value::Varchar("Alice".to_string())])?;
        table_data.insert_row(&[Value::Integer(2), Value::Varchar("Bob".to_string())])?;
        table_data.insert_row(&[Value::Integer(3), Value::Varchar("Charlie".to_string())])?;

        println!("Inserted 3 rows into table");
        println!("Table row count: {}", table_data.row_count());
    }

    // Get the existing "main" schema from catalog (it's created by default)
    let schema_arc = catalog.get_schema("main")?;
    {
        let mut schema_mut = schema_arc.write().unwrap();
        *schema_mut = schema;
    }

    // Create execution context
    let catalog_arc = Arc::new(RwLock::new(catalog));

    use prismdb::storage::BufferConfig;
    let buffer_config = BufferConfig::new(1024 * 1024 * 1024, 1000); // 1GB, 1000 pages
    let _buffer_manager = Arc::new(BufferManager::new(buffer_config));

    // Use temp directory for WAL
    let temp_dir = std::env::temp_dir();
    let wal_dir = temp_dir.join("duckdbrs_test_wal");
    let _wal_manager = Arc::new(WalManager::new(&wal_dir)?);

    let transaction_manager = Arc::new(TransactionManager::new());
    let context = ExecutionContext::new(transaction_manager, catalog_arc);

    // Create a PhysicalTableScan
    // use prismdb::expression::Expression; // Not needed
    use prismdb::planner::PhysicalColumn;

    let scan = PhysicalTableScan {
        table_name: "test_table".to_string(),
        schema: vec![
            PhysicalColumn::new("id".to_string(), LogicalType::Integer),
            PhysicalColumn::new("name".to_string(), LogicalType::Varchar),
        ],
        column_ids: vec![0, 1],
        filters: vec![],
        limit: None,
    };

    // Create TableScanSource
    let mut table_scan = TableScanSource::new(scan, context)?;

    // Read chunks from the table
    let mut total_rows = 0;
    let chunk_size = 1024;

    while let Some(chunk) = table_scan.next_chunk(chunk_size)? {
        total_rows += chunk.len();
        println!("Read chunk with {} rows", chunk.len());
        println!("Chunk column count: {}", chunk.column_count());
    }

    println!("Total rows read: {}", total_rows);

    // Verify we read all 3 rows
    assert_eq!(total_rows, 3, "Should have read 3 rows from table");

    println!("✅ TableScanSource successfully read data from storage!");

    Ok(())
}
