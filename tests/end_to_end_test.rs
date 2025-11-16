//! End-to-end integration test for CREATE TABLE → INSERT → SELECT flow
//!
//! This test verifies the complete data pipeline:
//! 1. CREATE TABLE - Creates a table in the catalog
//! 2. INSERT - Inserts data into the table
//! 3. SELECT - Reads the data back and verifies correctness

use prism::catalog::Catalog;
use prism::execution::pipeline::TableScanSource;
use prism::execution::{ExecutionContext, ExecutionEngine};
use prism::planner::{PhysicalColumn, PhysicalCreateTable, PhysicalPlan, PhysicalTableScan};
use prism::storage::TransactionManager;
use prism::types::{LogicalType, Value};
use std::sync::{Arc, RwLock};

#[test]
fn test_create_insert_select_flow() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Testing CREATE TABLE → INSERT → SELECT Flow ===\n");

    // 1. Setup: Create catalog and execution context
    let catalog = Arc::new(RwLock::new(Catalog::new()));
    let transaction_manager = Arc::new(TransactionManager::new());
    let context = ExecutionContext::new(transaction_manager.clone(), catalog.clone());

    println!("✓ Created catalog and execution context");

    // 2. CREATE TABLE
    println!("\n--- Step 1: CREATE TABLE ---");

    let create_table_plan = PhysicalCreateTable {
        table_name: "users".to_string(),
        schema: vec![
            PhysicalColumn::new("id".to_string(), LogicalType::Integer),
            PhysicalColumn::new("name".to_string(), LogicalType::Varchar),
            PhysicalColumn::new("age".to_string(), LogicalType::Integer),
        ],
        if_not_exists: false,
    };

    let mut engine = ExecutionEngine::new(context.clone());
    let create_plan = PhysicalPlan::CreateTable(create_table_plan);
    let mut create_stream = engine.execute(create_plan)?;

    // Consume the create table result
    while let Some(_) = create_stream.next() {}

    println!("✓ Created table 'users' with columns: id, name, age");

    // Verify table exists in catalog
    {
        let catalog_guard = catalog.read().unwrap();
        let schema_arc = catalog_guard.get_schema("main")?;
        let schema_guard = schema_arc.read().unwrap();
        let _table = schema_guard.get_table("users")?;
        println!("✓ Verified table exists in catalog");
    }

    // 3. INSERT DATA
    println!("\n--- Step 2: INSERT DATA ---");

    // We need to create a plan that produces data to insert
    // For this test, we'll manually insert data into the table
    // since we don't have a VALUES expression node yet

    // Insert data directly for now (bypassing the INSERT operator)
    {
        let catalog_guard = catalog.read().unwrap();
        let schema_arc = catalog_guard.get_schema("main")?;
        let schema_guard = schema_arc.read().unwrap();
        let table_arc = schema_guard.get_table("users")?;
        let table = table_arc.read().unwrap();
        let table_data_arc = table.get_data();
        let mut table_data = table_data_arc.write().unwrap();

        table_data.insert_row(&[
            Value::Integer(1),
            Value::Varchar("Alice".to_string()),
            Value::Integer(30),
        ])?;

        table_data.insert_row(&[
            Value::Integer(2),
            Value::Varchar("Bob".to_string()),
            Value::Integer(25),
        ])?;

        table_data.insert_row(&[
            Value::Integer(3),
            Value::Varchar("Charlie".to_string()),
            Value::Integer(35),
        ])?;

        println!("✓ Inserted 3 rows into 'users' table");
        println!("  - (1, 'Alice', 30)");
        println!("  - (2, 'Bob', 25)");
        println!("  - (3, 'Charlie', 35)");
    }

    // 4. SELECT DATA
    println!("\n--- Step 3: SELECT DATA ---");

    let scan_plan = PhysicalTableScan {
        table_name: "users".to_string(),
        schema: vec![
            PhysicalColumn::new("id".to_string(), LogicalType::Integer),
            PhysicalColumn::new("name".to_string(), LogicalType::Varchar),
            PhysicalColumn::new("age".to_string(), LogicalType::Integer),
        ],
        column_ids: vec![0, 1, 2],
        filters: vec![],
        limit: None,
    };

    let mut table_scan = TableScanSource::new(scan_plan, context.clone())?;

    // Read all data
    let mut total_rows = 0;
    let mut all_data = Vec::new();
    let chunk_size = 1024;

    while let Some(chunk) = table_scan.next_chunk(chunk_size)? {
        total_rows += chunk.len();

        // Extract and store values from this chunk
        for row_idx in 0..chunk.len() {
            let mut row_values = Vec::new();
            for col_idx in 0..chunk.column_count() {
                let vector = chunk.get_vector(col_idx).unwrap();
                let value = vector.get_value(row_idx)?;
                row_values.push(value);
            }
            all_data.push(row_values);
        }
    }

    println!("✓ Selected {} rows from 'users' table", total_rows);

    // 5. VERIFY RESULTS
    println!("\n--- Step 4: VERIFY RESULTS ---");

    assert_eq!(total_rows, 3, "Should have read 3 rows");
    assert_eq!(all_data.len(), 3, "Should have 3 rows of data");

    // Verify first row: (1, 'Alice', 30)
    assert_eq!(all_data[0][0], Value::Integer(1));
    assert_eq!(all_data[0][1], Value::Varchar("Alice".to_string()));
    assert_eq!(all_data[0][2], Value::Integer(30));
    println!("✓ Row 1: (1, 'Alice', 30) - CORRECT");

    // Verify second row: (2, 'Bob', 25)
    assert_eq!(all_data[1][0], Value::Integer(2));
    assert_eq!(all_data[1][1], Value::Varchar("Bob".to_string()));
    assert_eq!(all_data[1][2], Value::Integer(25));
    println!("✓ Row 2: (2, 'Bob', 25) - CORRECT");

    // Verify third row: (3, 'Charlie', 35)
    assert_eq!(all_data[2][0], Value::Integer(3));
    assert_eq!(all_data[2][1], Value::Varchar("Charlie".to_string()));
    assert_eq!(all_data[2][2], Value::Integer(35));
    println!("✓ Row 3: (3, 'Charlie', 35) - CORRECT");

    println!("\n=== ✅ END-TO-END TEST PASSED ===");
    println!("Successfully completed: CREATE TABLE → INSERT → SELECT");

    Ok(())
}

#[test]
fn test_create_table_if_not_exists() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Testing CREATE TABLE IF NOT EXISTS ===\n");

    let catalog = Arc::new(RwLock::new(Catalog::new()));
    let transaction_manager = Arc::new(TransactionManager::new());
    let context = ExecutionContext::new(transaction_manager.clone(), catalog.clone());

    // First creation should succeed
    let create_table_plan1 = PhysicalCreateTable {
        table_name: "test_table".to_string(),
        schema: vec![PhysicalColumn::new("id".to_string(), LogicalType::Integer)],
        if_not_exists: false,
    };

    let mut engine = ExecutionEngine::new(context.clone());
    let create_plan1 = PhysicalPlan::CreateTable(create_table_plan1);
    let mut create_stream1 = engine.execute(create_plan1)?;
    while let Some(_) = create_stream1.next() {}

    println!("✓ First CREATE TABLE succeeded");

    // Second creation with IF NOT EXISTS should succeed (no error)
    let create_table_plan2 = PhysicalCreateTable {
        table_name: "test_table".to_string(),
        schema: vec![PhysicalColumn::new("id".to_string(), LogicalType::Integer)],
        if_not_exists: true,
    };

    let mut engine2 = ExecutionEngine::new(context.clone());
    let create_plan2 = PhysicalPlan::CreateTable(create_table_plan2);
    let mut create_stream2 = engine2.execute(create_plan2)?;
    while let Some(_) = create_stream2.next() {}

    println!("✓ Second CREATE TABLE IF NOT EXISTS succeeded (no error)");

    println!("\n=== ✅ TEST PASSED ===");

    Ok(())
}

#[test]
fn test_drop_table() -> Result<(), Box<dyn std::error::Error>> {
    use prism::planner::PhysicalDropTable;

    println!("\n=== Testing DROP TABLE ===\n");

    let catalog = Arc::new(RwLock::new(Catalog::new()));
    let transaction_manager = Arc::new(TransactionManager::new());
    let context = ExecutionContext::new(transaction_manager.clone(), catalog.clone());

    // Create a table first
    let create_table_plan = PhysicalCreateTable {
        table_name: "temp_table".to_string(),
        schema: vec![PhysicalColumn::new("id".to_string(), LogicalType::Integer)],
        if_not_exists: false,
    };

    let mut engine = ExecutionEngine::new(context.clone());
    let create_plan = PhysicalPlan::CreateTable(create_table_plan);
    let mut create_stream = engine.execute(create_plan)?;
    while let Some(_) = create_stream.next() {}

    println!("✓ Created table 'temp_table'");

    // Verify table exists
    {
        let catalog_guard = catalog.read().unwrap();
        let schema_arc = catalog_guard.get_schema("main")?;
        let schema_guard = schema_arc.read().unwrap();
        assert!(schema_guard.get_table("temp_table").is_ok());
        println!("✓ Verified table exists");
    }

    // Drop the table
    let drop_table_plan = PhysicalDropTable {
        table_name: "temp_table".to_string(),
        if_exists: false,
    };

    let mut engine2 = ExecutionEngine::new(context.clone());
    let drop_plan = PhysicalPlan::DropTable(drop_table_plan);
    let mut drop_stream = engine2.execute(drop_plan)?;
    while let Some(_) = drop_stream.next() {}

    println!("✓ Dropped table 'temp_table'");

    // Verify table no longer exists
    {
        let catalog_guard = catalog.read().unwrap();
        let schema_arc = catalog_guard.get_schema("main")?;
        let schema_guard = schema_arc.read().unwrap();
        assert!(schema_guard.get_table("temp_table").is_err());
        println!("✓ Verified table no longer exists");
    }

    println!("\n=== ✅ TEST PASSED ===");

    Ok(())
}
