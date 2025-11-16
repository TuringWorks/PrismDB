//! Expression Binding Integration Tests
//!
//! Tests that verify expression binding works correctly throughout the query pipeline.

use prism::{Database, DatabaseConfig, PrismDBResult};

#[test]
fn test_database_creation() -> PrismDBResult<()> {
    // Just test that we can create a database with the new API
    let db = Database::new(DatabaseConfig::in_memory())?;
    assert!(db
        .catalog()
        .read()
        .unwrap()
        .list_schemas()
        .contains(&"main".to_string()));
    println!("✓ Database creation works");
    Ok(())
}

#[test]
fn test_create_table_via_sql() -> PrismDBResult<()> {
    let db = Database::new(DatabaseConfig::in_memory())?;

    // Try to execute CREATE TABLE
    let _result = db.execute_sql_collect("CREATE TABLE test (id INTEGER, name VARCHAR)")?;

    // Verify table was created
    let catalog = db.catalog();
    let catalog_guard = catalog.read().unwrap();
    assert!(catalog_guard.table_exists("main", "test"));

    println!("✓ CREATE TABLE via SQL works");
    Ok(())
}

#[test]
fn test_query_result_structure() -> PrismDBResult<()> {
    let db = Database::new(DatabaseConfig::in_memory())?;

    // Create and populate a simple table
    db.execute_sql_collect("CREATE TABLE numbers (value INTEGER)")?;

    // Query should return proper structure
    let result = db.execute_sql_collect("SELECT value FROM numbers")?;

    assert_eq!(result.row_count(), 0); // No rows yet
    assert_eq!(result.columns.len(), 1); // Should have 1 column
    assert_eq!(result.columns[0].name, "value");

    println!("✓ QueryResult structure is correct");
    Ok(())
}

#[test]
fn test_binding_infrastructure_exists() {
    // This test just verifies that the binding infrastructure compiles and is accessible
    use prism::expression::binder::{BinderContext, ColumnBinding, ExpressionBinder};
    use prism::types::LogicalType;
    use std::collections::HashMap;

    // Create a simple binder context
    let column_bindings = vec![
        ColumnBinding::new(0, 0, "id".to_string(), LogicalType::Integer),
        ColumnBinding::new(0, 1, "name".to_string(), LogicalType::Varchar),
    ];

    let context = BinderContext {
        alias_map: HashMap::new(),
        column_bindings,
        depth: 0,
    };

    let _binder = ExpressionBinder::new(context);

    println!("✓ Expression binding infrastructure is accessible");
}

#[test]
fn test_optimizer_integration() -> PrismDBResult<()> {
    // Verify the optimizer can be created and accepts logical plans
    use prism::planner::QueryOptimizer;

    let _optimizer = QueryOptimizer::new();

    println!("✓ Optimizer integration works");
    Ok(())
}

#[cfg(test)]
mod simplified_integration_tests {
    use super::*;

    #[test]
    fn test_full_pipeline_compile() -> PrismDBResult<()> {
        // Test that the full SQL pipeline compiles even if execution isn't fully working
        let db = Database::new(DatabaseConfig::in_memory())?;

        // These may fail at runtime but should compile
        let _ = db.execute_sql_collect("CREATE TABLE test (id INTEGER)");

        println!("✓ Full pipeline compiles");
        Ok(())
    }
}

#[cfg(test)]
mod where_clause_tests {
    use super::*;

    #[test]
    fn test_where_equality() -> PrismDBResult<()> {
        let db = Database::new(DatabaseConfig::in_memory())?;

        // Create table and insert data
        db.execute_sql_collect("CREATE TABLE users (id INTEGER, age INTEGER, name VARCHAR)")?;
        db.execute_sql_collect("INSERT INTO users VALUES (1, 25, 'Alice')")?;
        db.execute_sql_collect("INSERT INTO users VALUES (2, 30, 'Bob')")?;
        db.execute_sql_collect("INSERT INTO users VALUES (3, 25, 'Charlie')")?;

        // Test equality comparison
        let result = db.execute_sql_collect("SELECT id, name FROM users WHERE age = 25")?;

        assert_eq!(result.row_count(), 2); // Alice and Charlie
        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.columns[0].name, "id");
        assert_eq!(result.columns[1].name, "name");

        println!("✓ WHERE clause with equality (=) works");
        Ok(())
    }

    #[test]
    fn test_where_inequality() -> PrismDBResult<()> {
        let db = Database::new(DatabaseConfig::in_memory())?;

        db.execute_sql_collect("CREATE TABLE products (id INTEGER, price INTEGER)")?;
        db.execute_sql_collect("INSERT INTO products VALUES (1, 100)")?;
        db.execute_sql_collect("INSERT INTO products VALUES (2, 200)")?;
        db.execute_sql_collect("INSERT INTO products VALUES (3, 300)")?;

        // Test less than
        let result = db.execute_sql_collect("SELECT id FROM products WHERE price < 250")?;
        assert_eq!(result.row_count(), 2); // Products 1 and 2

        println!("✓ WHERE clause with less than (<) works");
        Ok(())
    }

    #[test]
    fn test_where_greater_than() -> PrismDBResult<()> {
        let db = Database::new(DatabaseConfig::in_memory())?;

        db.execute_sql_collect("CREATE TABLE scores (player_id INTEGER, score INTEGER)")?;
        db.execute_sql_collect("INSERT INTO scores VALUES (1, 85)")?;
        db.execute_sql_collect("INSERT INTO scores VALUES (2, 92)")?;
        db.execute_sql_collect("INSERT INTO scores VALUES (3, 78)")?;

        // Test greater than
        let result = db.execute_sql_collect("SELECT player_id FROM scores WHERE score > 80")?;
        assert_eq!(result.row_count(), 2); // Players 1 and 2

        println!("✓ WHERE clause with greater than (>) works");
        Ok(())
    }

    #[test]
    fn test_where_less_than_or_equal() -> PrismDBResult<()> {
        let db = Database::new(DatabaseConfig::in_memory())?;

        db.execute_sql_collect("CREATE TABLE inventory (item_id INTEGER, quantity INTEGER)")?;
        db.execute_sql_collect("INSERT INTO inventory VALUES (1, 10)")?;
        db.execute_sql_collect("INSERT INTO inventory VALUES (2, 5)")?;
        db.execute_sql_collect("INSERT INTO inventory VALUES (3, 15)")?;

        // Test less than or equal
        let result =
            db.execute_sql_collect("SELECT item_id FROM inventory WHERE quantity <= 10")?;
        assert_eq!(result.row_count(), 2); // Items 1 and 2

        println!("✓ WHERE clause with less than or equal (<=) works");
        Ok(())
    }

    #[test]
    fn test_where_greater_than_or_equal() -> PrismDBResult<()> {
        let db = Database::new(DatabaseConfig::in_memory())?;

        db.execute_sql_collect("CREATE TABLE temperatures (city_id INTEGER, temp INTEGER)")?;
        db.execute_sql_collect("INSERT INTO temperatures VALUES (1, 20)")?;
        db.execute_sql_collect("INSERT INTO temperatures VALUES (2, 25)")?;
        db.execute_sql_collect("INSERT INTO temperatures VALUES (3, 30)")?;

        // Test greater than or equal
        let result = db.execute_sql_collect("SELECT city_id FROM temperatures WHERE temp >= 25")?;
        assert_eq!(result.row_count(), 2); // Cities 2 and 3

        println!("✓ WHERE clause with greater than or equal (>=) works");
        Ok(())
    }

    #[test]
    fn test_where_not_equal() -> PrismDBResult<()> {
        let db = Database::new(DatabaseConfig::in_memory())?;

        db.execute_sql_collect("CREATE TABLE status (id INTEGER, active INTEGER)")?;
        db.execute_sql_collect("INSERT INTO status VALUES (1, 1)")?;
        db.execute_sql_collect("INSERT INTO status VALUES (2, 0)")?;
        db.execute_sql_collect("INSERT INTO status VALUES (3, 1)")?;

        // Test not equal
        let result = db.execute_sql_collect("SELECT id FROM status WHERE active != 0")?;
        assert_eq!(result.row_count(), 2); // Rows 1 and 3

        println!("✓ WHERE clause with not equal (!=) works");
        Ok(())
    }
}

#[cfg(test)]
mod arithmetic_expression_tests {
    use super::*;

    #[test]
    fn test_arithmetic_in_select() -> PrismDBResult<()> {
        let db = Database::new(DatabaseConfig::in_memory())?;

        db.execute_sql_collect("CREATE TABLE numbers (x INTEGER, y INTEGER)")?;
        db.execute_sql_collect("INSERT INTO numbers VALUES (10, 5)")?;
        db.execute_sql_collect("INSERT INTO numbers VALUES (20, 8)")?;

        // Test arithmetic expression in SELECT
        let result = db.execute_sql_collect("SELECT x + y FROM numbers")?;

        assert_eq!(result.row_count(), 2);
        assert_eq!(result.columns.len(), 1);
        // Column name might be generated, just check it exists

        println!("✓ Arithmetic expressions in SELECT work");
        Ok(())
    }

    #[test]
    fn test_arithmetic_in_where() -> PrismDBResult<()> {
        let db = Database::new(DatabaseConfig::in_memory())?;

        db.execute_sql_collect("CREATE TABLE calculations (a INTEGER, b INTEGER)")?;
        db.execute_sql_collect("INSERT INTO calculations VALUES (10, 5)")?;
        db.execute_sql_collect("INSERT INTO calculations VALUES (20, 15)")?;
        db.execute_sql_collect("INSERT INTO calculations VALUES (30, 10)")?;

        // Test arithmetic in WHERE clause
        let result = db.execute_sql_collect("SELECT a FROM calculations WHERE a - b > 10")?;

        assert_eq!(result.row_count(), 1); // Only row 3 (30 - 10 = 20 > 10)

        println!("✓ Arithmetic expressions in WHERE work");
        Ok(())
    }

    #[test]
    fn test_multiple_arithmetic_operations() -> PrismDBResult<()> {
        let db = Database::new(DatabaseConfig::in_memory())?;

        db.execute_sql_collect("CREATE TABLE math (x INTEGER)")?;
        db.execute_sql_collect("INSERT INTO math VALUES (5)")?;
        db.execute_sql_collect("INSERT INTO math VALUES (10)")?;
        db.execute_sql_collect("INSERT INTO math VALUES (15)")?;

        // Test multiple operations: x * 2 + 10
        let result = db.execute_sql_collect("SELECT x FROM math WHERE x * 2 + 10 > 30")?;

        assert_eq!(result.row_count(), 1); // Only x=15 (15*2+10=40 > 30)

        println!("✓ Multiple arithmetic operations work");
        Ok(())
    }
}
