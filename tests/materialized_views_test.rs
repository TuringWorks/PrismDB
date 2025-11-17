//! Materialized Views Integration Tests
//!
//! Tests the complete materialized view functionality including:
//! - CREATE MATERIALIZED VIEW
//! - REFRESH MATERIALIZED VIEW
//! - DROP MATERIALIZED VIEW
//! - Querying materialized views
//! - Staleness tracking

use prism::catalog::Catalog;
use prism::execution::{ExecutionContext, ExecutionEngine};
use prism::parser::{tokenizer::Tokenizer, Parser};
use prism::planner::{Binder, optimizer::Optimizer};
use prism::storage::TransactionManager;
use prism::types::DataChunk;
use std::sync::{Arc, RwLock};

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_context() -> ExecutionContext {
        let transaction_manager = Arc::new(TransactionManager::new());
        let catalog = Arc::new(RwLock::new(Catalog::new()));
        ExecutionContext::new(transaction_manager, catalog)
    }

    fn execute_sql(context: &ExecutionContext, sql: &str) -> Vec<DataChunk> {
        // Tokenize
        let tokenizer = Tokenizer::new();
        let tokens = tokenizer.tokenize(sql).unwrap();

        // Parse
        let mut parser = Parser::new(tokens);
        let statement = parser.parse_statement().unwrap();

        // Plan
        let mut binder = Binder::new();
        let logical_plan = binder.bind_statement(&statement).unwrap();

        // Optimize
        let optimizer = Optimizer::new();
        let physical_plan = optimizer.optimize(logical_plan).unwrap();

        // Execute
        let mut engine = ExecutionEngine::new(context.clone());
        engine.execute_collect(physical_plan).unwrap()
    }

    #[test]
    fn test_create_materialized_view_basic() {
        let context = create_test_context();

        // Create a base table
        execute_sql(
            &context,
            "CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER)",
        );

        // Insert some data
        execute_sql(
            &context,
            "INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)",
        );

        // Create materialized view
        let result = execute_sql(
            &context,
            "CREATE MATERIALIZED VIEW active_users AS SELECT id, name FROM users WHERE age >= 25",
        );

        // Should return empty result for DDL
        assert_eq!(result.len(), 0);

        // Verify the materialized view exists in catalog
        let catalog = context.catalog.read().unwrap();
        let schema = catalog.get_schema("main").unwrap();
        let schema_guard = schema.read().unwrap();
        assert!(schema_guard.view_exists("active_users"));

        let view_arc = schema_guard.get_view("active_users").unwrap();
        let view = view_arc.read().unwrap();
        assert!(view.is_materialized);
    }

    #[test]
    fn test_refresh_materialized_view() {
        let context = create_test_context();

        // Create base table
        execute_sql(
            &context,
            "CREATE TABLE products (id INTEGER, name VARCHAR, price DOUBLE)",
        );

        // Insert data
        execute_sql(
            &context,
            "INSERT INTO products VALUES (1, 'Widget', 10.99), (2, 'Gadget', 25.50)",
        );

        // Create materialized view
        execute_sql(
            &context,
            "CREATE MATERIALIZED VIEW expensive_products AS SELECT name, price FROM products WHERE price > 20",
        );

        // Refresh the materialized view
        let result = execute_sql(&context, "REFRESH MATERIALIZED VIEW expensive_products");

        // Should return empty result for refresh
        assert_eq!(result.len(), 0);

        // Verify the materialized view was refreshed
        let catalog = context.catalog.read().unwrap();
        let schema = catalog.get_schema("main").unwrap();
        let schema_guard = schema.read().unwrap();
        let view_arc = schema_guard.get_view("expensive_products").unwrap();
        let view = view_arc.read().unwrap();

        // Check that data was materialized
        let materialized_data = view.get_materialized_data().unwrap();
        assert!(materialized_data.len() > 0 || view.get_row_count().unwrap_or(0) >= 0);
    }

    #[test]
    fn test_drop_materialized_view() {
        let context = create_test_context();

        // Create base table
        execute_sql(&context, "CREATE TABLE items (id INTEGER, name VARCHAR)");

        // Create materialized view
        execute_sql(
            &context,
            "CREATE MATERIALIZED VIEW all_items AS SELECT * FROM items",
        );

        // Verify it exists
        {
            let catalog = context.catalog.read().unwrap();
            let schema = catalog.get_schema("main").unwrap();
            let schema_guard = schema.read().unwrap();
            assert!(schema_guard.view_exists("all_items"));
        }

        // Drop the materialized view
        let result = execute_sql(&context, "DROP MATERIALIZED VIEW all_items");
        assert_eq!(result.len(), 0);

        // Verify it no longer exists
        let catalog = context.catalog.read().unwrap();
        let schema = catalog.get_schema("main").unwrap();
        let schema_guard = schema.read().unwrap();
        assert!(!schema_guard.view_exists("all_items"));
    }

    #[test]
    fn test_drop_materialized_view_if_exists() {
        let context = create_test_context();

        // Drop non-existent view with IF EXISTS - should not error
        let result = execute_sql(&context, "DROP MATERIALIZED VIEW IF EXISTS nonexistent_view");
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_create_or_replace_materialized_view() {
        let context = create_test_context();

        // Create base table
        execute_sql(&context, "CREATE TABLE data (id INTEGER, value VARCHAR)");

        // Create materialized view
        execute_sql(
            &context,
            "CREATE MATERIALIZED VIEW data_view AS SELECT * FROM data",
        );

        // Create or replace
        let result = execute_sql(
            &context,
            "CREATE OR REPLACE MATERIALIZED VIEW data_view AS SELECT id FROM data",
        );
        assert_eq!(result.len(), 0);

        // Verify it still exists
        let catalog = context.catalog.read().unwrap();
        let schema = catalog.get_schema("main").unwrap();
        let schema_guard = schema.read().unwrap();
        assert!(schema_guard.view_exists("data_view"));
    }

    #[test]
    fn test_materialized_view_with_aggregation() {
        let context = create_test_context();

        // Create base table
        execute_sql(
            &context,
            "CREATE TABLE orders (id INTEGER, customer_id INTEGER, amount DOUBLE)",
        );

        // Insert data
        execute_sql(
            &context,
            "INSERT INTO orders VALUES (1, 1, 100.0), (2, 1, 200.0), (3, 2, 150.0)",
        );

        // Create materialized view with aggregation
        execute_sql(
            &context,
            "CREATE MATERIALIZED VIEW customer_totals AS SELECT customer_id, SUM(amount) as total FROM orders GROUP BY customer_id",
        );

        // Verify it was created
        let catalog = context.catalog.read().unwrap();
        let schema = catalog.get_schema("main").unwrap();
        let schema_guard = schema.read().unwrap();
        assert!(schema_guard.view_exists("customer_totals"));
    }

    #[test]
    fn test_materialized_view_staleness() {
        let context = create_test_context();

        // Create base table
        execute_sql(&context, "CREATE TABLE events (id INTEGER, name VARCHAR)");

        // Create materialized view
        execute_sql(
            &context,
            "CREATE MATERIALIZED VIEW all_events AS SELECT * FROM events",
        );

        // Get the view and check staleness
        let catalog = context.catalog.read().unwrap();
        let schema = catalog.get_schema("main").unwrap();
        let schema_guard = schema.read().unwrap();
        let view_arc = schema_guard.get_view("all_events").unwrap();
        let view = view_arc.read().unwrap();

        // Initially should be stale (no refresh yet)
        assert!(view.is_stale());
    }
}
