use prism::common::error::PrismDBResult;
use prism::database::{Database, DatabaseConfig};

fn main() -> PrismDBResult<()> {
    let db = Database::new(DatabaseConfig::in_memory())?;

    // Create table and insert data
    println!("Creating table...");
    db.execute_sql_collect("CREATE TABLE users (id INTEGER, age INTEGER, name VARCHAR)")?;

    println!("Inserting data...");
    db.execute_sql_collect("INSERT INTO users VALUES (1, 25, 'Alice')")?;
    db.execute_sql_collect("INSERT INTO users VALUES (2, 30, 'Bob')")?;
    db.execute_sql_collect("INSERT INTO users VALUES (3, 25, 'Charlie')")?;

    // Test simple SELECT first
    println!("\nTesting SELECT *...");
    let result = db.execute_sql_collect("SELECT * FROM users")?;
    println!(
        "✓ SELECT * returned {} rows (expected 3)",
        result.row_count()
    );
    println!(
        "  Columns: {:?}",
        result.columns.iter().map(|c| &c.name).collect::<Vec<_>>()
    );

    // Test WHERE clause
    println!("\nTesting SELECT with WHERE age = 25...");
    let result = db.execute_sql_collect("SELECT id, name FROM users WHERE age = 25")?;
    println!(
        "Result: {} rows (expected 2 - Alice and Charlie)",
        result.row_count()
    );

    if result.row_count() == 2 {
        println!("✓ WHERE clause filtering works correctly!");
    } else {
        println!(
            "✗ WHERE clause filtering FAILED - got {} rows instead of 2",
            result.row_count()
        );

        // Debug: Check the chunks
        println!("\nDebug info:");
        println!("  Number of chunks: {}", result.chunks().len());
        for (i, chunk) in result.chunks().iter().enumerate() {
            println!(
                "  Chunk {}: {} rows, {} columns",
                i,
                chunk.len(),
                chunk.column_count()
            );
        }
    }

    Ok(())
}
