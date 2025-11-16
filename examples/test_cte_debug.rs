use prismdb::{Database, PrismDBResult};

fn main() -> PrismDBResult<()> {
    let mut db = Database::new_in_memory()?;

    db.execute("CREATE TABLE employees (id INTEGER, name VARCHAR, dept_id INTEGER, salary INTEGER)")?;
    db.execute("INSERT INTO employees VALUES (1, 'Alice', 1, 80000)")?;
    db.execute("INSERT INTO employees VALUES (2, 'Bob', 1, 90000)")?;
    db.execute("INSERT INTO employees VALUES (5, 'Eve', 3, 95000)")?;
    db.execute("INSERT INTO employees VALUES (4, 'Diana', 2, 85000)")?;

    println!("Testing simple query without CTE:");
    let result1 = db.execute("SELECT name, salary FROM employees WHERE salary > 80000 ORDER BY salary")?;
    let collected1 = result1.collect()?;
    println!("Rows: {}", collected1.rows.len());
    for row in &collected1.rows {
        println!("  {:?}", row);
    }

    println!("\nTesting WITH CTE:");
    let result2 = db.execute("
        WITH high_earners AS (
            SELECT name, salary FROM employees WHERE salary > 80000
        )
        SELECT * FROM high_earners ORDER BY salary
    ")?;
    let collected2 = result2.collect()?;
    println!("Rows: {}", collected2.rows.len());
    for row in &collected2.rows {
        println!("  {:?}", row);
    }

    Ok(())
}
