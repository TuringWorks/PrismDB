use prismdb::*;

fn main() -> PrismDBResult<()> {
    let mut db = Database::new_in_memory()?;

    db.execute("CREATE TABLE employees (id INTEGER, name VARCHAR, dept_id INTEGER, salary INTEGER)")?;
    db.execute("INSERT INTO employees VALUES (1, 'Alice', 1, 80000)")?;
    db.execute("INSERT INTO employees VALUES (2, 'Bob', 1, 90000)")?;
    db.execute("INSERT INTO employees VALUES (3, 'Charlie', 2, 70000)")?;
    db.execute("INSERT INTO employees VALUES (4, 'Diana', 2, 85000)")?;
    db.execute("INSERT INTO employees VALUES (5, 'Eve', 3, 95000)")?;

    // First, check the CTE results
    println!("=== CTE dept_stats ===");
    let result = db.execute("
        WITH dept_stats AS (
            SELECT dept_id, COUNT(*) as emp_count, AVG(salary) as avg_salary
            FROM employees
            GROUP BY dept_id
        )
        SELECT * FROM dept_stats ORDER BY dept_id
    ")?;
    let collected = result.collect()?;
    println!("dept_stats rows: {}", collected.rows.len());
    for (i, row) in collected.rows.iter().enumerate() {
        println!("Row {}: dept_id={:?}, emp_count={:?}, avg_salary={:?}",
                 i, row[0], row[1], row[2]);
    }

    // Now check with WHERE filter
    println!("\n=== With WHERE avg_salary > 75000 ===");
    let result = db.execute("
        WITH dept_stats AS (
            SELECT dept_id, COUNT(*) as emp_count, AVG(salary) as avg_salary
            FROM employees
            GROUP BY dept_id
        )
        SELECT dept_id, emp_count, avg_salary FROM dept_stats WHERE avg_salary > 75000 ORDER BY dept_id
    ")?;
    let collected = result.collect()?;
    println!("Filtered rows: {}", collected.rows.len());
    for (i, row) in collected.rows.iter().enumerate() {
        println!("Row {}: dept_id={:?}, emp_count={:?}, avg_salary={:?}",
                 i, row[0], row[1], row[2]);
    }

    Ok(())
}
