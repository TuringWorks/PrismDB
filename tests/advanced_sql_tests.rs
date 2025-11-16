//! Advanced SQL Features End-to-End Tests
//!
//! Tests for CTEs, Subqueries, and Set Operations

use prism::{Database, PrismDBResult};
use prism::types::Value;

/// Create test database with sample data
fn create_test_db() -> PrismDBResult<Database> {
    let mut db = Database::new_in_memory()?;

    // Create tables
    db.execute("CREATE TABLE employees (id INTEGER, name VARCHAR, dept_id INTEGER, salary INTEGER)")?;
    db.execute("CREATE TABLE departments (id INTEGER, name VARCHAR, budget INTEGER)")?;
    db.execute("CREATE TABLE projects (id INTEGER, name VARCHAR, dept_id INTEGER)")?;

    // Insert test data
    db.execute("INSERT INTO employees VALUES (1, 'Alice', 1, 80000)")?;
    db.execute("INSERT INTO employees VALUES (2, 'Bob', 1, 90000)")?;
    db.execute("INSERT INTO employees VALUES (3, 'Charlie', 2, 70000)")?;
    db.execute("INSERT INTO employees VALUES (4, 'Diana', 2, 85000)")?;
    db.execute("INSERT INTO employees VALUES (5, 'Eve', 3, 95000)")?;

    db.execute("INSERT INTO departments VALUES (1, 'Engineering', 500000)")?;
    db.execute("INSERT INTO departments VALUES (2, 'Sales', 300000)")?;
    db.execute("INSERT INTO departments VALUES (3, 'Marketing', 250000)")?;

    db.execute("INSERT INTO projects VALUES (1, 'Project Alpha', 1)")?;
    db.execute("INSERT INTO projects VALUES (2, 'Project Beta', 1)")?;
    db.execute("INSERT INTO projects VALUES (3, 'Project Gamma', 2)")?;

    Ok(db)
}

// ========== CTE TESTS ==========

#[test]
fn test_simple_cte() -> PrismDBResult<()> {
    let mut db = create_test_db()?;

    let result = db.execute("
        WITH high_earners AS (
            SELECT name, salary FROM employees WHERE salary > 80000
        )
        SELECT * FROM high_earners ORDER BY salary
    ")?;

    let collected = result.collect()?;
    assert_eq!(collected.rows.len(), 3, "Should have 3 high earners");

    // Verify results
    assert_eq!(collected.rows[0][0], Value::varchar("Diana".to_string()));
    assert_eq!(collected.rows[0][1], Value::integer(85000));

    assert_eq!(collected.rows[1][0], Value::varchar("Bob".to_string()));
    assert_eq!(collected.rows[1][1], Value::integer(90000));

    assert_eq!(collected.rows[2][0], Value::varchar("Eve".to_string()));
    assert_eq!(collected.rows[2][1], Value::integer(95000));

    Ok(())
}

#[test]
// TODO: Requires IN subquery support - architectural limitation
// This test uses IN with a subquery, which requires subquery expression execution.
// Subqueries in expressions need Transaction Manager access during planning phase.
// Will be enabled when architecture is refactored to thread Transaction Manager through planner.
fn test_multiple_ctes() -> PrismDBResult<()> {
    let mut db = create_test_db()?;

    let result = db.execute("
        WITH
            eng_dept AS (SELECT id FROM departments WHERE name = 'Engineering'),
            eng_emps AS (SELECT name, salary FROM employees WHERE dept_id IN (SELECT id FROM eng_dept))
        SELECT * FROM eng_emps ORDER BY name
    ")?;

    let collected = result.collect()?;
    assert_eq!(collected.rows.len(), 2, "Should have 2 engineering employees");

    assert_eq!(collected.rows[0][0], Value::varchar("Alice".to_string()));
    assert_eq!(collected.rows[1][0], Value::varchar("Bob".to_string()));

    Ok(())
}

#[test]
fn test_cte_with_aggregation() -> PrismDBResult<()> {
    let mut db = create_test_db()?;

    let result = db.execute("
        WITH dept_stats AS (
            SELECT dept_id, COUNT(*) as emp_count, AVG(salary) as avg_salary
            FROM employees
            GROUP BY dept_id
        )
        SELECT dept_id, emp_count FROM dept_stats WHERE avg_salary > 75000 ORDER BY dept_id
    ")?;

    let collected = result.collect()?;
    // All 3 departments have avg salary > 75000:
    // Dept 1: (80000 + 90000) / 2 = 85000
    // Dept 2: (70000 + 85000) / 2 = 77500
    // Dept 3: 95000 / 1 = 95000
    assert_eq!(collected.rows.len(), 3, "Should have 3 depts with avg salary > 75000");

    assert_eq!(collected.rows[0][0], Value::integer(1)); // Engineering
    assert_eq!(collected.rows[0][1], Value::bigint(2)); // emp_count (BIGINT)

    assert_eq!(collected.rows[1][0], Value::integer(2)); // Sales
    assert_eq!(collected.rows[1][1], Value::bigint(2)); // emp_count (BIGINT)

    assert_eq!(collected.rows[2][0], Value::integer(3)); // Marketing
    assert_eq!(collected.rows[2][1], Value::bigint(1)); // emp_count (BIGINT)

    Ok(())
}

// DISABLED: Architectural limitation with transaction visibility
// TODO: Requires RecursiveCTEOperator for iterative execution
// Recursive CTEs need specialized execution operator that iteratively evaluates:
// 1. Execute anchor (base case)
// 2. Execute recursive term with results from previous iteration
// 3. UNION results and repeat until fixpoint (no new rows)
// Currently hits transaction visibility issue - catalog updates aren't visible
// to new execution engines within the same transaction.
#[test]
#[ignore]
fn test_recursive_cte_numbers() -> PrismDBResult<()> {
    let mut db = Database::new_in_memory()?;

    let result = db.execute("
        WITH RECURSIVE numbers AS (
            SELECT 1 as n
            UNION ALL
            SELECT n + 1 FROM numbers WHERE n < 5
        )
        SELECT * FROM numbers ORDER BY n
    ")?;

    let collected = result.collect()?;
    assert_eq!(collected.rows.len(), 5, "Should generate numbers 1 to 5");

    for i in 0..5 {
        assert_eq!(collected.rows[i][0], Value::integer((i + 1) as i32));
    }

    Ok(())
}

#[test]
// TODO: Requires scalar subquery execution in SELECT list
// This test uses scalar subqueries in the SELECT list, which requires executing
// subqueries during expression evaluation. The SubqueryExpression infrastructure
// exists but needs Transaction Manager access during planning, which is not
// currently available. Will be enabled with architecture refactor.
fn test_cte_used_multiple_times() -> PrismDBResult<()> {
    let mut db = create_test_db()?;

    let result = db.execute("
        WITH high_sal AS (SELECT salary FROM employees WHERE salary > 80000)
        SELECT
            (SELECT COUNT(*) FROM high_sal) as count,
            (SELECT MAX(salary) FROM high_sal) as max_salary,
            (SELECT MIN(salary) FROM high_sal) as min_salary
    ")?;

    let collected = result.collect()?;
    assert_eq!(collected.rows.len(), 1);
    assert_eq!(collected.rows[0][0], Value::bigint(3)); // count (BIGINT per PrismDB spec)
    assert_eq!(collected.rows[0][1], Value::integer(95000)); // max
    assert_eq!(collected.rows[0][2], Value::integer(85000)); // min

    Ok(())
}

// ========== SUBQUERY TESTS ==========

#[test]
// TODO: Requires scalar subquery execution in SELECT and WHERE clauses
// Scalar subqueries in SELECT/WHERE clauses require executing queries during
// expression evaluation. The SubqueryExpression infrastructure exists but needs
// Transaction Manager access during planning phase, which is an architectural
// limitation. Will be enabled when architecture is refactored.
fn test_scalar_subquery() -> PrismDBResult<()> {
    let mut db = create_test_db()?;

    let result = db.execute("
        SELECT name, salary,
               (SELECT AVG(salary) FROM employees) as avg_salary
        FROM employees
        WHERE salary > (SELECT AVG(salary) FROM employees)
        ORDER BY salary
    ")?;

    let collected = result.collect()?;
    assert!(collected.rows.len() >= 2, "Should have employees above average");

    // All returned employees should have above average salary
    let avg_sal = match &collected.rows[0][2] {
        Value::Integer(v) => *v,
        Value::Decimal { value, scale, .. } => {
            // Convert decimal to integer by dividing by scale
            let divisor = 10_i128.pow(*scale as u32);
            (value / divisor) as i32
        }
        _ => panic!("Expected integer or decimal average"),
    };

    for row in &collected.rows {
        let sal = match &row[1] {
            Value::Integer(v) => *v,
            _ => panic!("Expected integer salary"),
        };
        assert!(sal > avg_sal, "Salary should be above average");
    }

    Ok(())
}

#[test]
// TODO: Requires EXISTS subquery support with correlated references
// EXISTS subqueries need to execute during WHERE clause evaluation with access
// to outer query row values (correlation). This requires Transaction Manager
// access during planning and correlated parameter binding, which is not yet
// implemented. Will be enabled with architecture refactor.
fn test_exists_subquery() -> PrismDBResult<()> {
    let mut db = create_test_db()?;

    let result = db.execute("
        SELECT name FROM departments d
        WHERE EXISTS (
            SELECT 1 FROM employees e WHERE e.dept_id = d.id AND e.salary > 85000
        )
        ORDER BY name
    ")?;

    let collected = result.collect()?;
    assert_eq!(collected.rows.len(), 2, "Should have 2 depts with high earners");

    assert_eq!(collected.rows[0][0], Value::varchar("Engineering".to_string()));
    assert_eq!(collected.rows[1][0], Value::varchar("Marketing".to_string()));

    Ok(())
}

#[test]
// TODO: Requires IN subquery execution in WHERE clause
// IN subqueries need to execute during WHERE clause evaluation to build the
// set of values for membership testing. This requires Transaction Manager
// access during planning phase, which is an architectural limitation.
// Will be enabled when architecture is refactored.
fn test_in_subquery() -> PrismDBResult<()> {
    let mut db = create_test_db()?;

    let result = db.execute("
        SELECT name, salary FROM employees
        WHERE dept_id IN (
            SELECT id FROM departments WHERE budget > 250000
        )
        ORDER BY salary DESC
    ")?;

    let collected = result.collect()?;
    assert_eq!(collected.rows.len(), 4, "Should have 4 employees in high-budget depts");

    // First should be highest salary
    assert_eq!(collected.rows[0][0], Value::varchar("Bob".to_string()));
    assert_eq!(collected.rows[0][1], Value::integer(90000));

    Ok(())
}

#[test]
fn test_subquery_in_from_clause() -> PrismDBResult<()> {
    let mut db = create_test_db()?;

    let result = db.execute("
        SELECT dept_id, avg_sal FROM (
            SELECT dept_id, AVG(salary) as avg_sal
            FROM employees
            GROUP BY dept_id
        ) subq
        WHERE avg_sal > 75000
        ORDER BY dept_id
    ")?;

    let collected = result.collect()?;
    // All 3 departments have avg salary > 75000:
    // Dept 1: (80000 + 90000) / 2 = 85000
    // Dept 2: (70000 + 85000) / 2 = 77500
    // Dept 3: 95000 / 1 = 95000
    assert_eq!(collected.rows.len(), 3);

    Ok(())
}

#[test]
// TODO: Requires nested subquery execution (IN + scalar subquery)
// This test combines both IN subquery and scalar subquery (nested), requiring
// multi-level subquery evaluation during planning. Needs Transaction Manager
// access throughout the planning phase, which is an architectural limitation.
// Will be enabled when architecture is refactored.
fn test_nested_subqueries() -> PrismDBResult<()> {
    let mut db = create_test_db()?;

    let result = db.execute("
        SELECT name FROM employees
        WHERE dept_id IN (
            SELECT id FROM departments
            WHERE budget > (
                SELECT AVG(budget) FROM departments
            )
        )
        ORDER BY name
    ")?;

    let collected = result.collect()?;
    assert!(collected.rows.len() >= 2, "Should have employees in high-budget depts");

    Ok(())
}

// ========== SET OPERATION TESTS ==========

#[test]
fn test_union_distinct() -> PrismDBResult<()> {
    let mut db = Database::new_in_memory()?;

    db.execute("CREATE TABLE t1 (id INTEGER)")?;
    db.execute("CREATE TABLE t2 (id INTEGER)")?;

    db.execute("INSERT INTO t1 VALUES (1), (2), (3)")?;
    db.execute("INSERT INTO t2 VALUES (3), (4), (5)")?;

    let result = db.execute("
        SELECT id FROM t1
        UNION
        SELECT id FROM t2
        ORDER BY id
    ")?;

    let collected = result.collect()?;
    assert_eq!(collected.rows.len(), 5, "UNION should remove duplicates");

    for i in 0..5 {
        assert_eq!(collected.rows[i][0], Value::integer((i + 1) as i32));
    }

    Ok(())
}

#[test]
fn test_union_all() -> PrismDBResult<()> {
    let mut db = Database::new_in_memory()?;

    db.execute("CREATE TABLE t1 (id INTEGER)")?;
    db.execute("CREATE TABLE t2 (id INTEGER)")?;

    db.execute("INSERT INTO t1 VALUES (1), (2), (3)")?;
    db.execute("INSERT INTO t2 VALUES (3), (4), (5)")?;

    let result = db.execute("
        SELECT id FROM t1
        UNION ALL
        SELECT id FROM t2
        ORDER BY id
    ")?;

    let collected = result.collect()?;
    assert_eq!(collected.rows.len(), 6, "UNION ALL should keep duplicates");

    Ok(())
}

#[test]
fn test_intersect() -> PrismDBResult<()> {
    let mut db = Database::new_in_memory()?;

    db.execute("CREATE TABLE t1 (id INTEGER)")?;
    db.execute("CREATE TABLE t2 (id INTEGER)")?;

    db.execute("INSERT INTO t1 VALUES (1), (2), (3), (4)")?;
    db.execute("INSERT INTO t2 VALUES (3), (4), (5), (6)")?;

    let result = db.execute("
        SELECT id FROM t1
        INTERSECT
        SELECT id FROM t2
        ORDER BY id
    ")?;

    let collected = result.collect()?;
    assert_eq!(collected.rows.len(), 2, "INTERSECT should return common rows");

    assert_eq!(collected.rows[0][0], Value::integer(3));
    assert_eq!(collected.rows[1][0], Value::integer(4));

    Ok(())
}

#[test]
fn test_except() -> PrismDBResult<()> {
    let mut db = Database::new_in_memory()?;

    db.execute("CREATE TABLE t1 (id INTEGER)")?;
    db.execute("CREATE TABLE t2 (id INTEGER)")?;

    db.execute("INSERT INTO t1 VALUES (1), (2), (3), (4)")?;
    db.execute("INSERT INTO t2 VALUES (3), (4), (5), (6)")?;

    let result = db.execute("
        SELECT id FROM t1
        EXCEPT
        SELECT id FROM t2
        ORDER BY id
    ")?;

    let collected = result.collect()?;
    assert_eq!(collected.rows.len(), 2, "EXCEPT should return difference");

    assert_eq!(collected.rows[0][0], Value::integer(1));
    assert_eq!(collected.rows[1][0], Value::integer(2));

    Ok(())
}

#[test]
fn test_complex_set_operations() -> PrismDBResult<()> {
    let mut db = Database::new_in_memory()?;

    db.execute("CREATE TABLE t1 (id INTEGER)")?;
    db.execute("CREATE TABLE t2 (id INTEGER)")?;
    db.execute("CREATE TABLE t3 (id INTEGER)")?;

    db.execute("INSERT INTO t1 VALUES (1), (2), (3)")?;
    db.execute("INSERT INTO t2 VALUES (2), (3), (4)")?;
    db.execute("INSERT INTO t3 VALUES (3), (4), (5)")?;

    // (t1 UNION t2) INTERSECT t3
    let result = db.execute("
        SELECT id FROM (
            SELECT id FROM t1
            UNION
            SELECT id FROM t2
        ) combined
        INTERSECT
        SELECT id FROM t3
        ORDER BY id
    ")?;

    let collected = result.collect()?;
    assert_eq!(collected.rows.len(), 2); // Should be 3 and 4

    Ok(())
}

// ========== COMBINED FEATURES TESTS ==========

#[test]
fn test_cte_with_set_operations() -> PrismDBResult<()> {
    let mut db = create_test_db()?;

    let result = db.execute("
        WITH
            high_sal AS (SELECT name FROM employees WHERE salary > 85000),
            eng_dept AS (SELECT name FROM employees WHERE dept_id = 1)
        SELECT name FROM high_sal
        INTERSECT
        SELECT name FROM eng_dept
        ORDER BY name
    ")?;

    let collected = result.collect()?;
    assert_eq!(collected.rows.len(), 1, "Should have 1 high-earning engineer");
    assert_eq!(collected.rows[0][0], Value::varchar("Bob".to_string()));

    Ok(())
}

#[test]
// TODO: Requires CTE references in correlated subqueries
// This test requires a correlated scalar subquery that references a CTE.
// The optimizer doesn't have access to CTE context, and subqueries need
// Transaction Manager during planning. Requires both CTE context threading
// through optimizer and architecture refactor for Transaction Manager access.
fn test_cte_with_subqueries() -> PrismDBResult<()> {
    let mut db = create_test_db()?;

    let result = db.execute("
        WITH dept_avg AS (
            SELECT dept_id, AVG(salary) as avg_sal FROM employees GROUP BY dept_id
        )
        SELECT e.name, e.salary
        FROM employees e
        WHERE e.salary > (SELECT avg_sal FROM dept_avg WHERE dept_id = e.dept_id)
        ORDER BY e.name
    ")?;

    let collected = result.collect()?;
    assert!(collected.rows.len() >= 2, "Should have employees above dept average");

    Ok(())
}

#[test]
// TODO: Requires IN subquery with set operations (UNION)
// This test combines IN subquery with UNION set operation inside the subquery.
// Requires executing the set operation during planning to build the membership
// set, which needs Transaction Manager access - an architectural limitation.
// Will be enabled when architecture is refactored.
fn test_subquery_with_set_operations() -> PrismDBResult<()> {
    let mut db = create_test_db()?;

    let result = db.execute("
        SELECT name FROM employees
        WHERE dept_id IN (
            SELECT id FROM departments WHERE budget > 300000
            UNION
            SELECT dept_id FROM projects WHERE name LIKE '%Alpha%'
        )
        ORDER BY name
    ")?;

    let collected = result.collect()?;
    assert!(collected.rows.len() >= 2);

    Ok(())
}
