use prism::*;

fn main() -> PrismDBResult<()> {
    let mut db = Database::new_in_memory()?;

    db.execute("CREATE TABLE test (grp INTEGER, val INTEGER)")?;
    db.execute("INSERT INTO test VALUES (1, 10), (1, 20), (2, 100)")?;

    let result = db.execute("SELECT grp, AVG(val) as avg_val FROM test GROUP BY grp ORDER BY grp")?;
    let collected = result.collect()?;

    println!("Results:");
    for (i, row) in collected.rows.iter().enumerate() {
        println!("Row {}: grp={:?}, avg={:?}", i, row[0], row[1]);
    }

    // Group 1: (10 + 20) / 2 = 15.0
    // Group 2: 100 / 1 = 100.0
    // AVG returns Decimal values
    match &collected.rows[0][1] {
        Value::Decimal { value, scale, .. } => {
            let avg = *value as f64 / 10_f64.powi(*scale as i32);
            assert_eq!(avg, 15.0, "Expected AVG of group 1 to be 15.0");
        }
        other => panic!("Expected Decimal, got {:?}", other),
    }
    match &collected.rows[1][1] {
        Value::Decimal { value, scale, .. } => {
            let avg = *value as f64 / 10_f64.powi(*scale as i32);
            assert_eq!(avg, 100.0, "Expected AVG of group 2 to be 100.0");
        }
        other => panic!("Expected Decimal, got {:?}", other),
    }

    println!("\nAVG working correctly!");
    Ok(())
}
