use prismdb::{Database, PrismDBResult, Value};

fn main() -> PrismDBResult<()> {
    let mut db = Database::new_in_memory()?;

    println!("Creating table...");
    db.execute("CREATE TABLE test (name VARCHAR)")?;

    println!("\nInserting data...");
    db.execute("INSERT INTO test VALUES ('Alice')")?;
    db.execute("INSERT INTO test VALUES ('Bob')")?;

    println!("\nQuerying data...");
    let result = db.execute("SELECT * FROM test")?;
    let collected = result.collect()?;

    println!("Row count: {}", collected.rows.len());
    for (i, row) in collected.rows.iter().enumerate() {
        println!("Row {}: {:?}", i, row);
        if let Some(Value::Varchar(s)) = row.get(0) {
            println!("  String value: '{}' (length: {})", s, s.len());
        }
    }

    Ok(())
}
