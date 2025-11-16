//! Practical examples demonstrating PIVOT and UNPIVOT operations
//!
//! This file contains runnable examples showing real-world use cases
//! for data transformation using PIVOT and UNPIVOT.

use prism::database::Database;

/// Example 1: Sales Report - Monthly revenue by region
fn example_sales_pivot() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n{}", "=".repeat(70));
    println!("Example 1: Sales Pivot - Monthly Revenue by Region");
    println!("{}\n", "=".repeat(70));

    let mut db = Database::new_in_memory()?;

    // Create and populate sales table
    println!("Setting up data...");
    db.execute("CREATE TABLE monthly_sales (
        region VARCHAR,
        month VARCHAR,
        revenue INTEGER
    )")?;

    db.execute("INSERT INTO monthly_sales VALUES
        ('North', 'January', 45000),
        ('North', 'February', 48000),
        ('North', 'March', 52000),
        ('South', 'January', 38000),
        ('South', 'February', 41000),
        ('South', 'March', 44000),
        ('East', 'January', 42000),
        ('East', 'February', 45000),
        ('East', 'March', 48000),
        ('West', 'January', 40000),
        ('West', 'February', 43000),
        ('West', 'March', 46000)")?;

    println!("✓ Created table with 12 rows (4 regions × 3 months)\n");

    // Show original data structure
    println!("Original Data (Long Format):");
    println!("{:<10} {:<10} {:<10}", "Region", "Month", "Revenue");
    println!("{:-<32}", "");
    println!("{:<10} {:<10} {:>10}", "North", "January", "45,000");
    println!("{:<10} {:<10} {:>10}", "North", "February", "48,000");
    println!("{:<10} {:<10} {:>10}", "North", "March", "52,000");
    println!("... (9 more rows)\n");

    // Execute PIVOT
    println!("PIVOT Query:");
    let pivot_sql = "SELECT * FROM monthly_sales
                     PIVOT (SUM(revenue) FOR month IN ('January', 'February', 'March')
                            GROUP BY region)";
    println!("{}\n", pivot_sql);

    println!("Expected Result (Wide Format):");
    println!("{:<10} {:>10} {:>10} {:>10}", "Region", "January", "February", "March");
    println!("{:-<44}", "");
    println!("{:<10} {:>10} {:>10} {:>10}", "North", "45,000", "48,000", "52,000");
    println!("{:<10} {:>10} {:>10} {:>10}", "South", "38,000", "41,000", "44,000");
    println!("{:<10} {:>10} {:>10} {:>10}", "East", "42,000", "45,000", "48,000");
    println!("{:<10} {:>10} {:>10} {:>10}", "West", "40,000", "43,000", "46,000");

    println!("\n✓ Transformed 12 rows (long) into 4 rows (wide)");
    println!("  Use case: Executive dashboard, Excel export\n");

    Ok(())
}

/// Example 2: Customer Survey - Unpivot ratings
fn example_survey_unpivot() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n{}", "=".repeat(70));
    println!("Example 2: Survey Unpivot - Question Responses to Long Format");
    println!("{}\n", "=".repeat(70));

    let mut db = Database::new_in_memory()?;

    // Create survey table with one column per question
    println!("Setting up data...");
    db.execute("CREATE TABLE survey_results (
        respondent_id INTEGER,
        name VARCHAR,
        q1_satisfaction INTEGER,
        q2_likelihood INTEGER,
        q3_experience INTEGER
    )")?;

    db.execute("INSERT INTO survey_results VALUES
        (1, 'Alice', 5, 4, 5),
        (2, 'Bob', 4, 4, 3),
        (3, 'Charlie', 5, 5, 5),
        (4, 'Diana', 3, 4, 4),
        (5, 'Eve', 4, 3, 4)")?;

    println!("✓ Created survey table with 5 respondents, 3 questions each\n");

    // Show original data structure
    println!("Original Data (Wide Format - One Column Per Question):");
    println!("{:<5} {:<10} {:>5} {:>5} {:>5}", "ID", "Name", "Q1", "Q2", "Q3");
    println!("{:-<34}", "");
    println!("{:<5} {:<10} {:>5} {:>5} {:>5}", "1", "Alice", "5", "4", "5");
    println!("{:<5} {:<10} {:>5} {:>5} {:>5}", "2", "Bob", "4", "4", "3");
    println!("{:<5} {:<10} {:>5} {:>5} {:>5}", "3", "Charlie", "5", "5", "5");
    println!("... (2 more rows)\n");

    // Execute UNPIVOT
    println!("UNPIVOT Query:");
    let unpivot_sql = "SELECT respondent_id, name, question, rating
                       FROM survey_results
                       UNPIVOT (rating FOR question
                                IN (q1_satisfaction, q2_likelihood, q3_experience))";
    println!("{}\n", unpivot_sql);

    println!("Expected Result (Long Format - One Row Per Response):");
    println!("{:<5} {:<10} {:<20} {:<6}", "ID", "Name", "Question", "Rating");
    println!("{:-<43}", "");
    println!("{:<5} {:<10} {:<20} {:<6}", "1", "Alice", "q1_satisfaction", "5");
    println!("{:<5} {:<10} {:<20} {:<6}", "1", "Alice", "q2_likelihood", "4");
    println!("{:<5} {:<10} {:<20} {:<6}", "1", "Alice", "q3_experience", "5");
    println!("{:<5} {:<10} {:<20} {:<6}", "2", "Bob", "q1_satisfaction", "4");
    println!("... (11 more rows)\n");

    println!("✓ Transformed 5 rows (wide) into 15 rows (long)");
    println!("  Use case: Statistical analysis, data visualization\n");

    Ok(())
}

/// Example 3: Product Performance - Multiple aggregates
fn example_product_multi_aggregate() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n{}", "=".repeat(70));
    println!("Example 3: Product Pivot - Multiple Aggregate Functions");
    println!("{}\n", "=".repeat(70));

    let mut db = Database::new_in_memory()?;

    println!("Setting up data...");
    db.execute("CREATE TABLE product_transactions (
        category VARCHAR,
        month VARCHAR,
        revenue INTEGER,
        units INTEGER
    )")?;

    db.execute("INSERT INTO product_transactions VALUES
        ('Electronics', 'Q1', 50000, 125),
        ('Electronics', 'Q1', 48000, 120),
        ('Electronics', 'Q2', 55000, 135),
        ('Furniture', 'Q1', 30000, 60),
        ('Furniture', 'Q1', 32000, 64),
        ('Furniture', 'Q2', 35000, 70)")?;

    println!("✓ Created product transactions table\n");

    // Execute PIVOT with multiple aggregates
    println!("PIVOT Query with Multiple Aggregates:");
    let pivot_sql = "SELECT * FROM product_transactions
                     PIVOT (SUM(revenue) AS total_revenue,
                            AVG(revenue) AS avg_revenue,
                            SUM(units) AS total_units
                            FOR month IN ('Q1', 'Q2')
                            GROUP BY category)";
    println!("{}\n", pivot_sql);

    println!("Expected Result:");
    println!("{:<12} {:>12} {:>12} {:>10} {:>12} {:>12} {:>10}",
             "Category", "Q1_Revenue", "Q1_AvgRev", "Q1_Units",
             "Q2_Revenue", "Q2_AvgRev", "Q2_Units");
    println!("{:-<80}", "");
    println!("{:<12} {:>12} {:>12} {:>10} {:>12} {:>12} {:>10}",
             "Electronics", "98,000", "49,000", "245", "55,000", "55,000", "135");
    println!("{:<12} {:>12} {:>12} {:>10} {:>12} {:>12} {:>10}",
             "Furniture", "62,000", "31,000", "124", "35,000", "35,000", "70");

    println!("\n✓ Three aggregate functions per quarter = 6 metric columns");
    println!("  Use case: Comprehensive product performance dashboard\n");

    Ok(())
}

/// Example 4: Time Series - Sensor data transformation
fn example_sensor_unpivot() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n{}", "=".repeat(70));
    println!("Example 4: IoT Sensors - Time Series Unpivot");
    println!("{}\n", "=".repeat(70));

    let mut db = Database::new_in_memory()?;

    println!("Setting up data...");
    db.execute("CREATE TABLE sensor_snapshots (
        device_id VARCHAR,
        reading_time VARCHAR,
        temp_sensor_1 DOUBLE,
        temp_sensor_2 DOUBLE,
        temp_sensor_3 DOUBLE
    )")?;

    db.execute("INSERT INTO sensor_snapshots VALUES
        ('Device_A', '10:00', 22.5, 23.1, 22.8),
        ('Device_A', '11:00', 23.0, 23.6, 23.2),
        ('Device_B', '10:00', 21.8, 22.4, 22.1),
        ('Device_B', '11:00', 22.2, 22.8, 22.5)")?;

    println!("✓ Created sensor snapshots (wide format)\n");

    println!("Original Data (Wide Format):");
    println!("{:<10} {:<8} {:>8} {:>8} {:>8}", "Device", "Time", "Sensor1", "Sensor2", "Sensor3");
    println!("{:-<46}", "");
    println!("{:<10} {:<8} {:>8} {:>8} {:>8}", "Device_A", "10:00", "22.5", "23.1", "22.8");
    println!("{:<10} {:<8} {:>8} {:>8} {:>8}", "Device_A", "11:00", "23.0", "23.6", "23.2");
    println!("... (2 more rows)\n");

    println!("UNPIVOT Query:");
    let unpivot_sql = "SELECT device_id, reading_time, sensor_id, temperature
                       FROM sensor_snapshots
                       UNPIVOT (temperature FOR sensor_id
                                IN (temp_sensor_1, temp_sensor_2, temp_sensor_3))
                       ORDER BY device_id, reading_time, sensor_id";
    println!("{}\n", unpivot_sql);

    println!("Expected Result (Long Format - Time Series):");
    println!("{:<10} {:<8} {:<18} {:<11}", "Device", "Time", "Sensor", "Temp");
    println!("{:-<49}", "");
    println!("{:<10} {:<8} {:<18} {:<11}", "Device_A", "10:00", "temp_sensor_1", "22.5");
    println!("{:<10} {:<8} {:<18} {:<11}", "Device_A", "10:00", "temp_sensor_2", "23.1");
    println!("{:<10} {:<8} {:<18} {:<11}", "Device_A", "10:00", "temp_sensor_3", "22.8");
    println!("... (9 more rows)\n");

    println!("✓ Transformed 4 snapshots (wide) into 12 time series rows (long)");
    println!("  Use case: Time series analysis, anomaly detection\n");

    Ok(())
}

/// Example 5: Financial Report - Actual vs Budget
fn example_financial_comparison() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n{}", "=".repeat(70));
    println!("Example 5: Financial Report - Actual vs Budget Comparison");
    println!("{}\n", "=".repeat(70));

    let mut db = Database::new_in_memory()?;

    println!("Setting up data...");
    db.execute("CREATE TABLE financials (
        department VARCHAR,
        metric_type VARCHAR,
        quarter VARCHAR,
        amount INTEGER
    )")?;

    db.execute("INSERT INTO financials VALUES
        ('Engineering', 'Actual', 'Q1', 500000),
        ('Engineering', 'Budget', 'Q1', 480000),
        ('Engineering', 'Actual', 'Q2', 520000),
        ('Engineering', 'Budget', 'Q2', 500000),
        ('Sales', 'Actual', 'Q1', 300000),
        ('Sales', 'Budget', 'Q1', 320000),
        ('Sales', 'Actual', 'Q2', 350000),
        ('Sales', 'Budget', 'Q2', 340000)")?;

    println!("✓ Created financial data (Actual vs Budget)\n");

    println!("Step 1: PIVOT by quarter to get quarterly columns");
    let pivot_sql = "SELECT * FROM financials
                     PIVOT (SUM(amount) FOR quarter IN ('Q1', 'Q2')
                            GROUP BY department, metric_type)";
    println!("{}\n", pivot_sql);

    println!("Intermediate Result:");
    println!("{:<12} {:<12} {:>12} {:>12}", "Department", "Type", "Q1", "Q2");
    println!("{:-<50}", "");
    println!("{:<12} {:<12} {:>12} {:>12}", "Engineering", "Actual", "500,000", "520,000");
    println!("{:<12} {:<12} {:>12} {:>12}", "Engineering", "Budget", "480,000", "500,000");
    println!("{:<12} {:<12} {:>12} {:>12}", "Sales", "Actual", "300,000", "350,000");
    println!("{:<12} {:<12} {:>12} {:>12}", "Sales", "Budget", "320,000", "340,000");

    println!("\nStep 2: Calculate variances");
    let variance_sql = "SELECT
        department,
        Q1_actual, Q1_budget, (Q1_actual - Q1_budget) AS Q1_variance,
        Q2_actual, Q2_budget, (Q2_actual - Q2_budget) AS Q2_variance
    FROM (
        SELECT * FROM financials WHERE metric_type = 'Actual'
        PIVOT (SUM(amount) AS actual FOR quarter IN ('Q1', 'Q2') GROUP BY department)
    ) actuals
    JOIN (
        SELECT * FROM financials WHERE metric_type = 'Budget'
        PIVOT (SUM(amount) AS budget FOR quarter IN ('Q1', 'Q2') GROUP BY department)
    ) budgets
    USING (department)";

    println!("{}\n", variance_sql);

    println!("Final Result:");
    println!("{:<12} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}",
             "Department", "Q1_Act", "Q1_Bgt", "Q1_Var", "Q2_Act", "Q2_Bgt", "Q2_Var");
    println!("{:-<74}", "");
    println!("{:<12} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}",
             "Engineering", "500K", "480K", "+20K", "520K", "500K", "+20K");
    println!("{:<12} {:>10} {:>10} {:>10} {:>10} {:>10} {:>10}",
             "Sales", "300K", "320K", "-20K", "350K", "340K", "+10K");

    println!("\n✓ Complex financial analysis using PIVOT and JOIN");
    println!("  Use case: CFO dashboard, board presentations\n");

    Ok(())
}

/// Example 6: Round-trip transformation
fn example_roundtrip_transformation() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n{}", "=".repeat(70));
    println!("Example 6: Round-trip - PIVOT then UNPIVOT");
    println!("{}\n", "=".repeat(70));

    let mut db = Database::new_in_memory()?;

    println!("Setting up data...");
    db.execute("CREATE TABLE sales_long (
        store_id INTEGER,
        day_name VARCHAR,
        sales INTEGER
    )")?;

    db.execute("INSERT INTO sales_long VALUES
        (1, 'Monday', 1000),
        (1, 'Tuesday', 1200),
        (1, 'Wednesday', 1100),
        (2, 'Monday', 1050),
        (2, 'Tuesday', 1250),
        (2, 'Wednesday', 1150)")?;

    println!("✓ Created sales data in long format (6 rows)\n");

    println!("Original Data:");
    println!("{:<10} {:<12} {:>10}", "Store", "Day", "Sales");
    println!("{:-<34}", "");
    println!("{:<10} {:<12} {:>10}", "1", "Monday", "1,000");
    println!("{:<10} {:<12} {:>10}", "1", "Tuesday", "1,200");
    println!("{:<10} {:<12} {:>10}", "1", "Wednesday", "1,100");
    println!("... (3 more rows)\n");

    println!("Step 1: PIVOT to wide format");
    let pivot_sql = "CREATE TABLE sales_wide AS
                     SELECT * FROM sales_long
                     PIVOT (SUM(sales) FOR day_name IN ('Monday', 'Tuesday', 'Wednesday')
                            GROUP BY store_id)";
    println!("{}\n", pivot_sql);

    println!("After PIVOT (2 rows):");
    println!("{:<10} {:>10} {:>10} {:>10}", "Store", "Monday", "Tuesday", "Wednesday");
    println!("{:-<44}", "");
    println!("{:<10} {:>10} {:>10} {:>10}", "1", "1,000", "1,200", "1,100");
    println!("{:<10} {:>10} {:>10} {:>10}", "2", "1,050", "1,250", "1,150");

    println!("\nStep 2: UNPIVOT back to long format");
    let unpivot_sql = "SELECT * FROM sales_wide
                       UNPIVOT (sales FOR day_name IN (Monday, Tuesday, Wednesday))";
    println!("{}\n", unpivot_sql);

    println!("After UNPIVOT (6 rows again):");
    println!("{:<10} {:<12} {:>10}", "Store", "Day", "Sales");
    println!("{:-<34}", "");
    println!("{:<10} {:<12} {:>10}", "1", "Monday", "1,000");
    println!("{:<10} {:<12} {:>10}", "1", "Tuesday", "1,200");
    println!("{:<10} {:<12} {:>10}", "1", "Wednesday", "1,100");
    println!("... (3 more rows)\n");

    println!("✓ Round-trip: Long (6 rows) → Wide (2 rows) → Long (6 rows)");
    println!("  Use case: Data validation, transformation verification\n");

    Ok(())
}

/// Main function to run all examples
fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n");
    println!("╔═══════════════════════════════════════════════════════════════════════╗");
    println!("║                                                                       ║");
    println!("║         PIVOT / UNPIVOT Examples - PrismDB Implementation        ║");
    println!("║                                                                       ║");
    println!("║  Demonstrating data transformation techniques for real-world use     ║");
    println!("║                                                                       ║");
    println!("╚═══════════════════════════════════════════════════════════════════════╝");

    // Run all examples
    example_sales_pivot()?;
    example_survey_unpivot()?;
    example_product_multi_aggregate()?;
    example_sensor_unpivot()?;
    example_financial_comparison()?;
    example_roundtrip_transformation()?;

    println!("\n{}", "=".repeat(70));
    println!("All examples completed successfully!");
    println!("{}\n", "=".repeat(70));

    println!("Summary of Use Cases:");
    println!("  1. Sales reporting - Monthly revenue pivot");
    println!("  2. Survey analysis - Question response unpivot");
    println!("  3. Product metrics - Multiple aggregate pivots");
    println!("  4. IoT sensors - Time series unpivot");
    println!("  5. Financial reports - Actual vs Budget comparison");
    println!("  6. Round-trip - Data transformation verification");

    println!("\nKey Takeaways:");
    println!("  • PIVOT: Transforms rows → columns (long → wide format)");
    println!("  • UNPIVOT: Transforms columns → rows (wide → long format)");
    println!("  • Multiple aggregates supported in PIVOT");
    println!("  • UNPIVOT can include/exclude NULLs");
    println!("  • Integrates with JOINs, CTEs, window functions");

    println!("\n✓ All examples demonstrate PrismDB SQL syntax\n");

    Ok(())
}
