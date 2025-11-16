//! Integration tests for the query planner

#[cfg(test)]
mod tests {
    use crate::common::error::PrismDBResult;
    use crate::parser::parse_sql;
    use crate::planner::{LogicalPlan, QueryPlanner};

    #[test]
    fn test_simple_select_planning() -> PrismDBResult<()> {
        let sql = "SELECT id, name FROM users";
        let statement = parse_sql(sql)?;

        let mut planner = QueryPlanner::new();
        let logical_plan = planner.plan_statement(&statement)?;

        // Verify the plan structure
        match logical_plan {
            LogicalPlan::Projection(proj) => {
                assert_eq!(proj.expressions.len(), 2);
                // Should have a table scan as input
                match *proj.input {
                    LogicalPlan::TableScan(scan) => {
                        assert_eq!(scan.table_name, "users");
                    }
                    _ => panic!("Expected TableScan as input to Projection"),
                }
            }
            _ => panic!("Expected Projection as root plan node"),
        }

        Ok(())
    }

    #[test]
    fn test_select_with_where() -> PrismDBResult<()> {
        let sql = "SELECT id FROM users WHERE id > 10";
        let statement = parse_sql(sql)?;

        let mut planner = QueryPlanner::new();
        let logical_plan = planner.plan_statement(&statement)?;

        // Verify the plan structure: Projection -> Filter -> TableScan
        match logical_plan {
            LogicalPlan::Projection(proj) => match *proj.input {
                LogicalPlan::Filter(filter) => match *filter.input {
                    LogicalPlan::TableScan(scan) => {
                        assert_eq!(scan.table_name, "users");
                    }
                    _ => panic!("Expected TableScan as input to Filter"),
                },
                _ => panic!("Expected Filter as input to Projection"),
            },
            _ => panic!("Expected Projection as root plan node"),
        }

        Ok(())
    }

    #[test]
    fn test_select_with_order_by() -> PrismDBResult<()> {
        let sql = "SELECT name FROM users ORDER BY name DESC";
        let statement = parse_sql(sql)?;

        let mut planner = QueryPlanner::new();
        let logical_plan = planner.plan_statement(&statement)?;

        // Verify the plan structure: Projection -> Sort -> TableScan
        match logical_plan {
            LogicalPlan::Projection(proj) => {
                match *proj.input {
                    LogicalPlan::Sort(sort) => {
                        assert_eq!(sort.expressions.len(), 1);
                        assert!(!sort.expressions[0].ascending); // DESC
                        match *sort.input {
                            LogicalPlan::TableScan(scan) => {
                                assert_eq!(scan.table_name, "users");
                            }
                            _ => panic!("Expected TableScan as input to Sort"),
                        }
                    }
                    _ => panic!("Expected Sort as input to Projection"),
                }
            }
            _ => panic!("Expected Projection as root plan node"),
        }

        Ok(())
    }

    #[test]
    fn test_select_with_limit() -> PrismDBResult<()> {
        let sql = "SELECT id FROM users LIMIT 10";
        let statement = parse_sql(sql)?;

        let mut planner = QueryPlanner::new();
        let logical_plan = planner.plan_statement(&statement)?;

        // Verify the plan structure: Projection -> Limit -> TableScan
        match logical_plan {
            LogicalPlan::Projection(proj) => match *proj.input {
                LogicalPlan::Limit(limit) => {
                    assert_eq!(limit.limit, 10);
                    assert_eq!(limit.offset, 0);
                    match *limit.input {
                        LogicalPlan::TableScan(scan) => {
                            assert_eq!(scan.table_name, "users");
                        }
                        _ => panic!("Expected TableScan as input to Limit"),
                    }
                }
                _ => panic!("Expected Limit as input to Projection"),
            },
            _ => panic!("Expected Projection as root plan node"),
        }

        Ok(())
    }

    #[test]
    fn test_create_table_planning() -> PrismDBResult<()> {
        let sql = "CREATE TABLE users (id INTEGER, name TEXT)";
        let statement = parse_sql(sql)?;

        let mut planner = QueryPlanner::new();
        let logical_plan = planner.plan_statement(&statement)?;

        // Verify the plan structure
        match logical_plan {
            LogicalPlan::CreateTable(create) => {
                assert_eq!(create.table_name, "users");
                assert_eq!(create.schema.len(), 2);
                assert_eq!(create.schema[0].name, "id");
                assert_eq!(create.schema[1].name, "name");
            }
            _ => panic!("Expected CreateTable as plan node"),
        }

        Ok(())
    }

    #[test]
    fn test_drop_table_planning() -> PrismDBResult<()> {
        let sql = "DROP TABLE users";
        let statement = parse_sql(sql)?;

        let mut planner = QueryPlanner::new();
        let logical_plan = planner.plan_statement(&statement)?;

        // Verify the plan structure
        match logical_plan {
            LogicalPlan::DropTable(drop) => {
                assert_eq!(drop.table_name, "users");
                assert!(!drop.if_exists);
            }
            _ => panic!("Expected DropTable as plan node"),
        }

        Ok(())
    }

    #[test]
    fn test_explain_planning() -> PrismDBResult<()> {
        let sql = "EXPLAIN SELECT id FROM users";
        let statement = parse_sql(sql)?;

        let mut planner = QueryPlanner::new();
        let logical_plan = planner.plan_statement(&statement)?;

        // Verify the plan structure
        match logical_plan {
            LogicalPlan::Explain(explain) => {
                assert!(!explain.analyze);
                assert!(!explain.verbose);
                // Should contain a SELECT statement as input
                match *explain.input {
                    LogicalPlan::Projection(_) => {
                        // Expected
                    }
                    _ => panic!("Expected Projection as input to Explain"),
                }
            }
            _ => panic!("Expected Explain as plan node"),
        }

        Ok(())
    }
}
