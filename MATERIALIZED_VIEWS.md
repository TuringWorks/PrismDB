# Materialized Views in PrismDB

## Overview

Materialized views are pre-computed query results stored as physical data. Unlike regular views which execute their query on every access, materialized views cache the results for fast retrieval. This documentation explains how to use materialized views in PrismDB.

## Features

- ✅ **CREATE MATERIALIZED VIEW** - Create views that store query results
- ✅ **REFRESH MATERIALIZED VIEW** - Update materialized data
- ✅ **DROP MATERIALIZED VIEW** - Remove materialized views
- ✅ **Staleness Tracking** - Automatic tracking of data freshness
- ✅ **Multiple Refresh Strategies** - Manual, OnCommit, OnDemand, Incremental
- ✅ **External Data Support** - Materialize data from external sources via IteratorStream
- ✅ **Dependency Tracking** - Track which tables a materialized view depends on

## Basic Usage

### Creating a Materialized View

```sql
-- Basic syntax
CREATE MATERIALIZED VIEW view_name AS
SELECT column1, column2, ...
FROM table_name
WHERE condition;

-- With OR REPLACE
CREATE OR REPLACE MATERIALIZED VIEW active_users AS
SELECT id, name, email
FROM users
WHERE status = 'active';

-- With IF NOT EXISTS
CREATE MATERIALIZED VIEW IF NOT EXISTS user_summary AS
SELECT
    department,
    COUNT(*) as user_count,
    AVG(salary) as avg_salary
FROM users
GROUP BY department;

-- With explicit column names
CREATE MATERIALIZED VIEW top_products (product_name, total_sales) AS
SELECT name, SUM(quantity * price)
FROM products JOIN orders ON products.id = orders.product_id
GROUP BY name
ORDER BY total_sales DESC
LIMIT 10;
```

### Refreshing a Materialized View

```sql
-- Basic refresh
REFRESH MATERIALIZED VIEW active_users;

-- Concurrent refresh (allows reads during refresh)
REFRESH MATERIALIZED VIEW CONCURRENTLY user_summary;
```

### Querying a Materialized View

```sql
-- Query like a regular table
SELECT * FROM active_users WHERE name LIKE 'A%';

-- Join with other tables
SELECT u.name, u.email, s.subscription_type
FROM active_users u
JOIN subscriptions s ON u.id = s.user_id;
```

### Dropping a Materialized View

```sql
-- Basic drop
DROP MATERIALIZED VIEW active_users;

-- With IF EXISTS
DROP MATERIALIZED VIEW IF EXISTS user_summary;
```

## Advanced Features

### Refresh Strategies

```rust
use prism::catalog::view::RefreshStrategy;

// Manual refresh (default) - only refresh when explicitly requested
RefreshStrategy::Manual

// Refresh on commit - automatically refresh when base tables change
RefreshStrategy::OnCommit

// Refresh on demand - lazy refresh on first access after staleness
RefreshStrategy::OnDemand

// Incremental refresh - only update changed data
RefreshStrategy::Incremental
```

### Staleness Tracking

```rust
use prism::catalog::Catalog;

// Check if a materialized view is stale
let catalog = /* get catalog */;
let schema = catalog.get_schema("main")?;
let view = schema.read().unwrap().get_view("active_users")?;
let view_guard = view.read().unwrap();

if view_guard.is_stale() {
    // Refresh needed
    println!("View is stale and needs refresh");
}

// Get last refresh timestamp
if let Some(timestamp) = view_guard.materialized_metadata.as_ref()
    .and_then(|m| m.last_refresh) {
    println!("Last refreshed at: {}", timestamp);
}

// Get row count
if let Some(count) = view_guard.get_row_count() {
    println!("Materialized view has {} rows", count);
}
```

### Materializing External Data

PrismDB supports materializing data from external sources using the `IteratorStream` operator:

```rust
use prism::planner::{PhysicalPlan, PhysicalIteratorStream, PhysicalColumn};
use prism::types::{DataChunk, LogicalType, Value, Vector};

// Create data chunks from external source (e.g., CSV, API, Parquet)
let mut chunks = Vec::new();

// Build a chunk
let mut chunk = DataChunk::with_rows(2);
chunk.set_vector(0, Vector::from_values(&[
    Value::Integer(1),
    Value::Integer(2)
])?)?;
chunk.set_vector(1, Vector::from_values(&[
    Value::Varchar("Alice".to_string()),
    Value::Varchar("Bob".to_string())
])?)?;
chunks.push(chunk);

// Define schema
let schema = vec![
    PhysicalColumn::new("id".to_string(), LogicalType::Integer),
    PhysicalColumn::new("name".to_string(), LogicalType::Varchar),
];

// Create iterator stream
let stream = PhysicalIteratorStream::new(chunks, schema);

// Use in CREATE MATERIALIZED VIEW
// The planner will automatically use IteratorStream for external data
```

## Use Cases

### 1. Expensive Aggregations

```sql
-- Instead of running expensive aggregation every time
SELECT
    customer_id,
    DATE_TRUNC('month', order_date) as month,
    SUM(total_amount) as monthly_total,
    COUNT(*) as order_count
FROM orders
GROUP BY customer_id, month;

-- Create a materialized view
CREATE MATERIALIZED VIEW monthly_customer_sales AS
SELECT
    customer_id,
    DATE_TRUNC('month', order_date) as month,
    SUM(total_amount) as monthly_total,
    COUNT(*) as order_count
FROM orders
GROUP BY customer_id, month;

-- Refresh periodically (e.g., daily)
REFRESH MATERIALIZED VIEW monthly_customer_sales;
```

### 2. Complex Joins

```sql
-- Materialize complex multi-table joins
CREATE MATERIALIZED VIEW enriched_orders AS
SELECT
    o.id,
    o.order_date,
    c.name as customer_name,
    c.email,
    p.name as product_name,
    p.category,
    oi.quantity,
    oi.unit_price
FROM orders o
JOIN customers c ON o.customer_id = c.id
JOIN order_items oi ON o.id = oi.order_id
JOIN products p ON oi.product_id = p.id;
```

### 3. Dashboard Metrics

```sql
-- Pre-compute dashboard metrics
CREATE MATERIALIZED VIEW dashboard_metrics AS
SELECT
    COUNT(DISTINCT customer_id) as total_customers,
    COUNT(*) as total_orders,
    SUM(total_amount) as revenue,
    AVG(total_amount) as avg_order_value,
    MAX(order_date) as last_order_date
FROM orders
WHERE status = 'completed';

-- Refresh every hour or on-demand
REFRESH MATERIALIZED VIEW dashboard_metrics;
```

### 4. Reporting Tables

```sql
-- Create reporting table from multiple sources
CREATE MATERIALIZED VIEW sales_report AS
SELECT
    DATE_TRUNC('day', order_date) as report_date,
    region,
    product_category,
    SUM(quantity) as units_sold,
    SUM(revenue) as total_revenue,
    COUNT(DISTINCT customer_id) as unique_customers
FROM fact_sales
JOIN dim_products ON fact_sales.product_id = dim_products.id
JOIN dim_regions ON fact_sales.region_id = dim_regions.id
GROUP BY report_date, region, product_category;
```

## Performance Benefits

1. **Faster Query Response** - Pre-computed results eliminate query execution time
2. **Reduced CPU Load** - Complex aggregations computed once, not on every query
3. **Consistent Performance** - Predictable query times regardless of data volume
4. **Index Support** - Materialized views can have indexes like regular tables
5. **Join Optimization** - Pre-joined data reduces join overhead

## Best Practices

### 1. Choose Appropriate Refresh Strategy

```sql
-- For near real-time dashboards
CREATE MATERIALIZED VIEW realtime_metrics AS ...;
-- Refresh frequently or use OnCommit strategy

-- For historical reports
CREATE MATERIALIZED VIEW monthly_reports AS ...;
-- Refresh once per month

-- For cached expensive queries
CREATE MATERIALIZED VIEW cached_results AS ...;
-- Refresh on-demand when stale
```

### 2. Monitor Staleness

```rust
// Implement monitoring
fn check_view_freshness(view_name: &str) -> Result<bool> {
    let view = get_view(view_name)?;
    let view_guard = view.read().unwrap();

    if view_guard.is_stale() {
        log::warn!("Materialized view {} is stale", view_name);
        return Ok(false);
    }

    Ok(true)
}
```

### 3. Handle Dependencies

```rust
// When updating base tables, mark dependent views as stale
fn update_table(table_name: &str, data: Vec<DataChunk>) -> Result<()> {
    // Update table
    update_table_data(table_name, data)?;

    // Mark dependent materialized views as stale
    let schema = get_schema("main")?;
    schema.mark_dependent_views_stale(table_name)?;

    Ok(())
}
```

### 4. Incremental Refresh Pattern

```sql
-- Create with timestamp tracking
CREATE MATERIALIZED VIEW incremental_orders AS
SELECT
    *,
    CURRENT_TIMESTAMP as materialized_at
FROM orders;

-- Later, refresh incrementally (pseudo-code)
-- Only update rows modified since last refresh
INSERT INTO incremental_orders
SELECT *, CURRENT_TIMESTAMP
FROM orders
WHERE updated_at > (SELECT MAX(materialized_at) FROM incremental_orders);
```

## Limitations

1. **Manual Refresh Required** - By default, views don't auto-refresh
2. **Storage Overhead** - Materialized views consume disk space
3. **Refresh Cost** - Large views may take time to refresh
4. **Staleness Risk** - Data may be out of date between refreshes

## API Reference

### Catalog Methods

```rust
// Create materialized view
schema.create_materialized_view(
    view_name: &str,
    query: &str,
    column_names: Vec<String>,
    refresh_strategy: RefreshStrategy
) -> PrismDBResult<()>

// Refresh materialized view
schema.refresh_materialized_view(
    view_name: &str,
    data: Vec<DataChunk>
) -> PrismDBResult<()>

// Drop view
schema.drop_view(view_name: &str) -> PrismDBResult<()>

// Mark dependent views stale
schema.mark_dependent_views_stale(
    table_name: &str
) -> PrismDBResult<()>
```

### View Methods

```rust
// Get materialized data
view.get_materialized_data() -> PrismDBResult<&Vec<DataChunk>>

// Check staleness
view.is_stale() -> bool

// Mark as stale
view.mark_stale() -> PrismDBResult<()>

// Refresh
view.refresh(data: Vec<DataChunk>) -> PrismDBResult<()>

// Get refresh strategy
view.get_refresh_strategy() -> Option<&RefreshStrategy>

// Get row count
view.get_row_count() -> Option<usize>

// Add dependency
view.add_dependency(table_name: String) -> PrismDBResult<()>
```

## Examples

See `tests/materialized_views_test.rs` for comprehensive examples and integration tests.

## Future Enhancements / Roadmap

The current implementation provides a solid foundation for materialized views. Here are planned enhancements for future releases:

### 1. Automatic Refresh Scheduling

**Goal**: Enable background refresh jobs without manual intervention.

**Implementation**:
```rust
// Configuration in materialized view definition
CREATE MATERIALIZED VIEW sales_summary AS
SELECT ... FROM orders
WITH (refresh_interval = '1 hour');

// Scheduler service
struct MaterializedViewScheduler {
    refresh_jobs: HashMap<String, RefreshJob>,
}

impl MaterializedViewScheduler {
    fn schedule_refresh(&mut self, view_name: String, interval: Duration) {
        // Create background job
        let job = RefreshJob::new(view_name, interval);
        self.refresh_jobs.insert(view_name, job);
    }

    async fn run_scheduler(&self) {
        // Periodically check and refresh stale views
        loop {
            for (view_name, job) in &self.refresh_jobs {
                if job.should_refresh() {
                    self.refresh_view(view_name).await?;
                }
            }
            sleep(Duration::from_secs(60)).await;
        }
    }
}
```

**Benefits**:
- Zero manual intervention
- Predictable data freshness
- Resource scheduling during off-peak hours

### 2. True Incremental Refresh

**Goal**: Update only changed data instead of full re-computation.

**Implementation**:
```rust
// Track changes using change data capture (CDC)
struct ChangeLog {
    table_name: String,
    operation: Operation, // Insert, Update, Delete
    old_values: Option<Vec<Value>>,
    new_values: Option<Vec<Value>>,
    timestamp: u64,
}

impl MaterializedView {
    fn refresh_incremental(&mut self) -> PrismDBResult<()> {
        // Get changes since last refresh
        let changes = self.get_changes_since(self.last_refresh_time)?;

        // Apply changes incrementally
        for change in changes {
            match change.operation {
                Operation::Insert => self.apply_insert(change.new_values)?,
                Operation::Update => self.apply_update(change.old_values, change.new_values)?,
                Operation::Delete => self.apply_delete(change.old_values)?,
            }
        }

        Ok(())
    }
}
```

**SQL Syntax**:
```sql
-- Enable incremental refresh
CREATE MATERIALIZED VIEW sales_by_region AS
SELECT region, SUM(amount) as total
FROM sales
GROUP BY region
WITH (refresh_strategy = 'incremental');

-- Incremental refresh
REFRESH MATERIALIZED VIEW sales_by_region INCREMENTALLY;
```

**Benefits**:
- Faster refresh times for large datasets
- Lower resource consumption
- More frequent refreshes possible

### 3. Query Rewriting and Automatic Usage

**Goal**: Automatically use materialized views when they can satisfy a query.

**Implementation**:
```rust
struct QueryRewriter {
    catalog: Arc<RwLock<Catalog>>,
}

impl QueryRewriter {
    fn rewrite_query(&self, query: &LogicalPlan) -> PrismDBResult<LogicalPlan> {
        // Find materialized views that can satisfy this query
        let candidate_views = self.find_candidate_views(query)?;

        // Check if view is fresh enough
        for view in candidate_views {
            if self.can_use_view(&view, query)? && !view.is_too_stale() {
                return self.rewrite_to_use_view(query, &view);
            }
        }

        Ok(query)
    }

    fn can_use_view(&self, view: &View, query: &LogicalPlan) -> PrismDBResult<bool> {
        // Check if materialized view query subsumes the user query
        // This requires query containment checking
        Ok(self.query_contained_in(&query, &view.query))
    }
}
```

**Example**:
```sql
-- User writes this query
SELECT region, SUM(amount) FROM sales WHERE year = 2024 GROUP BY region;

-- System automatically rewrites to use materialized view
SELECT region, total FROM sales_by_region_2024 WHERE region IS NOT NULL;
```

**Benefits**:
- Transparent performance improvements
- No application changes needed
- Optimal query execution paths

### 4. Materialized View Indexing

**Goal**: Create indexes on materialized views for faster access.

**Implementation**:
```sql
-- Create index on materialized view
CREATE INDEX idx_sales_region ON sales_summary(region);

-- Composite index
CREATE INDEX idx_sales_region_date ON sales_summary(region, sale_date);

-- Unique index
CREATE UNIQUE INDEX idx_sales_id ON sales_summary(sale_id);
```

**Rust API**:
```rust
impl Schema {
    fn create_materialized_view_index(
        &mut self,
        view_name: &str,
        index_name: &str,
        columns: Vec<String>,
        unique: bool,
    ) -> PrismDBResult<()> {
        // Create B-tree or hash index on materialized data
        let view = self.get_view(view_name)?;
        let view_guard = view.read().unwrap();

        if !view_guard.is_materialized {
            return Err(PrismDBError::Catalog("Not a materialized view".into()));
        }

        // Build index on materialized data
        let index = Index::build_from_chunks(
            index_name,
            view_guard.get_materialized_data()?,
            &columns,
            unique,
        )?;

        self.create_index(&index)?;
        Ok(())
    }
}
```

**Benefits**:
- Point lookups on materialized data
- Range scans optimized
- Join performance improved

### 5. Distributed Materialized Views

**Goal**: Support materialized views across sharded/distributed deployments.

**Implementation**:
```rust
struct DistributedMaterializedView {
    view_name: String,
    shards: Vec<ShardInfo>,
    partitioning_key: String,
}

impl DistributedMaterializedView {
    async fn refresh_distributed(&self) -> PrismDBResult<()> {
        // Refresh each shard in parallel
        let refresh_tasks: Vec<_> = self.shards.iter()
            .map(|shard| async move {
                shard.refresh_materialized_view(&self.view_name).await
            })
            .collect();

        futures::future::try_join_all(refresh_tasks).await?;
        Ok(())
    }

    async fn query_distributed(&self, predicate: &Expression) -> PrismDBResult<Vec<DataChunk>> {
        // Route query to relevant shards
        let target_shards = self.route_query(predicate)?;

        // Parallel query execution
        let query_tasks: Vec<_> = target_shards.iter()
            .map(|shard| async move {
                shard.query_materialized_view(&self.view_name, predicate).await
            })
            .collect();

        let shard_results = futures::future::try_join_all(query_tasks).await?;

        // Merge results
        Ok(self.merge_results(shard_results))
    }
}
```

**SQL Syntax**:
```sql
-- Create distributed materialized view
CREATE MATERIALIZED VIEW global_sales AS
SELECT region, SUM(amount) as total
FROM sales
GROUP BY region
DISTRIBUTED BY HASH(region) SHARDS 4;
```

**Benefits**:
- Scale to massive datasets
- Parallel refresh across shards
- Distributed query execution

### 6. Materialized View Dependencies and Chains

**Goal**: Support materialized views built on other materialized views.

**Implementation**:
```sql
-- Base materialized view
CREATE MATERIALIZED VIEW daily_sales AS
SELECT DATE_TRUNC('day', order_date) as day, SUM(amount) as total
FROM orders
GROUP BY day;

-- Derived materialized view (depends on daily_sales)
CREATE MATERIALIZED VIEW monthly_sales AS
SELECT DATE_TRUNC('month', day) as month, SUM(total) as monthly_total
FROM daily_sales
GROUP BY month;

-- Cascading refresh
REFRESH MATERIALIZED VIEW daily_sales CASCADE;
-- This will also refresh monthly_sales automatically
```

**Dependency Graph**:
```rust
struct ViewDependencyGraph {
    dependencies: HashMap<String, Vec<String>>,
}

impl ViewDependencyGraph {
    fn refresh_cascade(&self, view_name: &str) -> PrismDBResult<Vec<String>> {
        let mut refresh_order = Vec::new();
        let mut visited = HashSet::new();

        // Topological sort to determine refresh order
        self.topological_sort(view_name, &mut visited, &mut refresh_order)?;

        // Refresh in dependency order
        for view in &refresh_order {
            self.refresh_view(view)?;
        }

        Ok(refresh_order)
    }
}
```

**Benefits**:
- Multi-level aggregations
- Simplified ETL pipelines
- Automatic cascade refresh

### 7. Cost-Based Refresh Optimization

**Goal**: Optimize when and how to refresh based on cost analysis.

**Implementation**:
```rust
struct RefreshCostAnalyzer {
    cost_model: CostModel,
}

impl RefreshCostAnalyzer {
    fn should_refresh(&self, view: &View) -> PrismDBResult<bool> {
        // Calculate refresh cost
        let refresh_cost = self.estimate_refresh_cost(view)?;

        // Calculate query cost without materialized view
        let query_cost = self.estimate_query_cost_without_view(view)?;

        // Estimate number of queries before next refresh
        let expected_queries = self.estimate_query_frequency(view)?;

        // Refresh if savings exceed cost
        let savings = query_cost * expected_queries;
        Ok(savings > refresh_cost)
    }

    fn choose_refresh_strategy(&self, view: &View) -> RefreshStrategy {
        let data_change_rate = self.estimate_change_rate(view);
        let query_frequency = self.estimate_query_frequency(view);

        match (data_change_rate, query_frequency) {
            (High, High) => RefreshStrategy::Incremental,
            (Low, High) => RefreshStrategy::OnDemand,
            (High, Low) => RefreshStrategy::Manual,
            (Low, Low) => RefreshStrategy::Manual,
        }
    }
}
```

### 8. External Data Source Integration

**Goal**: Enhanced support for materializing data from various external sources.

**Examples**:
```rust
// From S3 Parquet files
CREATE MATERIALIZED VIEW s3_analytics AS
SELECT * FROM read_parquet('s3://bucket/data/*.parquet')
WHERE timestamp > '2024-01-01';

// From REST API
CREATE MATERIALIZED VIEW api_data AS
SELECT * FROM read_json_auto('https://api.example.com/data')
WITH (refresh_interval = '15 minutes');

// From PostgreSQL
CREATE MATERIALIZED VIEW postgres_sync AS
SELECT * FROM postgres_scan('postgresql://host/db', 'public.users');

// From Kafka stream
CREATE MATERIALIZED VIEW kafka_events AS
SELECT * FROM kafka_scan('localhost:9092', 'events_topic')
WITH (refresh_strategy = 'incremental');
```

## Contributing

Contributions to implement any of these enhancements or improve existing materialized view functionality are welcome!

**Areas needing contribution**:
- Automatic refresh scheduling implementation
- Incremental refresh algorithm development
- Query rewriting and containment checking
- Materialized view indexing
- Distributed materialized views support
- Cost-based optimization models
- External data source connectors

**How to contribute**:
1. Check the [GitHub issues](https://github.com/your-repo/PrismDB/issues) for open tasks
2. Review the implementation patterns in existing code
3. Write comprehensive tests for new features
4. Update this documentation with your changes
5. Submit a pull request with clear description

## License

Same as PrismDB project license.
