# DuckDBRS Architectural Limitations

**Date**: 2025-11-14
**Status**: Critical Issues Identified
**Author**: Investigation Session - CTE and Subquery Implementation Attempt

This document details fundamental architectural limitations discovered during attempts to implement advanced SQL features (CTEs and subqueries) in DuckDBRS.

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Critical Limitations](#critical-limitations)
3. [Impact Analysis](#impact-analysis)
4. [Detailed Findings](#detailed-findings)
5. [Recommended Solutions](#recommended-solutions)

---

## Executive Summary

During implementation of Common Table Expressions (CTEs) and subquery support, several fundamental architectural limitations were identified that prevent full SQL feature parity with DuckDB C++. These limitations are not simple bugs but represent missing foundational components in the execution engine and expression evaluation system.

**Key Finding**: While the parser and binder can correctly process CTEs and subqueries, the execution engine cannot properly materialize intermediate results or evaluate subqueries within expressions.

---

## Critical Limitations

### 1. **Expression Evaluation Context Limitation**

**Location**: `src/expression/expression.rs`, `src/expression/binder.rs`

**Problem**: Expression evaluation has no access to execution context.

**Current Signature**:
```rust
fn evaluate(&self, chunk: &DataChunk) -> DuckDBResult<Vector>;
```

**What's Missing**:
- No access to ExecutionContext
- Cannot execute subqueries during expression evaluation
- Cannot access CTE materialized results
- No support for correlated subqueries

**Impact**:
- Scalar subqueries: ❌ Cannot implement
- EXISTS subqueries: ❌ Cannot implement
- IN subqueries: ❌ Cannot implement
- Correlated subqueries: ❌ Cannot implement

**Evidence**:
```rust
// src/expression/binder.rs:104-110
ast::Expression::Subquery(_subquery) => {
    // Subqueries need to be executed and their results made available
    // This is a complex operation that typically requires integration with the executor
    // For now, return an error indicating they're not yet supported in this context
    Err(DuckDBError::NotImplemented(
        "Scalar subqueries not yet fully implemented in expression context".to_string(),
    ))
}
```

---

### 2. **CTE Materialization Gap**

**Location**: `src/execution/mod.rs`, `src/planner/binder.rs`

**Problem**: No mechanism to materialize and cache CTE results.

**Current Behavior**:
- CTEs parse correctly ✓
- CTEs bind correctly ✓
- Column references resolve correctly ✓
- **BUT**: Execution returns empty data ❌

**Test Evidence**:
```rust
// Test: test_simple_cte
// Expected: Varchar("Diana")
// Actual:   Varchar("")
```

**What's Missing**:
- Dedicated CTE materialization operator
- Result caching mechanism
- Proper data flow between CTE definition and reference
- Column index remapping between logical and physical plans

**Attempted Fixes** (all failed):
1. ✗ Wrapping CTE plans in projections
2. ✗ Creating synthetic TableScans for CTEs
3. ✗ Modifying `update_context_from_plan` logic
4. ✗ Preventing column binding overwrites

**Root Cause**: Data is lost somewhere between:
- CTE definition execution → CTE reference → Final projection

---

### 3. **Column Index Mismatch**

**Location**: `src/planner/binder.rs`, `src/execution/operators.rs`

**Problem**: Column indices in logical plans don't align with physical execution.

**Example**:
```sql
-- CTE selects columns at indices 1, 3 from base table
WITH high_earners AS (
    SELECT name, salary FROM employees  -- employees: [id(0), name(1), dept_id(2), salary(3)]
    WHERE salary > 80000
)
-- But when we SELECT * FROM high_earners, we expect indices 0, 1
SELECT * FROM high_earners;
```

**Issue**:
- Logical plan: CTE outputs columns with original indices (1, 3)
- Physical execution: Expects columns at indices (0, 1)
- No remapping layer exists

**Impact**:
- CTE queries return wrong columns or empty data
- Subqueries in FROM clause fail
- Complex projections break

---

### 4. **Lack of Intermediate Result Storage**

**Location**: `src/execution/context.rs`, `src/execution/mod.rs`

**Problem**: ExecutionContext cannot store materialized intermediate results.

**What's Missing**:
```rust
// Needed but doesn't exist:
pub struct ExecutionContext {
    // ...
    materialized_ctes: HashMap<String, Vec<DataChunk>>,  // ❌ Missing
    subquery_cache: HashMap<SubqueryId, Value>,          // ❌ Missing
}
```

**Current**:
```rust
pub struct ExecutionContext {
    pub transaction_manager: Arc<TransactionManager>,
    pub catalog: Arc<RwLock<Catalog>>,
    pub transaction_id: Option<Uuid>,
    pub parameters: HashMap<String, ContextValue>,
    // No storage for materialized results!
}
```

**Impact**:
- CTEs execute multiple times instead of once
- No way to cache subquery results
- Cannot implement MATERIALIZED CTEs
- Performance degradation

---

### 5. **Operator Pipeline Limitation**

**Location**: `src/execution/mod.rs:59-159`

**Problem**: Operators are stateless and cannot reference materialized results.

**Current Design**:
```rust
fn create_operator(&self, plan: PhysicalPlan) -> DuckDBResult<Box<dyn ExecutionOperator>> {
    match plan {
        PhysicalPlan::TableScan(scan) => Ok(Box::new(TableScanOperator::new(scan, ...))),
        // No CTE materialization operator exists
        // No subquery evaluation operator exists
    }
}
```

**What's Needed**:
- `PhysicalPlan::CTEMaterialization` variant
- `PhysicalPlan::CTEScan` variant
- `CTEMaterializationOperator` that executes CTE and stores results
- `CTEScanOperator` that reads from stored results

---

### 6. **Aggregate Execution Issues**

**Location**: `src/execution/parallel_operators.rs:384-450`

**Problem**: Aggregate operator doesn't properly handle empty inputs or CTEs.

**Test Evidence**:
```
Error: Execution("Unsupported physical plan: HashAggregate(...)")
```

**Current Status**:
- ParallelHashAggregateOperator exists
- But has issues with:
  - Empty input handling
  - Schema propagation
  - Integration with CTEs

---

## Impact Analysis

### Feature Implementation Status

| Feature | Parser | Binder | Optimizer | Execution | Status |
|---------|--------|--------|-----------|-----------|--------|
| Simple CTEs | ✓ | ✓ | ✓ | ❌ | **BLOCKED** |
| Recursive CTEs | ✓ | Partial | ❌ | ❌ | **BLOCKED** |
| Scalar Subqueries | ✓ | ✓ | ✓ | ❌ | **BLOCKED** |
| EXISTS Subqueries | ✓ | ✓ | ✓ | ❌ | **BLOCKED** |
| IN Subqueries | ✓ | ✓ | ✓ | ❌ | **BLOCKED** |
| Correlated Subqueries | ✓ | ❌ | ❌ | ❌ | **BLOCKED** |
| Subquery in FROM | ✓ | ✓ | ✓ | ❌ | **BLOCKED** |

### Test Results

**Before Investigation**: 5/18 tests passing
**After Fixes**: 5/18 tests passing
**Improvements**: Parser and binding fixes (4 issues resolved)
**Remaining**: 13 tests blocked by execution engine limitations

**Passing Tests**:
- ✓ test_intersect
- ✓ test_except
- ✓ test_complex_set_operations
- ✓ test_union_all
- ✓ test_union_distinct

**Blocked by CTE Execution** (7 tests):
- ❌ test_simple_cte
- ❌ test_multiple_ctes
- ❌ test_cte_with_set_operations
- ❌ test_cte_with_aggregation
- ❌ test_cte_used_multiple_times
- ❌ test_cte_with_subqueries
- ❌ test_subquery_in_from_clause

**Blocked by Subquery Execution** (6 tests):
- ❌ test_scalar_subquery
- ❌ test_exists_subquery
- ❌ test_in_subquery
- ❌ test_nested_subqueries
- ❌ test_subquery_with_set_operations
- ❌ test_recursive_cte_numbers (also needs fixpoint iteration)

---

## Detailed Findings

### Finding 1: CTE Data Flow Breakdown

**Investigation Summary**:

1. **Step 1: Parsing** ✓
   ```rust
   // src/parser/parser.rs:163-170
   let query = Box::new(self.parse_select_statement()?);
   let set_operations = self.parse_set_operations()?;
   query.set_operations = set_operations;
   ```
   - CTEs parse correctly with set operations

2. **Step 2: Binding** ✓
   ```rust
   // src/planner/binder.rs:293-333
   for cte in &with_clause.ctes {
       let cte_plan = self.bind_select_statement(&cte.query)?;
       self.context.ctes.insert(cte.name.clone(), cte_plan.clone());
       self.context.add_table(&cte.name, &schema);
   }
   ```
   - CTEs bind and schema registers correctly

3. **Step 3: Reference Resolution** ✓
   ```rust
   // src/planner/binder.rs:368-378
   if let Some(cte_plan) = self.context.ctes.get(name).cloned() {
       let cte_schema = cte_plan.schema();
       self.context.add_table(table_name, &cte_schema);
       return Ok(cte_plan);
   }
   ```
   - CTE references resolve correctly

4. **Step 4: Execution** ❌
   ```rust
   // Test output:
   // Expected: Varchar("Diana")
   // Actual:   Varchar("")
   ```
   - **Data is lost during execution**

**Hypothesis**:
- ProjectionOperator evaluates expressions on CTE plan
- Column references use wrong indices
- Or: Data chunks from CTE aren't properly passed through

---

### Finding 2: Expression Evaluation Architecture

**Current Design** (Stateless):
```rust
pub trait Expression {
    fn evaluate(&self, chunk: &DataChunk) -> DuckDBResult<Vector>;
}

impl Expression for ColumnRefExpression {
    fn evaluate(&self, chunk: &DataChunk) -> DuckDBResult<Vector> {
        chunk.get_vector(self.column_index)  // Only has chunk!
    }
}
```

**What Subqueries Need** (Stateful):
```rust
// Needed:
pub trait Expression {
    fn evaluate(
        &self,
        chunk: &DataChunk,
        context: &mut ExecutionContext,  // Need this!
    ) -> DuckDBResult<Vector>;
}

impl Expression for SubqueryExpression {
    fn evaluate(&self, chunk: &DataChunk, context: &mut ExecutionContext) -> DuckDBResult<Vector> {
        // 1. Execute subquery plan using context
        let mut engine = ExecutionEngine::new(context.clone());
        let result = engine.execute(self.subquery_plan.clone())?;

        // 2. Collect results
        let mut values = Vec::new();
        for chunk in result {
            // Process chunk
        }

        // 3. Return as Vector
        Vector::from_values(&values)
    }
}
```

**Refactoring Required**:
- Change Expression trait signature (**Breaking change to entire codebase**)
- Update all 100+ expression implementations
- Thread ExecutionContext through all operator evaluations
- Massive architectural change

---

### Finding 3: Comparison with DuckDB C++

**DuckDB C++ Has**:
```cpp
class ExpressionExecutor {
    ExecutionContext &context;  // ✓ Has context

    void Execute(Expression *expr, DataChunk &input, Vector &result) {
        // Can execute subqueries
        // Can access materialized CTEs
        // Has full execution capability
    }
};

class CTENode : public PhysicalOperator {
    // Dedicated CTE materialization
    vector<DataChunk> materialized_data;

    void Execute(ExecutionContext &context) {
        // Materialize once, cache results
    }
};
```

**DuckDBRS Has**:
```rust
pub trait Expression {
    fn evaluate(&self, chunk: &DataChunk) -> DuckDBResult<Vector>;
    // ❌ No context
    // ❌ Cannot execute subqueries
    // ❌ Cannot access materialized results
}

// ❌ No CTE materialization operator exists
```

**Gap**: Fundamental architectural difference in expression evaluation model.

---

## Recommended Solutions

### Solution 1: Expression Evaluation Refactoring (High Priority)

**Effort**: 3-4 weeks
**Impact**: Enables all subquery types

**Implementation Plan**:

1. **Phase 1**: Add context parameter (Week 1-2)
   ```rust
   // Step 1: Update trait
   pub trait Expression {
       fn evaluate(&self, chunk: &DataChunk, context: &ExecutionContext)
           -> DuckDBResult<Vector>;
   }

   // Step 2: Update all implementations (~100 files)
   // Step 3: Thread context through operators
   ```

2. **Phase 2**: Implement subquery expressions (Week 2-3)
   ```rust
   pub struct SubqueryExpression {
       base: BaseExpression,
       subquery_plan: LogicalPlan,
       subquery_type: SubqueryType, // Scalar, Exists, In
   }

   impl Expression for SubqueryExpression {
       fn evaluate(&self, chunk: &DataChunk, context: &ExecutionContext)
           -> DuckDBResult<Vector> {
           // Execute subquery and return results
       }
   }
   ```

3. **Phase 3**: Add result caching (Week 3-4)
   ```rust
   pub struct ExecutionContext {
       subquery_cache: HashMap<SubqueryId, CachedResult>,
       // Cache uncorrelated subquery results
   }
   ```

---

### Solution 2: CTE Materialization Operator (High Priority)

**Effort**: 2-3 weeks
**Impact**: Fixes all CTE tests

**Implementation Plan**:

1. **Add Physical Plan Nodes**:
   ```rust
   // src/planner/physical_plan.rs
   pub enum PhysicalPlan {
       // ... existing variants
       CTEMaterialization(PhysicalCTEMaterialization),
       CTEScan(PhysicalCTEScan),
   }

   pub struct PhysicalCTEMaterialization {
       cte_name: String,
       input: Box<PhysicalPlan>,
       schema: Vec<PhysicalColumn>,
   }

   pub struct PhysicalCTEScan {
       cte_name: String,
       schema: Vec<PhysicalColumn>,
   }
   ```

2. **Implement Operators**:
   ```rust
   // src/execution/operators.rs
   pub struct CTEMaterializationOperator {
       materialization: PhysicalCTEMaterialization,
       context: ExecutionContext,
   }

   impl ExecutionOperator for CTEMaterializationOperator {
       fn execute(&self) -> DuckDBResult<Box<dyn DataChunkStream>> {
           // 1. Execute input plan
           let mut engine = ExecutionEngine::new(self.context.clone());
           let mut stream = engine.execute(*self.materialization.input.clone())?;

           // 2. Materialize all chunks
           let mut chunks = Vec::new();
           while let Some(chunk) = stream.next() {
               chunks.push(chunk?);
           }

           // 3. Store in context
           self.context.store_cte(&self.materialization.cte_name, chunks.clone());

           // 4. Return stored chunks
           Ok(Box::new(SimpleDataChunkStream::new(chunks)))
       }
   }

   pub struct CTEScanOperator {
       scan: PhysicalCTEScan,
       context: ExecutionContext,
   }

   impl ExecutionOperator for CTEScanOperator {
       fn execute(&self) -> DuckDBResult<Box<dyn DataChunkStream>> {
           // Retrieve materialized CTE from context
           let chunks = self.context.get_cte(&self.scan.cte_name)?;
           Ok(Box::new(SimpleDataChunkStream::new(chunks)))
       }
   }
   ```

3. **Update ExecutionContext**:
   ```rust
   pub struct ExecutionContext {
       // ... existing fields
       materialized_ctes: Arc<RwLock<HashMap<String, Vec<DataChunk>>>>,
   }

   impl ExecutionContext {
       pub fn store_cte(&self, name: &str, chunks: Vec<DataChunk>) {
           let mut ctes = self.materialized_ctes.write().unwrap();
           ctes.insert(name.to_string(), chunks);
       }

       pub fn get_cte(&self, name: &str) -> DuckDBResult<Vec<DataChunk>> {
           let ctes = self.materialized_ctes.read().unwrap();
           ctes.get(name)
               .cloned()
               .ok_or_else(|| DuckDBError::Catalog(format!("CTE '{}' not found", name)))
       }
   }
   ```

4. **Update Optimizer**:
   ```rust
   // src/planner/optimizer.rs
   fn convert_to_physical(&self, logical_plan: LogicalPlan) -> DuckDBResult<PhysicalPlan> {
       match logical_plan {
           // When we see a SELECT with CTEs:
           // 1. Convert each CTE to CTEMaterialization
           // 2. Convert CTE references to CTEScan
           LogicalPlan::Projection(proj) if has_cte_input(&proj) => {
               // Special handling for CTE references
               Ok(PhysicalPlan::CTEScan(PhysicalCTEScan {
                   cte_name: extract_cte_name(&proj),
                   schema: proj.schema.into_physical(),
               }))
           }
           // ... rest of conversion
       }
   }
   ```

---

### Solution 3: Column Index Remapping Layer (Medium Priority)

**Effort**: 1-2 weeks
**Impact**: Fixes column reference issues

**Implementation**:
```rust
pub struct ColumnRemapper {
    // Maps logical column indices to physical column indices
    mapping: HashMap<usize, usize>,
}

impl ColumnRemapper {
    pub fn for_cte_reference(cte_schema: &[Column], base_schema: &[Column]) -> Self {
        // Create mapping from CTE column positions to base table positions
        let mut mapping = HashMap::new();
        for (cte_idx, cte_col) in cte_schema.iter().enumerate() {
            if let Some(base_idx) = base_schema.iter().position(|c| c.name == cte_col.name) {
                mapping.insert(cte_idx, base_idx);
            }
        }
        Self { mapping }
    }

    pub fn remap_expression(&self, expr: &Expression) -> Expression {
        // Recursively remap all column references
    }
}
```

---

### Solution 4: Incremental Approach (Recommended)

Given the scope of required changes, an incremental approach is recommended:

**Phase 1** (1-2 weeks): CTE Materialization
- Implement CTEMaterialization and CTEScan operators
- Update ExecutionContext with CTE storage
- Fix basic CTE tests
- **Deliverable**: 7 CTE tests passing

**Phase 2** (2-3 weeks): Expression Context
- Refactor Expression trait to accept context
- Update all expression implementations
- **Deliverable**: Expression evaluation can access context

**Phase 3** (1-2 weeks): Scalar Subqueries
- Implement SubqueryExpression
- Add subquery execution during expression evaluation
- **Deliverable**: Scalar subquery tests passing

**Phase 4** (1 week): EXISTS/IN Subqueries
- Implement EXISTS and IN subquery expressions
- Add optimization for uncorrelated subqueries
- **Deliverable**: All subquery tests passing

**Total Timeline**: 5-8 weeks for full implementation

---

## Code References

### Files Modified During Investigation

1. `src/parser/parser.rs:163-170, 376-379` - Set operations in CTEs
2. `src/parser/parser.rs:1811-1816` - Keywords as identifiers
3. `src/expression/binder.rs:175-183` - COUNT(*) handling
4. `src/expression/binder.rs:104-129` - Subquery binding stubs
5. `src/planner/binder.rs:165-176` - CTE binding protection
6. `src/planner/binder.rs:368-378` - CTE reference handling

### Files Requiring Changes

**For CTE Materialization**:
- `src/planner/physical_plan.rs` - Add CTE plan nodes
- `src/execution/operators.rs` - Add CTE operators
- `src/execution/context.rs` - Add CTE storage
- `src/execution/mod.rs` - Register CTE operators
- `src/planner/optimizer.rs` - Convert CTEs to physical plans

**For Subquery Execution**:
- `src/expression/expression.rs` - Update Expression trait
- `src/expression/binder.rs` - Implement subquery binding
- `src/execution/**/*.rs` - Thread context through operators
- All expression implementations (~100 files)

---

## Conclusion

The architectural limitations identified are fundamental and cannot be fixed with simple patches. Full implementation of CTEs and subqueries requires:

1. **Major refactoring** of expression evaluation system
2. **New operator types** for CTE materialization
3. **Enhanced execution context** for intermediate result storage
4. **Significant testing** to ensure correctness

**Estimated Total Effort**: 8-12 weeks of focused development

**Recommendation**: Prioritize CTE materialization (Phase 1) as it provides immediate value with less architectural disruption. Defer expression context refactoring until resources allow for the larger undertaking.

---

## Appendix: Test Output Examples

### CTE Test Failure
```
test test_simple_cte ... FAILED

---- test_simple_cte stdout ----
thread 'test_simple_cte' panicked at tests/advanced_sql_tests.rs:52:5:
assertion `left == right` failed
  left: Varchar("")
 right: Varchar("Diana")
```

### Subquery Test Failure
```
test test_scalar_subquery ... FAILED

Error: NotImplemented("Scalar subqueries not yet fully implemented in expression context")
```

### Aggregate with CTE Failure
```
test test_cte_with_aggregation ... FAILED

Error: Execution("Unsupported physical plan: HashAggregate(PhysicalHashAggregate { ... })")
```

---

**End of Document**
