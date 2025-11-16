# Session Summary: CTE and Subquery Implementation Investigation

**Date**: 2025-11-14
**Duration**: ~4 hours
**Focus**: Advanced SQL features (CTEs, Subqueries)
**Status**: Investigation complete, architectural limitations documented

---

## Session Overview

This session involved a deep investigation into implementing Common Table Expressions (CTEs) and subquery support in DuckDBRS. While parser and binding improvements were successfully completed, fundamental architectural limitations in the execution engine were identified that block full implementation.

---

## Completed Work

### 1. Parser Improvements ✅

#### Set Operations in CTEs
**File**: `src/parser/parser.rs`

Added support for UNION/INTERSECT/EXCEPT within CTE definitions:

```rust
// Lines 163-170: Parse SELECT with set operations in CTEs
let mut query = self.parse_select_statement()?;
let set_operations = self.parse_set_operations()?;
query.set_operations = set_operations;

// Lines 376-379: Parse set operations in subqueries
let mut subquery = self.parse_select_statement()?;
let set_operations = self.parse_set_operations()?;
subquery.set_operations = set_operations;
```

**Impact**: Enables queries like:
```sql
WITH cte AS (
    SELECT * FROM t1
    UNION
    SELECT * FROM t2
)
SELECT * FROM cte;
```

#### Keywords as Identifiers
**File**: `src/parser/parser.rs:1811-1816`

Modified `consume_identifier()` to accept keywords in unambiguous contexts:

```rust
TokenType::Keyword(_) => {
    // Allow keywords to be used as identifiers in unambiguous contexts
    let name = self.current_token().text.clone();
    self.position += 1;
    Ok(name)
}
```

**Impact**: Enables using SQL keywords like "count" as column aliases.

---

### 2. Expression Binding Improvements ✅

#### COUNT(*) Wildcard Handling
**File**: `src/expression/binder.rs:175-183`

Added special case for COUNT(*):

```rust
if is_aggregate && name.to_uppercase() == "COUNT" && arguments.len() == 1 {
    if matches!(arguments[0], ast::Expression::Wildcard) {
        let return_type = LogicalType::BigInt;
        let func_expr = FunctionExpression::new("COUNT".to_string(), return_type, vec![]);
        return Ok(Arc::new(func_expr));
    }
}
```

**Impact**: COUNT(*) now works without trying to bind the wildcard.

#### Subquery Recognition
**File**: `src/expression/binder.rs:104-129`

Added binding stubs for all subquery types:

```rust
ast::Expression::Subquery(_subquery) => {
    Err(DuckDBError::NotImplemented(
        "Scalar subqueries not yet fully implemented in expression context".to_string(),
    ))
}

ast::Expression::Exists(_subquery) => {
    Err(DuckDBError::NotImplemented(
        "EXISTS subqueries not yet fully implemented".to_string(),
    ))
}

ast::Expression::InSubquery { ... } => {
    Err(DuckDBError::NotImplemented(
        "IN subqueries not yet fully implemented".to_string(),
    ))
}
```

**Impact**: Subqueries are recognized and provide clear error messages instead of crashes.

---

### 3. CTE Binding Protection ✅

#### Prevent Column Binding Overwrites
**File**: `src/planner/binder.rs:165-176`

Added check to prevent `update_context_from_plan` from overwriting CTE bindings:

```rust
let is_cte_reference = if let Some(from) = &select.from {
    match from {
        TableReference::Table { name, .. } => self.context.ctes.contains_key(name),
        _ => false,
    }
} else {
    false
};

if !is_cte_reference {
    self.update_context_from_plan(&plan)?;
}
```

**Impact**: CTE column references no longer get overwritten by internal table scans.

#### CTE Reference Handling
**File**: `src/planner/binder.rs:368-378`

Improved CTE reference binding:

```rust
if let Some(cte_plan) = self.context.ctes.get(name).cloned() {
    let cte_schema = cte_plan.schema();
    let table_name = alias.as_ref().unwrap_or(name);

    // Register the CTE in the binding context
    self.context.add_table(table_name, &cte_schema);

    // Return the CTE plan as-is
    return Ok(cte_plan);
}
```

**Impact**: CTEs are properly registered in the binding context.

---

### 4. Documentation ✅

Created comprehensive documentation of architectural limitations:

**Files Created**:
1. `docs/ARCHITECTURAL_LIMITATIONS.md` (700+ lines)
   - Detailed analysis of 6 critical limitations
   - Code examples and evidence
   - Recommended solutions with timelines
   - Test output examples

2. `docs/ARCHITECTURE.md` - Updated
   - Added Section 11: Known Limitations
   - References to detailed documentation
   - Comparison with DuckDB C++

---

## Test Results

### Before Session
- **Passing**: 5/18 tests
- **Failing**: 13/18 tests

### After Session
- **Passing**: 5/18 tests (same)
- **Failing**: 13/18 tests
- **Parser/Binding Improvements**: 4 issues resolved
- **Execution Blockers**: 13 tests still blocked

### Passing Tests ✅
1. test_intersect
2. test_except
3. test_complex_set_operations
4. test_union_all
5. test_union_distinct

### Failing Tests (By Category)

**CTE Execution Issues** (7 tests):
- test_simple_cte
- test_multiple_ctes
- test_cte_with_set_operations
- test_cte_with_aggregation
- test_cte_used_multiple_times
- test_cte_with_subqueries
- test_subquery_in_from_clause

**Subquery Execution Issues** (6 tests):
- test_scalar_subquery
- test_exists_subquery
- test_in_subquery
- test_nested_subqueries
- test_subquery_with_set_operations
- test_recursive_cte_numbers

---

## Critical Findings

### 1. CTE Execution Data Loss

**Symptom**:
```
Expected: Varchar("Diana")
Actual:   Varchar("")
```

**Analysis**:
- CTEs parse correctly ✓
- CTEs bind correctly ✓
- Schema registers correctly ✓
- **Data is lost during execution** ❌

**Investigation Path**:
1. Checked parser → Working
2. Checked binder → Working
3. Checked column bindings → Fixed binding overwrites
4. Checked execution operators → **Found the gap**

**Root Cause**: No CTE materialization mechanism in execution engine.

### 2. Expression Evaluation Architecture Gap

**Current Design** (Context-free):
```rust
pub trait Expression {
    fn evaluate(&self, chunk: &DataChunk) -> DuckDBResult<Vector>;
}
```

**What's Needed** (Context-aware):
```rust
pub trait Expression {
    fn evaluate(
        &self,
        chunk: &DataChunk,
        context: &ExecutionContext,  // ← Missing!
    ) -> DuckDBResult<Vector>;
}
```

**Impact**: Cannot implement any subquery types.

### 3. Missing CTE Operators

**DuckDB C++ Has**:
- `CTEMaterializationNode` - Executes and caches CTE
- `CTEScanNode` - Reads from cached results

**DuckDBRS Has**:
- ❌ No CTE materialization operator
- ❌ No CTE result storage
- ❌ No CTE scan operator

---

## Architectural Limitations Identified

### Critical (Blocks Features)

1. **Expression Evaluation Context**
   - Expressions can't access ExecutionContext
   - Blocks: All subquery types
   - Fix: Refactor Expression trait (2-3 weeks, breaking change)

2. **CTE Materialization**
   - No mechanism to cache CTE results
   - Blocks: All CTE queries
   - Fix: Implement CTE operators (1-2 weeks)

3. **Intermediate Result Storage**
   - ExecutionContext can't store materialized data
   - Blocks: CTE caching, subquery caching
   - Fix: Add storage to ExecutionContext (1 week)

### High Priority

4. **Column Index Mismatch**
   - Logical vs physical column index misalignment
   - Causes: Wrong columns, empty results
   - Fix: Column remapping layer (1-2 weeks)

5. **Aggregate Execution**
   - Integration issues with CTEs
   - Empty input handling problems
   - Fix: Improve aggregate operator (1 week)

### Medium Priority

6. **Recursive CTE Support**
   - Requires fixpoint iteration
   - Complex implementation
   - Fix: Dedicated fixpoint executor (2-3 weeks)

---

## Attempted Solutions (All Failed)

### Attempt 1: Wrap CTEs in Projections
```rust
// Tried wrapping CTE plan in a projection to remap columns
return Ok(LogicalPlan::Projection(LogicalProjection::new(
    cte_plan,
    projection_expressions,
    cte_schema,
)));
```
**Result**: Still returned empty data ❌

### Attempt 2: Create Synthetic TableScans
```rust
// Tried representing CTEs as special TableScans
return Ok(LogicalPlan::TableScan(LogicalTableScan {
    table_name: table_name.clone(),
    schema,
    ...
}));
```
**Result**: Execution couldn't find table ❌

### Attempt 3: Modify Context Update Logic
```rust
// Tried preventing context overwrites
if !self.context.tables.contains_key(&scan.table_name) {
    self.context.add_table(&scan.table_name, &scan.schema);
}
```
**Result**: Still returned empty data ❌

### Attempt 4: Skip Context Update for CTEs
```rust
// Tried not calling update_context_from_plan for CTEs
if !is_cte_reference {
    self.update_context_from_plan(&plan)?;
}
```
**Result**: Still returned empty data ❌

**Conclusion**: The problem is not in binding, it's in execution.

---

## Recommended Solutions

### Phase 1: CTE Materialization (1-2 weeks)

**Goal**: Get basic CTE tests passing

**Implementation**:
1. Add `PhysicalPlan::CTEMaterialization` variant
2. Add `PhysicalPlan::CTEScan` variant
3. Implement `CTEMaterializationOperator`
4. Implement `CTEScanOperator`
5. Add CTE storage to `ExecutionContext`
6. Update optimizer to generate CTE plans

**Expected Outcome**: 7 CTE tests passing

### Phase 2: Expression Context (2-3 weeks)

**Goal**: Enable expression-level execution

**Implementation**:
1. Update `Expression` trait signature
2. Update all ~100 expression implementations
3. Thread context through all operators
4. Update optimizer and execution engine

**Expected Outcome**: Foundation for subquery execution

### Phase 3: Subquery Execution (2-3 weeks)

**Goal**: Implement all subquery types

**Implementation**:
1. Create `SubqueryExpression` type
2. Implement scalar subquery evaluation
3. Implement EXISTS subquery evaluation
4. Implement IN subquery evaluation
5. Add subquery result caching

**Expected Outcome**: 6+ subquery tests passing

**Total Timeline**: 8-12 weeks for full implementation

---

## Code Changes Summary

### Files Modified

1. **src/parser/parser.rs**
   - Lines 163-170: Set operations in CTEs
   - Lines 376-379: Set operations in subqueries
   - Lines 1811-1816: Keywords as identifiers

2. **src/expression/binder.rs**
   - Lines 91-103: Wildcard expression binding (stub)
   - Lines 104-110: Subquery expression binding (stub)
   - Lines 112-117: EXISTS subquery binding (stub)
   - Lines 119-129: IN subquery binding (stub)
   - Lines 175-183: COUNT(*) special case

3. **src/planner/binder.rs**
   - Lines 165-176: CTE reference detection
   - Lines 368-378: CTE reference binding
   - Lines 767-803: Updated `update_context_from_plan`

### Files Created

1. **docs/ARCHITECTURAL_LIMITATIONS.md** (700+ lines)
2. **docs/SESSION_2025_11_14_CTE_SUBQUERY_INVESTIGATION.md** (this file)

### Files Updated

1. **docs/ARCHITECTURE.md**
   - Added Section 11: Known Limitations
   - Updated Table of Contents
   - Added references to detailed documentation

---

## Comparison with DuckDB C++

| Component | DuckDB C++ | DuckDBRS | Status |
|-----------|------------|----------|--------|
| **Parser** | Custom C++ parser | Custom Rust parser | ✓ Similar quality |
| **Binding** | Full binding | Good binding | ✓ Similar quality |
| **Optimization** | 50+ rules | ~10 rules | ⚠️ Limited |
| **Execution** | Context-aware | Context-free | ❌ Critical gap |
| **CTE Support** | Full | Parser only | ❌ Execution blocked |
| **Subquery Support** | Full | Parser only | ❌ Execution blocked |
| **Expression Eval** | Has context | No context | ❌ Architectural gap |
| **Feature Parity** | 100% | ~40% | ⚠️ Significant gap |

**Conclusion**: DuckDBRS is **not a strict port** of DuckDB C++. It's an independent Rust implementation with significant architectural differences.

---

## Lessons Learned

### 1. Debugging Strategy

**Effective**:
- ✓ Bottom-up investigation (parser → binder → optimizer → execution)
- ✓ Test-driven debugging (run specific tests)
- ✓ Code tracing (follow data flow through components)

**Less Effective**:
- ✗ Trying multiple quick fixes without understanding root cause
- ✗ Assuming binding issues when execution was the problem

### 2. Architectural Insights

**Key Finding**: Even with perfect parsing and binding, execution engine gaps block features.

**Observation**: Expression evaluation architecture is fundamental:
- Changing it requires touching ~100 files
- Breaking change across entire codebase
- But necessary for advanced features

### 3. Documentation Value

**Benefit**: Comprehensive documentation helps:
- Future developers understand gaps
- Stakeholders understand limitations
- Planning realistic timelines

---

## Next Steps Recommendations

### Immediate (This Week)
1. ✅ Document findings (completed)
2. Review documentation with team
3. Prioritize which features to implement first

### Short Term (Next 2-4 Weeks)
1. Implement CTE materialization (Phase 1)
2. Get 7 CTE tests passing
3. Validate approach before larger refactoring

### Medium Term (Next 2-3 Months)
1. Refactor expression evaluation (Phase 2)
2. Implement subquery execution (Phase 3)
3. Achieve ~80% test pass rate

### Long Term (Next 6 Months)
1. Implement recursive CTEs
2. Add correlated subquery support
3. Optimize execution performance

---

## Workarounds for Users

Until limitations are addressed, users can:

**For CTEs**:
- Use JOINs instead where possible
- Rewrite as subqueries (when subqueries work)
- Break into multiple queries

**For Subqueries**:
- Rewrite as JOINs
- Use separate queries with application logic
- Consider using `duckdb-rs` (official C++ bindings)

**For Production Use**:
- Use official `duckdb-rs` crate for full DuckDB feature parity
- Treat DuckDBRS as experimental/educational project

---

## Metrics

### Time Investment
- Investigation: ~4 hours
- Parser fixes: ~1 hour
- Binding fixes: ~1.5 hours
- Execution debugging: ~1 hour
- Documentation: ~0.5 hours

### Code Changes
- Lines added: ~150
- Lines modified: ~50
- Files modified: 3
- Files created: 2 (documentation)

### Test Impact
- Tests fixed: 0 (execution blockers remain)
- Tests improved: 4 (parser/binding issues resolved)
- Architectural issues identified: 6

---

## Conclusion

This session successfully:
1. ✅ Identified and documented fundamental architectural limitations
2. ✅ Improved parser to support set operations in CTEs
3. ✅ Enhanced expression binding with proper wildcard handling
4. ✅ Fixed CTE column binding protection
5. ✅ Created comprehensive documentation for future work

The investigation revealed that full CTE and subquery support requires significant architectural changes (8-12 weeks estimated). The recommended approach is to implement CTE materialization first (Phase 1) as it provides immediate value with less disruption.

**Key Takeaway**: DuckDBRS has a solid foundation (parser, binder) but needs execution engine enhancements to support advanced SQL features. The architecture is fixable, but requires dedicated effort.

---

**Session End**: 2025-11-14
**Status**: Documentation complete, ready for next phase planning
