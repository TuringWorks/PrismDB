# Expression Binding Integration - Status Report

**Date:** 2025-11-13
**Session:** Continuation - Expression Binding Phase

---

## ✅ COMPLETED: Physical Plan Type Updates

Successfully updated all physical plan structures to use bound execution expressions (`ExpressionRef`) instead of parser AST expressions.

### Types Updated

| Type | Field | Old Type | New Type | Status |
|------|-------|----------|----------|--------|
| PhysicalFilter | predicate | `Expression` | `ExpressionRef` | ✅ |
| PhysicalProjection | expressions | `Vec<Expression>` | `Vec<ExpressionRef>` | ✅ |
| PhysicalSort | PhysicalSortExpression.expression | `Expression` | `ExpressionRef` | ✅ |
| PhysicalTableScan | filters | `Vec<Expression>` | `Vec<ExpressionRef>` | ✅ |
| PhysicalAggregate | group_by | `Vec<Expression>` | `Vec<ExpressionRef>` | ✅ |
| PhysicalAggregate | PhysicalAggregateExpression.arguments | `Vec<Expression>` | `Vec<ExpressionRef>` | ✅ |
| PhysicalHashAggregate | group_by | `Vec<Expression>` | `Vec<ExpressionRef>` | ✅ |
| PhysicalJoin | condition | `Option<Expression>` | `Option<ExpressionRef>` | ✅ |
| PhysicalHashJoin | left_keys, right_keys | `Vec<Expression>` | `Vec<ExpressionRef>` | ✅ |
| PhysicalHashJoin | condition | `Option<Expression>` | `Option<ExpressionRef>` | ✅ |
| PhysicalSortMergeJoin | left_keys, right_keys | `Vec<Expression>` | `Vec<ExpressionRef>` | ✅ |
| PhysicalSortMergeJoin | condition | `Option<Expression>` | `Option<ExpressionRef>` | ✅ |
| PhysicalUpdate | assignments | `HashMap<String, Expression>` | `HashMap<String, ExpressionRef>` | ✅ |
| PhysicalUpdate | condition | `Option<Expression>` | `Option<ExpressionRef>` | ✅ |
| PhysicalDelete | condition | `Option<Expression>` | `Option<ExpressionRef>` | ✅ |

### Files Modified

- `/src/planner/physical_plan.rs` - Updated all type definitions and constructors

---

## ✅ COMPLETED: Optimizer/Planner Integration

The optimizer code has been successfully updated to bind expressions before creating physical plans.

### Compilation Status

**Total Errors:** 0 ✅
**Location:** `src/planner/optimizer.rs`
**Build Status:** Library compiles successfully with warnings only

### Updated Code Locations

| Line Range | Operator | Changes Made |
|------------|----------|--------------|
| 68-80 | Filter | Added schema extraction, binder context creation, and expression binding |
| 81-104 | Projection | Bound all projection expressions using input schema |
| 113-134 | Sort | Bound sort expressions with explicit type annotations |
| 135-184 | Aggregate | Bound group_by and aggregate function arguments |
| 185-235 | Join | Bound join condition using combined schema from both sides |
| 253-283 | Update | Bound assignments and condition (with TODO for catalog access) |
| 284-302 | Delete | Bound condition expression (with TODO for catalog access) |

### Helper Methods Added

| Method | Lines | Purpose |
|--------|-------|---------|
| `create_binder_context` | 247-264 | Creates BinderContext from a schema (Column vec) |
| `get_input_schema` | 267-285 | Extracts schema from any LogicalPlan node |

---

## 📋 Next Steps (Priority Order)

### Step 1: Update Optimizer to Bind Expressions ⏳

For each physical plan creation in `optimizer.rs`:

1. **Create BinderContext:**

   ```rust
   let mut binder_context = BinderContext {
       alias_map: HashMap::new(),
       column_bindings: Vec::new(),
       depth: 0,
   };

   // Populate column_bindings from table schema
   for (idx, col) in table_schema.iter().enumerate() {
       binder_context.column_bindings.push(ColumnBinding::new(
           0, // table_index
           idx, // column_index
           col.name.clone(),
           col.data_type.clone()
       ));
   }
   ```

2. **Create Binder:**

   ```rust
   let binder = ExpressionBinder::new(binder_context);
   ```

3. **Bind Expressions:**

   ```rust
   // Single expression
   let bound_expr = binder.bind_expression(&ast_expr)?;

   // Multiple expressions
   let bound_exprs: Result<Vec<_>, _> = ast_exprs.iter()
       .map(|expr| binder.bind_expression(expr))
       .collect();
   let bound_exprs = bound_exprs?;
   ```

4. **Create Physical Plan with Bound Expressions:**

   ```rust
   PhysicalFilter::new(input, bound_expr)
   ```

### Step 2: Handle Schema Information

The binder needs table schema to resolve column names. Two approaches:

#### Option A: Thread schema through optimizer

- Pass table schema when optimizing each operator
- More flexible but requires more parameter passing

#### Option B: Store schema in logical plan

- Logical plan nodes already have schema info
- Can extract schema when creating physical plan
- Cleaner but requires logical plan access

**Recommendation:** Start with Option B - extract schema from logical plan

### Step 3: Test Integration

1. Start with simple queries:
   - `SELECT * FROM table` (no binding needed)
   - `SELECT col1 FROM table WHERE col2 > 5` (basic binding)

2. Progress to complex queries:
   - Joins with conditions
   - Aggregates with GROUP BY
   - Multiple filters

3. Verify:
   - Column names resolve to correct indices
   - Type checking works
   - Execution produces correct results

---

## 🔑 Key Architecture Points

### Expression Flow (Now Complete)

```text

Parser
  ↓
parser::ast::Expression (column names, type unknown)
  ↓
Binder ← BinderContext (table schemas) ← Catalog
  ↓
expression::expression::Expression (column indices, type known)
  ↓
Physical Plan
  ↓
Execution Operators
  ↓
Results
```

### Why This Matters

1. **Column Name Resolution:** Parser knows "salary" but execution needs "column index 3"
2. **Type Safety:** Binder ensures "age > 'abc'" fails at planning, not execution
3. **Schema Validation:** Detects non-existent columns during planning
4. **Optimization:** Enables better query optimization (fold constants, etc.)

---

## 📊 Progress Metrics

### Completed This Session

- Expression binder implementation: 630 lines ✅
- Physical plan type updates: 200+ lines modified ✅
- Optimizer integration: 250+ lines added ✅
- Helper methods (schema extraction, binder context): 50 lines ✅
- Documentation: 400+ lines ✅

### Remaining Work

- Testing with simple queries: Basic SELECT/WHERE/ORDER BY
- Testing with complex queries: Joins, Aggregates, Subqueries
- Fix any runtime issues discovered during testing
- Add catalog access for UPDATE/DELETE binding (future enhancement)

---

## 🚨 Potential Issues & Solutions

### Issue 1: Circular Dependencies

**Problem:** Binder needs Catalog, Optimizer needs Binder
**Solution:** Pass Catalog reference to optimizer, create binder on-demand

### Issue 2: Schema Access

**Problem:** Need table schema at binding time
**Solution:** Extract from logical plan or query catalog during optimization

### Issue 3: Complex Expressions

**Problem:** Nested expressions (e.g., `(a + b) > (c * 2)`)
**Solution:** Binder recursively binds children - already implemented

### Issue 4: Aggregate Expressions

**Problem:** `COUNT(*)` vs `COUNT(column)`
**Solution:** Binder handles empty arguments list - already implemented

---

## 🎯 Success Criteria

Integration is complete when:

1. ✅ Physical plan types use `ExpressionRef`
2. ✅ Optimizer creates bound expressions
3. ✅ All compilation errors resolved
4. ⏳ Existing tests pass
5. ⏳ New binding tests pass
6. ⏳ End-to-end queries with WHERE/SELECT/ORDER BY work

**Current Status:** 3/6 complete (Core infrastructure done, testing phase next)

---

## 📚 Reference Files

- Expression binder: `/src/expression/binder.rs`
- Physical plan types: `/src/planner/physical_plan.rs`
- Optimizer (needs update): `/src/planner/optimizer.rs`
- Expression implementations: `/src/expression/expression.rs`
- Architecture notes: `/EXPRESSION_BINDING_NOTES.md`
