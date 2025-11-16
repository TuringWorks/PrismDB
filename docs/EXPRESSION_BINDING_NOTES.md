# Expression Binding Implementation Notes

## Current Status

Expression binding implementation is IN PROGRESS. Core infrastructure has been added to `/src/expression/binder.rs` but needs refinement to match the actual AST structure.

## Architecture Understanding

### Parser AST Expressions (`parser::ast::Expression`)

Located in: `src/parser/ast.rs`

Key variants:

- `Literal(LiteralValue)` - includes Null, Boolean, Integer, Float, String, Date, Time, Timestamp, Blob
- `ColumnReference { table: Option<String>, column: String }`
- `FunctionCall { name, arguments, distinct }`
- `AggregateFunction { name, arguments, distinct }`
- `Cast { expression, data_type }`
- `Binary { left, operator, right }` - field is `operator` not `op`
- `Unary { operator, expression }` - field is `operator` not `op`

Binary Operators (from `ast::BinaryOperator`):

- Arithmetic: `Add`, `Subtract`, `Multiply`, `Divide`, `Modulo`
- Comparison: `Equals`, `NotEquals`, `LessThan`, `LessThanOrEqual`, `GreaterThan`, `GreaterThanOrEqual`
- Logical: `And`, `Or`

Unary Operators (from `ast::UnaryOperator`):

- `Plus`, `Minus`, `Not`, `IsNull`, `IsNotNull`

### Execution Expressions (`expression::expression::Expression` trait)

Located in: `src/expression/expression.rs`

Concrete implementations:

- `ConstantExpression` - holds a Value
- `ColumnRefExpression` - has column_index (usize), column_name (String)
- `FunctionExpression` - has function_name, return_type, children
- `CastExpression` - wraps an expression with target type
- `ComparisonExpression` - uses `ComparisonType` enum (Equal, NotEqual, LessThan, etc.)

Key difference: **Parser uses column names**, **Execution uses column indices**

### Type Conflict Issue

There are TWO Expression trait definitions:

1. `expression::expression::Expression` (in expression.rs) - has `children()` method
2. `expression::Expression` (in mod.rs) - simpler interface

The concrete types (ConstantExpression, etc.) implement #1, but the module exports both. This causes compilation issues when trying to create `Arc<dyn Expression>`.

**Solution**: Use `expression::expression::ExpressionRef` explicitly, or wrap in `ExpressionEnum`.

## Implementation Plan

### Phase 1: Core Binding Methods ✅

- [x] Add bind_expression() entry point
- [x] Add bind_literal()
- [x] Add bind_column_ref()
- [x] Add bind_function_call()
- [x] Add bind_cast()
- [x] Add bind_binary_op()
- [x] Add bind_unary_op()

### Phase 2: Fix Compilation Issues ⏳

- [ ] Update field names: `op` → `operator`
- [ ] Update operator variants: `Eq` → `Equals`, `Plus` → `Add`, etc.
- [ ] Handle all LiteralValue variants (Date, Time, Timestamp, Blob)
- [ ] Fix Expression trait import/path issues
- [ ] Update CastExpression::new() call signature

### Phase 3: Integrate with Planner ⏳

- [ ] Update PhysicalPlan to use execution expressions instead of AST expressions
- [ ] Update PhysicalFilter.predicate type
- [ ] Update PhysicalProjection.expressions type
- [ ] Update PhysicalSort.expressions type
- [ ] Create BinderContext from table schema in planner
- [ ] Call bind_expression() during physical plan creation

### Phase 4: Update Operators ⏳

- [ ] Remove expression evaluation workarounds in operators
- [ ] Use bound expressions directly
- [ ] Test Filter operator with bound expressions
- [ ] Test Projection operator with bound expressions
- [ ] Test Sort operator with bound expressions

## Key Insights

1. **Expression binding resolves column names to indices** - This is why ColumnRefExpression needs column_index
2. **Binding happens in the planner** - After parsing, before physical plan execution
3. **Binding requires schema context** - Need table schema to resolve column names
4. **Type checking happens during binding** - Function signatures validated, casts checked
5. **Comparison vs Arithmetic** - Use ComparisonExpression for ==, !=, <, >; FunctionExpression for +, -, *, /

## Next Steps

1. Fix the binder.rs compilation errors by updating to match actual AST structure
2. Test bind_expression() with simple examples
3. Update planner to use bound expressions
4. Verify end-to-end query flow with binding

## Code Location

- Binder implementation: `/src/expression/binder.rs`
- Parser AST: `/src/parser/ast.rs`
- Execution expressions: `/src/expression/expression.rs`
- Physical plan: `/src/planner/physical_plan.rs`
