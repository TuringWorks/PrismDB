# DuckDB C++ to DuckDBRS Porting Plan

**Date:** November 14, 2025
**Source:** DuckDB C++ Repository Analysis
**Objective:** Close the 4% feature parity gap by porting critical features from DuckDB C++

---

## 📊 Repository Analysis Summary

### Directories Explored

- ✅ `/benchmark/` - Comprehensive benchmark suite (TPC-H, ClickBench, IMDB, etc.)
- ✅ `/benchmark/tpch/` - 22 TPC-H queries organized by pattern
- ✅ `/examples/` - Embedded C/C++ examples
- ✅ `/data/` - Test datasets (CSV, JSON, Parquet, GeoParquet)
- ✅ `/tools/` - Shell, utilities, language bindings
- ✅ `/third_party/` - 30+ external dependencies
- ✅ `/src/function/aggregate/` - Aggregate function implementations

---

## 🎯 Priority 1: Missing Aggregate Functions (1-2 Weeks)

### Implementation Location in DuckDB C++

- **File:** `/src/function/aggregate/distributive/`
  - `first_last_any.cpp` - FIRST, LAST, ANY_VALUE
  - `minmax.cpp` - MIN, MAX, ARG_MIN, ARG_MAX

### Functions to Port (11 total)

#### Group 1: Ordered Aggregates (4 functions) - **3 days**

**Source File:** `first_last_any.cpp` (lines 1-350)

1. **FIRST**(arg) / **FIRST_VALUE**(arg)

   ```cpp
   template <class T>
   struct FirstState {
       T value;
       bool is_set;
       bool is_null;
   };

   // FIRST: Returns first value in group
   // LAST || !state.is_set determines update logic
   ```

   - **Difficulty:** Easy
   - **Rust Implementation:** `src/expression/aggregate.rs`
   - **Lines:** ~150 lines
   - **Time:** 1 day

2. **LAST**(arg) / **LAST_VALUE**(arg)

   ```cpp
   // Same as FIRST but with LAST=true template parameter
   // Updates on every row (overwrites previous value)
   ```

   - **Difficulty:** Easy (reuse FIRST structure)
   - **Time:** 0.5 days

3. **ARG_MIN**(arg, val)

   ```cpp
   // From minmax.cpp
   // Returns 'arg' at the row where 'val' is minimum
   // Used for queries like: "Which product (arg) had the lowest price (val)?"
   ```

   - **Difficulty:** Easy
   - **Implementation:** Track both arg and min_val
   - **Time:** 1 day

4. **ARG_MAX**(arg, val)

   ```cpp
   // Returns 'arg' at the row where 'val' is maximum
   ```

   - **Difficulty:** Easy (mirror of ARG_MIN)
   - **Time:** 0.5 days

**Total:** 4 functions, ~300 lines, 3 days

---

#### Group 2: Regression Functions (7 functions) - **2 days**

**Note:** DuckDB doesn't have separate regression function files. These are likely computed as expressions using existing aggregates or in an extension.

**Formula-based implementation using existing aggregates:**

1. **REGR_SLOPE**(y, x)

   ```rust
   // Formula: COVAR_POP(y, x) / VAR_POP(x)
   // Reuses: CovarPopState, VarianceState
   fn regr_slope_finalize(state: &RegrState) -> Value {
       let covar = covar_pop_finalize(&state.covar_state)?;
       let var_x = var_pop_finalize(&state.var_x_state)?;
       Value::Double(covar / var_x)
   }
   ```

   - **Difficulty:** Easy
   - **Time:** 0.5 days

2. **REGR_INTERCEPT**(y, x)

   ```rust
   // Formula: AVG(y) - REGR_SLOPE(y, x) * AVG(x)
   // Reuses: AvgState, RegrSlopeState
   ```

   - **Difficulty:** Easy
   - **Time:** 0.5 days

3. **REGR_R2**(y, x)

   ```rust
   // Formula: POWER(CORR(y, x), 2)
   // Reuses: CorrState
   fn regr_r2_finalize(state: &RegrR2State) -> Value {
       let corr = corr_finalize(&state.corr_state)?;
       Value::Double(corr * corr)
   }
   ```

   - **Difficulty:** Trivial
   - **Time:** 0.25 days

4. **REGR_COUNT**(y, x)

   ```rust
   // Count of non-null pairs (where both x and y are non-null)
   struct RegrCountState {
       count: usize
   }

   fn update(&mut self, x: &Value, y: &Value) {
       if !x.is_null() && !y.is_null() {
           self.count += 1;
       }
   }
   ```

   - **Difficulty:** Trivial
   - **Time:** 0.25 days

5. **REGR_AVGX**(y, x)

   ```rust
   // AVG(x) where y is not null
   // Similar to AvgState but filters on y
   ```

   - **Difficulty:** Trivial
   - **Time:** 0.25 days

6. **REGR_AVGY**(y, x)

   ```rust
   // AVG(y) where x is not null
   ```

   - **Difficulty:** Trivial
   - **Time:** 0.25 days

7. **REGR_SXY**(y, x)

   ```rust
   // Sum of products of deviations: SUM((x - AVG(x)) * (y - AVG(y)))
   // This is COVAR_POP(y, x) * COUNT(*)
   // Can reuse CovarPopState
   ```

   - **Difficulty:** Easy
   - **Time:** 0.5 days

**Total:** 7 functions, ~400 lines, 2.5 days

---

#### Group 3: Boolean Aggregates (2 functions) - **0.5 days**

1. **BOOL_AND**(arg)

   ```rust
   struct BoolAndState {
       result: bool  // Start with true
   }

   fn update(&mut self, value: &Value) {
       if let Value::Boolean(b) = value {
           self.result = self.result && b;
       }
   }
   ```

   - **Difficulty:** Trivial
   - **Time:** 0.25 days

2. **BOOL_OR**(arg)

   ```rust
   // Same as BOOL_AND but with OR operation
   ```

   - **Difficulty:** Trivial
   - **Time:** 0.25 days

**Total:** 2 functions, ~100 lines, 0.5 days

---

### Phase 1 Summary: Aggregate Functions

```text

Total Functions: 11
Total Lines: ~800 lines of Rust code
Total Time: 6 days (1 week + buffer)
Result: 98% → 99.5% aggregate compatibility
```

**Implementation Plan:**

1. **Day 1-2:** FIRST, LAST (with comprehensive tests)
2. **Day 3:** ARG_MIN, ARG_MAX (with comprehensive tests)
3. **Day 4-5:** 7 regression functions (REGR_*)
4. **Day 6:** Boolean aggregates + integration testing
5. **Day 7:** Buffer for edge cases, documentation

---

## 🎯 Priority 2: Critical Third-Party Dependencies (2-3 Weeks)

### Analysis of `/third_party/` Directory

#### Essential Dependencies Already Available in Rust

1. ✅ **hyperloglog** → Rust crate: `hyperloglogplus`
   - Already used for APPROX_COUNT_DISTINCT ✅

2. ❌ **tdigest** → Rust crate: `t-digest`
   - **For:** APPROX_QUANTILE aggregate
   - **Difficulty:** Medium (integration)
   - **Impact:** HIGH - approximate quantiles for large datasets
   - **Time:** 3 days

3. ❌ **jaro_winkler** → Rust crate: `strsim`
   - **For:** String similarity functions
     - `jaro_similarity(s1, s2)`
     - `jaro_winkler_similarity(s1, s2)`
     - `levenshtein(s1, s2)`
     - `damerau_levenshtein(s1, s2)`
     - `hamming(s1, s2)`
   - **Difficulty:** Easy (crate has all these)
   - **Impact:** MEDIUM - string matching/fuzzy search
   - **Time:** 2 days (5 functions)

4. ❌ **re2** (regex) → Rust: Built-in `regex` crate
   - **For:** Advanced regex functions
     - `regexp_extract(string, pattern, group)`
     - `regexp_matches(string, pattern)`
     - `regexp_replace(string, pattern, replacement)`
     - `regexp_split_to_array(string, pattern)`
   - **Difficulty:** Easy
   - **Impact:** HIGH - data cleaning, text processing
   - **Time:** 3 days (4 functions)

5. ❌ **yyjson** → Rust crate: `serde_json`, `json`
   - **For:** JSON type support (if implemented)
   - **Difficulty:** Hard (requires JSON type in type system)
   - **Impact:** HIGH - but blocked by type system
   - **Time:** 2 weeks (type system + functions)

6. ❌ **parquet** → Rust crate: `parquet`
   - **For:** Parquet file I/O
     - `COPY table TO 'file.parquet'`
     - `FROM 'file.parquet'`
   - **Difficulty:** Medium
   - **Impact:** HIGH - data interchange
   - **Time:** 1 week

7. ✅ **fmt** → Rust: std::fmt (built-in)
   - Already available ✅

8. ❌ **fast_float** → Rust crate: `fast-float`
   - **For:** Fast number parsing
   - **Difficulty:** Easy
   - **Impact:** LOW (optimization)
   - **Time:** 1 day

#### Specialized Dependencies (Lower Priority)

1. **pdqsort**, **ska_sort**, **vergesort** → Rayon already fast
   - Not needed - Rayon parallel sort is excellent

2. **fsst** (Fast Static Symbol Table) → Compression
    - **For:** String compression in storage
    - **Difficulty:** Hard
    - **Impact:** MEDIUM (storage optimization)
    - **Time:** 1 week

3. **httplib**, **mbedtls** → Rust: `reqwest`, `rustls`
    - **For:** HTTP/HTTPS remote file access
    - **Difficulty:** Medium
    - **Impact:** MEDIUM (cloud integration)
    - **Time:** 1 week

---

### Phase 2 Recommendation: Third-Party Integration

```text

Priority Order (by impact/effort ratio):
1. t-digest (APPROX_QUANTILE) - 3 days
2. regex functions - 3 days
3. strsim (string similarity) - 2 days
4. Parquet I/O - 1 week
5. JSON type + functions - 2 weeks (DEFER for now)

Quick Wins (1-2 weeks):
- APPROX_QUANTILE
- Regex functions (4 functions)
- String similarity (5 functions)

Result: +9 important functions in 2 weeks
```

---

## 🎯 Priority 3: SQL Features (1-2 Weeks)

### From DuckDB C++ `/src/parser/` and `/src/planner/`

#### High-Impact SQL Features

1. **QUALIFY Clause**
   - **What:** Filter on window function results
   - **Example:**

     ```sql
     SELECT * FROM table
     QUALIFY row_number() OVER (PARTITION BY id ORDER BY date DESC) = 1
     ```

   - **Instead of:**

     ```sql
     WITH ranked AS (
       SELECT *, row_number() OVER (PARTITION BY id ORDER BY date DESC) AS rn
       FROM table
     )
     SELECT * FROM ranked WHERE rn = 1
     ```

   - **Location in DuckDB C++:** `/src/parser/statement/select_statement.cpp`
   - **Difficulty:** Medium
   - **Impact:** HIGH (much simpler top-N queries)
   - **Time:** 2-3 days

2. **PIVOT / UNPIVOT**
   - **What:** Transform rows to columns and vice versa
   - **Example:**

     ```sql
     PIVOT products
     ON category
     USING SUM(revenue) AS total_revenue
     GROUP BY region
     ```

   - **Location:** `/src/parser/transform/statement/transform_pivot.cpp`
   - **Difficulty:** Hard
   - **Impact:** HIGH (data reshaping)
   - **Time:** 1 week

3. **Advanced Window Frames**
   - **What:** Custom frame specifications
   - **Current:** Basic unbounded frames
   - **Missing:**
     - `ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING`
     - `RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW`
     - `GROUPS BETWEEN 2 PRECEDING AND CURRENT ROW`
   - **Difficulty:** Medium
   - **Impact:** MEDIUM (advanced analytics)
   - **Time:** 3-4 days

4. **Recursive CTEs**
   - **What:** CTEs that reference themselves
   - **Example:**

     ```sql
     WITH RECURSIVE hierarchy AS (
       SELECT id, parent_id, name FROM tree WHERE parent_id IS NULL
       UNION ALL
       SELECT t.id, t.parent_id, t.name
       FROM tree t JOIN hierarchy h ON t.parent_id = h.id
     )
     SELECT * FROM hierarchy
     ```

   - **Difficulty:** Hard
   - **Impact:** HIGH (hierarchical queries, graph traversal)
   - **Time:** 1-2 weeks

5. **ASOF Joins**
   - **What:** Time-series nearest-match joins
   - **Example:**

     ```sql
     SELECT * FROM quotes q
     ASOF LEFT JOIN trades t
     ON q.timestamp >= t.timestamp AND q.symbol = t.symbol
     ```

   - **Difficulty:** Very Hard
   - **Impact:** HIGH (time-series analytics)
   - **Time:** 2 weeks

---

### Phase 3 Recommendation: SQL Features

```text

Week 1: QUALIFY clause (2-3 days) + Advanced window frames (3-4 days)
Week 2: PIVOT/UNPIVOT (if time allows)

Defer: Recursive CTEs, ASOF joins (very complex)

Result: 96% → 97.5% overall parity
```

---

## 🎯 Priority 4: Type System Expansion (1-3 Months)

### From DuckDB C++ `/src/common/types/`

#### Critical Missing Types

1. **DECIMAL Type (Arbitrary Precision)**
   - **Current:** Using f64 (limited precision)
   - **Needed:** Full DECIMAL(38, 10) support
   - **Rust Crate:** `rust_decimal` or `bigdecimal`
   - **Difficulty:** Medium
   - **Impact:** HIGH (financial calculations)
   - **Time:** 1 week
   - **Enables:** Exact arithmetic, financial queries

2. **ARRAY/LIST Type**
   - **DuckDB:** `INTEGER[]`, `LIST<VARCHAR>`
   - **Rust Type:** `Vec<Value>`
   - **Difficulty:** Hard (type system integration)
   - **Impact:** VERY HIGH
   - **Time:** 2 weeks
   - **Enables:**
     - `LIST(arg)` / `ARRAY_AGG(arg)` aggregates
     - 30+ array functions
     - Nested queries

3. **JSON Type**
   - **DuckDB:** Native JSON with indexing
   - **Rust Crate:** `serde_json`
   - **Difficulty:** Medium
   - **Impact:** VERY HIGH
   - **Time:** 1 week (basic), 2 weeks (with indexing)
   - **Enables:**
     - `JSON_AGG(arg)` aggregate
     - 20+ JSON functions
     - Semi-structured data

4. **MAP Type**
   - **DuckDB:** `MAP<VARCHAR, INTEGER>`
   - **Rust Type:** `HashMap<Value, Value>`
   - **Difficulty:** Medium
   - **Impact:** MEDIUM
   - **Time:** 1 week
   - **Enables:** 10+ map functions

5. **STRUCT Enhancements**
   - **Current:** Basic STRUCT support
   - **Missing:** Deep nesting, NULL handling
   - **Difficulty:** Medium
   - **Impact:** MEDIUM
   - **Time:** 3-4 days

6. **UNION Type**
   - **DuckDB:** `UNION(num INTEGER, str VARCHAR)`
   - **Difficulty:** Hard
   - **Impact:** LOW (niche use case)
   - **Time:** 1 week
   - **Defer:** Low priority

---

### Phase 4 Recommendation: Type System

```text

Month 1: DECIMAL type (1 week)
Month 2: ARRAY/LIST type (2 weeks) + ~30 array functions (2 weeks)
Month 3: JSON type (2 weeks) + ~20 JSON functions (2 weeks)

Defer: MAP, UNION (lower priority)

Result:
- 98% → 99.2% overall parity
- Unlocks ~50 new functions
```

---

## 🎯 Priority 5: Storage & I/O (2-4 Weeks)

### From DuckDB C++ `/src/storage/` and test data

#### File Format Support

1. **Parquet I/O**
   - **Read:** `FROM 'file.parquet'`
   - **Write:** `COPY table TO 'file.parquet'`
   - **Rust Crate:** `parquet` (Apache Arrow project)
   - **Difficulty:** Medium
   - **Impact:** VERY HIGH (data interchange)
   - **Time:** 1 week
   - **Test Data:** `/data/parquet-testing/` (258 test files)

2. **CSV Export**
   - **Current:** CSV reading may be implemented
   - **Missing:** `COPY table TO 'file.csv'`
   - **Difficulty:** Easy
   - **Impact:** MEDIUM
   - **Time:** 2 days

3. **JSON File I/O**
   - **Read:** `FROM 'file.json'`
   - **Write:** `COPY table TO 'file.json'`
   - **Requires:** JSON type support
   - **Difficulty:** Medium
   - **Impact:** HIGH
   - **Time:** 3 days
   - **Test Data:** `/data/json/` (57 test files)

4. **Remote Files (HTTP/S3)**
   - **Read:** `FROM 's3://bucket/file.parquet'`
   - **Requires:** HTTP client, AWS SDK
   - **Rust Crates:** `reqwest`, `aws-sdk-s3`
   - **Difficulty:** Medium
   - **Impact:** HIGH (cloud integration)
   - **Time:** 1 week

---

### Phase 5 Recommendation: Storage & I/O

```text

Week 1-2: Parquet I/O (highest impact)
Week 3: CSV export + basic JSON I/O
Week 4: Remote file access (if needed)

Result: Production-ready data pipeline integration
```

---

## 📊 Complete Porting Roadmap (12 Weeks to 99%+ Parity)

### Weeks 1-2: Quick Wins (Aggregates + String Functions)

- ✅ 11 aggregate functions (FIRST, LAST, ARG_MIN, ARG_MAX, REGR_*, BOOL_*)
- ✅ APPROX_QUANTILE (t-digest)
- ✅ 5 string similarity functions (jaro_winkler, levenshtein, etc.)
- ✅ 4 regex functions
- **Result:** 98% → 99.5% aggregates, +9 string functions

### Weeks 3-4: SQL Features

- ✅ QUALIFY clause
- ✅ Advanced window frames (ROWS BETWEEN, RANGE BETWEEN)
- ✅ Comprehensive tests
- **Result:** 96% → 97.5% overall parity

### Weeks 5-6: Parquet I/O + DECIMAL Type

- ✅ Parquet read/write
- ✅ DECIMAL type (arbitrary precision)
- ✅ Integration tests with test data
- **Result:** Production-ready data interchange

### Weeks 7-8: ARRAY/LIST Type

- ✅ ARRAY/LIST type implementation
- ✅ LIST() / ARRAY_AGG() aggregates
- ✅ 15 core array functions (slice, concat, contains, position, etc.)
- **Result:** Array operations enabled

### Weeks 9-10: JSON Type

- ✅ JSON type (string-backed with serde_json)
- ✅ JSON_AGG() aggregate
- ✅ 10 core JSON functions (extract, valid, array, object, etc.)
- **Result:** Semi-structured data support

### Weeks 11-12: PIVOT + Buffer

- ✅ PIVOT/UNPIVOT operators
- ✅ Additional array/JSON functions
- ✅ Bug fixes, edge cases, optimization
- ✅ Comprehensive documentation
- **Result:** 96% → 99%+ overall parity

---

## 🎯 Expected Outcomes

### After 2 Weeks (Quick Wins)

```text

Aggregate Functions:     99.5% (+11 functions)
String Functions:        +9 functions
SQL Features:            +2 features (QUALIFY, window frames)
Overall Parity:          96% → 98%
```

### After 6 Weeks (Mid-term)

```text

Type System:             +2 types (DECIMAL, ARRAY)
Aggregate Functions:     100% (LIST aggregate enabled)
Array Functions:         +15 functions
Storage:                 Parquet I/O complete
Overall Parity:          98% → 99%
```

### After 12 Weeks (Complete)

```text

Type System:             +3 types (DECIMAL, ARRAY, JSON)
Total New Functions:     60+ functions
SQL Features:            +4 features
Storage:                 Full Parquet support
Overall Parity:          99%+
Production Ready:        ✅ YES for 99% of workloads
```

---

## 📚 Key Resources from DuckDB C++

### Implementation References

**Aggregates:**

- `/src/function/aggregate/distributive/first_last_any.cpp` (358 lines)
- `/src/function/aggregate/distributive/minmax.cpp` (800+ lines)
- `/src/function/aggregate/distributive/count.cpp`

**Benchmarks:**

- `/benchmark/tpch/sf1/` - All 22 TPC-H queries
- `/benchmark/micro/` - Micro-benchmarks for individual features
- `/benchmark/clickbench/` - ClickBench queries

**Test Data:**

- `/data/csv/` - 208 CSV test files
- `/data/parquet-testing/` - 258 Parquet test files
- `/data/json/` - 57 JSON test files

**Third-Party:**

- `/third_party/tdigest/` - APPROX_QUANTILE
- `/third_party/jaro_winkler/` - String similarity
- `/third_party/re2/` - Regex engine
- `/third_party/parquet/` - Parquet I/O
- `/third_party/yyjson/` - JSON parsing

---

## 🚀 Immediate Next Steps

### This Week

1. **Start aggregate function implementation**
   - FIRST/LAST (Day 1-2)
   - ARG_MIN/ARG_MAX (Day 3)
   - 7 REGR_ functions (Day 4-5)

2. **Set up test framework**
   - Port relevant tests from DuckDB
   - Use test data from `/data/csv/`

3. **Parallel track: Third-party integration**
   - Add `t-digest` crate for APPROX_QUANTILE
   - Add `strsim` crate for string similarity

### Next Week

1. **Complete aggregate functions**
2. **Implement string similarity functions**
3. **Start QUALIFY clause implementation**

---

## 📈 Success Metrics

### Code Quality

- ✅ 100% test pass rate maintained
- ✅ Zero unsafe code
- ✅ Comprehensive documentation
- ✅ Performance benchmarks

### Feature Parity

- 🎯 Week 2: 98% overall parity
- 🎯 Week 6: 99% overall parity
- 🎯 Week 12: 99%+ overall parity

### Production Readiness

- ✅ Parquet I/O
- ✅ Complete aggregate suite
- ✅ Advanced SQL features
- ✅ Type system expansion

---

**Document Status:** Complete
**Next Action:** Begin Phase 1 implementation (aggregate functions)
**Target:** 99%+ DuckDB feature parity in 12 weeks

---

*Generated from DuckDB C++ repository analysis*
*Source Repository: /Users/ravindraboddipalli/sources/git/duckdb*
*Target: DuckDBRS (/Users/ravindraboddipalli/sources/git/duckdbrs)*
*Date: November 14, 2025*
