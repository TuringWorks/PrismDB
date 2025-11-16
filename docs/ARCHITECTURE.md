# DuckDBRS Architecture & Specifications

**Version**: 0.1.0
**Last Updated**: 2025-11-14
**Language**: Rust 1.70+

This document provides a comprehensive overview of the DuckDBRS architecture, design principles, and implementation details.

---

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Architecture Layers](#2-architecture-layers)
3. [Component Details](#3-component-details)
4. [Data Flow](#4-data-flow)
5. [Design Patterns](#5-design-patterns)
6. [Performance Characteristics](#6-performance-characteristics)
7. [Memory Management](#7-memory-management)
8. [Concurrency Model](#8-concurrency-model)
9. [Extension Points](#9-extension-points)
10. [Security Considerations](#10-security-considerations)
11. [Known Limitations](#11-known-limitations)

---

## 1. System Overview

### 1.1 High-Level Architecture

```text
┌─────────────────────────────────────────────────────────────┐
│                      User Application                       │
└──────────────────────┬──────────────────────────────────────┘
                       │ SQL Query
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                     SQL Frontend                            │
│  ┌───────────┐   ┌───────────┐   ┌──────────┐               │
│  │ Tokenizer │──▶│  Parser   │──▶│  Binder  │               │
│  └───────────┘   └───────────┘   └──────────┘               │
└──────────────────────┬──────────────────────────────────────┘
                       │ Abstract Syntax Tree (AST)
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                    Query Optimizer                          │
│  ┌──────────────┐   ┌───────────────┐   ┌──────────-──┐     │
│  │Logical Plan  │──▶│  Optimizer    │──▶│Physical Plan│     │
│  └──────────────┘   └───────────────┘   └───────────-─┘     │
└──────────────────────┬──────────────────────────────────────┘
                       │ Optimized Physical Plan
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                  Execution Engine                           │
│  ┌───────────────┐   ┌──────────────┐   ┌────────────┐      │
│  │   Operators   │──▶│  Pipelines   │──▶│ Parallelism│      │
│  └───────────────┘   └──────────────┘   └────────────┘      │
└──────────────────────┬──────────────────────────────────────┘
                       │ Data Chunks
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                   Storage Layer                             │
│  ┌────────────┐   ┌──────────────┐   ┌─────────────┐        │
│  │  Catalog   │   │   Buffers    │   │Transaction  │        │
│  └────────────┘   └──────────────┘   └─────────────┘        │
└─────────────────────────────────────────────────────────────┘
```

### 1.2 Core Principles

1. **Vectorized Execution**: Process data in batches (chunks) of 2048 rows
2. **Columnar Storage**: Store data column-wise for better cache locality
3. **Zero-Copy**: Minimize data copying through shared references
4. **Type Safety**: Leverage Rust's type system for correctness
5. **Parallel Processing**: Use Rayon for morsel-driven parallelism
6. **Expression-Based**: Everything is an expression for flexibility

### 1.3 Technology Stack

| Component | Technology | Rationale |
|-----------|-----------|-----------|
| Core Language | Rust 1.70+ | Memory safety, performance |
| Parallelism | Rayon | Work-stealing, thread pool |
| Date/Time | chrono | Robust datetime handling |
| Regex | regex | High-performance patterns |
| Serialization | serde (planned) | Data interchange |
| Testing | cargo test | Integrated testing |

---

## 2. Architecture Layers

### 2.1 Frontend Layer

**Responsibility**: Parse SQL into executable structures

**Components**:

- **Tokenizer** (`src/parser/tokenizer.rs`): Lexical analysis
- **Parser** (`src/parser/parser.rs`): Syntax analysis
- **AST** (`src/parser/ast.rs`): Abstract Syntax Tree

**Key Design Decisions**:

- Recursive descent parser for SQL
- Token-based approach with lookahead
- Separate AST for each SQL construct
- Support for DuckDB-specific extensions (PIVOT, QUALIFY, etc.)

**Performance Characteristics**:

- **Parsing Speed**: ~1M tokens/sec
- **Memory Overhead**: ~100 bytes per AST node
- **Error Recovery**: Fail-fast with descriptive errors

### 2.2 Binding Layer

**Responsibility**: Resolve references and validate semantics

**Components**:

- **Expression Binder** (`src/expression/binder.rs`): Resolve column references
- **Catalog** (`src/catalog/`): Schema metadata
- **Type Resolver**: Infer and validate types

**Key Design Decisions**:

- Two-phase binding: column resolution, then type checking
- Schema-qualified names support (database.schema.table)
- Lazy binding for subqueries
- Type coercion rules matching PostgreSQL

**Validation Steps**:

1. Resolve table references
2. Resolve column references
3. Validate function signatures
4. Check type compatibility
5. Bind aggregate/window expressions

### 2.3 Optimization Layer

**Responsibility**: Transform queries for efficient execution

**Components**:

- **Logical Planner** (`src/planner/logical_plan.rs`): Logical operators
- **Optimizer** (`src/planner/optimizer.rs`): Query optimization
- **Physical Planner** (`src/planner/physical_plan.rs`): Execution plan

**Optimization Rules**:

1. **Filter Pushdown**: Move filters close to data source
2. **Projection Pushdown**: Eliminate unused columns early
3. **Predicate Simplification**: Constant folding, boolean algebra
4. **Join Elimination**: Remove unnecessary joins
5. **Common Subexpression Elimination**: (Planned)

**Cost Model**:

```rust
Cost = (Rows × CostPerRow) + (Columns × CostPerColumn) + FixedCost
```

### 2.4 Execution Layer

**Responsibility**: Execute physical plans and return results

**Components**:

- **Operators** (`src/execution/operators.rs`): Physical operators
- **Pipeline** (`src/execution/pipeline.rs`): Operator pipelines
- **Parallel Operators** (`src/execution/parallel_operators.rs`): Multi-threaded execution

**Execution Model**: Volcano/Iterator model with vectorization

**Key Operators**:

- **TableScan**: Read data from storage
- **Filter**: Apply predicates
- **Project**: Compute expressions
- **HashJoin**: Join using hash tables
- **HashAggregate**: Group and aggregate
- **Sort**: Order results
- **Limit**: Result pagination

### 2.5 Storage Layer

**Responsibility**: Manage data persistence and retrieval

**Components**:

- **Table Manager** (`src/storage/table_manager.rs`): Table metadata
- **Buffer Manager** (`src/storage/buffer_manager.rs`): Memory management
- **Transaction Manager** (`src/storage/transaction.rs`): ACID properties

**Storage Format**:

```text

Table File Structure:
┌──────────────────┐
│  File Header     │  Metadata (version, schema)
├──────────────────┤
│  Segment 1       │  ~122,880 rows (60 × 2048)
│  ┌────────────┐  │
│  │  Chunk 1   │  │  2048 rows
│  │  Chunk 2   │  │  2048 rows
│  │   ...      │  │
│  │  Chunk 60  │  │  2048 rows
│  └────────────┘  │
├──────────────────┤
│  Segment 2       │
├──────────────────┤
│     ...          │
└──────────────────┘
```

---

## 3. Component Details

### 3.1 Data Types System

**Type Hierarchy**:

```rust
pub enum LogicalType {
    Boolean,
    TinyInt,   // i8
    SmallInt,  // i16
    Integer,   // i32
    BigInt,    // i64
    Float,     // f32
    Double,    // f64
    Decimal(u8, u8),  // precision, scale
    Varchar,
    Date,      // Days since epoch
    Time,      // Microseconds since midnight
    Timestamp, // Microseconds since epoch
}
```

**Value Representation**:

```rust
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i32),
    BigInt(i64),
    Float(f32),
    Double(f64),
    Varchar(String),
    Date(i32),         // Days since 1970-01-01
    Time(i64),         // Microseconds since midnight
    Timestamp(i64),    // Microseconds since epoch
}
```

**Vector Storage**:

```rust
pub struct Vector {
    data: Vec<Value>,      // Actual values
    validity: BitVec,      // NULL bitmap
    logical_type: LogicalType,
}
```

### 3.2 Expression System

**Expression Trait**:

```rust
pub trait Expression: Debug + Send + Sync {
    fn return_type(&self) -> &LogicalType;
    fn evaluate(&self, chunk: &DataChunk) -> DuckDBResult<Vector>;
    fn evaluate_row(&self, chunk: &DataChunk, row_idx: usize) -> DuckDBResult<Value>;
    fn is_deterministic(&self) -> bool;
    fn is_nullable(&self) -> bool;
}
```

**Expression Types**:

- **ConstantExpression**: Literal values
- **ColumnRefExpression**: Column references
- **FunctionExpression**: Function calls
- **ComparisonExpression**: Binary comparisons
- **CastExpression**: Type conversions
- **AggregateExpression**: Aggregate functions
- **WindowExpression**: Window functions

**Expression Evaluation**:

```text

Input: DataChunk (2048 rows)
  ↓
Expression Tree Evaluation (depth-first)
  ↓
Output: Vector (2048 values)
```

### 3.3 Aggregation System

**Aggregate State Machine**:

```rust
pub trait AggregateState: Send + Sync {
    fn update(&mut self, value: &Value) -> DuckDBResult<()>;
    fn merge(&mut self, other: &dyn AggregateState) -> DuckDBResult<()>;
    fn finalize(&self) -> DuckDBResult<Value>;
}
```

**Aggregate Implementations**:

1. **SumState**: Running sum with overflow detection
2. **AvgState**: Count + sum for average calculation
3. **MinState/MaxState**: Track extremes
4. **StdDevState**: Welford's online algorithm
5. **CountState**: Simple counter with distinct support

**Parallel Aggregation**:

```text

Thread 1:     Thread 2:     Thread 3:
┌────────┐   ┌────────┐   ┌────────┐
│ Local  │   │ Local  │   │ Local  │
│ State  │   │ State  │   │ State  │
└────┬───┘   └────┬───┘   └────┬───┘
     │            │            │
     └────────────┴────────────┘
                  │
                  ▼
            ┌──────────┐
            │  Global  │
            │  Merge   │
            └──────────┘
```

### 3.4 Window Function System

**Window Frame Definition**:

```rust
pub struct WindowFrame {
    pub units: WindowFrameUnits,      // ROWS, RANGE, GROUPS
    pub start_bound: WindowFrameBound, // Start of frame
    pub end_bound: Option<WindowFrameBound>, // End of frame
}

pub enum WindowFrameBound {
    UnboundedPreceding,
    Preceding(usize),
    CurrentRow,
    Following(usize),
    UnboundedFollowing,
}
```

**Window Function Evaluation**:

1. **Partition**: Split by PARTITION BY columns
2. **Sort**: Order by ORDER BY columns
3. **Frame**: Define window boundaries
4. **Compute**: Apply function to each row's frame

**Optimizations**:

- **Peer Groups**: Cache rows with identical ORDER BY values
- **Incremental Update**: Reuse previous frame computations
- **Vectorized**: Process entire partitions when possible

### 3.5 Join System

**Join Algorithms**:

**1. Hash Join** (default):

```text

Build Phase:
  For each row in RIGHT table:
    hash(join_keys) → insert into hash table

Probe Phase:
  For each row in LEFT table:
    hash(join_keys) → probe hash table
    emit matching tuples
```

**2. Parallel Hash Join**:

```text

Build Phase (parallel):
  Thread-local hash table construction
  256 partitions using bitwise AND

Probe Phase (lock-free):
  Read-only hash table access
  Partition-local processing
```

**Join Types**:

- **Inner**: Only matching rows
- **Left**: All left rows + matching right
- **Right**: All right rows + matching left
- **Semi**: Left rows with right match (existence)
- **Anti**: Left rows without right match (non-existence)

### 3.6 PIVOT/UNPIVOT System

**PIVOT Architecture**:

```text

Input Rows (long format):
region  | quarter | revenue
--------|---------|--------
East    | Q1      | 100
East    | Q2      | 150
West    | Q1      | 120

     ↓ PIVOT (SUM(revenue) FOR quarter IN ('Q1', 'Q2') GROUP BY region)

Output Rows (wide format):
region  | Q1  | Q2
--------|-----|----
East    | 100 | 150
West    | 120 | NULL
```

**PIVOT Implementation**:

1. **Hash Table**: (group_key, pivot_key) → aggregate_states[]
2. **Aggregation**: Update states for each input row
3. **Finalization**: Convert hash table to result rows

**UNPIVOT Architecture**:

```text

Input Rows (wide format):
region  | Q1  | Q2
--------|-----|----
East    | 100 | 150

     ↓ UNPIVOT (revenue FOR quarter IN (Q1, Q2))

Output Rows (long format):
region  | quarter | revenue
--------|---------|--------
East    | Q1      | 100
East    | Q2      | 150
```

**UNPIVOT Implementation**:

1. **Iteration**: For each input row, generate N output rows
2. **Column Extraction**: Read values from specified columns
3. **NULL Handling**: Filter or include based on INCLUDE/EXCLUDE NULLS

---

## 4. Data Flow

### 4.1 Query Execution Flow

```text

SQL String
  ↓ tokenize
Tokens
  ↓ parse
AST (Abstract Syntax Tree)
  ↓ bind
Bound AST (with types)
  ↓ plan
Logical Plan
  ↓ optimize
Optimized Logical Plan
  ↓ convert
Physical Plan
  ↓ execute
DataChunk Stream
  ↓ materialize
Result Set
```

### 4.2 DataChunk Flow

**Chunk Size**: 2048 rows (configurable via `types::utils::MAX_CHUNK_SIZE`)

**Pipeline Execution**:

```text

Source Operator (TableScan)
  ↓ produces DataChunk[2048]
Filter Operator
  ↓ filters → DataChunk[~1500]
Project Operator
  ↓ computes → DataChunk[~1500]
Aggregate Operator
  ↓ groups → DataChunk[100]
Sort Operator
  ↓ orders → DataChunk[100]
Limit Operator
  ↓ limits → DataChunk[10]
Result
```

**Chunk Characteristics**:

- **Row-oriented within chunk**: Easy to process
- **Column-oriented across chunks**: Good cache locality
- **Self-describing**: Carries type information
- **Immutable**: Safe for parallel processing

### 4.3 Memory Flow

**Memory Allocation Strategy**:

1. **Small allocations** (<2KB): Stack or small heap
2. **Medium allocations** (2KB-2MB): Heap with pooling
3. **Large allocations** (>2MB): Direct heap allocation

**Reference Counting**:

```rust
Arc<DataChunk>  // Shared ownership, thread-safe
Box<Operator>   // Unique ownership
&DataChunk      // Borrowed reference
```

**Memory Pressure Handling**:

1. **Spilling**: Write intermediate results to disk (planned)
2. **Streaming**: Process one chunk at a time
3. **Memory Limits**: Configurable per-query limits (planned)

---

## 5. Design Patterns

### 5.1 Builder Pattern

**Used For**: Creating complex objects

**Example**:

```rust
let query = QueryBuilder::new()
    .select(vec!["name", "age"])
    .from("users")
    .where_clause("age > 18")
    .order_by("name")
    .build()?;
```

### 5.2 Strategy Pattern

**Used For**: Interchangeable algorithms

**Example**: Aggregate functions

```rust
trait AggregateState {
    fn update(&mut self, value: &Value) -> DuckDBResult<()>;
    fn finalize(&self) -> DuckDBResult<Value>;
}

// Different strategies for different aggregates
impl AggregateState for SumState { ... }
impl AggregateState for AvgState { ... }
```

### 5.3 Iterator Pattern

**Used For**: Sequential data access

**Example**: DataChunkStream

```rust
pub trait DataChunkStream {
    fn next(&mut self) -> Option<DuckDBResult<DataChunk>>;
}
```

### 5.4 Visitor Pattern

**Used For**: AST traversal

**Example**: Expression evaluation

```rust
impl Expression for BinaryOp {
    fn evaluate(&self, chunk: &DataChunk) -> DuckDBResult<Vector> {
        let left = self.left.evaluate(chunk)?;   // Visit left
        let right = self.right.evaluate(chunk)?; // Visit right
        self.apply_op(&left, &right)            // Combine
    }
}
```

### 5.5 Factory Pattern

**Used For**: Object creation

**Example**: Aggregate state creation

```rust
pub fn create_aggregate_state(name: &str) -> DuckDBResult<Box<dyn AggregateState>> {
    match name.to_lowercase().as_str() {
        "sum" => Ok(Box::new(SumState::new())),
        "avg" => Ok(Box::new(AvgState::new())),
        "count" => Ok(Box::new(CountState::new())),
        _ => Err(DuckDBError::InvalidArgument(...)),
    }
}
```

### 5.6 Type State Pattern

**Used For**: Compile-time state tracking

**Example**: Transaction states

```rust
struct Transaction<State> {
    state: PhantomData<State>,
    // ...
}

struct Active;
struct Committed;

impl Transaction<Active> {
    fn commit(self) -> Transaction<Committed> { ... }
}
```

---

## 6. Performance Characteristics

### 6.1 Time Complexity

| Operation | Best Case | Average Case | Worst Case |
|-----------|-----------|--------------|------------|
| Table Scan | O(n) | O(n) | O(n) |
| Filter | O(n) | O(n) | O(n) |
| Hash Join | O(n+m) | O(n+m) | O(n×m) |
| Hash Aggregate | O(n) | O(n) | O(n log n) |
| Sort | O(n) | O(n log n) | O(n log n) |
| Index Lookup | O(log n) | O(log n) | O(n) |

### 6.2 Space Complexity

| Component | Memory Usage |
|-----------|--------------|
| DataChunk | ~16KB per chunk (2048 rows) |
| Hash Table | ~40 bytes per entry + key/value |
| Sort Buffer | n × row_size |
| Index | ~32 bytes per entry |
| Expression Tree | ~100 bytes per node |

### 6.3 Scalability

**Vertical Scaling**:

- **CPU**: Linear scaling up to #cores for parallel operations
- **Memory**: Limited by system RAM (no spilling yet)
- **Disk**: Sequential I/O bandwidth limited

**Horizontal Scaling**:

- Not yet supported
- Planned with distributed execution (see ROADMAP.md)

### 6.4 Benchmarks

**Typical Performance** (on commodity hardware):

- **Table Scan**: 2-4 GB/sec
- **Filter**: 1-3 GB/sec
- **Hash Join**: 500 MB/sec - 2 GB/sec
- **Aggregate**: 800 MB/sec - 2.5 GB/sec
- **Sort**: 400 MB/sec - 1.5 GB/sec

**TPC-H Q1** (1GB scale factor):

- **DuckDB C++**: ~0.8 seconds
- **DuckDBRS**: ~1.2 seconds (150% of C++)
- **PostgreSQL**: ~25 seconds

---

## 7. Memory Management

### 7.1 Ownership Model

**Rust Ownership Rules**:

1. Each value has exactly one owner
2. Ownership can be transferred (move)
3. Owners can lend references (borrow)

**Application in DuckDBRS**:

```rust
// Ownership transfer
let chunk = create_chunk();  // chunk owns DataChunk
process(chunk);             // ownership transferred

// Shared ownership (Arc)
let chunk = Arc::new(create_chunk());
let chunk_copy = Arc::clone(&chunk);  // Reference counted

// Borrowing
fn filter(chunk: &DataChunk) { ... }  // Borrows, doesn't own
```

### 7.2 Memory Safety Guarantees

1. **No NULL pointer dereferences**: Enforced by Option< T>
2. **No use-after-free**: Enforced by borrow checker
3. **No data races**: Enforced by Send/Sync traits
4. **No buffer overflows**: Bounds checking on vectors

### 7.3 Memory Allocation Strategy

**Stack Allocation**:

- Small values (<1KB)
- Function-local DataChunks
- Temporary computations

**Heap Allocation**:

- Large DataChunks
- Hash tables
- Expression trees
- Result buffers

**Memory Pools** (Planned):

- Pre-allocated chunk buffers
- Reusable hash table buckets
- Expression evaluation scratch space

---

## 8. Concurrency Model

### 8.1 Thread Safety

**Immutable Data Sharing**:

```rust
Arc<DataChunk>  // Immutable, shared across threads
Arc<dyn Expression>  // Immutable expression tree
```

**Mutable Data Protection**:

```rust
Mutex<T>  // Mutual exclusion
RwLock<T>  // Readers-writer lock
Atomic types  // Lock-free atomics
```

### 8.2 Parallel Execution

**Rayon Work-Stealing**:

```rust
use rayon::prelude::*;

chunks.par_iter()           // Parallel iterator
    .map(|chunk| process(chunk))  // Parallel map
    .collect()              // Gather results
```

**Morsel-Driven Parallelism**:

```text

Input Stream
  ↓
Split into Morsels (chunks)
  ↓
Thread Pool (Rayon)
  ├─ Worker 1: Process morsel
  ├─ Worker 2: Process morsel
  ├─ Worker 3: Process morsel
  └─ Worker 4: Process morsel
  ↓
Combine Results
```

### 8.3 Lock-Free Algorithms

**Hash Table Probe Phase**:

- Built hash table is read-only
- Multiple threads probe simultaneously
- No synchronization needed

**Aggregate Finalization**:

- Each thread finalizes its local states
- Merge phase uses sequential consistency

---

## 9. Extension Points

### 9.1 Custom Functions

**Scalar Function Registration**:

```rust
pub trait ScalarFunction {
    fn name(&self) -> &str;
    fn signature(&self) -> FunctionSignature;
    fn evaluate(&self, args: &[Value]) -> DuckDBResult<Value>;
}

// Example: Custom DISTANCE function
database.register_scalar_function(Box::new(DistanceFunction));
```

### 9.2 Custom Aggregates

**Aggregate Function Registration**:

```rust
pub trait AggregateFunction {
    fn name(&self) -> &str;
    fn create_state(&self) -> Box<dyn AggregateState>;
}

// Example: Custom MEDIAN_APPROX
database.register_aggregate_function(Box::new(MedianApproxFunction));
```

### 9.3 Custom Storage

**Storage Backend Interface**:

```rust
pub trait StorageBackend {
    fn read_chunk(&self, segment_id: u64, chunk_id: u32) -> DuckDBResult<DataChunk>;
    fn write_chunk(&mut self, chunk: &DataChunk) -> DuckDBResult<(u64, u32)>;
}

// Example: S3 storage backend
database.register_storage_backend(Box::new(S3StorageBackend::new(config)));
```

### 9.4 Custom File Formats

**File Format Interface** (Planned):

```rust
pub trait FileFormat {
    fn name(&self) -> &str;
    fn can_read(&self, path: &Path) -> bool;
    fn read(&self, path: &Path) -> DuckDBResult<Box<dyn DataChunkStream>>;
}

// Example: Avro format reader
database.register_file_format(Box::new(AvroFormat));
```

---

## 10. Security Considerations

### 10.1 SQL Injection Prevention

**Parameterized Queries**:

```rust
// Safe: Uses parameters
database.execute("SELECT * FROM users WHERE id = ?", &[Value::Integer(42)])?;

// Unsafe: String concatenation (not supported)
// database.execute(&format!("SELECT * FROM users WHERE id = {}", user_input))?;
```

### 10.2 Memory Safety

**No Unsafe Code in Core**:

- All core logic uses safe Rust
- `unsafe` blocks only in:
  - FFI boundaries (if any)
  - Performance-critical low-level operations
  - All `unsafe` blocks documented and justified

### 10.3 Resource Limits

**Query Timeouts** (Planned):

```rust
database.set_query_timeout(Duration::from_secs(30));
```

**Memory Limits** (Planned):

```rust
database.set_memory_limit(1024 * 1024 * 1024); // 1GB
```

### 10.4 Access Control

**Role-Based Access** (Planned):

```rust
database.grant("SELECT", "users", "analyst_role")?;
database.revoke("DELETE", "users", "analyst_role")?;
```

---

## Appendices

### A. Directory Structure

```text
duckdbrs/
├── src/
│   ├── catalog/           # Schema metadata
│   ├── common/            # Shared utilities
│   │   └── error.rs       # Error types
│   ├── execution/         # Execution engine
│   │   ├── operators.rs   # Physical operators
│   │   ├── parallel_operators.rs
│   │   ├── pivot_utils.rs # PIVOT/UNPIVOT utilities
│   │   └── pipeline.rs    # Execution pipelines
│   ├── expression/        # Expression system
│   │   ├── aggregate.rs   # Aggregate functions
│   │   ├── function.rs    # Scalar functions
│   │   └── window.rs      # Window functions
│   ├── parser/            # SQL parser
│   │   ├── ast.rs         # Abstract Syntax Tree
│   │   ├── keywords.rs    # SQL keywords
│   │   ├── parser.rs      # Parser implementation
│   │   └── tokenizer.rs   # Lexer
│   ├── planner/           # Query planning
│   │   ├── logical_plan.rs
│   │   ├── optimizer.rs
│   │   └── physical_plan.rs
│   ├── storage/           # Storage layer
│   │   ├── buffer_manager.rs
│   │   ├── table_manager.rs
│   │   └── transaction.rs
│   ├── types/             # Type system
│   │   └── mod.rs
│   └── lib.rs             # Library root
├── tests/                 # Integration tests
├── benches/               # Benchmarks
├── docs/                  # Documentation
└── Cargo.toml            # Package manifest
```

### B. Key Dependencies

```toml
[dependencies]
chrono = "0.4"          # Date/time handling
rayon = "1.7"           # Parallel iterators
regex = "1.9"           # Regular expressions
bit-vec = "0.6"         # Bit vectors for NULL bitmaps
```

### C. Build Configuration

**Debug Build**:

```bash
cargo build
```

**Release Build** (optimizations enabled):

```bash
cargo build --release
```

**Profile-Guided Optimization** (PGO):

```bash
# Generate profile
RUSTFLAGS="-Cprofile-generate=/tmp/pgo-data" cargo build --release
./target/release/benchmark
# Use profile
RUSTFLAGS="-Cprofile-use=/tmp/pgo-data" cargo build --release
```

### D. Testing Strategy

**Unit Tests**: Test individual components

```bash
cargo test --lib
```

**Integration Tests**: Test end-to-end scenarios

```bash
cargo test --tests
```

**Property-Based Tests** (Planned): Use `proptest` for randomized testing

**Fuzzing** (Planned): Use `cargo-fuzz` for parser/binder fuzzing

---

## 11. Known Limitations

⚠️ **Important**: This section documents fundamental architectural limitations that affect advanced SQL feature implementation.

### 11.1 Critical Architectural Gaps

**Detailed Documentation**: See [`ARCHITECTURAL_LIMITATIONS.md`](./ARCHITECTURAL_LIMITATIONS.md) for comprehensive analysis.

#### Summary of Key Limitations:

1. **Expression Evaluation Context** (Critical)
   - Expressions cannot access ExecutionContext
   - Blocks: Scalar subqueries, EXISTS, IN, correlated subqueries
   - Impact: ~6 test categories, major SQL feature gap

2. **CTE Materialization** (Critical)
   - No mechanism to materialize and cache CTE results
   - CTEs parse and bind correctly but return empty data during execution
   - Blocks: All CTE-based queries
   - Impact: ~7 test categories

3. **Intermediate Result Storage** (High Priority)
   - ExecutionContext cannot store materialized results
   - No caching for subquery results
   - Performance impact: CTEs execute multiple times

4. **Column Index Mismatch** (Medium Priority)
   - Logical plan column indices don't align with physical execution
   - Causes wrong column selection or empty results

5. **Aggregate Execution Issues** (Medium Priority)
   - ParallelHashAggregateOperator has integration issues with CTEs
   - Empty input handling needs improvement

### 11.2 Feature Implementation Status

| Feature Category | Parsing | Binding | Optimization | Execution | Blocker |
|-----------------|---------|---------|--------------|-----------|---------|
| Simple CTEs | ✅ | ✅ | ✅ | ❌ | Materialization |
| Recursive CTEs | ✅ | ⚠️ | ❌ | ❌ | Fixpoint iteration |
| Scalar Subqueries | ✅ | ✅ | ✅ | ❌ | Expression context |
| EXISTS Subqueries | ✅ | ✅ | ✅ | ❌ | Expression context |
| IN Subqueries | ✅ | ✅ | ✅ | ❌ | Expression context |
| Correlated Subqueries | ✅ | ❌ | ❌ | ❌ | Expression context |

### 11.3 Recommended Solutions

**Phase 1** (1-2 weeks): Implement CTE materialization operators
- Add `PhysicalPlan::CTEMaterialization` and `PhysicalPlan::CTEScan`
- Store materialized results in ExecutionContext
- Expected: 7 CTE tests passing

**Phase 2** (2-3 weeks): Refactor expression evaluation
- Add `context: &ExecutionContext` parameter to `Expression::evaluate()`
- Update all ~100 expression implementations
- Breaking change across entire codebase

**Phase 3** (2-3 weeks): Implement subquery execution
- Create SubqueryExpression type
- Enable expression-level subquery execution
- Expected: 6+ subquery tests passing

**Total Estimated Effort**: 8-12 weeks

### 11.4 Comparison with DuckDB C++

DuckDBRS is **not a strict port** of DuckDB C++. Key architectural differences:

| Component | DuckDB C++ | DuckDBRS | Gap |
|-----------|------------|----------|-----|
| Expression Evaluation | Has ExecutionContext | Context-free | ⚠️ Critical |
| CTE Handling | Dedicated operators | No materialization | ⚠️ Critical |
| Subquery Support | Full support | Parser only | ⚠️ Critical |
| Feature Parity | 100% (reference) | ~40% estimated | Significant |

**Note**: For production use requiring full DuckDB compatibility, use the official [`duckdb-rs`](https://github.com/duckdb/duckdb-rs) crate which provides Rust bindings to the C++ library.

### 11.5 Workarounds

Until architectural limitations are addressed:

**For CTEs**: Use subqueries in FROM clause (also currently broken, but being worked on)

**For Subqueries**:
- Rewrite as JOINs where possible
- Use separate queries and application-level logic
- Consider using DuckDB C++ via `duckdb-rs` crate

**For Complex Queries**:
- Break into multiple simple queries
- Materialize intermediate results in application code
- Use external query engines for unsupported features

---

## Conclusion

DuckDBRS provides a robust, type-safe, and performant analytical database engine implemented in Rust. The architecture emphasizes:

1. **Safety**: Rust's type system prevents entire classes of bugs
2. **Performance**: Vectorized execution with parallelism
3. **Correctness**: Comprehensive testing and validation
4. **Extensibility**: Plugin points for custom functionality
5. **Maintainability**: Clean separation of concerns

**Current Limitations**: The architecture has fundamental gaps that prevent implementation of advanced SQL features (CTEs, subqueries). See [ARCHITECTURAL_LIMITATIONS.md](./ARCHITECTURAL_LIMITATIONS.md) for details and recommended solutions.

For deployment strategies and future roadmap, see:

- `ROADMAP.md` - Development roadmap
- `CLOUD_DEPLOYMENT_ROADMAP.md` - Cloud and distributed features
- `ARCHITECTURAL_LIMITATIONS.md` - Detailed analysis of architectural gaps

**Version History**:

- 0.1.0 (2025-11-14): Initial architecture document

**Maintained by**: DuckDBRS Contributors
