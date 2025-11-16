# PrismDB Architecture

**Version:** 0.1.0
**Last Updated:** November 2025
**Status:** Active Development

## Table of Contents

1. [Overview](#overview)
2. [System Architecture](#system-architecture)
3. [Core Components](#core-components)
4. [Data Flow](#data-flow)
5. [Storage Engine](#storage-engine)
6. [Query Processing](#query-processing)
7. [Transaction Management](#transaction-management)
8. [Extension System](#extension-system)
9. [Performance Optimizations](#performance-optimizations)

---

## Overview

PrismDB is a high-performance analytical database system written in Rust, designed for OLAP (Online Analytical Processing) workloads. The system follows a columnar storage architecture with vectorized execution, enabling efficient analytical query processing.

### Design Principles

- **Columnar Storage**: Data organized by columns for better compression and cache efficiency
- **Vectorized Execution**: Process data in batches (vectors) using SIMD operations
- **Zero-Copy Operations**: Minimize data copying through smart memory management
- **Morsel-Driven Parallelism**: Work-stealing scheduler for parallel query execution
- **ACID Compliance**: Full transaction support with MVCC (Multi-Version Concurrency Control)

### Key Features

- SQL query support with comprehensive SQL-92 compliance
- Columnar storage with adaptive compression
- Vectorized query execution engine
- Parallel query processing
- ACID transactions with MVCC
- Support for CSV, Parquet, JSON, and SQLite data sources
- In-memory and persistent storage modes

---

## System Architecture

```text
┌─────────────────────────────────────────────────────────────┐
│                         Client API                          │
│                    (SQL Interface / CLI)                    │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                         Parser Layer                        │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │  Tokenizer   │→ │    Parser    │→ │     AST      │       │
│  └──────────────┘  └──────────────┘  └──────────────┘       │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                        Binder Layer                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │   Catalog    │← │    Binder    │→ │   Semantic   │       │
│  │   Lookup     │  │              │  │   Analysis   │       │
│  └──────────────┘  └──────────────┘  └──────────────┘       │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                        Planner Layer                        │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │   Logical    │→ │  Optimizer   │→ │   Physical   │       │
│  │     Plan     │  │              │  │     Plan     │       │
│  └──────────────┘  └──────────────┘  └──────────────┘       │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                      Execution Engine                       │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │  Vectorized  │  │   Parallel   │  │  Operators   │       │
│  │  Execution   │  │   Scheduler  │  │   (Hash/     │       │
│  │              │  │              │  │   Sort/Join) │       │
│  └──────────────┘  └──────────────┘  └──────────────┘       │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                       Storage Layer                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │    Buffer    │  │    Column    │  │ Transaction  │       │
│  │   Manager    │  │    Store     │  │   Manager    │       │
│  └──────────────┘  └──────────────┘  └──────────────┘       │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │ Compression  │  │     WAL      │  │    Block     │       │
│  │   Engine     │  │   Manager    │  │   Manager    │       │
│  └──────────────┘  └──────────────┘  └──────────────┘       │
└─────────────────────────────────────────────────────────────┘
```

---

## Core Components

### 1. Parser (`src/parser/`)

**Purpose**: Transform SQL text into Abstract Syntax Tree (AST)

**Components**:

- **Tokenizer** (`tokenizer.rs`): Breaks SQL text into tokens
- **Parser** (`parser.rs`): Builds AST from tokens using recursive descent parsing
- **Keywords** (`keywords.rs`): SQL keyword definitions and extensions
- **AST** (`ast.rs`): Abstract syntax tree node definitions

**Supported SQL Features**:

- DDL: CREATE TABLE, DROP TABLE, ALTER TABLE
- DML: SELECT, INSERT, UPDATE, DELETE
- Advanced: CTEs, Window Functions, QUALIFY, PIVOT/UNPIVOT
- Set Operations: UNION, INTERSECT, EXCEPT

### 2. Catalog (`src/catalog/`)

**Purpose**: Metadata management for database objects

**Components**:

- **Schema** (`schema.rs`): Database schema management
- **Table** (`table.rs`): Table metadata and structure
- **View** (`view.rs`): View definitions
- **Function** (`function.rs`): User-defined function registry
- **Index** (`index.rs`): Index metadata
- **Transaction** (`transaction.rs`): Transaction-scoped catalog changes

**Metadata Stored**:

- Table schemas (columns, types, constraints)
- View definitions
- Function signatures
- Index structures
- Statistics for query optimization

### 3. Planner (`src/planner/`)

**Purpose**: Query optimization and plan generation

**Components**:

- **Binder** (`binder.rs`): Semantic analysis and name resolution
- **Logical Plan** (`logical_plan.rs`): Relational algebra representation
- **Optimizer** (`optimizer.rs`): Rule-based and cost-based optimization
- **Physical Plan** (`physical_plan.rs`): Executable operator tree

**Optimization Strategies**:

- **Filter Pushdown**: Move filters closer to data sources
- **Projection Pushdown**: Only read required columns
- **Constant Folding**: Evaluate constants at compile time
- **Join Reordering**: Optimize join order based on cardinality
- **Limit Pushdown**: Early termination for LIMIT queries

### 4. Execution Engine (`src/execution/`)

**Purpose**: Execute physical query plans

**Architecture**: Morsel-driven parallel execution

**Components**:

- **Operators** (`operators.rs`): Core query operators
  - Scan (Table, Index)
  - Filter, Project
  - Hash Join, Sort-Merge Join
  - Hash Aggregate
  - Sort, Limit
  - Union, Intersect, Except

- **Parallel Execution** (`parallel.rs`):
  - Morsel size: ~100K rows per work unit
  - Work-stealing scheduler using Rayon
  - Lock-free data structures for coordination

- **Hash Table** (`hash_table.rs`):
  - Partitioned hash tables for parallel building
  - Probe optimization with SIMD
  - Spill-to-disk for large datasets (planned)

**Vectorization**:

- Process data in batches (DataChunks)
- Typical vector size: 2048 rows
- SIMD operations where applicable
- Null-aware processing with validity masks

### 5. Storage Engine (`src/storage/`)

**Purpose**: Persistent and in-memory data storage

**Components**:

#### Buffer Manager (`buffer.rs`)

- **Memory Pool**: Reusable buffer allocation
- **Page Management**: 4KB pages with LRU eviction
- **Memory Limit**: Configurable memory bounds

#### Column Store (`column.rs`)

- **Columnar Layout**: Each column stored separately
- **Type-specific Storage**: Optimized per data type
- **Statistics**: Min/max, null count, distinct count

#### Compression Engine (`compression/`)

- **Dictionary Encoding**: For low-cardinality columns
- **Run-Length Encoding (RLE)**: For repeated values
- **Adaptive Selection**: Automatic compression method selection
- **Analyze Phase**: Sample data to choose best compression

#### Block Manager (`block_manager.rs`)

- **Block Size**: 256KB blocks (like DuckDB)
- **Free Block Management**: Bitmap-based allocation
- **Block Metadata**: Headers with compression info

#### Transaction Manager (`transaction.rs`)

- **MVCC**: Multi-version concurrency control
- **Isolation Levels**: Read Uncommitted, Read Committed, Repeatable Read, Serializable
- **Snapshot Isolation**: Point-in-time consistent reads
- **Transaction IDs**: Monotonically increasing

#### Write-Ahead Log (`wal.rs`)

- **Durability**: Persist changes before commit
- **Recovery**: Replay log on restart
- **Checkpointing**: Periodic log truncation
- **Redo/Undo Logging**: For crash recovery

### 6. Type System (`src/types/`)

**Purpose**: Type definitions and operations

**Components**:

- **Logical Types** (`logical_type.rs`): User-facing types
  - Numeric: INT, BIGINT, FLOAT, DOUBLE, DECIMAL
  - String: VARCHAR, CHAR, TEXT
  - Temporal: DATE, TIME, TIMESTAMP
  - Boolean: BOOLEAN
  - Nested: STRUCT, LIST, MAP, ARRAY

- **Physical Types** (`physical_type.rs`): Internal representation
  - Fixed-size vs. variable-size
  - Alignment requirements
  - In-memory layout

- **Value** (`value.rs`): Runtime value representation
  - Type-tagged union
  - Null handling
  - Casting and coercion

- **Vector** (`vector.rs`): Columnar data batch
  - Flat vectors: Contiguous storage
  - Constant vectors: Single value repeated
  - Dictionary vectors: Indices + dictionary
  - Selection vectors: Filtered indices

- **DataChunk** (`data_chunk.rs`): Multi-column vector batch
  - Horizontal row representation
  - Vectorized processing unit
  - Memory-efficient iteration

### 7. Expression System (`src/expression/`)

**Purpose**: Expression evaluation and functions

**Components**:

- **Expressions** (`expression.rs`): Expression tree nodes
  - Column references
  - Constants
  - Function calls
  - Operators
  - Casts

- **Functions**:
  - **Scalar Functions** (`function.rs`): Row-by-row operations
  - **Aggregate Functions** (`aggregate.rs`): Multi-row reduction
  - **Window Functions** (`window_functions.rs`): Ordered window operations
  - **Math Functions** (`math_functions.rs`): Mathematical operations
  - **String Functions** (`string_functions.rs`): String manipulation
  - **Date/Time Functions** (`datetime_functions.rs`): Temporal operations

- **Operators** (`operator.rs`):
  - Arithmetic: +, -, *, /, %
  - Comparison: =, !=, <, >, <=, >=
  - Logical: AND, OR, NOT
  - String: LIKE, CONCAT

### 8. Extensions (`src/extensions/`)

**Purpose**: External data sources and plugins

**File Readers**:

- **CSV** (`csv_reader.rs`): Delimiter-separated values
- **Parquet** (`parquet_reader.rs`): Columnar file format
- **JSON** (`json_reader.rs`): JSON documents
- **SQLite** (`sqlite_reader.rs`): SQLite database files

**Extension Management** (`mod.rs`):

- Extension discovery and loading
- Configuration management
- Secret management for credentials

**Cloud Integration**:

- **AWS Signature** (`aws_signature.rs`): S3 authentication
- HTTP/HTTPS file reading
- Streaming data ingestion

---

## Data Flow

### Query Execution Flow

```text
SQL Query
   │
   ├─→ Tokenization
   │      └─→ Tokens
   │
   ├─→ Parsing
   │      └─→ AST
   │
   ├─→ Binding
   │      ├─→ Catalog Lookup
   │      ├─→ Type Resolution
   │      └─→ Semantic Validation
   │
   ├─→ Logical Planning
   │      └─→ Relational Algebra Tree
   │
   ├─→ Optimization
   │      ├─→ Rule-based Optimization
   │      ├─→ Cost-based Optimization
   │      └─→ Optimized Logical Plan
   │
   ├─→ Physical Planning
   │      ├─→ Operator Selection
   │      ├─→ Join Strategy
   │      └─→ Physical Operator Tree
   │
   ├─→ Execution
   │      ├─→ Pipeline Breakers (Sort, Hash Build)
   │      ├─→ Parallel Execution
   │      ├─→ Vectorized Processing
   │      └─→ Data Streaming
   │
   └─→ Result Set
         └─→ DataChunk Iterator
```

### Data Storage Flow

```text
INSERT/UPDATE
   │
   ├─→ Transaction Begin
   │      └─→ Get Transaction ID
   │
   ├─→ Write to WAL
   │      ├─→ Log Entry Creation
   │      └─→ Force to Disk
   │
   ├─→ Update In-Memory Data
   │      ├─→ Add to Buffer Pool
   │      ├─→ MVCC Version Creation
   │      └─→ Update Statistics
   │
   ├─→ Transaction Commit
   │      ├─→ Visibility Update
   │      └─→ WAL Checkpoint (periodic)
   │
   └─→ Background Tasks
         ├─→ Compression
         ├─→ Statistics Update
         └─→ Vacuum (cleanup old versions)
```

---

## Storage Engine

### Columnar Storage Layout

```text
Table: users (id, name, age, active)

┌─────────────────────────────────────────────────┐
│ Column: id (INTEGER)                            │
│ ┌─────────────────────────────────────────────┐ │
│ │ Block 0: [1, 2, 3, ..., 100000]             │ │
│ │ Compression: Uncompressed                   │ │
│ │ Min: 1, Max: 100000                         │ │
│ └─────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────┐
│ Column: name (VARCHAR)                          │
│ ┌─────────────────────────────────────────────┐ │
│ │ Block 0: ["Alice", "Bob", ...]              │ │
│ │ Compression: Dictionary                     │ │
│ │ Dictionary: {0: "Alice", 1: "Bob", ...}     │ │
│ │ Indices: [0, 1, 0, 2, ...]                  │ │
│ └─────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────┐
│ Column: age (INTEGER)                           │
│ ┌─────────────────────────────────────────────┐ │
│ │ Block 0: [25, 25, 25, 30, 30, ...]          │ │
│ │ Compression: RLE                            │ │
│ │ Runs: [(25, 3), (30, 2), ...]               │ │
│ └─────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────┘
```

### Block Structure

```text
┌─────────────────────────────────────────────────────────┐
│                      Block Header                       │
│  ┌────────────────────────────────────────────────────┐ │
│  │ Magic: "PRSM"                                      │ │
│  │ Version: 1                                         │ │
│  │ Block ID: 42                                       │ │
│  │ Row Count: 100000                                  │ │
│  │ Compression: Dictionary                            │ │
│  │ Compressed Size: 65536                             │ │
│  │ Uncompressed Size: 262144                          │ │
│  │ Checksum: 0xABCD1234                               │ │
│  └────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────┐
│                     Compressed Data                     │
│  ┌────────────────────────────────────────────────────┐ │
│  │ [compressed bytes ...]                             │ │
│  └────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────┘
```

### MVCC Implementation

```text
Transaction Timeline:

T1 (id=100): BEGIN → INSERT (1, 'Alice') → COMMIT
T2 (id=101): BEGIN → UPDATE id=1 SET name='Alicia' → COMMIT
T3 (id=102): BEGIN → SELECT * → COMMIT

Row Versions:
┌─────────────────────────────────────────────────┐
│ Row ID: 1                                       │
│ ┌─────────────────────────────────────────────┐ │
│ │ Version 1:                                  │ │
│ │   Data: (1, 'Alice')                        │ │
│ │   Xmin: 100 (created by T1)                 │ │
│ │   Xmax: 101 (deleted by T2)                 │ │
│ │   Next: → Version 2                         │ │
│ └─────────────────────────────────────────────┘ │
│ ┌─────────────────────────────────────────────┐ │
│ │ Version 2:                                  │ │
│ │   Data: (1, 'Alicia')                       │ │
│ │   Xmin: 101 (created by T2)                 │ │
│ │   Xmax: NULL (current version)              │ │
│ │   Next: NULL                                │ │
│ └─────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────┘

Visibility Rules:
- T3 (id=102) sees Version 2 if T2 committed before T3 started
- T3 sees Version 1 if T2 committed after T3 started (snapshot isolation)
```

---

## Query Processing

### Vectorized Execution Example

```rust
// Execute: SELECT age + 5 FROM users WHERE age > 18

// Input: DataChunk with 2048 rows
let chunk = scan_table("users"); // [age: [15, 20, 25, 30, ...]]

// Step 1: Filter (age > 18)
let filter = chunk.filter(|age| age > 18);
// Selection Vector: [false, true, true, true, ...]
// Filtered count: 1500 rows

// Step 2: Project (age + 5)
let result = filter.project(|age| age + 5);
// Result: [25, 30, 35, ...]

// SIMD optimization applied automatically for arithmetic
```

### Parallel Hash Join

```text
Table A: 1M rows
Table B: 100K rows

┌─────────────────────────────────────────────┐
│            Phase 1: Hash Build              │
│  ┌────────────────────────────────────────┐ │
│  │ Partition B into 16 partitions         │ │
│  │ Thread 1: Build hash table for P0-P3   │ │
│  │ Thread 2: Build hash table for P4-P7   │ │
│  │ Thread 3: Build hash table for P8-P11  │ │
│  │ Thread 4: Build hash table for P12-P15 │ │
│  └────────────────────────────────────────┘ │
└─────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────┐
│            Phase 2: Hash Probe              │
│  ┌────────────────────────────────────────┐ │
│  │ Process A in morsels (100K rows)       │ │
│  │ Thread 1: Probe morsel 0               │ │
│  │ Thread 2: Probe morsel 1               │ │
│  │ Thread 3: Probe morsel 2               │ │
│  │ Thread 4: Probe morsel 3               │ │
│  │ .. (work stealing as threads complete) │ │
│  └────────────────────────────────────────┘ │
└─────────────────────────────────────────────┘
```

---

## Transaction Management

### Isolation Levels

| Level | Dirty Read | Non-Repeatable Read | Phantom Read |
|-------|-----------|---------------------|--------------|
| Read Uncommitted | Yes | Yes | Yes |
| Read Committed | No | Yes | Yes |
| Repeatable Read | No | No | Yes |
| Serializable | No | No | No |

### Transaction Lifecycle

```text
BEGIN
  ├─→ Allocate Transaction ID
  ├─→ Create Snapshot (for Read Committed+)
  ├─→ Acquire Locks (for Serializable)
  │
  ├─→ Execute Operations
  │    ├─→ Write to WAL
  │    ├─→ Update Data (with MVCC versions)
  │    └─→ Track Modified Pages
  │
  └─→ COMMIT/ROLLBACK
       ├─→ COMMIT:
       │    ├─→ Mark transaction as committed
       │    ├─→ Force WAL to disk
       │    ├─→ Release locks
       │    └─→ Update visibility
       │
       └─→ ROLLBACK:
            ├─→ Mark transaction as aborted
            ├─→ Discard WAL entries
            ├─→ Release locks
            └─→ Keep MVCC versions for garbage collection
```

---

## Performance Optimizations

### 1. Columnar Storage Benefits

- **Better Compression**: Similar values together
- **Cache Efficiency**: Read only needed columns
- **SIMD Friendly**: Contiguous data layout

### 2. Vectorized Processing

- **Batch Processing**: Amortize function call overhead
- **CPU Cache Utilization**: Process 2048 rows at once
- **SIMD Instructions**: Parallel operations on vectors

### 3. Adaptive Compression

- **Dictionary Encoding**: Low cardinality (< 10% unique)
- **RLE**: Sorted or repeated data (> 20% consecutive duplicates)
- **Uncompressed**: High entropy data

### 4. Parallel Execution

- **Morsel-Driven**: 100K row work units
- **Work Stealing**: Dynamic load balancing
- **Lock-Free**: Minimize synchronization overhead

### 5. Query Optimization

- **Filter Pushdown**: Reduce data early
- **Projection Pruning**: Read only needed columns
- **Join Reordering**: Smallest tables first
- **Predicate Reordering**: Most selective first

---

## Extension System

### Architecture

```text
┌─────────────────────────────────────────────┐
│          Extension Manager                  │
│  ┌────────────────────────────────────────┐ │
│  │ Extension Registry                     │ │
│  │  - CSV Reader                          │ │
│  │  - Parquet Reader                      │ │
│  │  - JSON Reader                         │ │
│  │  - SQLite Reader                       │ │
│  │  - Custom Extensions...                │ │
│  └────────────────────────────────────────┘ │
└─────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────┐
│          Configuration Manager              │
│  ┌────────────────────────────────────────┐ │
│  │ Settings: Memory, Threads, Compression │ │
│  │ Secrets: AWS Keys, Database Credential │ │
│  └────────────────────────────────────────┘ │
└─────────────────────────────────────────────┘
```

### Adding New Extensions

1. Implement `FileReader` trait
2. Register with `ExtensionManager`
3. Define table functions (e.g., `read_parquet()`)
4. Handle type mapping and schema inference

---

## Future Enhancements

### Planned Features

- [ ] Spill-to-disk for large hash joins
- [ ] External sorting for datasets > memory
- [ ] Query result caching
- [ ] Materialized views
- [ ] User-defined functions (UDFs)
- [ ] Additional compression algorithms (LZ4, ZSTD)
- [ ] Bitmap indices for low-cardinality columns
- [ ] Zone maps for min/max filtering

### Performance Improvements

- [ ] JIT compilation for expressions
- [ ] Adaptive join strategies
- [ ] Statistics-based optimization
- [ ] Query compilation (LLVM backend)

---

## References

### Inspired By

- [DuckDB](https://duckdb.org/): Modern analytical database design
- [ClickHouse](https://clickhouse.com/): Columnar OLAP database
- [Apache Arrow](https://arrow.apache.org/): Columnar memory format

### Academic Papers

- "MonetDB/X100: Hyper-Pipelining Query Execution" (Boncz et al.)
- "Morsel-Driven Parallelism" (Leis et al.)
- "Volcano - An Extensible and Parallel Query Evaluation System" (Graefe)

---

**Document Version**: 1.0
**Contributors**: PrismDB Team
**License**: MIT
