# PrismDB HTAP Extension Specification

**Version:** 2.0.0
**Target Release:** Q2 2026
**Status:** Specification / Planning Phase

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [HTAP Overview](#htap-overview)
3. [Document Store Extension](#document-store-extension)
4. [Enhanced Type System](#enhanced-type-system)
5. [Vector Database Features](#vector-database-features)
6. [Graph Database Features](#graph-database-features)
7. [Architecture Changes](#architecture-changes)
8. [Implementation Roadmap](#implementation-roadmap)
9. [Performance Considerations](#performance-considerations)
10. [Migration Strategy](#migration-strategy)
11. [Appendix](#appendix)

---

## Executive Summary

This specification outlines the transformation of PrismDB from a pure OLAP database into a **Hybrid Transactional/Analytical Processing (HTAP)** system with support for:

- **Document Store**: MongoDB-compatible document operations
- **JSON/JSONB Types**: First-class JSON support with indexing
- **Binary Types**: BLOB, BYTEA with compression
- **Vector Database**: Embeddings storage and similarity search
- **Graph Database**: Property graph model with traversal queries

### Goals

1. **Unified Platform**: Single database for OLTP, OLAP, documents, vectors, and graphs
2. **MongoDB Compatibility**: Support MongoDB query language and wire protocol
3. **Performance**: Maintain analytical performance while adding transactional capabilities
4. **Flexibility**: Schema-on-read for documents, schema-on-write for tables
5. **Modern AI/ML**: Native vector similarity search for embeddings

### Non-Goals

- Full MongoDB cluster compatibility (sharding, replica sets)
- Real-time graph traversal at Neo4j scale (focus on analytical graph queries)
- Replace specialized systems (use PrismDB as unified layer)

---

## HTAP Overview

### What is HTAP?

HTAP combines:

- **OLTP** (Online Transaction Processing): Row-level operations, high concurrency, low latency
- **OLAP** (Online Analytical Processing): Bulk operations, complex queries, high throughput

### Architecture Approach

```text
┌──────────────────────────────────────────────────────────────────┐
│                         Unified Storage Layer                    │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐   │
│  │   Row Store     │  │  Column Store   │  │  Hybrid Store   │   │
│  │                 │  │                 │  │                 │   │
│  │ • OLTP          │  │ • OLAP          │  │ • Hot Data      │   │
│  │ • Transactions  │  │ • Analytics     │  │ • Recent Rows   │   │
│  │ • Point Queries │  │ • Scans         │  │ • Auto-tiering  │   │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘   │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### Hybrid Storage Strategy

**Row Store**: Hot data (recent inserts/updates)

- B+ tree indexing
- MVCC for transactions
- Low-latency point queries
- Automatic conversion to columnar after time/size threshold

**Column Store**: Cold data (analytical queries)

- Existing PrismDB columnar storage
- Compression and vectorized execution
- Optimized for scans and aggregations

**Data Flow**:

```text
INSERT → Row Store → Background Process → Column Store
                          ↓
                    Threshold-based:
                    • Age > 1 hour
                    • Size > 1MB
                    • Manual trigger
```

---

## Document Store Extension

### Data Model

Documents are stored as BSON (Binary JSON) with flexible schemas:

```rust
pub struct Document {
    pub id: ObjectId,           // Unique 12-byte identifier
    pub data: BsonValue,        // Nested JSON-like structure
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

pub enum BsonValue {
    Null,
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    Double(f64),
    Decimal128(Decimal128),
    String(String),
    Binary(BinarySubtype, Vec<u8>),
    ObjectId(ObjectId),
    Array(Vec<BsonValue>),
    Document(HashMap<String, BsonValue>),
    Timestamp(Timestamp),
    DateTime(DateTime<Utc>),
}
```

### Storage Layout

**Hybrid Storage** (Recommended):

```text
Hot Tier (Row Store - Last 1 hour):
┌──────────────────────────────────┐
│ Doc 1: {_id: 1, name: "Alice"}   │
│ Doc 2: {_id: 2, age: 25}         │
│ ...                              │
│ Index: B-tree on _id             │
└──────────────────────────────────┘

Cold Tier (Column Store - Older data):
┌──────────────────────────────────┐
│ Flattened Columns:               │
│  _id: [1, 2, 3, ...]             │
│  name: ["Alice", NULL, "Bob"]    │
│  age: [NULL, 25, 30, ...]        │
│  tags: [["a"], NULL, ["b","c"]]  │
└──────────────────────────────────┘
```

### Query Language

#### MongoDB API

```javascript
// Insert
db.users.insertOne({
    name: "Alice",
    age: 30,
    tags: ["admin", "developer"],
    address: { city: "NYC", zip: "10001" }
})

// Find with nested query
db.users.find({
    "address.city": "NYC",
    age: { $gt: 25 }
})

// Update
db.users.updateOne(
    { name: "Alice" },
    { $set: { age: 31 }, $push: { tags: "manager" } }
)

// Aggregation pipeline
db.users.aggregate([
    { $match: { age: { $gt: 25 } } },
    { $group: { _id: "$address.city", count: { $sum: 1 } } },
    { $sort: { count: -1 } }
])
```

#### SQL Extensions

```sql
-- Create collection
CREATE COLLECTION users (
    validator = {
        $jsonSchema: {
            required: ["name", "email"],
            properties: {
                name: { type: "string" },
                age: { type: "integer", minimum: 0 }
            }
        }
    }
);

-- Insert document
INSERT INTO users VALUES (
    '{"name": "Alice", "age": 30, "tags": ["admin"]}'::DOCUMENT
);

-- Query nested fields
SELECT
    data->>'name' as name,
    data->'address'->>'city' as city
FROM users
WHERE (data->>'age')::int > 25;

-- Array operations
SELECT * FROM users
WHERE data->'tags' @> '["admin"]'::JSONB;
```

### Indexing

**1. Multikey Index** (for arrays):

```sql
CREATE INDEX idx_tags ON users USING GIN (data->'tags');
```

**2. Path Index** (for nested fields):

```sql
CREATE INDEX idx_city ON users ((data->'address'->>'city'));
```

**3. Text Index** (full-text search):

```sql
CREATE INDEX idx_description_text ON products
USING FTS ((data->>'description'));
```

---

## Enhanced Type System

### JSON/JSONB Types

**JSON**: Text storage (preserves formatting)
**JSONB**: Binary storage (fast indexed access)

#### Operators

```sql
-- Access operators
data->>'name'              -- Text extraction
data->'address'            -- Object extraction
data->'tags'->0            -- Array element
data#>'{address,city}'     -- Path extraction

-- Containment operators
data @> '{"age": 30}'           -- Contains
data <@ '{"age": 30, "x": 1}'   -- Contained by
data ? 'age'                    -- Has key
data ?| array['age', 'name']    -- Has any key
data ?& array['age', 'name']    -- Has all keys

-- JSONPath queries
data @@ '$.age > 25'
```

### Binary Types

#### BLOB (Binary Large Object)

```sql
CREATE TABLE files (
    id INTEGER,
    name VARCHAR,
    content BLOB,
    thumbnail BLOB COMPRESSED WITH LZ4
);

INSERT INTO files VALUES (
    1,
    'document.pdf',
    read_file('/path/to/file.pdf')::BLOB,
    generate_thumbnail('/path/to/file.pdf')
);
```

#### Binary Functions

```sql
-- Encoding/Decoding
encode(data, 'base64')
decode('SGVsbG8=', 'base64')

-- Hashing
sha256(data)
md5(data)
blake3(data)

-- Compression
compress(data, 'zstd')
decompress(data, 'zstd')
```

---

## Vector Database Features

### Vector Type

```sql
-- Create table with vector column
CREATE TABLE embeddings (
    id INTEGER PRIMARY KEY,
    content TEXT,
    embedding VECTOR(1536)  -- OpenAI ada-002 dimensions
);

-- Insert vectors
INSERT INTO embeddings VALUES (
    1,
    'Rust is a systems programming language',
    '[0.123, -0.456, 0.789, ...]'::VECTOR(1536)
);

-- Similarity search (k-NN)
SELECT id, content,
       embedding <-> '[0.1, 0.2, ...]'::VECTOR(1536) AS distance
FROM embeddings
ORDER BY distance ASC
LIMIT 10;
```

### Distance Operators

```sql
embedding <-> query    -- L2 distance (Euclidean)
embedding <#> query    -- Negative dot product
embedding <=> query    -- Cosine distance (1 - cosine similarity)
```

### Vector Indexing

#### HNSW (Hierarchical Navigable Small World)

**Best for**: High recall, moderate dataset size (< 10M vectors)

```sql
CREATE INDEX idx_embedding_hnsw ON embeddings
USING HNSW (embedding)
WITH (m = 16, ef_construction = 200);

SET hnsw.ef_search = 100;  -- Accuracy vs speed tradeoff
```

**Parameters**:

- `m`: Max connections per node (default: 16)
- `ef_construction`: Dynamic candidate list size during build (default: 200)
- `ef_search`: Search width (higher = more accurate, slower)

#### IVF (Inverted File Index)

**Best for**: Large datasets (> 10M vectors), approximate search

```sql
CREATE INDEX idx_embedding_ivf ON embeddings
USING IVF (embedding)
WITH (lists = 1000, nprobe = 10);
```

**Parameters**:

- `lists`: Number of clusters (default: sqrt(num_vectors))
- `nprobe`: Number of clusters to search (higher = more accurate)

### Hybrid Search

```sql
-- Vector search with filters
SELECT * FROM products
WHERE category = 'electronics'
  AND price < 1000
ORDER BY embedding <-> query_embedding
LIMIT 10;

-- Multiple vector searches
SELECT
    p1.id,
    p1.embedding <-> '[...]' as image_sim,
    p1.text_embedding <-> '[...]' as text_sim
FROM products p1
ORDER BY (image_sim + text_sim) / 2
LIMIT 10;
```

### Vector Operations

```sql
-- Vector arithmetic
SELECT embedding + other AS sum;
SELECT embedding - other AS diff;
SELECT embedding * 2.0 AS scaled;

-- Normalization
SELECT normalize(embedding) AS unit_vector;

-- Quantization (compression)
SELECT quantize(embedding, 'int8') AS quantized;
```

---

## Graph Database Features

### Property Graph Model

**Vertices** (Nodes):

```sql
CREATE VERTEX TABLE person (
    id BIGINT PRIMARY KEY,
    name VARCHAR,
    age INTEGER,
    properties JSONB
) IN GRAPH social_network;
```

**Edges** (Relationships):

```sql
CREATE EDGE TABLE knows (
    id BIGINT PRIMARY KEY,
    from_person BIGINT,
    to_person BIGINT,
    since INTEGER,
    strength FLOAT,
    FOREIGN KEY (from_person) REFERENCES person(id),
    FOREIGN KEY (to_person) REFERENCES person(id)
) IN GRAPH social_network;
```

### Cypher Query Language

```cypher
-- Create vertices
CREATE (a:Person {name: 'Alice', age: 30})
CREATE (b:Person {name: 'Bob', age: 25})

-- Create edges
CREATE (a)-[:KNOWS {since: 2020}]->(b)

-- Pattern matching
MATCH (a:Person)-[:KNOWS]->(b:Person)
WHERE a.age > b.age
RETURN a.name, b.name

-- Multi-hop traversal
MATCH (a:Person)-[:KNOWS*1..3]-(b:Person)
WHERE a.name = 'Alice'
RETURN b.name, length(path) as hops

-- Shortest path
MATCH path = shortestPath((a:Person)-[:KNOWS*]-(b:Person))
WHERE a.name = 'Alice' AND b.name = 'Charlie'
RETURN path
```

### SQL/PGQ (ISO Standard)

```sql
-- Graph pattern matching
SELECT a.name as person1, b.name as person2
FROM GRAPH_TABLE (social_network
    MATCH (a:person)-[:knows]->(b:person)
    WHERE a.age > b.age
    COLUMNS (a.name, b.name)
);

-- Recursive CTE for traversal
WITH RECURSIVE paths(source, target, depth) AS (
    SELECT from_person, to_person, 1
    FROM knows
    WHERE from_person = 1

    UNION ALL

    SELECT p.source, k.to_person, p.depth + 1
    FROM paths p
    JOIN knows k ON p.target = k.from_person
    WHERE p.depth < 3
)
SELECT DISTINCT target FROM paths;
```

### Graph Algorithms

```sql
-- PageRank
SELECT id, pagerank(social_network, damping => 0.85)
FROM vertices;

-- Community Detection
SELECT id, community_detection(social_network, algorithm => 'louvain')
FROM vertices;

-- Centrality measures
SELECT id,
       degree_centrality(social_network),
       betweenness_centrality(social_network),
       closeness_centrality(social_network)
FROM vertices;

-- Shortest path
SELECT shortest_path(
    social_network,
    source => 1,
    target => 100,
    algorithm => 'dijkstra'
);

-- K-hop neighbors
SELECT k_hop_neighbors(social_network, vertex_id => 1, k => 3);
```

---

## Architecture Changes

### New Components

#### 1. Hybrid Storage Manager

```rust
pub struct HybridStorageManager {
    row_store: RowStore,         // Hot data
    column_store: ColumnStore,   // Cold data
    tiering: TieringPolicy,      // Migration rules
    migrator: DataMigrator,      // Background worker
}

pub struct TieringPolicy {
    age_threshold: Duration,     // Migrate after this age
    size_threshold: usize,       // Migrate after this size
    access_tracker: AccessTracker, // Track hot/cold access
}
```

#### 2. Document Query Engine

```rust
pub struct DocumentQueryEngine {
    parser: MongoQueryParser,     // Parse MongoDB queries
    planner: DocumentPlanner,     // Plan document operations
    executor: DocumentExecutor,   // Execute operations
}
```

#### 3. Vector Search Engine

```rust
pub struct VectorSearchEngine {
    indices: HashMap<TableId, VectorIndex>,
    config: VectorConfig,
}

pub enum VectorIndex {
    Hnsw(HnswIndex),
    Ivf(IvfIndex),
    Flat(FlatIndex),
}
```

#### 4. Graph Engine

```rust
pub struct GraphEngine {
    storage: GraphStorage,           // Vertex/edge storage
    executor: GraphExecutor,         // Query executor
    algorithms: AlgorithmRegistry,   // Built-in algorithms
}

pub struct GraphStorage {
    vertices: TableId,
    edges: TableId,
    outgoing_index: Index,  // (source, label) -> [edge_ids]
    incoming_index: Index,  // (target, label) -> [edge_ids]
    csr: Option<CsrGraph>,  // For analytics
}
```

### Modified Components

#### Type System Extensions

```rust
pub enum LogicalType {
    // Existing types
    Integer, BigInt, Float, Double,
    Varchar, Date, Timestamp,

    // New types
    Json,                              // Text JSON
    Jsonb,                             // Binary JSON
    Blob(CompressionType),             // Binary data
    Vector(VectorElementType, usize),  // Vector embeddings
    Document,                          // BSON document
    Graph(GraphElementType),           // Vertex or Edge
}
```

#### Unified Query Planner

```rust
pub struct UnifiedPlanner {
    sql_planner: SqlPlanner,
    mongo_planner: MongoPlanner,
    cypher_planner: CypherPlanner,
}
```

---

## Implementation Roadmap

### Phase 1: Enhanced Type System (3 months)

#### Q1 2026

- [ ] JSON/JSONB type implementation
- [ ] Binary type (BLOB, BYTEA)
- [ ] JSON operators and functions
- [ ] GIN index for JSONB
- [ ] Type casting and coercion

**Deliverables**:

- Working JSON/JSONB columns
- Full operator support (`->`, `->>`, `@>`, etc.)
- GIN index with 90% of PostgreSQL compatibility
- 90% test coverage

### Phase 2: Document Store (4 months)

#### Q2 2026 - Document Store

- [ ] Document storage format (BSON)
- [ ] Collection metadata management
- [ ] MongoDB query parser
- [ ] Document query executor
- [ ] Document indices (multikey, text, path)
- [ ] Aggregation pipeline
- [ ] MongoDB wire protocol (optional)

**Deliverables**:

- Create/drop collections
- CRUD operations (90% MongoDB compatibility)
- Query language support
- Basic aggregation pipeline (20+ operators)
- 85% MongoDB API compatibility

### Phase 3: Hybrid Storage (4 months)

#### Q2 2026 - Hybrid Storage

- [ ] Row store implementation
- [ ] Hot/cold data tiering
- [ ] Background data migration
- [ ] Unified query interface
- [ ] MVCC for row store
- [ ] Performance benchmarking

**Deliverables**:

- Row store for OLTP workloads
- Automatic tiering based on age/access
- ACID transactions across both stores
- < 10ms p99 latency for point queries
- Maintain current OLAP performance

### Phase 4: Vector Database (3 months)

#### Q3 2026

- [ ] Vector type implementation
- [ ] Distance metrics (L2, cosine, dot product)
- [ ] HNSW index implementation
- [ ] IVF index (optional)
- [ ] Vector operators and functions
- [ ] Hybrid search (vector + filter)
- [ ] Quantization (int8, binary)

**Deliverables**:

- Vector columns with k-NN search
- < 10ms p99 for k=10 on 1M vectors (768 dims)
- HNSW index with 95% recall @ ef=100
- Integration with popular embedding models
- Quantization for 4-8x storage reduction

### Phase 5: Graph Database (5 months)

#### Q3-Q4 2026

- [ ] Graph schema (vertices, edges)
- [ ] Property graph model
- [ ] Cypher query parser (subset)
- [ ] Pattern matching engine
- [ ] Graph algorithms library (10+ algorithms)
- [ ] SQL/PGQ support
- [ ] CSR representation for analytics

**Deliverables**:

- Vertex/edge storage with properties
- Basic Cypher support (MATCH, CREATE, WHERE, RETURN)
- 10+ graph algorithms (PageRank, community detection, etc.)
- < 100ms for 3-hop traversal on 1M vertices
- PageRank on 10M vertices in < 10s

### Phase 6: Integration & Optimization (3 months)

#### Q4 2026

- [ ] Cross-feature integration
- [ ] Query optimizer enhancements
- [ ] Performance tuning
- [ ] Comprehensive benchmarking
- [ ] Complete documentation
- [ ] Migration tools

**Deliverables**:

- Seamless multi-model queries
- Performance on par with specialized systems
- Complete user and developer documentation
- Migration tools from MongoDB/Neo4j/Pinecone
- Production-ready release

---

## Performance Considerations

### Benchmarks

| Workload | Target Metric | Measurement |
|----------|--------------|-------------|
| OLTP Inserts | 100K TPS | Simple row inserts |
| OLAP (TPC-H SF100) | < 60s | End-to-end 22 queries |
| Document Inserts | 50K/sec | MongoDB-style documents |
| Document Queries | < 5ms p99 | Indexed lookups |
| Vector k-NN | < 10ms p99 | k=10, 1M vectors, 768 dims |
| Vector Recall | > 95% | HNSW @ ef=100 |
| Graph Traversal | < 100ms | 3-hop, 1M vertices, 10M edges |
| PageRank | < 10s | 10M vertices |

### Resource Requirements

**Recommended Hardware**:

- CPU: 16+ cores (for parallel execution)
- RAM: 64GB+ (for in-memory caching)
- Storage: NVMe SSD (for WAL and indices)

**Memory Allocation**:

```text
Total RAM: 64GB
├─ Buffer Pool: 32GB (50%)
├─ Row Store Cache: 13GB (20%)
├─ Vector Index: 10GB (15%)
├─ Graph Index: 6GB (10%)
└─ Query Working Memory: 3GB (5%)
```

### Optimization Strategies

### 1. Hybrid Storage

- Hot data in row store for fast point queries
- Cold data in columnar store for analytics
- Minimize migration overhead

#### 2. Index Selection

- HNSW for vectors < 10M
- IVF for vectors > 10M
- Adaptive index building

#### 3. Query Optimization

- Push filters to appropriate storage tier
- Use covering indices where possible
- Parallel execution across all storage types

---

## Migration Strategy

### From Current PrismDB

```sql
-- No changes required for existing functionality
-- New features are opt-in

-- Add new column types
ALTER TABLE users ADD COLUMN metadata JSONB;
ALTER TABLE products ADD COLUMN embedding VECTOR(768);

-- Convert table to collection
ALTER TABLE logs SET STORAGE = DOCUMENT;

-- Create graph from existing tables
CREATE GRAPH social FROM TABLES (
    users AS VERTEX,
    friendships AS EDGE
);
```

### From MongoDB

```bash
# Export from MongoDB
mongodump --uri="mongodb://localhost/mydb" --out=/backup

# Import to PrismDB
prismdb-import mongodb \
    --input=/backup/mydb \
    --collection=users,orders,products

# Or use live migration
prismdb-migrate from-mongodb \
    --source="mongodb://localhost/mydb" \
    --target="prismdb://localhost/prismdb" \
    --collections=all \
    --live
```

### From PostgreSQL (with pgvector)

```bash
# Direct migration with vector support
prismdb-migrate from-postgres \
    --source="postgresql://localhost/mydb" \
    --target="prismdb://localhost/prismdb" \
    --tables=embeddings \
    --vector-columns=embedding \
    --preserve-indices
```

### From Neo4j

```bash
# Export graph
neo4j-admin dump --database=neo4j --to=/backup/graph.dump

# Import to PrismDB
prismdb-import neo4j \
    --input=/backup/graph.dump \
    --graph=social_network \
    --preserve-properties
```

---

## Appendix

### Glossary

- **HTAP**: Hybrid Transactional/Analytical Processing
- **OLTP**: Online Transaction Processing
- **OLAP**: Online Analytical Processing
- **BSON**: Binary JSON (MongoDB format)
- **MVCC**: Multi-Version Concurrency Control
- **k-NN**: k-Nearest Neighbors
- **HNSW**: Hierarchical Navigable Small World
- **IVF**: Inverted File Index
- **CSR**: Compressed Sparse Row
- **GIN**: Generalized Inverted Index
- **JSONPath**: Query language for JSON
- **Cypher**: Graph query language (Neo4j)
- **SQL/PGQ**: SQL Property Graph Queries (ISO standard)

### Type Compatibility Matrix

| PrismDB Type | MongoDB Type | PostgreSQL Type | Notes |
|--------------|--------------|-----------------|-------|
| DOCUMENT | Document | JSONB | Full compatibility |
| JSONB | - | JSONB | 90% compatible |
| VECTOR(n) | - | vector(n) | pgvector compatible |
| BLOB | Binary | BYTEA | Full compatibility |
| Graph Vertex | - | - | PrismDB extension |
| Graph Edge | - | - | PrismDB extension |

### References

#### Academic Papers

- **Vectorized Execution**: "MonetDB/X100: Hyper-Pipelining Query Execution" (Boncz et al.)
- **HTAP**: "F1: A Distributed SQL Database That Scales" (Shute et al.)
- **Vector Search**: "Efficient and robust approximate nearest neighbor search using HNSW" (Malkov & Yashunin)
- **Graph Processing**: "Pregel: A System for Large-Scale Graph Processing" (Malewicz et al.)

#### Systems

- [MongoDB](https://www.mongodb.com/docs/) - Document database reference
- [PostgreSQL JSONB](https://www.postgresql.org/docs/current/datatype-json.html) - JSON implementation
- [pgvector](https://github.com/pgvector/pgvector) - Vector similarity search
- [Neo4j](https://neo4j.com/docs/) - Property graph database
- [ClickHouse](https://clickhouse.com/docs/) - Columnar OLAP
- [TiDB](https://docs.pingcap.com/) - HTAP reference

#### Standards

- [SQL/PGQ](https://www.iso.org/standard/76120.html) - ISO graph query standard
- [BSON Specification](http://bsonspec.org/) - Binary JSON format
- [MongoDB Wire Protocol](https://www.mongodb.com/docs/manual/reference/mongodb-wire-protocol/)

---

**Document Version**: 1.0
**Status**: Specification / RFC
**Next Review**: 2026-Q1
**Contributors**: PrismDB Architecture Team
