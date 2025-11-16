# PrismDB Documentation

Welcome to the PrismDB documentation! This directory contains comprehensive documentation for understanding, using, and extending PrismDB.

## Documentation Index

### Core Documentation

#### 1. [ARCHITECTURE.md](./ARCHITECTURE.md)

##### Current system architecture and design

Comprehensive overview of PrismDB's current implementation:

- System architecture and components
- Query processing pipeline
- Storage engine details
- Transaction management
- Vectorized execution
- Parallel processing

**Who should read this**: Developers, Contributors, Database Engineers

---

#### 2. [HTAP_EXTENSION_SPECIFICATION.md](./HTAP_EXTENSION_SPECIFICATION.md)

##### Future HTAP database specification

Detailed specification for extending PrismDB to HTAP with:

- Document store (MongoDB-compatible)
- JSON/JSONB and Binary types
- Vector database features
- Graph database capabilities
- Hybrid OLTP/OLAP storage

**Who should read this**: System Architects, Product Planners, Advanced Users
**Who should read this**: System Architects, Product Planners, Advanced Users

---

##### Technical implementation details for HTAP features

Low-level implementation details:

- BSON encoding/decoding
- Vector indexing algorithms (HNSW, IVF)
- Graph storage layouts
- Performance optimizations
- Testing strategies

**Who should read this**: Implementation Engineers, Performance Engineers

**Who should read this**: Implementation Engineers, Performance Engineers

---

## Quick Start Guide

### For Users

1. **Start here**: [README.md](../README.md) - Basic usage and quick start
2. **Deep dive**: [ARCHITECTURE.md](./ARCHITECTURE.md) - Understanding how PrismDB works
3. **Future features**: [HTAP_EXTENSION_SPECIFICATION.md](./HTAP_EXTENSION_SPECIFICATION.md) - Upcoming capabilities

### For Contributors

1. **System overview**: [ARCHITECTURE.md](./ARCHITECTURE.md) - Core components
2. **Implementation details**: [HTAP_TECHNICAL_DESIGN.md](./HTAP_TECHNICAL_DESIGN.md) - Code-level design
3. **Codebase**: `src/` - Explore the implementation

### For Architects

1. **Current design**: [ARCHITECTURE.md](./ARCHITECTURE.md) - Existing system
2. **Future roadmap**: [HTAP_EXTENSION_SPECIFICATION.md](./HTAP_EXTENSION_SPECIFICATION.md) - Planned features
3. **Technical feasibility**: [HTAP_TECHNICAL_DESIGN.md](./HTAP_TECHNICAL_DESIGN.md) - Implementation approach

---

## Documentation Structure

```text
docs/
├── README.md                              # This file
├── ARCHITECTURE.md                        # Current architecture
├── HTAP_EXTENSION_SPECIFICATION.md        # HTAP specification
└── HTAP_TECHNICAL_DESIGN.md              # Technical implementation details
```

---

## Key Concepts

### Current PrismDB (v0.1.0)

**Best for**: Complex analytical queries, data warehousing

**Key Features**:

- SQL query support
- Columnar compression
- Parallel execution
- ACID transactions
- Multiple file formats (CSV, Parquet, JSON, SQLite)

### Future PrismDB (v2.0.0) - HTAP

- Multiple file formats (CSV, Parquet, JSON, SQLite)
**Type**: Hybrid Transactional/Analytical Processing
**Storage**: Hybrid (Row + Columnar + Document + Vector + Graph)
**Execution**: Multi-modal (OLTP + OLAP + Vector Search + Graph Traversal)
**Best for**: Unified data platform

**Planned Features**:

- MongoDB-compatible document store
- Vector similarity search (embeddings)
- Graph database with Cypher support
- JSON/JSONB first-class types
- Binary types with compression
- Hybrid hot/cold storage

---

## Architecture Diagrams

### Current Architecture (v0.1.0)

```text
SQL Query
   ↓
Parser → Binder → Planner → Optimizer → Executor
                                           ↓
                                    Column Store
                                    (Compressed)
```

### Future Architecture (v2.0.0)

```text
                    ┌─── SQL Query
                    ├─── MongoDB Query
Query Interface ────┼─── Vector Search
                    ├─── Cypher Query
                    └─── Graph Algorithm

        ↓

Unified Query Engine
        ↓
                    ┌─── Row Store (OLTP)
                    ├─── Column Store (OLAP)
Storage Layer ──────┼─── Document Store (Documents)
                    ├─── Vector Store (Embeddings)
                    └─── Graph Store (Vertices/Edges)
```

---

## Feature Comparison

### Current vs. Future

| Feature | Current (v0.1.0) | Future (v2.0.0) |
|---------|------------------|-----------------|
| **SQL Support** | ✅ Full | ✅ Enhanced |
| **OLAP** | ✅ Yes | ✅ Yes |
| **OLTP** | ❌ Limited | ✅ Full |
| **Documents** | ❌ No | ✅ MongoDB-compatible |
| **JSON** | ⚠️ Via extensions | ✅ Native JSONB |
| **Vectors** | ❌ No | ✅ Native + indices |
| **Graphs** | ❌ No | ✅ Property graph + Cypher |
| **Storage** | Columnar only | Hybrid multi-modal |
| **Performance (OLAP)** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| **Performance (OLTP)** | ⭐⭐ | ⭐⭐⭐⭐ |

---

## Roadmap Overview

### Phase 1: Enhanced Type System (Q1 2026)

- JSON/JSONB types
- Binary types
- Type system refactoring

### Phase 2: Document Store (Q2 2026)

- MongoDB-compatible operations
- BSON storage
- Document indexing

### Phase 3: Hybrid Storage (Q2 2026)

- Row + columnar hybrid
- HTAP capabilities
- Auto-tiering

### Phase 4: Vector Database (Q3 2026)

- Vector types
- HNSW/IVF indices
- Similarity search

### Phase 5: Graph Database (Q3-Q4 2026)

- Property graph model
- Cypher query language
- Graph algorithms

### Phase 6: Integration (Q4 2026)

- Unified query interface
- Performance optimization
- Production readiness

---

## Contributing

Contributions are welcome! Please see:

### Areas for Contribution

**Current System**:

- Performance optimizations
- Bug fixes
- New SQL functions
- Additional file format support

**Future System**:

- New SQL functions
- Additional file format support

**Future System**:

- Document store implementation
- Vector indexing algorithms
- Graph algorithms
- Query optimization

---

## External Resources

### Academic Papers

- **Vectorized Execution**: "MonetDB/X100: Hyper-Pipelining Query Execution" (Boncz et al.)
- **Parallel Processing**: "Morsel-Driven Parallelism" (Leis et al.)
- **Vector Search**: "Efficient and robust approximate nearest neighbor search using HNSW" (Malkov & Yashunin)
- **Graph Processing**: "The Graph BLAS effort and its implications for Exascale" (Mattson et al.)

### Similar Systems

- **DuckDB**: Inspiration for analytical engine
- **MongoDB**: Document store reference
- **PostgreSQL**: HTAP and JSONB implementation
- **Pinecone**: Vector database design
- **Neo4j**: Property graph model

---

## FAQ

**Q: Is PrismDB production-ready?**
A: v0.1.0 is suitable for analytical workloads but not recommended for critical production use. v2.0.0 (HTAP) will target production readiness.

**Q: Will v2.0.0 break compatibility with v0.1.0?**
A: No. All v0.1.0 features will remain supported. New features are additive.

**Q: When will HTAP features be available?**
A: Phased rollout from Q1 2026 to Q4 2026. See roadmap above.

**Q: Can I contribute to the HTAP implementation?**
A: Yes! See [HTAP_TECHNICAL_DESIGN.md](./HTAP_TECHNICAL_DESIGN.md) for implementation details.

**Q: How does PrismDB compare to DuckDB?**
A: PrismDB is inspired by DuckDB but is an independent implementation. Future versions will add document, vector, and graph capabilities not present in DuckDB.

---

## License

This documentation is part of the PrismDB project and is licensed under the MIT License.

---

**Last Updated**: November 2025
**Documentation Version**: 1.0
**PrismDB Version**: 0.1.0 (current), 2.0.0 (planned)
