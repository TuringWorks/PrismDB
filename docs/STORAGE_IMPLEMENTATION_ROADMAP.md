# Storage Layer Implementation Roadmap

**Date**: 2025-11-14
**Objective**: Implement 9 critical missing storage features to achieve production-grade storage
**Total Estimated LOC**: ~11,900 LOC (MVP scope)
**Estimated Timeline**: 7-12 months (1 engineer)

---

## Overview

This roadmap implements the critical P0 and P1 storage features identified in the Comprehensive Gap Analysis. The implementation is divided into 3 phases, each building on the previous phase.

### Features to Implement:

1. ✅ **Compression Framework** - Dictionary + RLE algorithms (3,000 LOC)
2. ✅ **Row Groups** - 122,880-row partitioning for parallelism (1,500 LOC)
3. ✅ **Column Segments** - Variable-size segmentation (1,000 LOC)
4. ✅ **Zone Maps** - Segment-level statistics for pruning (500 LOC)
5. ✅ **HyperLogLog Statistics** - Distinct count estimation (800 LOC)
6. ✅ **Multi-queue LRU** - Advanced buffer pool (1,000 LOC)
7. ✅ **Checkpointing** - Persistent storage (2,000 LOC)
8. ✅ **Update Segments** - MVCC update chains (1,500 LOC)
9. ✅ **Metadata Management** - Metadata block system (600 LOC)

**Total**: ~11,900 LOC

---

## Phase 1: Storage Foundation (3-4 months, ~5,500 LOC)

**Goal**: Establish core storage infrastructure with compression, row groups, and segments

### 1.1 Compression Framework (3,000 LOC)

**Priority**: P0 - Critical
**Timeline**: 2-3 months
**Dependencies**: None

#### Components:

**1.1.1 Compression Infrastructure** (800 LOC)
- `src/storage/compression/mod.rs` - Module exports
- `src/storage/compression/types.rs` - Compression types enum
- `src/storage/compression/traits.rs` - CompressionFunction trait
- `src/storage/compression/analyze.rs` - Compression selection
- `src/storage/compression/metadata.rs` - Compression metadata

**Core Types**:
```rust
pub enum CompressionType {
    Uncompressed,
    Dictionary,
    RLE,
    // Future: BitPacking, FSST, Zstd, ALP, Chimp
}

pub trait CompressionFunction {
    fn analyze(&self, data: &[Value]) -> AnalyzeResult;
    fn compress(&self, data: &[Value]) -> Result<CompressedSegment>;
    fn decompress(&self, segment: &CompressedSegment) -> Result<Vec<Value>>;
    fn scan(&self, segment: &CompressedSegment, selection: &SelectionVector) -> Result<Vec<Value>>;
}

pub struct AnalyzeResult {
    pub compression_type: CompressionType,
    pub estimated_size: usize,
    pub compression_ratio: f64,
}
```

**1.1.2 Dictionary Compression** (1,200 LOC)
- `src/storage/compression/dictionary.rs` - Dictionary implementation

**Features**:
- String dictionary with hash map
- Integer dictionary for low-cardinality columns
- Supports 8-bit, 16-bit, 32-bit index widths
- Optimal index width selection based on cardinality
- SIMD-optimized dictionary lookup (future)

**Algorithm**:
1. Analyze phase: Build dictionary, count unique values
2. Compress phase: Encode values as dictionary indices
3. Decompress phase: Lookup indices in dictionary
4. Scan phase: Direct index lookup with predicate pushdown

**Expected Compression Ratio**:
- Low cardinality strings: 10-50x
- High cardinality strings: 2-5x
- Categorical data: 20-100x

**1.1.3 RLE Compression** (800 LOC)
- `src/storage/compression/rle.rs` - Run-length encoding

**Features**:
- Count runs of identical values
- Store (value, count) pairs
- Supports all data types
- Efficient for sorted/repeated data

**Algorithm**:
1. Analyze phase: Count runs, estimate compression
2. Compress phase: Encode as (value, run_length) pairs
3. Decompress phase: Expand runs
4. Scan phase: Binary search runs for range queries

**Expected Compression Ratio**:
- Sorted data: 100-1000x
- Repeated values: 10-100x
- Random data: 0.5-1x (may expand)

**1.1.4 Uncompressed Fallback** (200 LOC)
- `src/storage/compression/uncompressed.rs` - Passthrough

**Features**:
- No compression overhead
- Direct value storage
- Used when compression doesn't help

#### Testing:
- Unit tests for each compression algorithm (200 tests)
- Compression ratio benchmarks
- Scan performance tests
- Edge cases: nulls, empty data, single value

#### Deliverables:
- ✅ CompressionType enum
- ✅ CompressionFunction trait
- ✅ Dictionary compression (full)
- ✅ RLE compression (full)
- ✅ Uncompressed fallback
- ✅ Analyze phase for optimal selection
- ✅ 100+ unit tests

---

### 1.2 Row Group Architecture (1,500 LOC)

**Priority**: P0 - Critical
**Timeline**: 1-2 months
**Dependencies**: None

#### Components:

**1.2.1 Row Group Structure** (800 LOC)
- `src/storage/row_group.rs` - RowGroup implementation

**Structure**:
```rust
pub struct RowGroup {
    pub row_group_id: u64,
    pub start_row: u64,
    pub row_count: u64, // Max 122,880
    pub columns: Vec<ColumnData>,
    pub version_info: VersionInfo,
    pub statistics: RowGroupStatistics,
    pub metadata_pointer: MetadataPointer,
}

pub struct RowGroupCollection {
    pub row_groups: Vec<RowGroup>,
    pub total_rows: u64,
}
```

**Features**:
- Fixed 122,880 row size (DuckDB standard)
- Lazy loading of column data
- Zone map statistics per row group
- Version information for MVCC
- Metadata block pointers

**1.2.2 Row Group Operations** (400 LOC)
- Insert: Append to current row group, create new when full
- Scan: Parallel scan across row groups
- Filter: Zone map pruning before loading data
- Update: Mark row group as modified
- Delete: Tombstone marking

**1.2.3 Parallel Scan Support** (300 LOC)
- RowGroupScanState per thread
- Morsel generation per row group
- Parallel filter application

#### Testing:
- Row group creation and overflow
- Parallel scanning
- Zone map filtering
- Statistics tracking

#### Deliverables:
- ✅ RowGroup struct
- ✅ RowGroupCollection manager
- ✅ Lazy column loading
- ✅ Parallel scan support
- ✅ Zone map integration
- ✅ 50+ unit tests

---

### 1.3 Column Segments (1,000 LOC)

**Priority**: P0 - Critical
**Timeline**: 1 month
**Dependencies**: Compression framework

#### Components:

**1.3.1 Column Segment Structure** (500 LOC)
- `src/storage/column_segment.rs` - ColumnSegment implementation

**Structure**:
```rust
pub struct ColumnSegment {
    pub segment_id: u64,
    pub column_id: u64,
    pub start_row: u64,
    pub row_count: u64,
    pub compression_type: CompressionType,
    pub compressed_data: Vec<u8>,
    pub statistics: SegmentStatistics,
    pub block_offset: u64,
    pub segment_type: SegmentType, // Transient or Persistent
}

pub enum SegmentType {
    Transient,  // In-memory, not yet checkpointed
    Persistent, // Persisted to disk
}
```

**Features**:
- Variable-size segments (up to 256KB compressed)
- Per-segment compression
- Segment statistics (min/max/distinct)
- Block offset tracking
- Transient → Persistent conversion

**1.3.2 Segment Operations** (300 LOC)
- Compress: Analyze and compress data
- Decompress: Load and decompress segment
- Scan: Filtered scan with predicate pushdown
- Convert: Transient → Persistent

**1.3.3 Segment Statistics** (200 LOC)
```rust
pub struct SegmentStatistics {
    pub min_value: Option<Value>,
    pub max_value: Option<Value>,
    pub distinct_count: u64,
    pub null_count: u64,
    pub has_nulls: bool,
}
```

#### Testing:
- Segment creation with different compression types
- Scan with predicate pushdown
- Statistics accuracy
- Transient/Persistent conversion

#### Deliverables:
- ✅ ColumnSegment struct
- ✅ Variable-size segmentation
- ✅ Per-segment compression
- ✅ Segment statistics
- ✅ Scan with pushdown
- ✅ 40+ unit tests

---

## Phase 2: Advanced Storage (2-3 months, ~2,300 LOC)

**Goal**: Add zone maps, advanced statistics, and better memory management

### 2.1 Zone Maps (500 LOC)

**Priority**: P1 - High
**Timeline**: 2-3 weeks
**Dependencies**: Column segments, row groups

#### Components:

**2.1.1 Zone Map Structure** (200 LOC)
- `src/storage/zone_map.rs` - ZoneMap implementation

**Structure**:
```rust
pub struct ZoneMap {
    pub column_id: u64,
    pub zones: Vec<Zone>,
}

pub struct Zone {
    pub segment_id: u64,
    pub min_value: Value,
    pub max_value: Value,
    pub null_count: u64,
}
```

**2.1.2 Zone Map Filtering** (300 LOC)
- Filter predicates against zone maps
- Skip segments that cannot contain matches
- Support for: =, <, >, <=, >=, BETWEEN, IN

**Algorithm**:
1. Build zone map from segment statistics
2. Evaluate filter predicates against zones
3. Return segment IDs that may contain matches
4. Skip segments with impossible ranges

**Expected Speedup**: 10-100x for selective queries

#### Deliverables:
- ✅ ZoneMap struct
- ✅ Zone filtering logic
- ✅ Predicate evaluation
- ✅ Integration with table scan
- ✅ 30+ unit tests

---

### 2.2 HyperLogLog Statistics (800 LOC)

**Priority**: P1 - High
**Timeline**: 1 month
**Dependencies**: None

#### Components:

**2.2.1 HyperLogLog Implementation** (500 LOC)
- `src/storage/statistics/hyperloglog.rs` - HLL implementation

**Algorithm**:
- HyperLogLog with 2^14 registers (standard precision)
- Hash function: xxHash or SipHash
- Bias correction for low cardinalities
- Merge support for parallel aggregation

**Accuracy**: ±2% error rate

**2.2.2 Distinct Count Statistics** (300 LOC)
- Integration with SegmentStatistics
- Update on insert/delete
- Merge on segment consolidation

#### Deliverables:
- ✅ HyperLogLog implementation
- ✅ Distinct count tracking
- ✅ Merge support
- ✅ Integration with optimizer
- ✅ 40+ unit tests

---

### 2.3 Multi-queue LRU Buffer Pool (1,000 LOC)

**Priority**: P1 - High
**Timeline**: 1 month
**Dependencies**: None

#### Components:

**2.3.1 Multi-queue Structure** (600 LOC)
- `src/storage/buffer/multi_queue_lru.rs` - Multi-queue LRU

**Architecture**:
```rust
pub struct MultiQueueLRU {
    pub queues: [EvictionQueue; 8],
    pub queue_sizes: [usize; 8],
    pub total_capacity: usize,
}

pub enum QueuePriority {
    Block,           // Persistent blocks
    ExternalFile,    // File data
    ManagedBuffer1,  // User buffers (6 levels)
    ManagedBuffer2,
    ManagedBuffer3,
    ManagedBuffer4,
    ManagedBuffer5,
    ManagedBuffer6,
    TinyBuffer,      // Small allocations
}
```

**Features**:
- 8 priority queues with separate LRU lists
- Age-based eviction with LRU timestamps
- Dead node garbage collection
- Memory pressure handling
- Per-queue size limits

**2.3.2 Eviction Logic** (400 LOC)
- Evict from lowest priority queue first
- Age-based purging (evict oldest first)
- Memory reservation system
- Bulk deallocation support

#### Deliverables:
- ✅ Multi-queue LRU implementation
- ✅ Priority-based eviction
- ✅ Memory pressure handling
- ✅ Integration with BufferManager
- ✅ 50+ unit tests

---

## Phase 3: Persistence & MVCC (2-3 months, ~4,100 LOC)

**Goal**: Add durability with checkpointing, MVCC support, and metadata management

### 3.1 Checkpointing System (2,000 LOC)

**Priority**: P0 - Critical
**Timeline**: 1-2 months
**Dependencies**: Compression, row groups, segments

#### Components:

**3.1.1 Checkpoint Manager** (800 LOC)
- `src/storage/checkpoint/manager.rs` - CheckpointManager

**Checkpoint Types**:
```rust
pub enum CheckpointType {
    Full,        // Complete snapshot
    Append,      // Incremental append
    Incremental, // Delta changes
}

pub struct CheckpointManager {
    pub block_manager: Arc<BlockManager>,
    pub wal_manager: Arc<WalFileManager>,
    pub checkpoint_interval: Duration,
}
```

**3.1.2 Table Data Writer** (800 LOC)
- `src/storage/checkpoint/table_writer.rs` - Write table data

**Process**:
1. Iterate over row groups
2. For each row group, write column segments
3. Write segment metadata
4. Write row group metadata
5. Update block pointers

**3.1.3 Metadata Writer** (400 LOC)
- `src/storage/checkpoint/metadata_writer.rs` - Write catalog metadata

**Process**:
1. Serialize schema definitions
2. Serialize table metadata
3. Serialize constraints
4. Write to metadata blocks

#### Testing:
- Full checkpoint and recovery
- Incremental checkpoint
- Concurrent checkpoint during writes
- Corruption detection

#### Deliverables:
- ✅ CheckpointManager
- ✅ Table data writer
- ✅ Metadata writer
- ✅ Recovery logic
- ✅ WAL truncation after checkpoint
- ✅ 60+ unit tests

---

### 3.2 Update Segments (1,500 LOC)

**Priority**: P1 - High
**Timeline**: 1 month
**Dependencies**: Row groups, segments

#### Components:

**3.2.1 Update Segment Structure** (600 LOC)
- `src/storage/update_segment.rs` - UpdateSegment implementation

**Structure**:
```rust
pub struct UpdateSegment {
    pub segment_id: u64,
    pub column_id: u64,
    pub updates: BTreeMap<u64, Value>, // row_id -> value
    pub version_vector: Vec<VersionInfo>,
}

pub struct VersionInfo {
    pub transaction_id: u64,
    pub timestamp: u64,
}
```

**Features**:
- Separate storage for updates (not in-place)
- Update chain per tuple
- Version vector for MVCC
- Garbage collection of old versions

**3.2.2 Update Operations** (500 LOC)
- Insert update into segment
- Scan with version visibility
- Merge updates with base data
- Garbage collection

**3.2.3 MVCC Integration** (400 LOC)
- Transaction visibility checks
- Version chain traversal
- Snapshot isolation support

#### Deliverables:
- ✅ UpdateSegment struct
- ✅ Update chain management
- ✅ Version vector tracking
- ✅ MVCC visibility logic
- ✅ Garbage collection
- ✅ 40+ unit tests

---

### 3.3 Metadata Management (600 LOC)

**Priority**: P1 - Medium
**Timeline**: 2-3 weeks
**Dependencies**: Block manager

#### Components:

**3.3.1 Metadata Manager** (300 LOC)
- `src/storage/metadata/manager.rs` - MetadataManager

**Structure**:
```rust
pub struct MetadataManager {
    pub block_manager: Arc<BlockManager>,
    pub metadata_blocks: BTreeMap<u64, MetadataBlock>,
}

pub struct MetadataBlock {
    pub block_id: u64,
    pub data: Vec<u8>,
    pub next_block: Option<u64>, // For chaining
}

pub struct MetadataPointer {
    pub block_id: u64,
    pub offset: u64,
}
```

**3.3.2 Metadata I/O** (300 LOC)
- MetadataWriter: Write metadata to blocks
- MetadataReader: Read metadata from blocks
- Block chaining for large metadata
- Caching for frequently accessed metadata

#### Deliverables:
- ✅ MetadataManager
- ✅ Metadata block allocation
- ✅ MetadataWriter/Reader
- ✅ Block chaining
- ✅ Metadata caching
- ✅ 30+ unit tests

---

## Implementation Order

### Week 1-4: Compression Infrastructure
1. Create compression module structure
2. Define CompressionType enum and traits
3. Implement analyze phase framework
4. Add compression metadata

### Week 5-8: Dictionary Compression
5. Implement dictionary builder
6. Add hash map-based encoding
7. Implement decoder
8. Add scan with predicate pushdown
9. Optimize index width selection

### Week 9-12: RLE Compression
10. Implement run counting
11. Add (value, count) encoding
12. Implement decoder
13. Add scan with binary search
14. Handle edge cases (nulls, single values)

### Week 13-16: Row Groups
15. Create RowGroup structure
16. Implement RowGroupCollection
17. Add lazy column loading
18. Implement parallel scan
19. Integrate zone map filtering

### Week 17-20: Column Segments
20. Create ColumnSegment structure
21. Integrate compression
22. Add segment statistics
23. Implement scan with pushdown
24. Add Transient/Persistent conversion

### Week 21-24: Zone Maps & HyperLogLog
25. Implement ZoneMap structure
26. Add zone filtering logic
27. Implement HyperLogLog
28. Integrate with segment statistics

### Week 25-28: Multi-queue LRU
29. Create multi-queue structure
30. Implement eviction logic
31. Add memory pressure handling
32. Integrate with BufferManager

### Week 29-36: Checkpointing
33. Create CheckpointManager
34. Implement table data writer
35. Add metadata writer
36. Implement recovery logic
37. Add WAL truncation

### Week 37-40: Update Segments
38. Create UpdateSegment structure
39. Implement update operations
40. Add MVCC visibility logic
41. Implement garbage collection

### Week 41-44: Metadata Management
42. Create MetadataManager
43. Implement metadata I/O
44. Add block chaining
45. Implement caching

---

## Testing Strategy

### Unit Tests (500+ tests)
- Per-component unit tests
- Edge case coverage (nulls, empty, overflow)
- Error handling tests
- Concurrency tests

### Integration Tests (100+ tests)
- End-to-end table operations
- Checkpoint and recovery
- Parallel operations
- MVCC scenarios

### Performance Tests (50+ benchmarks)
- Compression ratios
- Scan performance
- Memory usage
- Checkpoint time

### Correctness Tests
- Data integrity after checkpoint/recovery
- MVCC isolation levels
- Concurrent read/write
- Zone map accuracy

---

## Success Metrics

### Compression:
- ✅ Dictionary compression: 10-50x for low cardinality
- ✅ RLE compression: 10-100x for sorted data
- ✅ Overall storage reduction: 5-10x on typical datasets

### Performance:
- ✅ Zone map pruning: 10-100x speedup on selective queries
- ✅ Parallel scanning: Near-linear scaling with core count
- ✅ Checkpoint time: < 1 second per 1M rows

### Reliability:
- ✅ 100% data recovery after checkpoint
- ✅ MVCC isolation correctness
- ✅ No data loss on crash

---

## Risk Mitigation

### High-Risk Areas:
1. **Compression correctness** - Extensive testing, property-based tests
2. **MVCC race conditions** - Careful locking, formal verification
3. **Checkpoint consistency** - Atomic operations, checksums
4. **Memory leaks** - Rust ownership, valgrind testing

### Contingency Plans:
- If compression too complex: Start with uncompressed, add later
- If MVCC too complex: Start with simple locking
- If checkpointing too complex: Start with full checkpoints only

---

## Post-Implementation

### Phase 4: Advanced Compression (Optional, 3-6 months)
- BitPacking for integers
- FSST for strings
- Zstd for general compression
- ALP for floating-point
- Chimp for time series

### Phase 5: Distributed Storage (6-12 months)
- Follow Cloud Deployment Roadmap
- S3/Azure/GCS integration
- Distributed checkpointing
- Remote segment loading

---

## Conclusion

This roadmap provides a structured path to implementing the 9 critical missing storage features. The total estimated effort is **11,900 LOC over 7-12 months** for a single engineer.

**Priority Order**:
1. Compression (P0, 2-3 months)
2. Row Groups + Segments (P0, 2 months)
3. Zone Maps + HyperLogLog (P1, 1.5 months)
4. Multi-queue LRU (P1, 1 month)
5. Checkpointing (P0, 2 months)
6. Update Segments (P1, 1 month)
7. Metadata Management (P1, 0.5 months)

Upon completion, DuckDBRS will have a production-grade storage layer with:
- ✅ 5-10x storage reduction via compression
- ✅ 10-100x query speedup via zone maps
- ✅ Parallel scanning with row groups
- ✅ Durable persistence with checkpointing
- ✅ MVCC support with update segments
- ✅ Advanced memory management

**Next Step**: Begin Phase 1.1 - Compression Framework implementation.
