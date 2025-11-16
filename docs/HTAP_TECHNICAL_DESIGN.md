# HTAP Technical Design Document

**Version:** 1.0
**Status:** Design Phase

---

## Table of Contents

1. [Document Store Implementation](#document-store-implementation)
2. [Vector Database Implementation](#vector-database-implementation)
3. [Graph Database Implementation](#graph-database-implementation)
4. [Performance Optimizations](#performance-optimizations)
5. [Testing Strategy](#testing-strategy)

---

## Document Store Implementation

### Physical Storage Format

#### BSON Encoding

```rust
/// BSON document encoding following MongoDB spec
pub struct BsonEncoder {
    buffer: Vec<u8>,
}

impl BsonEncoder {
    /// Encode document to BSON
    pub fn encode(&mut self, doc: &Document) -> Result<Vec<u8>> {
        // BSON structure:
        // [4 bytes: total size]
        // [elements...]
        // [1 byte: 0x00 terminator]

        let start = self.buffer.len();
        self.buffer.extend_from_slice(&[0u8; 4]); // Placeholder for size

        for (key, value) in &doc.fields {
            self.encode_element(key, value)?;
        }

        self.buffer.push(0x00); // Terminator

        // Update size
        let size = (self.buffer.len() - start) as i32;
        let size_bytes = size.to_le_bytes();
        self.buffer[start..start + 4].copy_from_slice(&size_bytes);

        Ok(self.buffer.clone())
    }

    fn encode_element(&mut self, key: &str, value: &BsonValue) -> Result<()> {
        match value {
            BsonValue::Double(v) => {
                self.buffer.push(0x01); // Type code
                self.encode_cstring(key);
                self.buffer.extend_from_slice(&v.to_le_bytes());
            }
            BsonValue::String(s) => {
                self.buffer.push(0x02);
                self.encode_cstring(key);
                self.encode_string(s);
            }
            BsonValue::Document(doc) => {
                self.buffer.push(0x03);
                self.encode_cstring(key);
                self.encode(doc)?;
            }
            BsonValue::Array(arr) => {
                self.buffer.push(0x04);
                self.encode_cstring(key);
                self.encode_array(arr)?;
            }
            BsonValue::Binary(subtype, data) => {
                self.buffer.push(0x05);
                self.encode_cstring(key);
                self.buffer.extend_from_slice(&(data.len() as i32).to_le_bytes());
                self.buffer.push(*subtype as u8);
                self.buffer.extend_from_slice(data);
            }
            // ... other types
        }
        Ok(())
    }
}
```

#### Hybrid Storage Layout

```rust
/// Collection with hybrid hot/cold storage
pub struct HybridCollection {
    /// Hot tier: recent documents in row format
    hot_tier: RowStoreCollection,

    /// Cold tier: old documents in columnar format
    cold_tier: ColumnarCollection,

    /// Tiering configuration
    config: TieringConfig,

    /// Background migration worker
    migrator: Option<MigrationWorker>,
}

pub struct TieringConfig {
    /// Move to cold after this duration
    pub age_threshold: Duration,

    /// Move to cold after this size
    pub size_threshold: usize,

    /// Check interval for migration
    pub check_interval: Duration,
}

impl HybridCollection {
    /// Insert new document (goes to hot tier)
    pub fn insert(&mut self, doc: Document) -> Result<ObjectId> {
        let id = self.hot_tier.insert(doc)?;

        // Check if migration needed
        if self.should_migrate() {
            self.schedule_migration()?;
        }

        Ok(id)
    }

    /// Query documents (check both tiers)
    pub fn find(&self, filter: &BsonDocument) -> Result<DocumentCursor> {
        // Query hot tier
        let hot_results = self.hot_tier.find(filter)?;

        // Query cold tier
        let cold_results = self.cold_tier.find(filter)?;

        // Merge results
        Ok(DocumentCursor::merge(hot_results, cold_results))
    }

    /// Background migration process
    async fn migrate_to_cold(&mut self) -> Result<()> {
        // 1. Select documents to migrate
        let cutoff = Instant::now() - self.config.age_threshold;
        let docs = self.hot_tier.select_older_than(cutoff)?;

        // 2. Extract schema from documents
        let schema = self.infer_schema(&docs);

        // 3. Flatten documents to columns
        let columns = self.flatten_documents(&docs, &schema)?;

        // 4. Append to cold tier
        self.cold_tier.append(columns)?;

        // 5. Delete from hot tier
        let ids: Vec<_> = docs.iter().map(|d| d.id).collect();
        self.hot_tier.delete_many(&ids)?;

        Ok(())
    }

    /// Infer schema from heterogeneous documents
    fn infer_schema(&self, docs: &[Document]) -> Schema {
        let mut schema = Schema::new();

        for doc in docs {
            for (key, value) in &doc.fields {
                // Track field presence and types
                let field_type = schema.get_or_create_field(key);
                field_type.add_observation(value);
            }
        }

        // Choose most common type for each field
        schema.finalize()
    }

    /// Flatten nested documents into columnar format
    fn flatten_documents(&self, docs: &[Document], schema: &Schema) -> Result<Vec<Column>> {
        let mut columns = vec![Column::new_with_capacity(docs.len()); schema.fields.len()];

        for doc in docs {
            for (i, field) in schema.fields.iter().enumerate() {
                // Extract value or null
                let value = doc.get(&field.name)
                    .map(|v| self.coerce_to_type(v, &field.field_type))
                    .unwrap_or(Value::Null);

                columns[i].push(value);
            }
        }

        Ok(columns)
    }
}
```

### Document Indexing

#### Multikey Index for Arrays

```rust
/// Index for array fields (e.g., tags: ["db", "rust"])
pub struct MultikeyIndex {
    /// B-tree: array_element -> [doc_ids]
    index: BTreeMap<BsonValue, HashSet<ObjectId>>,

    /// Reverse mapping: doc_id -> [array_elements]
    reverse: HashMap<ObjectId, Vec<BsonValue>>,
}

impl MultikeyIndex {
    /// Build index from array field
    pub fn build(docs: &[Document], field_path: &str) -> Self {
        let mut index = BTreeMap::new();
        let mut reverse = HashMap::new();

        for doc in docs {
            if let Some(BsonValue::Array(arr)) = doc.get(field_path) {
                for elem in arr {
                    index.entry(elem.clone())
                        .or_insert_with(HashSet::new)
                        .insert(doc.id);

                    reverse.entry(doc.id)
                        .or_insert_with(Vec::new)
                        .push(elem.clone());
                }
            }
        }

        Self { index, reverse }
    }

    /// Find documents where array contains value
    pub fn contains(&self, value: &BsonValue) -> Vec<ObjectId> {
        self.index.get(value)
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Find documents where array contains any of values
    pub fn contains_any(&self, values: &[BsonValue]) -> Vec<ObjectId> {
        let mut result = HashSet::new();
        for value in values {
            if let Some(docs) = self.index.get(value) {
                result.extend(docs);
            }
        }
        result.into_iter().collect()
    }
}
```

#### Path Index for Nested Fields

```rust
/// Index for nested document fields (e.g., "address.city")
pub struct PathIndex {
    /// B-tree: value -> [doc_ids]
    index: BTreeMap<BsonValue, HashSet<ObjectId>>,

    /// Path to indexed field
    path: Vec<String>,
}

impl PathIndex {
    pub fn build(docs: &[Document], path: &str) -> Self {
        let path_parts: Vec<String> = path.split('.').map(String::from).collect();
        let mut index = BTreeMap::new();

        for doc in docs {
            if let Some(value) = Self::extract_path(doc, &path_parts) {
                index.entry(value)
                    .or_insert_with(HashSet::new)
                    .insert(doc.id);
            }
        }

        Self { index, path: path_parts }
    }

    fn extract_path(doc: &Document, path: &[String]) -> Option<BsonValue> {
        let mut current = &doc.data;

        for part in path {
            match current {
                BsonValue::Document(map) => {
                    current = map.get(part)?;
                }
                _ => return None,
            }
        }

        Some(current.clone())
    }
}
```

---

## Vector Database Implementation

### Vector Storage

```rust
/// Optimized vector storage
pub struct VectorColumn {
    /// Number of dimensions (fixed for column)
    dimensions: usize,

    /// Element type
    element_type: VectorElementType,

    /// Flattened values: [v0_d0, v0_d1, ..., v1_d0, v1_d1, ...]
    values: Vec<f32>,

    /// Validity mask for nulls
    validity: ValidityMask,

    /// Optional normalization metadata
    normalized: bool,
}

impl VectorColumn {
    /// Access vector by index (zero-copy)
    pub fn get(&self, index: usize) -> Option<&[f32]> {
        if !self.validity.is_valid(index) {
            return None;
        }

        let start = index * self.dimensions;
        let end = start + self.dimensions;
        Some(&self.values[start..end])
    }

    /// SIMD-accelerated L2 distance
    #[cfg(target_feature = "avx2")]
    pub fn l2_distance_simd(&self, idx1: usize, idx2: usize) -> f32 {
        use std::arch::x86_64::*;

        let v1 = self.get(idx1).unwrap();
        let v2 = self.get(idx2).unwrap();

        unsafe {
            let mut sum = _mm256_setzero_ps();

            for i in (0..self.dimensions).step_by(8) {
                let a = _mm256_loadu_ps(&v1[i]);
                let b = _mm256_loadu_ps(&v2[i]);
                let diff = _mm256_sub_ps(a, b);
                let sq = _mm256_mul_ps(diff, diff);
                sum = _mm256_add_ps(sum, sq);
            }

            // Horizontal sum
            let mut result = [0f32; 8];
            _mm256_storeu_ps(result.as_mut_ptr(), sum);
            result.iter().sum::<f32>().sqrt()
        }
    }

    /// Quantize to int8 for compression
    pub fn quantize_int8(&self) -> QuantizedVectorColumn {
        // Find min/max for scaling
        let min = self.values.iter().cloned().fold(f32::INFINITY, f32::min);
        let max = self.values.iter().cloned().fold(f32::NEG_INFINITY, f32::max);

        let scale = (max - min) / 255.0;
        let offset = min;

        let quantized: Vec<i8> = self.values.iter()
            .map(|&v| (((v - offset) / scale) as i8))
            .collect();

        QuantizedVectorColumn {
            dimensions: self.dimensions,
            values: quantized,
            scale,
            offset,
            validity: self.validity.clone(),
        }
    }
}
```

### HNSW Index

```rust
/// Hierarchical Navigable Small World index
pub struct HnswIndex {
    /// Layers (layer 0 = all nodes)
    layers: Vec<HnswLayer>,

    /// Entry point (top layer)
    entry_point: NodeId,

    /// Configuration
    m: usize,              // Max connections per node
    m_max: usize,          // Max connections for layer 0
    ef_construction: usize, // Search width during construction
    ml: f64,                // Layer selection factor

    /// Distance function
    distance_fn: Box<dyn Fn(&[f32], &[f32]) -> f32 + Send + Sync>,
}

pub struct HnswLayer {
    /// Adjacency lists: node_id -> [neighbor_ids]
    neighbors: HashMap<NodeId, Vec<NodeId>>,
}

impl HnswIndex {
    /// Insert new vector
    pub fn insert(&mut self, id: NodeId, vector: &[f32]) {
        // 1. Select layer for new node
        let layer = self.select_layer();

        // 2. Find nearest neighbors at each layer
        let mut nearest = vec![self.entry_point];

        for l in (layer + 1..self.layers.len()).rev() {
            nearest = self.search_layer(&vector, nearest, 1, l);
        }

        // 3. Insert at each layer from top to bottom
        for l in (0..=layer).rev() {
            let candidates = self.search_layer(&vector, nearest.clone(), self.ef_construction, l);

            // Select M neighbors using heuristic
            let neighbors = self.select_neighbors(id, &candidates, self.m, l);

            // Add bidirectional links
            for &neighbor in &neighbors {
                self.layers[l].neighbors.entry(id)
                    .or_default()
                    .push(neighbor);

                self.layers[l].neighbors.entry(neighbor)
                    .or_default()
                    .push(id);

                // Prune if needed
                self.prune_connections(neighbor, l);
            }

            nearest = candidates;
        }

        // Update entry point if needed
        if layer > self.get_layer(self.entry_point) {
            self.entry_point = id;
        }
    }

    /// k-NN search
    pub fn search(&self, query: &[f32], k: usize, ef: usize) -> Vec<(NodeId, f32)> {
        let mut nearest = vec![self.entry_point];

        // Navigate to layer 0
        for l in (1..self.layers.len()).rev() {
            nearest = self.search_layer(query, nearest, 1, l);
        }

        // Search layer 0 with ef
        let mut candidates = self.search_layer(query, nearest, ef, 0);

        // Return top-k
        candidates.truncate(k);
        candidates
    }

    fn search_layer(
        &self,
        query: &[f32],
        entry_points: Vec<NodeId>,
        ef: usize,
        layer: usize,
    ) -> Vec<(NodeId, f32)> {
        let mut visited = HashSet::new();
        let mut candidates = BinaryHeap::new(); // Max heap
        let mut result = BinaryHeap::new();      // Max heap

        // Initialize with entry points
        for &ep in &entry_points {
            let dist = (self.distance_fn)(query, self.get_vector(ep));
            candidates.push(Reverse((OrderedFloat(dist), ep)));
            result.push((OrderedFloat(dist), ep));
            visited.insert(ep);
        }

        while let Some(Reverse((c_dist, c_node))) = candidates.pop() {
            // Early termination
            if let Some(&(f_dist, _)) = result.peek() {
                if c_dist > f_dist {
                    break;
                }
            }

            // Explore neighbors
            if let Some(neighbors) = self.layers[layer].neighbors.get(&c_node) {
                for &neighbor in neighbors {
                    if visited.insert(neighbor) {
                        let dist = (self.distance_fn)(query, self.get_vector(neighbor));
                        let of_dist = OrderedFloat(dist);

                        if result.len() < ef || of_dist < result.peek().unwrap().0 {
                            candidates.push(Reverse((of_dist, neighbor)));
                            result.push((of_dist, neighbor));

                            if result.len() > ef {
                                result.pop();
                            }
                        }
                    }
                }
            }
        }

        result.into_sorted_vec().into_iter()
            .map(|(OrderedFloat(d), id)| (id, d))
            .collect()
    }

    fn select_layer(&self) -> usize {
        let r: f64 = rand::random();
        (-r.ln() * self.ml).floor() as usize
    }
}
```

---

## Graph Database Implementation

### Graph Storage

```rust
/// Property graph storage
pub struct PropertyGraph {
    /// Vertex storage
    vertices: VertexStore,

    /// Edge storage
    edges: EdgeStore,

    /// Indices for fast traversal
    outgoing_index: AdjacencyIndex,  // source -> edges
    incoming_index: AdjacencyIndex,  // target -> edges

    /// Optional CSR for analytics
    csr: Option<CsrGraph>,
}

pub struct VertexStore {
    /// Primary key index
    id_index: BTreeMap<VertexId, VertexData>,

    /// Label index
    label_index: HashMap<String, HashSet<VertexId>>,

    /// Property indices
    property_indices: HashMap<String, BTreeMap<Value, HashSet<VertexId>>>,
}

pub struct EdgeStore {
    /// Primary key index
    id_index: BTreeMap<EdgeId, EdgeData>,

    /// Label index
    label_index: HashMap<String, HashSet<EdgeId>>,
}

pub struct AdjacencyIndex {
    /// (vertex_id, edge_label) -> [edge_ids]
    index: HashMap<(VertexId, String), Vec<EdgeId>>,
}

impl PropertyGraph {
    /// Single-hop traversal
    pub fn traverse_out(
        &self,
        vertex_id: VertexId,
        edge_label: Option<&str>,
    ) -> Vec<VertexId> {
        let edges = if let Some(label) = edge_label {
            self.outgoing_index.get(vertex_id, label)
        } else {
            self.outgoing_index.get_all(vertex_id)
        };

        edges.iter()
            .map(|&edge_id| self.edges.get(edge_id).unwrap().target)
            .collect()
    }

    /// Multi-hop traversal
    pub fn traverse_multi_hop(
        &self,
        start: VertexId,
        pattern: &PathPattern,
        max_depth: usize,
    ) -> Vec<Path> {
        let mut paths = vec![Path::new(start)];
        let mut result = Vec::new();

        for depth in 0..max_depth {
            let mut next_paths = Vec::new();

            for path in paths {
                let current = path.end();

                // Get outgoing edges matching pattern
                let edges = self.match_pattern(current, &pattern.steps[depth]);

                for edge in edges {
                    let target = self.edges.get(edge).unwrap().target;
                    let mut new_path = path.clone();
                    new_path.push(edge, target);

                    if depth == max_depth - 1 {
                        result.push(new_path);
                    } else {
                        next_paths.push(new_path);
                    }
                }
            }

            paths = next_paths;
        }

        result
    }
}
```

### CSR Representation for Analytics

```rust
/// Compressed Sparse Row graph representation
/// Optimized for read-only analytical queries
pub struct CsrGraph {
    /// Number of vertices
    num_vertices: usize,

    /// Row pointers: row_ptr[i] = start index in col_idx for vertex i
    /// Length: num_vertices + 1
    row_ptr: Vec<usize>,

    /// Column indices: targets of edges
    col_idx: Vec<VertexId>,

    /// Edge data (optional)
    edge_data: Option<Vec<EdgeId>>,

    /// Transpose graph (for incoming edges)
    transpose: Option<Box<CsrGraph>>,
}

impl CsrGraph {
    /// Build from property graph
    pub fn from_property_graph(graph: &PropertyGraph) -> Self {
        let num_vertices = graph.vertices.len();
        let mut row_ptr = vec![0; num_vertices + 1];
        let mut col_idx = Vec::new();
        let mut edge_data = Vec::new();

        // Count outgoing edges per vertex
        for vid in 0..num_vertices {
            let edges = graph.outgoing_index.get_all(vid as VertexId);
            row_ptr[vid + 1] = row_ptr[vid] + edges.len();
        }

        // Fill column indices and edge data
        for vid in 0..num_vertices {
            let edges = graph.outgoing_index.get_all(vid as VertexId);
            for &edge_id in edges {
                let edge = graph.edges.get(edge_id).unwrap();
                col_idx.push(edge.target);
                edge_data.push(edge_id);
            }
        }

        Self {
            num_vertices,
            row_ptr,
            col_idx,
            edge_data: Some(edge_data),
            transpose: None,
        }
    }

    /// Get neighbors of vertex
    pub fn neighbors(&self, vertex: VertexId) -> &[VertexId] {
        let start = self.row_ptr[vertex as usize];
        let end = self.row_ptr[vertex as usize + 1];
        &self.col_idx[start..end]
    }

    /// PageRank algorithm
    pub fn pagerank(&self, damping: f64, max_iterations: usize) -> Vec<f64> {
        let n = self.num_vertices;
        let mut rank = vec![1.0 / n as f64; n];
        let mut new_rank = vec![0.0; n];

        for _ in 0..max_iterations {
            new_rank.fill(0.0);

            for i in 0..n {
                let neighbors = self.neighbors(i as VertexId);
                let contribution = rank[i] / neighbors.len() as f64;

                for &neighbor in neighbors {
                    new_rank[neighbor as usize] += contribution;
                }
            }

            // Apply damping
            for i in 0..n {
                new_rank[i] = (1.0 - damping) / n as f64 + damping * new_rank[i];
            }

            std::mem::swap(&mut rank, &mut new_rank);
        }

        rank
    }

    /// Breadth-first search
    pub fn bfs(&self, start: VertexId) -> Vec<usize> {
        let mut distance = vec![usize::MAX; self.num_vertices];
        let mut queue = VecDeque::new();

        distance[start as usize] = 0;
        queue.push_back(start);

        while let Some(current) = queue.pop_front() {
            let current_dist = distance[current as usize];

            for &neighbor in self.neighbors(current) {
                if distance[neighbor as usize] == usize::MAX {
                    distance[neighbor as usize] = current_dist + 1;
                    queue.push_back(neighbor);
                }
            }
        }

        distance
    }
}
```

---

## Performance Optimizations

### Cache-Aware Data Structures

```rust
/// Cache-line aligned vector for better performance
#[repr(align(64))]
pub struct CacheAlignedVec<T> {
    data: Vec<T>,
}

/// Prefetching for HNSW search
impl HnswIndex {
    fn search_with_prefetch(&self, query: &[f32], k: usize) -> Vec<(NodeId, f32)> {
        use std::intrinsics::prefetch_read_data;

        // ... search logic ...

        // Prefetch next candidates
        for candidate in &candidates[..min(4, candidates.len())] {
            unsafe {
                let ptr = self.get_vector_ptr(candidate.id);
                prefetch_read_data(ptr, 3); // Locality level 3
            }
        }

        // ... continue search ...
    }
}
```

### Parallel Graph Algorithms

```rust
use rayon::prelude::*;

impl CsrGraph {
    /// Parallel PageRank
    pub fn pagerank_parallel(&self, damping: f64, max_iterations: usize) -> Vec<f64> {
        let n = self.num_vertices;
        let mut rank = vec![1.0 / n as f64; n];
        let mut new_rank = vec![0.0; n];

        for _ in 0..max_iterations {
            // Parallel contribution calculation
            new_rank.par_iter_mut().enumerate().for_each(|(i, r)| {
                *r = 0.0;
            });

            (0..n).into_par_iter().for_each(|i| {
                let neighbors = self.neighbors(i as VertexId);
                let contribution = rank[i] / neighbors.len() as f64;

                for &neighbor in neighbors {
                    // Atomic add for thread safety
                    let ptr = &new_rank[neighbor as usize] as *const f64 as *mut f64;
                    unsafe {
                        let current = std::ptr::read_volatile(ptr);
                        std::ptr::write_volatile(ptr, current + contribution);
                    }
                }
            });

            // Apply damping in parallel
            new_rank.par_iter_mut().enumerate().for_each(|(i, r)| {
                *r = (1.0 - damping) / n as f64 + damping * *r;
            });

            std::mem::swap(&mut rank, &mut new_rank);
        }

        rank
    }
}
```

---

## Testing Strategy

### Unit Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bson_encoding() {
        let doc = Document {
            fields: vec![
                ("name".to_string(), BsonValue::String("Alice".to_string())),
                ("age".to_string(), BsonValue::Int32(30)),
            ].into_iter().collect(),
        };

        let encoded = BsonEncoder::new().encode(&doc).unwrap();
        let decoded = BsonDecoder::new().decode(&encoded).unwrap();

        assert_eq!(doc, decoded);
    }

    #[test]
    fn test_vector_distance() {
        let v1 = VectorType::new(vec![1.0, 2.0, 3.0]);
        let v2 = VectorType::new(vec![4.0, 5.0, 6.0]);

        let dist = v1.l2_distance(&v2);
        assert!((dist - 5.196).abs() < 0.001);
    }

    #[test]
    fn test_graph_traversal() {
        let mut graph = PropertyGraph::new();

        let v1 = graph.add_vertex(Vertex::new().with_label("Person"));
        let v2 = graph.add_vertex(Vertex::new().with_label("Person"));

        graph.add_edge(Edge::new(v1, v2, "KNOWS"));

        let neighbors = graph.traverse_out(v1, Some("KNOWS"));
        assert_eq!(neighbors, vec![v2]);
    }
}
```

### Integration Tests

```rust
#[test]
fn test_hybrid_storage_migration() {
    let mut collection = HybridCollection::new(TieringConfig {
        age_threshold: Duration::from_secs(60),
        size_threshold: 1024 * 1024,
        check_interval: Duration::from_secs(10),
    });

    // Insert 1000 documents
    for i in 0..1000 {
        collection.insert(Document {
            fields: vec![("id".to_string(), BsonValue::Int32(i))].into_iter().collect(),
        }).unwrap();
    }

    // Wait for migration
    std::thread::sleep(Duration::from_secs(65));
    collection.run_migration().unwrap();

    // Verify data in cold tier
    assert_eq!(collection.cold_tier.count(), 1000);
    assert_eq!(collection.hot_tier.count(), 0);
}
```

### Benchmark Tests

```rust
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn bench_vector_search(c: &mut Criterion) {
    let index = build_hnsw_index(100_000, 768);
    let query = vec![0.0; 768];

    c.bench_function("hnsw_search_k10", |b| {
        b.iter(|| {
            index.search(black_box(&query), black_box(10), 100)
        });
    });
}

criterion_group!(benches, bench_vector_search);
criterion_main!(benches);
```

---

**Document Version**: 1.0
**Status**: Design Phase
**Next Steps**: Implementation of Phase 1 (Enhanced Type System)
