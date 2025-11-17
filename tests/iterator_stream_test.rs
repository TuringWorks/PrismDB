//! Iterator stream test - tests the IteratorStream physical operator

use prism::execution::{ExecutionContext, ExecutionEngine};
use prism::planner::{PhysicalColumn, PhysicalIteratorStream, PhysicalPlan};
use prism::storage::TransactionManager;
use prism::catalog::Catalog;
use prism::types::{DataChunk, LogicalType, Value, Vector};
use std::sync::{Arc, RwLock};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_iterator_stream_basic() {
        // Create execution context
        let transaction_manager = Arc::new(TransactionManager::new());
        let catalog = Arc::new(RwLock::new(Catalog::new()));
        let context = ExecutionContext::new(transaction_manager, catalog);
        let mut engine = ExecutionEngine::new(context);

        // Create schema
        let schema = vec![
            PhysicalColumn::new("id".to_string(), LogicalType::Integer),
            PhysicalColumn::new("name".to_string(), LogicalType::Varchar),
        ];

        // Create data chunks
        let mut chunk1 = DataChunk::with_rows(2);
        chunk1.set_vector(0, Vector::from_values(&[Value::Integer(1), Value::Integer(2)]).unwrap()).unwrap();
        chunk1.set_vector(1, Vector::from_values(&[Value::Varchar("Alice".to_string()), Value::Varchar("Bob".to_string())]).unwrap()).unwrap();

        let mut chunk2 = DataChunk::with_rows(2);
        chunk2.set_vector(0, Vector::from_values(&[Value::Integer(3), Value::Integer(4)]).unwrap()).unwrap();
        chunk2.set_vector(1, Vector::from_values(&[Value::Varchar("Charlie".to_string()), Value::Varchar("David".to_string())]).unwrap()).unwrap();

        let chunks = vec![chunk1, chunk2];

        // Create iterator stream
        let stream = PhysicalIteratorStream::new(chunks, schema.clone());
        let plan = PhysicalPlan::IteratorStream(stream);

        // Execute the plan
        let result_chunks = engine.execute_collect(plan).unwrap();

        // Verify results
        assert_eq!(result_chunks.len(), 2);
        assert_eq!(result_chunks[0].count(), 2);
        assert_eq!(result_chunks[1].count(), 2);

        // Verify first chunk data
        let vec0 = result_chunks[0].get_vector(0).unwrap();
        assert_eq!(vec0.get_value(0).unwrap(), Value::Integer(1));
        assert_eq!(vec0.get_value(1).unwrap(), Value::Integer(2));

        let vec1 = result_chunks[0].get_vector(1).unwrap();
        assert_eq!(vec1.get_value(0).unwrap(), Value::Varchar("Alice".to_string()));
        assert_eq!(vec1.get_value(1).unwrap(), Value::Varchar("Bob".to_string()));
    }

    #[test]
    fn test_iterator_stream_empty() {
        // Create execution context
        let transaction_manager = Arc::new(TransactionManager::new());
        let catalog = Arc::new(RwLock::new(Catalog::new()));
        let context = ExecutionContext::new(transaction_manager, catalog);
        let mut engine = ExecutionEngine::new(context);

        // Create schema
        let schema = vec![
            PhysicalColumn::new("id".to_string(), LogicalType::Integer),
        ];

        // Create empty iterator stream
        let stream = PhysicalIteratorStream::empty(schema.clone());
        let plan = PhysicalPlan::IteratorStream(stream);

        // Execute the plan
        let result_chunks = engine.execute_collect(plan).unwrap();

        // Verify results
        assert_eq!(result_chunks.len(), 0);
    }

    #[test]
    fn test_iterator_stream_single_chunk() {
        // Create execution context
        let transaction_manager = Arc::new(TransactionManager::new());
        let catalog = Arc::new(RwLock::new(Catalog::new()));
        let context = ExecutionContext::new(transaction_manager, catalog);
        let mut engine = ExecutionEngine::new(context);

        // Create schema
        let schema = vec![
            PhysicalColumn::new("value".to_string(), LogicalType::Integer),
        ];

        // Create single data chunk
        let mut chunk = DataChunk::with_rows(3);
        chunk.set_vector(0, Vector::from_values(&[Value::Integer(10), Value::Integer(20), Value::Integer(30)]).unwrap()).unwrap();

        let chunks = vec![chunk];

        // Create iterator stream
        let stream = PhysicalIteratorStream::new(chunks, schema.clone());
        let plan = PhysicalPlan::IteratorStream(stream);

        // Execute the plan
        let result_chunks = engine.execute_collect(plan).unwrap();

        // Verify results
        assert_eq!(result_chunks.len(), 1);
        assert_eq!(result_chunks[0].count(), 3);

        let vec = result_chunks[0].get_vector(0).unwrap();
        assert_eq!(vec.get_value(0).unwrap(), Value::Integer(10));
        assert_eq!(vec.get_value(1).unwrap(), Value::Integer(20));
        assert_eq!(vec.get_value(2).unwrap(), Value::Integer(30));
    }

    #[test]
    fn test_iterator_stream_schema() {
        // Create schema
        let schema = vec![
            PhysicalColumn::new("a".to_string(), LogicalType::Integer),
            PhysicalColumn::new("b".to_string(), LogicalType::Varchar),
            PhysicalColumn::new("c".to_string(), LogicalType::Double),
        ];

        // Create iterator stream
        let stream = PhysicalIteratorStream::empty(schema.clone());
        let plan = PhysicalPlan::IteratorStream(stream);

        // Verify schema
        let plan_schema = plan.schema();
        assert_eq!(plan_schema.len(), 3);
        assert_eq!(plan_schema[0].name, "a");
        assert_eq!(plan_schema[1].name, "b");
        assert_eq!(plan_schema[2].name, "c");
    }
}
