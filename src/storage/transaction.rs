use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

use crate::common::error::{PrismDBError, Result};
use crate::storage::table::{RowId, TableData};

/// Transaction isolation levels
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

/// Transaction state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    Active,
    Committed,
    Aborted,
}

/// Transaction operation types for logging
#[derive(Debug, Clone)]
pub enum TransactionOperation {
    Insert { table_id: String, row_id: RowId },
    Update { table_id: String, row_id: RowId },
    Delete { table_id: String, row_id: RowId },
}

/// Transaction metadata
#[derive(Debug)]
pub struct TransactionMetadata {
    pub id: Uuid,
    pub start_time: u64,
    pub state: TransactionState,
    pub isolation_level: IsolationLevel,
    pub operations: Vec<TransactionOperation>,
}

impl TransactionMetadata {
    pub fn new(isolation_level: IsolationLevel) -> Self {
        Self {
            id: Uuid::new_v4(),
            start_time: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            state: TransactionState::Active,
            isolation_level,
            operations: Vec::new(),
        }
    }

    pub fn add_operation(&mut self, operation: TransactionOperation) {
        self.operations.push(operation);
    }

    pub fn commit(&mut self) {
        self.state = TransactionState::Committed;
    }

    pub fn abort(&mut self) {
        self.state = TransactionState::Aborted;
    }
}

/// Snapshot of data for transaction isolation
#[derive(Debug)]
pub struct DataSnapshot {
    pub timestamp: u64,
    pub data: HashMap<String, Vec<Vec<crate::types::Value>>>,
}

impl DataSnapshot {
    pub fn new() -> Self {
        Self {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            data: HashMap::new(),
        }
    }

    pub fn add_table_data(&mut self, table_id: &str, rows: Vec<Vec<crate::types::Value>>) {
        self.data.insert(table_id.to_string(), rows);
    }

    pub fn get_table_data(&self, table_id: &str) -> Option<&Vec<Vec<crate::types::Value>>> {
        self.data.get(table_id)
    }
}

/// Transaction context for managing transaction state
#[derive(Debug)]
pub struct TransactionContext {
    pub metadata: TransactionMetadata,
    pub snapshot: Option<DataSnapshot>,
    pub modified_tables: HashMap<String, Arc<RwLock<TableData>>>,
    pub rollback_data: HashMap<String, Vec<Vec<crate::types::Value>>>,
}

impl TransactionContext {
    pub fn new(isolation_level: IsolationLevel) -> Self {
        Self {
            metadata: TransactionMetadata::new(isolation_level),
            snapshot: None,
            modified_tables: HashMap::new(),
            rollback_data: HashMap::new(),
        }
    }

    pub fn register_table(&mut self, table_id: String, table: Arc<RwLock<TableData>>) {
        self.modified_tables.insert(table_id, table);
    }

    pub fn save_rollback_data(&mut self, table_id: &str, data: Vec<Vec<crate::types::Value>>) {
        self.rollback_data.insert(table_id.to_string(), data);
    }

    pub fn get_rollback_data(&self, table_id: &str) -> Option<&Vec<Vec<crate::types::Value>>> {
        self.rollback_data.get(table_id)
    }
}

/// Transaction manager for handling multiple concurrent transactions
#[derive(Debug)]
pub struct TransactionManager {
    active_transactions: Arc<RwLock<HashMap<Uuid, Arc<RwLock<TransactionContext>>>>>,
    global_lock: Arc<RwLock<()>>,
}

impl TransactionManager {
    pub fn new() -> Self {
        Self {
            active_transactions: Arc::new(RwLock::new(HashMap::new())),
            global_lock: Arc::new(RwLock::new(())),
        }
    }

    /// Begin a new transaction
    pub fn begin_transaction(&self, isolation_level: IsolationLevel) -> Result<Uuid> {
        let context = TransactionContext::new(isolation_level);
        let transaction_id = context.metadata.id;

        // Create snapshot for repeatable read and serializable isolation
        if matches!(
            isolation_level,
            IsolationLevel::RepeatableRead | IsolationLevel::Serializable
        ) {
            // In a real implementation, this would capture the current state of all tables
            // For now, we'll create an empty snapshot
        }

        let context_arc = Arc::new(RwLock::new(context));

        {
            let mut transactions = self.active_transactions.write().unwrap();
            transactions.insert(transaction_id, context_arc);
        }

        Ok(transaction_id)
    }

    /// Commit a transaction
    pub fn commit_transaction(&self, transaction_id: Uuid) -> Result<()> {
        let context_arc = {
            let transactions = self.active_transactions.read().unwrap();
            transactions.get(&transaction_id).cloned()
        };

        if let Some(context_arc) = context_arc {
            let mut context = context_arc.write().unwrap();

            // Mark as committed
            context.metadata.commit();

            // In a real implementation, this would:
            // 1. Write to WAL
            // 2. Apply changes to tables
            // 3. Release locks

            // Remove from active transactions
            {
                let mut transactions = self.active_transactions.write().unwrap();
                transactions.remove(&transaction_id);
            }

            Ok(())
        } else {
            Err(PrismDBError::Transaction(format!(
                "Transaction {} not found",
                transaction_id
            )))
        }
    }

    /// Abort a transaction
    pub fn abort_transaction(&self, transaction_id: Uuid) -> Result<()> {
        let context_arc = {
            let transactions = self.active_transactions.read().unwrap();
            transactions.get(&transaction_id).cloned()
        };

        if let Some(context_arc) = context_arc {
            let mut context = context_arc.write().unwrap();

            // Rollback changes
            for (table_id, rollback_data) in &context.rollback_data {
                if let Some(table_arc) = context.modified_tables.get(table_id) {
                    let mut table = table_arc.write().unwrap();
                    // Restore original data
                    // In a real implementation, this would restore the exact state
                    let _ = table.clear_rows();
                    for _row in rollback_data {
                        // This is simplified - real implementation would handle column-wise restoration
                    }
                }
            }

            // Mark as aborted
            context.metadata.abort();

            // Remove from active transactions
            {
                let mut transactions = self.active_transactions.write().unwrap();
                transactions.remove(&transaction_id);
            }

            Ok(())
        } else {
            Err(PrismDBError::Transaction(format!(
                "Transaction {} not found",
                transaction_id
            )))
        }
    }

    /// Get transaction context
    pub fn get_transaction(&self, transaction_id: Uuid) -> Option<Arc<RwLock<TransactionContext>>> {
        let transactions = self.active_transactions.read().unwrap();
        transactions.get(&transaction_id).cloned()
    }

    /// Check if transaction is active
    pub fn is_transaction_active(&self, transaction_id: Uuid) -> bool {
        let transactions = self.active_transactions.read().unwrap();
        transactions.contains_key(&transaction_id)
    }

    /// Get all active transaction IDs
    pub fn get_active_transactions(&self) -> Vec<Uuid> {
        let transactions = self.active_transactions.read().unwrap();
        transactions.keys().cloned().collect()
    }

    /// Acquire global lock (for serializable isolation)
    pub fn acquire_global_lock<'a>(&'a self) -> Result<std::sync::RwLockReadGuard<'a, ()>> {
        self.global_lock
            .read()
            .map_err(|_| PrismDBError::Transaction("Failed to acquire global lock".to_string()))
    }

    /// Acquire global write lock (for exclusive operations)
    pub fn acquire_global_write_lock<'a>(&'a self) -> Result<std::sync::RwLockWriteGuard<'a, ()>> {
        self.global_lock.write().map_err(|_| {
            PrismDBError::Transaction("Failed to acquire global write lock".to_string())
        })
    }
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Transaction handle for easier transaction management
#[derive(Debug)]
pub struct Transaction {
    pub id: Uuid,
    manager: Arc<TransactionManager>,
    context: Arc<RwLock<TransactionContext>>,
}

impl Transaction {
    pub fn new(manager: Arc<TransactionManager>, isolation_level: IsolationLevel) -> Result<Self> {
        let id = manager.begin_transaction(isolation_level)?;
        let context = manager.get_transaction(id).ok_or_else(|| {
            PrismDBError::Transaction("Failed to get transaction context".to_string())
        })?;

        Ok(Self {
            id,
            manager,
            context,
        })
    }

    pub fn commit(self) -> Result<()> {
        self.manager.commit_transaction(self.id)
    }

    pub fn abort(self) -> Result<()> {
        self.manager.abort_transaction(self.id)
    }

    pub fn get_context(&self) -> Arc<RwLock<TransactionContext>> {
        self.context.clone()
    }

    pub fn add_operation(&self, operation: TransactionOperation) -> Result<()> {
        let mut context = self.context.write().unwrap();
        context.metadata.add_operation(operation);
        Ok(())
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        // Auto-abort if not explicitly committed or aborted
        if self.manager.is_transaction_active(self.id) {
            let _ = self.manager.abort_transaction(self.id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_metadata() {
        let mut metadata = TransactionMetadata::new(IsolationLevel::ReadCommitted);
        assert_eq!(metadata.state, TransactionState::Active);

        metadata.commit();
        assert_eq!(metadata.state, TransactionState::Committed);

        let mut metadata2 = TransactionMetadata::new(IsolationLevel::Serializable);
        metadata2.abort();
        assert_eq!(metadata2.state, TransactionState::Aborted);
    }

    #[test]
    fn test_transaction_manager() {
        let manager = TransactionManager::new();

        // Begin transaction
        let tx_id = manager
            .begin_transaction(IsolationLevel::ReadCommitted)
            .unwrap();
        assert!(manager.is_transaction_active(tx_id));

        // Get transaction
        let context = manager.get_transaction(tx_id);
        assert!(context.is_some());

        // Commit transaction
        manager.commit_transaction(tx_id).unwrap();
        assert!(!manager.is_transaction_active(tx_id));
    }

    #[test]
    fn test_transaction_abort() {
        let manager = TransactionManager::new();

        let tx_id = manager
            .begin_transaction(IsolationLevel::ReadCommitted)
            .unwrap();
        assert!(manager.is_transaction_active(tx_id));

        manager.abort_transaction(tx_id).unwrap();
        assert!(!manager.is_transaction_active(tx_id));
    }

    #[test]
    fn test_transaction_handle() {
        let manager = Arc::new(TransactionManager::new());

        let tx = Transaction::new(manager.clone(), IsolationLevel::ReadCommitted).unwrap();
        let tx_id = tx.id;
        assert!(manager.is_transaction_active(tx_id));

        tx.commit().unwrap();
        assert!(!manager.is_transaction_active(tx_id));
    }

    #[test]
    fn test_auto_abort_on_drop() {
        let manager = Arc::new(TransactionManager::new());

        {
            let _tx = Transaction::new(manager.clone(), IsolationLevel::ReadCommitted).unwrap();
            let tx_id = _tx.id;
            assert!(manager.is_transaction_active(tx_id));
        } // Transaction drops here

        // Should be auto-aborted
        let active_txs = manager.get_active_transactions();
        assert!(active_txs.is_empty());
    }

    #[test]
    fn test_data_snapshot() {
        let mut snapshot = DataSnapshot::new();
        let rows = vec![
            vec![
                crate::types::Value::Integer(1),
                crate::types::Value::Varchar("test".to_string()),
            ],
            vec![
                crate::types::Value::Integer(2),
                crate::types::Value::Varchar("test2".to_string()),
            ],
        ];

        snapshot.add_table_data("test_table", rows.clone());

        let retrieved = snapshot.get_table_data("test_table");
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap(), &rows);

        let not_found = snapshot.get_table_data("nonexistent");
        assert!(not_found.is_none());
    }
}
