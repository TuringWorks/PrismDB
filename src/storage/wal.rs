use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::common::error::{PrismDBError, Result};
use crate::types::Value;

/// WAL record types
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WalRecordType {
    BeginTransaction,
    CommitTransaction,
    AbortTransaction,
    Insert,
    Update,
    Delete,
    Checkpoint,
}

/// WAL record for logging operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalRecord {
    pub record_type: WalRecordType,
    pub transaction_id: Option<uuid::Uuid>,
    pub timestamp: u64,
    pub sequence_number: u64,
    pub data: WalRecordData,
}

/// WAL record data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalRecordData {
    BeginTransaction {
        isolation_level: String,
    },
    CommitTransaction,
    AbortTransaction,
    Insert {
        table_id: String,
        row_id: u64,
        values: Vec<Value>,
    },
    Update {
        table_id: String,
        row_id: u64,
        old_values: Vec<Value>,
        new_values: Vec<Value>,
    },
    Delete {
        table_id: String,
        row_id: u64,
        old_values: Vec<Value>,
    },
    Checkpoint {
        checkpoint_id: u64,
    },
}

impl WalRecord {
    pub fn new(
        record_type: WalRecordType,
        transaction_id: Option<uuid::Uuid>,
        data: WalRecordData,
    ) -> Self {
        Self {
            record_type,
            transaction_id,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            sequence_number: 0, // Will be set by WAL manager
            data,
        }
    }

    /// Serialize record to bytes
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let config = bincode::config::standard();
        let data = bincode::serde::encode_to_vec(self, config)
            .map_err(|e| PrismDBError::Wal(format!("Failed to serialize WAL record: {}", e)))?;
        Ok(data)
    }

    /// Deserialize record from bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let config = bincode::config::standard();
        let (record, _) = bincode::serde::decode_from_slice(bytes, config)
            .map_err(|e| PrismDBError::Wal(format!("Failed to deserialize WAL record: {}", e)))?;
        Ok(record)
    }
}

/// WAL file manager
#[derive(Debug)]
pub struct WalFileManager {
    wal_dir: PathBuf,
    current_file: Arc<Mutex<Option<BufWriter<File>>>>,
    current_file_number: Arc<Mutex<u64>>,
    max_file_size: usize,
    sequence_number: Arc<Mutex<u64>>,
}

impl WalFileManager {
    pub fn new<P: AsRef<Path>>(wal_dir: P, max_file_size: usize) -> Result<Self> {
        let wal_dir = wal_dir.as_ref().to_path_buf();

        // Create WAL directory if it doesn't exist
        std::fs::create_dir_all(&wal_dir)
            .map_err(|e| PrismDBError::Wal(format!("Failed to create WAL directory: {}", e)))?;

        Ok(Self {
            wal_dir,
            current_file: Arc::new(Mutex::new(None)),
            current_file_number: Arc::new(Mutex::new(0)),
            max_file_size,
            sequence_number: Arc::new(Mutex::new(0)),
        })
    }

    /// Get the current WAL file path
    fn get_wal_file_path(&self, file_number: u64) -> PathBuf {
        self.wal_dir.join(format!("wal_{:020}.log", file_number))
    }

    /// Rotate to a new WAL file
    fn rotate_wal_file(&self) -> Result<()> {
        let mut current_file = self.current_file.lock().unwrap();
        let mut file_number = self.current_file_number.lock().unwrap();

        // Close current file if open
        if let Some(mut writer) = current_file.take() {
            writer
                .flush()
                .map_err(|e| PrismDBError::Wal(format!("Failed to flush WAL file: {}", e)))?;
        }

        // Increment file number
        *file_number += 1;

        // Open new file
        let file_path = self.get_wal_file_path(*file_number);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)
            .map_err(|e| {
                PrismDBError::Wal(format!(
                    "Failed to open WAL file {}: {}",
                    file_path.display(),
                    e
                ))
            })?;

        *current_file = Some(BufWriter::new(file));

        Ok(())
    }

    /// Ensure WAL file is open and ready for writing
    fn ensure_file_open(&self) -> Result<()> {
        let mut current_file = self.current_file.lock().unwrap();

        if current_file.is_none() {
            let file_number = *self.current_file_number.lock().unwrap();
            let file_path = self.get_wal_file_path(file_number);

            // Check if file exists, if not create it
            if !file_path.exists() {
                let file = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .open(&file_path)
                    .map_err(|e| {
                        PrismDBError::Wal(format!(
                            "Failed to create WAL file {}: {}",
                            file_path.display(),
                            e
                        ))
                    })?;
                *current_file = Some(BufWriter::new(file));
            } else {
                let file = OpenOptions::new()
                    .append(true)
                    .open(&file_path)
                    .map_err(|e| {
                        PrismDBError::Wal(format!(
                            "Failed to open WAL file {}: {}",
                            file_path.display(),
                            e
                        ))
                    })?;
                *current_file = Some(BufWriter::new(file));
            }
        }

        Ok(())
    }

    /// Write a record to the WAL
    pub fn write_record(&self, mut record: WalRecord) -> Result<()> {
        self.ensure_file_open()?;

        // Set sequence number
        {
            let mut seq_num = self.sequence_number.lock().unwrap();
            record.sequence_number = *seq_num;
            *seq_num += 1;
        }

        // Serialize record
        let bytes = record.to_bytes()?;
        let length = bytes.len() as u32;

        let mut current_file = self.current_file.lock().unwrap();
        if let Some(writer) = current_file.as_mut() {
            // Write record length
            writer
                .write_u32::<LittleEndian>(length)
                .map_err(|e| PrismDBError::Wal(format!("Failed to write record length: {}", e)))?;

            // Write record data
            writer
                .write_all(&bytes)
                .map_err(|e| PrismDBError::Wal(format!("Failed to write record data: {}", e)))?;

            // Flush to ensure durability
            writer
                .flush()
                .map_err(|e| PrismDBError::Wal(format!("Failed to flush WAL record: {}", e)))?;
        }

        // Check if we need to rotate file
        self.check_file_rotation()?;

        Ok(())
    }

    /// Check if current WAL file needs rotation
    fn check_file_rotation(&self) -> Result<()> {
        let current_file_number = *self.current_file_number.lock().unwrap();
        let file_path = self.get_wal_file_path(current_file_number);

        if let Ok(metadata) = std::fs::metadata(&file_path) {
            if metadata.len() as usize >= self.max_file_size {
                self.rotate_wal_file()?;
            }
        }

        Ok(())
    }

    /// Read all records from WAL files
    pub fn read_all_records(&self) -> Result<Vec<WalRecord>> {
        let mut records = Vec::new();

        // Get all WAL files
        let wal_files = self.get_wal_files()?;

        for file_path in wal_files {
            let file_records = self.read_records_from_file(&file_path)?;
            records.extend(file_records);
        }

        Ok(records)
    }

    /// Get all WAL file paths sorted by file number
    fn get_wal_files(&self) -> Result<Vec<PathBuf>> {
        let mut files = Vec::new();

        if let Ok(entries) = std::fs::read_dir(&self.wal_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                    if file_name.starts_with("wal_") && file_name.ends_with(".log") {
                        files.push(path);
                    }
                }
            }
        }

        // Sort by file number
        files.sort();

        Ok(files)
    }

    /// Read records from a specific WAL file
    fn read_records_from_file(&self, file_path: &Path) -> Result<Vec<WalRecord>> {
        let mut records = Vec::new();

        let file = File::open(file_path).map_err(|e| {
            PrismDBError::Wal(format!(
                "Failed to open WAL file {}: {}",
                file_path.display(),
                e
            ))
        })?;

        let mut reader = BufReader::new(file);

        loop {
            // Read record length
            match reader.read_u32::<LittleEndian>() {
                Ok(length) => {
                    // Read record data
                    let mut buffer = vec![0u8; length as usize];
                    reader.read_exact(&mut buffer).map_err(|e| {
                        PrismDBError::Wal(format!("Failed to read record data: {}", e))
                    })?;

                    // Deserialize record
                    let record = WalRecord::from_bytes(&buffer)?;
                    records.push(record);
                }
                Err(_) => break, // End of file
            }
        }

        Ok(records)
    }

    /// Flush all pending writes
    pub fn flush(&self) -> Result<()> {
        let mut current_file = self.current_file.lock().unwrap();
        if let Some(writer) = current_file.as_mut() {
            writer
                .flush()
                .map_err(|e| PrismDBError::Wal(format!("Failed to flush WAL: {}", e)))?;
        }
        Ok(())
    }

    /// Close current WAL file
    pub fn close(&self) -> Result<()> {
        let mut current_file = self.current_file.lock().unwrap();
        if let Some(mut writer) = current_file.take() {
            writer
                .flush()
                .map_err(|e| PrismDBError::Wal(format!("Failed to flush WAL on close: {}", e)))?;
        }
        Ok(())
    }
}

/// Write-Ahead Log manager
#[derive(Debug)]
pub struct WalManager {
    file_manager: WalFileManager,
    enabled: Arc<RwLock<bool>>,
}

impl WalManager {
    pub fn new<P: AsRef<Path>>(wal_dir: P) -> Result<Self> {
        let file_manager = WalFileManager::new(wal_dir, 100 * 1024 * 1024)?; // 100MB max file size

        Ok(Self {
            file_manager,
            enabled: Arc::new(RwLock::new(true)),
        })
    }

    /// Enable or disable WAL
    pub fn set_enabled(&self, enabled: bool) {
        let mut enabled_lock = self.enabled.write().unwrap();
        *enabled_lock = enabled;
    }

    /// Check if WAL is enabled
    pub fn is_enabled(&self) -> bool {
        *self.enabled.read().unwrap()
    }

    /// Log transaction begin
    pub fn log_begin_transaction(
        &self,
        transaction_id: uuid::Uuid,
        isolation_level: &str,
    ) -> Result<()> {
        if !self.is_enabled() {
            return Ok(());
        }

        let data = WalRecordData::BeginTransaction {
            isolation_level: isolation_level.to_string(),
        };

        let record = WalRecord::new(WalRecordType::BeginTransaction, Some(transaction_id), data);

        self.file_manager.write_record(record)
    }

    /// Log transaction commit
    pub fn log_commit_transaction(&self, transaction_id: uuid::Uuid) -> Result<()> {
        if !self.is_enabled() {
            return Ok(());
        }

        let record = WalRecord::new(
            WalRecordType::CommitTransaction,
            Some(transaction_id),
            WalRecordData::CommitTransaction,
        );

        self.file_manager.write_record(record)
    }

    /// Log transaction abort
    pub fn log_abort_transaction(&self, transaction_id: uuid::Uuid) -> Result<()> {
        if !self.is_enabled() {
            return Ok(());
        }

        let record = WalRecord::new(
            WalRecordType::AbortTransaction,
            Some(transaction_id),
            WalRecordData::AbortTransaction,
        );

        self.file_manager.write_record(record)
    }

    /// Log insert operation
    pub fn log_insert(
        &self,
        transaction_id: uuid::Uuid,
        table_id: &str,
        row_id: u64,
        values: Vec<Value>,
    ) -> Result<()> {
        if !self.is_enabled() {
            return Ok(());
        }

        let data = WalRecordData::Insert {
            table_id: table_id.to_string(),
            row_id,
            values,
        };

        let record = WalRecord::new(WalRecordType::Insert, Some(transaction_id), data);

        self.file_manager.write_record(record)
    }

    /// Log update operation
    pub fn log_update(
        &self,
        transaction_id: uuid::Uuid,
        table_id: &str,
        row_id: u64,
        old_values: Vec<Value>,
        new_values: Vec<Value>,
    ) -> Result<()> {
        if !self.is_enabled() {
            return Ok(());
        }

        let data = WalRecordData::Update {
            table_id: table_id.to_string(),
            row_id,
            old_values,
            new_values,
        };

        let record = WalRecord::new(WalRecordType::Update, Some(transaction_id), data);

        self.file_manager.write_record(record)
    }

    /// Log delete operation
    pub fn log_delete(
        &self,
        transaction_id: uuid::Uuid,
        table_id: &str,
        row_id: u64,
        old_values: Vec<Value>,
    ) -> Result<()> {
        if !self.is_enabled() {
            return Ok(());
        }

        let data = WalRecordData::Delete {
            table_id: table_id.to_string(),
            row_id,
            old_values,
        };

        let record = WalRecord::new(WalRecordType::Delete, Some(transaction_id), data);

        self.file_manager.write_record(record)
    }

    /// Log checkpoint
    pub fn log_checkpoint(&self, checkpoint_id: u64) -> Result<()> {
        if !self.is_enabled() {
            return Ok(());
        }

        let record = WalRecord::new(
            WalRecordType::Checkpoint,
            None,
            WalRecordData::Checkpoint { checkpoint_id },
        );

        self.file_manager.write_record(record)
    }

    /// Replay WAL records for recovery
    pub fn replay(&self) -> Result<Vec<WalRecord>> {
        let records = self.file_manager.read_all_records()?;

        // Filter and sort records for replay
        let mut replay_records = Vec::new();
        let mut committed_transactions = std::collections::HashSet::new();

        // First pass: identify committed transactions
        for record in &records {
            match record.record_type {
                WalRecordType::CommitTransaction => {
                    if let Some(tx_id) = record.transaction_id {
                        committed_transactions.insert(tx_id);
                    }
                }
                _ => {}
            }
        }

        // Second pass: collect records from committed transactions
        for record in records {
            match record.record_type {
                WalRecordType::BeginTransaction
                | WalRecordType::CommitTransaction
                | WalRecordType::AbortTransaction => {
                    // Always include transaction control records
                    replay_records.push(record);
                }
                _ => {
                    // Only include data records from committed transactions
                    if let Some(tx_id) = record.transaction_id {
                        if committed_transactions.contains(&tx_id) {
                            replay_records.push(record);
                        }
                    }
                }
            }
        }

        Ok(replay_records)
    }

    /// Flush WAL to disk
    pub fn flush(&self) -> Result<()> {
        self.file_manager.flush()
    }

    /// Close WAL
    pub fn close(&self) -> Result<()> {
        self.file_manager.close()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use uuid::Uuid;

    #[test]
    fn test_wal_record_serialization() {
        let data = WalRecordData::Insert {
            table_id: "test_table".to_string(),
            row_id: 1,
            values: vec![Value::Integer(42), Value::Varchar("test".to_string())],
        };

        let record = WalRecord::new(WalRecordType::Insert, Some(Uuid::new_v4()), data);

        let bytes = record.to_bytes().unwrap();
        let deserialized = WalRecord::from_bytes(&bytes).unwrap();

        assert_eq!(record.record_type, deserialized.record_type);
        assert_eq!(record.transaction_id, deserialized.transaction_id);
    }

    #[test]
    fn test_wal_file_manager() {
        let temp_dir = TempDir::new().unwrap();
        let manager = WalFileManager::new(temp_dir.path(), 1024).unwrap();

        let data = WalRecordData::Insert {
            table_id: "test".to_string(),
            row_id: 1,
            values: vec![Value::Integer(42)],
        };

        let record = WalRecord::new(WalRecordType::Insert, Some(Uuid::new_v4()), data);

        manager.write_record(record).unwrap();
        manager.flush().unwrap();

        let records = manager.read_all_records().unwrap();
        assert_eq!(records.len(), 1);
    }

    #[test]
    fn test_wal_manager() {
        let temp_dir = TempDir::new().unwrap();
        let wal_manager = WalManager::new(temp_dir.path()).unwrap();

        let tx_id = Uuid::new_v4();

        // Test transaction logging
        wal_manager
            .log_begin_transaction(tx_id, "ReadCommitted")
            .unwrap();
        wal_manager
            .log_insert(tx_id, "test_table", 1, vec![Value::Integer(42)])
            .unwrap();
        wal_manager.log_commit_transaction(tx_id).unwrap();

        wal_manager.flush().unwrap();

        // Test replay
        let records = wal_manager.replay().unwrap();
        assert_eq!(records.len(), 3);
    }

    #[test]
    fn test_wal_enable_disable() {
        let temp_dir = TempDir::new().unwrap();
        let wal_manager = WalManager::new(temp_dir.path()).unwrap();

        assert!(wal_manager.is_enabled());

        wal_manager.set_enabled(false);
        assert!(!wal_manager.is_enabled());

        // Should not write when disabled
        let tx_id = Uuid::new_v4();
        wal_manager
            .log_begin_transaction(tx_id, "ReadCommitted")
            .unwrap();

        let records = wal_manager.replay().unwrap();
        assert_eq!(records.len(), 0);
    }
}
