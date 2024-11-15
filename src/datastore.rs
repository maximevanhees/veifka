use crate::DataStoreError;
use fjall::{Config, Keyspace, PartitionCreateOptions, PartitionHandle};
use std::sync::Arc;

#[derive(Clone)]
pub struct DataStore {
    // Keep keyspace around as long as we need it's partitions!
    keyspace: Keyspace,
    partition_handle: Arc<PartitionHandle>,
}

impl DataStore {
    pub fn new(keyspace_name: &str, partition_name: &str) -> Result<Self, DataStoreError> {
        // A keyspace is a database, which may contain multiple collections ("partitions")
        let keyspace = Config::new(keyspace_name)
            .open()
            .map_err(|e| DataStoreError::KeyspaceError(e.to_string()))?;

        // Each partition is its own physical LSM-tree
        let partition_handle = keyspace
            .open_partition(
                partition_name,
                // Manual journal persist is done for testing write amplification
                PartitionCreateOptions::default().manual_journal_persist(true),
            )
            .map_err(|e| DataStoreError::PartitionError(e.to_string()))?;

        Ok(DataStore {
            keyspace,
            partition_handle: Arc::new(partition_handle),
        })
    }

    pub fn keyspace(&self) -> &Keyspace {
        &self.keyspace
    }

    pub fn partition_handle(&self) -> Arc<PartitionHandle> {
        Arc::clone(&self.partition_handle)
    }

    pub fn set(&self, key: &[u8], value: &[u8]) -> Result<(), fjall::Error> {
        self.partition_handle.insert(key, value)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, fjall::Error> {
        self.partition_handle
            .get(key)
            .map(|opt| opt.map(|v| v.to_vec()))
    }

    pub fn delete(&self, key: &[u8]) -> Result<(), fjall::Error> {
        self.partition_handle.remove(key)
    }

    pub fn exists(&self, key: &[u8]) -> Result<bool, fjall::Error> {
        self.get(key).map(|opt| opt.is_some())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_store() -> DataStore {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        DataStore::new(temp_dir.path().to_str().unwrap(), "test_partition").unwrap()
    }

    #[test]
    fn test_basic_operations() {
        let store = create_test_store();

        // Test set and get
        store.set(b"key1", b"value1").unwrap();
        assert_eq!(store.get(b"key1").unwrap(), Some(b"value1".to_vec()));

        // Test exists
        assert!(store.exists(b"key1").unwrap());
        assert!(!store.exists(b"nonexistent").unwrap());

        // Test delete
        store.delete(b"key1").unwrap();
        assert_eq!(store.get(b"key1").unwrap(), None);
    }
}
