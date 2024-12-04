use crate::DataStoreError;
use fjall::{Config, Keyspace, PartitionCreateOptions, PartitionHandle};
use std::sync::Arc;

#[derive(Clone)]
pub struct DataStore {
    // Keep keyspace around as long as we need its partitions!
    keyspace: Keyspace,
    // partition_handle: Arc<PartitionHandle>,
}

impl DataStore {
    // pub fn new(keyspace_name: &str, partition_name: &str) -> Result<Self, DataStoreError> {
    pub fn new(keyspace_name: &str) -> Result<Self, DataStoreError> {
        // A keyspace is a database, which may contain multiple collections ("partitions")
        let keyspace = Config::new(keyspace_name)
            .open()
            .map_err(|e| DataStoreError::KeyspaceError(e.to_string()))?;

        // Each partition is its own physical LSM-tree
        // let partition_handle = keyspace
        //     .open_partition(
        //         partition_name,
        //         // Manual journal persist is done for testing write amplification
        //         PartitionCreateOptions::default().manual_journal_persist(true),
        //     )
        //     .map_err(|e| DataStoreError::PartitionError(e.to_string()))?;

        Ok(DataStore {
            keyspace,
            // partition_handle: Arc::new(partition_handle),
        })
    }

    pub fn keyspace(&self) -> &Keyspace {
        &self.keyspace
    }

    pub fn create_partition(
        &self,
        partition_name: &str,
    ) -> Result<PartitionHandle, DataStoreError> {
        let partition_handle = self
            .keyspace
            .open_partition(partition_name, PartitionCreateOptions::default())
            .map_err(|e| DataStoreError::PartitionError(e.to_string()))?;

        Ok(partition_handle)
    }

    // pub fn partition_handle(&self) -> Arc<PartitionHandle> {
    //     Arc::clone(&self.partition_handle)
    // }
    //
    // pub fn set(&self, key: &[u8], value: &[u8]) -> Result<(), fjall::Error> {
    //     self.partition_handle.insert(key, value)
    // }
    //
    // pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, fjall::Error> {
    //     self.partition_handle
    //         .get(key)
    //         .map(|opt| opt.map(|v| v.to_vec()))
    // }
    //
    // pub fn delete(&self, key: &[u8]) -> Result<(), fjall::Error> {
    //     self.partition_handle.remove(key)
    // }
    //
    // pub fn exists(&self, key: &[u8]) -> Result<bool, fjall::Error> {
    //     self.get(key).map(|opt| opt.is_some())
    // }
}

#[derive(Clone)]
pub struct DataStorePartition {
    partition_handle: Arc<PartitionHandle>,
}

impl DataStorePartition {
    pub fn new(partition_handle: PartitionHandle) -> Self {
        DataStorePartition {
            partition_handle: Arc::new(partition_handle),
        }
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

    fn create_test_store() -> (DataStore, DataStorePartition) {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let data_store = DataStore::new(temp_dir.path().to_str().unwrap()).unwrap();
        let partition_handle = data_store.create_partition("test_partition").unwrap();
        let partition = DataStorePartition::new(partition_handle);
        (data_store, partition)
    }

    #[test]
    fn test_basic_operations() {
        let (_data_store, store) = create_test_store();

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
