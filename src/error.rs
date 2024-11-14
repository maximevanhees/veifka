use thiserror::Error;
#[derive(Error, Debug, Clone)]
pub enum DataStoreError {
    #[error("Keyspace error: {0}")]
    KeyspaceError(String),
    #[error("Partition error: {0}")]
    PartitionError(String),
    #[error("Data error: {0}")]
    DataError(String),
}
