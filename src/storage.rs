use anyhow::Result;
use async_trait::async_trait;

#[async_trait]
pub trait StableStorage: Send + Sync {
    /// Adds an entry to the storage.
    async fn append(&self, key: &[u8], value: &[u8]) -> Result<()>;

    /// Ensures that entries that are in memory or
    /// in the operating system cache are flushed to the disk.
    async fn flush(&self) -> Result<()>;
}

pub struct DiskLogStorage {}

#[async_trait]
impl StableStorage for DiskLogStorage {
    #[tracing::instrument(name = "DiskLogStorage::append", skip_all)]
    async fn append(&self, key: &[u8], value: &[u8]) -> Result<()> {
        todo!()
    }

    #[tracing::instrument(name = "DiskLogStorage::flush", skip_all)]
    async fn flush(&self) -> Result<()> {
        todo!()
    }
}
