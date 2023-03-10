use tokio::{
    sync::Mutex,
    fs::{File, OpenOptions},
    io::AsyncWriteExt,
};

use anyhow::Result;
use async_trait::async_trait;
use tokio::io::BufWriter;

#[async_trait]
pub trait StableStorage: Send + Sync {
    /// Adds an entry to the storage.
    async fn append(&self, key: &[u8], value: &[u8]) -> Result<()>;

    /// Ensures that entries that are in memory or
    /// in the operating system cache are flushed to the disk.
    async fn flush(&self) -> Result<()>;
}

pub struct DiskLogStorage {
    volatile_state: Mutex<VolatileState>
}

struct VolatileState {
/// File used to store the log.
file_writer: BufWriter<File>,
/// The position where the next entry will be added.
position: u64,
}

impl DiskLogStorage {
    #[tracing::instrument(name = "DiskLogStorage::new", skip_all, fields(
        file_path = ?file_path
    ))]
    pub async fn new(file_path: &str) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(file_path)
            .await?;

        let metadata = file.metadata().await?;

        Ok(Self {
            volatile_state: Mutex::new(VolatileState {
                file_writer: BufWriter::new(file),
                position: metadata.len(),
            })
        })
    }
}

#[async_trait]
impl StableStorage for DiskLogStorage {
    #[tracing::instrument(name = "DiskLogStorage::append", skip_all)]
    async fn append(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let entry_len = std::mem::size_of<u32>() + key.len()
        + std::mem::size_of<u32>() + value.len();

        self.file_writer.write_u32(key.len() as u32).await?;
        self.file_writer.write_all(key).await?;

        self.file_writer.write_u32(value.len() as u32).await?;
        self.file_writer.write_all(value).await?;

        let entry_starts_at = self.position;
        self.position+=entry_len;

        Ok(())
    }

    #[tracing::instrument(name = "DiskLogStorage::flush", skip_all)]
    async fn flush(&self) -> Result<()> {
        self.file_writer.flush().await?;
        Ok(())
    }
}
