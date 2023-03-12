use std::collections::BTreeMap;

use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
    sync::Mutex,
};

use anyhow::Result;
use async_trait::async_trait;
use tokio::io::BufWriter;
use tracing::info;

#[async_trait]
pub trait StableStorage: Send + Sync {
    /// Adds an entry to the storage.
    async fn append(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()>;

    /// Ensures that entries that are in memory or
    /// in the operating system cache are flushed to the disk.
    async fn flush(&self) -> Result<()>;

    /// Returns the value associated to the key if it exists.
    async fn get(&self, key: &[u8]) -> Option<Vec<u8>>;

    // TODO: this copy is unecessary.
    /// Returns a copy of the key value pairs that are in memory.
    async fn entries(&self) -> BTreeMap<Vec<u8>, Vec<u8>>;
}

pub struct DiskLogStorage {
    volatile_state: Mutex<VolatileState>,
}

struct VolatileState {
    /// File used to store the log.
    file_writer: BufWriter<File>,
    /// The position where the next entry will be added.
    position: u64,
    /// The key value pairs the are in the storage.
    entries: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl DiskLogStorage {
    #[tracing::instrument(name = "DiskLogStorage::new", skip_all, fields(
        file_path = ?file_path
    ))]
    pub async fn new(file_path: &str) -> Result<Self> {
        let file_name = format!("{file_path}/data.log");

        info!(?file_path, ?file_name, "creating log file");
        tokio::fs::create_dir_all(file_path).await?;

        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(file_name)
            .await?;

        let metadata = file.metadata().await?;

        let entries = DiskLogStorage::read_state_from_disk(&mut file).await?;

        Ok(Self {
            volatile_state: Mutex::new(VolatileState {
                file_writer: BufWriter::new(file),
                position: metadata.len(),
                entries,
            }),
        })
    }

    #[tracing::instrument(name = "DiskLogStorage::read_state_from_disk", skip_all)]
    async fn read_state_from_disk(file: &mut File) -> Result<BTreeMap<Vec<u8>, Vec<u8>>> {
        let mut entries = BTreeMap::new();

        let mut position = 0;
        let file_size = file.metadata().await?.len();

        let mut reader = BufReader::new(file);

        while position < file_size {
            let key_len = reader.read_u32().await?;

            let mut key = vec![0_u8; key_len as usize];
            reader.read_exact(&mut key).await?;

            let value_len = reader.read_u32().await?;
            let mut value = vec![0_u8; value_len as usize];
            reader.read_exact(&mut value).await?;

            position += key_len as u64 + key.len() as u64 + value_len as u64 + key.len() as u64;

            entries.insert(key, value);
        }

        Ok(entries)
    }
}

#[async_trait]
impl StableStorage for DiskLogStorage {
    #[tracing::instrument(name = "DiskLogStorage::append", skip_all)]
    async fn append(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        const KEY_LEN: usize = 4;
        const VALUE_LEN: usize = 4;

        let entry_len = (KEY_LEN + key.len() + VALUE_LEN + value.len()) as u64;

        let mut volatile_state = self.volatile_state.lock().await;

        volatile_state
            .file_writer
            .write_u32(key.len() as u32)
            .await?;
        volatile_state.file_writer.write_all(&key).await?;

        volatile_state
            .file_writer
            .write_u32(value.len() as u32)
            .await?;
        volatile_state.file_writer.write_all(&value).await?;

        volatile_state.position += entry_len;

        volatile_state.entries.insert(key, value);

        Ok(())
    }

    #[tracing::instrument(name = "DiskLogStorage::flush", skip_all)]
    async fn flush(&self) -> Result<()> {
        let mut volatile_state = self.volatile_state.lock().await;
        volatile_state.file_writer.flush().await?;
        Ok(())
    }

    #[tracing::instrument(name = "DiskLogStorage::get", skip_all)]
    async fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let volatile_state = self.volatile_state.lock().await;
        volatile_state.entries.get(key).cloned()
    }

    #[tracing::instrument(name = "DiskLogStorage::entries", skip_all)]
    async fn entries(&self) -> BTreeMap<Vec<u8>, Vec<u8>> {
        let volatile_state = self.volatile_state.lock().await;
        volatile_state.entries.clone()
    }
}
