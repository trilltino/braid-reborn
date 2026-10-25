//! One transaction contains application state, pending identities and cursor.
//! Native and browser adapters must publish only after their durable commit.
use crate::{
    types::{Update, Version},
    BraidError, Result,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

pub const MAX_JOURNAL_BYTES: usize = 32 * 1024 * 1024;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ChannelCheckpoint {
    #[serde(default)]
    pub resource_url: Option<String>,
    pub receive_frontier: Vec<Version>,
    pub local_frontier: Vec<Version>,
    pub outbox: VecDeque<Update>,
    pub application: Option<serde_json::Value>,
}

/// A backend owns exclusive writing and commits the entire record atomically.
#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
#[allow(clippy::double_must_use)] // async-trait marks its boxed Future must_use.
pub trait JournalBackend: Send + Sync + 'static {
    async fn load(&mut self) -> Result<Option<Vec<u8>>>;
    async fn store(&mut self, bytes: &[u8]) -> Result<()>;
}
/// Browser database handles and transaction futures stay on their local executor.
#[cfg(target_arch = "wasm32")]
#[async_trait(?Send)]
#[allow(clippy::double_must_use)] // Same async-trait expansion for local futures.
pub trait JournalBackend: 'static {
    async fn load(&mut self) -> Result<Option<Vec<u8>>>;
    async fn store(&mut self, bytes: &[u8]) -> Result<()>;
}

struct MemoryJournal;
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch="wasm32", async_trait(?Send))]
impl JournalBackend for MemoryJournal {
    async fn load(&mut self) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }
    async fn store(&mut self, _: &[u8]) -> Result<()> {
        Ok(())
    }
}

pub struct ChannelJournal {
    pub(super) checkpoint: ChannelCheckpoint,
    backend: Box<dyn JournalBackend>,
}
impl ChannelJournal {
    pub fn memory() -> Self {
        Self {
            checkpoint: ChannelCheckpoint::default(),
            backend: Box::new(MemoryJournal),
        }
    }
    pub async fn from_backend(mut backend: impl JournalBackend) -> Result<Self> {
        let checkpoint = match backend.load().await? {
            Some(bytes) => {
                if bytes.len() > MAX_JOURNAL_BYTES {
                    return Err(journal_limit());
                }
                serde_json::from_slice(&bytes).map_err(|error| {
                    BraidError::Config(format!(
                        "Invalid journal at line {}, column {}",
                        error.line(),
                        error.column()
                    ))
                })?
            }
            None => ChannelCheckpoint::default(),
        };
        validate_checkpoint(&checkpoint)?;
        Ok(Self {
            checkpoint,
            backend: Box::new(backend),
        })
    }
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn open(path: impl AsRef<std::path::Path>) -> Result<Self> {
        Self::from_backend(FileJournal::open(path.as_ref()).await?).await
    }
    pub fn checkpoint(&self) -> &ChannelCheckpoint {
        &self.checkpoint
    }
    pub async fn initialize_application(
        &mut self,
        frontier: Vec<Version>,
        application: serde_json::Value,
    ) -> Result<()> {
        if self.checkpoint.application.is_some() || !self.checkpoint.outbox.is_empty() {
            return Err(BraidError::Config(
                "Cannot initialize a populated channel journal".into(),
            ));
        }
        let mut next = self.checkpoint.clone();
        next.receive_frontier = frontier.clone();
        next.local_frontier = frontier;
        next.application = Some(application);
        self.commit(next).await
    }
    pub(super) async fn commit(&mut self, next: ChannelCheckpoint) -> Result<()> {
        validate_checkpoint(&next)?;
        let bytes = serde_json::to_vec(&next)?;
        if bytes.len() > MAX_JOURNAL_BYTES {
            return Err(journal_limit());
        }
        self.backend.store(&bytes).await?;
        self.checkpoint = next;
        Ok(())
    }
}
fn validate_checkpoint(checkpoint: &ChannelCheckpoint) -> Result<()> {
    if let Some(url) = &checkpoint.resource_url {
        super::validate_resource_url(url)?;
    }
    for update in &checkpoint.outbox {
        let mut metadata = update.extra_headers.clone();
        crate::protocol::strip_transport_headers(&mut metadata);
        if metadata != update.extra_headers {
            return Err(BraidError::Config("Legacy outbox contains transport headers; preserve it for explicit local migration".into()));
        }
    }
    Ok(())
}
fn journal_limit() -> BraidError {
    BraidError::ResourceLimit {
        resource: "channel journal",
        limit: MAX_JOURNAL_BYTES as u64,
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub struct FileJournal {
    path: std::path::PathBuf,
    _lease: std::fs::File,
}
#[cfg(not(target_arch = "wasm32"))]
impl FileJournal {
    pub async fn open(path: &std::path::Path) -> Result<Self> {
        let path = std::path::absolute(path)?;
        let parent = path
            .parent()
            .ok_or_else(|| BraidError::Config("Journal needs a parent directory".into()))?;
        tokio::fs::create_dir_all(parent).await?;
        let lease = crate::durable::writer_lease(&path.with_extension("lock")).await?;
        Ok(Self {
            path,
            _lease: lease,
        })
    }
}
#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl JournalBackend for FileJournal {
    async fn load(&mut self) -> Result<Option<Vec<u8>>> {
        use tokio::io::AsyncReadExt;
        let file = match tokio::fs::File::open(&self.path).await {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let mut bytes = Vec::new();
        file.take(MAX_JOURNAL_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .await?;
        if bytes.len() > MAX_JOURNAL_BYTES {
            return Err(journal_limit());
        }
        Ok(Some(bytes))
    }
    async fn store(&mut self, bytes: &[u8]) -> Result<()> {
        Ok(crate::durable::atomic_write(&self.path, bytes).await?)
    }
}
