//! Binary file synchronization for BraidFS.
//!
//! Implements binary file sync using braid-blob for non-text files.
//! Matches JS `init_binary_sync()` from braidfs/index.js.

use crate::core::Result;
use crate::fs::config::{get_root_dir, is_binary};
use crate::fs::rate_limiter::ReconnectRateLimiter;
use braid_blob::BlobStore;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;

/// State for a binary sync operation.
#[derive(Debug)]
pub struct BinarySyncState {
    /// The URL being synced.
    pub url: String,
    /// Peer ID for this sync.
    pub peer: String,
    /// Merge type (should be "aww" for binary).
    pub merge_type: String,
    /// Last known file modification time (nanoseconds as string).
    pub file_mtime_ns_str: Option<String>,
    /// Whether the file is read-only.
    pub file_read_only: Option<bool>,
    /// Abort controller.
    pub aborted: bool,
}

impl BinarySyncState {
    pub fn new(url: String) -> Self {
        Self {
            url,
            peer: uuid::Uuid::new_v4().to_string()[..12].to_string(),
            merge_type: "aww".to_string(),
            file_mtime_ns_str: None,
            file_read_only: None,
            aborted: false,
        }
    }
}

/// Binary sync manager for multiple URLs.
#[derive(Debug)]
pub struct BinarySyncManager {
    /// Active sync states.
    syncs: Arc<RwLock<HashMap<String, BinarySyncState>>>,
    /// Rate limiter for reconnections.
    rate_limiter: Arc<ReconnectRateLimiter>,
    /// Blob store for persistence.
    blob_store: Option<Arc<BlobStore>>,
    /// Temp folder for atomic writes.
    temp_folder: PathBuf,
    /// Meta folder for sync metadata.
    meta_folder: PathBuf,
}

impl BinarySyncManager {
    /// Create a new binary sync manager.
    pub fn new(
        rate_limiter: Arc<ReconnectRateLimiter>,
        blob_store: Arc<BlobStore>,
    ) -> Result<Self> {
        let root = get_root_dir().map_err(|e| crate::core::BraidError::Config(e.to_string()))?;
        let braidfs_dir = root.join(".braidfs");

        Ok(Self {
            syncs: Arc::new(RwLock::new(HashMap::new())),
            rate_limiter,
            blob_store: Some(blob_store),
            temp_folder: braidfs_dir.join("temp"),
            meta_folder: braidfs_dir.join("braid-blob-meta"),
        })
    }

    /// Initialize a binary sync for a URL.
    pub async fn init_binary_sync(&self, url: &str, fullpath: &Path) -> Result<()> {
        tracing::info!("init_binary_sync: {}", url);

        // Create state
        let mut state = BinarySyncState::new(url.to_string());

        // Try to load existing metadata
        let meta_path = self.get_meta_path(url);
        if let Ok(content) = tokio::fs::read_to_string(&meta_path).await {
            if let Ok(meta) = serde_json::from_str::<serde_json::Value>(&content) {
                if let Some(peer) = meta.get("peer").and_then(|v| v.as_str()) {
                    state.peer = peer.to_string();
                }
                if let Some(mtime) = meta.get("file_mtime_ns_str").and_then(|v| v.as_str()) {
                    state.file_mtime_ns_str = Some(mtime.to_string());
                }
            }
        }

        // Save metadata
        self.save_meta(url, &state).await?;

        // Signal initial file read
        self.signal_file_needs_reading(url, fullpath).await?;

        // Store state
        self.syncs.write().await.insert(url.to_string(), state);

        Ok(())
    }

    /// Signal that a file needs reading and potentially uploading.
    pub async fn signal_file_needs_reading(&self, url: &str, fullpath: &Path) -> Result<()> {
        // Check if file exists
        let metadata = match tokio::fs::metadata(fullpath).await {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(crate::core::BraidError::Io(e)),
        };

        // Get modification time
        let mtime = metadata
            .modified()
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH)
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let mtime_str = mtime.to_string();

        // Check if changed
        let needs_upload = {
            let syncs = self.syncs.read().await;
            if let Some(state) = syncs.get(url) {
                state.file_mtime_ns_str.as_ref() != Some(&mtime_str)
            } else {
                true
            }
        };

        if needs_upload {
            // Read file content
            let data = tokio::fs::read(fullpath).await?;

            // Upload to blob store (if configured)
            if let Some(store) = &self.blob_store {
                store
                    .put(url, data.into(), vec![], vec![], None)
                    .await
                    .map_err(crate::core::BraidError::Client)?;
            }

            // Update mtime
            let mut syncs = self.syncs.write().await;
            if let Some(state) = syncs.get_mut(url) {
                state.file_mtime_ns_str = Some(mtime_str);
                drop(syncs); // Release lock before save
                self.save_meta(url, &self.syncs.read().await.get(url).unwrap())
                    .await?;
            }
        }

        Ok(())
    }

    /// Save metadata for a sync.
    async fn save_meta(&self, url: &str, state: &BinarySyncState) -> Result<()> {
        tokio::fs::create_dir_all(&self.meta_folder).await?;

        let meta = serde_json::json!({
            "merge_type": state.merge_type,
            "peer": state.peer,
            "file_mtime_ns_str": state.file_mtime_ns_str,
        });

        let meta_path = self.get_meta_path(url);
        braid_blob::store::atomic_write(
            &meta_path,
            serde_json::to_string_pretty(&meta)?.as_bytes(),
            &self.temp_folder,
        )
        .await
        .map_err(crate::core::BraidError::Client)?;

        Ok(())
    }

    /// Get the metadata file path for a URL.
    fn get_meta_path(&self, url: &str) -> PathBuf {
        let encoded = braid_blob::store::encode_filename(url);
        self.meta_folder.join(encoded)
    }

    /// Disconnect a sync.
    pub async fn disconnect(&self, url: &str) {
        let mut syncs = self.syncs.write().await;
        if let Some(state) = syncs.get_mut(url) {
            state.aborted = true;
        }
        self.rate_limiter.on_diss(url).await;
    }

    /// Reconnect a sync.
    pub async fn reconnect(&self, url: &str, fullpath: &Path) -> Result<()> {
        self.rate_limiter.get_turn(url).await;
        self.rate_limiter.on_conn(url).await;
        self.signal_file_needs_reading(url, fullpath).await
    }

    pub fn blob_store(&self) -> Arc<BlobStore> {
        self.blob_store
            .clone()
            .expect("BlobStore must be initialized")
    }
}

/// Database interface for binary sync (matches JS `db` object).
pub struct BinarySyncDb {
    fullpath: PathBuf,
    temp_folder: PathBuf,
}

impl BinarySyncDb {
    pub fn new(fullpath: PathBuf, temp_folder: PathBuf) -> Self {
        Self {
            fullpath,
            temp_folder,
        }
    }

    /// Read file content.
    pub async fn read(&self, _key: &str) -> Result<Option<Vec<u8>>> {
        match tokio::fs::read(&self.fullpath).await {
            Ok(data) => Ok(Some(data)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(crate::core::BraidError::Io(e)),
        }
    }

    /// Write file content atomically.
    pub async fn write(&self, _key: &str, data: &[u8]) -> Result<std::fs::Metadata> {
        braid_blob::store::atomic_write(&self.fullpath, data, &self.temp_folder)
            .await
            .map_err(crate::core::BraidError::Client)
    }

    /// Delete the file.
    pub async fn delete(&self, _key: &str) -> Result<()> {
        match tokio::fs::remove_file(&self.fullpath).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(crate::core::BraidError::Io(e)),
        }
    }
}

/// Check if a file should use binary sync.
pub fn should_use_binary_sync(path: &str) -> bool {
    is_binary(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_use_binary_sync() {
        assert!(should_use_binary_sync("image.jpg"));
        assert!(should_use_binary_sync("document.pdf"));
        assert!(!should_use_binary_sync("readme.txt"));
        assert!(!should_use_binary_sync("code.rs"));
    }
}
