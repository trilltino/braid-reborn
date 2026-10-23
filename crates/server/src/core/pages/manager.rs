//! Pages Manager
//!
//! Core service for file-based page storage, broadcast, and sync.
//! Powers the unified Pages Editor for Web and Tauri clients.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs;
use tokio::sync::{broadcast, Mutex};
use tokio::sync::RwLock;
use tracing::{info, warn};

use braid_core::core::merge::merge_type::MergePatch;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PagesUpdate {
    pub path: String,
    pub version: Vec<braid_http::types::Version>,
    pub parents: Vec<braid_http::types::Version>,
    pub patches: Option<Vec<MergePatch>>,
    pub content: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PageInfo {
    pub path: String,
    pub title: String,
    pub last_modified: u64,
    pub size: u64,
}

/// Channel state that keeps the broadcast alive
struct ChannelState {
    tx: broadcast::Sender<PagesUpdate>,
    /// Store last update so new subscribers can get current state
    last_update: Arc<Mutex<Option<PagesUpdate>>>,
    /// Keep one receiver alive to prevent channel closure
    _keepalive: broadcast::Receiver<PagesUpdate>,
}

pub struct PagesManager {
    pub daemon_port: u16,
    pub storage_dir: PathBuf,
    // Map of page_path -> channel state
    channels: RwLock<HashMap<String, ChannelState>>,
}

impl PagesManager {
    pub fn new(daemon_port: u16, storage_dir: PathBuf) -> Self {
        info!(
            "[PagesManager] Initialized with storage: {:?}, daemon_port: {}",
            storage_dir, daemon_port
        );
        Self {
            daemon_port,
            storage_dir,
            channels: RwLock::new(HashMap::new()),
        }
    }

    /// Ensure storage directories exist
    pub async fn ensure_dirs(&self) -> anyhow::Result<()> {
        fs::create_dir_all(&self.storage_dir).await?;
        Ok(())
    }

    /// List all markdown pages in the wiki directory
    pub async fn list_pages(&self) -> Vec<PageInfo> {
        let mut pages = Vec::new();
        let mut entries = match fs::read_dir(&self.storage_dir).await {
            Ok(e) => e,
            Err(_) => return pages,
        };

        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            if path.extension().map_or(false, |ext| ext == "md") {
                if let Ok(meta) = entry.metadata().await {
                    let relative_path = path
                        .strip_prefix(&self.storage_dir)
                        .unwrap_or(&path)
                        .to_string_lossy()
                        .to_string();

                    pages.push(PageInfo {
                        title: self.extract_title(&path).await,
                        path: relative_path,
                        last_modified: meta
                            .modified()
                            .ok()
                            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                            .map(|d| d.as_secs())
                            .unwrap_or(0),
                        size: meta.len(),
                    });
                }
            }
        }
        pages
    }

    /// Extract a title from the first H1 or return the filename
    async fn extract_title(&self, path: &Path) -> String {
        if let Ok(content) = fs::read_to_string(path).await {
            for line in content.lines() {
                if line.starts_with("# ") {
                    return line[2..].trim().to_string();
                }
            }
        }
        path.file_stem()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| "Untitled".to_string())
    }

    /// Search for text across all pages
    pub async fn search_pages(&self, query: &str) -> Vec<PageInfo> {
        let all_pages = self.list_pages().await;
        let mut results = Vec::new();
        let query = query.to_lowercase();

        for page in all_pages {
            let full_path = self.storage_dir.join(&page.path);
            if let Ok(content) = fs::read_to_string(full_path).await {
                if content.to_lowercase().contains(&query)
                    || page.title.to_lowercase().contains(&query)
                {
                    results.push(page);
                }
            }
        }
        results
    }

    /// Start wiki discovery background task
    pub async fn start_discovery(&self) -> anyhow::Result<()> {
        info!("[PagesManager] Discovery task started");
        // In a real implementation, we'd use 'notify' crate here.
        // For now, we just ensure directories are ready.
        self.ensure_dirs().await?;
        Ok(())
    }

    /// Get a subscription channel for a page
    pub async fn subscribe(&self, path: &str) -> (broadcast::Receiver<PagesUpdate>, Option<PagesUpdate>) {
        let mut channels = self.channels.write().await;
        
        if let Some(state) = channels.get(path) {
            let rx = state.tx.subscribe();
            // Get the last update for initial state
            let last_update = state.last_update.lock().await.clone();
            info!("[PagesManager] Subscribed to existing channel for path: {} (receivers: {})", 
                  path, state.tx.receiver_count());
            (rx, last_update)
        } else {
            // Create new channel with keepalive
            let (tx, keepalive_rx) = broadcast::channel(100);
            let state = ChannelState {
                tx: tx.clone(),
                last_update: Arc::new(Mutex::new(None)),
                _keepalive: keepalive_rx,
            };
            channels.insert(path.to_string(), state);
            let rx = tx.subscribe();
            info!("[PagesManager] Created new channel for path: {}", path);
            (rx, None)
        }
    }

    /// Notify subscribers of a change
    pub async fn notify_update(
        &self,
        path: &str,
        version: Vec<braid_http::types::Version>,
        parents: Vec<braid_http::types::Version>,
        patches: Option<Vec<MergePatch>>,
        content: Option<String>,
    ) {
        let update = PagesUpdate {
            path: path.to_string(),
            version,
            parents,
            patches,
            content,
        };
        
        let channels = self.channels.read().await;
        
        if let Some(state) = channels.get(path) {
            // Store update for future subscribers
            *state.last_update.lock().await = Some(update.clone());
            
            // Broadcast to all current subscribers
            match state.tx.send(update) {
                Ok(count) => {
                    info!("[PagesManager] Broadcast sent to {} receivers for path: {}", count, path);
                }
                Err(e) => {
                    warn!("[PagesManager] Failed to broadcast update for path {}: {:?}", path, e);
                }
            }
        } else {
            // No channel exists yet - create one and store the update
            drop(channels); // Release read lock
            
            let mut channels = self.channels.write().await;
            let (tx, keepalive_rx) = broadcast::channel(100);
            let state = ChannelState {
                tx: tx.clone(),
                last_update: Arc::new(Mutex::new(Some(update.clone()))),
                _keepalive: keepalive_rx,
            };
            channels.insert(path.to_string(), state);
            info!("[PagesManager] Created channel with initial update for path: {}", path);
        }
    }
    
    /// Get subscriber count for a path (for debugging)
    pub async fn subscriber_count(&self, path: &str) -> usize {
        let channels = self.channels.read().await;
        channels.get(path).map(|state| state.tx.receiver_count()).unwrap_or(0)
    }
}
