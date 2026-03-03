//! Local.org Manager for the Website Service
//!
//! Manages local file-based wiki storage at `braid_data/local.org/`.
//! Provides Braid-compatible GET/PUT with live subscriptions.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::fs;
use tokio::sync::broadcast;
use tokio::sync::RwLock;
use tracing::{info, warn};

/// A patch representing a text change (simpleton-compatible)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TextPatch {
    pub range: (usize, usize), // [start, end] character positions
    pub content: String,       // Replacement content
}

/// Update event for local.org subscriptions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalOrgUpdate {
    pub path: String,
    pub version: u64,
    pub parent_version: u64,
    pub patches: Vec<TextPatch>,
    pub full_content: Option<String>, // Sent on initial subscribe
}

/// Metadata for a local.org page
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalPageInfo {
    pub path: String,
    pub title: String,
    pub version: u64,
}

/// Per-page state tracking
struct PageState {
    version: AtomicU64,
    tx: broadcast::Sender<LocalOrgUpdate>,
}

pub struct LocalOrgManager {
    storage_dir: PathBuf,
    pages: RwLock<HashMap<String, PageState>>,
}

impl LocalOrgManager {
    pub fn new(braid_root: &str) -> Self {
        let storage_dir = PathBuf::from(braid_root).join("local.org");
        info!("[LocalOrgManager] Initialized at {:?}", storage_dir);
        Self {
            storage_dir,
            pages: RwLock::new(HashMap::new()),
        }
    }

    /// Ensure storage directory exists
    pub async fn ensure_dir(&self) -> anyhow::Result<()> {
        fs::create_dir_all(&self.storage_dir).await?;
        Ok(())
    }

    /// Get the file path for a page
    fn page_path(&self, name: &str) -> PathBuf {
        let name = name.trim_start_matches('/');
        let name = if name.ends_with(".md") {
            name.to_string()
        } else {
            format!("{}.md", name)
        };
        self.storage_dir.join(name)
    }

    /// List all pages in local.org
    pub async fn list_pages(&self) -> Vec<LocalPageInfo> {
        let mut pages = Vec::new();
        let mut entries = match fs::read_dir(&self.storage_dir).await {
            Ok(e) => e,
            Err(_) => return pages,
        };

        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            if path.extension().map_or(false, |ext| ext == "md") {
                let relative = path
                    .file_name()
                    .map(|s| s.to_string_lossy().to_string())
                    .unwrap_or_default();
                let title = self.extract_title(&path).await;
                pages.push(LocalPageInfo {
                    path: relative,
                    title,
                    version: 0,
                });
            }
        }
        pages
    }

    /// Extract title from first H1 or filename
    async fn extract_title(&self, path: &PathBuf) -> String {
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

    /// Get page content and version
    pub async fn get_page(&self, name: &str) -> anyhow::Result<(String, u64)> {
        let path = self.page_path(name);
        let content = fs::read_to_string(&path).await?;

        // Get or create page state
        let pages = self.pages.read().await;
        let version = pages
            .get(name)
            .map(|s| s.version.load(Ordering::SeqCst))
            .unwrap_or(0);
        
        Ok((content, version))
    }

    /// Create a new page
    pub async fn create_page(&self, name: &str, content: &str) -> anyhow::Result<u64> {
        self.ensure_dir().await?;
        let path = self.page_path(name);
        
        if path.exists() {
            anyhow::bail!("Page already exists: {}", name);
        }

        fs::write(&path, content).await?;
        info!("[LocalOrgManager] Created page: {}", name);

        // Initialize page state
        let (tx, _) = broadcast::channel(100);
        let state = PageState {
            version: AtomicU64::new(1),
            tx,
        };
        self.pages.write().await.insert(name.to_string(), state);

        Ok(1)
    }

    /// Apply patches and broadcast to subscribers
    pub async fn apply_patches(
        &self,
        name: &str,
        patches: Vec<TextPatch>,
        parent_version: u64,
    ) -> anyhow::Result<u64> {
        let path = self.page_path(name);
        
        // Read current content
        let mut content = fs::read_to_string(&path).await.unwrap_or_default();
        
        // Apply patches (simpleton-style: character range replacement)
        // Sort by range start descending so later patches don't invalidate earlier offsets
        let mut sorted_patches = patches.clone();
        sorted_patches.sort_by(|a, b| b.range.0.cmp(&a.range.0));
        
        for patch in &sorted_patches {
            let start = patch.range.0.min(content.len());
            let end = patch.range.1.min(content.len());
            content.replace_range(start..end, &patch.content);
        }
        
        // Write updated content
        fs::write(&path, &content).await?;
        
        // Update version and broadcast
        let mut pages = self.pages.write().await;
        let state = pages.entry(name.to_string()).or_insert_with(|| {
            let (tx, _) = broadcast::channel(100);
            PageState {
                version: AtomicU64::new(0),
                tx,
            }
        });
        
        let new_version = state.version.fetch_add(1, Ordering::SeqCst) + 1;
        
        let update = LocalOrgUpdate {
            path: name.to_string(),
            version: new_version,
            parent_version,
            patches,
            full_content: None,
        };
        
        let _ = state.tx.send(update);
        
        Ok(new_version)
    }

    /// Subscribe to a page's updates
    pub async fn subscribe(&self, name: &str) -> broadcast::Receiver<LocalOrgUpdate> {
        let mut pages = self.pages.write().await;
        let state = pages.entry(name.to_string()).or_insert_with(|| {
            let (tx, _) = broadcast::channel(100);
            PageState {
                version: AtomicU64::new(0),
                tx,
            }
        });
        state.tx.subscribe()
    }

    /// Get current version for a page
    pub async fn get_version(&self, name: &str) -> u64 {
        let pages = self.pages.read().await;
        pages
            .get(name)
            .map(|s| s.version.load(Ordering::SeqCst))
            .unwrap_or(0)
    }
}
