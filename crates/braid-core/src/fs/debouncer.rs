use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tokio::time::{self, Duration, Instant};
use tracing::{error, info};

use crate::fs::state::DaemonState;
use crate::fs::sync::sync_local_to_remote;

/// A request to sync a specific URL from a specific local path.
#[derive(Debug, Clone)]
struct DebounceRequest {
    url: String,
    path: PathBuf,
}

/// Manages debouncing of sync requests to prevent network flooding
/// while maintaining high responsiveness for "sync-as-you-type".
pub struct DebouncedSyncManager {
    tx: mpsc::Sender<DebounceRequest>,
}

impl DebouncedSyncManager {
    /// Create a placeholder manager (used for circular initialization)
    pub fn new_placeholder() -> Self {
        let (tx, _) = mpsc::channel(1);
        Self { tx }
    }

    /// Create a new manager and spawn its processing loop.
    pub fn new(state: DaemonState, debounce_ms: u64) -> Arc<Self> {
        let (tx, rx) = mpsc::channel(100);
        let manager = Arc::new(Self { tx });

        // Spawn the background processing task
        let state_clone = state.clone();
        tokio::spawn(async move {
            Self::process_loop(rx, state_clone, Duration::from_millis(debounce_ms)).await;
        });

        manager
    }

    /// Request a sync for a given URL and path.
    pub async fn request_sync(&self, url: String, path: PathBuf) {
        if let Err(e) = self.tx.send(DebounceRequest { url, path }).await {
            error!("[Debouncer] Failed to send sync request: {}", e);
        }
    }

    async fn process_loop(
        mut rx: mpsc::Receiver<DebounceRequest>,
        state: DaemonState,
        debounce_duration: Duration,
    ) {
        // Track the latest path and the next scheduled sync time for each URL
        let pending: Arc<RwLock<HashMap<String, (PathBuf, Instant)>>> =
            Arc::new(RwLock::new(HashMap::new()));

        let pending_clone = pending.clone();
        let state_sync = state.clone();

        // 1. Task to handle incoming requests and update deadlines
        tokio::spawn(async move {
            while let Some(req) = rx.recv().await {
                let now = Instant::now();
                let mut p = pending_clone.write().await;

                // Live sync: minimal debounce for real-time collaboration
                let deadline = if !p.contains_key(&req.url) {
                    now + Duration::from_millis(10)  // First keystroke: 10ms
                } else {
                    now + debounce_duration  // Subsequent: from config (default 10ms)
                };

                info!(
                    "[Debouncer] Received request for {}. Setting deadline in {:?}",
                    req.url,
                    deadline.duration_since(now)
                );
                p.insert(req.url, (req.path, deadline));
            }
        });

        // 2. Monitoring loop to trigger syncs when deadlines expire
        loop {
            time::sleep(Duration::from_millis(50)).await;

            let mut to_sync = Vec::new();
            {
                let mut p = pending.write().await;
                let now = Instant::now();

                p.retain(|url, (path, deadline)| {
                    if now >= *deadline {
                        to_sync.push((url.clone(), path.clone()));
                        false
                    } else {
                        true
                    }
                });
            }

            for (url, path) in to_sync {
                let state_inner = state_sync.clone();
                info!("[Debouncer] Deadline expired for {}. Triggering sync.", url);
                tokio::spawn(async move {
                    if let Err(e) = Self::perform_sync(&path, &url, state_inner).await {
                        error!("[Debouncer] Sync failed for {}: {}", url, e);
                    }
                });
            }
        }
    }

    async fn perform_sync(
        path: &PathBuf,
        url: &str,
        state: DaemonState,
    ) -> crate::core::Result<()> {
        let mut attempts = 0;
        let mut content = None;
        while attempts < 3 {
            match tokio::fs::read_to_string(path).await {
                Ok(c) => {
                    content = Some(c);
                    break;
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    info!("[Debouncer] File removed, propagating deletion for {}", url);
                    content = Some(String::new());
                    break;
                }
                Err(e)
                    if e.kind() == std::io::ErrorKind::PermissionDenied
                        || e.raw_os_error() == Some(32) =>
                {
                    attempts += 1;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(e) => {
                    return Err(crate::core::BraidError::Io(e));
                }
            }
        }

        let content = content.ok_or_else(|| {
            crate::core::BraidError::Fs(format!("Failed to read file after retries: {:?}", path))
        })?;

        let parents = {
            let store = state.version_store.read().await;
            store
                .get(url)
                .map(|v| v.current_version.clone())
                .unwrap_or_default()
        };

        let original_content = {
            let cache = state.content_cache.read().await;
            cache.get(url).cloned()
        };

        sync_local_to_remote(path, url, &parents, original_content, content, None, state).await
    }
}
