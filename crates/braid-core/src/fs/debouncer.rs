use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;
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
    task: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for DebouncedSyncManager {
    fn drop(&mut self) {
        self.shutdown();
    }
}
impl DebouncedSyncManager {
    pub fn shutdown(&self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
    /// Create a placeholder manager (used for circular initialization)
    pub fn new_placeholder() -> Self {
        let (tx, _) = mpsc::channel(1);
        Self { tx, task: None }
    }

    /// Create a new manager and spawn its processing loop.
    pub fn new(state: DaemonState, debounce_ms: u64) -> Arc<Self> {
        let (tx, rx) = mpsc::channel(100);
        let task = tokio::spawn(async move {
            Self::process_loop(rx, state, Duration::from_millis(debounce_ms)).await;
        });
        Arc::new(Self {
            tx,
            task: Some(task),
        })
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
        let mut pending: HashMap<String, (PathBuf, Instant)> = HashMap::new();
        let mut active = std::collections::HashSet::new();
        let mut work = tokio::task::JoinSet::new();
        let mut tick = time::interval(Duration::from_millis(10));
        loop {
            tokio::select! {
                request = rx.recv(), if pending.len() < 128 => match request {
                    Some(request) => { pending.insert(request.url,(request.path,Instant::now()+debounce_duration)); },
                    None => return,
                },
                Some(done) = work.join_next(), if !work.is_empty() => match done {
                    Ok(url) => { active.remove(&url); },
                    Err(error) => { error!(%error,"Sync worker failed"); return; },
                },
                _ = tick.tick() => {},
            }
            let ready: Vec<String> = pending
                .iter()
                .filter(|(url, (_, deadline))| {
                    *deadline <= Instant::now() && !active.contains(*url)
                })
                .take(8usize.saturating_sub(work.len()))
                .map(|(url, _)| url.clone())
                .collect();
            for url in ready {
                if let Some((path, _)) = pending.remove(&url) {
                    active.insert(url.clone());
                    let state = state.clone();
                    work.spawn(async move {
                        if let Err(error) = Self::perform_sync(&path, &url, state).await {
                            error!(%error,%url,"Local edit retained after synchronization failed");
                        }
                        url
                    });
                }
            }
        }
    }

    async fn perform_sync(
        path: &PathBuf,
        url: &str,
        state: DaemonState,
    ) -> crate::core::Result<()> {
        if super::operations::blocks(url, &state).await? {
            return Err(crate::core::BraidError::Fs(
                "NFS operation owns this pathname; local bytes retained".into(),
            ));
        }
        let session = state.text_sessions.lock().await.get(url).cloned();
        if let Some(session) = session {
            super::file_service::reconcile(url, &state, &session, true, None).await?;
            return super::sync::wait_for_ack(&session).await;
        }
        if let Ok(metadata) = tokio::fs::metadata(path).await {
            if metadata.len() > 8 * 1024 * 1024 {
                return Err(crate::core::BraidError::Fs(
                    "Text edit exceeds the 8 MiB channel limit".into(),
                ));
            }
        }
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
