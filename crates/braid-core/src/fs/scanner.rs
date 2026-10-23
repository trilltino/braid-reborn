//! File scanning module for BraidFS.
//!
//! Implements periodic directory scanning to catch any changes
//! missed by the filesystem watcher.

use crate::core::{BraidError, Result};
use crate::fs::config::{get_root_dir, skip_file};
use crate::fs::mapping;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;

/// State for file scanning.
#[derive(Debug)]
pub struct ScanState {
    /// Last modification time for each tracked file.
    pub file_mtimes: HashMap<PathBuf, u128>,
    /// Whether a scan is currently running.
    scan_gate: Arc<tokio::sync::Semaphore>,
    /// Number of watcher misses detected.
    pub watcher_misses: u32,
}

impl Default for ScanState {
    fn default() -> Self {
        Self {
            file_mtimes: HashMap::new(),
            scan_gate: Arc::new(tokio::sync::Semaphore::new(1)),
            watcher_misses: 0,
        }
    }
}
impl ScanState {
    pub fn new() -> Self {
        Self::default()
    }
}

/// Scan the root directory for file changes.
///
/// Returns a list of files that have changed since the last scan.
pub async fn scan_files(
    root_dir: &Path,
    state: &Arc<RwLock<ScanState>>,
    sync_urls: &HashMap<String, bool>,
) -> Result<Vec<PathBuf>> {
    let gate = state.read().await.scan_gate.clone();
    let Ok(_permit) = gate.try_acquire() else {
        return Ok(Vec::new());
    };
    let previous = state.read().await.file_mtimes.clone();
    let mut observed = HashMap::new();
    let mut changed = Vec::new();
    let mut directories = vec![(root_dir.to_owned(), 0usize)];
    let mut entries_seen = 0usize;
    while let Some((directory, depth)) = directories.pop() {
        let mut entries = tokio::fs::read_dir(&directory).await?;
        while let Some(entry) = entries.next_entry().await? {
            entries_seen += 1;
            if entries_seen > 100_000 {
                return Err(BraidError::Fs(
                    "Directory scan exceeds 100000 entries".into(),
                ));
            }
            let path = entry.path();
            let relative = path
                .strip_prefix(root_dir)
                .map_err(|e| BraidError::Fs(e.to_string()))?;
            if skip_file(&relative.to_string_lossy()) {
                continue;
            }
            let kind = entry.file_type().await?;
            if kind.is_symlink() {
                continue;
            }
            if kind.is_dir() {
                if depth >= 32 || directories.len() >= 4096 {
                    return Err(BraidError::Fs(
                        "Directory scan depth/queue limit exceeded".into(),
                    ));
                }
                directories.push((path, depth + 1));
            } else if kind.is_file() {
                let Ok(url) = mapping::path_to_url(&path) else {
                    continue;
                };
                if !sync_urls.get(&url).copied().unwrap_or(false) {
                    continue;
                }
                let mtime = entry
                    .metadata()
                    .await?
                    .modified()?
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .map_err(|e| BraidError::Fs(e.to_string()))?
                    .as_nanos();
                if previous.get(&path) != Some(&mtime) {
                    changed.push(path.clone());
                }
                observed.insert(path, mtime);
            }
        }
    }
    // Publish observations together, only after the complete scan succeeds.
    // Cancellation releases the permit automatically and never wedges future scans.
    state.write().await.file_mtimes = observed;
    Ok(changed)
}

/// Normalize a URL by removing trailing /index patterns.
///
/// Matches JS `normalize_url()` from braidfs/index.js.
pub fn normalize_url(url: &str) -> String {
    let mut normalized = url.to_string();

    // Remove trailing /index/index/... patterns
    while normalized.ends_with("/index") {
        normalized = normalized[..normalized.len() - 6].to_string();
    }

    // Remove trailing slash
    while normalized.ends_with('/') {
        normalized.pop();
    }

    normalized
}

/// Check if a URL is well-formed and absolute.
pub fn is_well_formed_absolute_url(url: &str) -> bool {
    url::Url::parse(url).is_ok()
}

/// Start the periodic file scanning loop.
pub async fn start_scan_loop<F: std::future::Future<Output = ()> + Send>(
    state: Arc<RwLock<ScanState>>,
    sync_urls: Arc<RwLock<HashMap<String, bool>>>,
    scan_interval: Duration,
    on_change: impl Fn(PathBuf) -> F + Send + Sync + 'static,
) {
    let on_change = Arc::new(on_change);

    loop {
        tokio::time::sleep(scan_interval).await;

        let root_dir = match get_root_dir() {
            Ok(dir) => dir,
            Err(e) => {
                tracing::error!("Failed to get root dir: {}", e);
                continue;
            }
        };

        let urls = sync_urls.read().await.clone();
        match scan_files(&root_dir, &state, &urls).await {
            Ok(changed) => {
                for path in changed {
                    on_change(path).await;
                }
            }
            Err(e) => {
                tracing::error!("Scan error: {}", e);
            }
        }
    }
}

/// Called when the file watcher misses an event.
pub async fn on_watcher_miss(state: &Arc<RwLock<ScanState>>, message: &str, trigger_scan: bool) {
    {
        let mut s = state.write().await;
        s.watcher_misses = s.watcher_misses.saturating_add(1);
        tracing::warn!("watcher miss: {} [total: {}]", message, s.watcher_misses);
    }

    if trigger_scan {
        // Trigger a scan shortly
        tracing::info!("Triggering scan due to watcher miss");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_url() {
        assert_eq!(
            normalize_url("http://example.com/path/index"),
            "http://example.com/path"
        );
        assert_eq!(
            normalize_url("http://example.com/index/index"),
            "http://example.com"
        );
        assert_eq!(
            normalize_url("http://example.com/path/"),
            "http://example.com/path"
        );
        assert_eq!(normalize_url("http://example.com"), "http://example.com");
    }

    #[test]
    fn test_is_well_formed_absolute_url() {
        assert!(is_well_formed_absolute_url("http://example.com"));
        assert!(is_well_formed_absolute_url("https://braid.org/path"));
        assert!(!is_well_formed_absolute_url("not-a-url"));
        assert!(!is_well_formed_absolute_url("relative/path"));
    }
}
