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
#[derive(Debug, Default)]
pub struct ScanState {
    /// Last modification time for each tracked file.
    pub file_mtimes: HashMap<PathBuf, u128>,
    /// Whether a scan is currently running.
    pub running: bool,
    /// Number of watcher misses detected.
    pub watcher_misses: u32,
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
    // Check if already running
    {
        let mut s = state.write().await;
        if s.running {
            return Ok(Vec::new());
        }
        s.running = true;
    }

    let start_time = std::time::Instant::now();
    let mut changed_files = Vec::new();

    // Recursively scan directory
    let result = scan_directory(root_dir, root_dir, state, sync_urls, &mut changed_files).await;

    // Mark as done
    {
        let mut s = state.write().await;
        s.running = false;
    }

    let elapsed = start_time.elapsed();
    tracing::trace!(
        "scan_files completed in {:?}, found {} changes",
        elapsed,
        changed_files.len()
    );

    if let Err(e) = result {
        tracing::error!("Error during scan: {}", e);
    }

    Ok(changed_files)
}

/// Recursively scan a directory.
async fn scan_directory(
    dir: &Path,
    root: &Path,
    state: &Arc<RwLock<ScanState>>,
    sync_urls: &HashMap<String, bool>,
    changed: &mut Vec<PathBuf>,
) -> Result<()> {
    let mut entries = tokio::fs::read_dir(dir)
        .await
        .map_err(|e| BraidError::Io(e))?;

    while let Some(entry) = entries.next_entry().await.map_err(|e| BraidError::Io(e))? {
        let path = entry.path();
        let rel_path = path.strip_prefix(root).unwrap_or(&path);
        let rel_str = rel_path.to_string_lossy();

        // Skip ignored files
        if skip_file(&rel_str) {
            continue;
        }

        let metadata = entry.metadata().await.map_err(|e| BraidError::Io(e))?;

        if metadata.is_dir() {
            // Recurse into subdirectories
            Box::pin(scan_directory(&path, root, state, sync_urls, changed)).await?;
        } else if metadata.is_file() {
            // Check if this file is being synced
        } else if metadata.is_file() {
            // Check if this file is being synced
            if let Ok(url) = mapping::path_to_url(&path) {
                if !sync_urls.get(&url).copied().unwrap_or(false) {
                    continue;
                }

                // Check modification time
                let mtime = metadata
                    .modified()
                    .unwrap_or(SystemTime::UNIX_EPOCH)
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos();

                let needs_sync = {
                    let s = state.read().await;
                    match s.file_mtimes.get(&path) {
                        Some(&last_mtime) => mtime != last_mtime,
                        None => true, // New file
                    }
                };

                if needs_sync {
                    changed.push(path.clone());

                    // Update mtime
                    let mut s = state.write().await;
                    s.file_mtimes.insert(path, mtime);
                }
            }
        }
    }

    Ok(())
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
pub async fn start_scan_loop(
    state: Arc<RwLock<ScanState>>,
    sync_urls: Arc<RwLock<HashMap<String, bool>>>,
    scan_interval: Duration,
    on_change: impl Fn(PathBuf) + Send + Sync + 'static,
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
                    on_change(path);
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
        s.watcher_misses += 1;
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
