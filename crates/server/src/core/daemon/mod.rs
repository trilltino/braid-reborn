//! BraidFS Daemon Integration
//!
//! This module provides integration with the local braidfs-daemon for:
//! - File synchronization between server and daemon
//! - NFS mount management
//! - Real-time file watching
//! - Bidirectional sync protocol

use crate::core::models::{ChatRoom, RoomSyncStatus, SyncStatus};
use crate::core::store::json_store::JsonChatStore;
use anyhow::{Context, Result};
use notify::{Event, RecommendedWatcher, RecursiveMode, Watcher};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, warn};

/// Daemon integration manager
pub struct DaemonIntegration {
    config: crate::core::config::ChatServerConfig,
    store: Arc<JsonChatStore>,
    /// Daemon HTTP API client
    daemon_client: reqwest::Client,
    /// Daemon base URL
    daemon_url: String,
    /// File watcher for external changes
    watcher: Option<RecommendedWatcher>,
    /// Channel for file change events
    file_events: mpsc::Sender<FileChangeEvent>,
    /// Sync status for each room
    sync_status: Arc<RwLock<HashMap<String, RoomSyncStatus>>>,
}

#[derive(Debug, Clone)]
pub struct FileChangeEvent {
    pub room_id: String,
    pub path: PathBuf,
    pub change_type: ChangeType,
}

#[derive(Debug, Clone)]
pub enum ChangeType {
    Created,
    Modified,
    Deleted,
}

impl DaemonIntegration {
    /// Create new daemon integration
    pub async fn new(
        config: crate::core::config::ChatServerConfig,
        store: Arc<JsonChatStore>,
    ) -> Result<(Self, mpsc::Receiver<FileChangeEvent>)> {
        let daemon_url = format!("http://127.0.0.1:{}", config.daemon_port);
        let (tx, rx) = mpsc::channel(100);

        let integration = Self {
            config,
            store,
            daemon_client: reqwest::Client::new(),
            daemon_url,
            watcher: None,
            file_events: tx,
            sync_status: Arc::new(RwLock::new(HashMap::new())),
        };

        Ok((integration, rx))
    }

    /// Start the daemon integration service
    pub async fn start_service(mut self) -> Result<()> {
        info!("Starting Daemon Integration service...");

        // Check if daemon is available
        match self.check_daemon_health().await {
            Ok(()) => info!("BraidFS daemon is available at {}", self.daemon_url),
            Err(e) => {
                warn!(
                    "BraidFS daemon not available: {}. Will retry on sync operations.",
                    e
                );
            }
        }

        // Setup file watcher
        self.setup_file_watcher().await?;

        Ok(())
    }

    /// Check if daemon is healthy
    async fn check_daemon_health(&self) -> Result<()> {
        let resp = self
            .daemon_client
            .get(format!("{}/health", self.daemon_url))
            .send()
            .await
            .context("Failed to connect to daemon")?;

        if resp.status().is_success() {
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "Daemon health check failed: {}",
                resp.status()
            ))
        }
    }

    /// Setup file watcher for external changes
    async fn setup_file_watcher(&mut self) -> Result<()> {
        let tx = self.file_events.clone();

        let storage_dir = self.config.storage_dir.clone(); // Clone for closure
        let watcher = notify::recommended_watcher(move |res: Result<Event, notify::Error>| {
            match res {
                Ok(event) => {
                    for path in event.paths {
                        // Filter: Only watch user profile (root files) and AI folder

                        let Ok(relative) = path.strip_prefix(&storage_dir) else {
                            continue;
                        };

                        let is_relevant = if let Some(first_comp) = relative.components().next() {
                            let s = first_comp.as_os_str().to_string_lossy();
                            // Allow "ai" folder
                            if s == "ai" {
                                true
                            }
                            // Allow "local" folder (User Profile content)
                            else if s == "local" {
                                true
                            }
                            // Allow root files (metadata)
                            else if relative.components().count() == 1 {
                                true
                            } else {
                                false
                            }
                        } else {
                            false
                        };

                        if !is_relevant {
                            continue;
                        }

                        if let Some(ext) = path.extension() {
                            if ext == "json" || ext == "md" {
                                let change_type = match event.kind {
                                    notify::EventKind::Create(_) => ChangeType::Created,
                                    notify::EventKind::Modify(_) => ChangeType::Modified,
                                    notify::EventKind::Remove(_) => ChangeType::Deleted,
                                    _ => continue,
                                };

                                let room_id = path
                                    .file_stem()
                                    .and_then(|s| s.to_str())
                                    .unwrap_or("unknown")
                                    .to_string();

                                let _ = tx.try_send(FileChangeEvent {
                                    room_id,
                                    path,
                                    change_type,
                                });
                            }
                        }
                    }
                }
                Err(e) => error!("File watcher error: {}", e),
            }
        })?;

        self.watcher = Some(watcher);

        // Watch the storage directory
        if let Some(ref mut watcher) = self.watcher {
            watcher.watch(&self.config.storage_dir, RecursiveMode::Recursive)?;
            info!(
                "Watching {:?} for external changes",
                self.config.storage_dir
            );
        }

        Ok(())
    }

    /// Sync a room to the daemon
    pub async fn sync_room_to_daemon(&self, room_id: &str) -> Result<()> {
        // Update sync status
        {
            let mut status = self.sync_status.write().await;
            status.insert(
                room_id.to_string(),
                RoomSyncStatus {
                    room_id: room_id.to_string(),
                    status: SyncStatus::Syncing,
                    last_sync: None,
                    pending_changes: 0,
                },
            );
        }

        // Check daemon availability
        if let Err(e) = self.check_daemon_health().await {
            warn!("Cannot sync room {}: daemon unavailable - {}", room_id, e);
            self.update_sync_status(room_id, SyncStatus::Disconnected)
                .await;
            return Err(e);
        }

        // Get room data
        let room_path = self.config.storage_dir.join(format!("{}.json", room_id));
        if !room_path.exists() {
            return Err(anyhow::anyhow!("Room file not found: {:?}", room_path));
        }

        // Read room data
        let room_data = fs::read_to_string(&room_path).await?;

        // Construct daemon URL for this room
        let daemon_room_url = format!("{}/chat/{}", self.daemon_url, room_id);

        // Send to daemon via PUT request
        let resp = self
            .daemon_client
            .put(&daemon_room_url)
            .header("Content-Type", "application/json")
            .body(room_data)
            .send()
            .await
            .context("Failed to send room to daemon")?;

        if resp.status().is_success() {
            info!("Synced room {} to daemon", room_id);
            self.update_sync_status(room_id, SyncStatus::Connected)
                .await;
            Ok(())
        } else {
            let err = format!("Daemon returned error: {}", resp.status());
            error!("{}", err);
            self.update_sync_status(room_id, SyncStatus::Disconnected)
                .await;
            Err(anyhow::anyhow!(err))
        }
    }

    /// Import a room from the daemon
    pub async fn import_room_from_daemon(&self, room_id: &str) -> Result<ChatRoom> {
        let daemon_room_url = format!("{}/chat/{}", self.daemon_url, room_id);

        let resp = self
            .daemon_client
            .get(&daemon_room_url)
            .send()
            .await
            .context("Failed to fetch room from daemon")?;

        if !resp.status().is_success() {
            return Err(anyhow::anyhow!("Daemon returned: {}", resp.status()));
        }

        let room_data = resp.text().await?;
        let room: ChatRoom =
            serde_json::from_str(&room_data).context("Failed to parse room data from daemon")?;

        info!("Imported room {} from daemon", room_id);
        Ok(room)
    }

    /// Handle external file change
    pub async fn handle_file_change(&self, event: FileChangeEvent) -> Result<()> {
        debug!(
            "Handling file change for room {}: {:?}",
            event.room_id, event.change_type
        );

        match event.change_type {
            ChangeType::Modified | ChangeType::Created => {
                // Reload room from disk
                if let Some(room_lock) = self.store.get_room(&event.room_id).await? {
                    let _room = room_lock.read().await;

                    // Broadcast sync event
                    let update = crate::core::store::json_store::RoomUpdate {
                        room_id: event.room_id.clone(),
                        update_type: crate::core::store::json_store::UpdateType::Sync,
                        data: serde_json::json!({
                            "source": "daemon",
                            "action": "external_update"
                        }),
                        crdt_version: None,
                    };
                    self.store.broadcast(&event.room_id, update).await?;

                    info!("Reloaded room {} from external change", event.room_id);
                }
            }
            ChangeType::Deleted => {
                // Room was deleted externally
                warn!("Room {} was deleted externally", event.room_id);
            }
        }

        Ok(())
    }

    /// Request NFS mount for a room
    pub async fn request_nfs_mount(&self, room_id: &str, mount_point: &str) -> Result<()> {
        let nfs_url = format!("{}/api/nfs/mount", self.daemon_url);

        let payload = serde_json::json!({
            "room_id": room_id,
            "mount_point": mount_point,
        });

        let resp = self
            .daemon_client
            .post(&nfs_url)
            .json(&payload)
            .send()
            .await
            .context("Failed to request NFS mount")?;

        if resp.status().is_success() {
            info!(
                "NFS mount requested for room {} at {}",
                room_id, mount_point
            );
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "NFS mount request failed: {}",
                resp.status()
            ))
        }
    }

    /// Get sync status for a room
    pub async fn get_sync_status(&self, room_id: &str) -> RoomSyncStatus {
        self.sync_status
            .read()
            .await
            .get(room_id)
            .cloned()
            .unwrap_or_else(|| RoomSyncStatus {
                room_id: room_id.to_string(),
                status: SyncStatus::Offline,
                last_sync: None,
                pending_changes: 0,
            })
    }

    /// Update sync status
    async fn update_sync_status(&self, room_id: &str, status: SyncStatus) {
        let mut sync_status = self.sync_status.write().await;
        if let Some(entry) = sync_status.get_mut(room_id) {
            entry.status = status.clone();
            if matches!(status, SyncStatus::Connected) {
                entry.last_sync = Some(chrono::Utc::now());
            }
        }
    }
    /// Set a cookie for a domain
    pub async fn set_cookie(&self, domain: &str, value: &str) -> anyhow::Result<()> {
        let url = format!("{}/api/cookie", self.daemon_url);
        let resp = self
            .daemon_client
            .put(&url)
            .json(&serde_json::json!({ "domain": domain, "value": value }))
            .send()
            .await
            .context("Failed to set cookie")?;

        if resp.status().is_success() {
            info!("Set cookie for domain: {}", domain);
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "Daemon failed to set cookie: {}",
                resp.status()
            ))
        }
    }
}

/// Background task for handling file change events
pub async fn file_watcher_task(
    mut rx: mpsc::Receiver<FileChangeEvent>,
    _integration: Arc<DaemonIntegration>,
) {
    while let Some(event) = rx.recv().await {
        // Handle file change
        debug!("File change event: {:?}", event);
    }
}

/// Background task for periodic sync
pub async fn periodic_sync_task(_integration: Arc<DaemonIntegration>, interval_secs: u64) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(interval_secs));

    loop {
        interval.tick().await;
        info!("Periodic sync tick - would sync all rooms");
    }
}
