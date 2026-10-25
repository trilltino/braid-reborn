//! # BraidFS Core Logic
//!
//! This module implements the synchronization daemon.

use crate::core::{BraidClient, Result};
use crate::fs::api::run_server;
use crate::fs::binary_sync::BinarySyncManager;
use crate::fs::config::Config;
use crate::fs::rate_limiter::ReconnectRateLimiter;
use crate::fs::scanner::{start_scan_loop, ScanState};
use crate::fs::versions::VersionStore;
use notify::{Event, RecursiveMode, Watcher};
use parking_lot::Mutex;
use rusqlite::Connection;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

pub mod api;
pub mod binary_sync;
pub mod blob_handlers;
pub mod config;
pub mod debouncer;
pub mod diff;
pub(crate) mod file_service;
pub mod local_server;
pub mod mapping;
#[cfg(feature = "nfs")]
pub mod mount;
#[cfg(feature = "nfs")]
pub mod nfs;
pub mod operations;
pub mod rate_limiter;
pub mod scanner;
pub mod server_handlers;
pub mod state;
pub mod subscription;
pub mod sync;
pub mod versions;
pub mod watcher;

use state::{Command, DaemonState};
use subscription::spawn_subscription;
use watcher::handle_fs_event;

lazy_static::lazy_static! {
    pub static ref PEER_ID: Arc<RwLock<String>> = Arc::new(RwLock::new(String::new()));
}

#[derive(Clone)]
pub struct PendingWrites {
    // Map path -> Expiration Time (when we stop ignoring it)
    paths: Arc<Mutex<HashMap<String, std::time::Instant>>>,
}

impl PendingWrites {
    pub fn new() -> Self {
        Self {
            paths: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn normalize(path: &std::path::Path) -> String {
        path.to_string_lossy().to_lowercase().replace('\\', "/")
    }

    pub fn add(&self, path: PathBuf) {
        // Ignore events for this path for 100ms
        let expiry = std::time::Instant::now() + Duration::from_millis(100);
        self.paths.lock().insert(Self::normalize(&path), expiry);
    }

    pub fn remove(&self, path: &std::path::Path) {
        self.paths.lock().remove(&Self::normalize(path));
    }

    pub fn should_ignore(&self, path: &std::path::Path) -> bool {
        let mut paths = self.paths.lock();
        let key = Self::normalize(path);

        if let Some(&expiry) = paths.get(&key) {
            if std::time::Instant::now() < expiry {
                return true; // Still within ignore window
            } else {
                paths.remove(&key); // Expired, cleanup
                return false;
            }
        }
        false
    }
}

impl Default for PendingWrites {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
pub struct ActivityTracker {
    // Map URL -> Last Activity Time
    activity: Arc<Mutex<HashMap<String, std::time::Instant>>>,
}

impl ActivityTracker {
    pub fn new() -> Self {
        Self {
            activity: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn mark(&self, url: &str) {
        let mut activity = self.activity.lock();
        activity.insert(url.to_string(), std::time::Instant::now());
    }

    pub fn is_active(&self, url: &str) -> bool {
        // Log is active if there was activity in the last 10 minutes
        let activity = self.activity.lock();
        if let Some(&last_time) = activity.get(url) {
            std::time::Instant::now().duration_since(last_time) < Duration::from_secs(600)
        } else {
            false
        }
    }
}

impl Default for ActivityTracker {
    fn default() -> Self {
        Self::new()
    }
}

pub async fn run_daemon(port: u16) -> Result<()> {
    let _owner =
        braid_http::durable::writer_lease(&config::get_root_dir()?.join(".braidfs/daemon.lock"))
            .await?;
    let mut background = tokio::task::JoinSet::new();
    let mut config = Config::load().await?;
    config.port = port;

    config.save().await?;
    let config = Arc::new(RwLock::new(config));

    // Set global PEER_ID from config
    {
        let cfg = config.read().await;
        let mut id = PEER_ID.write().await;
        *id = cfg.peer_id.clone();
    }

    let content_cache = Arc::new(RwLock::new(std::collections::HashMap::new()));

    // Initialize Merge Registry
    let mut merge_registry = crate::core::merge::MergeTypeRegistry::new();
    // Antimatter removed.
    // Simpleton (braid-text) is the primary merge type for text documents
    merge_registry.register("simpleton", |id| {
        Box::new(crate::core::merge::simpleton::SimpletonMergeType::new(id))
    });
    merge_registry.register("braid-text", |id| {
        Box::new(crate::core::merge::simpleton::SimpletonMergeType::new(id))
    });
    let merge_registry = Arc::new(merge_registry);
    let active_merges = Arc::new(RwLock::new(HashMap::new()));

    let version_store = VersionStore::load().await?;
    let version_store = Arc::new(RwLock::new(version_store));

    let root_dir =
        config::get_root_dir().map_err(|e| crate::core::BraidError::Fs(e.to_string()))?;
    tokio::fs::create_dir_all(&root_dir)
        .await
        .map_err(crate::core::BraidError::Io)?;

    tracing::info!("BraidFS root: {:?}", root_dir);

    let pending_writes = PendingWrites::new();
    let activity_tracker = ActivityTracker::new();

    // Setup file watcher
    let (tx_fs, mut rx_fs) = tokio::sync::mpsc::channel(100);
    let tx_fs_watcher = tx_fs.clone();
    let mut watcher = notify::recommended_watcher(move |res: notify::Result<Event>| match res {
        Ok(event) => {
            let _ = tx_fs_watcher.blocking_send(event);
        }
        Err(e) => tracing::error!("Watch error: {:?}", e),
    })?;

    watcher.watch(&root_dir, RecursiveMode::Recursive)?;

    let (tx_cmd, rx_cmd) = async_channel::unbounded::<Command>();
    let rate_limiter = Arc::new(ReconnectRateLimiter::new(100));
    let scan_state = Arc::new(RwLock::new(ScanState::new()));

    // Initialize BlobStore
    let braidfs_dir = root_dir.join(".braidfs");
    let blob_store = Arc::new(
        crate::blob::BlobStore::new(braidfs_dir.join("blobs"), braidfs_dir.join("meta.sqlite"))
            .await
            .map_err(|e| crate::core::BraidError::Anyhow(e.to_string()))?,
    );

    // Initialize Inode DB
    let inode_db_path = braidfs_dir.join("inodes.sqlite");
    let inode_conn = Connection::open(&inode_db_path)
        .map_err(|e| crate::core::BraidError::Fs(format!("Failed to open inode DB: {}", e)))?;
    inode_conn
        .execute(
            "CREATE TABLE IF NOT EXISTS inodes (
            id INTEGER PRIMARY KEY,
            path TEXT UNIQUE NOT NULL
        )",
            [],
        )
        .map_err(|e| {
            crate::core::BraidError::Fs(format!("Failed to create inodes table: {}", e))
        })?;
    let inode_db = Arc::new(parking_lot::Mutex::new(inode_conn));

    let binary_sync_manager = BinarySyncManager::new(rate_limiter.clone(), blob_store.clone())
        .map_err(|e| crate::core::BraidError::Anyhow(e.to_string()))?;
    let binary_sync_manager = Arc::new(binary_sync_manager);

    // Track recently failed syncs to avoid log spam
    let failed_syncs = Arc::new(RwLock::new(HashMap::new()));

    // Track synced URLs for scanner
    let sync_urls_map = Arc::new(RwLock::new({
        let cfg = config.read().await;
        cfg.sync
            .iter()
            .map(|(u, e)| (u.clone(), *e))
            .collect::<HashMap<String, bool>>()
    }));

    // Start scan loop
    let scan_state_clone = scan_state.clone();
    let sync_urls_clone = sync_urls_map.clone();
    let tx_fs_clone = tx_fs.clone();
    background.spawn(async move {
        start_scan_loop(
            scan_state_clone,
            sync_urls_clone,
            Duration::from_secs(10), // Scan every 10s for more responsiveness during testing
            move |path| {
                let tx_fs_clone = tx_fs_clone.clone();
                async move {
                    tracing::info!("Scanner detected change in {:?}, triggering sync", path);
                    // Send a fake event to the FS watcher channel to trigger standard sync logic
                    let mut event = notify::Event::new(notify::EventKind::Modify(
                        notify::event::ModifyKind::Any,
                    ));
                    event.paths.push(path);
                    let _ = tx_fs_clone.send(event).await;
                }
            },
        )
        .await;
    });

    let braid_client = BraidClient::new()?;

    let state = DaemonState {
        text_sessions: Default::default(),
        config,
        content_cache: content_cache.clone(), // Fix for later access
        version_store: version_store.clone(),
        tracker: activity_tracker,
        merge_registry,
        active_merges,
        pending: pending_writes,
        client: braid_client,
        failed_syncs,
        binary_sync: binary_sync_manager,
        inode_db,
        tx_cmd: tx_cmd.clone(),
        debouncer: Arc::new(debouncer::DebouncedSyncManager::new_placeholder()), // Placeholder to fix circularity
        local_server_managed: Arc::new(RwLock::new(std::collections::HashSet::new())),
    };

    // Initialize the real debouncer with the state
    let debouncer = debouncer::DebouncedSyncManager::new(state.clone(), 100);

    // Update state with the real debouncer
    let mut state = state;
    state.debouncer = debouncer;

    background.spawn(operations::run(state.clone()));

    // Replay durable requests after restart, including edits whose ACK was lost.
    let replay_state = state.clone();
    background.spawn(async move {
        loop {
            let pending = replay_state.version_store.read().await.outbox.clone();
            for (url, edit) in pending {
                let enabled = replay_state
                    .config
                    .read()
                    .await
                    .sync
                    .get(&url)
                    .copied()
                    .unwrap_or(false);
                if !enabled {
                    continue;
                }
                if let Ok(path) = mapping::url_to_path(&url) {
                    let current = match super::fs::file_service::read_text(&path).await {
                        Ok(Some(text)) => text,
                        Ok(None) => edit.content.clone(), // Replay the immutable request even after unlink.
                        Err(error) => { tracing::error!(%url,%error,"Cannot read durable edit; retained for repair"); continue; }
                    };
                    if let Err(e) = sync::sync_local_to_remote(
                        &path,
                        &url,
                        &edit.parents,
                        Some(edit.base),
                        current,
                        Some(edit.content_type),
                        replay_state.clone(),
                    )
                    .await
                    {
                        tracing::warn!(%url,%e,"Durable edit retained for retry");
                    }
                }
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    });

    let state_server = state.clone();
    background.spawn(async move {
        if let Err(e) = run_server(port, state_server).await {
            tracing::error!("API Server crashed: {}", e);
        }
    });

    // ---------------------------------------------------------
    // Interactive Console (for Token/Cookie Entry)
    // ---------------------------------------------------------
    let state_console = state.clone();
    background.spawn(async move {
        use tokio::io::{self, AsyncBufReadExt, BufReader};
        let mut reader = BufReader::new(io::stdin()).lines();

        println!("\n[BraidFS CONSOLE] Ready for commands.");
        println!("Available: token <domain> <value>  (e.g. token braid.org ud8zp...)");
        println!("           sync <url>               (e.g. sync https://braid.org/tino)");

        while let Ok(Some(line)) = reader.next_line().await {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.is_empty() {
                continue;
            }

            match parts[0] {
                "token" | "cookie" if parts.len() >= 3 => {
                    let domain = parts[1].to_string();
                    let value = parts[2].to_string();
                    let _ = state_console
                        .tx_cmd
                        .send(Command::SetCookie { domain, value })
                        .await;
                    println!("[BraidFS] Cookie update queued for {}", parts[1]);
                }
                "sync" if parts.len() >= 2 => {
                    let url_str = parts[1].to_string();
                    if braid_http::client::validate_resource_url(&url_str).is_err() {
                        println!("[BraidFS] Invalid resource URL or embedded credentials");
                        continue;
                    }
                    if let Ok(u) = url::Url::parse(&url_str) {
                        if let Some(domain) = u.domain() {
                            let cfg = state_console.config.read().await;
                            let scoped = braid_http::client::http_origin(&url_str)
                                .is_ok_and(|origin| cfg.origin_credentials.contains_key(&origin));
                            if !scoped
                                && !cfg.cookies.contains_key(domain)
                                && (domain == "braid.org" || domain.ends_with(".braid.org"))
                            {
                                println!(
                                    "[BraidFS] ⚠️ Missing cookie for {}. Write access will fail.",
                                    domain
                                );
                                println!(
                                    "[BraidFS] Please set it first: token {} client=<your-cookie>",
                                    domain
                                );
                            }
                        }
                    }
                    let _ = state_console
                        .tx_cmd
                        .send(Command::Sync {
                            url: url_str.clone(),
                        })
                        .await;
                    println!("[BraidFS] Sync triggered for {}", url_str);
                }
                "help" => {
                    println!("Commands: token <domain> <value>, sync <url>");
                }
                _ => {
                    println!("[BraidFS] Unknown command. Try 'token' or 'sync'.");
                }
            }
        }
    });

    let mut nfs_handle: Option<tokio::task::JoinHandle<()>> = None;

    let mut subscriptions: HashMap<String, tokio::task::JoinHandle<()>> = HashMap::new();
    let mut resource_refresh = tokio::time::interval(Duration::from_secs(5));

    let enabled_urls: Vec<String> = state
        .config
        .read()
        .await
        .sync
        .iter()
        .filter(|(_, enabled)| **enabled)
        .map(|(url, _)| url.clone())
        .collect();
    for url in enabled_urls {
        spawn_subscription(url, &mut subscriptions, state.clone()).await;
    }

    // Start local HTTP 209 server for IDE subscriptions
    // Note: Polling only happens when there are active subscribers
    {
        let state_clone = state.clone();
        background.spawn(async move {
            let server = local_server::LocalBraidServer::new(state_clone, 5);
            if let Err(e) = server.start(45679).await {
                tracing::error!("[BraidFS] Local server error: {}", e);
            }
        });
        tracing::info!("[BraidFS] HTTP 209 server on port 45679 (active when IDE connected)");
    }

    #[cfg(feature = "nfs")]
    let mut active_mount_point: Option<String> = None;

    // Main Event Loop
    loop {
        tokio::select! {
            _ = resource_refresh.tick() => {
                let enabled = state.config.read().await.sync.clone();
                *sync_urls_map.write().await = enabled.clone();
                for (url, enabled) in enabled {
                    if enabled && !subscriptions.contains_key(&url) {
                        spawn_subscription(url,&mut subscriptions,state.clone()).await;
                    }
                }
            }

            _ = tokio::signal::ctrl_c() => {
                tracing::info!("Shutdown signal received");
                #[cfg(feature = "nfs")]
                if let Some(mp) = active_mount_point.take() {
                    tracing::info!("Unmounting {}...", mp);
                    let _ = mount::unmount(std::path::Path::new(&mp));
                }
                if let Some(handle) = nfs_handle.take() {
                    handle.abort();
                }
                for (_,task) in subscriptions.drain() { task.abort(); }
                let sessions = std::mem::take(&mut *state.text_sessions.lock().await);
                for (_,session) in sessions { session.shutdown().await; }
                state.debouncer.shutdown();
                background.abort_all();
                while background.join_next().await.is_some() {}
                break;
            }
            Some(result) = background.join_next(), if !background.is_empty() => {
                if let Err(error) = result { tracing::error!(%error,"Daemon background task failed"); }
            }

            Some(event) = rx_fs.recv() => {
                handle_fs_event(event, state.clone()).await;
            }

            Ok(cmd) = rx_cmd.recv() => {
                match cmd {
                    Command::Sync { url } => {
                        if braid_http::client::validate_resource_url(&url).is_err() {tracing::error!("Rejected invalid sync URL or embedded credentials"); continue;}
                        tracing::info!("[DEBUG] === Command::Sync received for {}", url);
                        {
                            let mut cfg = state.config.write().await;
                            let mut candidate = cfg.clone();
                            candidate.sync.insert(url.clone(), true);
                            if let Err(error) = candidate.save().await {tracing::error!(%error,"Sync configuration was not saved"); continue;}
                            *cfg = candidate;
                        }
                        tracing::info!("[DEBUG] About to call spawn_subscription for {}", url);
                        spawn_subscription(url.clone(), &mut subscriptions, state.clone()).await;
                        tracing::info!("[DEBUG] spawn_subscription completed for {}", url);

                        if binary_sync::should_use_binary_sync(&url) {
                            let bsm = state.binary_sync.clone();
                            let url_clone = url.clone();
                            let root = config::get_root_dir()?;
                            let fullpath = root.join(url.trim_start_matches('/'));
                            background.spawn(async move {
                                let _ = bsm.init_binary_sync(&url_clone, &fullpath).await;
                            });
                        }
                        sync_urls_map.write().await.insert(url, true);
                    }
                    Command::Unsync { url } => {
                        if braid_http::client::validate_resource_url(&url).is_err() {tracing::error!("Rejected invalid sync URL or embedded credentials"); continue;}
                        tracing::info!("Disable Sync: {}", url);
                        {
                            let mut cfg = state.config.write().await;
                            let mut candidate = cfg.clone();
                            candidate.sync.remove(&url);
                            if let Err(error) = candidate.save().await {tracing::error!(%error,"Unsync configuration was not saved"); continue;}
                            *cfg = candidate;
                        }
                        if let Some(handle) = subscriptions.remove(&url) {
                            handle.abort();
                        }
                        let session = state.text_sessions.lock().await.remove(&url);
                        if let Some(session) = session { session.shutdown().await; }
                        sync_urls_map.write().await.remove(&url);
                    }
                    Command::SetCookie { domain, value } => {
                        let mut cfg = state.config.write().await;
                        let mut candidate = cfg.clone();
                        candidate.cookies.insert(domain, value);
                        if let Err(error) = candidate.save().await {tracing::error!(%error,"Cookie configuration was not saved"); continue;}
                        *cfg = candidate;
                        tracing::info!("Cookie configuration saved");
                    }
                    Command::SetOriginCredentials {origin, credentials, result} => {
                        let saved = async {
                            let mut config = state.config.write().await;
                            let mut candidate = config.clone();
                            candidate.origin_credentials.insert(origin.clone(), credentials);
                            candidate.save().await?;
                            *config = candidate;
                            Ok::<(), crate::core::BraidError>(())
                        }.await;
                        let restart = saved.is_ok();
                        let _ = result.send(saved).await;
                        if restart {
                            let urls: Vec<_> = state.config.read().await.sync.iter()
                                .filter(|(url, enabled)| **enabled && braid_http::client::http_origin(url).is_ok_and(|scope| scope == origin))
                                .map(|(url, _)| url.clone()).collect();
                            for url in urls {
                                if let Some(task) = subscriptions.remove(&url) {task.abort(); let _ = task.await;}
                                let session = state.text_sessions.lock().await.remove(&url);
                                if let Some(session) = session {session.shutdown().await;}
                                spawn_subscription(url, &mut subscriptions, state.clone()).await;
                            }
                        }
                    }
                    Command::SetIdentity { domain, email } => {
                        tracing::info!("Set Identity: {} for {}", email, domain);
                        let mut cfg = state.config.write().await;
                        let mut candidate = cfg.clone();
                        candidate.identities.insert(domain, email);
                        if let Err(error) = candidate.save().await {tracing::error!(%error,"Identity configuration was not saved"); continue;}
                        *cfg = candidate;
                    }
                    #[cfg(feature = "nfs")]
                    Command::Mount { port, mount_point } => {
                        if nfs_handle.is_some() {
                            tracing::warn!("NFS Server already running");
                        } else {
                            let state_nfs = state.clone();
                            let handle = tokio::spawn(async move {
                                let backend = match nfs::BraidNfsBackend::new(state_nfs.clone(), state_nfs.binary_sync.blob_store()) {
                                    Ok(backend) => backend,
                                    Err(error) => { tracing::error!(%error, "NFS inode recovery failed"); return; }
                                };
                                tracing::info!("Starting NFS server on port {}", port);
                                 match nfsserve::tcp::NFSTcpListener::bind(&format!("127.0.0.1:{}", port), backend).await {
                                     Ok(listener) => {
                                         use nfsserve::tcp::NFSTcp;
                                         if let Err(e) = listener.handle_forever().await {
                                             tracing::error!("NFS Server error: {}", e);
                                         }
                                     }
                                     Err(e) => {
                                         tracing::error!("Failed to bind NFS server to port {}: {}", port, e);
                                     }
                                 }
                            });
                            nfs_handle = Some(handle);

                            // Trigger OS-level mount if requested
                            if let Some(mp) = mount_point {
                                tracing::info!("Triggering OS mount to {}...", mp);
                                if let Err(e) = mount::mount(port, std::path::Path::new(&mp)) {
                                    tracing::error!("Failed to mount: {}", e);
                                } else {
                                    active_mount_point = Some(mp);
                                }
                            }
                        }
                    }
                    #[cfg(feature = "nfs")]
                    Command::Unmount => {
                        if let Some(mp) = active_mount_point.take() {
                             tracing::info!("Unmounting {}...", mp);
                             let _ = mount::unmount(std::path::Path::new(&mp));
                        }
                        if let Some(handle) = nfs_handle.take() {
                            tracing::info!("Stopping NFS server");
                            handle.abort();
                        }
                    }
                }
            }
        }
    }
    Ok(())
}
