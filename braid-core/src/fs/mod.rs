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
use rusqlite::Connection;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::RwLock;

pub mod api;
pub mod binary_sync;
pub mod blob_handlers;
pub mod config;
pub mod debouncer;
pub mod diff;
pub mod local_server;
pub mod mapping;
#[cfg(feature = "nfs")]
pub mod mount;
#[cfg(feature = "nfs")]
pub mod nfs;
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
        self.paths
            .lock()
            .unwrap()
            .insert(Self::normalize(&path), expiry);
    }

    pub fn remove(&self, path: &PathBuf) {
        self.paths.lock().unwrap().remove(&Self::normalize(path));
    }

    pub fn should_ignore(&self, path: &PathBuf) -> bool {
        let mut paths = self.paths.lock().unwrap();
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
        let mut activity = self.activity.lock().unwrap();
        activity.insert(url.to_string(), std::time::Instant::now());
    }

    pub fn is_active(&self, url: &str) -> bool {
        // Log is active if there was activity in the last 10 minutes
        let activity = self.activity.lock().unwrap();
        if let Some(&last_time) = activity.get(url) {
            std::time::Instant::now().duration_since(last_time) < Duration::from_secs(600)
        } else {
            false
        }
    }
}

pub async fn run_daemon(port: u16) -> Result<()> {
    let mut config = Config::load().await?;
    config.port = port;

    // Filter out dead domains
    // Force remove known problematic URLs that might persist in config
    let bad_urls = vec![
        "https://braid.org/Braid",
        "https://braid.org/Main",
        "https://braid.org/Welcome",
        "https://braid.org/Protocol",
        "https://braid.org/wiki",
        "https://braid.org/about",
        "https://braid.org/editing",
    ];

    for url in bad_urls {
        if config.sync.contains_key(url) {
            tracing::warn!("[Config] Purging deprecated subscription: {}", url);
            config.sync.remove(url);
        }
    }

    // Filter out other dead/test domains
    config.sync.retain(|url, _| {
        !url.contains("mail.braid.org")
            && !url.contains("braid.org/tino")
            && !url.contains("braid.org/tino_test")
            && !url.contains("braid.org/xfmail")
            && !url.contains("braid.org/127_xfmail")
            && url != "https://braid.org/"
            && url != "https://braid.org"
    });

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

    // Cache Warming AND Metadata Stubbing
    {
        let cfg = config.read().await;
        let mut cache = content_cache.write().await;
        for (url, enabled) in &cfg.sync {
            if let Ok(path) = mapping::url_to_path(url) {
                if path.exists() {
                    // Cache warming for existing files
                    if *enabled {
                        if let Ok(content) = tokio::fs::read_to_string(&path).await {
                            tracing::info!("[BraidFS] Cache warming for {} from {:?}", url, path);
                            cache.insert(url.clone(), content);
                        }
                    }
                } else {
                    // Metadata Stubbing: Create empty file if it doesn't exist
                    tracing::info!("[Discovery] Creating stub for {}", url);
                    if let Some(parent) = path.parent() {
                        let _ = tokio::fs::create_dir_all(parent).await;
                    }
                    if let Err(e) = tokio::fs::write(&path, "").await {
                        tracing::error!("[Discovery] Failed to create stub for {}: {}", url, e);
                    }
                }
            }
        }
    }

    let version_store = VersionStore::load().await?;
    let version_store = Arc::new(RwLock::new(version_store));

    let root_dir =
        config::get_root_dir().map_err(|e| crate::core::BraidError::Fs(e.to_string()))?;
    tokio::fs::create_dir_all(&root_dir)
        .await
        .map_err(|e| crate::core::BraidError::Io(e))?;

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
    tokio::spawn(async move {
        start_scan_loop(
            scan_state_clone,
            sync_urls_clone,
            Duration::from_secs(10), // Scan every 10s for more responsiveness during testing
            move |path| {
                tracing::info!("Scanner detected change in {:?}, triggering sync", path);
                // Send a fake event to the FS watcher channel to trigger standard sync logic
                let mut event =
                    notify::Event::new(notify::EventKind::Modify(notify::event::ModifyKind::Any));
                event.paths.push(path);
                let _ = tx_fs_clone.blocking_send(event);
            },
        )
        .await;
    });

    let braid_client = BraidClient::new()?;

    let state = DaemonState {
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

    let state_server = state.clone();
    tokio::spawn(async move {
        if let Err(e) = run_server(port, state_server).await {
            tracing::error!("API Server crashed: {}", e);
        }
    });

    // ---------------------------------------------------------
    // Interactive Console (for Token/Cookie Entry)
    // ---------------------------------------------------------
    let state_console = state.clone();
    tokio::spawn(async move {
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
                    let _ = state_console.tx_cmd.send(Command::SetCookie { domain, value }).await;
                    println!("[BraidFS] Cookie updated for {}", parts[1]);
                }
                "sync" if parts.len() >= 2 => {
                    let url_str = parts[1].to_string();
                    if let Ok(u) = url::Url::parse(&url_str) {
                         if let Some(domain) = u.domain() {
                             let cfg = state_console.config.read().await;
                             if !cfg.cookies.contains_key(domain) && domain.contains("braid.org") {
                                 println!("[BraidFS] ⚠️ Missing cookie for {}. Write access will fail.", domain);
                                 println!("[BraidFS] Please set it first: token {} client=<your-cookie>", domain);
                             }
                         }
                    }
                    let _ = state_console.tx_cmd.send(Command::Sync { url: url_str.clone() }).await;
                    println!("[BraidFS] Sync triggered for {}", url_str);
                }
                "help" => {
                    println!("Commands: token <domain> <value>, sync <url>");
                }
                _ => {
                    println!(
                        "[BraidFS] Unknown command: {}. Try 'token' or 'sync'.",
                        parts[0]
                    );
                }
            }
        }
    });

    let mut nfs_handle: Option<tokio::task::JoinHandle<()>> = None;

    let mut subscriptions: HashMap<String, tokio::task::JoinHandle<()>> = HashMap::new();

    // Start subscriptions for all enabled sync URLs
    {
        let cfg = state.config.read().await;
        for (url, enabled) in &cfg.sync {
            if *enabled {
                tracing::info!("[BraidFS] Starting subscription for {}", url);
                spawn_subscription(url.clone(), &mut subscriptions, state.clone()).await;
            }
        }
    }

    // Start local HTTP 209 server for IDE subscriptions
    // Note: Polling only happens when there are active subscribers
    {
        let state_clone = state.clone();
        tokio::spawn(async move {
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
                break;
            }

            Some(event) = rx_fs.recv() => {
                handle_fs_event(event, state.clone()).await;
            }

            Ok(cmd) = rx_cmd.recv() => {
                match cmd {
                    Command::Sync { url } => {
                        tracing::info!("[DEBUG] === Command::Sync received for {}", url);
                        {
                            let mut cfg = state.config.write().await;
                            cfg.sync.insert(url.clone(), true);
                            let _ = cfg.save().await;
                        }
                        tracing::info!("[DEBUG] About to call spawn_subscription for {}", url);
                        spawn_subscription(url.clone(), &mut subscriptions, state.clone()).await;
                        tracing::info!("[DEBUG] spawn_subscription completed for {}", url);

                        if binary_sync::should_use_binary_sync(&url) {
                            let bsm = state.binary_sync.clone();
                            let url_clone = url.clone();
                            let root = config::get_root_dir()?;
                            let fullpath = root.join(url.trim_start_matches('/'));
                            tokio::spawn(async move {
                                let _ = bsm.init_binary_sync(&url_clone, &fullpath).await;
                            });
                        }
                        sync_urls_map.write().await.insert(url, true);
                    }
                    Command::Unsync { url } => {
                        tracing::info!("Disable Sync: {}", url);
                        {
                            let mut cfg = state.config.write().await;
                            cfg.sync.remove(&url);
                            let _ = cfg.save().await;
                        }
                        if let Some(handle) = subscriptions.remove(&url) {
                            handle.abort();
                        }
                        sync_urls_map.write().await.remove(&url);
                    }
                    Command::SetCookie { domain, value } => {
                        tracing::info!("Set Cookie: {} for {}", value, domain);
                        let mut cfg = state.config.write().await;
                        cfg.cookies.insert(domain, value);
                        let _ = cfg.save().await;
                    }
                    Command::SetIdentity { domain, email } => {
                        tracing::info!("Set Identity: {} for {}", email, domain);
                        let mut cfg = state.config.write().await;
                        cfg.identities.insert(domain, email);
                        let _ = cfg.save().await;
                    }
                    #[cfg(feature = "nfs")]
                    Command::Mount { port, mount_point } => {
                        if nfs_handle.is_some() {
                            tracing::warn!("NFS Server already running");
                        } else {
                            let state_nfs = state.clone();
                            let handle = tokio::spawn(async move {
                                let backend = nfs::BraidNfsBackend::new(state_nfs.clone(), state_nfs.binary_sync.blob_store());
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
