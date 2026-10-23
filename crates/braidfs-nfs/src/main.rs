use braid_core::fs::{self, config, state, versions, ActivityTracker, PendingWrites};
use braid_core::BraidClient;
use clap::Parser;
use nfsserve::tcp::NFSTcp;
use parking_lot::Mutex;
use rusqlite::Connection;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info};

#[derive(Parser)]
struct Cli {
    #[arg(short, long, default_value = "20491")]
    nfs_port: u16,
    #[arg(short, long, default_value = "45678")]
    daemon_port: u16,
    #[arg(short, long)]
    mount_point: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();

    info!("=== BraidFS NFS Monitor [crate: braidfs-nfs] ===");
    info!("Manages the network drive bridge and discovery.");
    info!("Mode: Standalone Server (Port {})", cli.nfs_port);

    // 1. Load Config (Shared with Daemon)
    let config = config::Config::load().await?;
    let root_dir = config::get_root_dir()?;
    let braidfs_dir = root_dir.join(".braidfs");
    let config = Arc::new(RwLock::new(config));

    // 2. Initialize Stores (Shared DBs)
    let blob_store = Arc::new(
        braid_core::blob::BlobStore::new(
            braidfs_dir.join("blobs"),
            braidfs_dir.join("meta.sqlite"),
        )
        .await
        .map_err(|e| anyhow::anyhow!("Failed to init BlobStore: {}", e))?,
    );

    let inode_db_path = braidfs_dir.join("inodes.sqlite");
    let inode_conn = Connection::open(&inode_db_path)
        .map_err(|e| anyhow::anyhow!("Failed to open inode DB: {}", e))?;
    let inode_db = Arc::new(Mutex::new(inode_conn));

    let version_store = versions::VersionStore::load().await?;
    let version_store = Arc::new(RwLock::new(version_store));

    // 3. Construct Minimal DaemonState
    // NFS needs access to the DBs and Config, but doesn't need to drive the active sync logic.
    // We provide valid handles to the stores, and dummy/new instances for the rest.

    let content_cache = Arc::new(RwLock::new(HashMap::new()));
    let activity_tracker = ActivityTracker::new();
    let merge_registry = Arc::new(braid_core::core::merge::MergeTypeRegistry::new());
    let active_merges = Arc::new(RwLock::new(HashMap::new()));
    let pending_writes = PendingWrites::new();
    let client = BraidClient::new()?; // Standalone client, separate from Daemon's
    let failed_syncs = Arc::new(RwLock::new(HashMap::new()));
    let debouncer = Arc::new(fs::debouncer::DebouncedSyncManager::new_placeholder());

    // We need a dummy channel for tx_cmd since we won't be sending commands to ourselves this way
    let (tx_cmd, _) = async_channel::unbounded();

    // Binary Sync Manager needed for state struct, but NFS might not use it directly for syncing
    let rate_limiter = Arc::new(fs::rate_limiter::ReconnectRateLimiter::new(100));
    let binary_sync_manager =
        fs::binary_sync::BinarySyncManager::new(rate_limiter, blob_store.clone())
            .map_err(|e| anyhow::anyhow!(e))?;
    let binary_sync_manager = Arc::new(binary_sync_manager);

    let state = state::DaemonState {
        config,
        content_cache,
        version_store,
        tracker: activity_tracker,
        merge_registry,
        active_merges,
        pending: pending_writes,
        client,
        failed_syncs,
        binary_sync: binary_sync_manager,
        inode_db,
        tx_cmd,
        debouncer,
        local_server_managed: Arc::new(RwLock::new(std::collections::HashSet::new())),
    };

    // 4. Start NFS Server
    let backend = fs::nfs::BraidNfsBackend::new(state.clone(), blob_store);

    // Trigger OS Mount if requested
    if let Some(mp) = &cli.mount_point {
        info!("Mounting to {}...", mp);
        if let Err(e) = fs::mount::mount(cli.nfs_port, std::path::Path::new(mp)) {
            error!("Failed to mount: {}", e);
        } else {
            info!("Mount successful.");
        }
    }

    info!("Starting NFS TCP Listener on 127.0.0.1:{}", cli.nfs_port);
    let listener =
        nfsserve::tcp::NFSTcpListener::bind(&format!("127.0.0.1:{}", cli.nfs_port), backend)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to bind NFS port: {}", e))?;

    // Handle signals
    tokio::select! {
        res = listener.handle_forever() => {
            if let Err(e) = res {
                error!("NFS Server crashed: {}", e);
            }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Shutdown signal received.");
        }
    }

    // Cleanup unmount
    if let Some(mp) = &cli.mount_point {
        info!("Unmounting {}...", mp);
        let _ = fs::mount::unmount(std::path::Path::new(mp));
    }

    Ok(())
}
