#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)
)]
#![forbid(unsafe_code)]
use braid_core::fs::{self, config};
use clap::Parser;
use nfsserve::tcp::NFSTcp;
use parking_lot::Mutex;
use rusqlite::Connection;
use std::sync::Arc;
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

    let root_dir = config::get_root_dir()?;
    let braidfs_dir = root_dir.join(".braidfs");

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

    let backend = fs::nfs::BraidNfsBackend::open(root_dir, inode_db, blob_store)?;

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
