pub mod discovery;
pub mod node;
pub mod protocol;
pub mod proxy;
pub mod subscription;

pub use discovery::*;
pub use node::*;
pub use subscription::*;

use std::sync::Arc;

/// Core application state that can be shared across consumers
pub struct BraidIrohState {
    pub peer: Arc<BraidIrohNode>,
    pub node_id: String,
    pub node_name: String,
}

/// Helper function to configure and spawn a Braid-Iroh Node
pub async fn spawn_node(
    name: &str,
    port: Option<u16>,
    secret_key_override: Option<iroh::SecretKey>,
    discovery: DiscoveryConfig,
) -> anyhow::Result<(Arc<BraidIrohState>, iroh_gossip::api::GossipReceiver)> {
    let secret_key = if let Some(sk) = secret_key_override {
        sk
    } else {
        get_or_create_secret_key(name).await?
    };

    #[cfg(feature = "proxy")]
    let proxy_config = match port {
        Some(port) => {
            let peer = std::env::var("BRAID_PROXY_PEER").map_err(|_| {
                anyhow::anyhow!(
                    "A proxy port requires BRAID_PROXY_PEER; or set ProxyConfig explicitly"
                )
            })?;
            Some(crate::node::ProxyConfig {
                listen_addr: std::net::SocketAddr::from(([127, 0, 0, 1], port)),
                default_peer: peer.parse()?,
            })
        }
        None => None,
    };
    #[cfg(not(feature = "proxy"))]
    let proxy_config = {
        anyhow::ensure!(port.is_none(), "Proxy feature is disabled");
        None
    };

    let peer = BraidIrohNode::spawn(BraidIrohConfig {
        storage_dir: None,
        access: Default::default(),
        discovery,
        secret_key: Some(secret_key),
        proxy_config,
    })
    .await?;

    let peer = Arc::new(peer);
    let peer_id = peer.node_id();

    tracing::info!("[INIT] Node ID: {}", peer_id);

    // Initial default subscription (generic, no bootsrap peers yet)
    let rx = peer
        .subscribe("/demo-doc", vec![])
        .await
        .map_err(|e| anyhow::anyhow!("Subscribe failed: {}", e))?;

    let state = Arc::new(BraidIrohState {
        peer,
        node_id: format!("{}", peer_id),
        node_name: name.to_string(),
    });

    Ok((state, rx))
}

/// Persist a random endpoint identity. Names select a key file, never key material.
pub async fn get_or_create_secret_key(name: &str) -> anyhow::Result<iroh::SecretKey> {
    let root = std::env::var_os("BRAID_ROOT")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| "braid_data".into());
    let path = root
        .join("keys")
        .join(format!("{}.key", blake3::hash(name.as_bytes()).to_hex()));
    let _guard = braid_http_rs::replica::storage_lock(path.clone()).await;
    let keys = path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("Invalid identity path"))?;
    tokio::fs::create_dir_all(keys).await?;
    let lock_path = path.with_extension("lock");
    // The kernel releases this lock on process death. Waiting never blocks an
    // async executor thread, and the handle remains owned until publication.
    let _process_lock = tokio::task::spawn_blocking(move || -> std::io::Result<std::fs::File> {
        let mut options = std::fs::OpenOptions::new();
        options.create(true).truncate(false).read(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(lock_path)?;
        file.lock()?;
        Ok(file)
    })
    .await??;
    match tokio::fs::read(&path).await {
        Ok(bytes) => {
            let key: [u8; 32] = bytes
                .try_into()
                .map_err(|_| anyhow::anyhow!("Invalid persisted Iroh secret key"))?;
            return Ok(iroh::SecretKey::from_bytes(&key));
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        tokio::fs::set_permissions(keys, std::fs::Permissions::from_mode(0o700)).await?;
    }
    let mut bytes = [0u8; 32];
    use rand::Rng;
    rand::rng().fill_bytes(&mut bytes);
    braid_http_rs::replica::atomic_write_private(&path, &bytes).await?;
    Ok(iroh::SecretKey::from_bytes(&bytes))
}
