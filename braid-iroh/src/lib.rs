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
        get_or_create_secret_key(name).await
    };

    let proxy_port = port.unwrap_or(8080);
    
    tracing::info!("[INIT] Spawning Node: {} | Port: {}", name, proxy_port);

    #[cfg(feature = "proxy")]
    let proxy_config = Some(crate::node::ProxyConfig {
        listen_addr: format!("127.0.0.1:{}", proxy_port).parse().unwrap(),
        default_peer: iroh::EndpointId::from_bytes(&[0u8; 32]).expect("Invalid placeholder key"), 
    });
    
    #[cfg(not(feature = "proxy"))]
    let proxy_config = None;

    let peer = BraidIrohNode::spawn(BraidIrohConfig {
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

/// Helper to predictably derive keys for "alice" and "bob" or hash other names
pub async fn get_or_create_secret_key(name: &str) -> iroh::SecretKey {
    if name == "alice" {
        return iroh::SecretKey::from_bytes(&[1u8; 32]);
    }
    if name == "bob" {
        return iroh::SecretKey::from_bytes(&[2u8; 32]);
    }
    
    let hash = blake3::hash(name.as_bytes());
    iroh::SecretKey::from_bytes(hash.as_bytes())
}
