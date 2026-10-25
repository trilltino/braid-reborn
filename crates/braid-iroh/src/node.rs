//! BraidIrohNode — the main entry point for a Braid-over-Iroh peer.
//!
//! Orchestrates an iroh `Endpoint`, a gossip `Gossip` instance, and the
//! Braid protocol handler. This is the type you create to participate
//! in the P2P Braid network.

use braid_http_rs::Update;

use iroh::{protocol::Router, Endpoint, EndpointAddr, EndpointId, SecretKey};
use iroh_gossip::api::GossipReceiver;
use iroh_gossip::net::Gossip;

use std::sync::Arc;

use crate::discovery::{DiscoveryConfig, MockDiscoveryMap};
use crate::protocol::{self, BraidAppState};
use crate::subscription::SubscriptionManager;

/// ALPN protocol identifier for Braid-over-H3.
/// Peers negotiate this during the QUIC handshake.
pub const BRAID_H3_ALPN: &[u8] = b"braid-h3/0";

/// Configuration for spawning a BraidIrohNode.
#[derive(Clone)]
pub struct BraidIrohConfig {
    /// Override the replica root without changing process environment.
    pub storage_dir: Option<std::path::PathBuf>,
    /// Empty by default: authorize peers explicitly, or choose Public deliberately.
    pub access: crate::protocol::AccessPolicy,
    /// Discovery configuration.
    pub discovery: DiscoveryConfig,

    /// Optional pre-generated secret key for a stable identity.
    /// If `None`, a random identity is generated.
    pub secret_key: Option<SecretKey>,

    /// Optional configuration for the TCP proxy bridge.
    pub proxy_config: Option<ProxyConfig>,
}

/// Configuration for the TCP proxy bridge.
#[derive(Clone)]
pub struct ProxyConfig {
    /// Local address to listen on (e.g. 127.0.0.1:8080).
    pub listen_addr: std::net::SocketAddr,
    /// Default peer ID to forward requests to.
    pub default_peer: EndpointId,
}

impl Default for BraidIrohConfig {
    fn default() -> Self {
        Self {
            storage_dir: None,
            access: Default::default(),
            discovery: DiscoveryConfig::Mock(MockDiscoveryMap::new()),
            secret_key: None,
            proxy_config: None,
        }
    }
}

/// A Braid-capable P2P peer. Holds the iroh endpoint, gossip, and
/// subscription state. Create one per peer identity.
pub struct BraidIrohNode {
    access: Arc<parking_lot::RwLock<crate::protocol::AccessPolicy>>,
    policy_changed: tokio::sync::watch::Sender<u64>,
    proxy_task: Option<tokio::task::JoinHandle<()>>,
    endpoint: Endpoint,
    #[allow(dead_code)]
    router: Router,
    subscription_mgr: Arc<SubscriptionManager>,
    replica: braid_http_rs::replica::ReplicaStore,
}

impl BraidIrohNode {
    /// Spawn a new Braid-over-Iroh node.
    ///
    /// This sets up the iroh endpoint, starts the gossip protocol,
    /// mounts the Braid-HTTP Axum routes via `IrohAxum`, and begins
    /// accepting incoming connections.
    pub async fn spawn(config: BraidIrohConfig) -> anyhow::Result<Self> {
        // 1. Build the iroh endpoint with discovery + Braid ALPN
        let mut builder = match &config.discovery {
            DiscoveryConfig::Mock(_) => Endpoint::builder(iroh::endpoint::presets::Minimal),
            DiscoveryConfig::Real => Endpoint::builder(iroh::endpoint::presets::N0),
        }
        .alpns(vec![
            BRAID_H3_ALPN.to_vec(),
            iroh_gossip::net::GOSSIP_ALPN.to_vec(),
        ]);

        // Apply optional secret key
        if let Some(key) = config.secret_key {
            builder = builder.secret_key(key);
        }

        // Apply discovery logic
        match config.discovery {
            DiscoveryConfig::Mock(map) => {
                builder = builder.address_lookup(map);
            }
            DiscoveryConfig::Real => {
                // Default Iroh discovery is already enabled by default in Endpoint::builder()
                // unless we override it. So we do nothing here.
            }
        }

        let endpoint = builder.bind().await?;

        tracing::info!(id = %endpoint.id(), "iroh endpoint bound");

        // 2. Start the gossip protocol on this endpoint
        // Gossip is owned by the router and stops during router shutdown.
        let gossip = Gossip::builder().spawn(endpoint.clone());

        // 3. Build shared state
        let root = config
            .storage_dir
            .or_else(|| std::env::var_os("BRAID_ROOT").map(std::path::PathBuf::from))
            .unwrap_or_else(|| "braid_data".into());
        let replica = braid_http_rs::replica::ReplicaStore::new(
            root.join("iroh").join(endpoint.id().to_string()),
        )
        .map_err(|error| anyhow::anyhow!(error))?;
        let subscription_mgr = Arc::new(SubscriptionManager::new(gossip.clone()));
        let access = Arc::new(parking_lot::RwLock::new(config.access));
        let (policy_changed, _) = tokio::sync::watch::channel(0);
        let app_state = BraidAppState {
            policy_changed: policy_changed.clone(),
            replica: replica.clone(),
            access: access.clone(),
        };

        // 4. Mount the Braid protocol handler on the iroh router
        let braid_handler = protocol::build_protocol_handler(app_state);

        let router = Router::builder(endpoint.clone())
            .accept(BRAID_H3_ALPN, braid_handler)
            .accept(iroh_gossip::net::GOSSIP_ALPN, gossip.clone())
            .spawn();

        // 5. Start TCP Proxy if configured (Phase 4)
        #[cfg(feature = "proxy")]
        let mut proxy_task = None;
        #[cfg(not(feature = "proxy"))]
        let proxy_task = None;
        #[cfg(feature = "proxy")]
        if let Some(proxy_conf) = config.proxy_config {
            let endpoint_clone = endpoint.clone();
            proxy_task = Some(tokio::spawn(async move {
                if let Err(e) = crate::proxy::bridge::start_proxy(
                    &endpoint_clone,
                    proxy_conf.listen_addr,
                    proxy_conf.default_peer,
                )
                .await
                {
                    tracing::error!("TCP Proxy failed: {}", e);
                }
            }));
        }

        Ok(Self {
            policy_changed,
            proxy_task,
            access,
            endpoint,
            router,
            subscription_mgr,
            replica,
        })
    }

    /// This node's public identity (EndpointId / NodeId).
    pub fn node_id(&self) -> EndpointId {
        self.endpoint.id()
    }

    /// Full address info for this node (id + addresses + relay).
    pub async fn node_addr(&self) -> anyhow::Result<EndpointAddr> {
        Ok(self.endpoint.addr())
    }

    /// Subscribe to a resource URL on the gossip network.
    /// Returns a receiver stream of gossip events for this resource.
    pub async fn subscribe(
        &self,
        url: &str,
        bootstrap: Vec<EndpointId>,
    ) -> anyhow::Result<GossipReceiver> {
        let normalized = SubscriptionManager::normalize_url(url);
        if !self
            .replica
            .policy_store()?
            .get(&normalized)
            .await?
            .is_some_and(|policy| policy.public_read)
        {
            anyhow::bail!("Private resources require authenticated H3 subscriptions");
        }
        let (_sender, receiver) = self.subscription_mgr.subscribe(url, bootstrap).await?;
        Ok(receiver)
    }

    /// PUT a Braid Update to a resource. Stores it locally and broadcasts
    /// to all gossip subscribers.
    pub async fn put(&self, url: &str, update: Update) -> anyhow::Result<()> {
        let normalized = SubscriptionManager::normalize_url(url);
        let policies = self.replica.policy_store()?;
        self.replica
            .commit_authorized(
                &normalized,
                &update,
                update.merge_type.as_deref().unwrap_or("simpleton"),
                &policies,
                &braid_http_rs::access::Principal::Peer(self.node_id().to_string()),
            )
            .await?;
        // Gossip failure never rolls back an already durable commit. Peers catch up via HTTP.
        let public_resource = match policies.get(&normalized).await {
            Ok(policy) => policy.is_some_and(|policy| policy.public_read),
            Err(error) => {
                tracing::warn!(%error, "Gossip policy unavailable after durable commit");
                false
            }
        };
        if public_resource && matches!(*self.access.read(), crate::protocol::AccessPolicy::Public) {
            if let Err(e) = self.subscription_mgr.broadcast(&normalized, &update).await {
                tracing::warn!(%e,"Gossip announcement failed after commit");
            }
        }
        Ok(())
    }
    /// Trusted local compatibility adapter. Remote ingestion must carry an
    /// authenticated principal through the protected HTTP/H3 mutation boundary.
    #[deprecated(
        note = "Use try_store_update and handle storage errors; never ingest untrusted gossip bodies"
    )]
    pub async fn store_update(&self, url: &str, update: Update) {
        if let Err(e) = self.try_store_update(url, update).await {
            tracing::warn!(%e,%url,"Local update was not committed");
        }
    }
    pub async fn try_store_update(
        &self,
        url: &str,
        update: Update,
    ) -> Result<(), braid_http_rs::replica::ReplicaError> {
        let normalized = SubscriptionManager::normalize_url(url);
        self.replica
            .commit(
                &normalized,
                &update,
                update.merge_type.as_deref().unwrap_or("simpleton"),
            )
            .await?;
        Ok(())
    }
    #[deprecated(note = "Use try_get to distinguish unavailable storage from a missing resource")]
    pub async fn get(&self, url: &str) -> Option<Update> {
        self.try_get(url).await.ok().flatten()
    }
    pub async fn try_get(
        &self,
        url: &str,
    ) -> Result<Option<Update>, braid_http_rs::replica::ReplicaError> {
        let url = SubscriptionManager::normalize_url(url);
        Ok(self.replica.load(&url).await?.map(|doc| doc.snapshot()))
    }
    /// Legacy event lookup. For materialized historical content use snapshot_at.
    #[deprecated(note = "Use try_get_version or try_snapshot_at and handle storage errors")]
    pub async fn get_version(&self, url: &str, version_id: &str) -> Option<Update> {
        self.try_get_version(url, version_id).await.ok().flatten()
    }
    pub async fn try_get_version(
        &self,
        url: &str,
        version_id: &str,
    ) -> Result<Option<Update>, braid_http_rs::replica::ReplicaError> {
        let url = SubscriptionManager::normalize_url(url);
        Ok(self.replica.load(&url).await?.and_then(|document| {
            document
                .history
                .into_iter()
                .find(|u| u.version.iter().any(|v| v.to_string() == version_id))
        }))
    }
    pub async fn try_snapshot_at(
        &self,
        url: &str,
        frontier: &[braid_http_rs::Version],
    ) -> Result<Option<Update>, braid_http_rs::replica::ReplicaError> {
        self.replica
            .snapshot_at(&SubscriptionManager::normalize_url(url), frontier)
            .await
    }
    #[deprecated(note = "Use try_get_history to distinguish storage errors from empty history")]
    pub async fn get_history(&self, url: &str) -> Vec<String> {
        self.try_get_history(url).await.unwrap_or_default()
    }
    pub async fn try_get_history(
        &self,
        url: &str,
    ) -> Result<Vec<String>, braid_http_rs::replica::ReplicaError> {
        let url = SubscriptionManager::normalize_url(url);
        Ok(self
            .replica
            .load(&url)
            .await?
            .map(|doc| {
                doc.history
                    .into_iter()
                    .flat_map(|u| u.version.into_iter().map(|v| v.to_string()))
                    .collect()
            })
            .unwrap_or_default())
    }

    /// Shut down the node gracefully.
    #[allow(dead_code)]
    pub async fn shutdown(self) -> anyhow::Result<()> {
        if let Some(task) = self.proxy_task {
            task.abort();
            let _ = task.await;
        }
        self.router.shutdown().await?;
        Ok(())
    }

    /// Access the subscription manager (for advanced usage).
    #[allow(dead_code)]
    pub fn subscriptions(&self) -> &Arc<SubscriptionManager> {
        &self.subscription_mgr
    }

    /// Access the iroh endpoint (for advanced usage).
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// Trusted local administration. Network grants must additionally authorize
    /// the caller as an owner; this handle is never sent to remote peers.
    pub fn resource_policies(
        &self,
    ) -> Result<braid_http_rs::access::PolicyStore, braid_http_rs::access::AccessError> {
        self.replica.policy_store()
    }

    pub fn allow_peer(&self, peer: EndpointId) {
        if let crate::protocol::AccessPolicy::Peers(peers) = &mut *self.access.write() {
            peers.insert(peer);
        }
    }

    pub fn revoke_peer(&self, peer: EndpointId) {
        if let crate::protocol::AccessPolicy::Peers(peers) = &mut *self.access.write() {
            peers.remove(&peer);
        }
        self.policy_changed
            .send_modify(|generation| *generation = generation.wrapping_add(1));
    }

    /// Join additional peers to an existing gossip topic.
    pub async fn join_peers(&self, url: &str, peers: Vec<EndpointId>) -> anyhow::Result<()> {
        println!("[NODE] Joining {} peers to topic {}", peers.len(), url);
        for peer in &peers {
            println!("[NODE]   joining: {}", peer);
        }
        self.subscription_mgr.join_peers(url, peers).await
    }
}
