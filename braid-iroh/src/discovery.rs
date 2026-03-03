//! Peer discovery configuration for braid_iroh.
//!
//! For the demo, we use `MockDiscoveryMap` (an in-memory peer registry
//! shared across endpoints on the same machine). In production you'd
//! swap this for iroh's DNS + Pkarr discovery.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use iroh::address_lookup::{
    AddressLookup, EndpointData, EndpointInfo, Error, IntoAddressLookup, Item,
};
use iroh::{Endpoint, EndpointAddr, EndpointId};
use n0_future::boxed::BoxStream;
use n0_future::StreamExt;

/// How peers find each other on the network.
#[derive(Clone)]
pub enum DiscoveryConfig {
    /// In-memory discovery — peers register themselves in a shared map.
    /// Use this for tests and the Leptos demo (both peers on the same machine).
    Mock(MockDiscoveryMap),
    /// Use Iroh's real discovery (DNS, Pkarr, MDNS).
    Real,
}

impl DiscoveryConfig {
    /// Create a new mock discovery instance. Clone it and pass the same
    /// instance to every peer that should be able to find each other.
    pub fn mock() -> Self {
        DiscoveryConfig::Mock(MockDiscoveryMap::new())
    }

    /// Register a node's address so other peers using the same mock can find it.
    pub fn add_node(&self, node_addr: EndpointAddr) {
        if let DiscoveryConfig::Mock(map) = self {
            let info = EndpointInfo::from(node_addr);
            map.add_node(info.endpoint_id, info.data);
        }
        // DiscoveryConfig::Real doesn't need explicit registration - uses Iroh's built-in discovery
    }
}

/// A shared in-memory discovery map.
///
/// All endpoints that hold a clone of the same `MockDiscoveryMap` can
/// find each other — perfect for demos and tests.
#[derive(Debug, Default, Clone)]
pub struct MockDiscoveryMap {
    peers: Arc<RwLock<BTreeMap<EndpointId, Arc<EndpointData>>>>,
}

impl MockDiscoveryMap {
    /// Create a new empty discovery map.
    pub fn new() -> Self {
        Default::default()
    }

    /// Add a node's address information to the mock discovery map.
    /// Useful for manually registering peers if needed.
    pub fn add_node(&self, id: EndpointId, data: EndpointData) {
        self.peers.write().unwrap().insert(id, Arc::new(data));
    }
}

impl IntoAddressLookup for MockDiscoveryMap {
    fn into_address_lookup(
        self,
        endpoint: &Endpoint,
    ) -> Result<impl AddressLookup, iroh::address_lookup::IntoAddressLookupError> {
        Ok(MockDiscoveryInstance {
            id: endpoint.id(),
            map: self,
        })
    }
}

/// Per-endpoint discovery instance that publishes its own address and
/// resolves peers from the shared map.
#[derive(Debug, Clone)]
struct MockDiscoveryInstance {
    id: EndpointId,
    map: MockDiscoveryMap,
}

impl AddressLookup for MockDiscoveryInstance {
    fn publish(&self, data: &EndpointData) {
        self.map
            .peers
            .write()
            .unwrap()
            .insert(self.id, Arc::new(data.clone()));
    }

    fn resolve(&self, endpoint_id: EndpointId) -> Option<BoxStream<Result<Item, Error>>> {
        let data = self.map.peers.read().unwrap().get(&endpoint_id).cloned()?;
        let info = EndpointInfo::from_parts(endpoint_id, data.as_ref().clone());
        let item = Item::new(info, "mock-braid", None);
        Some(n0_future::stream::once(Ok(item)).boxed())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mock_discovery_map_new() {
        let map = MockDiscoveryMap::new();
        assert!(map.peers.read().unwrap().is_empty());
    }

    #[test]
    fn test_mock_discovery_default() {
        let map: MockDiscoveryMap = Default::default();
        assert!(map.peers.read().unwrap().is_empty());
    }

    #[test]
    fn test_discovery_config_mock() {
        let config = DiscoveryConfig::mock();
        matches!(config, DiscoveryConfig::Mock(_));
    }

    #[test]
    fn test_discovery_config_clone() {
        let config = DiscoveryConfig::mock();
        let cloned = config.clone();

        // Both should be Mock variant
        matches!(config, DiscoveryConfig::Mock(_));
        matches!(cloned, DiscoveryConfig::Mock(_));
    }

    #[test]
    fn test_mock_discovery_map_add_node() {
        let map = MockDiscoveryMap::new();
        let id = EndpointId::from_bytes(&[1u8; 32]).expect("valid test key");
        let data = EndpointData::default();

        map.add_node(id, data);

        assert_eq!(map.peers.read().unwrap().len(), 1);
        assert!(map.peers.read().unwrap().contains_key(&id));
    }

    #[test]
    fn test_mock_discovery_map_multiple_nodes() {
        let map = MockDiscoveryMap::new();

        for i in 0..5 {
            let id = EndpointId::from_bytes(&[i as u8; 32]).expect("valid test key");
            let data = EndpointData::default();
            map.add_node(id, data);
        }

        assert_eq!(map.peers.read().unwrap().len(), 5);
    }
}
