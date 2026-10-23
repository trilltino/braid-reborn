//! Gossip-backed Braid subscriptions.
//!
//! Maps resource URLs to iroh-gossip topics. When a peer PUTs an update,
//! it gets broadcast to everyone subscribed to that URL's topic.
//! This replaces Braid's traditional long-lived HTTP subscription responses
//! with fully decentralized gossip.

use braid_http_rs::{Update, Version};
use bytes::Bytes;
use iroh::EndpointId;
use iroh_gossip::api::{GossipReceiver, GossipSender, GossipTopic};
use iroh_gossip::net::Gossip;
use iroh_gossip::proto::TopicId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Bounded notification. Fetch authoritative snapshots/history over HTTP/3.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GossipAnnouncement {
    pub resource: String,
    pub version: Vec<Version>,
}

/// Manages active gossip subscriptions keyed by resource URL.
///
/// Each URL gets a deterministic `TopicId` via blake3 hash, so any peer
/// that knows the URL can join the correct topic without coordination.
pub struct SubscriptionManager {
    gossip: Gossip,
    /// Active topics: URL → sender handle
    topics: Arc<Mutex<HashMap<String, GossipSender>>>,
}

impl SubscriptionManager {
    /// Wrap an existing gossip instance.
    pub fn new(gossip: Gossip) -> Self {
        Self {
            gossip,
            topics: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Normalize a URL for consistent topic lookup.
    /// Ensures the URL starts with / and has no trailing /.
    pub fn normalize_url(url: &str) -> String {
        if url.starts_with('/') {
            url.trim_end_matches('/').to_string()
        } else {
            format!("/{}", url.trim_end_matches('/'))
        }
    }

    /// Derive a deterministic TopicId from a resource URL.
    /// Any peer hashing the same URL gets the same topic.
    /// Normalizes the URL to ensure consistency.
    pub fn topic_for_url(url: &str) -> TopicId {
        let normalized = Self::normalize_url(url);
        let hash = blake3::hash(normalized.as_bytes());
        TopicId::from_bytes(*hash.as_bytes())
    }

    /// Subscribe to a resource URL. Joins the gossip topic and returns
    /// a receiver stream of incoming gossip events.
    ///
    /// `bootstrap` should contain at least one known peer so the gossip
    /// protocol can form the initial overlay.
    pub async fn subscribe(
        &self,
        url: &str,
        bootstrap: Vec<EndpointId>,
    ) -> anyhow::Result<(GossipSender, GossipReceiver)> {
        let normalized = Self::normalize_url(url);
        let topic_id = Self::topic_for_url(&normalized);
        let topic: GossipTopic = self.gossip.subscribe(topic_id, bootstrap).await?;
        let (sender, receiver) = topic.split();

        // Stash the sender so we can broadcast later
        self.topics.lock().await.insert(normalized, sender.clone());

        Ok((sender, receiver))
    }

    /// Broadcast a Braid Update to all peers on a resource's gossip topic.
    /// Announces the frontier; full update bodies are never sent through this path.
    pub async fn broadcast(&self, url: &str, update: &Update) -> anyhow::Result<()> {
        let normalized = Self::normalize_url(url);
        let bytes = serde_json::to_vec(&GossipAnnouncement {
            resource: normalized.clone(),
            version: update.version.clone(),
        })?;
        self.broadcast_raw(&normalized, Bytes::from(bytes)).await
    }

    /// Broadcast raw bytes to all peers on a resource's gossip topic.
    /// This allows sending wrapped messages with metadata.
    pub async fn broadcast_raw(&self, url: &str, data: Bytes) -> anyhow::Result<()> {
        if data.len() > 16 * 1024 {
            anyhow::bail!("Gossip announcement exceeds 16 KiB");
        }
        let normalized = Self::normalize_url(url);
        let existing = { self.topics.lock().await.get(&normalized).cloned() };
        let sender = if let Some(sender) = existing {
            sender
        } else {
            let topic = self
                .gossip
                .subscribe(Self::topic_for_url(&normalized), vec![])
                .await?;
            let (sender, _receiver) = topic.split();
            let mut topics = self.topics.lock().await;
            topics.entry(normalized).or_insert(sender).clone()
        };
        sender.broadcast(data).await?;
        Ok(())
    }

    /// Access the underlying gossip instance (e.g. for shutdown).
    #[allow(dead_code)]
    pub fn gossip(&self) -> &Gossip {
        &self.gossip
    }

    /// Join additional peers to an existing topic.
    /// This is useful for connecting to a peer after initial subscription.
    pub async fn join_peers(&self, url: &str, peers: Vec<EndpointId>) -> anyhow::Result<()> {
        let normalized = Self::normalize_url(url);
        let sender = { self.topics.lock().await.get(&normalized).cloned() };
        if let Some(sender) = sender {
            sender.join_peers(peers).await?;
            Ok(())
        } else {
            anyhow::bail!("Topic not found: {}", normalized)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_derivation_deterministic() {
        let url = "/test/resource/123";
        let topic1 = SubscriptionManager::topic_for_url(url);
        let topic2 = SubscriptionManager::topic_for_url(url);

        assert_eq!(topic1, topic2);
    }

    #[test]
    fn test_topic_derivation_unique_per_url() {
        let topic1 = SubscriptionManager::topic_for_url("/resource/a");
        let topic2 = SubscriptionManager::topic_for_url("/resource/b");

        assert_ne!(topic1, topic2);
    }

    #[test]
    fn test_topic_derivation_empty_url() {
        let topic = SubscriptionManager::topic_for_url("");
        let topic_bytes: &[u8; 32] = topic.as_ref();

        // Should produce a valid 32-byte hash (not panic)
        assert_eq!(topic_bytes.len(), 32);
    }

    #[test]
    fn test_topic_derivation_long_url() {
        let long_url = "/a/very/long/url/path".repeat(100);
        let topic = SubscriptionManager::topic_for_url(&long_url);
        let topic_bytes: &[u8; 32] = topic.as_ref();

        // Should still produce a valid 32-byte hash
        assert_eq!(topic_bytes.len(), 32);
    }

    #[test]
    fn test_topic_derivation_special_chars() {
        let url = "/resource/with spaces/and/special/chars/!@#$%";
        let topic1 = SubscriptionManager::topic_for_url(url);
        let topic2 = SubscriptionManager::topic_for_url(url);

        // Should be deterministic even with special characters
        assert_eq!(topic1, topic2);
    }
}
