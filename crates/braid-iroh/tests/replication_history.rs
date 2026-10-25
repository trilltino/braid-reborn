//! Authenticated replication ingestion over the internal H3 endpoint.
use braid_http_rs::{
    access::{PolicyChange, Principal, Role},
    replica::{HistoryPage, REPLICATION_HISTORY_PATH},
    Update, Version,
};
use braid_iroh::{BraidIrohConfig, BraidIrohNode, DiscoveryConfig, BRAID_H3_ALPN};
use iroh_h3_client::IrohH3Client;
use std::time::Duration;

async fn spawn(
    discovery: &DiscoveryConfig,
    dir: std::path::PathBuf,
    secret: Option<iroh::SecretKey>,
) -> BraidIrohNode {
    let node = BraidIrohNode::spawn(BraidIrohConfig {
        discovery: discovery.clone(),
        storage_dir: Some(dir),
        secret_key: secret,
        ..Default::default()
    })
    .await
    .unwrap();
    discovery.add_node(node.node_addr().await.unwrap());
    node
}

fn history_request(resource: &str, from: &[Version], max_events: usize) -> serde_json::Value {
    serde_json::json!({
        "resource": resource,
        "from": from,
        "limits": { "max_events": max_events, "max_bytes": 1024 * 1024 }
    })
}

#[tokio::test]
async fn paged_history_preserves_original_identities_and_is_authorized() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let temp = tempfile::tempdir().unwrap();
        let discovery = DiscoveryConfig::mock();
        let server = spawn(&discovery, temp.path().join("server"), None).await;
        let writer = spawn(&discovery, temp.path().join("writer"), None).await;
        let reader = spawn(&discovery, temp.path().join("reader"), None).await;
        let stranger = spawn(&discovery, temp.path().join("stranger"), None).await;
        for node in [&writer, &reader, &stranger] {
            server.allow_peer(node.node_id());
        }
        assert!(server.replication_history_enabled());
        let write = IrohH3Client::new(writer.endpoint().clone(), BRAID_H3_ALPN.to_vec());
        let read = IrohH3Client::new(reader.endpoint().clone(), BRAID_H3_ALPN.to_vec());
        let other = IrohH3Client::new(stranger.endpoint().clone(), BRAID_H3_ALPN.to_vec());
        let base = format!("iroh+h3://{}", server.node_id());

        let mut parent: Option<String> = None;
        for index in 0..5 {
            let mut request = write
                .put(format!("{base}/log"))
                .header("version", format!("\"e{index}\""));
            if let Some(parent) = &parent {
                request = request.header("parents", format!("\"{parent}\""));
            }
            let status = request
                .text(format!("content {index}"))
                .unwrap()
                .send()
                .await
                .unwrap()
                .status;
            assert_eq!(status, 200, "event {index}");
            parent = Some(format!("e{index}"));
        }
        let policies = server.resource_policies().unwrap();
        policies
            .change(
                "/log",
                &Principal::Peer(writer.node_id().to_string()),
                PolicyChange::Grant(Principal::Peer(reader.node_id().to_string()), Role::Reader),
            )
            .await
            .unwrap();

        let url = format!("{base}{REPLICATION_HISTORY_PATH}");
        let mut from: Vec<Version> = vec![];
        let mut identities = Vec::new();
        let mut pages = 0;
        loop {
            let response = read
                .post(&url)
                .header("content-type", "application/json")
                .bytes(serde_json::to_vec(&history_request("/log", &from, 2)).unwrap())
                .unwrap()
                .send()
                .await
                .unwrap();
            assert_eq!(response.status, 200);
            let page: HistoryPage =
                serde_json::from_slice(&response.bytes().await.unwrap()).unwrap();
            assert!(page.events.len() <= 2);
            pages += 1;
            for event in &page.events {
                identities.push(event.version[0].to_string());
                // Original events travel as stored: no manufactured Peer header.
                assert!(!event.extra_headers.contains_key("peer"));
            }
            assert!(page.complete || page.through != from);
            from = page.through;
            if page.complete {
                break;
            }
        }
        assert_eq!(pages, 3);
        assert_eq!(identities, ["e0", "e1", "e2", "e3", "e4"]);
        assert_eq!(from, vec![Version::new("e4")]);

        // Transport admission alone grants nothing; the resource policy decides.
        let denied = other
            .post(&url)
            .header("content-type", "application/json")
            .bytes(serde_json::to_vec(&history_request("/log", &[], 2)).unwrap())
            .unwrap()
            .send()
            .await
            .unwrap();
        assert_eq!(denied.status, 403);
        // A missing resource is indistinguishable from a forbidden one.
        let missing = read
            .post(&url)
            .header("content-type", "application/json")
            .bytes(serde_json::to_vec(&history_request("/never-created", &[], 2)).unwrap())
            .unwrap()
            .send()
            .await
            .unwrap();
        assert_eq!(missing.status, 403);
        // Malformed, oversized, and namespace-targeting requests fail without detail leaks.
        let malformed = read
            .post(&url)
            .header("content-type", "application/json")
            .text("{\"resource\": 7}")
            .unwrap()
            .send()
            .await
            .unwrap();
        assert_eq!(malformed.status, 400);
        let namespace = read
            .post(&url)
            .header("content-type", "application/json")
            .bytes(serde_json::to_vec(&history_request(REPLICATION_HISTORY_PATH, &[], 2)).unwrap())
            .unwrap()
            .send()
            .await
            .unwrap();
        assert_eq!(namespace.status, 400);
        let too_many = read
            .post(&url)
            .header("content-type", "application/json")
            .bytes(serde_json::to_vec(&history_request("/log", &[], 5000)).unwrap())
            .unwrap()
            .send()
            .await
            .unwrap();
        assert_eq!(too_many.status, 400);
        // GET is not routed to the endpoint.
        assert_ne!(read.get(&url).send().await.unwrap().status, 200);
        // Ordinary finite GET behavior is unchanged for the same peer.
        assert_eq!(
            read.get(format!("{base}/log"))
                .send()
                .await
                .unwrap()
                .text()
                .await
                .unwrap(),
            "content 4"
        );
    })
    .await
    .expect("replication history scenario timed out");
}

#[tokio::test]
async fn legacy_namespace_collisions_disable_the_endpoint_without_modifying_data() {
    tokio::time::timeout(Duration::from_secs(60), async {
        let temp = tempfile::tempdir().unwrap();
        let discovery = DiscoveryConfig::mock();
        let secret = iroh::SecretKey::from_bytes(&[7u8; 32]);
        let id = secret.public();
        let storage = temp.path().join("server");
        let root = storage.join("iroh").join(id.to_string());
        std::fs::create_dir_all(&root).unwrap();
        // A legacy sanitized-name resource that the reserved path would shadow.
        let legacy = root.join("_.well-known_braid_replication_v1_history.json");
        std::fs::write(&legacy, b"{\"legacy\":true}").unwrap();
        let server = spawn(&discovery, storage.clone(), Some(secret)).await;
        let reader = spawn(&discovery, temp.path().join("reader"), None).await;
        server.allow_peer(reader.node_id());
        assert!(!server.replication_history_enabled());
        let read = IrohH3Client::new(reader.endpoint().clone(), BRAID_H3_ALPN.to_vec());
        let response = read
            .post(format!(
                "iroh+h3://{}{REPLICATION_HISTORY_PATH}",
                server.node_id()
            ))
            .header("content-type", "application/json")
            .bytes(serde_json::to_vec(&history_request("/anything", &[], 2)).unwrap())
            .unwrap()
            .send()
            .await
            .unwrap();
        assert_eq!(response.status, 503);
        // Nothing was renamed, deleted or adopted.
        assert_eq!(std::fs::read(&legacy).unwrap(), b"{\"legacy\":true}");
        let _ = Update::snapshot(Version::new("unused"), "");
    })
    .await
    .expect("collision scenario timed out");
}
