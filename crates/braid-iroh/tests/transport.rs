use braid_iroh::{BraidIrohConfig, BraidIrohNode, DiscoveryConfig, BRAID_H3_ALPN};
use futures::StreamExt;
use iroh_h3_client::IrohH3Client;
use std::time::Duration;

#[tokio::test]
async fn authenticated_h3_put_replay_subscription_and_revocation() {
    tokio::time::timeout(Duration::from_secs(40), scenario())
        .await
        .expect("H3 scenario timed out");
}

#[tokio::test]
async fn resource_sharing_is_independent_of_transport_allowlist() {
    tokio::time::timeout(Duration::from_secs(40), resource_acl_scenario())
        .await
        .unwrap();
}

async fn resource_acl_scenario() {
    use braid_http_rs::access::{administration::Assignment, PolicyChange, Principal, Role};
    let temp = tempfile::tempdir().unwrap();
    let discovery = DiscoveryConfig::mock();
    let mut nodes = Vec::new();
    for name in ["owner", "server", "reader"] {
        let node = BraidIrohNode::spawn(BraidIrohConfig {
            discovery: discovery.clone(),
            storage_dir: Some(temp.path().join(name)),
            ..Default::default()
        })
        .await
        .unwrap();
        discovery.add_node(node.node_addr().await.unwrap());
        nodes.push(node);
    }
    let (owner, server, reader) = (&nodes[0], &nodes[1], &nodes[2]);
    server.allow_peer(owner.node_id());
    server.allow_peer(reader.node_id());
    let a = IrohH3Client::new(owner.endpoint().clone(), BRAID_H3_ALPN.to_vec());
    let c = IrohH3Client::new(reader.endpoint().clone(), BRAID_H3_ALPN.to_vec());
    let url = format!("iroh+h3://{}/private", server.node_id());
    assert_eq!(
        a.put(&url)
            .header("version", "\"seed\"")
            .text("private")
            .unwrap()
            .send()
            .await
            .unwrap()
            .status,
        200
    );
    assert_eq!(c.get(&url).send().await.unwrap().status, 403);
    assert_eq!(
        c.get(&url)
            .header("version", "\"missing\"")
            .send()
            .await
            .unwrap()
            .status,
        403
    );
    let policies = server.resource_policies().unwrap();
    let actor = Principal::Peer(owner.node_id().to_string());
    let grantee = Principal::Peer(reader.node_id().to_string());
    policies
        .change(
            "/private",
            &actor,
            PolicyChange::Grant(grantee.clone(), Role::Reader),
        )
        .await
        .unwrap();
    assert_eq!(
        c.get(&url).send().await.unwrap().text().await.unwrap(),
        "private"
    );
    assert_eq!(
        c.put(&url)
            .header("version", "\"attack\"")
            .header("parents", "\"seed\"")
            .text("overwrite")
            .unwrap()
            .send()
            .await
            .unwrap()
            .status,
        403
    );
    let response = c.get(&url).header("subscribe", "?1").send().await.unwrap();
    assert_eq!(response.status, 209);
    let mut stream = Box::pin(response.bytes_stream());
    assert!(stream.next().await.unwrap().is_ok());
    policies
        .change("/private", &actor, PolicyChange::Revoke(grantee))
        .await
        .unwrap();
    let closed = tokio::time::timeout(Duration::from_secs(3), stream.next())
        .await
        .unwrap();
    assert!(closed.is_none() || closed.unwrap().is_err());
    assert_eq!(c.get(&url).send().await.unwrap().status, 403);
    // Ownerless data cannot be claimed by an allowed network peer.
    server
        .try_store_update(
            "/legacy",
            braid_http_rs::Update::snapshot(braid_http_rs::Version::new("old"), "legacy"),
        )
        .await
        .unwrap();
    let legacy_url = format!("iroh+h3://{}/legacy", server.node_id());
    assert_eq!(a.get(&legacy_url).send().await.unwrap().status, 403);
    assert_eq!(
        a.put(&legacy_url)
            .header("version", "\"claim\"")
            .header("parents", "\"old\"")
            .text("claimed")
            .unwrap()
            .send()
            .await
            .unwrap()
            .status,
        403
    );
    let plan = policies
        .plan_adoption(vec![Assignment {
            resource: "/legacy".into(),
            owner: actor.clone(),
        }])
        .await
        .unwrap();
    policies.apply_adoption(&plan).await.unwrap();
    assert_eq!(
        a.get(&legacy_url)
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap(),
        "legacy"
    );
    assert_eq!(c.get(&legacy_url).send().await.unwrap().status, 403);
    drop(stream);
    drop(a);
    drop(c);
    for node in nodes {
        node.shutdown().await.unwrap();
    }
}

async fn scenario() {
    let temp = tempfile::tempdir().unwrap();
    let discovery = DiscoveryConfig::mock();
    let a = BraidIrohNode::spawn(BraidIrohConfig {
        discovery: discovery.clone(),
        storage_dir: Some(temp.path().join("a")),
        ..Default::default()
    })
    .await
    .unwrap();
    let b = BraidIrohNode::spawn(BraidIrohConfig {
        discovery: discovery.clone(),
        storage_dir: Some(temp.path().join("b")),
        ..Default::default()
    })
    .await
    .unwrap();
    discovery.add_node(a.node_addr().await.unwrap());
    discovery.add_node(b.node_addr().await.unwrap());
    let client = IrohH3Client::new(a.endpoint().clone(), BRAID_H3_ALPN.to_vec());
    let url = format!("iroh+h3://{}/doc", b.node_id());
    assert_eq!(client.get(&url).send().await.unwrap().status, 403);
    b.allow_peer(a.node_id());
    let response = client
        .put(&url)
        .header("version", "\"one\"")
        .text("a\u{1f600}")
        .unwrap()
        .send()
        .await
        .unwrap();
    assert_eq!(response.status, 200);
    assert_eq!(
        client.get(&url).send().await.unwrap().text().await.unwrap(),
        "a\u{1f600}"
    );
    let response = client
        .get(&url)
        .header("subscribe", "?1")
        .header("parents", "\"one\"")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status, 209);
    let mut stream = Box::pin(response.bytes_stream());
    let mut parser = braid_http_rs::client::MessageParser::new();
    let mut materialized = "a\u{1f600}".to_owned();
    let mut frontier = vec![braid_http_rs::types::Version::new("one")];
    for (id, parent, text) in [("two", "one", "second"), ("three", "two", "third")] {
        let response = client
            .put(&url)
            .header("version", format!("\"{id}\""))
            .header("parents", format!("\"{parent}\""))
            .text(text)
            .unwrap()
            .send()
            .await
            .unwrap();
        assert_eq!(response.status, 200);
        while frontier != vec![braid_http_rs::types::Version::new(id)] {
            let frame = tokio::time::timeout(Duration::from_secs(5), stream.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            for frame in parser.feed(&frame).unwrap() {
                let update = braid_http_rs::client::message_to_update(frame).unwrap();
                assert_eq!(update.parents, frontier);
                materialized = braid_http_rs::core::protocol_mod::text::apply_text_patches(
                    &materialized,
                    update.patches.as_ref().unwrap(),
                )
                .unwrap();
                frontier = update.version;
            }
        }
        assert_eq!(materialized, text);
    }
    // A lost acknowledgement/retry cannot rewind the committed head.
    assert_eq!(
        client
            .put(&url)
            .header("version", "\"one\"")
            .text("a\u{1f600}")
            .unwrap()
            .send()
            .await
            .unwrap()
            .status,
        200
    );
    assert_eq!(
        client.get(&url).send().await.unwrap().text().await.unwrap(),
        "third"
    );
    b.revoke_peer(a.node_id());
    let closed = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap();
    assert!(closed.is_none() || closed.unwrap().is_err());
    assert_eq!(client.get(&url).send().await.unwrap().status, 403);
    drop(stream);
    drop(client);
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}
