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
