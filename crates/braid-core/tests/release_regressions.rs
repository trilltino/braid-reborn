use axum::{
    body::to_bytes,
    http::{HeaderMap, HeaderValue, StatusCode},
};
use braid_core::{
    replica::ReplicaStore,
    server::{replica_http, ConflictResolver, ResourceStateManager},
    Update, Version,
};

#[tokio::test]
async fn historical_get_must_honor_version_or_use_draft_fallback() {
    let root = tempfile::tempdir().unwrap();
    let store = ReplicaStore::new(root.path().into()).unwrap();
    store
        .commit(
            "doc",
            &Update::snapshot(Version::new("v1"), "old"),
            "simpleton",
        )
        .await
        .unwrap();
    store
        .commit(
            "doc",
            &Update::snapshot(Version::new("v2"), "new").with_parent(Version::new("v1")),
            "simpleton",
        )
        .await
        .unwrap();
    let mut headers = HeaderMap::new();
    headers.insert("version", HeaderValue::from_static("\"v1\""));
    let response = replica_http::get(store, "doc".into(), headers, "simpleton").await;
    let status = response.status();
    let version = response.headers().get("version").cloned();
    let body = to_bytes(response.into_body(), 1024).await.unwrap();
    assert!(
        !status.is_success()
            || version.is_none()
            || (body.as_ref() == b"old" && version.as_ref().unwrap() == "\"v1\""),
        "historical GET returned {status}, Version={version:?}, body={body:?}"
    );
}

#[tokio::test]
async fn subscribe_with_version_must_be_rejected() {
    let root = tempfile::tempdir().unwrap();
    let store = ReplicaStore::new(root.path().into()).unwrap();
    let mut headers = HeaderMap::new();
    headers.insert("subscribe", HeaderValue::from_static("?1"));
    headers.insert("version", HeaderValue::from_static("\"v1\""));
    let response = replica_http::get(store, "doc".into(), headers, "simpleton").await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn legacy_resolver_must_not_acknowledge_a_partially_applied_batch() {
    let manager = ResourceStateManager::new();
    manager
        .apply_update("doc", "abc", "alice", Some("seed"), Some("diamond"))
        .unwrap();
    let resolver = ConflictResolver::new(manager.clone());
    let update = Update::snapshot(
        Version::new("batch"),
        r#"{"inserts":[{"pos":0,"text":"X"},{"pos":100,"text":"Y"}]}"#,
    )
    .with_parent(Version::new("seed"))
    .with_merge_type("diamond");
    let result = resolver.resolve_update("doc", &update, "bob").await;
    let content = resolver.get_resource_content("doc").unwrap();
    assert!(
        result.is_err() && content == "abc",
        "result={result:?}, content={content:?}"
    );
}

#[tokio::test]
async fn legacy_batch_records_one_identity_and_preserves_all_operations() {
    let manager = ResourceStateManager::new();
    manager
        .apply_update("doc", "abc", "alice", Some("seed"), Some("diamond"))
        .unwrap();
    let resolver = ConflictResolver::new(manager.clone());
    let update = Update::snapshot(
        Version::new("batch"),
        r#"{"inserts":[{"pos":0,"text":"X"},{"pos":4,"text":"Y"}]}"#,
    )
    .with_parent(Version::new("seed"))
    .with_merge_type("diamond");
    resolver
        .resolve_update("doc", &update, "bob")
        .await
        .unwrap();
    resolver
        .resolve_update("doc", &update, "bob")
        .await
        .unwrap();
    assert_eq!(resolver.get_resource_content("doc").unwrap(), "XabcY");
    assert!(manager.has_version("doc", "batch"));
    let collision = Update::snapshot(Version::new("batch"), "collision")
        .with_parent(Version::new("seed"))
        .with_merge_type("diamond");
    assert!(resolver
        .resolve_update("doc", &collision, "bob")
        .await
        .is_err());
    assert_eq!(resolver.get_resource_content("doc").unwrap(), "XabcY");
}

#[tokio::test]
async fn rejected_first_batch_does_not_publish_an_empty_resource() {
    let manager = ResourceStateManager::new();
    let mut indices = manager.subscribe_to_indices();
    let resolver = ConflictResolver::new(manager.clone());
    let update = Update::snapshot(
        Version::new("bad"),
        r#"{"inserts":[{"pos":100,"text":"lost"}]}"#,
    )
    .with_merge_type("diamond");
    assert!(resolver
        .resolve_update("new", &update, "alice")
        .await
        .is_err());
    assert!(manager.list_resources().is_empty());
    assert!(indices.try_recv().is_err());
}

#[tokio::test]
async fn a_false_subscribe_header_still_cannot_accompany_version() {
    let root = tempfile::tempdir().unwrap();
    let store = ReplicaStore::new(root.path().into()).unwrap();
    let mut headers = HeaderMap::new();
    headers.insert("subscribe", HeaderValue::from_static("?0"));
    headers.insert("version", HeaderValue::from_static("\"v1\""));
    assert_eq!(
        replica_http::get(store, "doc".into(), headers, "simpleton")
            .await
            .status(),
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
async fn historical_frontiers_and_intervals_keep_independent_branches() {
    let root = tempfile::tempdir().unwrap();
    let store = ReplicaStore::new(root.path().into()).unwrap();
    store
        .commit(
            "doc",
            &Update::snapshot(Version::new("seed"), "a").with_merge_type("diamond"),
            "diamond",
        )
        .await
        .unwrap();
    for (id, text) in [("alice", "A"), ("bob", "B")] {
        let event = Update::patched(
            Version::new(id),
            vec![braid_core::Patch::text("[1:1]", text)],
        )
        .with_parent(Version::new("seed"))
        .with_merge_type("diamond");
        store.commit("doc", &event, "diamond").await.unwrap();
    }
    let document = store.load("doc").await.unwrap().unwrap();
    assert_eq!(
        document
            .snapshot_at(&[Version::new("alice")])
            .unwrap()
            .body_str(),
        Some("aA")
    );
    assert_eq!(
        document
            .snapshot_at(&[Version::new("bob")])
            .unwrap()
            .body_str(),
        Some("aB")
    );
    let heads = document.snapshot().version;
    assert_eq!(
        document.snapshot_at(&heads).unwrap().body_str(),
        Some(document.content.as_str())
    );
    let interval = document
        .causal_history(&[Version::new("alice")], &heads)
        .unwrap();
    assert_eq!(interval.len(), 1);
    assert_eq!(interval[0].version, vec![Version::new("bob")]);
    assert!(document
        .causal_history(&heads, &[Version::new("alice")])
        .is_err());
    let mut headers = HeaderMap::new();
    headers.insert("version", HeaderValue::from_static("\"unknown\""));
    let response = replica_http::get(store, "doc".into(), headers, "diamond").await;
    assert_eq!(response.status().as_u16(), 432);
    assert_eq!(response.headers()["version"], "\"unknown\"");
}

#[tokio::test]
async fn unavailable_subscription_history_echoes_parents_not_a_fabricated_version() {
    let root = tempfile::tempdir().unwrap();
    let store = ReplicaStore::new(root.path().into()).unwrap();
    let mut headers = HeaderMap::new();
    headers.insert("subscribe", HeaderValue::from_static("?1"));
    headers.insert("parents", HeaderValue::from_static("\"missing\""));
    let response = replica_http::get(store, "doc".into(), headers, "simpleton").await;
    assert_eq!(response.status().as_u16(), 432);
    assert_eq!(response.headers()["parents"], "\"missing\"");
    assert!(!response.headers().contains_key("version"));
}
