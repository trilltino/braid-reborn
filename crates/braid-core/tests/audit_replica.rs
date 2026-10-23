use braid_core::{
    merge::{DiamondCRDT, DiamondMergeType, MergePatch, MergeType},
    replica::{ReplicaError, ReplicaStore},
    types::{Patch, Update, Version},
};

#[tokio::test]
async fn committed_history_and_diamond_ops_survive_restart_and_lost_ack() {
    let temp = tempfile::tempdir().unwrap();
    let first = Update::snapshot(Version::new("alice-1"), "a😀b").with_merge_type("simpleton");
    let store = ReplicaStore::new(temp.path().into()).unwrap();
    store.commit("doc", &first, "simpleton").await.unwrap();
    let store = ReplicaStore::new(temp.path().into()).unwrap();
    store.commit("doc", &first, "simpleton").await.unwrap();
    let second = Update::patched(Version::new("alice-2"), vec![Patch::text("[1:2]", "é")])
        .with_parent(Version::new("alice-1"));
    store.commit("doc", &second, "simpleton").await.unwrap();
    store.commit("doc", &first, "simpleton").await.unwrap();
    let doc = store.load("doc").await.unwrap().unwrap();
    assert_eq!(doc.content, "aéb");
    assert_eq!(doc.history.len(), 2);
    let crdt = DiamondCRDT::restore_checkpoint(&doc.merge_state).unwrap();
    assert_eq!(crdt.content(), doc.content);
    assert_eq!(
        doc.replay_since(&["alice-1".into()]).unwrap()[0].version,
        second.version
    );
    let changed = Update::snapshot(Version::new("alice-1"), "different");
    assert!(matches!(
        store.commit("doc", &changed, "simpleton").await,
        Err(ReplicaError::Conflict(_))
    ));
    assert_eq!(store.load("doc").await.unwrap().unwrap().content, "aéb");
}

#[tokio::test]
async fn failed_multi_patch_and_stale_parent_never_commit() {
    let temp = tempfile::tempdir().unwrap();
    let store = ReplicaStore::new(temp.path().into()).unwrap();
    store
        .commit(
            "doc",
            &Update::snapshot(Version::new("a"), "hello"),
            "simpleton",
        )
        .await
        .unwrap();
    let bad = Update::patched(
        Version::new("b"),
        vec![Patch::text("[0:1]", "ok"), Patch::text("[99:100]", "bad")],
    )
    .with_parent(Version::new("a"));
    assert!(store.commit("doc", &bad, "simpleton").await.is_err());
    let missing = Update::snapshot(Version::new("c"), "lost").with_parent(Version::new("missing"));
    assert!(store.commit("doc", &missing, "simpleton").await.is_err());
    let doc = store.load("doc").await.unwrap().unwrap();
    assert_eq!(doc.content, "hello");
    assert_eq!(doc.history.len(), 1);
}

#[tokio::test]
async fn racing_writers_are_serialized_and_one_conflicts() {
    let temp = tempfile::tempdir().unwrap();
    let store = ReplicaStore::new(temp.path().into()).unwrap();
    store
        .commit(
            "doc",
            &Update::snapshot(Version::new("root"), "base"),
            "simpleton",
        )
        .await
        .unwrap();
    let a = Update::snapshot(Version::new("a"), "Alice").with_parent(Version::new("root"));
    let b = Update::snapshot(Version::new("b"), "Bob").with_parent(Version::new("root"));
    let (a, b) = tokio::join!(
        store.commit("doc", &a, "simpleton"),
        store.commit("doc", &b, "simpleton")
    );
    assert_ne!(a.is_ok(), b.is_ok());
    assert_eq!(store.load("doc").await.unwrap().unwrap().history.len(), 2);
}

#[test]
fn diamond_parents_precede_edits_and_unicode_replacement_is_safe() {
    let mut merge = DiamondMergeType::new("alice");
    merge.initialize("😀é");
    let before = merge.get_version();
    let result = merge.local_edit(MergePatch::new("everything", serde_json::json!("你好")));
    assert!(result.success);
    assert_eq!(result.rebased_patches[0].parents, before);
    assert_ne!(
        result.rebased_patches[0].parents[0],
        result.version.unwrap()
    );
    assert_eq!(merge.get_content(), "你好");
    let mut clone = merge.clone();
    clone.initialize("independent");
    assert_eq!(merge.get_content(), "你好");
    let result = merge.apply_patch(MergePatch::with_version(
        "[0:1]",
        serde_json::json!("x"),
        Version::new("remote"),
        vec![Version::new("unknown")],
    ));
    assert!(!result.success);
    assert_eq!(merge.get_content(), "你好");
}

#[tokio::test]
async fn corrupt_storage_is_never_recreated_as_an_empty_document() {
    let temp = tempfile::tempdir().unwrap();
    let store = ReplicaStore::new(temp.path().into()).unwrap();
    tokio::fs::write(store.path("doc"), b"{partial")
        .await
        .unwrap();
    assert!(store.load("doc").await.is_err());
    assert!(store
        .commit(
            "doc",
            &Update::snapshot(Version::new("new"), "overwrite"),
            "simpleton"
        )
        .await
        .is_err());
    assert_eq!(
        tokio::fs::read(store.path("doc")).await.unwrap(),
        b"{partial"
    );
}

#[tokio::test]
async fn blob_events_use_parents_and_idempotency_instead_of_lexical_order() {
    let temp = tempfile::tempdir().unwrap();
    let store =
        braid_core::BlobStore::new(temp.path().join("data"), temp.path().join("meta.sqlite"))
            .await
            .unwrap();
    let first = vec![Version::new("z-99")];
    let second = vec![Version::new("a-1")];
    store
        .put("blob", "first".into(), first.clone(), vec![], None)
        .await
        .unwrap();
    store
        .put("blob", "second".into(), second.clone(), first.clone(), None)
        .await
        .unwrap();
    store
        .put("blob", "first".into(), first.clone(), vec![], None)
        .await
        .unwrap();
    assert_eq!(
        store.get("blob").await.unwrap().unwrap().0,
        bytes::Bytes::from_static(b"second")
    );
    assert!(store
        .put("blob", "collision".into(), first.clone(), vec![], None)
        .await
        .is_err());
    assert!(store
        .put(
            "blob",
            "concurrent".into(),
            vec![Version::new("other")],
            first,
            None
        )
        .await
        .is_err());
    assert_eq!(
        store.get_meta("blob").await.unwrap().unwrap().version,
        second
    );
}

#[tokio::test]
async fn native_http_put_and_subscription_share_the_durable_codec() {
    let temp = tempfile::tempdir().unwrap();
    let store = ReplicaStore::new(temp.path().into()).unwrap();
    let get_store = store.clone();
    let put_store = store.clone();
    let app = axum::Router::new().route(
        "/doc",
        axum::routing::get(move |headers: axum::http::HeaderMap| {
            let store = get_store.clone();
            async move {
                braid_core::server::replica_http::get(store, "doc".into(), headers, "simpleton")
                    .await
            }
        })
        .put(move |headers: axum::http::HeaderMap, body: bytes::Bytes| {
            let store = put_store.clone();
            async move {
                braid_core::server::replica_http::put(
                    store,
                    "doc".into(),
                    headers,
                    body,
                    "simpleton",
                )
                .await
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/doc", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let client = braid_core::BraidClient::new().unwrap();
    let mut subscription = client
        .subscribe(&url, braid_core::BraidRequest::new().subscribe())
        .await
        .unwrap();
    let initial = tokio::time::timeout(std::time::Duration::from_secs(3), subscription.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(initial.body_str(), Some(""));
    assert!(initial.version.is_empty());
    let request = braid_core::BraidRequest::new()
        .with_method("PUT")
        .with_version(Version::new("event-3"))
        .with_parents(vec![])
        .with_merge_type("simpleton")
        .with_patches(vec![Patch::text("[0:0]", "a😀é")]);
    let response = client.fetch(&url, request).await.unwrap();
    assert_eq!(response.status, 200);
    let update = tokio::time::timeout(std::time::Duration::from_secs(3), subscription.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let applied =
        braid_http::protocol::text::apply_text_patches("", update.patches.as_ref().unwrap())
            .unwrap();
    assert_eq!(applied, store.load("doc").await.unwrap().unwrap().content);
    assert_eq!(update.version, vec![Version::new("event-3")]);
    assert_eq!(store.load("doc").await.unwrap().unwrap().content, "a😀é");
    drop(subscription);
    task.abort();
}

#[test]
fn resource_history_tracks_only_successful_validated_operations() {
    let manager = braid_core::server::ResourceStateManager::new();
    manager
        .apply_update("doc", "a😀", "alice", Some("v1"), None)
        .unwrap();
    assert!(manager.get_history("doc", &["v1"]).is_ok());
    assert!(manager
        .apply_update("doc", "collision", "alice", Some("v1"), None)
        .is_err());
    assert!(manager
        .apply_remote_insert_versioned("doc", "bob", &["missing"], 0, "lost", Some("bad"), None)
        .is_err());
    assert!(!manager.has_version("doc", "bad"));
    manager
        .apply_remote_insert_versioned("doc", "bob", &["v1"], 2, "!", Some("v2"), None)
        .unwrap();
    assert!(manager.has_version("doc", "v2"));
    assert_eq!(
        manager.get_resource_state("doc").unwrap()["content"],
        "a😀!"
    );
}

#[test]
fn simpleton_empty_snapshots_clear_content_and_local_patches_name_their_parents() {
    let mut merge = braid_core::merge::SimpletonMergeType::new("alice");
    let first = merge.local_edit(MergePatch::new("everything", serde_json::json!("😀")));
    let before = merge.get_version();
    let second = merge.local_edit(MergePatch::new("everything", serde_json::json!("é")));
    assert!(first.success && second.success);
    assert_eq!(second.rebased_patches[0].parents, before);
    assert_eq!(second.rebased_patches[0].version, second.version);
    merge.initialize("");
    assert_eq!(merge.get_content(), "");
}
