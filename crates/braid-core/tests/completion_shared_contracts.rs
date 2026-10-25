//! Shared-contract regressions. Execution is deferred while the user cleans Cargo.
use braid_core::{
    access::{PolicyChange, Principal, Role},
    replica::{HistoryLimits, ReplicaDocument, ReplicaError, ReplicaStore},
    types::{Patch, Update, Version},
};

fn json_event(index: usize) -> Update {
    Update::snapshot(
        Version::new(format!("v{index}")),
        format!(r#"{{"value":{index}}}"#),
    )
    .with_parents(if index == 0 {
        vec![]
    } else {
        vec![Version::new(format!("v{}", index - 1))]
    })
    .with_merge_type("json-snapshot")
    .with_content_type("application/json")
}

#[test]
fn causal_pages_cross_the_finite_get_cap_without_positional_replay() {
    let mut document = ReplicaDocument::empty("json-snapshot");
    for index in 0..1100 {
        document.apply(&json_event(index)).unwrap();
    }
    // Retrieval must derive dependency order instead of trusting file order.
    document.history.reverse();
    let mut from = vec![];
    let mut target = None;
    let mut received = Vec::new();
    loop {
        let page = document
            .history_page(&from, target.as_deref(), HistoryLimits::default())
            .unwrap();
        assert!(page.events.len() <= 128);
        assert!(serde_json::to_vec(&page).unwrap().len() <= 32 * 1024 * 1024);
        if let Some(target) = &target {
            assert_eq!(target, &page.target);
        }
        target = Some(page.target.clone());
        for event in &page.events {
            let index = received.len();
            assert_eq!(event.version, json_event(index).version);
            assert_eq!(event.parents, json_event(index).parents);
            received.push(event.clone());
        }
        assert!(page.complete || page.through != from);
        from = page.through;
        if page.complete {
            break;
        }
    }
    assert_eq!(received.len(), 1100);
    assert_eq!(from, vec![Version::new("v1099")]);
    assert!(matches!(
        document.causal_history(&[], target.as_ref().unwrap()),
        Err(ReplicaError::Limit(_))
    ));
}

#[test]
fn history_pages_reject_missing_events_and_never_emit_an_oversized_singleton() {
    let mut document = ReplicaDocument::empty("json-snapshot");
    document.apply(&json_event(0)).unwrap();
    document.apply(&json_event(1)).unwrap();
    assert!(matches!(
        document.history_page(
            &[],
            None,
            HistoryLimits {
                max_events: 128,
                max_bytes: 1
            }
        ),
        Err(ReplicaError::Limit(_))
    ));
    assert!(matches!(
        document.history_page(
            &[Version::new("v0"), Version::new("v1")],
            None,
            HistoryLimits::default()
        ),
        Err(ReplicaError::Invalid(_))
    ));
    document
        .history
        .retain(|event| event.version != vec![Version::new("v0")]);
    assert!(matches!(
        document.history_page(&[], None, HistoryLimits::default()),
        Err(ReplicaError::HistoryUnavailable(_))
    ));
}

#[tokio::test]
async fn json_snapshots_keep_declared_type_and_exact_historical_frontiers() {
    let directory = tempfile::tempdir().unwrap();
    let store = ReplicaStore::new(directory.path().into()).unwrap();
    store
        .commit("data", &json_event(0), "json-snapshot")
        .await
        .unwrap();
    store
        .commit("data", &json_event(1), "json-snapshot")
        .await
        .unwrap();
    let before = std::fs::read(store.path("data")).unwrap();
    let old = store
        .representation_at("data", Some(&[Version::new("v0")]))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(old.media_type, "application/json");
    assert_eq!(old.frontier, vec![Version::new("v0")]);
    assert_eq!(old.bytes.as_ref(), br#"{"value":0}"#);
    assert!(!store
        .load("data")
        .await
        .unwrap()
        .unwrap()
        .merge_state
        .to_string()
        .contains("diamond"));
    let stale = Update::snapshot(Version::new("stale"), "{}")
        .with_parent(Version::new("v0"))
        .with_content_type("application/json");
    assert!(matches!(
        store.commit("data", &stale, "json-snapshot").await,
        Err(ReplicaError::Conflict(_))
    ));
    let invalid = Update::snapshot(Version::new("bad"), "not JSON")
        .with_parent(Version::new("v1"))
        .with_content_type("application/json");
    assert!(store
        .commit("data", &invalid, "json-snapshot")
        .await
        .is_err());
    assert_eq!(std::fs::read(store.path("data")).unwrap(), before);
    assert!(store
        .commit("data", &json_event(1), "diamond")
        .await
        .is_err());
    let reopened = ReplicaStore::new(directory.path().into()).unwrap();
    assert_eq!(
        reopened
            .representation_at("data", None)
            .await
            .unwrap()
            .unwrap()
            .bytes
            .as_ref(),
        br#"{"value":1}"#
    );
}

#[tokio::test]
async fn linked_json_is_explicit_and_invalid_creation_never_publishes() {
    let directory = tempfile::tempdir().unwrap();
    let store = ReplicaStore::new(directory.path().into()).unwrap();
    let linked = Update::snapshot(
        Version::new("one"),
        r#"{"friend":{"link":"./other"},"unknown":true}"#,
    )
    .with_merge_type("json-snapshot")
    .with_content_type("application/linked+json");
    store
        .commit("links", &linked, "json-snapshot")
        .await
        .unwrap();
    assert_eq!(
        store
            .representation_at("links", None)
            .await
            .unwrap()
            .unwrap()
            .media_type,
        "application/linked+json"
    );
    assert!(store
        .commit(
            "absent",
            &Update::snapshot(Version::new("bad"), "{}"),
            "json-snapshot"
        )
        .await
        .is_err());
    assert!(!store.path("absent").exists());
    store
        .commit(
            "text",
            &Update::snapshot(Version::new("plain"), "{}"),
            "simpleton",
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .representation_at("text", None)
            .await
            .unwrap()
            .unwrap()
            .media_type,
        "text/plain; charset=utf-8"
    );
}

#[tokio::test]
async fn causal_ingestion_uses_real_authority_and_preserves_original_event_metadata() {
    let directory = tempfile::tempdir().unwrap();
    let store = ReplicaStore::new(directory.path().into()).unwrap();
    let policies = store.policy_store().unwrap();
    let owner = Principal::Peer("alice".into());
    let writer = Principal::Peer("bob".into());
    store
        .commit_causal_authorized(
            "doc",
            &Update::snapshot(Version::new("seed"), "ab"),
            "simpleton",
            &policies,
            &owner,
        )
        .await
        .unwrap();
    policies
        .change(
            "doc",
            &owner,
            PolicyChange::Grant(writer.clone(), Role::Editor),
        )
        .await
        .unwrap();
    let left = Update::patched(Version::new("left"), vec![Patch::new("text", "[0:0]", "X")])
        .with_parent(Version::new("seed"));
    let right = Update::patched(
        Version::new("right"),
        vec![Patch::new("text", "[2:2]", "Y")],
    )
    .with_parent(Version::new("seed"));
    store
        .commit_causal_authorized("doc", &left, "simpleton", &policies, &owner)
        .await
        .unwrap();
    let document = store
        .commit_causal_authorized("doc", &right, "simpleton", &policies, &writer)
        .await
        .unwrap();
    assert_eq!(document.content, "XabY");
    assert_eq!(document.heads, vec!["left", "right"]);
    let page = store
        .history_page_authorized(
            "doc",
            &[],
            None,
            HistoryLimits::default(),
            &policies,
            Some(&writer),
        )
        .await
        .unwrap()
        .unwrap();
    assert!(page.complete);
    assert!(page
        .events
        .iter()
        .all(|event| !event.extra_headers.contains_key("peer")));
    assert_eq!(
        serde_json::to_value(&page.events[2]).unwrap(),
        serde_json::to_value(right).unwrap()
    );
    let stranger = Principal::User("stranger".into());
    assert!(matches!(
        store
            .history_page_authorized(
                "doc",
                &[],
                None,
                HistoryLimits::default(),
                &policies,
                Some(&stranger)
            )
            .await,
        Err(ReplicaError::Access(_))
    ));
    assert!(matches!(
        store
            .representation_at_authorized(
                "doc",
                Some(&[Version::new("hidden")]),
                &policies,
                Some(&stranger)
            )
            .await,
        Err(ReplicaError::Access(_))
    ));
}

#[tokio::test]
async fn reserved_replication_namespace_blocks_new_resources_and_reports_legacy_collisions() {
    use braid_core::replica::{REPLICATION_HISTORY_PATH, REPLICATION_NAMESPACE};
    let directory = tempfile::tempdir().unwrap();
    let store = ReplicaStore::new(directory.path().into()).unwrap();
    assert!(store
        .replication_namespace_collisions()
        .await
        .unwrap()
        .is_empty());
    // New resources cannot be created inside the reserved namespace.
    let attempt = Update::snapshot(Version::new("x"), "x");
    assert!(matches!(
        store
            .commit(REPLICATION_HISTORY_PATH, &attempt, "simpleton")
            .await,
        Err(ReplicaError::Invalid(_))
    ));
    assert!(!store.path(REPLICATION_HISTORY_PATH).exists());

    // Pre-existing data is reported, never modified: a resource document, an
    // ownership policy, and a legacy sanitized file name.
    let named = format!("{REPLICATION_NAMESPACE}v1/legacy");
    let document = serde_json::json!({
        "resource": named, "content": "old", "heads": ["a"], "version_graph": {"a": []},
        "merge_type": "simpleton", "merge_state": null, "created_at": 0, "modified_at": 0
    });
    std::fs::write(store.path(&named), serde_json::to_vec(&document).unwrap()).unwrap();
    std::fs::write(
        directory
            .path()
            .join("_.well-known_braid_replication_v1_other.json"),
        b"{}",
    )
    .unwrap();
    let policies = store.policy_store().unwrap();
    let owner = Principal::Peer("alice".into());
    let policy_resource = format!("{REPLICATION_NAMESPACE}v1/private");
    policies
        .create(&policy_resource, owner.clone())
        .await
        .unwrap();
    let before = std::fs::read(store.path(&named)).unwrap();
    let collisions = store.replication_namespace_collisions().await.unwrap();
    assert_eq!(collisions.len(), 3, "{collisions:?}");
    assert!(collisions.contains(&named));
    assert!(collisions.contains(&policy_resource));
    assert_eq!(std::fs::read(store.path(&named)).unwrap(), before);
    // Existing legacy data stays writable; only new creation is refused.
    store.load(&named).await.unwrap().unwrap();
}
