use braid_core::access::{AccessError, Action, PolicyChange, PolicyStore, Principal, Role};

#[tokio::test]
async fn adoption_is_read_only_until_apply_resumes_and_rolls_back_exact_policies() {
    use braid_core::access::administration::Assignment;
    use sha2::{Digest, Sha256};
    let root = tempfile::tempdir().unwrap();
    let directory = root.path().join("policies");
    let store = PolicyStore::new(directory.clone()).unwrap();
    let payload = root.path().join("original.json");
    tokio::fs::write(&payload, b"legacy bytes remain unchanged")
        .await
        .unwrap();
    let assignments = ["chat/legacy", "wiki/page"]
        .into_iter()
        .map(|resource| Assignment {
            resource: resource.into(),
            owner: Principal::User("alice".into()),
        })
        .collect();
    let plan = store.plan_adoption(assignments).await.unwrap();
    assert!(!directory.exists(), "preparation must not create a store");
    assert_eq!(
        store.inspect_adoption(&plan).await.unwrap().missing.len(),
        2
    );
    assert!(!directory.exists(), "dry-run must remain read-only");
    // The process died after the first policy was durable, before the second.
    let first = &plan.policies[0];
    let path = directory.join(format!(
        "{:x}.json",
        Sha256::digest(first.resource.as_bytes())
    ));
    braid_http::durable::atomic_write(&path, &serde_json::to_vec(first).unwrap())
        .await
        .unwrap();
    assert_eq!(
        store.inspect_adoption(&plan).await.unwrap().applied.len(),
        1
    );
    let serialized = serde_json::to_vec(&plan).unwrap();
    let reopened = PolicyStore::new(directory).unwrap();
    let plan = serde_json::from_slice(&serialized).unwrap();
    assert_eq!(
        reopened.apply_adoption(&plan).await.unwrap().applied.len(),
        2
    );
    assert_eq!(
        reopened.apply_adoption(&plan).await.unwrap().applied.len(),
        2
    );
    assert_eq!(reopened.inventory().await.unwrap().len(), 2);
    assert_eq!(
        reopened
            .rollback_adoption(&plan)
            .await
            .unwrap()
            .missing
            .len(),
        2
    );
    assert_eq!(
        reopened
            .rollback_adoption(&plan)
            .await
            .unwrap()
            .missing
            .len(),
        2
    );
    assert_eq!(
        tokio::fs::read(payload).await.unwrap(),
        b"legacy bytes remain unchanged"
    );
}

#[tokio::test]
async fn rollback_never_removes_a_policy_changed_after_adoption() {
    use braid_core::access::administration::Assignment;
    let root = tempfile::tempdir().unwrap();
    let store = PolicyStore::new(root.path().join("policies")).unwrap();
    let alice = Principal::User("alice".into());
    let plan = store
        .plan_adoption(vec![Assignment {
            resource: "chat/legacy".into(),
            owner: alice.clone(),
        }])
        .await
        .unwrap();
    store.apply_adoption(&plan).await.unwrap();
    store
        .change(
            "chat/legacy",
            &alice,
            PolicyChange::Grant(Principal::User("bob".into()), Role::Reader),
        )
        .await
        .unwrap();
    assert!(store.rollback_adoption(&plan).await.is_err());
    assert!(store.apply_adoption(&plan).await.is_err());
    assert_eq!(store.get("chat/legacy").await.unwrap().unwrap().revision, 1);
    let mut invalid = plan;
    invalid.policies[0].public_read = true;
    assert!(store.inspect_adoption(&invalid).await.is_err());
}

#[tokio::test]
async fn adoption_rollback_is_blocked_after_a_data_writer_starts() {
    use braid_core::{access::administration::Assignment, replica::ReplicaStore, Update, Version};
    let root = tempfile::tempdir().unwrap();
    let store = ReplicaStore::new(root.path().join("replica")).unwrap();
    let policies = PolicyStore::new(root.path().join("policies")).unwrap();
    let actor = Principal::User("alice".into());
    store
        .commit(
            "legacy",
            &Update::snapshot(Version::new("old"), "old"),
            "simpleton",
        )
        .await
        .unwrap();
    let plan = policies
        .plan_adoption(vec![Assignment {
            resource: "legacy".into(),
            owner: actor.clone(),
        }])
        .await
        .unwrap();
    policies.apply_adoption(&plan).await.unwrap();
    store
        .commit_authorized(
            "legacy",
            &Update::snapshot(Version::new("new"), "new").with_parent(Version::new("old")),
            "simpleton",
            &policies,
            &actor,
        )
        .await
        .unwrap();
    assert!(policies.rollback_adoption(&plan).await.is_err());
    assert_eq!(
        policies
            .get("legacy")
            .await
            .unwrap()
            .unwrap()
            .write_generation,
        1
    );
    assert_eq!(store.load("legacy").await.unwrap().unwrap().content, "new");
}

#[tokio::test]
async fn inventory_identifies_ownerless_data_without_guessing_missing_names() {
    use braid_core::{replica::ReplicaStore, Update, Version};
    use sha2::{Digest, Sha256};
    let root = tempfile::tempdir().unwrap();
    let policy_root = root.path().join("policies");
    let policies = PolicyStore::new(policy_root.clone()).unwrap();
    let files = root.path().join("files");
    tokio::fs::create_dir(&files).await.unwrap();
    tokio::fs::write(files.join("page.md"), "original bytes")
        .await
        .unwrap();
    let inventory = policies.scan_data("wiki", &files).await.unwrap();
    assert_eq!(inventory.len(), 1);
    assert_eq!(inventory[0].resource.as_deref(), Some("wiki/page.md"));
    assert!(inventory[0].policy.is_none());
    assert_eq!(
        inventory[0].sha256,
        format!("{:x}", Sha256::digest(b"original bytes"))
    );
    assert!(!policy_root.exists());
    let replica = ReplicaStore::new(root.path().join("replica")).unwrap();
    let mut document = replica
        .commit(
            "doc",
            &Update::snapshot(Version::new("v1"), "content"),
            "simpleton",
        )
        .await
        .unwrap();
    let inventory = policies
        .scan_data("v2", &root.path().join("replica"))
        .await
        .unwrap();
    assert_eq!(inventory[0].resource.as_deref(), Some("v2/doc"));
    document.resource = None;
    tokio::fs::write(replica.path("doc"), serde_json::to_vec(&document).unwrap())
        .await
        .unwrap();
    let inventory = policies
        .scan_data("v2", &root.path().join("replica"))
        .await
        .unwrap();
    assert!(
        inventory[0].resource.is_none(),
        "a hashed filename is not an original resource ID"
    );
}

#[test]
fn local_admin_defaults_to_dry_run_and_reuses_a_saved_plan() {
    let root = tempfile::tempdir().unwrap();
    let policy_root = root.path().join("policies");
    let assignments = root.path().join("assignments.json");
    let plan = root.path().join("plan.json");
    std::fs::write(
        &assignments,
        r#"[{"resource":"chat/legacy","owner":{"kind":"user","id":"alice"}}]"#,
    )
    .unwrap();
    let run = |command: &str, tail: &[&std::ffi::OsStr]| {
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_braid-admin"))
            .arg(command)
            .arg(&policy_root)
            .args(tail)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        output
    };
    run("plan", &[assignments.as_os_str(), plan.as_os_str()]);
    run("apply", &[plan.as_os_str()]);
    assert!(!policy_root.exists());
    run(
        "apply",
        &[plan.as_os_str(), std::ffi::OsStr::new("--apply")],
    );
    let output = run("inventory", &[]);
    assert!(String::from_utf8_lossy(&output.stdout).contains("chat/legacy"));
    run("rollback", &[plan.as_os_str()]);
    let output = run("get", &[std::ffi::OsStr::new("chat/legacy")]);
    assert!(!String::from_utf8_lossy(&output.stdout).contains("null"));
    run(
        "rollback",
        &[plan.as_os_str(), std::ffi::OsStr::new("--apply")],
    );
    let output = run("get", &[std::ffi::OsStr::new("chat/legacy")]);
    assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "null");
}

#[tokio::test]
async fn authorized_replica_rejects_ownerless_claims_and_stages_policy_after_validation() {
    use braid_core::{
        replica::{ReplicaError, ReplicaStore},
        Update, Version,
    };
    let root = tempfile::tempdir().unwrap();
    let policies = PolicyStore::new(root.path().join("policies")).unwrap();
    let store = ReplicaStore::new(root.path().join("replica")).unwrap();
    let alice = Principal::User("alice".into());
    let bob = Principal::User("bob".into());
    store
        .commit(
            "legacy",
            &Update::snapshot(Version::new("old"), "private legacy"),
            "simpleton",
        )
        .await
        .unwrap();
    let forged =
        Update::snapshot(Version::new("claim"), "overwrite").with_parent(Version::new("old"));
    assert!(matches!(
        store
            .commit_authorized("legacy", &forged, "simpleton", &policies, &bob)
            .await,
        Err(ReplicaError::Access(AccessError::Denied))
    ));
    assert!(policies.get("legacy").await.unwrap().is_none());
    assert_eq!(
        store.load("legacy").await.unwrap().unwrap().content,
        "private legacy"
    );
    let invalid = Update {
        version: vec![],
        ..Update::default()
    };
    assert!(store
        .commit_authorized("new", &invalid, "simpleton", &policies, &alice)
        .await
        .is_err());
    assert!(
        policies.get("new").await.unwrap().is_none(),
        "invalid content cannot reserve ownership"
    );
    let first = Update::snapshot(Version::new("first"), "alice's data");
    store
        .commit_authorized("new", &first, "simpleton", &policies, &alice)
        .await
        .unwrap();
    assert_eq!(policies.get("new").await.unwrap().unwrap().owner, alice);
    assert!(
        store
            .commit_authorized("new", &first, "simpleton", &policies, &bob)
            .await
            .is_err(),
        "even an identical retry needs authority"
    );
    // A failed content write leaves a private reservation for recovery, not a
    // successful ACK or an opportunity for another user to claim it.
    tokio::fs::create_dir(store.path("failed")).await.unwrap();
    assert!(store
        .commit_authorized("failed", &first, "simpleton", &policies, &alice)
        .await
        .is_err());
    assert!(
        policies.get("failed").await.unwrap().is_none(),
        "unreadable existing storage fails before reservation"
    );
}

#[tokio::test]
async fn protected_http_views_hide_history_and_revoke_idle_streams() {
    use axum::http::{HeaderMap, HeaderValue};
    use braid_core::{replica::ReplicaStore, server::replica_http, Update, Version};
    use futures::StreamExt;
    let root = tempfile::tempdir().unwrap();
    let policies = PolicyStore::new(root.path().join("policies")).unwrap();
    let store = ReplicaStore::new(root.path().join("replica")).unwrap();
    let alice = Principal::User("alice".into());
    let bob = Principal::User("bob".into());
    store
        .commit_authorized(
            "doc",
            &Update::snapshot(Version::new("seed"), "secret"),
            "simpleton",
            &policies,
            &alice,
        )
        .await
        .unwrap();
    let mut headers = HeaderMap::new();
    headers.insert("version", HeaderValue::from_static("\"unknown\""));
    let denied = replica_http::get_authorized(
        store.clone(),
        policies.clone(),
        Some(bob.clone()),
        "doc".into(),
        headers,
        "simpleton",
    )
    .await;
    assert_eq!(denied.status(), 403);
    assert!(!denied.headers().contains_key("version"));
    policies
        .change(
            "doc",
            &alice,
            PolicyChange::Grant(bob.clone(), Role::Reader),
        )
        .await
        .unwrap();
    let mut headers = HeaderMap::new();
    headers.insert("subscribe", HeaderValue::from_static("?1"));
    let response = replica_http::get_authorized(
        store,
        policies.clone(),
        Some(bob.clone()),
        "doc".into(),
        headers,
        "simpleton",
    )
    .await;
    assert_eq!(response.status(), 209);
    let mut stream = response.into_body().into_data_stream();
    assert!(stream.next().await.unwrap().is_ok());
    policies
        .change("doc", &alice, PolicyChange::Revoke(bob))
        .await
        .unwrap();
    let frame = tokio::time::timeout(std::time::Duration::from_secs(2), stream.next())
        .await
        .unwrap()
        .unwrap();
    assert!(
        frame.is_err(),
        "revocation must terminate an idle stream before its next heartbeat"
    );
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn authenticated_http_put_does_not_write_credentials_to_history() {
    use axum::http::{HeaderMap, HeaderValue};
    use braid_core::{replica::ReplicaStore, server::replica_http};
    let root = tempfile::tempdir().unwrap();
    let store = ReplicaStore::new(root.path().join("replica")).unwrap();
    let policies = store.policy_store().unwrap();
    let mut headers = HeaderMap::new();
    headers.insert("version", HeaderValue::from_static("\"v1\""));
    headers.insert(
        "authorization",
        HeaderValue::from_static("Bearer secret-session-value"),
    );
    headers.insert(
        "cookie",
        HeaderValue::from_static("token=secret-cookie-value"),
    );
    let response = replica_http::put_authorized(
        store.clone(),
        policies,
        Principal::User("alice".into()),
        "doc".into(),
        headers,
        bytes::Bytes::from_static(b"data"),
        "simpleton",
    )
    .await;
    assert_eq!(response.status(), 200);
    let bytes = tokio::fs::read(store.path("doc")).await.unwrap();
    let serialized = String::from_utf8(bytes).unwrap();
    assert!(!serialized.contains("secret-session-value"));
    assert!(!serialized.contains("secret-cookie-value"));
    assert_eq!(store.load("doc").await.unwrap().unwrap().history.len(), 1);
}

#[tokio::test]
async fn policy_is_private_durable_and_only_owners_can_change_it() {
    let root = tempfile::tempdir().unwrap();
    let policies = PolicyStore::new(root.path().join("policies")).unwrap();
    let alice = Principal::User("alice".into());
    let bob = Principal::User("bob".into());
    let peer = Principal::Peer("bob".into());
    assert!(matches!(
        policies
            .authorize("page/doc", Some(&alice), Action::Read)
            .await,
        Err(AccessError::Denied)
    ));
    policies.create("page/doc", alice.clone()).await.unwrap();
    assert!(policies
        .authorize("page/doc", None, Action::Read)
        .await
        .is_err());
    assert!(policies
        .authorize("page/doc", Some(&bob), Action::Write)
        .await
        .is_err());
    policies
        .change(
            "page/doc",
            &alice,
            PolicyChange::Grant(bob.clone(), Role::Reader),
        )
        .await
        .unwrap();
    let reopened = PolicyStore::new(root.path().join("policies")).unwrap();
    reopened
        .authorize("page/doc", Some(&bob), Action::Read)
        .await
        .unwrap();
    assert!(reopened
        .authorize("page/doc", Some(&bob), Action::Write)
        .await
        .is_err());
    assert!(
        reopened
            .authorize("page/doc", Some(&peer), Action::Read)
            .await
            .is_err(),
        "peer/user namespaces must be distinct"
    );
    assert!(matches!(
        reopened
            .change("page/doc", &bob, PolicyChange::PublicRead(true))
            .await,
        Err(AccessError::Denied)
    ));
    policies
        .change(
            "page/doc",
            &alice,
            PolicyChange::Grant(peer.clone(), Role::Editor),
        )
        .await
        .unwrap();
    reopened
        .authorize("page/doc", Some(&peer), Action::Write)
        .await
        .unwrap();
    assert!(reopened
        .authorize("page/doc", Some(&peer), Action::Manage)
        .await
        .is_err());
    policies
        .change("page/doc", &alice, PolicyChange::Revoke(bob.clone()))
        .await
        .unwrap();
    assert!(reopened
        .authorize("page/doc", Some(&bob), Action::Read)
        .await
        .is_err());
    policies
        .change("page/doc", &alice, PolicyChange::PublicRead(true))
        .await
        .unwrap();
    reopened
        .authorize("page/doc", None, Action::Read)
        .await
        .unwrap();
    assert!(reopened
        .authorize("page/doc", None, Action::Write)
        .await
        .is_err());
    policies
        .change("page/doc", &alice, PolicyChange::Transfer(bob.clone()))
        .await
        .unwrap();
    reopened
        .authorize("page/doc", Some(&bob), Action::Manage)
        .await
        .unwrap();
    assert!(reopened
        .authorize("page/doc", Some(&alice), Action::Manage)
        .await
        .is_err());
    assert_eq!(reopened.get("page/doc").await.unwrap().unwrap().revision, 5);
}

#[tokio::test]
async fn simultaneous_adoption_cannot_replace_an_owner() {
    let root = tempfile::tempdir().unwrap();
    let a = PolicyStore::new(root.path().join("policies")).unwrap();
    let b = PolicyStore::new(root.path().join("policies")).unwrap();
    let (a_result, b_result) = tokio::join!(
        a.create("legacy/page", Principal::User("alice".into())),
        b.create("legacy/page", Principal::User("bob".into()))
    );
    assert!(a_result.is_ok() ^ b_result.is_ok());
    assert!(
        matches!(a_result, Err(AccessError::AlreadyOwned))
            || matches!(b_result, Err(AccessError::AlreadyOwned))
    );
}

#[tokio::test]
async fn corrupt_or_future_policy_never_becomes_a_public_default() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("policies");
    let policies = PolicyStore::new(path.clone()).unwrap();
    let policy = policies
        .create("page/doc", Principal::User("alice".into()))
        .await
        .unwrap();
    let file = std::fs::read_dir(&path)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|p| p.extension().is_some_and(|e| e == "json"))
        .unwrap();
    tokio::fs::write(&file, b"{").await.unwrap();
    assert!(matches!(
        policies.get("page/doc").await,
        Err(AccessError::Json(_))
    ));
    let mut future = policy;
    future.schema_version = 999;
    tokio::fs::write(&file, serde_json::to_vec(&future).unwrap())
        .await
        .unwrap();
    assert!(matches!(
        policies.authorize("page/doc", None, Action::Read).await,
        Err(AccessError::Invalid(_))
    ));
}
