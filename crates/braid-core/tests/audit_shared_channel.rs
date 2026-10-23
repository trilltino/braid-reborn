use braid_core::{
    fs::{self, state::DaemonState, versions::VersionStore},
    replica::ReplicaStore,
    types::{Update, Version},
};
use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::sync::RwLock;

async fn state(root: &std::path::Path, url: &str) -> DaemonState {
    std::fs::create_dir_all(root.join(".braidfs")).unwrap();
    let mut config = fs::config::Config::default();
    config.sync.insert(url.to_owned(), true);
    let blob = Arc::new(
        braid_core::BlobStore::new(root.join("blobs"), root.join("meta.sqlite"))
            .await
            .unwrap(),
    );
    let (tx, _) = async_channel::bounded(1);
    DaemonState {
        text_sessions: Default::default(),
        config: Arc::new(RwLock::new(config)),
        content_cache: Default::default(),
        version_store: Arc::new(RwLock::new(
            VersionStore::load_from(root.join("versions.json"))
                .await
                .unwrap(),
        )),
        tracker: fs::ActivityTracker::new(),
        merge_registry: Arc::new(braid_core::merge::MergeTypeRegistry::new()),
        active_merges: Default::default(),
        pending: fs::PendingWrites::new(),
        client: braid_core::BraidClient::with_config(braid_core::ClientConfig {
            max_retries: 0,
            ..Default::default()
        })
        .unwrap(),
        failed_syncs: Default::default(),
        binary_sync: Arc::new(
            fs::binary_sync::BinarySyncManager::new(
                Arc::new(fs::rate_limiter::ReconnectRateLimiter::new(1)),
                blob,
            )
            .unwrap(),
        ),
        inode_db: Arc::new(parking_lot::Mutex::new(
            rusqlite::Connection::open(root.join(".braidfs/inodes.sqlite")).unwrap(),
        )),
        tx_cmd: tx,
        debouncer: Arc::new(fs::debouncer::DebouncedSyncManager::new_placeholder()),
        local_server_managed: Arc::new(RwLock::new(HashSet::new())),
    }
}

#[tokio::test]
async fn shared_text_owner_recovers_lost_ack_without_independent_puts() {
    let temp = tempfile::tempdir().unwrap();
    std::env::set_var("BRAID_ROOT", temp.path());
    let store = ReplicaStore::new(temp.path().join("remote")).unwrap();
    let seed = Update::snapshot(Version::new("seed"), "base");
    store.commit("doc", &seed, "simpleton").await.unwrap();
    let attempts = Arc::new(AtomicUsize::new(0));
    let put_store = store.clone();
    let put_attempts = attempts.clone();
    let get_store = store.clone();
    let partition = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let put_partition = partition.clone();
    let rename_attempts = Arc::new(AtomicUsize::new(0));
    let rename_counter = rename_attempts.clone();
    let app = axum::Router::new().route(
        "/{name}",
        axum::routing::get(
            move |axum::extract::Path(name): axum::extract::Path<String>,
                  headers: axum::http::HeaderMap| {
                let store = get_store.clone();
                async move {
                    braid_core::server::replica_http::get(store, name, headers, "simpleton").await
                }
            },
        )
        .put(
            move |axum::extract::Path(name): axum::extract::Path<String>,
                  headers: axum::http::HeaderMap,
                  body: bytes::Bytes| {
                let store = put_store.clone();
                let attempts = put_attempts.clone();
                let partition = put_partition.clone();
                let rename_counter = rename_counter.clone();
                async move {
                    use axum::response::IntoResponse;
                    if partition.load(Ordering::SeqCst) {
                        return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response();
                    }
                    let is_rename = name == "renamed";
                    let response = braid_core::server::replica_http::put(
                        store,
                        name,
                        headers,
                        body,
                        "simpleton",
                    )
                    .await;
                    if response.status().is_success()
                        && (attempts.fetch_add(1, Ordering::SeqCst) == 0
                            || (is_rename && rename_counter.fetch_add(1, Ordering::SeqCst) == 0))
                    {
                        use axum::response::IntoResponse;
                        axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response()
                    } else {
                        response
                    }
                }
            },
        ),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/doc", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let state = state(temp.path(), &url).await;
    fs::subscription::materialize_update(&url, &state, &seed)
        .await
        .unwrap();
    let path = fs::mapping::url_to_path(&url).unwrap();
    tokio::fs::write(&path, "base local").await.unwrap();
    let subscribe_url = url.clone();
    let subscribe_state = state.clone();
    let subscription = tokio::spawn(async move {
        fs::subscription::subscribe_loop(subscribe_url, subscribe_state).await
    });
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let session = state.text_sessions.lock().await.get(&url).cloned();
            if let Some(session) = session {
                let current = session.status().borrow().clone();
                assert!(current.error.is_none(), "{:?}", current.error);
                if current.initialized
                    && current.text == "base local"
                    && current.outstanding_puts == 0
                    && !current.has_pending_edit
                {
                    break;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let document = store.load("doc").await.unwrap().unwrap();
    assert_eq!(document.history.len(), 2);
    assert!(attempts.load(Ordering::SeqCst) >= 2);
    assert_eq!(
        tokio::fs::read_to_string(&path).await.unwrap(),
        "base local"
    );
    assert!(state.version_store.read().await.outbox.is_empty());
    // The same owner receives the next edit and persists a recovery record before
    // materialization. Restoring a session never needs to invent its parent version.
    let parents = store
        .load("doc")
        .await
        .unwrap()
        .unwrap()
        .heads
        .into_iter()
        .map(Version::new)
        .collect();
    store
        .commit(
            "doc",
            &Update::patched(
                Version::new("remote-edit"),
                vec![braid_core::Patch::text("[10:10]", " remote")],
            )
            .with_parents(parents),
            "simpleton",
        )
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if tokio::fs::read_to_string(&path).await.unwrap() == "base local remote" {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let current = state.text_sessions.lock().await.get(&url).unwrap().clone();
    current
        .changed("base local remote second".into())
        .await
        .unwrap();
    let mut status = current.status();
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let value = status.borrow_and_update().clone();
            if value.outstanding_puts == 0 && !value.has_pending_edit {
                break;
            }
            status.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    current.shutdown().await;
    drop(current);
    let _ = subscription.await;
    assert!(state.text_sessions.lock().await.is_empty());
    let journal_path = temp.path().join(".braidfs/channels").join(format!(
        "{:x}.json",
        <sha2::Sha256 as sha2::Digest>::digest(url.as_bytes())
    ));
    let journal = braid_http::client::ChannelJournal::open(journal_path)
        .await
        .unwrap();
    assert!(journal.checkpoint().outbox.is_empty());
    assert_eq!(
        journal.checkpoint().application.as_ref().unwrap()["text"],
        "base local remote second"
    );
    drop(journal);
    drop(state);
    #[cfg(feature = "nfs")]
    nfs_namespace_recovery(temp.path(), &url, store.clone(), partition, rename_attempts).await;
    server.abort();
    std::env::remove_var("BRAID_ROOT");
}

#[cfg(feature = "nfs")]
async fn nfs_namespace_recovery(
    root: &std::path::Path,
    url: &str,
    remote: ReplicaStore,
    partition: Arc<std::sync::atomic::AtomicBool>,
    rename_attempts: Arc<AtomicUsize>,
) {
    use nfsserve::{nfs::nfsstring, vfs::NFSFileSystem};
    let state_before = state(root, url).await;
    let backend = fs::nfs::BraidNfsBackend::with_root(
        state_before.clone(),
        state_before.binary_sync.blob_store(),
        root.to_owned(),
    )
    .unwrap();
    let path = fs::mapping::url_to_path(url).unwrap();
    let original = tokio::fs::read_to_string(&path).await.unwrap();
    let domain = path
        .parent()
        .unwrap()
        .file_name()
        .unwrap()
        .to_str()
        .unwrap();
    let directory = backend
        .lookup(1, &nfsstring(domain.as_bytes().to_vec()))
        .await
        .unwrap();
    let file = backend
        .lookup(directory, &nfsstring(b"doc".to_vec()))
        .await
        .unwrap();
    backend
        .rename(
            directory,
            &nfsstring(b"doc".to_vec()),
            directory,
            &nfsstring(b"renamed".to_vec()),
        )
        .await
        .unwrap();
    let destination = path.with_file_name("renamed");
    assert!(!path.exists());
    backend
        .write(file, original.len() as u64, b" extra")
        .await
        .unwrap();
    partition.store(true, Ordering::SeqCst);
    let interrupted = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        fs::operations::reconcile_next(&state_before),
    )
    .await;
    assert!(interrupted.is_err(), "{interrupted:?}");
    let destination_url = url.replace("/doc", "/renamed");
    let journal_path = root.join(".braidfs/channels").join(format!(
        "{:x}.json",
        <sha2::Sha256 as sha2::Digest>::digest(destination_url.as_bytes())
    ));
    let queued: serde_json::Value =
        serde_json::from_slice(&tokio::fs::read(&journal_path).await.unwrap()).unwrap();
    assert_eq!(queued["outbox"].as_array().unwrap().len(), 1);
    let identity = queued["outbox"][0]["version"].clone();
    let sessions = std::mem::take(&mut *state_before.text_sessions.lock().await);
    for (_, session) in sessions {
        session.shutdown().await;
    }
    drop(backend);
    drop(state_before);
    let recovered = state(root, url).await;
    let backend = fs::nfs::BraidNfsBackend::with_root(
        recovered.clone(),
        recovered.binary_sync.blob_store(),
        root.to_owned(),
    )
    .unwrap();
    let queued_again: serde_json::Value =
        serde_json::from_slice(&tokio::fs::read(&journal_path).await.unwrap()).unwrap();
    assert_eq!(queued_again["outbox"][0]["version"], identity);
    partition.store(false, Ordering::SeqCst);
    assert!(tokio::time::timeout(
        std::time::Duration::from_secs(20),
        fs::operations::reconcile_next(&recovered)
    )
    .await
    .unwrap()
    .unwrap());
    let session = recovered
        .text_sessions
        .lock()
        .await
        .get(&destination_url)
        .unwrap()
        .clone();
    let mut status = session.status();
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let current = status.borrow_and_update().clone();
            assert!(current.error.is_none(), "{:?}", current.error);
            if !current.has_pending_edit && current.outstanding_puts == 0 {
                break;
            }
            status.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    let expected = format!("{original} extra");
    assert_eq!(
        remote.load("renamed").await.unwrap().unwrap().content,
        expected
    );
    assert_eq!(remote.load("doc").await.unwrap().unwrap().content, "");
    assert_eq!(
        tokio::fs::read_to_string(&destination).await.unwrap(),
        expected
    );
    assert!(!path.exists());
    assert!(rename_attempts.load(Ordering::SeqCst) >= 2);
    session.changed_for_operation(1, original).await.unwrap();
    assert_eq!(
        session.status().borrow().text,
        expected,
        "Replaying a namespace intention must not undo a later edit"
    );
    let revision = session.status().borrow().revision;
    assert!(session
        .changed_if_revision(revision.saturating_sub(1), "stale decision".into())
        .await
        .is_err());
    let state_value: String = recovered
        .inode_db
        .lock()
        .query_row("SELECT state FROM nfs_changes WHERE id=1", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(state_value, "remote_confirmed");
    assert!(root.join(".braidfs/nfs-ops/1/0.bin").exists());
    backend
        .remove(directory, &nfsstring(b"renamed".to_vec()))
        .await
        .unwrap();
    assert!(fs::operations::reconcile_next(&recovered).await.unwrap());
    assert_eq!(remote.load("renamed").await.unwrap().unwrap().content, "");
    assert!(!destination.exists());
    let sessions = std::mem::take(&mut *recovered.text_sessions.lock().await);
    for (_, session) in sessions {
        session.shutdown().await;
    }
}
