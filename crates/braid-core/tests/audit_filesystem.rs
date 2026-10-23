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

#[tokio::test]
async fn lost_ack_restart_dirty_reconnect_and_failed_materialization_preserve_edits() {
    let temp = tempfile::tempdir().unwrap();
    std::env::set_var("BRAID_ROOT", temp.path());
    let store = ReplicaStore::new(temp.path().join("remote")).unwrap();
    let seed = Update::snapshot(Version::new("seed"), "a😀");
    store.commit("doc", &seed, "simpleton").await.unwrap();
    let attempts = Arc::new(AtomicUsize::new(0));
    let put_store = store.clone();
    let put_attempts = attempts.clone();
    let app = axum::Router::new().route(
        "/doc",
        axum::routing::put(move |headers: axum::http::HeaderMap, body: bytes::Bytes| {
            let store = put_store.clone();
            let attempts = put_attempts.clone();
            async move {
                let response = braid_core::server::replica_http::put(
                    store,
                    "doc".into(),
                    headers,
                    body,
                    "simpleton",
                )
                .await;
                if response.status().is_success() && attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    use axum::response::IntoResponse;
                    axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response()
                } else {
                    response
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/doc", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let mut config = fs::config::Config::default();
    config.sync.insert(url.clone(), true);
    let blob = Arc::new(
        braid_core::BlobStore::new(temp.path().join("blobs"), temp.path().join("meta.sqlite"))
            .await
            .unwrap(),
    );
    let (tx, _) = async_channel::bounded(1);
    let state = DaemonState {
        text_sessions: Default::default(),
        config: Arc::new(RwLock::new(config)),
        content_cache: Default::default(),
        version_store: Arc::new(RwLock::new(
            VersionStore::load_from(temp.path().join("versions.json"))
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
            rusqlite::Connection::open_in_memory().unwrap(),
        )),
        tx_cmd: tx,
        debouncer: Arc::new(fs::debouncer::DebouncedSyncManager::new_placeholder()),
        local_server_managed: Arc::new(RwLock::new(HashSet::new())),
    };
    fs::subscription::materialize_update(&url, &state, &seed)
        .await
        .unwrap();
    let path = fs::mapping::url_to_path(&url).unwrap();
    tokio::fs::write(&path, "a😀 local").await.unwrap();
    // Server commits, then its acknowledgment is lost. The durable request remains.
    assert!(fs::sync::sync_local_to_remote(
        &path,
        &url,
        &[],
        None,
        "a😀 local".into(),
        None,
        state.clone()
    )
    .await
    .is_err());
    let pending = state.version_store.read().await.outbox[&url].clone();
    assert_eq!(store.load("doc").await.unwrap().unwrap().history.len(), 2);
    // A fresh process has no cache; replay uses the saved base, bytes, parents and ID.
    let disk = VersionStore::load_from(temp.path().join("versions.json"))
        .await
        .unwrap();
    *state.version_store.write().await = disk;
    state.content_cache.write().await.clear();
    fs::sync::sync_local_to_remote(
        &path,
        &url,
        &[],
        None,
        "a😀 local".into(),
        None,
        state.clone(),
    )
    .await
    .unwrap();
    assert!(state.version_store.read().await.outbox.is_empty());
    assert_eq!(store.load("doc").await.unwrap().unwrap().history.len(), 2);
    assert_eq!(
        state
            .version_store
            .read()
            .await
            .get(&url)
            .unwrap()
            .current_version,
        vec![pending.version]
    );
    // File replacement can succeed before cursor persistence fails. Replaying the
    // same patch must recognize the already materialized bytes rather than apply twice.
    let cursor_before = state
        .version_store
        .read()
        .await
        .get(&url)
        .unwrap()
        .current_version
        .clone();
    let replay = Update::patched(
        Version::new("replay"),
        vec![braid_core::Patch::text("[1:2]", "?")],
    )
    .with_parents(cursor_before.clone());
    let cursor_path = state.version_store.read().await.path.clone();
    let blocked = temp.path().join("blocked-cursor");
    tokio::fs::create_dir(&blocked).await.unwrap();
    state.version_store.write().await.path = blocked;
    assert!(fs::subscription::materialize_update(&url, &state, &replay)
        .await
        .is_err());
    assert_eq!(
        state
            .version_store
            .read()
            .await
            .get(&url)
            .unwrap()
            .current_version,
        cursor_before
    );
    assert_eq!(tokio::fs::read_to_string(&path).await.unwrap(), "a? local");
    state.version_store.write().await.path = cursor_path;
    fs::subscription::materialize_update(&url, &state, &replay)
        .await
        .unwrap();
    assert_eq!(
        state
            .version_store
            .read()
            .await
            .get(&url)
            .unwrap()
            .current_version,
        replay.version
    );
    // Both authors edit. A reconnect must preserve the unsent local file and cursor.
    tokio::fs::write(&path, "unsent local").await.unwrap();
    let remote = Update::snapshot(Version::new("remote"), "remote edit");
    let before = state
        .version_store
        .read()
        .await
        .get(&url)
        .unwrap()
        .current_version
        .clone();
    assert!(fs::subscription::materialize_update(&url, &state, &remote)
        .await
        .is_err());
    assert_eq!(
        tokio::fs::read_to_string(&path).await.unwrap(),
        "unsent local"
    );
    assert_eq!(
        state
            .version_store
            .read()
            .await
            .get(&url)
            .unwrap()
            .current_version,
        before
    );
    // A disk failure cannot advance the cursor either.
    tokio::fs::remove_file(&path).await.unwrap();
    tokio::fs::create_dir(&path).await.unwrap();
    assert!(fs::subscription::materialize_update(&url, &state, &remote)
        .await
        .is_err());
    assert_eq!(
        state
            .version_store
            .read()
            .await
            .get(&url)
            .unwrap()
            .current_version,
        before
    );
    server.abort();
    std::env::remove_var("BRAID_ROOT");
}
