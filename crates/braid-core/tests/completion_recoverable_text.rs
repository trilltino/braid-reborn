//! Recoverable text state: verified base, application-owned acknowledgments, legacy
//! migration with backup, and pending-preserving repair. A controllable fake server
//! lets each test decide exactly which frames and PUT outcomes the client sees.
use axum::{
    body::Body,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
};
use base64::Engine;
use braid_core::{
    simpleton::{RecoveryState, SimpletonClient},
    BraidClient, Patch, Update, Version,
};
use braid_http::{client::ChannelJournal, protocol};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicU16, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::mpsc;

fn digest(text: &str) -> String {
    format!(
        "sha-256=:{}:",
        base64::engine::general_purpose::STANDARD.encode(Sha256::digest(text.as_bytes()))
    )
}
fn snapshot(version: &str, text: &str) -> Update {
    Update::snapshot(Version::new(version), text.to_owned())
        .with_header("repr-digest", digest(text))
}
fn wrong_digest_patch(version: &str, parent: &str, at: usize, text: &str) -> Update {
    Update::patched(
        Version::new(version),
        vec![Patch::new("text", format!("[{at}:{at}]"), text.to_owned())],
    )
    .with_parents(vec![Version::new(parent)])
    .with_header("repr-digest", digest("this is not the resulting text"))
}

struct Fake {
    streams: Mutex<VecDeque<mpsc::UnboundedReceiver<Update>>>,
    put_status: AtomicU16,
    puts: Mutex<Vec<(String, Vec<u8>)>>,
    history: Mutex<HashMap<String, String>>,
    subscriptions: Mutex<Vec<HeaderMap>>,
    /// Senders for connections nobody scripted; kept open so reconnects idle quietly.
    idle: Mutex<Vec<mpsc::UnboundedSender<Update>>>,
}
impl Fake {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            streams: Default::default(),
            put_status: AtomicU16::new(200),
            puts: Default::default(),
            history: Default::default(),
            subscriptions: Default::default(),
            idle: Default::default(),
        })
    }
    fn stream(&self) -> mpsc::UnboundedSender<Update> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.streams.lock().unwrap().push_back(rx);
        tx
    }
}
async fn serve(fake: Arc<Fake>) -> String {
    let reader = fake.clone();
    let writer = fake;
    let app = axum::Router::new().route(
        "/doc",
        get(move |headers: HeaderMap| {
            let fake = reader.clone();
            async move {
                if headers.contains_key("subscribe") {
                    fake.subscriptions.lock().unwrap().push(headers);
                    let scripted = fake.streams.lock().unwrap().pop_front();
                    let mut rx = match scripted {
                        Some(rx) => rx,
                        None => {
                            let (tx, rx) = mpsc::unbounded_channel();
                            fake.idle.lock().unwrap().push(tx);
                            rx
                        }
                    };
                    let stream = async_stream::stream! {
                        while let Some(update) = rx.recv().await {
                            yield Ok::<_, std::io::Error>(protocol::format_update(&update).unwrap());
                        }
                    };
                    Response::builder()
                        .status(209)
                        .header("subscribe", "true")
                        .header("merge-type", "simpleton")
                        .body(Body::from_stream(stream))
                        .unwrap()
                } else {
                    let fields = protocol::headers_from_http(&headers).unwrap();
                    let key = fields.get("version").cloned().unwrap_or_default();
                    match fake.history.lock().unwrap().get(&key) {
                        Some(text) => (StatusCode::OK, text.clone()).into_response(),
                        None => StatusCode::from_u16(432).unwrap().into_response(),
                    }
                }
            }
        })
        .put(move |headers: HeaderMap, body: bytes::Bytes| {
            let fake = writer.clone();
            async move {
                let version = headers
                    .get("version")
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or_default()
                    .to_owned();
                fake.puts.lock().unwrap().push((version, body.to_vec()));
                // Status 0 holds the request in flight, leaving the subscription
                // undisturbed, until the test chooses an outcome.
                loop {
                    match fake.put_status.load(Ordering::SeqCst) {
                        0 => tokio::time::sleep(Duration::from_millis(10)).await,
                        status => break StatusCode::from_u16(status).unwrap(),
                    }
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/doc", listener.local_addr().unwrap());
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    url
}
async fn wait_for(mut predicate: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(15), async {
        while !predicate() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("condition was not reached");
}
fn journal_json(path: &std::path::Path) -> serde_json::Value {
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}
fn peer_version(fake: &Fake, index: usize) -> String {
    let header = fake.puts.lock().unwrap()[index].0.clone();
    protocol::parse_version_header(&header).unwrap()[0].to_string()
}

#[tokio::test]
async fn acknowledged_edits_advance_the_verified_base_with_their_own_digest() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("journal.json");
    let fake = Fake::new();
    let url = serve(fake.clone()).await;
    let server = fake.stream();
    server.send(snapshot("v1", "hello")).unwrap();
    let client = SimpletonClient::connect(
        &BraidClient::new().unwrap(),
        &url,
        ChannelJournal::open(&path).await.unwrap(),
    )
    .await
    .unwrap();
    let status = client.status();
    wait_for(|| status.borrow().initialized).await;
    assert_eq!(status.borrow().recovery, RecoveryState::Verified);
    let saved = journal_json(&path);
    assert_eq!(saved["application"]["base"]["text"], "hello");
    assert_eq!(saved["application"]["schema"], 2);

    client.changed("hello!".into()).await.unwrap();
    wait_for(|| {
        let saved = journal_json(&path);
        saved["application"]["base"]["text"] == "hello!"
            && saved["outbox"].as_array().is_some_and(Vec::is_empty)
    })
    .await;
    let saved = journal_json(&path);
    let id = peer_version(&fake, 0);
    assert_eq!(saved["application"]["base"]["text"], "hello!");
    assert_eq!(
        saved["application"]["base"]["frontier"],
        serde_json::json!([id])
    );
    assert_eq!(saved["application"]["base"]["digest"], digest("hello!"));
    assert_eq!(saved["outbox"], serde_json::json!([]));
    client.close().await;
}

#[tokio::test]
async fn an_unacknowledged_edit_keeps_its_base_and_replays_identically_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("journal.json");
    let fake = Fake::new();
    let url = serve(fake.clone()).await;
    fake.put_status.store(0, Ordering::SeqCst);
    let first = fake.stream();
    first.send(snapshot("v1", "hello")).unwrap();
    let client = SimpletonClient::connect(
        &BraidClient::new().unwrap(),
        &url,
        ChannelJournal::open(&path).await.unwrap(),
    )
    .await
    .unwrap();
    let status = client.status();
    wait_for(|| status.borrow().initialized).await;
    client.changed("hello!".into()).await.unwrap();
    wait_for(|| !fake.puts.lock().unwrap().is_empty()).await;
    client.close().await;

    let saved = journal_json(&path);
    assert_eq!(
        saved["application"]["base"]["text"], "hello",
        "base is the acknowledged state"
    );
    assert_eq!(
        saved["application"]["text"], "hello!",
        "optimistic text is separate"
    );
    assert_eq!(saved["outbox"].as_array().unwrap().len(), 1);

    // The server finally accepts; the restarted client replays the same identity.
    fake.put_status.store(200, Ordering::SeqCst);
    let second = fake.stream();
    second.send(snapshot("v1", "hello")).unwrap();
    let client = SimpletonClient::connect(
        &BraidClient::new().unwrap(),
        &url,
        ChannelJournal::open(&path).await.unwrap(),
    )
    .await
    .unwrap();
    wait_for(|| {
        fake.puts.lock().unwrap().len() >= 2
            && journal_json(&path)["outbox"]
                .as_array()
                .is_some_and(Vec::is_empty)
    })
    .await;
    {
        let puts = fake.puts.lock().unwrap();
        let identities: std::collections::HashSet<_> = puts
            .iter()
            .map(|(version, body)| (version.clone(), body.clone()))
            .collect();
        assert_eq!(
            identities.len(),
            1,
            "every attempt is the same immutable event"
        );
    }
    assert_eq!(journal_json(&path)["application"]["base"]["text"], "hello!");
    client.close().await;
}

fn legacy_state(text: &str, version: &str, pending: Option<&str>) -> serde_json::Value {
    serde_json::json!({
        "peer": "legacy-peer", "version": [version], "text": text, "pending": pending,
        "char_counter": 0, "initialized": true, "last_operation": 0, "revision": 3
    })
}

#[tokio::test]
async fn legacy_journal_without_a_queue_gets_a_verified_base_and_a_byte_exact_backup() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("journal.json");
    let original = serde_json::to_vec(&serde_json::json!({
        "resource_url": null, "receive_frontier": ["v7"], "local_frontier": ["v7"],
        "outbox": [], "application": legacy_state("legacy text", "v7", Some("legacy text edited"))
    }))
    .unwrap();
    std::fs::write(&path, &original).unwrap();
    let fake = Fake::new();
    let url = serve(fake.clone()).await;
    let server = fake.stream();
    let client = SimpletonClient::connect(
        &BraidClient::new().unwrap(),
        &url,
        ChannelJournal::open(&path).await.unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(client.status().borrow().recovery, RecoveryState::Verified);
    assert_eq!(
        client.status().borrow().text,
        "legacy text edited",
        "pending intent preserved"
    );
    let saved = journal_json(&path);
    assert_eq!(saved["application"]["base"]["text"], "legacy text");
    assert_eq!(
        saved["application"]["base"]["frontier"],
        serde_json::json!(["v7"])
    );
    assert_eq!(saved["application"]["pending"], "legacy text edited");
    assert_eq!(
        saved["application"]["peer"], "legacy-peer",
        "identity is unchanged"
    );
    let backup = dir.path().join("journal.json.pre-recovery-v2.bak");
    assert_eq!(std::fs::read(&backup).unwrap(), original);
    client.close().await;
    drop(server);
}

fn legacy_queued_journal(path: &std::path::Path) -> Vec<u8> {
    let edit = Update::patched(
        Version::new("legacy-peer-0"),
        vec![Patch::new("text", "[2:2]", "c")],
    )
    .with_parents(vec![Version::new("seed")])
    .with_merge_type("simpleton")
    .with_content_type("text/plain")
    .with_header("repr-digest", digest("abc"));
    let bytes = serde_json::to_vec(&serde_json::json!({
        "resource_url": null, "receive_frontier": ["seed"], "local_frontier": ["legacy-peer-0"],
        "outbox": [edit], "application": legacy_state("abc", "legacy-peer-0", None)
    }))
    .unwrap();
    std::fs::write(path, &bytes).unwrap();
    bytes
}

#[tokio::test]
async fn legacy_queue_is_recovered_by_materializing_its_frontier_and_replaying_unchanged_edits() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("journal.json");
    let original = legacy_queued_journal(&path);
    let fake = Fake::new();
    let url = serve(fake.clone()).await;
    fake.history
        .lock()
        .unwrap()
        .insert("\"seed\"".into(), "ab".into());
    fake.put_status.store(0, Ordering::SeqCst);
    let server = fake.stream();
    let client = SimpletonClient::connect(
        &BraidClient::new().unwrap(),
        &url,
        ChannelJournal::open(&path).await.unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        client.status().borrow().recovery,
        RecoveryState::NeedsLegacyBaseline
    );
    let status = client.status();
    wait_for(|| status.borrow().recovery == RecoveryState::Verified).await;
    let saved = journal_json(&path);
    assert_eq!(saved["application"]["base"]["text"], "ab");
    assert_eq!(
        saved["application"]["base"]["frontier"],
        serde_json::json!(["seed"])
    );
    assert_eq!(
        saved["outbox"].as_array().unwrap().len(),
        1,
        "queued operation is untouched"
    );
    assert_eq!(
        saved["outbox"][0]["version"],
        serde_json::json!(["legacy-peer-0"])
    );
    assert_eq!(
        std::fs::read(dir.path().join("journal.json.pre-recovery-v2.bak")).unwrap(),
        original
    );
    // Once the server accepts the unchanged PUT the base advances through the edit.
    fake.put_status.store(200, Ordering::SeqCst);
    wait_for(|| {
        let saved = journal_json(&path);
        saved["application"]["base"]["text"] == "abc"
            && saved["outbox"].as_array().is_some_and(Vec::is_empty)
    })
    .await;
    client.close().await;
    drop(server);
}

#[tokio::test]
async fn unverifiable_legacy_baseline_is_reported_and_the_original_is_retained() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("journal.json");
    let original = legacy_queued_journal(&path);
    let fake = Fake::new();
    let url = serve(fake.clone()).await;
    // The server cannot materialize the recorded frontier (432), and it never accepts the PUT.
    fake.put_status.store(0, Ordering::SeqCst);
    let server = fake.stream();
    let client = SimpletonClient::connect(
        &BraidClient::new().unwrap(),
        &url,
        ChannelJournal::open(&path).await.unwrap(),
    )
    .await
    .unwrap();
    let status = client.status();
    wait_for(|| status.borrow().online).await;
    wait_for(|| !fake.puts.lock().unwrap().is_empty()).await;
    assert_eq!(status.borrow().recovery, RecoveryState::NeedsLegacyBaseline);
    assert!(journal_json(&path)["application"]["base"].is_null());
    assert_eq!(
        journal_json(&path)["application"]["schema"],
        0,
        "still marked legacy"
    );
    assert_eq!(
        std::fs::read(dir.path().join("journal.json.pre-recovery-v2.bak")).unwrap(),
        original
    );
    client.close().await;
    drop(server);
}

#[tokio::test]
async fn digest_failure_on_the_first_snapshot_repairs_without_overwriting_pending_text() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("journal.json");
    let fake = Fake::new();
    let url = serve(fake.clone()).await;
    let bad = fake.stream();
    let good = fake.stream();
    // An offline draft exists before any server state is known.
    let client = SimpletonClient::connect(
        &BraidClient::new().unwrap(),
        &url,
        ChannelJournal::open(&path).await.unwrap(),
    )
    .await
    .unwrap();
    client.changed("local draft".into()).await.unwrap();
    bad.send(
        Update::snapshot(Version::new("v1"), "server text")
            .with_header("repr-digest", digest("not it")),
    )
    .unwrap();
    wait_for(|| fake.subscriptions.lock().unwrap().len() >= 2).await;
    let status = client.status();
    assert!(
        status.borrow().error.is_none(),
        "{:?}",
        status.borrow().error
    );
    assert!(
        status.borrow().has_pending_edit
            || !status.borrow().initialized
            || status.borrow().text == "local draft"
    );
    good.send(snapshot("v1", "server text")).unwrap();
    // The draft is sent as an edit over the verified snapshot rather than replaced by it.
    wait_for(|| !fake.puts.lock().unwrap().is_empty()).await;
    wait_for(|| {
        let saved = journal_json(&path);
        saved["application"]["base"]["text"] == "local draft"
            && saved["outbox"].as_array().is_some_and(Vec::is_empty)
    })
    .await;
    let saved = journal_json(&path);
    assert_eq!(saved["application"]["text"], "local draft");
    assert_eq!(saved["application"]["base"]["text"], "local draft");
    client.close().await;
}

#[tokio::test]
async fn digest_failure_with_queued_edits_rebuilds_from_the_base_and_keeps_every_edit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("journal.json");
    let fake = Fake::new();
    let url = serve(fake.clone()).await;
    fake.put_status.store(0, Ordering::SeqCst);
    let first = fake.stream();
    let second = fake.stream();
    first.send(snapshot("v1", "hello")).unwrap();
    let client = SimpletonClient::connect(
        &BraidClient::new().unwrap(),
        &url,
        ChannelJournal::open(&path).await.unwrap(),
    )
    .await
    .unwrap();
    let status = client.status();
    wait_for(|| status.borrow().initialized).await;
    client.changed("hello!".into()).await.unwrap();
    wait_for(|| !fake.puts.lock().unwrap().is_empty()).await;
    let id = peer_version(&fake, 0);
    let before = fake.puts.lock().unwrap()[0].clone();
    // A server frame on top of the queued edit that does not match what the client computes.
    first.send(wrong_digest_patch("v2", &id, 6, "?")).unwrap();
    wait_for(|| fake.subscriptions.lock().unwrap().len() >= 2).await;
    assert!(
        status.borrow().error.is_none(),
        "{:?}",
        status.borrow().error
    );
    assert_eq!(
        status.borrow().text,
        "hello!",
        "text rebuilt from base plus queued edit"
    );
    assert_eq!(
        status.borrow().outstanding_puts,
        1,
        "queued edit survives repair"
    );
    let saved = journal_json(&path);
    assert_eq!(saved["application"]["base"]["text"], "hello");
    // The resumed subscription starts from the base frontier, not the optimistic one.
    let resumed = fake.subscriptions.lock().unwrap()[1].clone();
    assert_eq!(resumed.get("parents").unwrap().to_str().unwrap(), "\"v1\"");
    // Acceptance later still uses the identical event.
    fake.put_status.store(200, Ordering::SeqCst);
    wait_for(|| {
        let saved = journal_json(&path);
        saved["application"]["base"]["text"] == "hello!"
            && saved["outbox"].as_array().is_some_and(Vec::is_empty)
    })
    .await;
    assert!(fake.puts.lock().unwrap().iter().all(|put| put == &before));
    assert_eq!(journal_json(&path)["application"]["base"]["text"], "hello!");
    client.close().await;
    drop(second);
}
