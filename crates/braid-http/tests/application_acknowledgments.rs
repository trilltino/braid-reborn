//! Opt-in application-owned acknowledgments, immutable payload references and
//! diagnostics. Generic callers keep automatic acknowledgment (see reliable_channel).
use async_trait::async_trait;
use braid_http::{
    client::{
        ChannelCheckpoint, ChannelConfig, ChannelEvent, ChannelJournal, ChannelState,
        JournalBackend, PayloadRef, PayloadSource, PayloadStream, ReliableChannel, RepairCause,
        RetryConfig, RetryReason,
    },
    traits::BraidNetwork,
    types::{BraidRequest, Update, Version},
    BraidError, BraidResponse, Result,
};
use bytes::Bytes;
use futures::StreamExt;
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Duration,
};

/// Request, the bytes the transport read from the body stream, and how reading ended.
type Streamed = (BraidRequest, Vec<u8>, Result<()>);

#[derive(Default)]
struct Network {
    receivers: Mutex<VecDeque<Result<async_channel::Receiver<Result<Update>>>>>,
    puts: Mutex<Vec<BraidRequest>>,
    streamed: Mutex<Vec<Streamed>>,
}
#[async_trait]
impl BraidNetwork for Network {
    async fn fetch(&self, _: &str, request: BraidRequest) -> Result<BraidResponse> {
        self.puts.lock().unwrap().push(request);
        Ok(BraidResponse::new(200, ""))
    }
    async fn put_stream(
        &self,
        _: &str,
        request: BraidRequest,
        mut body: PayloadStream,
        _length: u64,
    ) -> Result<BraidResponse> {
        let mut bytes = Vec::new();
        let mut outcome = Ok(());
        while let Some(chunk) = body.next().await {
            match chunk {
                Ok(chunk) => bytes.extend_from_slice(&chunk),
                Err(error) => {
                    outcome = Err(error);
                    break;
                }
            }
        }
        let failed = outcome
            .as_ref()
            .err()
            .map(|error| BraidError::Payload(error.to_string()));
        self.streamed
            .lock()
            .unwrap()
            .push((request, bytes, outcome));
        match failed {
            Some(error) => Err(error),
            None => Ok(BraidResponse::new(200, "")),
        }
    }
    async fn subscribe(
        &self,
        _: &str,
        _: BraidRequest,
    ) -> Result<async_channel::Receiver<Result<Update>>> {
        self.receivers
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(Err(BraidError::SubscriptionClosed))
    }
}

#[derive(Default)]
struct Store {
    bytes: Option<Vec<u8>>,
    fail_dequeue: bool,
    backups: Vec<(String, Vec<u8>)>,
}
struct Journal(Arc<Mutex<Store>>);
#[async_trait]
impl JournalBackend for Journal {
    async fn load(&mut self) -> Result<Option<Vec<u8>>> {
        Ok(self.0.lock().unwrap().bytes.clone())
    }
    async fn store(&mut self, bytes: &[u8]) -> Result<()> {
        let mut store = self.0.lock().unwrap();
        let next: ChannelCheckpoint = serde_json::from_slice(bytes)?;
        let previous: ChannelCheckpoint = store
            .bytes
            .as_deref()
            .map(serde_json::from_slice)
            .transpose()?
            .unwrap_or_default();
        if store.fail_dequeue && !previous.outbox.is_empty() && next.outbox.is_empty() {
            store.fail_dequeue = false;
            return Err(std::io::Error::from(std::io::ErrorKind::StorageFull).into());
        }
        store.bytes = Some(bytes.to_vec());
        Ok(())
    }
    async fn backup(&mut self, label: &str, bytes: &[u8]) -> Result<()> {
        self.0
            .lock()
            .unwrap()
            .backups
            .push((label.into(), bytes.to_vec()));
        Ok(())
    }
}

fn config() -> ChannelConfig {
    ChannelConfig {
        put_timeout: Duration::from_millis(500),
        retry: RetryConfig::default()
            .with_initial_backoff(Duration::from_millis(1))
            .with_max_backoff(Duration::from_millis(2)),
        application_acknowledgments: true,
        ..Default::default()
    }
}
fn start(network: Arc<Network>, journal: ChannelJournal, config: ChannelConfig) -> ReliableChannel {
    ReliableChannel::start(
        network,
        "http://test/doc".into(),
        BraidRequest::new(),
        journal,
        config,
    )
    .unwrap()
}
fn online(network: &Network) -> async_channel::Sender<Result<Update>> {
    let (tx, rx) = async_channel::bounded(4);
    network.receivers.lock().unwrap().push_back(Ok(rx));
    tx
}
async fn wait_for(mut predicate: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !predicate() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
}
fn checkpoint(store: &Arc<Mutex<Store>>) -> ChannelCheckpoint {
    serde_json::from_slice(store.lock().unwrap().bytes.as_ref().unwrap()).unwrap()
}

#[tokio::test]
async fn acknowledged_put_stays_durable_until_the_application_commits_it_atomically() {
    let store = Arc::new(Mutex::new(Store::default()));
    let network = Arc::new(Network::default());
    let _server = online(&network);
    let channel = start(
        network.clone(),
        ChannelJournal::from_backend(Journal(store.clone()))
            .await
            .unwrap(),
        config(),
    );
    let first = Update::snapshot(Version::new("one"), "1");
    let second = Update::snapshot(Version::new("two"), "2").with_parent(Version::new("one"));
    channel
        .queue(first, Some(serde_json::json!({"state": "after-one"})))
        .await
        .unwrap();
    channel
        .queue(second, Some(serde_json::json!({"state": "after-two"})))
        .await
        .unwrap();

    let ChannelEvent::PutAcknowledged {
        version, update, ..
    } = channel.next_event().await.unwrap().unwrap()
    else {
        panic!("expected an acknowledgment event first");
    };
    assert_eq!(version, vec![Version::new("one")]);
    assert_eq!(update.body.as_deref(), Some(&b"1"[..]));
    // The accepted entry is still durable and the next PUT is held back.
    assert_eq!(checkpoint(&store).outbox.len(), 2);
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert_eq!(network.puts.lock().unwrap().len(), 1);
    assert_eq!(
        channel.diagnostics().borrow().awaiting_acknowledgment,
        Some(vec![Version::new("one")])
    );

    channel
        .commit_ack(
            version.clone(),
            Some(serde_json::json!({"state": "acked-one"})),
        )
        .await
        .unwrap();
    let saved = checkpoint(&store);
    assert_eq!(saved.outbox.len(), 1);
    assert_eq!(
        saved.application,
        Some(serde_json::json!({"state": "acked-one"}))
    );
    // Repeating the commit is harmless and does not touch application state.
    channel
        .commit_ack(version, Some(serde_json::json!({"state": "stale"})))
        .await
        .unwrap();
    assert_eq!(
        checkpoint(&store).application,
        Some(serde_json::json!({"state": "acked-one"}))
    );

    let ChannelEvent::PutAcknowledged { version, .. } =
        channel.next_event().await.unwrap().unwrap()
    else {
        panic!("expected the second acknowledgment");
    };
    assert_eq!(version, vec![Version::new("two")]);
    channel.commit_ack(version, None).await.unwrap();
    assert!(checkpoint(&store).outbox.is_empty());
    channel.close().await;
}

#[tokio::test]
async fn failed_acknowledgment_persistence_is_retryable_and_replays_unchanged_identity() {
    let store = Arc::new(Mutex::new(Store {
        fail_dequeue: true,
        ..Default::default()
    }));
    let network = Arc::new(Network::default());
    let _server = online(&network);
    let channel = start(
        network.clone(),
        ChannelJournal::from_backend(Journal(store.clone()))
            .await
            .unwrap(),
        config(),
    );
    let edit = Update::snapshot(Version::new("lost-ack"), "payload");
    channel
        .queue(edit, Some(serde_json::json!({"state": "optimistic"})))
        .await
        .unwrap();
    let ChannelEvent::PutAcknowledged { version, .. } =
        channel.next_event().await.unwrap().unwrap()
    else {
        panic!("expected acknowledgment");
    };
    let before = store.lock().unwrap().bytes.clone();
    // The store refuses the dequeue: the call fails and nothing changed.
    assert!(channel
        .commit_ack(version.clone(), Some(serde_json::json!({"state": "acked"})))
        .await
        .is_err());
    assert_eq!(store.lock().unwrap().bytes, before);
    assert_ne!(
        channel.diagnostics().borrow().state,
        ChannelState::RepairRequired
    );
    // A restart now replays the very same immutable PUT.
    channel.close().await;
    let network2 = Arc::new(Network::default());
    let _server2 = online(&network2);
    let channel = start(
        network2.clone(),
        ChannelJournal::from_backend(Journal(store.clone()))
            .await
            .unwrap(),
        config(),
    );
    let ChannelEvent::PutAcknowledged { version: again, .. } =
        channel.next_event().await.unwrap().unwrap()
    else {
        panic!("expected a replayed acknowledgment");
    };
    assert_eq!(again, version);
    assert_eq!(
        braid_http::protocol::encode_request(&network.puts.lock().unwrap()[0]).unwrap(),
        braid_http::protocol::encode_request(&network2.puts.lock().unwrap()[0]).unwrap()
    );
    channel
        .commit_ack(again, Some(serde_json::json!({"state": "acked"})))
        .await
        .unwrap();
    let saved = checkpoint(&store);
    assert!(saved.outbox.is_empty());
    assert_eq!(
        saved.application,
        Some(serde_json::json!({"state": "acked"}))
    );
    channel.close().await;
}

#[tokio::test]
async fn generic_channels_reject_application_commits_and_never_emit_acknowledgments() {
    let network = Arc::new(Network::default());
    let _server = online(&network);
    let channel = start(
        network.clone(),
        ChannelJournal::memory(),
        ChannelConfig {
            application_acknowledgments: false,
            ..config()
        },
    );
    channel
        .queue(Update::snapshot(Version::new("auto"), "x"), None)
        .await
        .unwrap();
    let diagnostics = channel.diagnostics();
    tokio::time::timeout(Duration::from_secs(3), async {
        while diagnostics.borrow().pending_count != 0 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    assert!(matches!(
        channel.commit_ack(vec![Version::new("auto")], None).await,
        Err(BraidError::Config(_))
    ));
    channel.close().await;
}

struct Blob(Mutex<Vec<u8>>);
#[async_trait]
impl PayloadSource for Blob {
    async fn open(&self, _: &PayloadRef) -> Result<PayloadStream> {
        let bytes = self.0.lock().unwrap().clone();
        let chunks: Vec<Result<Bytes>> = bytes
            .chunks(7)
            .map(|chunk| Ok(Bytes::copy_from_slice(chunk)))
            .collect();
        Ok(futures::stream::iter(chunks).boxed())
    }
}

#[tokio::test]
async fn payloads_stream_by_reference_and_never_enter_the_journal() {
    let content = vec![b'x'; 5000];
    let payload = PayloadRef::for_bytes("application/octet-stream", &content);
    let store = Arc::new(Mutex::new(Store::default()));
    let network = Arc::new(Network::default());
    let _server = online(&network);
    let source = Arc::new(Blob(Mutex::new(content.clone())));
    let channel = ReliableChannel::start_with_payloads(
        network.clone(),
        "http://test/doc".into(),
        BraidRequest::new(),
        ChannelJournal::from_backend(Journal(store.clone()))
            .await
            .unwrap(),
        config(),
        Some(source.clone()),
    )
    .unwrap();
    channel
        .queue_payload(
            Update::snapshot(Version::new("blob"), Bytes::new()).with_content_type("image/x-test"),
            payload.clone(),
            None,
        )
        .await
        .unwrap_err(); // metadata must not carry an inline body
    let mut metadata = Update::snapshot(Version::new("blob"), Bytes::new());
    metadata.body = None;
    channel
        .queue_payload(metadata, payload.clone(), None)
        .await
        .unwrap();
    assert!(String::from_utf8_lossy(store.lock().unwrap().bytes.as_ref().unwrap()).len() < 1000);
    let ChannelEvent::PutAcknowledged { .. } = channel.next_event().await.unwrap().unwrap() else {
        panic!("expected acknowledgment");
    };
    {
        let streamed = network.streamed.lock().unwrap();
        assert_eq!(streamed.len(), 1);
        assert_eq!(streamed[0].1, content);
        assert!(streamed[0].2.is_ok());
        assert_eq!(
            streamed[0].0.content_type.as_deref(),
            Some("application/octet-stream")
        );
    }
    channel
        .commit_ack(vec![Version::new("blob")], None)
        .await
        .unwrap();
    assert!(checkpoint(&store).payloads.is_empty());
    channel.close().await;
}

#[tokio::test]
async fn a_journal_naming_payloads_cannot_start_without_a_source() {
    let payload = PayloadRef::for_bytes("text/plain", b"x");
    let mut metadata = Update::snapshot(Version::new("needs-source"), Bytes::new());
    metadata.body = None;
    let key = braid_http::protocol::format_version_header(&metadata.version);
    let saved = serde_json::json!({
        "receive_frontier": [], "local_frontier": [], "outbox": [metadata],
        "application": null, "payloads": { key: payload }
    });
    let store = Arc::new(Mutex::new(Store {
        bytes: Some(serde_json::to_vec(&saved).unwrap()),
        ..Default::default()
    }));
    let journal = ChannelJournal::from_backend(Journal(store)).await.unwrap();
    assert!(matches!(
        ReliableChannel::start(
            Arc::new(Network::default()),
            "http://test/doc".into(),
            BraidRequest::new(),
            journal,
            config()
        ),
        Err(BraidError::Config(_))
    ));
    // A reference with no matching outbox entry is not a valid journal.
    let orphan = serde_json::json!({
        "receive_frontier": [], "local_frontier": [], "outbox": [],
        "application": null, "payloads": { "\"orphan\"": PayloadRef::for_bytes("text/plain", b"x") }
    });
    assert!(
        ChannelJournal::from_backend(Journal(Arc::new(Mutex::new(Store {
            bytes: Some(serde_json::to_vec(&orphan).unwrap()),
            ..Default::default()
        }))))
        .await
        .is_err()
    );
}

#[tokio::test]
async fn substituted_or_truncated_payloads_fail_closed_with_a_named_cause() {
    let content = b"original immutable bytes".to_vec();
    let payload = PayloadRef::for_bytes("text/plain", &content);
    let network = Arc::new(Network::default());
    let _server = online(&network);
    let source = Arc::new(Blob(Mutex::new(content)));
    let channel = ReliableChannel::start_with_payloads(
        network.clone(),
        "http://test/doc".into(),
        BraidRequest::new(),
        ChannelJournal::memory(),
        config(),
        Some(source.clone()),
    )
    .unwrap();
    let mut metadata = Update::snapshot(Version::new("swap"), Bytes::new());
    metadata.body = None;
    channel
        .queue_payload(metadata.clone(), payload.clone(), None)
        .await
        .unwrap();
    // Replace the bytes after queuing but before the request is built.
    let ChannelEvent::PutAcknowledged { .. } = channel.next_event().await.unwrap().unwrap() else {
        panic!("first attempt streams the original bytes");
    };
    assert_eq!(network.streamed.lock().unwrap().len(), 1);
    channel
        .commit_ack(vec![Version::new("swap")], None)
        .await
        .unwrap();

    *source.0.lock().unwrap() = b"substituted bytes!!!!!!!".to_vec();
    let mut second = metadata;
    second.version = vec![Version::new("swap-2")];
    channel.queue_payload(second, payload, None).await.unwrap();
    wait_for(|| channel.diagnostics().borrow().state == ChannelState::RepairRequired).await;
    assert!(matches!(
        channel.diagnostics().borrow().repair_cause,
        Some(RepairCause::Payload(_))
    ));
    // The rejected body was never accepted as complete.
    let streamed = network.streamed.lock().unwrap();
    assert!(streamed.last().unwrap().2.is_err());
    assert_eq!(channel.diagnostics().borrow().pending_count, 1);
}

#[tokio::test]
async fn diagnostics_name_retry_reasons_pending_work_and_receive_frontier() {
    let network = Arc::new(Network::default());
    for _ in 0..200 {
        network
            .receivers
            .lock()
            .unwrap()
            .push_back(Err(BraidError::SubscriptionStatus {
                status: 503,
                retry_after: Some(Duration::from_millis(1)),
            }));
    }
    let channel = start(network.clone(), ChannelJournal::memory(), config());
    let diagnostics = channel.diagnostics();
    wait_for(|| {
        matches!(
            diagnostics.borrow().retry_reason,
            Some(RetryReason::Status { status: 503, .. })
        )
    })
    .await;
    // Recover: the stale reason clears, and the compatibility status API agrees.
    network.receivers.lock().unwrap().clear();
    let _server = online(&network);
    wait_for(|| diagnostics.borrow().state == ChannelState::Online).await;
    assert_eq!(diagnostics.borrow().retry_reason, None);
    assert_eq!(channel.status().borrow().state, ChannelState::Online);
    assert!(channel.status().borrow().reconnects >= 1);
    assert_eq!(diagnostics.borrow().pending_count, 0);
    assert!(diagnostics.borrow().receive_frontier.is_empty());
    channel.close().await;
}

#[tokio::test]
async fn journals_without_payload_references_round_trip_byte_for_byte() {
    let legacy = serde_json::json!({
        "resource_url": "http://test/doc",
        "receive_frontier": [],
        "local_frontier": [],
        "outbox": [],
        "application": {"legacy": true}
    });
    let bytes = serde_json::to_vec(&legacy).unwrap();
    let parsed: ChannelCheckpoint = serde_json::from_slice(&bytes).unwrap();
    assert!(parsed.payloads.is_empty());
    let reserialized: serde_json::Value = serde_json::to_value(&parsed).unwrap();
    assert!(reserialized.get("payloads").is_none());
}

#[tokio::test]
async fn original_journal_bytes_are_backed_up_before_migration_and_never_replaced() {
    let store = Arc::new(Mutex::new(Store {
        bytes: Some(br#"{"receive_frontier":[],"local_frontier":[],"outbox":[],"application":{"legacy":1}}"#.to_vec()),
        ..Default::default()
    }));
    let mut journal = ChannelJournal::from_backend(Journal(store.clone()))
        .await
        .unwrap();
    journal.backup_original("pre-recovery-v2").await.unwrap();
    let backups = store.lock().unwrap().backups.clone();
    assert_eq!(backups.len(), 1);
    assert_eq!(backups[0].1, store.lock().unwrap().bytes.clone().unwrap());
    assert!(journal.backup_original("../escape").await.is_err());
}
