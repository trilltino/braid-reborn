use async_trait::async_trait;
use braid_http::{
    client::{ChannelConfig, ChannelJournal, ChannelState, ReliableChannel, RetryConfig},
    traits::BraidNetwork,
    types::{BraidRequest, Update, Version},
    BraidError, BraidResponse, Result,
};
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

#[derive(Default)]
struct Network {
    subscriptions: Mutex<Vec<BraidRequest>>,
    receivers: Mutex<VecDeque<async_channel::Receiver<Result<Update>>>>,
    puts: Mutex<Vec<BraidRequest>>,
    responses: Mutex<VecDeque<u16>>,
    pending: AtomicUsize,
    canceled: AtomicUsize,
}
struct InFlight<'a>(&'a Network);
impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        self.0.canceled.fetch_add(1, Ordering::SeqCst);
    }
}
#[async_trait]
impl BraidNetwork for Network {
    async fn fetch(&self, _: &str, request: BraidRequest) -> Result<BraidResponse> {
        self.puts.lock().unwrap().push(request);
        if self
            .pending
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            let _guard = InFlight(self);
            std::future::pending::<()>().await;
        }
        Ok(BraidResponse::new(
            self.responses.lock().unwrap().pop_front().unwrap_or(200),
            "",
        ))
    }
    async fn subscribe(
        &self,
        _: &str,
        request: BraidRequest,
    ) -> Result<async_channel::Receiver<Result<Update>>> {
        self.subscriptions.lock().unwrap().push(request);
        self.receivers
            .lock()
            .unwrap()
            .pop_front()
            .ok_or(BraidError::SubscriptionClosed)
    }
}
fn config() -> ChannelConfig {
    ChannelConfig {
        put_timeout: Duration::from_millis(200),
        retry: RetryConfig::default()
            .with_initial_backoff(Duration::from_millis(1))
            .with_max_backoff(Duration::from_millis(2)),
        ..Default::default()
    }
}

#[tokio::test]
async fn scoped_channel_credentials_reach_transport_without_entering_recovery_bytes() {
    use braid_http::client::{OriginCredentials, ScopedCredentials};
    let path = std::env::temp_dir().join(format!(
        "braid-scoped-channel-{}.json",
        uuid::Uuid::new_v4()
    ));
    let network = Arc::new(Network::default());
    let (_sender, receiver) = async_channel::bounded(1);
    network.receivers.lock().unwrap().push_back(receiver);
    let credentials = ScopedCredentials::new(
        "http://test",
        OriginCredentials {
            bearer_token: Some("private-channel-token".into()),
            cookie: None,
        },
    )
    .unwrap();
    let request = credentials
        .authenticate("http://test/doc", BraidRequest::new())
        .unwrap();
    let channel = ReliableChannel::start(
        network.clone(),
        "http://test/doc".into(),
        request,
        ChannelJournal::open(&path).await.unwrap(),
        config(),
    )
    .unwrap();
    channel
        .queue(Update::snapshot(Version::new("one"), "content"), None)
        .await
        .unwrap();
    wait_for(|| !network.puts.lock().unwrap().is_empty()).await;
    assert_eq!(
        network.puts.lock().unwrap()[0].extra_headers["authorization"],
        "Bearer private-channel-token"
    );
    channel.close().await;
    let bytes = std::fs::read(&path).unwrap();
    assert!(!String::from_utf8_lossy(&bytes).contains("private-channel-token"));
    std::fs::remove_file(&path).unwrap();
    std::fs::remove_file(path.with_extension("lock")).unwrap();
}

#[tokio::test]
async fn transport_headers_cannot_enter_an_outbox_and_old_bytes_are_retained_for_migration() {
    let path = std::env::temp_dir().join(format!(
        "braid-secret-boundary-{}.json",
        uuid::Uuid::new_v4()
    ));
    let network = Arc::new(Network::default());
    let (_sender, receiver) = async_channel::bounded(1);
    network.receivers.lock().unwrap().push_back(receiver);
    let journal = ChannelJournal::open(&path).await.unwrap();
    let channel = start(network.clone(), journal, config());
    let mut update = Update::snapshot(Version::new("uncertain"), "content");
    update
        .extra_headers
        .insert("authorization".into(), "Bearer private-value".into());
    assert!(channel.queue(update.clone(), None).await.is_err());
    channel.close().await;
    assert!(network.puts.lock().unwrap().is_empty());
    let mut checkpoint = braid_http::client::ChannelCheckpoint::default();
    checkpoint.outbox.push_back(update);
    let original = serde_json::to_vec(&checkpoint).unwrap();
    std::fs::write(&path, &original).unwrap();
    assert!(matches!(
        ChannelJournal::open(&path).await,
        Err(BraidError::Config(_))
    ));
    assert_eq!(
        std::fs::read(&path).unwrap(),
        original,
        "never silently rewrite an uncertain PUT during recovery"
    );
    std::fs::remove_file(&path).unwrap();
    std::fs::remove_file(path.with_extension("lock")).unwrap();
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
async fn wait_for(mut predicate: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !predicate() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn subscription_failure_cancels_put_and_replays_exact_payload() {
    let network = Arc::new(Network::default());
    let (tx1, rx1) = async_channel::bounded(2);
    let (_tx2, rx2) = async_channel::bounded(2);
    network.receivers.lock().unwrap().extend([rx1, rx2]);
    network.pending.store(1, Ordering::SeqCst);
    let mut journal = ChannelJournal::memory();
    journal
        .initialize_application(vec![Version::new("known")], serde_json::json!({}))
        .await
        .unwrap();
    let channel = start(
        network.clone(),
        journal,
        ChannelConfig {
            resume_local_frontier: true,
            ..config()
        },
    );
    let update = Update::snapshot(Version::new("stable-id"), "\u{1f600} immutable")
        .with_parents(vec![Version::new("known")]);
    channel.queue(update, None).await.unwrap();
    wait_for(|| network.puts.lock().unwrap().len() == 1).await;
    tx1.send(Err(BraidError::Http("connection reset".into())))
        .await
        .unwrap();
    wait_for(|| network.puts.lock().unwrap().len() == 2).await;
    assert_eq!(network.canceled.load(Ordering::SeqCst), 1);
    let puts = network.puts.lock().unwrap().clone();
    assert_eq!(puts[0].version, puts[1].version);
    assert_eq!(puts[0].body, puts[1].body);
    assert_eq!(
        network.subscriptions.lock().unwrap()[1].parents,
        Some(vec![Version::new("known")]),
        "Reconnect cannot name a version which may never have reached the server"
    );
    wait_for(|| channel.status().borrow().outstanding_puts == 0).await;
    channel.close().await;
}

#[tokio::test]
async fn lost_ack_restart_retains_identity_application_and_writer_exclusion() {
    let path = std::env::temp_dir().join(format!("braid-channel-{}.json", uuid::Uuid::new_v4()));
    let journal = ChannelJournal::open(&path).await.unwrap();
    assert!(ChannelJournal::open(&path).await.is_err());
    let network = Arc::new(Network::default());
    let (_tx, rx) = async_channel::bounded(2);
    network.receivers.lock().unwrap().push_back(rx);
    network.pending.store(1, Ordering::SeqCst);
    let channel = start(network.clone(), journal, config());
    channel
        .queue(
            Update::snapshot(Version::new("survives-crash"), "local"),
            Some(serde_json::json!({"text":"local"})),
        )
        .await
        .unwrap();
    wait_for(|| network.puts.lock().unwrap().len() == 1).await;
    channel.close().await;
    let journal = ChannelJournal::open(&path).await.unwrap();
    assert_eq!(
        journal.checkpoint().outbox[0].version,
        vec![Version::new("survives-crash")]
    );
    assert_eq!(
        journal.checkpoint().application.as_ref().unwrap()["text"],
        "local"
    );
    let (_tx2, rx2) = async_channel::bounded(2);
    network.receivers.lock().unwrap().push_back(rx2);
    let channel = start(network.clone(), journal, config());
    wait_for(|| channel.status().borrow().outstanding_puts == 0).await;
    channel.close().await;
    let journal = ChannelJournal::open(&path).await.unwrap();
    assert!(journal.checkpoint().outbox.is_empty());
    drop(journal);
    std::fs::remove_file(&path).unwrap();
    std::fs::remove_file(path.with_extension("lock")).unwrap();
}

#[tokio::test]
async fn cursor_advances_only_after_application_commit_and_bad_syntax_is_terminal() {
    let network = Arc::new(Network::default());
    let (tx, rx) = async_channel::bounded(2);
    network.receivers.lock().unwrap().push_back(rx);
    let channel = start(network.clone(), ChannelJournal::memory(), config());
    tx.send(Ok(Update::snapshot(Version::new("received"), "text")))
        .await
        .unwrap();
    let update = channel.next().await.unwrap().unwrap();
    assert!(channel
        .status()
        .borrow()
        .durable_receive_frontier
        .is_empty());
    assert!(matches!(
        channel.applied(vec![Version::new("wrong")], None).await,
        Err(BraidError::Protocol(_))
    ));
    channel.applied(update.version.clone(), None).await.unwrap();
    assert_eq!(
        channel.status().borrow().durable_receive_frontier,
        update.version
    );
    tx.send(Err(BraidError::HeaderParse(
        "invalid structured field".into(),
    )))
    .await
    .unwrap();
    assert!(matches!(
        channel.next().await,
        Some(Err(BraidError::HeaderParse(_)))
    ));
    assert_eq!(
        channel.status().borrow().state,
        ChannelState::RepairRequired
    );
    assert_eq!(network.subscriptions.lock().unwrap().len(), 1);
    channel.close().await;
}

#[tokio::test]
async fn bounded_queue_rejects_reuse_and_overflow_without_dropping_original() {
    let network = Arc::new(Network::default());
    let (_tx, rx) = async_channel::bounded(2);
    network.receivers.lock().unwrap().push_back(rx);
    network.pending.store(1, Ordering::SeqCst);
    let channel = start(
        network,
        ChannelJournal::memory(),
        ChannelConfig {
            max_pending: 1,
            put_timeout: Duration::from_secs(60),
            ..config()
        },
    );
    channel
        .queue(Update::snapshot(Version::new("one"), "original"), None)
        .await
        .unwrap();
    assert!(matches!(
        channel
            .queue(Update::snapshot(Version::new("one"), "changed"), None)
            .await,
        Err(BraidError::Protocol(_))
    ));
    assert!(matches!(
        channel
            .queue(Update::snapshot(Version::new("two"), "second"), None)
            .await,
        Err(BraidError::ResourceLimit { .. })
    ));
    assert_eq!(channel.status().borrow().outstanding_puts, 1);
    channel.close().await;
}

#[test]
fn explicit_negotiated_representation_type_survives_patch_encoding() {
    let request = BraidRequest::new()
        .with_method("PUT")
        .with_version(Version::new("v1"))
        .with_content_type("text/plain")
        .with_header("Repr-Type", "text/html")
        .with_patches(vec![braid_http::types::Patch::text("[0:0]", "text")]);
    let (headers, _) = braid_http::protocol::encode_request(&request).unwrap();
    assert_eq!(headers["repr-type"], "text/html");
    assert_eq!(headers["content-type"], "application/http-patches; count=1");
}

#[tokio::test]
#[ignore = "Only invoked by the process-crash regression"]
async fn child_journal_writer() {
    let Some(path) = std::env::var_os("BRAID_TEST_JOURNAL") else {
        return;
    };
    let path = std::path::PathBuf::from(path);
    let journal = ChannelJournal::open(&path).await.unwrap();
    let network = Arc::new(Network::default());
    let (_tx, rx) = async_channel::bounded(1);
    network.receivers.lock().unwrap().push_back(rx);
    network.pending.store(1, Ordering::SeqCst);
    let channel = start(
        network.clone(),
        journal,
        ChannelConfig {
            put_timeout: Duration::from_secs(60),
            ..config()
        },
    );
    channel
        .queue(
            Update::snapshot(
                Version::new("process-stable"),
                "survives process termination",
            ),
            Some(serde_json::json!({"text":"survives process termination"})),
        )
        .await
        .unwrap();
    wait_for(|| network.puts.lock().unwrap().len() == 1).await;
    std::fs::write(path.with_extension("ready"), b"ready").unwrap();
    std::future::pending::<()>().await;
}

#[tokio::test]
async fn killed_process_releases_lease_and_replays_its_durable_unacknowledged_edit() {
    let path = std::env::temp_dir().join(format!("braid-process-{}.json", uuid::Uuid::new_v4()));
    let mut child = tokio::process::Command::new(std::env::current_exe().unwrap())
        .args(["--ignored", "--exact", "child_journal_writer"])
        .env("BRAID_TEST_JOURNAL", &path)
        .kill_on_drop(true)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    wait_for(|| path.with_extension("ready").exists()).await;
    assert!(ChannelJournal::open(&path).await.is_err());
    child.kill().await.unwrap();
    child.wait().await.unwrap();
    let journal = ChannelJournal::open(&path).await.unwrap();
    assert_eq!(
        journal.checkpoint().outbox[0].version,
        vec![Version::new("process-stable")]
    );
    assert_eq!(
        journal.checkpoint().outbox[0].body_str(),
        Some("survives process termination")
    );
    let network = Arc::new(Network::default());
    let (_tx, rx) = async_channel::bounded(1);
    network.receivers.lock().unwrap().push_back(rx);
    let channel = start(network.clone(), journal, config());
    wait_for(|| channel.status().borrow().outstanding_puts == 0).await;
    assert_eq!(
        network.puts.lock().unwrap()[0].version,
        Some(vec![Version::new("process-stable")])
    );
    channel.close().await;
    for file in [
        &path,
        &path.with_extension("lock"),
        &path.with_extension("ready"),
    ] {
        std::fs::remove_file(file).unwrap();
    }
}
