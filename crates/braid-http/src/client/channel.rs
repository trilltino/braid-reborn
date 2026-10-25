//! One owner for a subscription, immutable outgoing updates and their durable cursor.
use super::{
    parse_retry_after, ChannelJournal, OwnedTask, PayloadRef, PayloadSource, RetryConfig,
    RetryDecision, RetryState, VerifiedPayload,
};
use crate::{
    traits::BraidNetwork,
    types::{BraidRequest, Update, Version},
    BraidClient, BraidError, Result,
};
use std::{sync::Arc, time::Duration};
use tokio::sync::{mpsc, oneshot, watch};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChannelState {
    Disconnected,
    Connecting,
    CatchingUp,
    Online,
    Backoff,
    RepairRequired,
    Stopped,
}

/// Everything an application can receive from a channel.
#[derive(Clone, Debug)]
pub enum ChannelEvent {
    /// A delivered update; answer with `applied` or `ignored`.
    Update(Box<Update>),
    /// The server accepted this PUT. Emitted only with
    /// [`ChannelConfig::application_acknowledgments`]. The outbox entry stays durable
    /// until the application calls [`ReliableChannel::commit_ack`].
    PutAcknowledged {
        version: Vec<Version>,
        status: u16,
        /// The immutable accepted outbox entry, so the application can advance
        /// its own verified state in the same transaction as `commit_ack`.
        update: Box<Update>,
    },
}

/// Why the channel is waiting to reconnect.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RetryReason {
    Transport(String),
    Timeout,
    Status {
        status: u16,
        retry_after: Option<Duration>,
    },
    SubscriptionClosed,
    TruncatedFrame,
}

/// Why the channel stopped and requires an operator or application repair.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RepairCause {
    Storage(String),
    Protocol(String),
    Authorization { status: u16 },
    Payload(String),
    Other(String),
}
impl RepairCause {
    fn classify(error: &BraidError) -> Self {
        match error {
            BraidError::Io(_) | BraidError::Fs(_) | BraidError::Json(_) => {
                Self::Storage(error.to_string())
            }
            BraidError::Protocol(_)
            | BraidError::HeaderParse(_)
            | BraidError::BodyParse(_)
            | BraidError::InvalidVersion(_) => Self::Protocol(error.to_string()),
            BraidError::SubscriptionStatus {
                status: status @ (401 | 403),
                ..
            } => Self::Authorization { status: *status },
            BraidError::Payload(_) => Self::Payload(error.to_string()),
            _ => Self::Other(error.to_string()),
        }
    }
}

/// Machine-readable diagnostics. `ChannelStatus` remains the compatibility API.
#[derive(Clone, Debug)]
pub struct ChannelDiagnostics {
    pub state: ChannelState,
    pub pending_count: usize,
    pub receive_frontier: Vec<Version>,
    pub retry_reason: Option<RetryReason>,
    pub repair_cause: Option<RepairCause>,
    /// The accepted PUT awaiting an application `commit_ack`.
    pub awaiting_acknowledgment: Option<Vec<Version>>,
}

#[derive(Clone, Debug)]
pub struct ChannelStatus {
    pub state: ChannelState,
    pub outstanding_puts: usize,
    pub durable_receive_frontier: Vec<Version>,
    pub reconnects: u64,
    pub last_error: Option<String>,
}

#[derive(Clone, Debug)]
pub struct ChannelConfig {
    pub max_pending: usize,
    pub max_pending_bytes: usize,
    pub put_timeout: Duration,
    pub retry: RetryConfig,
    /// Simpleton resumes from its durably materialized optimistic state, separately
    /// from the receive cursor. Generic consumers resume only acknowledged receives.
    pub resume_local_frontier: bool,
    /// Opt in to application-owned acknowledgments. A successful PUT then emits
    /// [`ChannelEvent::PutAcknowledged`] and the entry is removed only by
    /// [`ReliableChannel::commit_ack`], atomically with application state.
    /// Off by default: generic callers keep automatic acknowledgment.
    pub application_acknowledgments: bool,
}
impl Default for ChannelConfig {
    fn default() -> Self {
        Self {
            max_pending: 64,
            max_pending_bytes: 16 * 1024 * 1024,
            put_timeout: Duration::from_secs(20),
            retry: RetryConfig::default(),
            resume_local_frontier: false,
            application_acknowledgments: false,
        }
    }
}

enum Command {
    Queue(
        Box<Update>,
        Option<serde_json::Value>,
        oneshot::Sender<Result<()>>,
    ),
    Applied(
        Option<Vec<Version>>,
        Option<serde_json::Value>,
        oneshot::Sender<Result<()>>,
    ),
    QueuePayload(
        Box<Update>,
        PayloadRef,
        Option<serde_json::Value>,
        oneshot::Sender<Result<()>>,
    ),
    CommitAck(
        Vec<Version>,
        Option<serde_json::Value>,
        oneshot::Sender<Result<()>>,
    ),
    Save(serde_json::Value, oneshot::Sender<Result<()>>),
    Pending(oneshot::Sender<Vec<Update>>),
    ResetCursor(serde_json::Value, oneshot::Sender<Result<()>>),
    CheckpointReceived(Vec<Version>, serde_json::Value, oneshot::Sender<Result<()>>),
    Reconnect,
    Stop(oneshot::Sender<()>),
}

pub struct ReliableChannel {
    commands: mpsc::Sender<Command>,
    updates: async_channel::Receiver<Result<ChannelEvent>>,
    status: watch::Receiver<ChannelStatus>,
    diagnostics: watch::Receiver<ChannelDiagnostics>,
    task: OwnedTask,
}
impl ReliableChannel {
    /// Sender-only replication owners call this only after validating and
    /// durably storing the referenced causal events. Cursor and progress commit
    /// in one record; this never acknowledges an outgoing mutation.
    pub async fn checkpoint_received(&self, frontier: Vec<Version>, application: serde_json::Value) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.commands.send(Command::CheckpointReceived(frontier, application, tx)).await
            .map_err(|_| BraidError::SubscriptionClosed)?;
        rx.await.map_err(|_| BraidError::SubscriptionClosed)?
    }
    pub fn status(&self) -> watch::Receiver<ChannelStatus> {
        self.status.clone()
    }
    pub fn diagnostics(&self) -> watch::Receiver<ChannelDiagnostics> {
        self.diagnostics.clone()
    }
    /// Next delivered update. With application acknowledgments enabled, use
    /// [`Self::next_event`]; an acknowledgment event here is a usage error.
    pub async fn next(&self) -> Option<Result<Update>> {
        match self.updates.recv().await.ok()? {
            Ok(ChannelEvent::Update(update)) => Some(Ok(*update)),
            Ok(ChannelEvent::PutAcknowledged { .. }) => Some(Err(BraidError::Config(
                "Application acknowledgments require next_event()".into(),
            ))),
            Err(error) => Some(Err(error)),
        }
    }
    pub async fn next_event(&self) -> Option<Result<ChannelEvent>> {
        self.updates.recv().await.ok()
    }
    /// A copy of the immutable unacknowledged entries, oldest first.
    pub async fn pending_updates(&self) -> Result<Vec<Update>> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(Command::Pending(tx))
            .await
            .map_err(|_| BraidError::SubscriptionClosed)?;
        rx.await.map_err(|_| BraidError::SubscriptionClosed)
    }
    /// Forget both frontier cursors and persist `application`, so the next
    /// subscription starts from a fresh snapshot. Refused while any immutable
    /// outgoing entry exists, because those entries name their parents.
    pub async fn reset_cursor(&self, application: serde_json::Value) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(Command::ResetCursor(application, tx))
            .await
            .map_err(|_| BraidError::SubscriptionClosed)?;
        rx.await.map_err(|_| BraidError::SubscriptionClosed)?
    }
    /// Atomically persist `application` and remove the acknowledged outbox entry.
    /// Idempotent for an entry that was already removed. On failure the entry and
    /// previous application state remain and the call may be retried.
    pub async fn commit_ack(
        &self,
        version: Vec<Version>,
        application: Option<serde_json::Value>,
    ) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(Command::CommitAck(version, application, tx))
            .await
            .map_err(|_| BraidError::SubscriptionClosed)?;
        rx.await.map_err(|_| BraidError::SubscriptionClosed)?
    }
    /// Queue metadata plus an immutable payload reference. The update must carry
    /// neither a body nor patches; the bytes are streamed from the configured
    /// [`PayloadSource`] and verified against the reference on every attempt.
    pub async fn queue_payload(
        &self,
        update: Update,
        payload: PayloadRef,
        application: Option<serde_json::Value>,
    ) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(Command::QueuePayload(
                Box::new(update),
                payload,
                application,
                tx,
            ))
            .await
            .map_err(|_| BraidError::SubscriptionClosed)?;
        rx.await.map_err(|_| BraidError::SubscriptionClosed)?
    }
    /// Returns after the immutable update and application state are committed locally.
    pub async fn queue(
        &self,
        update: Update,
        application: Option<serde_json::Value>,
    ) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(Command::Queue(Box::new(update), application, tx))
            .await
            .map_err(|_| BraidError::SubscriptionClosed)?;
        rx.await.map_err(|_| BraidError::SubscriptionClosed)?
    }
    /// Called after applying and validating the delivered update. Persisted together.
    pub async fn applied(
        &self,
        frontier: Vec<Version>,
        application: Option<serde_json::Value>,
    ) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(Command::Applied(Some(frontier), application, tx))
            .await
            .map_err(|_| BraidError::SubscriptionClosed)?;
        rx.await.map_err(|_| BraidError::SubscriptionClosed)?
    }
    /// Consume an update that the application deliberately did not apply. Its
    /// frontier must not become a durable receive cursor.
    pub async fn ignored(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(Command::Applied(None, None, tx))
            .await
            .map_err(|_| BraidError::SubscriptionClosed)?;
        rx.await.map_err(|_| BraidError::SubscriptionClosed)?
    }
    pub async fn reconnect(&self) -> Result<()> {
        self.commands
            .send(Command::Reconnect)
            .await
            .map_err(|_| BraidError::SubscriptionClosed)
    }
    /// Persist offline application edits without manufacturing a wire version.
    pub async fn save_application(&self, application: serde_json::Value) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(Command::Save(application, tx))
            .await
            .map_err(|_| BraidError::SubscriptionClosed)?;
        rx.await.map_err(|_| BraidError::SubscriptionClosed)?
    }
    pub async fn close(mut self) {
        let (tx, rx) = oneshot::channel();
        if self.commands.send(Command::Stop(tx)).await.is_ok() {
            let _ = rx.await;
        }
        self.task.join().await;
    }
}
impl Drop for ReliableChannel {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl BraidClient {
    /// A durable serial sender for resources whose causal receive path is owned
    /// by an authenticated replication worker. No subscription is synthesized.
    pub fn reliable_sender(&self, url: &str, request: BraidRequest, journal: ChannelJournal, config: ChannelConfig) -> Result<ReliableChannel> {
        ReliableChannel::start_sender(self.network.clone(), url.to_owned(), request, journal, config)
    }
    pub fn reliable_channel_authenticated(
        &self,
        url: &str,
        request: BraidRequest,
        journal: ChannelJournal,
        config: ChannelConfig,
        credentials: &super::ScopedCredentials,
    ) -> Result<ReliableChannel> {
        self.reliable_channel(
            url,
            credentials.authenticate(url, request)?,
            journal,
            config,
        )
    }
    pub fn reliable_channel(
        &self,
        url: &str,
        request: BraidRequest,
        journal: ChannelJournal,
        config: ChannelConfig,
    ) -> Result<ReliableChannel> {
        ReliableChannel::start(
            self.network.clone(),
            url.to_owned(),
            request,
            journal,
            config,
        )
    }
    pub fn reliable_channel_with_payloads(
        &self,
        url: &str,
        request: BraidRequest,
        journal: ChannelJournal,
        config: ChannelConfig,
        payloads: Arc<dyn PayloadSource>,
    ) -> Result<ReliableChannel> {
        ReliableChannel::start_with_payloads(
            self.network.clone(),
            url.to_owned(),
            request,
            journal,
            config,
            Some(payloads),
        )
    }
}

impl ReliableChannel {
    pub fn start(
        network: Arc<dyn BraidNetwork>,
        url: String,
        request: BraidRequest,
        journal: ChannelJournal,
        config: ChannelConfig,
    ) -> Result<Self> {
        Self::start_with_payloads(network, url, request, journal, config, None)
    }
    pub fn start_with_payloads(
        network: Arc<dyn BraidNetwork>,
        url: String,
        request: BraidRequest,
        journal: ChannelJournal,
        config: ChannelConfig,
        payloads: Option<Arc<dyn PayloadSource>>,
    ) -> Result<Self> {
        Self::start_profile(network, url, request, journal, config, payloads, false)
    }

    pub fn start_sender(network: Arc<dyn BraidNetwork>, url: String, request: BraidRequest, journal: ChannelJournal, config: ChannelConfig) -> Result<Self> {
        Self::start_profile(network, url, request, journal, config, None, true)
    }

    pub fn start_sender_with_payloads(network: Arc<dyn BraidNetwork>, url: String, request: BraidRequest, journal: ChannelJournal, config: ChannelConfig, payloads: Arc<dyn PayloadSource>) -> Result<Self> {
        Self::start_profile(network, url, request, journal, config, Some(payloads), true)
    }

    fn start_profile(
        network: Arc<dyn BraidNetwork>, url: String, request: BraidRequest,
        journal: ChannelJournal, config: ChannelConfig,
        payloads: Option<Arc<dyn PayloadSource>>, sender_only: bool,
    ) -> Result<Self> {
        super::validate_resource_url(&url)?;
        if !journal.checkpoint.payloads.is_empty() && payloads.is_none() {
            return Err(BraidError::Config(
                "Journal references immutable payloads; configure a payload source".into(),
            ));
        }
        if journal
            .checkpoint
            .resource_url
            .as_ref()
            .is_some_and(|resource| resource != &url)
        {
            return Err(BraidError::Config(
                "Channel journal belongs to a different resource URL".into(),
            ));
        }
        if config.max_pending == 0 || config.max_pending_bytes == 0 || config.put_timeout.is_zero()
        {
            return Err(BraidError::Config("Channel limits must be positive".into()));
        }
        if journal.checkpoint.outbox.len() > config.max_pending
            || serde_json::to_vec(&journal.checkpoint.outbox)?.len() > config.max_pending_bytes
        {
            return Err(BraidError::ResourceLimit {
                resource: "outbox",
                limit: config.max_pending_bytes as u64,
            });
        }
        let (commands, rx) = mpsc::channel(16);
        // At most one delivered update and one pending acknowledgment exist at a time.
        let (tx, updates) = async_channel::bounded(2);
        let (status_tx, status) = watch::channel(ChannelStatus {
            state: ChannelState::Disconnected,
            outstanding_puts: journal.checkpoint.outbox.len(),
            durable_receive_frontier: journal.checkpoint.receive_frontier.clone(),
            reconnects: 0,
            last_error: None,
        });
        let (diagnostics_tx, diagnostics) = watch::channel(ChannelDiagnostics {
            state: ChannelState::Disconnected,
            pending_count: journal.checkpoint.outbox.len(),
            receive_frontier: journal.checkpoint.receive_frontier.clone(),
            retry_reason: None,
            repair_cause: None,
            awaiting_acknowledgment: None,
        });
        let task = OwnedTask::spawn(async move {
            let mut driver = Driver {
                network,
                url,
                request,
                journal,
                config,
                commands: rx,
                updates: tx,
                status: status_tx,
                diagnostics: diagnostics_tx,
                payloads,
                awaiting_apply: None,
                awaiting_ack: None,
                sender_only,
            };
            if let Err(error) = driver.run().await {
                driver
                    .diagnostics
                    .send_modify(|d| d.repair_cause = Some(RepairCause::classify(&error)));
                driver.state(ChannelState::RepairRequired);
                driver
                    .status
                    .send_modify(|s| s.last_error = Some(error.to_string()));
                // A full application slot must not prevent terminal cancellation.
                // Status always carries the error even when the slot is occupied.
                let _ = driver.updates.try_send(Err(error));
            } else {
                driver.state(ChannelState::Stopped);
            }
        })?;
        Ok(Self {
            commands,
            updates,
            status,
            diagnostics,
            task,
        })
    }
}

#[cfg(not(target_arch = "wasm32"))]
type PutFuture = futures::future::BoxFuture<'static, Result<crate::BraidResponse>>;
#[cfg(target_arch = "wasm32")]
type PutFuture = futures::future::LocalBoxFuture<'static, Result<crate::BraidResponse>>;

struct Driver {
    network: Arc<dyn BraidNetwork>,
    url: String,
    request: BraidRequest,
    journal: ChannelJournal,
    config: ChannelConfig,
    commands: mpsc::Receiver<Command>,
    updates: async_channel::Sender<Result<ChannelEvent>>,
    status: watch::Sender<ChannelStatus>,
    diagnostics: watch::Sender<ChannelDiagnostics>,
    payloads: Option<Arc<dyn PayloadSource>>,
    awaiting_apply: Option<Vec<Version>>,
    awaiting_ack: Option<Vec<Version>>,
    sender_only: bool,
}
impl Driver {
    fn state(&self, state: ChannelState) {
        self.status.send_modify(|s| {
            s.state = state;
            s.outstanding_puts = self.journal.checkpoint.outbox.len();
            s.durable_receive_frontier = self.journal.checkpoint.receive_frontier.clone();
        });
        self.diagnostics.send_modify(|d| {
            d.state = state;
            d.pending_count = self.journal.checkpoint.outbox.len();
            d.receive_frontier = self.journal.checkpoint.receive_frontier.clone();
            d.awaiting_acknowledgment = self.awaiting_ack.clone();
            if state == ChannelState::Online {
                d.retry_reason = None;
            }
        });
    }
    fn retry_reason(error: &BraidError) -> RetryReason {
        match error {
            BraidError::Timeout => RetryReason::Timeout,
            BraidError::SubscriptionClosed => RetryReason::SubscriptionClosed,
            BraidError::SubscriptionStatus {
                status,
                retry_after,
            } => RetryReason::Status {
                status: *status,
                retry_after: *retry_after,
            },
            BraidError::Protocol(message) if message.starts_with("Truncated Braid frame") => {
                RetryReason::TruncatedFrame
            }
            other => RetryReason::Transport(other.to_string()),
        }
    }
    async fn command(&mut self, command: Option<Command>) -> Result<bool> {
        match command {
            Some(Command::CheckpointReceived(frontier, application, done)) => {
                let result = if self.sender_only {
                    let mut next = self.journal.checkpoint.clone();
                    next.receive_frontier = frontier;
                    next.application = Some(application);
                    self.journal.commit(next).await
                } else {
                    Err(BraidError::Config("External receive checkpoints require a sender-only channel".into()))
                };
                let _ = done.send(result);
            }
            None => return Err(BraidError::Aborted),
            Some(Command::Stop(done)) => {
                let _ = done.send(());
                return Err(BraidError::Aborted);
            }
            Some(Command::Reconnect) => return Ok(true),
            Some(Command::Save(application, done)) => {
                let mut next = self.journal.checkpoint.clone();
                next.application = Some(application);
                let result = self.journal.commit(next).await;
                let _ = done.send(result);
            }
            Some(Command::Applied(frontier, application, done)) => {
                if self.awaiting_apply.is_none() {
                    let _ = done.send(Err(BraidError::Protocol(
                        "No delivered update awaiting application".into(),
                    )));
                    return Ok(false);
                }
                if frontier
                    .as_ref()
                    .is_some_and(|frontier| Some(frontier) != self.awaiting_apply.as_ref())
                {
                    let _ = done.send(Err(BraidError::Protocol(
                        "Applied frontier does not match the delivered update".into(),
                    )));
                    return Ok(false);
                }
                let mut next = self.journal.checkpoint.clone();
                if let Some(frontier) = frontier {
                    next.receive_frontier = frontier.clone();
                    if self.config.resume_local_frontier {
                        next.local_frontier = frontier;
                    }
                }
                if let Some(app) = application {
                    next.application = Some(app);
                }
                match self.journal.commit(next).await {
                    Ok(()) => {
                        self.awaiting_apply = None;
                        let state = self.status.borrow().state;
                        self.state(state);
                        let _ = done.send(Ok(()));
                    }
                    Err(e) => {
                        let message = e.to_string();
                        let _ = done.send(Err(e));
                        return Err(BraidError::Fs(message));
                    }
                }
            }
            Some(Command::Pending(done)) => {
                let _ = done.send(self.journal.checkpoint.outbox.iter().cloned().collect());
            }
            Some(Command::ResetCursor(application, done)) => {
                let result = if self.journal.checkpoint.outbox.is_empty() {
                    let mut next = self.journal.checkpoint.clone();
                    next.receive_frontier.clear();
                    next.local_frontier.clear();
                    next.application = Some(application);
                    self.journal.commit(next).await
                } else {
                    Err(BraidError::Config(
                        "Cannot reset cursors while outgoing entries are pending".into(),
                    ))
                };
                let state = self.status.borrow().state;
                self.state(state);
                let _ = done.send(result);
            }
            Some(Command::CommitAck(version, application, done)) => {
                let result = self.commit_ack(version, application).await;
                let state = self.status.borrow().state;
                self.state(state);
                let _ = done.send(result);
            }
            Some(Command::QueuePayload(update, payload, application, done)) => {
                let result = self.enqueue_payload(*update, payload, application).await;
                let state = self.status.borrow().state;
                self.state(state);
                let _ = done.send(result);
            }
            Some(Command::Queue(update, application, done)) => {
                let result = self.enqueue(*update, application).await;
                let state = self.status.borrow().state;
                self.state(state);
                // IO failure leaves the old journal authoritative; caller retains its edit.
                let _ = done.send(result);
            }
        }
        let state = self.status.borrow().state;
        self.state(state);
        Ok(false)
    }
    async fn commit_ack(
        &mut self,
        version: Vec<Version>,
        application: Option<serde_json::Value>,
    ) -> Result<()> {
        if !self.config.application_acknowledgments {
            return Err(BraidError::Config(
                "Application acknowledgments are not enabled for this channel".into(),
            ));
        }
        let Some(position) = self
            .journal
            .checkpoint
            .outbox
            .iter()
            .position(|entry| entry.version == version)
        else {
            // Already committed; the earlier transaction carried the application state.
            return Ok(());
        };
        if position != 0 || self.awaiting_ack.as_ref() != Some(&version) {
            return Err(BraidError::Protocol(
                "Only the accepted head of the outbox can be acknowledged".into(),
            ));
        }
        let mut next = self.journal.checkpoint.clone();
        next.outbox.pop_front();
        next.payloads.remove(&payload_key(&version));
        if let Some(application) = application {
            next.application = Some(application);
        }
        self.journal.commit(next).await?;
        self.awaiting_ack = None;
        Ok(())
    }
    async fn enqueue_payload(
        &mut self,
        update: Update,
        payload: PayloadRef,
        application: Option<serde_json::Value>,
    ) -> Result<()> {
        payload.validate()?;
        let Some(source) = self.payloads.clone() else {
            return Err(BraidError::Config(
                "No immutable payload source is configured".into(),
            ));
        };
        if update.body.is_some() || update.patches.is_some() || update.content_range.is_some() {
            return Err(BraidError::Config(
                "A payload update carries neither a body nor patches".into(),
            ));
        }
        // Prove the content is reachable before promising to send it later.
        drop(source.open(&payload).await?);
        self.enqueue_entry(update, Some(payload), application).await
    }
    async fn enqueue(
        &mut self,
        update: Update,
        application: Option<serde_json::Value>,
    ) -> Result<()> {
        self.enqueue_entry(update, None, application).await
    }
    async fn enqueue_entry(
        &mut self,
        update: Update,
        payload: Option<PayloadRef>,
        application: Option<serde_json::Value>,
    ) -> Result<()> {
        let mut metadata = update.extra_headers.clone();
        crate::protocol::strip_transport_headers(&mut metadata);
        if metadata != update.extra_headers {
            return Err(BraidError::Config(
                "Transport credentials/headers belong on the channel request, not a durable update"
                    .into(),
            ));
        }
        if update.version.is_empty() {
            return Err(BraidError::InvalidVersion(
                "Queued updates require a stable version".into(),
            ));
        }
        // Reject invalid framing/identities before acknowledging local queuing.
        if payload.is_some() {
            let mut framed = update.clone();
            framed.body = Some(bytes::Bytes::new());
            crate::protocol::format_update(&framed)?;
        } else {
            crate::protocol::format_update(&update)?;
        }
        if let Some(old) = self
            .journal
            .checkpoint
            .outbox
            .iter()
            .find(|old| old.version == update.version)
        {
            if serde_json::to_vec(old)? == serde_json::to_vec(&update)?
                && self
                    .journal
                    .checkpoint
                    .payloads
                    .get(&payload_key(&update.version))
                    == payload.as_ref()
            {
                return Ok(());
            }
            return Err(BraidError::Protocol(
                "Version reused with different queued payload".into(),
            ));
        }
        let mut next = self.journal.checkpoint.clone();
        next.local_frontier = update.version.clone();
        if let Some(payload) = payload {
            next.payloads.insert(payload_key(&update.version), payload);
        }
        next.outbox.push_back(update);
        if next.outbox.len() > self.config.max_pending
            || serde_json::to_vec(&next.outbox)?.len() + serde_json::to_vec(&next.payloads)?.len()
                > self.config.max_pending_bytes
        {
            return Err(BraidError::ResourceLimit {
                resource: "outbox",
                limit: self.config.max_pending_bytes as u64,
            });
        }
        if let Some(app) = application {
            next.application = Some(app);
        }
        self.journal.commit(next).await
    }
    async fn run(&mut self) -> Result<()> {
        if self.journal.checkpoint.resource_url.is_none() {
            let mut next = self.journal.checkpoint.clone();
            next.resource_url = Some(self.url.clone());
            self.journal.commit(next).await?;
        }
        let mut retries = RetryState::new(self.config.retry.clone());
        loop {
            self.state(ChannelState::Connecting);
            let error = if self.sender_only {
                match self.session(None).await {
                    Ok(()) => { retries.reset(); continue; },
                    Err(BraidError::Aborted) => return Ok(()),
                    Err(error) => error,
                }
            } else {
            let mut request = self.request.clone();
            // The newest optimistic version may never have reached the server.
            // FIFO parents are known before its first unacknowledged mutation;
            // the same immutable PUT can then replay after GET reconnects.
            let cursor = if let Some(first) = self.journal.checkpoint.outbox.front() {
                &first.parents
            } else if self.config.resume_local_frontier
                && !self.journal.checkpoint.local_frontier.is_empty()
            {
                &self.journal.checkpoint.local_frontier
            } else {
                &self.journal.checkpoint.receive_frontier
            };
            request.parents = (!cursor.is_empty()).then(|| cursor.clone());
            request.subscribe = true;
            let network = self.network.clone();
            let url = self.url.clone();
            let connecting = network.subscribe(&url, request);
            tokio::pin!(connecting);
            let connected = loop {
                tokio::select! {
                    result = &mut connecting => break result,
                    command = self.commands.recv() => match self.command(command).await { Ok(_) => {}, Err(BraidError::Aborted) => return Ok(()), Err(e) => return Err(e) },
                }
            };
            match connected {
                Ok(receiver) => {
                    self.state(ChannelState::CatchingUp);
                    self.state(ChannelState::Online);
                    match self.session(Some(receiver)).await {
                        Ok(()) => {
                            retries.reset();
                            continue;
                        }
                        Err(BraidError::Aborted) => return Ok(()),
                        Err(e) => e,
                    }
                }
                Err(error) => error,
            }
            };
            let retry_after = match &error {
                BraidError::Http(_) | BraidError::Timeout | BraidError::SubscriptionClosed => None,
                BraidError::SubscriptionStatus {
                    status: 309 | 408 | 425 | 429 | 432 | 500 | 502 | 503 | 504,
                    retry_after,
                } => *retry_after,
                BraidError::Protocol(message) if message.starts_with("Truncated Braid frame") => {
                    None
                }
                _ => return Err(error),
            };
            let delay = match retries.should_retry_error(false) {
                RetryDecision::Retry(delay) => retry_after
                    .unwrap_or(delay)
                    .min(self.config.retry.max_backoff),
                RetryDecision::DontRetry => return Err(error),
            };
            self.state(ChannelState::Backoff);
            self.diagnostics
                .send_modify(|d| d.retry_reason = Some(Self::retry_reason(&error)));
            self.status.send_modify(|s| {
                s.reconnects = s.reconnects.saturating_add(1);
                s.last_error = Some(error.to_string());
            });
            let sleep = super::utils::sleep(
                delay.saturating_add(Duration::from_millis(rand::random::<u64>() % 100)),
            );
            tokio::pin!(sleep);
            loop {
                tokio::select! {
                    _ = &mut sleep => break,
                    command = self.commands.recv() => match self.command(command).await { Ok(true) => break, Ok(false) => {}, Err(BraidError::Aborted) => return Ok(()), Err(e) => return Err(e) },
                }
            }
        }
    }
    async fn session(&mut self, receiver: Option<async_channel::Receiver<Result<Update>>>) -> Result<()> {
        let mut inflight: Option<PutFuture> = None;
        loop {
            if inflight.is_none() && self.awaiting_ack.is_none() {
                if let Some(update) = self.journal.checkpoint.outbox.front() {
                    let payload = self
                        .journal
                        .checkpoint
                        .payloads
                        .get(&payload_key(&update.version))
                        .cloned();
                    let mut request = self.request.clone();
                    request.method = "PUT".into();
                    request.subscribe = false;
                    request.version = Some(update.version.clone());
                    request.parents = Some(update.parents.clone());
                    request.body = update.body.clone().unwrap_or_default();
                    request.patches = update.patches.clone();
                    request.merge_type = update.merge_type.clone().or(request.merge_type);
                    request.content_type = update.content_type.clone().or(request.content_type);
                    request.extra_headers.extend(update.extra_headers.clone());
                    let network = self.network.clone();
                    let url = self.url.clone();
                    let mut timeout = self.config.put_timeout;
                    if let Some(payload) = payload {
                        let source = self.payloads.clone().ok_or_else(|| {
                            BraidError::Config("No immutable payload source is configured".into())
                        })?;
                        request
                            .content_type
                            .get_or_insert_with(|| payload.media_type.clone());
                        timeout = timeout
                            .saturating_add(Duration::from_secs(payload.length / (1024 * 1024)));
                        inflight = Some(Box::pin(async move {
                            let stream = source.open(&payload).await?;
                            let length = payload.length;
                            let body: super::PayloadStream =
                                Box::pin(VerifiedPayload::new(stream, payload));
                            super::runtime::timeout(
                                timeout,
                                network.put_stream(&url, request, body, length),
                            )
                            .await?
                        }));
                    } else {
                        inflight = Some(Box::pin(async move {
                            super::runtime::timeout(timeout, network.fetch(&url, request)).await?
                        }));
                    }
                }
            }
            tokio::select! {
                command = self.commands.recv() => if self.command(command).await? { return Ok(()); },
                result = async { match &mut inflight { Some(future) => future.await, None => std::future::pending().await } } => {
                    let response = result?;
                    if !response.is_success() { return Err(BraidError::SubscriptionStatus { status: response.status, retry_after: response.header("retry-after").and_then(parse_retry_after) }); }
                    inflight = None;
                    if self.config.application_acknowledgments {
                        let update = self
                            .journal
                            .checkpoint
                            .outbox
                            .front()
                            .cloned()
                            .ok_or_else(|| {
                                BraidError::Protocol("PUT acknowledged with an empty outbox".into())
                            })?;
                        let version = update.version.clone();
                        self.awaiting_ack = Some(version.clone());
                        self.state(ChannelState::Online);
                        self.updates
                            .send(Ok(ChannelEvent::PutAcknowledged {
                                version,
                                status: response.status,
                                update: Box::new(update),
                            }))
                            .await
                            .map_err(|_| BraidError::Aborted)?;
                    } else {
                        let mut next = self.journal.checkpoint.clone();
                        next.outbox.pop_front();
                        self.journal.commit(next).await?;
                        self.state(ChannelState::Online);
                    }
                }
                update = async { match &receiver { Some(receiver) => receiver.recv().await, None => std::future::pending().await } }, if self.awaiting_apply.is_none() => {
                    let update = update.map_err(|_| BraidError::SubscriptionClosed)??;
                    if let Some(repr) = update.extra_headers.get("repr-type") { self.request.extra_headers.insert("repr-type".into(), repr.clone()); }
                    self.awaiting_apply = Some(update.version.clone());
                    self.updates.send(Ok(ChannelEvent::Update(Box::new(update)))).await.map_err(|_| BraidError::Aborted)?;
                }
            }
        }
    }
}

fn payload_key(version: &[Version]) -> String {
    crate::protocol::format_version_header(version)
}
