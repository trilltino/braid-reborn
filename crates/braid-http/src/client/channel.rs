//! One owner for a subscription, immutable outgoing updates and their durable cursor.
use super::{parse_retry_after, RetryConfig, RetryDecision, RetryState};
use crate::{
    traits::BraidNetwork,
    types::{BraidRequest, Update, Version},
    BraidClient, BraidError, Result,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
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

#[derive(Clone, Debug)]
pub struct ChannelStatus {
    pub state: ChannelState,
    pub outstanding_puts: usize,
    pub durable_receive_frontier: Vec<Version>,
    pub reconnects: u64,
    pub last_error: Option<String>,
}

/// Only pending entries are stored. A crashed in-flight send is replayed unchanged.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ChannelCheckpoint {
    #[serde(default)]
    pub resource_url: Option<String>,
    pub receive_frontier: Vec<Version>,
    pub local_frontier: Vec<Version>,
    pub outbox: VecDeque<Update>,
    pub application: Option<serde_json::Value>,
}

/// A single-writer journal; the optional application value shares the outbox transaction.
pub struct ChannelJournal {
    checkpoint: ChannelCheckpoint,
    path: Option<PathBuf>,
    _lease: Option<std::fs::File>,
}

impl ChannelJournal {
    pub fn memory() -> Self {
        Self {
            checkpoint: ChannelCheckpoint::default(),
            path: None,
            _lease: None,
        }
    }

    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = std::path::absolute(path)?;
        let parent = path
            .parent()
            .ok_or_else(|| BraidError::Config("Journal needs a parent directory".into()))?;
        tokio::fs::create_dir_all(parent).await?;
        let lease = crate::durable::writer_lease(&path.with_extension("lock")).await?;
        let checkpoint: ChannelCheckpoint = match tokio::fs::metadata(&path).await {
            Ok(meta) if meta.len() > 32 * 1024 * 1024 => {
                return Err(BraidError::ResourceLimit {
                    resource: "channel journal",
                    limit: 32 * 1024 * 1024,
                })
            }
            Ok(_) => serde_json::from_slice(&tokio::fs::read(&path).await?)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => ChannelCheckpoint::default(),
            Err(e) => return Err(e.into()),
        };
        for update in &checkpoint.outbox {
            let mut metadata = update.extra_headers.clone();
            crate::protocol::strip_transport_headers(&mut metadata);
            if metadata != update.extra_headers {
                return Err(BraidError::Config("Legacy outbox contains transport headers; preserve it for explicit local migration".into()));
            }
        }
        if let Some(url) = &checkpoint.resource_url {
            super::validate_resource_url(url)?;
        }
        Ok(Self {
            checkpoint,
            path: Some(path),
            _lease: Some(lease),
        })
    }

    pub fn checkpoint(&self) -> &ChannelCheckpoint {
        &self.checkpoint
    }
    /// Seed an empty journal from an already durable application replica.
    pub async fn initialize_application(
        &mut self,
        frontier: Vec<Version>,
        application: serde_json::Value,
    ) -> Result<()> {
        if self.checkpoint.application.is_some() || !self.checkpoint.outbox.is_empty() {
            return Err(BraidError::Config(
                "Cannot initialize a populated channel journal".into(),
            ));
        }
        let mut next = self.checkpoint.clone();
        next.receive_frontier = frontier.clone();
        next.local_frontier = frontier;
        next.application = Some(application);
        self.commit(next).await
    }

    async fn commit(&mut self, next: ChannelCheckpoint) -> Result<()> {
        let bytes = serde_json::to_vec(&next)?;
        if bytes.len() > 32 * 1024 * 1024 {
            return Err(BraidError::ResourceLimit {
                resource: "channel journal",
                limit: 32 * 1024 * 1024,
            });
        }
        if let Some(path) = &self.path {
            crate::durable::atomic_write(path, &bytes).await?;
        }
        self.checkpoint = next;
        Ok(())
    }
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
}
impl Default for ChannelConfig {
    fn default() -> Self {
        Self {
            max_pending: 64,
            max_pending_bytes: 16 * 1024 * 1024,
            put_timeout: Duration::from_secs(20),
            retry: RetryConfig::default(),
            resume_local_frontier: false,
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
    Save(serde_json::Value, oneshot::Sender<Result<()>>),
    Reconnect,
    Stop(oneshot::Sender<()>),
}

pub struct ReliableChannel {
    commands: mpsc::Sender<Command>,
    updates: async_channel::Receiver<Result<Update>>,
    status: watch::Receiver<ChannelStatus>,
    task: tokio::task::JoinHandle<()>,
}
impl ReliableChannel {
    pub fn status(&self) -> watch::Receiver<ChannelStatus> {
        self.status.clone()
    }
    pub async fn next(&self) -> Option<Result<Update>> {
        self.updates.recv().await.ok()
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
        let _ = (&mut self.task).await;
    }
}
impl Drop for ReliableChannel {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl BraidClient {
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
}

impl ReliableChannel {
    pub fn start(
        network: Arc<dyn BraidNetwork>,
        url: String,
        request: BraidRequest,
        journal: ChannelJournal,
        config: ChannelConfig,
    ) -> Result<Self> {
        super::validate_resource_url(&url)?;
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
        let (tx, updates) = async_channel::bounded(1);
        let (status_tx, status) = watch::channel(ChannelStatus {
            state: ChannelState::Disconnected,
            outstanding_puts: journal.checkpoint.outbox.len(),
            durable_receive_frontier: journal.checkpoint.receive_frontier.clone(),
            reconnects: 0,
            last_error: None,
        });
        let task = tokio::spawn(async move {
            let mut driver = Driver {
                network,
                url,
                request,
                journal,
                config,
                commands: rx,
                updates: tx,
                status: status_tx,
                awaiting_apply: None,
            };
            if let Err(error) = driver.run().await {
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
        });
        Ok(Self {
            commands,
            updates,
            status,
            task,
        })
    }
}

struct Driver {
    network: Arc<dyn BraidNetwork>,
    url: String,
    request: BraidRequest,
    journal: ChannelJournal,
    config: ChannelConfig,
    commands: mpsc::Receiver<Command>,
    updates: async_channel::Sender<Result<Update>>,
    status: watch::Sender<ChannelStatus>,
    awaiting_apply: Option<Vec<Version>>,
}
impl Driver {
    fn state(&self, state: ChannelState) {
        self.status.send_modify(|s| {
            s.state = state;
            s.outstanding_puts = self.journal.checkpoint.outbox.len();
            s.durable_receive_frontier = self.journal.checkpoint.receive_frontier.clone();
        });
    }
    async fn command(&mut self, command: Option<Command>) -> Result<bool> {
        match command {
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
    async fn enqueue(
        &mut self,
        update: Update,
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
        crate::protocol::format_update(&update)?;
        if let Some(old) = self
            .journal
            .checkpoint
            .outbox
            .iter()
            .find(|old| old.version == update.version)
        {
            if serde_json::to_vec(old)? == serde_json::to_vec(&update)? {
                return Ok(());
            }
            return Err(BraidError::Protocol(
                "Version reused with different queued payload".into(),
            ));
        }
        let mut next = self.journal.checkpoint.clone();
        next.local_frontier = update.version.clone();
        next.outbox.push_back(update);
        if next.outbox.len() > self.config.max_pending
            || serde_json::to_vec(&next.outbox)?.len() > self.config.max_pending_bytes
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
            let error = match connected {
                Ok(receiver) => {
                    self.state(ChannelState::CatchingUp);
                    self.state(ChannelState::Online);
                    match self.session(receiver).await {
                        Ok(()) => {
                            retries.reset();
                            continue;
                        }
                        Err(BraidError::Aborted) => return Ok(()),
                        Err(e) => e,
                    }
                }
                Err(error) => error,
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
            self.status.send_modify(|s| {
                s.reconnects = s.reconnects.saturating_add(1);
                s.last_error = Some(error.to_string());
            });
            let sleep = tokio::time::sleep(
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
    async fn session(&mut self, receiver: async_channel::Receiver<Result<Update>>) -> Result<()> {
        let mut inflight: Option<
            futures::future::BoxFuture<'static, Result<crate::BraidResponse>>,
        > = None;
        loop {
            if inflight.is_none() {
                if let Some(update) = self.journal.checkpoint.outbox.front() {
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
                    let timeout = self.config.put_timeout;
                    inflight = Some(Box::pin(async move {
                        tokio::time::timeout(timeout, network.fetch(&url, request))
                            .await
                            .map_err(|_| BraidError::Timeout)?
                    }));
                }
            }
            tokio::select! {
                command = self.commands.recv() => if self.command(command).await? { return Ok(()); },
                result = async { match &mut inflight { Some(future) => future.await, None => std::future::pending().await } } => {
                    let response = result?;
                    if !response.is_success() { return Err(BraidError::SubscriptionStatus { status: response.status, retry_after: response.header("retry-after").and_then(parse_retry_after) }); }
                    let mut next = self.journal.checkpoint.clone();
                    next.outbox.pop_front();
                    self.journal.commit(next).await?;
                    inflight = None;
                    self.state(ChannelState::Online);
                }
                update = receiver.recv(), if self.awaiting_apply.is_none() => {
                    let update = update.map_err(|_| BraidError::SubscriptionClosed)??;
                    if let Some(repr) = update.extra_headers.get("repr-type") { self.request.extra_headers.insert("repr-type".into(), repr.clone()); }
                    self.awaiting_apply = Some(update.version.clone());
                    self.updates.send(Ok(update)).await.map_err(|_| BraidError::Aborted)?;
                }
            }
        }
    }
}
