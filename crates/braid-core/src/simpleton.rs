//! Parent-based Simpleton client, following pinned braid-text/client/simpleton.js.
//! Offline text remains pending; the server transforms edits against their parents.
//! This is a coordinated client, not a general multi-writer merge engine.
use base64::Engine;
use braid_http::{
    client::{
        ChannelConfig, ChannelDiagnostics, ChannelEvent, ChannelJournal, ChannelState, OwnedTask,
        ReliableChannel,
    },
    protocol::text::apply_text_patches,
    types::{BraidRequest, Patch, Update, Version},
    BraidClient, BraidError, Result,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::{mpsc, oneshot, watch};

const MAX_TEXT_BYTES: usize = 8 * 1024 * 1024;
/// Version 2 adds the verified acknowledged base. Older states have no schema field.
const STATE_SCHEMA: u32 = 2;
const MAX_CONSECUTIVE_REPAIRS: u32 = 2;

/// Text at a causal frontier that the server has acknowledged, with its digest.
/// Pending intent and immutable outbox entries are always relative to this anchor.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct VerifiedBase {
    text: String,
    frontier: Vec<Version>,
    digest: String,
}
impl VerifiedBase {
    fn of(text: &str, frontier: &[Version]) -> Self {
        Self {
            text: text.to_owned(),
            frontier: frontier.to_vec(),
            digest: digest(text),
        }
    }
}

/// Whether the persisted acknowledged base can be proven.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoveryState {
    Verified,
    /// A pre-recovery journal has queued edits but no recorded acknowledged base.
    /// The original bytes are preserved and behavior is unchanged until the
    /// baseline is materialized from the server or the queue drains.
    NeedsLegacyBaseline,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TextState {
    #[serde(default)]
    schema: u32,
    #[serde(default)]
    base: Option<VerifiedBase>,
    peer: String,
    version: Vec<Version>,
    text: String,
    pending: Option<String>,
    char_counter: i64,
    initialized: bool,
    #[serde(default)]
    last_operation: u64,
    #[serde(default)]
    revision: u64,
}
impl TextState {
    fn new() -> Result<Self> {
        #[cfg(target_arch = "wasm32")]
        let peer = braid_http::client::browser::checked_peer_id()?;
        #[cfg(not(target_arch = "wasm32"))]
        let peer = uuid::Uuid::new_v4().to_string();
        Ok(Self {
            schema: STATE_SCHEMA,
            base: None,
            peer,
            version: vec![],
            text: String::new(),
            pending: None,
            char_counter: -1,
            initialized: false,
            last_operation: 0,
            revision: 0,
        })
    }
}

#[derive(Clone, Debug)]
pub struct SimpletonStatus {
    pub text: String,
    pub version: Vec<Version>,
    pub online: bool,
    pub outstanding_puts: usize,
    pub has_pending_edit: bool,
    pub initialized: bool,
    pub error: Option<String>,
    pub last_operation: u64,
    pub revision: u64,
    pub recovery: RecoveryState,
}

enum Command {
    Changed(
        String,
        Option<u64>,
        Option<u64>,
        oneshot::Sender<Result<()>>,
    ),
    Stop(oneshot::Sender<()>),
}

pub struct SimpletonClient {
    commands: mpsc::Sender<Command>,
    status: watch::Receiver<SimpletonStatus>,
    diagnostics: watch::Receiver<ChannelDiagnostics>,
    task: OwnedTask,
}
/// A previously persisted text replica, with an optional dirty local file.
pub struct SimpletonBase {
    pub text: String,
    pub version: Vec<Version>,
    pub pending: Option<String>,
    pub initialized: bool,
    pub last_operation: u64,
}
impl Drop for SimpletonClient {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl SimpletonClient {
    pub async fn connect(client: &BraidClient, url: &str, journal: ChannelJournal) -> Result<Self> {
        Self::connect_with_request(client, url, journal, BraidRequest::new()).await
    }
    pub async fn connect_with_request(
        client: &BraidClient,
        url: &str,
        journal: ChannelJournal,
        request: BraidRequest,
    ) -> Result<Self> {
        Self::connect_seeded(client, url, journal, request, None).await
    }
    pub async fn connect_seeded(
        client: &BraidClient,
        url: &str,
        mut journal: ChannelJournal,
        request: BraidRequest,
        base: Option<SimpletonBase>,
    ) -> Result<Self> {
        #[cfg(target_arch = "wasm32")]
        braid_http::client::browser::check_runtime_capabilities()?;
        if journal.checkpoint().application.is_none() {
            if let Some(base) = base {
                check_size(&base.text)?;
                if let Some(pending) = &base.pending {
                    check_size(pending)?;
                }
                let state = TextState {
                    base: base
                        .initialized
                        .then(|| VerifiedBase::of(&base.text, &base.version)),
                    text: base.text,
                    version: base.version,
                    pending: base.pending,
                    initialized: base.initialized,
                    last_operation: base.last_operation,
                    ..TextState::new()?
                };
                journal
                    .initialize_application(state.version.clone(), serde_json::to_value(state)?)
                    .await?;
            }
        }
        let mut state: TextState = match &journal.checkpoint().application {
            Some(value) => serde_json::from_value(value.clone())?,
            None => TextState::new()?,
        };
        let mut recovery = RecoveryState::Verified;
        if state.schema < STATE_SCHEMA && journal.checkpoint().application.is_some() {
            // Keep the exact pre-migration bytes before anything is rewritten.
            journal.backup_original("pre-recovery-v2").await?;
            if !state.initialized {
                state.schema = STATE_SCHEMA;
            } else {
                // A legacy cursor is not proof that the saved bytes materialize
                // that frontier, even when its outbox is empty.
                recovery = RecoveryState::NeedsLegacyBaseline;
            }
        }
        check_size(&state.text)?;
        if let Some(pending) = &state.pending {
            check_size(pending)?;
        }
        let credentials = request.extra_headers.clone();
        let request = request
            .with_merge_type("simpleton")
            .with_peer(&state.peer)
            .with_heartbeat(5);
        let channel = client.reliable_channel(
            url,
            request,
            journal,
            ChannelConfig {
                resume_local_frontier: true,
                application_acknowledgments: true,
                ..Default::default()
            },
        )?;
        let diagnostics = channel.diagnostics();
        channel
            .save_application(serde_json::to_value(&state)?)
            .await?;
        let (commands, rx) = mpsc::channel(16);
        let (tx, status) = watch::channel(status_of(&state, recovery, false, 0, None));
        let client = client.clone();
        let url = url.to_owned();
        let task = OwnedTask::spawn(async move {
            let mut driver = Driver {
                state,
                channel,
                commands: rx,
                status: tx,
                online: false,
                outstanding: 0,
                recovery,
                repairs: 0,
                recovery_attempted: false,
                client,
                url,
                credentials,
            };
            if let Err(error) = driver.run().await {
                let status = status_of(
                    &driver.state,
                    driver.recovery,
                    false,
                    driver.outstanding,
                    Some(error.to_string()),
                );
                driver.status.send_replace(status);
            }
            driver.channel.close().await;
        })?;
        Ok(Self {
            commands,
            status,
            diagnostics,
            task,
        })
    }
    pub fn status(&self) -> watch::Receiver<SimpletonStatus> {
        self.status.clone()
    }
    /// Channel state, pending count, receive frontier, retry reason and repair cause.
    pub fn diagnostics(&self) -> watch::Receiver<ChannelDiagnostics> {
        self.diagnostics.clone()
    }
    /// Returns only after the edit is durable, whether online or offline.
    pub async fn changed(&self, text: String) -> Result<()> {
        self.changed_identified(text, None, None).await
    }
    /// Monotonic filesystem operation identity. Replaying an older operation
    /// cannot overwrite edits already journaled after it.
    pub async fn changed_for_operation(&self, sequence: u64, text: String) -> Result<()> {
        if sequence == 0 {
            return Err(BraidError::Config(
                "Operation sequence must be positive".into(),
            ));
        }
        self.changed_identified(text, Some(sequence), None).await
    }
    pub async fn changed_if_revision(&self, revision: u64, text: String) -> Result<()> {
        self.changed_identified(text, None, Some(revision)).await
    }
    async fn changed_identified(
        &self,
        text: String,
        sequence: Option<u64>,
        revision: Option<u64>,
    ) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(Command::Changed(text, sequence, revision, tx))
            .await
            .map_err(|_| BraidError::SubscriptionClosed)?;
        rx.await.map_err(|_| BraidError::SubscriptionClosed)?
    }
    pub async fn close(mut self) {
        self.shutdown().await;
        self.task.join().await;
    }
    /// Graceful shared-owner shutdown, including release of the journal lease.
    pub async fn shutdown(&self) {
        let (tx, rx) = oneshot::channel();
        if self.commands.send(Command::Stop(tx)).await.is_ok() {
            let _ = rx.await;
        }
        let mut status = self.status.clone();
        while status.changed().await.is_ok() {}
    }
}

fn status_of(
    state: &TextState,
    recovery: RecoveryState,
    online: bool,
    outstanding_puts: usize,
    error: Option<String>,
) -> SimpletonStatus {
    SimpletonStatus {
        text: state.pending.as_ref().unwrap_or(&state.text).clone(),
        version: state.version.clone(),
        online,
        outstanding_puts,
        has_pending_edit: state.pending.is_some(),
        initialized: state.initialized,
        error,
        last_operation: state.last_operation,
        revision: state.revision,
        recovery,
    }
}
fn check_size(text: &str) -> Result<()> {
    if text.len() > MAX_TEXT_BYTES {
        Err(BraidError::ResourceLimit {
            resource: "Simpleton text",
            limit: MAX_TEXT_BYTES as u64,
        })
    } else {
        Ok(())
    }
}
fn versions_eq(a: &[Version], b: &[Version]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut remaining = b.to_vec();
    for version in a {
        match remaining.iter().position(|candidate| candidate == version) {
            Some(index) => {
                remaining.swap_remove(index);
            }
            None => return false,
        }
    }
    true
}

struct Driver {
    state: TextState,
    channel: ReliableChannel,
    commands: mpsc::Receiver<Command>,
    status: watch::Sender<SimpletonStatus>,
    online: bool,
    outstanding: usize,
    recovery: RecoveryState,
    repairs: u32,
    recovery_attempted: bool,
    client: BraidClient,
    url: String,
    credentials: std::collections::BTreeMap<String, String>,
}
impl Driver {
    fn publish(&self) {
        self.status.send_replace(status_of(
            &self.state,
            self.recovery,
            self.online,
            self.outstanding,
            None,
        ));
    }
    async fn run(&mut self) -> Result<()> {
        let mut channel_status = self.channel.status();
        loop {
            // Read the current value too: the channel may connect before this task starts.
            let current = channel_status.borrow_and_update().clone();
            self.online = current.state == ChannelState::Online;
            self.outstanding = current.outstanding_puts;
            if !self.online {
                self.recovery_attempted = false;
            } else if self.recovery == RecoveryState::NeedsLegacyBaseline
                && !self.recovery_attempted
            {
                self.recovery_attempted = true;
                match self.recover_legacy_baseline().await {
                    Ok(()) => {},
                    Err(BraidError::Aborted) => return Ok(()),
                    Err(error) => return Err(error),
                }
            }
            if self.online && self.state.initialized && self.outstanding < 10
                && self.recovery == RecoveryState::Verified {
                if let Some(pending) = self.state.pending.clone() {
                    if pending == self.state.text {
                        let mut next = self.state.clone();
                        next.pending = None;
                        self.channel
                            .save_application(serde_json::to_value(&next)?)
                            .await?;
                        self.state = next;
                        self.channel.reconnect().await?;
                    } else {
                        self.send(pending, None).await?;
                    }
                }
            }
            self.publish();
            tokio::select! {
                changed = channel_status.changed() => {
                    if changed.is_err() { return Err(BraidError::SubscriptionClosed); }
                    if channel_status.borrow().state == ChannelState::RepairRequired {
                        return Err(BraidError::Subscription(channel_status.borrow().last_error.clone().unwrap_or_else(|| "Channel requires repair".into())));
                    }
                }
                event = self.channel.next_event() => match event.ok_or(BraidError::SubscriptionClosed)?? {
                    ChannelEvent::Update(update) => self.receive(*update).await?,
                    ChannelEvent::PutAcknowledged { update, .. } => self.acknowledge(*update).await?,
                },
                command = self.commands.recv() => match command {
                    None => return Ok(()),
                    Some(Command::Stop(done)) => { let _ = done.send(()); return Ok(()); },
                    Some(Command::Changed(text, sequence, revision, done)) => {
                        let result = if revision.is_some_and(|revision| revision != self.state.revision) {Err(BraidError::MergeConflict("Application revision changed".into()))} else {self.changed(text,sequence).await};
                        self.publish();
                        let _ = done.send(result);
                    }
                }
            }
        }
    }
    async fn changed(&mut self, text: String, sequence: Option<u64>) -> Result<()> {
        if sequence.is_some_and(|sequence| sequence <= self.state.last_operation) {
            return Ok(());
        }
        check_size(&text)?;
        if self.online && self.state.initialized && self.outstanding < 10
            && self.recovery == RecoveryState::Verified {
            self.send(text, sequence).await
        } else {
            let mut next = self.state.clone();
            next.revision = next_revision(next.revision)?;
            next.pending = Some(text);
            if let Some(sequence) = sequence {
                next.last_operation = sequence;
            }
            self.channel
                .save_application(serde_json::to_value(&next)?)
                .await?;
            self.state = next;
            Ok(())
        }
    }
    async fn send(&mut self, text: String, sequence: Option<u64>) -> Result<()> {
        if text == self.state.text {
            if let Some(sequence) = sequence {
                let mut next = self.state.clone();
                next.revision = next_revision(next.revision)?;
                next.last_operation = sequence;
                next.pending = None;
                self.channel
                    .save_application(serde_json::to_value(&next)?)
                    .await?;
                self.state = next;
            }
            return Ok(());
        }
        let (patch, count) = scalar_diff(&self.state.text, &text)?;
        let mut next = self.state.clone();
        next.revision = next_revision(next.revision)?;
        if let Some(sequence) = sequence {
            next.last_operation = sequence;
        }
        next.char_counter = next
            .char_counter
            .checked_add(
                i64::try_from(count)
                    .map_err(|_| BraidError::InvalidVersion("Text counter overflow".into()))?,
            )
            .ok_or_else(|| BraidError::InvalidVersion("Text counter overflow".into()))?;
        let version = Version::new(format!("{}-{}", next.peer, next.char_counter));
        let update = Update::patched(version.clone(), vec![patch])
            .with_parents(next.version.clone())
            .with_merge_type("simpleton")
            .with_content_type("text/plain")
            .with_header("repr-digest", digest(&text));
        next.text = text;
        next.version = vec![version];
        next.pending = None;
        self.channel
            .queue(update, Some(serde_json::to_value(&next)?))
            .await?;
        self.state = next;
        self.outstanding = self.channel.status().borrow().outstanding_puts;
        Ok(())
    }
    async fn receive(&mut self, update: Update) -> Result<()> {
        let mut next = self.state.clone();
        let accepted = (!next.initialized && update.body.is_some())
            || (next.pending.is_none() && versions_eq(&next.version, &update.parents));
        if accepted {
            next.revision = next_revision(next.revision)?;
            let text = match (&update.body, &update.patches) {
                (Some(body), None) => String::from_utf8(body.to_vec())?,
                (None, Some(patches)) => apply_text_patches(&next.text, patches)?,
                _ => {
                    return Err(BraidError::Protocol(
                        "Simpleton update needs exactly one snapshot or patch list".into(),
                    ))
                }
            };
            check_size(&text)?;
            if let Some(expected) = update.extra_headers.get("repr-digest") {
                if expected != &digest(&text) {
                    return self.repair().await;
                }
            }
            if update.version.is_empty() && update.body.is_none() {
                return Err(BraidError::InvalidVersion(
                    "Simpleton patch has no version".into(),
                ));
            }
            next.text = text;
            next.version = update.version;
            next.initialized = true;
            // With nothing awaiting acknowledgment the received state is the acknowledged state.
            if self.outstanding == 0 {
                next.base = Some(VerifiedBase::of(&next.text, &next.version));
                if next.schema < STATE_SCHEMA {
                    next.schema = STATE_SCHEMA;
                    self.recovery = RecoveryState::Verified;
                }
            }
        }
        // Ignored updates cannot advance the application's known frontier.
        if accepted {
            self.channel
                .applied(next.version.clone(), Some(serde_json::to_value(&next)?))
                .await?;
        } else {
            self.channel.ignored().await?;
        }
        if accepted {
            self.repairs = 0;
        }
        self.state = next;
        Ok(())
    }
    /// Advance the verified base with the accepted edit, atomically with removal
    /// of its outbox entry. The edit must reproduce its own recorded digest.
    async fn acknowledge(&mut self, update: Update) -> Result<()> {
        let mut next = self.state.clone();
        let remaining = self.outstanding.saturating_sub(1);
        if let Some(base) = &next.base {
            let patches = update.patches.as_ref().ok_or_else(|| {
                BraidError::Protocol("Acknowledged Simpleton edit has no patches".into())
            })?;
            if !versions_eq(&base.frontier, &update.parents) {
                return Err(BraidError::Protocol(
                    "Acknowledged edit does not continue the verified base".into(),
                ));
            }
            let text = apply_text_patches(&base.text, patches)?;
            let actual = digest(&text);
            if update.extra_headers.get("repr-digest") != Some(&actual) {
                return Err(BraidError::Protocol(
                    "Acknowledged edit does not reproduce its recorded digest".into(),
                ));
            }
            next.base = Some(VerifiedBase {
                text,
                frontier: update.version.clone(),
                digest: actual,
            });
        } else if next.initialized
            && remaining == 0
            && versions_eq(&next.version, &update.version)
            && update.extra_headers.get("repr-digest") == Some(&digest(&next.text))
        {
            // A legacy queue drained completely: its final state is the acknowledged state.
            next.base = Some(VerifiedBase::of(&next.text, &next.version));
            next.schema = STATE_SCHEMA;
            self.recovery = RecoveryState::Verified;
        }
        self.channel
            .commit_ack(update.version.clone(), Some(serde_json::to_value(&next)?))
            .await?;
        self.state = next;
        Ok(())
    }
    /// Rebuild text at an acknowledged frontier plus the immutable queued edits.
    /// Every edit must continue the previous frontier and reproduce its digest.
    fn replay(base: &VerifiedBase, outbox: &[Update]) -> Result<(String, Vec<Version>)> {
        let mut text = base.text.clone();
        let mut frontier = base.frontier.clone();
        for entry in outbox {
            let patches = entry.patches.as_ref().ok_or_else(|| {
                BraidError::Protocol("Queued Simpleton edit has no patches".into())
            })?;
            if !versions_eq(&frontier, &entry.parents) {
                return Err(BraidError::Protocol(
                    "Queued edits do not continue the verified base".into(),
                ));
            }
            text = apply_text_patches(&text, patches)?;
            if entry.extra_headers.get("repr-digest") != Some(&digest(&text)) {
                return Err(BraidError::Protocol(
                    "Queued edit does not reproduce its recorded digest".into(),
                ));
            }
            frontier = entry.version.clone();
        }
        Ok((text, frontier))
    }
    /// A delivered update failed its digest. Discard it without moving the receive
    /// cursor and restore text from verified state. Pending intent and immutable
    /// queued edits are never replaced by a newer snapshot.
    async fn repair(&mut self) -> Result<()> {
        self.repairs += 1;
        if self.repairs > MAX_CONSECUTIVE_REPAIRS {
            return Err(BraidError::Protocol(
                "Repr-Digest mismatch persisted after repair".into(),
            ));
        }
        self.channel.ignored().await?;
        let outbox = self.channel.pending_updates().await?;
        let mut next = self.state.clone();
        next.revision = next_revision(next.revision)?;
        if outbox.is_empty() {
            next.initialized = false;
            next.text.clear();
            next.version.clear();
            next.base = None;
            self.channel
                .reset_cursor(serde_json::to_value(&next)?)
                .await?;
        } else {
            let base = next.base.clone().ok_or_else(|| {
                BraidError::Protocol(
                    "Repr-Digest mismatch with queued edits and no verified base".into(),
                )
            })?;
            let (text, frontier) = Self::replay(&base, &outbox)?;
            next.text = text;
            next.version = frontier;
            self.channel
                .save_application(serde_json::to_value(&next)?)
                .await?;
        }
        self.state = next;
        self.channel.reconnect().await?;
        Ok(())
    }
    /// Materialize the acknowledged text at the first queued edit's parents and
    /// accept it only if replaying every recorded edit reproduces each recorded
    /// digest and the persisted optimistic text. Failure keeps the legacy state.
    async fn recover_legacy_baseline(&mut self) -> Result<()> {
        let outbox = self.channel.pending_updates().await?;
        let frontier = outbox.first().map(|first| first.parents.clone())
            .unwrap_or_else(|| self.state.version.clone());
        let Some(text) = self.fetch_legacy_base(&frontier).await? else { return Ok(()); };
        let candidate = VerifiedBase::of(&text, &frontier);
        match Self::replay(&candidate, &outbox) {
            Ok((replayed, at))
                if replayed == self.state.text && versions_eq(&at, &self.state.version) =>
            {
                let mut next = self.state.clone();
                next.base = Some(candidate);
                next.schema = STATE_SCHEMA;
                self.channel
                    .save_application(serde_json::to_value(&next)?)
                    .await?;
                self.state = next;
                self.recovery = RecoveryState::Verified;
            }
            _ => tracing::warn!("Legacy Simpleton baseline could not be verified"),
        }
        Ok(())
    }

    async fn fetch_legacy_base(&mut self, frontier: &[Version]) -> Result<Option<String>> {
        if frontier.is_empty() { return Ok(Some(String::new())); }
        let mut request = BraidRequest::new().with_versions(frontier.to_vec());
        request.extra_headers = self.credentials.clone();
        let client = self.client.clone();
        let url = self.url.clone();
        let fetch = client.fetch(&url, request);
        let timeout = braid_http::client::sleep(std::time::Duration::from_secs(20));
        tokio::pin!(fetch, timeout);
        let response = loop {
            tokio::select! {
                response = &mut fetch => break response,
                _ = &mut timeout => return Ok(None),
                command = self.commands.recv() => match command {
                    None => return Err(BraidError::Aborted),
                    Some(Command::Stop(done)) => { let _ = done.send(()); return Err(BraidError::Aborted); },
                    Some(Command::Changed(text, sequence, revision, done)) => {
                        let result = if revision.is_some_and(|revision| revision != self.state.revision) {
                            Err(BraidError::MergeConflict("Application revision changed".into()))
                        } else { self.changed(text, sequence).await };
                        self.publish();
                        let _ = done.send(result);
                    }
                },
            }
        };
        match response {
                Ok(response) if response.status == 200 => {
                    // A successful historical lookup must identify the selected
                    // frontier. Never label a latest-state fallback as the old
                    // base merely because its text happens to replay correctly.
                    if !response.get_version().as_ref().is_some_and(|at| versions_eq(at, frontier)) {
                        return Ok(None);
                    }
                    if response.body.len() > MAX_TEXT_BYTES {
                        return Ok(None);
                    }
                    let text = match std::str::from_utf8(&response.body) {
                        Ok(text) => text,
                        Err(_) => return Ok(None),
                    };
                    if response.header("repr-digest").is_some_and(|expected| expected != digest(text)) {
                        return Ok(None);
                    }
                    Ok(Some(text.to_owned()))
                }
                _ => Ok(None),
        }
    }
}

pub(crate) fn digest(text: &str) -> String {
    format!(
        "sha-256=:{}:",
        base64::engine::general_purpose::STANDARD.encode(Sha256::digest(text.as_bytes()))
    )
}
fn next_revision(revision: u64) -> Result<u64> {
    revision
        .checked_add(1)
        .ok_or_else(|| BraidError::InvalidVersion("Application revision overflow".into()))
}
pub(crate) fn scalar_diff(old: &str, new: &str) -> Result<(Patch, usize)> {
    let old: Vec<char> = old.chars().collect();
    let new: Vec<char> = new.chars().collect();
    let prefix = old.iter().zip(&new).take_while(|(a, b)| a == b).count();
    let suffix = old[prefix..]
        .iter()
        .rev()
        .zip(new[prefix..].iter().rev())
        .take_while(|(a, b)| a == b)
        .count();
    let end = old.len() - suffix;
    let inserted: String = new[prefix..new.len() - suffix].iter().collect();
    let count = end - prefix + inserted.chars().count();
    Ok((
        Patch::new("text", format!("[{prefix}:{end}]"), inserted),
        count,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unicode_diff_roundtrips_and_counts_scalar_operations() {
        for (before, after) in [
            ("a\u{1f600}b", "a\u{1f601}b"),
            ("", "hello"),
            ("abc", ""),
            ("same", "same"),
            ("a\u{301}", "\u{e1}"),
        ] {
            let (patch, _) = scalar_diff(before, after).unwrap();
            assert_eq!(apply_text_patches(before, &[patch]).unwrap(), after);
        }
        assert_eq!(scalar_diff("a\u{1f600}b", "a\u{1f601}b").unwrap().1, 2);
    }
    #[test]
    #[cfg(all(feature = "serde", feature = "smallvec", not(target_arch = "wasm32")))]
    fn peer_versions_at_the_engine_agent_limit_do_not_panic() {
        for identity in [
            "x".repeat(49),
            "x".repeat(50),
            "ROOT".into(),
            "\u{1f600}".repeat(13),
        ] {
            let mut document = crate::core::merge::DiamondCRDT::new(identity);
            document.add_insert(0, "safe");
            assert_eq!(document.content(), "safe");
        }
    }
}
