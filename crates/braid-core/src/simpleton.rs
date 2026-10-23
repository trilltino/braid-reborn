//! Parent-based Simpleton client, following pinned braid-text/client/simpleton.js.
//! Offline text remains pending; the server transforms edits against their parents.
//! This is a coordinated client, not a general multi-writer merge engine.
use base64::Engine;
use braid_http::{
    client::{ChannelConfig, ChannelJournal, ChannelState, ReliableChannel},
    protocol::text::apply_text_patches,
    types::{BraidRequest, Patch, Update, Version},
    BraidClient, BraidError, Result,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::{mpsc, oneshot, watch};

const MAX_TEXT_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TextState {
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
impl Default for TextState {
    fn default() -> Self {
        Self {
            peer: uuid::Uuid::new_v4().to_string(),
            version: vec![],
            text: String::new(),
            pending: None,
            char_counter: -1,
            initialized: false,
            last_operation: 0,
            revision: 0,
        }
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
    task: tokio::task::JoinHandle<()>,
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
        if journal.checkpoint().application.is_none() {
            if let Some(base) = base {
                check_size(&base.text)?;
                if let Some(pending) = &base.pending {
                    check_size(pending)?;
                }
                let state = TextState {
                    text: base.text,
                    version: base.version,
                    pending: base.pending,
                    initialized: base.initialized,
                    last_operation: base.last_operation,
                    ..Default::default()
                };
                journal
                    .initialize_application(state.version.clone(), serde_json::to_value(state)?)
                    .await?;
            }
        }
        let state: TextState = match &journal.checkpoint().application {
            Some(value) => serde_json::from_value(value.clone())?,
            None => TextState::default(),
        };
        check_size(&state.text)?;
        if let Some(pending) = &state.pending {
            check_size(pending)?;
        }
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
                ..Default::default()
            },
        )?;
        channel
            .save_application(serde_json::to_value(&state)?)
            .await?;
        let (commands, rx) = mpsc::channel(16);
        let (tx, status) = watch::channel(status_of(&state, false, 0, None));
        let task = tokio::spawn(async move {
            let mut driver = Driver {
                state,
                channel,
                commands: rx,
                status: tx,
                online: false,
                outstanding: 0,
            };
            if let Err(error) = driver.run().await {
                let status = status_of(
                    &driver.state,
                    false,
                    driver.outstanding,
                    Some(error.to_string()),
                );
                driver.status.send_replace(status);
            }
            driver.channel.close().await;
        });
        Ok(Self {
            commands,
            status,
            task,
        })
    }
    pub fn status(&self) -> watch::Receiver<SimpletonStatus> {
        self.status.clone()
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
        let _ = (&mut self.task).await;
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
}
impl Driver {
    fn publish(&self) {
        self.status
            .send_replace(status_of(&self.state, self.online, self.outstanding, None));
    }
    async fn run(&mut self) -> Result<()> {
        let mut channel_status = self.channel.status();
        loop {
            // Read the current value too: the channel may connect before this task starts.
            let current = channel_status.borrow_and_update().clone();
            self.online = current.state == ChannelState::Online;
            self.outstanding = current.outstanding_puts;
            if self.online && self.state.initialized && self.outstanding < 10 {
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
                update = self.channel.next() => self.receive(update.ok_or(BraidError::SubscriptionClosed)??).await?,
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
        if self.online && self.state.initialized && self.outstanding < 10 {
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
                    return Err(BraidError::Protocol("Repr-Digest mismatch".into()));
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
        }
        // Ignored updates cannot advance the application's known frontier.
        if accepted {
            self.channel
                .applied(next.version.clone(), Some(serde_json::to_value(&next)?))
                .await?;
        } else {
            self.channel.ignored().await?;
        }
        self.state = next;
        Ok(())
    }
}

fn digest(text: &str) -> String {
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
