//! Diamond-type CRDT Wrapper for Chat
//!
//! Provides a simplified API for chat rooms to use Diamond-types
//! conflict resolution with full edit history support.

use crate::core::models::{BlobRef, ChatPatch, ChatUpdate, EditRecord, Message, MessageType};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ChatCrdtState {
    #[serde(default)]
    pub events: Vec<ChatUpdate>,
    pub room_id: String,
    pub node_id: String,
    pub next_seq: u64,
    pub current_version: Vec<braid_http::types::Version>,
    pub version_graph: HashMap<String, Vec<braid_http::types::Version>>,
    pub messages: HashMap<String, Message>,
}

#[derive(Clone)]
pub struct ChatCrdt {
    events: Vec<ChatUpdate>,
    room_id: String,
    node_id: String,
    next_seq: u64,
    messages: HashMap<String, Message>,
    /// Track message IDs by their version (for edit lookups)
    version_to_msg: HashMap<String, String>,
    frontier: Vec<braid_http::types::Version>,
    version_graph: HashMap<String, Vec<braid_http::types::Version>>,
}

impl ChatCrdt {
    /// Reconstruct retained structured events at a causal frontier. Legacy
    /// snapshots can still serve their current view, but cannot invent events.
    pub fn snapshot_at(
        &self,
        frontier: &[braid_http::types::Version],
    ) -> Result<Self, braid_core::replica::ReplicaError> {
        use braid_core::replica::ReplicaError;
        let ids: Vec<_> = frontier.iter().map(ToString::to_string).collect();
        let current: Vec<_> = self.frontier.iter().map(ToString::to_string).collect();
        if braid_core::replica::same_frontier(&ids, &current) {
            return Ok(self.clone());
        }
        let known = self
            .ancestors(frontier)
            .map_err(|_| ReplicaError::HistoryUnavailable(frontier.to_vec()))?;
        let retained: std::collections::HashSet<_> = self
            .events
            .iter()
            .map(|event| event.version.as_str())
            .collect();
        if known.iter().any(|id| !retained.contains(id.as_str())) {
            return Err(ReplicaError::HistoryUnavailable(frontier.to_vec()));
        }
        let mut candidate = Self::new(&self.room_id, &self.node_id);
        for event in self
            .events
            .iter()
            .filter(|event| known.contains(&event.version))
        {
            candidate
                .merge_updates(vec![event.clone()])
                .map_err(|error| {
                    ReplicaError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        error.to_string(),
                    ))
                })?;
        }
        Ok(candidate)
    }

    pub fn validate_interval(
        &self,
        from: &[braid_http::types::Version],
        to: &[braid_http::types::Version],
    ) -> Result<(), braid_core::replica::ReplicaError> {
        use braid_core::replica::ReplicaError;
        let from_set = self
            .ancestors(from)
            .map_err(|_| ReplicaError::HistoryUnavailable(from.to_vec()))?;
        let to_set = self
            .ancestors(to)
            .map_err(|_| ReplicaError::HistoryUnavailable(to.to_vec()))?;
        if !from_set.is_subset(&to_set) {
            return Err(ReplicaError::Invalid(
                "Chat history boundaries are not causally ordered".into(),
            ));
        }
        Ok(())
    }

    pub fn new(room_id: &str, node_id: &str) -> Self {
        Self {
            events: Vec::new(),
            room_id: room_id.to_string(),
            node_id: node_id.to_string(),
            next_seq: 1,
            messages: HashMap::new(),
            version_to_msg: HashMap::new(),
            frontier: Vec::new(),
            version_graph: HashMap::new(),
        }
    }

    pub fn import_state(state: ChatCrdtState) -> Self {
        let mut crdt = Self::new(&state.room_id, &state.node_id);
        crdt.events = state.events;
        crdt.next_seq = state.next_seq.max(1);
        crdt.version_graph = state.version_graph;

        // Rebuild message index
        for msg in state.messages.values() {
            crdt.messages.insert(msg.id.clone(), msg.clone());
            for edit in &msg.edit_history {
                crdt.version_to_msg
                    .insert(edit.version.clone(), msg.id.clone());
                crdt.version_graph
                    .entry(edit.version.clone())
                    .or_insert_with(|| {
                        edit.parents
                            .iter()
                            .filter(|p| p.to_string() != edit.version)
                            .cloned()
                            .collect()
                    });
            }
            crdt.version_to_msg
                .insert(msg.version.clone(), msg.id.clone());
            // Older exports omitted the graph and accidentally used self-parents.
            // Preserve every stored message/edit; do not invent missing historical edits.
            crdt.version_graph
                .entry(msg.version.clone())
                .or_insert_with(|| {
                    msg.parents
                        .iter()
                        .filter(|p| p.to_string() != msg.version)
                        .cloned()
                        .collect()
                });
        }
        let parents: std::collections::HashSet<_> = crdt
            .version_graph
            .values()
            .flatten()
            .map(ToString::to_string)
            .collect();
        crdt.frontier = crdt
            .version_graph
            .keys()
            .filter(|id| !parents.contains(*id))
            .map(braid_http::types::Version::new)
            .collect();
        crdt.frontier.sort_by_key(ToString::to_string);

        crdt
    }

    pub fn export_state(&self) -> ChatCrdtState {
        ChatCrdtState {
            events: self.events.clone(),
            room_id: self.room_id.clone(),
            node_id: self.node_id.clone(),
            next_seq: self.next_seq,
            current_version: self.get_frontier(),
            version_graph: self.version_graph.clone(),
            messages: self.messages.clone(),
        }
    }

    pub fn get_frontier(&self) -> Vec<braid_http::types::Version> {
        self.frontier.clone()
    }

    /// Generate a new version ID
    fn generate_version(&mut self) -> String {
        let version = format!("{}@{}-{}", self.next_seq, self.node_id, Uuid::new_v4());
        self.next_seq = self.next_seq.saturating_add(1);
        version
    }

    fn record_version(&mut self, version: &str, parents: Vec<braid_http::types::Version>) {
        self.frontier.retain(|v| !parents.contains(v));
        self.frontier.push(braid_http::types::Version::new(version));
        self.frontier.sort_by_key(ToString::to_string);
        self.frontier.dedup();
        self.version_graph.insert(version.to_owned(), parents);
    }

    /// Add a new message to the chat
    pub fn add_message(
        &mut self,
        sender: &str,
        content: &str,
        msg_type: MessageType,
        reply_to: Option<&str>,
        blob_refs: Vec<BlobRef>,
    ) -> (String, Message) {
        let parents = self.get_frontier();
        let version = self.generate_version();
        let msg_id = Uuid::new_v4().to_string();

        let message = Message {
            id: msg_id.clone(),
            sender: sender.to_string(),
            content: content.to_string(),
            message_type: msg_type,
            version: version.clone(),
            parents: parents.clone(),
            created_at: Utc::now(),
            edited_at: None,
            edit_history: Vec::new(),
            reply_to: reply_to.map(|s| s.to_string()),
            reactions: Vec::new(),
            blob_refs,
            deleted: false,
        };

        self.messages.insert(msg_id.clone(), message.clone());
        self.version_to_msg.insert(version.clone(), msg_id);
        self.record_version(&version, parents.clone());
        self.events.push(ChatUpdate {
            version: version.clone(),
            parents,
            patches: vec![ChatPatch::AddMessage {
                id: message.id.clone(),
                sender: message.sender.clone(),
                content: message.content.clone(),
                message_type: message.message_type.clone(),
                reply_to: message.reply_to.clone(),
                blob_refs: message.blob_refs.clone(),
            }],
            timestamp: message.created_at,
            author: sender.to_owned(),
        });
        (version, message)
    }

    /// Edit an existing message
    ///
    /// # Arguments
    /// * `msg_id` - The message ID to edit
    /// * `new_content` - The new content
    /// * `editor` - Who is making the edit (must be original sender)
    ///
    /// # Returns
    /// Ok((version, message)) on success, Err on failure
    pub fn edit_message(
        &mut self,
        msg_id: &str,
        new_content: &str,
        editor: &str,
    ) -> anyhow::Result<(String, Message)> {
        // Check message exists and permissions first
        let msg = self
            .messages
            .get(msg_id)
            .ok_or_else(|| anyhow::anyhow!("Message not found: {}", msg_id))?;

        // Only the original sender can edit
        if msg.sender != editor {
            anyhow::bail!("Only the original sender can edit this message");
        }

        // Cannot edit deleted messages
        if msg.deleted {
            anyhow::bail!("Cannot edit a deleted message");
        }

        let parents = self.get_frontier();

        // Generate new version for this edit
        let new_version = self.generate_version();

        // Now get mutable reference to modify
        let msg = self
            .messages
            .get_mut(msg_id)
            .ok_or_else(|| anyhow::anyhow!("Message disappeared during edit"))?;

        // Add edit to history
        msg.add_edit(
            new_version.clone(),
            new_content.to_string(),
            parents.clone(),
        );

        // Track the new version
        let msg_clone = msg.clone();
        self.version_to_msg
            .insert(new_version.clone(), msg_id.to_string());
        self.record_version(&new_version, parents.clone());
        self.events.push(ChatUpdate {
            version: new_version.clone(),
            parents,
            patches: vec![ChatPatch::EditMessage {
                id: msg_id.to_owned(),
                new_content: new_content.to_owned(),
            }],
            timestamp: msg_clone.edited_at.unwrap_or(msg_clone.created_at),
            author: editor.to_owned(),
        });

        Ok((new_version, msg_clone))
    }

    /// Soft-delete a message (sets deleted flag)
    pub fn delete_message(
        &mut self,
        msg_id: &str,
        deleter: &str,
    ) -> anyhow::Result<(String, Message)> {
        // Check permissions first
        let msg = self
            .messages
            .get(msg_id)
            .ok_or_else(|| anyhow::anyhow!("Message not found: {}", msg_id))?;

        // Only sender can delete
        if msg.sender != deleter {
            anyhow::bail!("Only the original sender can delete this message");
        }

        let parents = self.get_frontier();
        let version = self.generate_version();

        // Now modify
        let msg = self
            .messages
            .get_mut(msg_id)
            .ok_or_else(|| anyhow::anyhow!("Message disappeared during deletion"))?;
        msg.add_edit(version.clone(), msg.content.clone(), parents.clone());
        msg.deleted = true;
        let message = msg.clone();
        self.version_to_msg
            .insert(version.clone(), msg_id.to_owned());
        self.record_version(&version, parents.clone());
        self.events.push(ChatUpdate {
            version: version.clone(),
            parents,
            patches: vec![ChatPatch::DeleteMessage {
                id: msg_id.to_owned(),
            }],
            timestamp: message.edited_at.unwrap_or(message.created_at),
            author: deleter.to_owned(),
        });
        Ok((version, message))
    }

    /// Get a message by ID
    pub fn get_message(&self, msg_id: &str) -> Option<&Message> {
        self.messages.get(msg_id)
    }

    /// Get message by version
    pub fn get_message_by_version(&self, version: &str) -> Option<&Message> {
        self.version_to_msg
            .get(version)
            .and_then(|msg_id| self.messages.get(msg_id))
    }

    /// Get edit history for a message
    pub fn get_edit_history(&self, msg_id: &str) -> Option<&[EditRecord]> {
        self.messages
            .get(msg_id)
            .map(|msg| msg.edit_history.as_slice())
    }

    /// Get all messages
    pub fn messages(&self) -> &HashMap<String, Message> {
        &self.messages
    }

    /// Get messages sorted by creation time
    pub fn get_messages_sorted(&self) -> Vec<&Message> {
        let mut msgs: Vec<_> = self.messages.values().collect();
        msgs.sort_by_key(|a| a.created_at);
        msgs
    }

    /// Get messages for a specific user
    pub fn get_messages_by_sender(&self, sender: &str) -> Vec<&Message> {
        self.messages
            .values()
            .filter(|m| m.sender == sender)
            .collect()
    }

    pub fn add_reaction(&mut self, msg_id: &str, emoji: &str, user: &str) -> anyhow::Result<()> {
        self.local_reaction(
            ChatPatch::AddReaction {
                msg_id: msg_id.to_owned(),
                emoji: emoji.to_owned(),
                user: user.to_owned(),
            },
            user,
        )
    }

    pub fn remove_reaction(&mut self, msg_id: &str, emoji: &str, user: &str) -> anyhow::Result<()> {
        self.local_reaction(
            ChatPatch::RemoveReaction {
                msg_id: msg_id.to_owned(),
                emoji: emoji.to_owned(),
                user: user.to_owned(),
            },
            user,
        )
    }

    fn local_reaction(&mut self, patch: ChatPatch, author: &str) -> anyhow::Result<()> {
        let mut candidate = self.clone();
        let update = ChatUpdate {
            version: candidate.generate_version(),
            parents: candidate.get_frontier(),
            patches: vec![patch],
            timestamp: Utc::now(),
            author: author.to_owned(),
        };
        candidate.merge_updates(vec![update])?;
        *self = candidate;
        Ok(())
    }

    /// Apply a complete validated batch to a candidate before publishing it.
    /// Chat edits are coordinated: a concurrent edit to the same message conflicts.
    pub fn merge_updates(&mut self, updates: Vec<ChatUpdate>) -> anyhow::Result<Vec<Message>> {
        let mut candidate = self.clone();
        let mut changed = Vec::new();
        for update in updates {
            if let Some(previous) = candidate
                .events
                .iter()
                .find(|e| e.version == update.version)
            {
                anyhow::ensure!(
                    serde_json::to_value(previous)? == serde_json::to_value(&update)?,
                    "Chat event ID reused with another payload"
                );
                continue;
            }
            anyhow::ensure!(
                !update.version.is_empty()
                    && !candidate.version_graph.contains_key(&update.version),
                "Unknown legacy event identity"
            );
            anyhow::ensure!(
                update.patches.len() == 1,
                "Each chat event must mutate one message"
            );
            anyhow::ensure!(
                update
                    .parents
                    .iter()
                    .all(|p| p.to_string() != update.version
                        && candidate.version_graph.contains_key(&p.to_string())),
                "Unknown or self chat parent"
            );
            let observed = candidate.ancestors(&update.parents)?;
            let patch = &update.patches[0];
            match patch {
                ChatPatch::AddMessage {
                    id,
                    content,
                    sender,
                    message_type,
                    reply_to,
                    blob_refs,
                } => {
                    anyhow::ensure!(
                        sender == &update.author && !candidate.messages.contains_key(id),
                        "Invalid chat sender or reused message ID"
                    );
                    let mut message =
                        Message::new(id, sender, content, &update.version, update.parents.clone())
                            .with_message_type(message_type.clone());
                    message.created_at = update.timestamp;
                    message.reply_to = reply_to.clone();
                    message.blob_refs = blob_refs.clone();
                    candidate.messages.insert(id.clone(), message.clone());
                    candidate
                        .version_to_msg
                        .insert(update.version.clone(), id.clone());
                    changed.push(message);
                }
                _ => {
                    let id = match patch {
                        ChatPatch::EditMessage { id, .. } | ChatPatch::DeleteMessage { id } => id,
                        ChatPatch::AddReaction { msg_id, .. }
                        | ChatPatch::RemoveReaction { msg_id, .. } => msg_id,
                        ChatPatch::AddMessage { id, .. } => id,
                    };
                    let message = candidate
                        .messages
                        .get_mut(id)
                        .ok_or_else(|| anyhow::anyhow!("Unknown message"))?;
                    anyhow::ensure!(
                        observed.contains(&message.version),
                        "Concurrent chat mutation requires resolution"
                    );
                    match patch {
                        ChatPatch::EditMessage { new_content, .. } => {
                            anyhow::ensure!(
                                message.sender == update.author && !message.deleted,
                                "Forbidden chat edit"
                            );
                            message.add_edit(
                                update.version.clone(),
                                new_content.clone(),
                                update.parents.clone(),
                            );
                        }
                        ChatPatch::DeleteMessage { .. } => {
                            anyhow::ensure!(
                                message.sender == update.author,
                                "Forbidden chat deletion"
                            );
                            message.add_edit(
                                update.version.clone(),
                                message.content.clone(),
                                update.parents.clone(),
                            );
                            message.deleted = true;
                        }
                        ChatPatch::AddReaction { emoji, user, .. } => {
                            anyhow::ensure!(user == &update.author, "Forbidden reaction author");
                            if !message
                                .reactions
                                .iter()
                                .any(|r| r.emoji == *emoji && r.user == *user)
                            {
                                message.reactions.push(crate::core::models::Reaction {
                                    emoji: emoji.clone(),
                                    user: user.clone(),
                                    timestamp: update.timestamp,
                                });
                            }
                        }
                        ChatPatch::RemoveReaction { emoji, user, .. } => {
                            anyhow::ensure!(user == &update.author, "Forbidden reaction author");
                            message
                                .reactions
                                .retain(|r| r.emoji != *emoji || r.user != *user);
                        }
                        _ => {}
                    }
                    message.version = update.version.clone();
                    message.parents = update.parents.clone();
                    message.edited_at = Some(update.timestamp);
                    candidate
                        .version_to_msg
                        .insert(update.version.clone(), id.clone());
                    changed.push(message.clone());
                }
            }
            candidate.record_version(&update.version, update.parents.clone());
            candidate.events.push(update);
        }
        *self = candidate;
        Ok(changed)
    }

    fn ancestors(
        &self,
        frontier: &[braid_http::types::Version],
    ) -> anyhow::Result<std::collections::HashSet<String>> {
        let mut pending: Vec<_> = frontier.iter().map(ToString::to_string).collect();
        let mut seen = std::collections::HashSet::new();
        while let Some(id) = pending.pop() {
            if !seen.insert(id.clone()) {
                continue;
            }
            let parents = self
                .version_graph
                .get(&id)
                .ok_or_else(|| anyhow::anyhow!("Unknown chat frontier"))?;
            pending.extend(parents.iter().map(ToString::to_string));
        }
        Ok(seen)
    }

    pub fn generate_sync_braid(
        &self,
        known_versions: &[braid_http::types::Version],
    ) -> anyhow::Result<Vec<ChatUpdate>> {
        let known = self.ancestors(known_versions)?;
        let available: std::collections::HashSet<_> =
            self.events.iter().map(|e| e.version.clone()).collect();
        anyhow::ensure!(self.version_graph.keys().all(|id| known.contains(id) || available.contains(id)), "Legacy room requires a full room snapshot; its missing event history cannot be reconstructed");
        Ok(self
            .events
            .iter()
            .filter(|e| !known.contains(&e.version))
            .cloned()
            .collect())
    }

    /// Chat state is structured application data, rather than a text CRDT log.
    pub fn get_crdt_content(&self) -> anyhow::Result<String> {
        self.export_to_crdt()
    }

    pub fn import_from_crdt(&mut self, content: &str) -> anyhow::Result<()> {
        let messages = content
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(serde_json::from_str::<Message>)
            .collect::<Result<Vec<_>, _>>()?;
        let mut state = ChatCrdtState {
            room_id: self.room_id.clone(),
            node_id: self.node_id.clone(),
            ..Default::default()
        };
        for message in messages {
            anyhow::ensure!(
                !state.messages.contains_key(&message.id),
                "Duplicate message in chat snapshot"
            );
            state.messages.insert(message.id.clone(), message);
        }
        *self = Self::import_state(state);
        Ok(())
    }

    pub fn export_to_crdt(&self) -> anyhow::Result<String> {
        Ok(self
            .get_messages_sorted()
            .iter()
            .map(serde_json::to_string)
            .collect::<Result<Vec<_>, _>>()?
            .join("\n"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_message() {
        let mut crdt = ChatCrdt::new("room1", "alice");
        let (version, msg) = crdt.add_message("alice", "Hello", MessageType::Text, None, vec![]);

        assert_eq!(msg.content, "Hello");
        assert_eq!(msg.sender, "alice");
        assert!(!msg.is_edited());
        assert!(version.contains("alice"));
    }

    #[test]
    fn test_edit_message() {
        let mut crdt = ChatCrdt::new("room1", "alice");
        let (_, msg) = crdt.add_message("alice", "Hello", MessageType::Text, None, vec![]);
        let msg_id = msg.id.clone();

        // Edit the message
        let (edit_version, edited_msg) =
            crdt.edit_message(&msg_id, "Hello world!", "alice").unwrap();

        assert_eq!(edited_msg.content, "Hello world!");
        assert!(edited_msg.is_edited());
        assert_eq!(edited_msg.edit_history.len(), 1);
        assert_eq!(edited_msg.edit_history[0].content, "Hello");
        assert!(edit_version.contains("alice"));
    }

    #[test]
    fn test_edit_unauthorized() {
        let mut crdt = ChatCrdt::new("room1", "alice");
        let (_, msg) = crdt.add_message("alice", "Hello", MessageType::Text, None, vec![]);

        // Bob tries to edit alice's message
        let result = crdt.edit_message(&msg.id, "Hacked!", "bob");
        assert!(result.is_err());
    }

    #[test]
    fn test_delete_message() {
        let mut crdt = ChatCrdt::new("room1", "alice");
        let (_, msg) = crdt.add_message("alice", "Hello", MessageType::Text, None, vec![]);

        let (_, deleted) = crdt.delete_message(&msg.id, "alice").unwrap();
        assert!(deleted.deleted);
    }
}
