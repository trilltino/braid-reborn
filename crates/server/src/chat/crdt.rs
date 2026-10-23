//! Diamond-type CRDT Wrapper for Chat
//!
//! Provides a simplified API for chat rooms to use Diamond-types
//! conflict resolution with full edit history support.

use crate::core::models::{BlobRef, ChatUpdate, EditRecord, Message, MessageType};
use braid_core::core::merge::diamond::DiamondCRDT;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ChatCrdtState {
    pub room_id: String,
    pub node_id: String,
    pub next_seq: u64,
    pub current_version: Vec<braid_http::types::Version>,
    pub version_graph: HashMap<String, Vec<braid_http::types::Version>>,
    pub messages: HashMap<String, Message>,
}

pub struct ChatCrdt {
    inner: DiamondCRDT,
    room_id: String,
    node_id: String,
    next_seq: u64,
    messages: HashMap<String, Message>,
    /// Track message IDs by their version (for edit lookups)
    version_to_msg: HashMap<String, String>,
}

impl ChatCrdt {
    pub fn new(room_id: &str, node_id: &str) -> Self {
        Self {
            inner: DiamondCRDT::new(node_id),
            room_id: room_id.to_string(),
            node_id: node_id.to_string(),
            next_seq: 1,
            messages: HashMap::new(),
            version_to_msg: HashMap::new(),
        }
    }

    pub fn import_state(state: ChatCrdtState) -> Self {
        let mut crdt = Self::new(&state.room_id, &state.node_id);
        crdt.next_seq = state.next_seq;
        
        // Rebuild message index
        for (version, msg) in &state.messages {
            crdt.messages.insert(msg.id.clone(), msg.clone());
            crdt.version_to_msg.insert(version.clone(), msg.id.clone());
        }
        
        crdt
    }

    pub fn export_state(&self) -> ChatCrdtState {
        ChatCrdtState {
            room_id: self.room_id.clone(),
            node_id: self.node_id.clone(),
            next_seq: self.next_seq,
            current_version: self.get_frontier(),
            version_graph: HashMap::new(), // Simplified for now
            messages: self.messages.clone(),
        }
    }

    pub fn get_frontier(&self) -> Vec<braid_http::types::Version> {
        vec![braid_http::types::Version::String(format!(
            "{}@{}",
            self.next_seq.saturating_sub(1),
            self.node_id
        ))]
    }

    /// Generate a new version ID
    fn generate_version(&mut self) -> String {
        let version = format!("{}@{}", self.next_seq, self.node_id);
        self.next_seq += 1;
        version
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
        let version = self.generate_version();
        let msg_id = Uuid::new_v4().to_string();

        let message = Message {
            id: msg_id.clone(),
            sender: sender.to_string(),
            content: content.to_string(),
            message_type: msg_type,
            version: version.clone(),
            parents: self.get_frontier(),
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
        let msg = self.messages.get(msg_id)
            .ok_or_else(|| anyhow::anyhow!("Message not found: {}", msg_id))?;
        
        // Only the original sender can edit
        if msg.sender != editor {
            anyhow::bail!("Only the original sender can edit this message");
        }
        
        // Cannot edit deleted messages
        if msg.deleted {
            anyhow::bail!("Cannot edit a deleted message");
        }

        let old_version = msg.version.clone();
        
        // Generate new version for this edit
        let new_version = self.generate_version();
        let parents = vec![braid_http::types::Version::String(old_version)];
        
        // Now get mutable reference to modify
        let msg = self.messages.get_mut(msg_id).unwrap();
        
        // Add edit to history
        msg.add_edit(new_version.clone(), new_content.to_string(), parents);
        
        // Track the new version
        let msg_clone = msg.clone();
        self.version_to_msg.insert(new_version.clone(), msg_id.to_string());
        
        Ok((new_version, msg_clone))
    }

    /// Soft-delete a message (sets deleted flag)
    pub fn delete_message(
        &mut self,
        msg_id: &str,
        deleter: &str,
    ) -> anyhow::Result<(String, Message)> {
        // Check permissions first
        let msg = self.messages.get(msg_id)
            .ok_or_else(|| anyhow::anyhow!("Message not found: {}", msg_id))?;
        
        // Only sender can delete
        if msg.sender != deleter {
            anyhow::bail!("Only the original sender can delete this message");
        }
        
        let version = self.generate_version();
        
        // Now modify
        let msg = self.messages.get_mut(msg_id).unwrap();
        msg.deleted = true;
        msg.edited_at = Some(Utc::now());
        
        Ok((version, msg.clone()))
    }

    /// Get a message by ID
    pub fn get_message(&self, msg_id: &str) -> Option<&Message> {
        self.messages.get(msg_id)
    }

    /// Get message by version
    pub fn get_message_by_version(&self, version: &str) -> Option<&Message> {
        self.version_to_msg.get(version)
            .and_then(|msg_id| self.messages.get(msg_id))
    }

    /// Get edit history for a message
    pub fn get_edit_history(&self, msg_id: &str) -> Option<&[EditRecord]> {
        self.messages.get(msg_id)
            .map(|msg| msg.edit_history.as_slice())
    }

    /// Get all messages
    pub fn messages(&self) -> &HashMap<String, Message> {
        &self.messages
    }

    /// Get messages sorted by creation time
    pub fn get_messages_sorted(&self) -> Vec<&Message> {
        let mut msgs: Vec<_> = self.messages.values().collect();
        msgs.sort_by(|a, b| a.created_at.cmp(&b.created_at));
        msgs
    }

    /// Get messages for a specific user
    pub fn get_messages_by_sender(&self, sender: &str) -> Vec<&Message> {
        self.messages.values()
            .filter(|m| m.sender == sender)
            .collect()
    }

    /// Add a reaction to a message
    pub fn add_reaction(
        &mut self,
        msg_id: &str,
        emoji: &str,
        user: &str,
    ) -> anyhow::Result<()> {
        let msg = self.messages.get_mut(msg_id)
            .ok_or_else(|| anyhow::anyhow!("Message not found"))?;
        
        // Check if user already reacted with this emoji
        if msg.reactions.iter().any(|r| r.emoji == emoji && r.user == user) {
            anyhow::bail!("Already reacted");
        }
        
        msg.reactions.push(crate::core::models::Reaction {
            emoji: emoji.to_string(),
            user: user.to_string(),
            timestamp: Utc::now(),
        });
        
        Ok(())
    }

    /// Remove a reaction
    pub fn remove_reaction(
        &mut self,
        msg_id: &str,
        emoji: &str,
        user: &str,
    ) -> anyhow::Result<()> {
        let msg = self.messages.get_mut(msg_id)
            .ok_or_else(|| anyhow::anyhow!("Message not found"))?;
        
        msg.reactions.retain(|r| !(r.emoji == emoji && r.user == user));
        Ok(())
    }

    /// Merge updates from remote
    pub fn merge_updates(&mut self, updates: Vec<ChatUpdate>) -> Vec<Message> {
        let mut new_msgs = Vec::new();
        
        for update in updates {
            for patch in update.patches {
                match patch {
                    crate::core::models::ChatPatch::AddMessage {
                        id,
                        content,
                        sender,
                        message_type,
                    } => {
                        if !self.messages.contains_key(&id) {
                            let m = Message::new(
                                id.clone(),
                                sender,
                                content,
                                update.version.clone(),
                                update.parents.clone(),
                            ).with_message_type(message_type);
                            
                            self.messages.insert(id.clone(), m.clone());
                            self.version_to_msg.insert(update.version.clone(), id);
                            new_msgs.push(m);
                        }
                    }
                    crate::core::models::ChatPatch::EditMessage { id, new_content } => {
                        if let Some(msg) = self.messages.get_mut(&id) {
                            msg.add_edit(
                                update.version.clone(),
                                new_content,
                                update.parents.clone(),
                            );
                        }
                    }
                    crate::core::models::ChatPatch::DeleteMessage { id } => {
                        if let Some(msg) = self.messages.get_mut(&id) {
                            msg.deleted = true;
                            msg.edited_at = Some(Utc::now());
                        }
                    }
                    crate::core::models::ChatPatch::AddReaction { msg_id, emoji, user } => {
                        let _ = self.add_reaction(&msg_id, &emoji, &user);
                    }
                    crate::core::models::ChatPatch::RemoveReaction { msg_id, emoji, user } => {
                        let _ = self.remove_reaction(&msg_id, &emoji, &user);
                    }
                }
            }
        }
        
        new_msgs
    }

    /// Generate sync updates for Braid protocol
    pub fn generate_sync_braid(
        &self,
        _known_versions: &[braid_http::types::Version],
    ) -> Vec<ChatUpdate> {
        // Return all messages as updates
        // Real implementation would filter by known_versions
        vec![]
    }

    /// Get the underlying Diamond CRDT content (for serialization)
    pub fn get_crdt_content(&self) -> String {
        self.inner.content()
    }

    /// Import from Diamond CRDT content
    pub fn import_from_crdt(&mut self, content: &str) {
        // Clear and rebuild
        self.messages.clear();
        self.version_to_msg.clear();
        
        // Parse content as JSON Lines of messages
        for line in content.lines() {
            if let Ok(msg) = serde_json::from_str::<Message>(line) {
                self.version_to_msg.insert(msg.version.clone(), msg.id.clone());
                self.messages.insert(msg.id.clone(), msg);
            }
        }
    }

    /// Export to Diamond CRDT content format (JSON Lines)
    pub fn export_to_crdt(&self) -> String {
        let msgs = self.get_messages_sorted();
        msgs.iter()
            .map(|m| serde_json::to_string(m).unwrap_or_default())
            .collect::<Vec<_>>()
            .join("\n")
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
        let (edit_version, edited_msg) = crdt.edit_message(&msg_id, "Hello world!", "alice").unwrap();
        
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
