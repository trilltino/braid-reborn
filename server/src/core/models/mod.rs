use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// A chat room with CRDT state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatRoom {
    pub id: String,
    pub name: String,
    pub created_at: DateTime<Utc>,
    pub created_by: String,
    pub participants: Vec<String>,
    #[serde(flatten)]
    pub crdt_state: CrdtState,
}

impl ChatRoom {
    pub fn new(
        id: impl Into<String>,
        name: impl Into<String>,
        created_by: impl Into<String>,
    ) -> Self {
        let id = id.into();
        let now = Utc::now();
        Self {
            id: id.clone(),
            name: name.into(),
            created_at: now,
            created_by: created_by.into(),
            participants: Vec::new(),
            crdt_state: CrdtState::new(&id),
        }
    }
}

/// CRDT state for a chat room
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrdtState {
    /// Node ID for this CRDT instance
    pub node_id: String,
    /// Next sequence number for version generation
    pub next_seq: u64,
    /// Current frontier versions (leaves of the DAG)
    pub current_version: Vec<braid_http::types::Version>,
    /// The version graph (DAG): version -> set of parent versions
    pub version_graph: HashMap<String, Vec<braid_http::types::Version>>,
    /// Messages in this room (version -> message)
    pub messages: HashMap<String, Message>,
}

impl CrdtState {
    pub fn new(node_id: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
            next_seq: 0,
            current_version: Vec::new(),
            version_graph: HashMap::new(),
            messages: HashMap::new(),
        }
    }

    /// Generate a new unique version ID
    pub fn generate_version(&mut self) -> String {
        let version = format!("{}@{}", self.next_seq, self.node_id);
        self.next_seq += 1;
        version
    }

    /// Add a version to the graph
    pub fn add_version(
        &mut self,
        version: String,
        parents: Vec<braid_http::types::Version>,
        message: Message,
    ) {
        // Update version graph
        self.version_graph.insert(version.clone(), parents.clone());

        // Update current version (frontier)
        for parent in parents {
            let parent_str = parent.to_string();
            self.current_version.retain(|v| v.to_string() != parent_str);
        }
        self.current_version
            .push(braid_http::types::Version::String(version.clone()));

        // Store message
        self.messages.insert(version, message);
    }

    /// Get messages sorted by causal order
    pub fn get_messages_sorted(&self) -> Vec<&Message> {
        let mut versions: Vec<_> = self.messages.iter().collect();
        // Sort by timestamp for now (could be improved with topological sort)
        versions.sort_by(|a, b| a.1.created_at.cmp(&b.1.created_at));
        versions.into_iter().map(|(_, msg)| msg).collect()
    }
}

/// Edit record for message history
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EditRecord {
    /// Version ID of this edit
    pub version: String,
    /// When the edit occurred
    pub timestamp: DateTime<Utc>,
    /// Content at this version
    pub content: String,
    /// Parent versions
    pub parents: Vec<braid_http::types::Version>,
}

/// A single chat message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub id: String,
    pub sender: String,
    pub content: String,
    #[serde(rename = "type")]
    pub message_type: MessageType,
    pub version: String,
    pub parents: Vec<braid_http::types::Version>,
    pub created_at: DateTime<Utc>,
    pub edited_at: Option<DateTime<Utc>>,
    /// Full edit history (empty for never-edited messages)
    #[serde(default)]
    pub edit_history: Vec<EditRecord>,
    pub reply_to: Option<String>,
    pub reactions: Vec<Reaction>,
    pub blob_refs: Vec<BlobRef>,
    /// Tombstone for deleted messages
    #[serde(default)]
    pub deleted: bool,
}

impl Message {
    pub fn new(
        id: impl Into<String>,
        sender: impl Into<String>,
        content: impl Into<String>,
        version: impl Into<String>,
        parents: Vec<braid_http::types::Version>,
    ) -> Self {
        Self {
            id: id.into(),
            sender: sender.into(),
            content: content.into(),
            message_type: MessageType::Text,
            version: version.into(),
            parents,
            created_at: Utc::now(),
            edited_at: None,
            edit_history: Vec::new(),
            reply_to: None,
            reactions: Vec::new(),
            blob_refs: Vec::new(),
            deleted: false,
        }
    }

    /// Check if this message has been edited
    pub fn is_edited(&self) -> bool {
        !self.edit_history.is_empty()
    }

    /// Get the original content (first version)
    pub fn original_content(&self) -> Option<&str> {
        self.edit_history.first().map(|e| e.content.as_str())
    }

    /// Add an edit to the history
    pub fn add_edit(&mut self, version: String, content: String, parents: Vec<braid_http::types::Version>) {
        // Save current content to history before updating
        if self.edit_history.is_empty() {
            // First edit - save original
            self.edit_history.push(EditRecord {
                version: self.version.clone(),
                timestamp: self.created_at,
                content: self.content.clone(),
                parents: self.parents.clone(),
            });
        }
        
        // Update to new content
        self.content = content;
        self.version = version;
        self.parents = parents;
        self.edited_at = Some(Utc::now());
    }

    pub fn with_blob(mut self, blob_ref: BlobRef) -> Self {
        self.blob_refs.push(blob_ref);
        self
    }

    pub fn with_message_type(mut self, msg_type: MessageType) -> Self {
        self.message_type = msg_type;
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "data")]
pub enum MessageType {
    Text,
    Image {
        width: Option<u32>,
        height: Option<u32>,
    },
    File {
        filename: String,
        size: u64,
    },
    System {
        action: String,
    },
}

impl Default for MessageType {
    fn default() -> Self {
        MessageType::Text
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Reaction {
    pub emoji: String,
    pub user: String,
    pub timestamp: DateTime<Utc>,
}

/// Reference to a blob in storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobRef {
    pub hash: String,
    pub content_type: String,
    pub filename: String,
    pub size: u64,
    /// Inline data for small blobs (base64 encoded)
    pub inline_data: Option<String>,
}

/// Presence information (who's online)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Presence {
    pub user: String,
    pub status: PresenceStatus,
    pub last_seen: DateTime<Utc>,
    pub current_room: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PresenceStatus {
    Online,
    Away,
    Offline,
}

/// Typing indicator
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypingIndicator {
    pub user: String,
    pub room_id: String,
    pub is_typing: bool,
    pub timestamp: DateTime<Utc>,
}

/// Chat room snapshot (returned by Braid GET)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatSnapshot {
    pub room: ChatRoom,
    pub messages: Vec<Message>,
}

/// Input for creating a message
#[derive(Debug, Deserialize)]
pub struct CreateMessageInput {
    pub content: String,
    #[serde(default = "default_message_type")]
    pub message_type: MessageTypeInput,
    pub reply_to: Option<String>,
    pub blob_refs: Option<Vec<BlobRefInput>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "data")]
pub enum MessageTypeInput {
    Text,
    Image {
        width: Option<u32>,
        height: Option<u32>,
    },
    File {
        filename: String,
        size: u64,
    },
}

impl Default for MessageTypeInput {
    fn default() -> Self {
        MessageTypeInput::Text
    }
}

#[derive(Debug, Deserialize)]
pub struct BlobRefInput {
    pub hash: String,
    pub content_type: String,
    pub filename: String,
    pub size: u64,
}

fn default_message_type() -> MessageTypeInput {
    MessageTypeInput::Text
}

/// CRDT update for synchronization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatUpdate {
    pub version: String,
    pub parents: Vec<braid_http::types::Version>,
    pub patches: Vec<ChatPatch>,
    pub timestamp: DateTime<Utc>,
    pub author: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "action")]
pub enum ChatPatch {
    AddMessage {
        id: String,
        content: String,
        sender: String,
        #[serde(rename = "type")]
        message_type: MessageType,
    },
    EditMessage {
        id: String,
        new_content: String,
    },
    DeleteMessage {
        id: String,
    },
    AddReaction {
        msg_id: String,
        emoji: String,
        user: String,
    },
    RemoveReaction {
        msg_id: String,
        emoji: String,
        user: String,
    },
}

/// Sync status for a room
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoomSyncStatus {
    pub room_id: String,
    pub status: SyncStatus,
    pub last_sync: Option<DateTime<Utc>>,
    pub pending_changes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncStatus {
    Connected,
    Disconnected,
    Syncing,
    Offline,
    Reconnecting,
}

/// Draft message for offline support
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DraftMessage {
    pub local_id: String,
    pub room_id: String,
    pub content: String,
    pub created_at: DateTime<Utc>,
    pub message_type: MessageType,
}
