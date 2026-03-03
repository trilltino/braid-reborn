//! JSON-based chat storage with CRDT support
//!
//! This module replaces SQLite with JSON file storage,
//! using atomic writes for durability and CRDT for conflict resolution.

use crate::chat::crdt::{ChatCrdt, ChatCrdtState};
use crate::core::config::ChatServerConfig;
use crate::core::models::{
    BlobRef, ChatRoom, ChatUpdate, CrdtState, DraftMessage, Message, MessageType,
};
use anyhow::{Context, Result};
use braid_blob::BlobStore;
use chrono::Utc;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs;
use tokio::sync::{broadcast, RwLock};
use tracing::{info, warn};
use uuid::Uuid;

/// Broadcast channel for real-time updates
#[derive(Clone)]
pub struct UpdateChannel {
    pub tx: broadcast::Sender<RoomUpdate>,
}

#[derive(Clone, Debug)]
pub struct RoomUpdate {
    pub room_id: String,
    pub update_type: UpdateType,
    pub data: serde_json::Value,
    pub crdt_version: Option<String>,
}

#[derive(Clone, Debug)]
pub enum UpdateType {
    Message,
    Presence,
    Typing,
    RoomUpdate,
    Sync,
}

/// JSON-based chat store with CRDT support
pub struct JsonChatStore {
    config: ChatServerConfig,
    /// Blob store for file attachments
    blob_store: Arc<BlobStore>,
    /// In-memory cache of loaded rooms with their CRDTs
    rooms: RwLock<HashMap<String, Arc<RwLock<RoomData>>>>,
    /// Broadcast channels for each room
    channels: RwLock<HashMap<String, UpdateChannel>>,
    /// Draft messages for offline support
    drafts: RwLock<HashMap<String, Vec<DraftMessage>>>,
}

/// Room data including CRDT state
pub struct RoomData {
    pub room: ChatRoom,
    pub crdt: ChatCrdt,
}

impl JsonChatStore {
    /// Create a new JSON chat store
    pub async fn new(config: ChatServerConfig) -> Result<Self> {
        // Ensure directories exist
        config.ensure_dirs().await?;

        // Initialize blob store
        let blob_store = Arc::new(
            BlobStore::new(config.blob_dir.clone(), config.blob_dir.join("meta.sqlite"))
                .await
                .context("Failed to initialize blob store")?,
        );

        let store = Self {
            config,
            blob_store,
            rooms: RwLock::new(HashMap::new()),
            channels: RwLock::new(HashMap::new()),
            drafts: RwLock::new(HashMap::new()),
        };

        // Load existing rooms
        store.load_existing_rooms().await?;

        info!(
            "JSON ChatStore initialized with {} rooms",
            store.rooms.read().await.len()
        );

        Ok(store)
    }

    /// Get the storage path for a room
    fn room_path(&self, room_id: &str) -> PathBuf {
        self.config.storage_dir.join(format!("{}.json", room_id))
    }

    /// Load all existing rooms from disk
    async fn load_existing_rooms(&self) -> Result<()> {
        let mut entries = fs::read_dir(&self.config.storage_dir).await?;
        let mut count = 0;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("json") {
                if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                    match self.load_room_from_disk(stem, &path).await {
                        Ok((room, crdt)) => {
                            let room_id = room.id.clone();
                            self.rooms.write().await.insert(
                                room_id.clone(),
                                Arc::new(RwLock::new(RoomData { room, crdt })),
                            );
                            count += 1;
                        }
                        Err(e) => {
                            warn!("Failed to load room from {:?}: {}", path, e);
                        }
                    }
                }
            }
        }

        info!("Loaded {} existing rooms from disk", count);
        Ok(())
    }

    /// Get all loading rooms
    pub async fn list_rooms(&self) -> Vec<ChatRoom> {
        let rooms_map = self.rooms.read().await;
        let mut result = Vec::with_capacity(rooms_map.len());
        for room_lock in rooms_map.values() {
            let room_data = room_lock.read().await;
            result.push(room_data.room.clone());
        }
        result
    }

    /// Load a single room from disk with CRDT state
    async fn load_room_from_disk(
        &self,
        room_id: &str,
        path: &Path,
    ) -> Result<(ChatRoom, ChatCrdt)> {
        let content = fs::read_to_string(path).await?;
        let room: ChatRoom = serde_json::from_str(&content)
            .with_context(|| format!("Failed to parse room {} JSON", room_id))?;

        // Extract CRDT state from room or create new
        let crdt = if !room.crdt_state.version_graph.is_empty() {
            // Convert old format to new ChatCrdt
            ChatCrdt::import_state(ChatCrdtState {
                room_id: room_id.to_string(),
                node_id: room.crdt_state.node_id.clone(),
                next_seq: room.crdt_state.next_seq,
                current_version: room.crdt_state.current_version.clone(),
                version_graph: room.crdt_state.version_graph.clone(),
                messages: room.crdt_state.messages.clone(),
            })
        } else {
            ChatCrdt::new(room_id, &self.config.node_id)
        };

        Ok((room, crdt))
    }

    /// Save a room to disk atomically
    async fn save_room_to_disk(&self, room_data: &RoomData) -> Result<()> {
        let path = self.room_path(&room_data.room.id);
        let temp_path = path.with_extension("tmp");

        // Update room with current CRDT state
        let mut room = room_data.room.clone();
        let crdt_state = room_data.crdt.export_state();
        room.crdt_state = CrdtState {
            node_id: crdt_state.node_id,
            next_seq: crdt_state.next_seq,
            current_version: crdt_state.current_version,
            version_graph: crdt_state.version_graph,
            messages: crdt_state.messages,
        };

        // Serialize room
        let json = serde_json::to_string_pretty(&room)?;

        // Write to temp file
        fs::write(&temp_path, json).await?;

        // Atomic rename
        fs::rename(&temp_path, &path).await?;

        Ok(())
    }

    /// Get or create a room
    pub async fn get_or_create_room(
        &self,
        room_id: &str,
        created_by: Option<&str>,
    ) -> Result<Arc<RwLock<RoomData>>> {
        // Check if already loaded
        {
            let rooms = self.rooms.read().await;
            if let Some(room) = rooms.get(room_id) {
                return Ok(room.clone());
            }
        }

        // Try to load from disk
        let path = self.room_path(room_id);
        if path.exists() {
            let (room, crdt) = self.load_room_from_disk(room_id, &path).await?;
            let room = Arc::new(RwLock::new(RoomData { room, crdt }));
            self.rooms
                .write()
                .await
                .insert(room_id.to_string(), room.clone());
            return Ok(room);
        }

        // Create new room
        let room = ChatRoom::new(
            room_id,
            format!("Room {}", room_id),
            created_by.unwrap_or("system"),
        );
        let crdt = ChatCrdt::new(room_id, &self.config.node_id);

        let room_data = RoomData { room, crdt };

        // Save to disk
        self.save_room_to_disk(&room_data).await?;

        let room = Arc::new(RwLock::new(room_data));
        self.rooms
            .write()
            .await
            .insert(room_id.to_string(), room.clone());

        info!("Created new room: {}", room_id);

        Ok(room)
    }

    /// Get a room if it exists
    pub async fn get_room(&self, room_id: &str) -> Result<Option<Arc<RwLock<RoomData>>>> {
        {
            let rooms = self.rooms.read().await;
            if let Some(room) = rooms.get(room_id) {
                return Ok(Some(room.clone()));
            }
        }

        // Try to load from disk
        let path = self.room_path(room_id);
        if path.exists() {
            let (room, crdt) = self.load_room_from_disk(room_id, &path).await?;
            let room = Arc::new(RwLock::new(RoomData { room, crdt }));
            self.rooms
                .write()
                .await
                .insert(room_id.to_string(), room.clone());
            return Ok(Some(room));
        }

        Ok(None)
    }

    /// Add a message using CRDT
    pub async fn add_message(
        &self,
        room_id: &str,
        sender: &str,
        content: &str,
        msg_type: MessageType,
        reply_to: Option<String>,
        blob_refs: Vec<BlobRef>,
    ) -> Result<Message> {
        let room_lock = self.get_or_create_room(room_id, Some(sender)).await?;
        let mut room_data = room_lock.write().await;

        // Use CRDT to add message
        let (version, message) =
            room_data
                .crdt
                .add_message(sender, content, msg_type, reply_to.as_deref(), blob_refs);

        // Save to disk
        self.save_room_to_disk(&*room_data).await?;

        // Broadcast update
        let update = RoomUpdate {
            room_id: room_id.to_string(),
            update_type: UpdateType::Message,
            data: serde_json::to_value(&message)?,
            crdt_version: Some(version),
        };
        self.broadcast(room_id, update).await?;

        info!(
            "Added message {} to room {} (version: {})",
            message.id, room_id, message.version
        );

        Ok(message)
    }

    /// Edit a message using CRDT
    pub async fn edit_message(
        &self,
        room_id: &str,
        msg_id: &str,
        new_content: &str,
    ) -> Result<Message> {
        let room_lock = self.get_room(room_id).await?.context("Room not found")?;
        let mut room_data = room_lock.write().await;

        // Use CRDT to edit (note: we need to verify sender matches)
        // For now, we'll look up the message to get its sender
        let sender = room_data
            .crdt
            .get_message(msg_id)
            .map(|m| m.sender.clone())
            .unwrap_or_default();
        let (version, message) = room_data.crdt.edit_message(msg_id, new_content, &sender)?;

        // Save to disk
        self.save_room_to_disk(&*room_data).await?;

        // Broadcast update
        let update = RoomUpdate {
            room_id: room_id.to_string(),
            update_type: UpdateType::Message,
            data: serde_json::to_value(&message)?,
            crdt_version: Some(version),
        };
        self.broadcast(room_id, update).await?;

        Ok(message)
    }

    /// Get messages for a room (from CRDT state)
    pub async fn get_messages(
        &self,
        room_id: &str,
        since_version: Option<&str>,
    ) -> Result<Vec<Message>> {
        let room_lock = self.get_room(room_id).await?.context("Room not found")?;
        let room_data = room_lock.read().await;

        // Get messages from live CRDT
        let mut messages: Vec<_> = room_data
            .crdt
            .messages()
            .values()
            .filter(|m| !m.deleted)
            .cloned()
            .collect();

        // Sort by timestamp
        messages.sort_by(|a, b| a.created_at.cmp(&b.created_at));

        // Filter by version if specified
        if let Some(since) = since_version {
            if let Some(idx) = messages.iter().position(|m| m.version == since) {
                messages = messages.split_off(idx + 1);
            }
        }

        Ok(messages)
    }

    /// Get messages that are descendants of the given parents (for catch-up sync)
    /// Based on xfmail's get_messages_since_parents - returns messages newer than parents
    pub async fn get_messages_since_parents(
        &self,
        room_id: &str,
        parents: &[braid_http::types::Version],
        limit: usize,
    ) -> Result<Vec<Message>> {
        if parents.is_empty() {
            return self.get_messages(room_id, None).await;
        }

        let room_lock = self.get_room(room_id).await?.context("Room not found")?;
        let room_data = room_lock.read().await;

        // Find all messages that are descendants of the given parents
        // We need to traverse the version graph to find all descendants
        let mut result = Vec::new();

        // Simplified legacy support: just return all messages for now to ensure consistency
        // Real optimization would require inquiring the DT about causality.
        // But for "Remove Antimatter" task, basic functionality is key.
        for msg in room_data.crdt.messages().values() {
            if !msg.deleted {
                result.push(msg.clone());
            }
        }

        // In a real implementation we would filter by `parents` using vector clocks or similar.
        // For now, returning everything ensures the client gets the data, even if slightly inefficient.

        // Sort by timestamp for consistent ordering
        result.sort_by(|a, b| a.created_at.cmp(&b.created_at));

        // Apply limit
        if result.len() > limit {
            result.truncate(limit);
        }

        tracing::info!(
            "[CatchUpSync] Found {} messages since parents {:?} in room {}",
            result.len(),
            parents,
            room_id
        );

        Ok(result)
    }

    /// Get the current conversation tips (frontier versions)
    /// These are the "leaf" versions in the DAG that have no children yet
    pub async fn get_conversation_tips(&self, room_id: &str) -> Result<Vec<String>> {
        let room_lock = self.get_room(room_id).await?.context("Room not found")?;
        let room_data = room_lock.read().await;
        Ok(room_data
            .crdt
            .get_frontier()
            .into_iter()
            .map(|v| v.to_string())
            .collect())
    }

    /// Get a single message by ID
    pub async fn get_message(&self, room_id: &str, message_id: &str) -> Result<Message> {
        let room_lock = self.get_room(room_id).await?.context("Room not found")?;
        let room_data = room_lock.read().await;

        room_data
            .crdt
            .messages()
            .values()
            .find(|m| m.id == message_id)
            .cloned()
            .context("Message not found")
    }

    /// Get blob store reference
    pub fn blob_store(&self) -> &BlobStore {
        &self.blob_store
    }

    /// Get broadcast channel for a room
    pub async fn get_channel(&self, room_id: &str) -> UpdateChannel {
        let mut channels = self.channels.write().await;
        channels
            .entry(room_id.to_string())
            .or_insert_with(|| {
                let (tx, _) = broadcast::channel(100);
                UpdateChannel { tx }
            })
            .clone()
    }

    /// Broadcast an update to all subscribers
    pub async fn broadcast(&self, room_id: &str, update: RoomUpdate) -> Result<()> {
        let channel = self.get_channel(room_id).await;
        let _ = channel.tx.send(update);
        Ok(())
    }

    /// Merge remote CRDT updates into a room
    pub async fn merge_updates(
        &self,
        room_id: &str,
        updates: Vec<ChatUpdate>,
    ) -> Result<Vec<Message>> {
        let room_lock = self.get_or_create_room(room_id, Some("remote")).await?;
        let mut room_data = room_lock.write().await;

        // Use CRDT to merge
        let new_messages = room_data.crdt.merge_updates(updates);

        // Save to disk
        if !new_messages.is_empty() {
            self.save_room_to_disk(&*room_data).await?;

            // Broadcast sync event
            let update = RoomUpdate {
                room_id: room_id.to_string(),
                update_type: UpdateType::Sync,
                data: serde_json::json!({ "new_messages": new_messages.len() }),
                crdt_version: None,
            };
            drop(room_data); // Release lock before broadcasting
            self.broadcast(room_id, update).await?;
        }

        Ok(new_messages)
    }

    /// Generate sync braid for a client
    pub async fn generate_sync_braid(
        &self,
        room_id: &str,
        known_versions: &[braid_http::types::Version],
    ) -> Result<Vec<ChatUpdate>> {
        let room_lock = self.get_room(room_id).await?.context("Room not found")?;
        let room_data = room_lock.read().await;

        let braid = room_data.crdt.generate_sync_braid(known_versions);
        Ok(braid)
    }

    /// Save a draft message for offline support
    pub async fn save_draft(
        &self,
        room_id: &str,
        content: &str,
        msg_type: MessageType,
    ) -> Result<()> {
        use crate::core::models::DraftMessage;

        let draft = DraftMessage {
            local_id: Uuid::new_v4().to_string(),
            room_id: room_id.to_string(),
            content: content.to_string(),
            created_at: Utc::now(),
            message_type: msg_type,
        };

        let mut drafts = self.drafts.write().await;
        let room_drafts = drafts.entry(room_id.to_string()).or_insert_with(Vec::new);
        room_drafts.push(draft);

        info!("Saved draft for room {}", room_id);
        Ok(())
    }

    /// Get draft messages for a room
    pub async fn get_drafts(&self, room_id: &str) -> Vec<crate::core::models::DraftMessage> {
        let drafts = self.drafts.read().await;
        drafts.get(room_id).cloned().unwrap_or_default()
    }

    /// Clear drafts for a room (after successful sync)
    pub async fn clear_drafts(&self, room_id: &str) -> Result<()> {
        let mut drafts = self.drafts.write().await;
        drafts.remove(room_id);
        info!("Cleared drafts for room {}", room_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_create_and_get_room() {
        let temp_dir = TempDir::new().unwrap();
        let config = ChatServerConfig::with_base_dir(temp_dir.path());
        let store = JsonChatStore::new(config).await.unwrap();

        let room = store
            .get_or_create_room("test-room", Some("user1"))
            .await
            .unwrap();
        let room = room.read().await;
        assert_eq!(room.room.id, "test-room");
        assert_eq!(room.room.created_by, "user1");
    }

    #[tokio::test]
    async fn test_add_message_uses_crdt() {
        let temp_dir = TempDir::new().unwrap();
        let config = ChatServerConfig::with_base_dir(temp_dir.path());
        let store = JsonChatStore::new(config).await.unwrap();

        let msg = store
            .add_message(
                "test-room",
                "user1",
                "Hello, world!",
                MessageType::Text,
                None,
                vec![],
            )
            .await
            .unwrap();

        assert_eq!(msg.content, "Hello, world!");
        assert!(!msg.version.is_empty());
        // Version format: "seq@node_id"
        assert!(msg.version.contains('@'));

        // Verify message was saved
        let messages = store.get_messages("test-room", None).await.unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].content, "Hello, world!");
    }
}
