//! AI Chat Server Implementation
//!
//! This module provides server-side AI chat functionality with:
//! - Ollama/genai integration for responses
//! - File-based chat history watching
//! - Context injection from related files
//! - Braid protocol integration for sync
//! - Thinking indicator support

use crate::core::models::{Message, MessageType};
use crate::core::store::json_store::JsonChatStore;
use anyhow::Result;
use notify::{Event, RecursiveMode, Watcher};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, warn};

// GenAI imports
use genai::chat::{ChatMessage, ChatRequest};
use genai::Client as GenAIClient;

/// AI Assistant configuration
#[derive(Clone, Debug)]
pub struct AiConfig {
    /// Default model to use
    pub model: String,
    /// System prompt
    pub system_prompt: String,
    /// Whether to include folder context
    pub enable_context: bool,
    /// Max context files to include
    pub max_context_files: usize,
}

impl Default for AiConfig {
    fn default() -> Self {
        Self {
            model: "ollama::qwen3:4b".to_string(),
            system_prompt: "You are @BraidBot, a helpful assistant in a group chat. Keep responses concise and use Markdown.".to_string(),
            enable_context: true,
            max_context_files: 5,
        }
    }
}

/// AI Chat Manager
pub struct AiChatManager {
    config: AiConfig,
    _store: Arc<JsonChatStore>,
    ai_chats_dir: PathBuf,
    /// Track which rooms are AI chats
    ai_rooms: Arc<RwLock<HashMap<String, AiRoomState>>>,
    /// GenAI client for API calls
    genai_client: GenAIClient,
    /// Track pending AI responses (room_id -> thinking_message_id)
    _pending_responses: Arc<RwLock<HashMap<String, String>>>,
    /// System context directory
    context_dir: PathBuf,
}

#[derive(Clone, Debug)]
struct AiRoomState {
    room_id: String,
    last_processed_version: Option<String>,
    pending_mentions: Vec<String>,
}

impl AiChatManager {
    /// Create a new AI chat manager
    pub async fn new(
        config: AiConfig,
        store: Arc<JsonChatStore>,
        base_dir: impl Into<PathBuf>,
    ) -> Result<Self> {
        let ai_chats_dir = base_dir.into().join("ai");

        // Ensure directory exists
        tokio::fs::create_dir_all(&ai_chats_dir).await?;

        // Initialize GenAI client
        let genai_client = GenAIClient::default();

        info!("[@BraidBot] AI Chat Manager initialized");
        info!("[@BraidBot] AI chats directory: {:?}", ai_chats_dir);
        info!("[@BraidBot] Using model: {}", config.model);

        let context_dir = braid_common::ai_context_dir();
        tokio::fs::create_dir_all(&context_dir).await?;

        Ok(Self {
            config,
            _store: store,
            ai_chats_dir,
            ai_rooms: Arc::new(RwLock::new(HashMap::new())),
            genai_client,
            _pending_responses: Arc::new(RwLock::new(HashMap::new())),
            context_dir,
        })
    }

    /// Register a room as an AI chat
    pub async fn register_ai_room(&self, room_id: &str) -> Result<()> {
        let state = AiRoomState {
            room_id: room_id.to_string(),
            last_processed_version: None,
            pending_mentions: Vec::new(),
        };

        self.ai_rooms
            .write()
            .await
            .insert(room_id.to_string(), state);

        info!("[@BraidBot] Registered AI room: {}", room_id);

        // Create initial markdown file for the room
        self.create_ai_chat_file(room_id).await?;

        Ok(())
    }

    /// Create the markdown file for an AI chat
    async fn create_ai_chat_file(&self, room_id: &str) -> Result<PathBuf> {
        let path = self.ai_chats_dir.join(format!("{}.md", room_id));

        if !path.exists() {
            let content = format!("# AI Chat: {}\n\n", room_id);
            tokio::fs::write(&path, content).await?;
            info!("[@BraidBot] Created AI chat file: {:?}", path);
        }

        Ok(path)
    }

    /// Process incoming message for AI mentions
    /// Returns immediately with a "thinking" message, then spawns async task for AI response
    pub async fn process_message(
        &self,
        room_id: &str,
        message: &Message,
    ) -> Result<Option<Message>> {
        // Check if this is an AI room
        let is_ai_room = self.ai_rooms.read().await.contains_key(room_id);

        if !is_ai_room {
            return Ok(None);
        }

        // Check for @BraidBot mention
        if !message.content.contains("@BraidBot") && !message.content.contains("@BraidBot!") {
            return Ok(None);
        }

        // Check if we've already responded to this message
        if message.sender == "@BraidBot" {
            return Ok(None);
        }

        info!(
            "[@BraidBot] Triggered in room {} by {}",
            room_id, message.sender
        );

        // Add "thinking..." message immediately so user sees feedback
        let thinking_msg = self
            ._store
            .add_message(
                room_id,
                "@BraidBot",
                "🤔 *Thinking...*",
                MessageType::Text,
                Some(message.id.clone()),
                vec![],
            )
            .await?;

        let thinking_id = thinking_msg.id.clone();
        let _store = self._store.clone();
        let config = self.config.clone();
        let genai_client = self.genai_client.clone();
        let user_msg = message.clone();
        let room_id_owned = room_id.to_string();
        let ai_chats_dir = self.ai_chats_dir.clone();
        let context_dir = self.context_dir.clone();

        // Spawn async task to generate AI response
        tokio::spawn(async move {
            match Self::generate_ai_response(
                &genai_client,
                &config,
                &_store,
                &room_id_owned,
                &user_msg,
                &context_dir,
            )
            .await
            {
                Ok(response_text) => {
                    // Edit the thinking message with the actual response
                    if let Err(e) = _store
                        .edit_message(&room_id_owned, &thinking_id, &response_text)
                        .await
                    {
                        warn!("[@BraidBot] Failed to edit thinking message: {}", e);
                        // Fallback: add as new message
                        if let Err(e2) = _store
                            .add_message(
                                &room_id_owned,
                                "@BraidBot",
                                &response_text,
                                MessageType::Text,
                                Some(user_msg.id.clone()),
                                vec![],
                            )
                            .await
                        {
                            warn!("[@BraidBot] Failed to add response message: {}", e2);
                        }
                    }

                    // Update markdown file
                    if let Ok(bot_msg) = _store.get_message(&room_id_owned, &thinking_id).await {
                        let _ = Self::append_to_markdown_static(
                            &ai_chats_dir,
                            &room_id_owned,
                            &user_msg,
                            &bot_msg,
                        )
                        .await;
                    }

                    info!("[@BraidBot] Responded in room {}", room_id_owned);
                }
                Err(e) => {
                    // Edit thinking message to show error
                    let error_msg =
                        format!("❌ *Error: Could not generate response. Please try again.*");
                    if let Err(e2) = _store
                        .edit_message(&room_id_owned, &thinking_id, &error_msg)
                        .await
                    {
                        warn!(
                            "[@BraidBot] Failed to edit thinking message with error: {}",
                            e2
                        );
                    }
                    warn!("[@BraidBot] Failed to generate response: {}", e);
                }
            }
        });

        // Return the thinking message immediately
        Ok(Some(thinking_msg))
    }

    /// Generate AI response using GenAI - static method for spawned task
    async fn generate_ai_response(
        client: &GenAIClient,
        config: &AiConfig,
        store: &Arc<JsonChatStore>,
        room_id: &str,
        trigger_message: &Message,
        context_dir: &Path,
    ) -> Result<String> {
        // Get chat history for context
        let history = store.get_messages(room_id, None).await?;

        // Build chat request with history
        let mut chat_messages = vec![ChatMessage::system(&config.system_prompt)];

        // Check for "ai read context" command
        if trigger_message
            .content
            .to_lowercase()
            .contains("ai read context")
        {
            let content = trigger_message.content.to_lowercase();
            if let Some(idx) = content.find("ai read context") {
                let after = &trigger_message.content[idx + "ai read context".len()..];
                let filename = after.trim().trim_matches('"');
                if !filename.is_empty() {
                    let context_path = context_dir.join(filename);
                    info!(
                        "[@BraidBot] Attempting to read context file: {:?}",
                        context_path
                    );

                    if context_path.exists() {
                        match tokio::fs::read_to_string(&context_path).await {
                            Ok(content) => {
                                info!("[@BraidBot] Successfully read context from {}", filename);
                                chat_messages.push(ChatMessage::system(&format!(
                                    "DOCKER CONTEXT FILE ({}):\n\n{}",
                                    filename, content
                                )));
                            }
                            Err(e) => {
                                warn!(
                                    "[@BraidBot] Failed to read context file {}: {}",
                                    filename, e
                                );
                            }
                        }
                    } else {
                        warn!("[@BraidBot] Context file not found: {:?}", context_path);
                    }
                }
            }
        }

        // Add recent history (last 10 messages)
        for msg in history.iter().rev().take(10).rev() {
            if msg.sender == "@BraidBot" {
                chat_messages.push(ChatMessage::assistant(&msg.content));
            } else {
                chat_messages.push(ChatMessage::user(&format!(
                    "{}: {}",
                    msg.sender, msg.content
                )));
            }
        }

        // Add the current trigger message if not already in history
        if !history.iter().any(|m| m.id == trigger_message.id) {
            chat_messages.push(ChatMessage::user(&format!(
                "{}: {}",
                trigger_message.sender, trigger_message.content
            )));
        }

        let chat_req = ChatRequest::new(chat_messages);

        // Call the AI API
        info!("[@BraidBot] Calling {} for response...", config.model);

        let response = client
            .exec_chat(&config.model, chat_req, None)
            .await
            .map_err(|e| anyhow::anyhow!("GenAI error: {}", e))?;

        let response_text = response
            .first_text()
            .unwrap_or("*No response generated*")
            .to_string();

        Ok(response_text)
    }

    /// Static helper for markdown append in spawned task
    async fn append_to_markdown_static(
        ai_chats_dir: &PathBuf,
        room_id: &str,
        user_msg: &Message,
        bot_msg: &Message,
    ) -> Result<()> {
        let path = ai_chats_dir.join(format!("{}.md", room_id));

        let append = format!(
            "\n**{}** ({}): {}\n\n**@BraidBot** ({}): {}\n",
            user_msg.sender,
            user_msg.created_at.format("%Y-%m-%d %H:%M"),
            user_msg.content,
            bot_msg.created_at.format("%Y-%m-%d %H:%M"),
            bot_msg.content
        );

        use tokio::io::AsyncWriteExt;
        let mut file = tokio::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(&path)
            .await?;

        file.write_all(append.as_bytes()).await?;

        Ok(())
    }

    /// Build context from folder files
    async fn _build_folder_context(&self, room_id: &str) -> Result<String> {
        let mut context = String::new();
        let mut count = 0;

        let mut entries = tokio::fs::read_dir(&self.ai_chats_dir).await?;

        while let Some(entry) = entries.next_entry().await? {
            if count >= self.config.max_context_files {
                break;
            }

            let path = entry.path();
            if path.is_file() && path.extension() == Some("md".as_ref()) {
                if let Some(stem) = path.file_stem() {
                    if stem != room_id {
                        if let Ok(content) = tokio::fs::read_to_string(&path).await {
                            context.push_str(&format!(
                                "\n--- From {} ---\n{}\n",
                                path.file_name().unwrap_or_default().to_string_lossy(),
                                content.chars().take(1000).collect::<String>()
                            ));
                            count += 1;
                        }
                    }
                }
            }
        }

        Ok(context)
    }

    /// Append messages to markdown file
    async fn append_to_markdown(
        &self,
        room_id: &str,
        user_msg: &Message,
        bot_msg: &Message,
    ) -> Result<()> {
        let path = self.ai_chats_dir.join(format!("{}.md", room_id));

        let append = format!(
            "\n**{}** ({})\n{}\n\n**@BraidBot** ({})\n{}\n",
            user_msg.sender,
            user_msg.created_at.format("%Y-%m-%d %H:%M"),
            user_msg.content,
            bot_msg.created_at.format("%Y-%m-%d %H:%M"),
            bot_msg.content
        );

        // Append to file
        use tokio::io::AsyncWriteExt;
        let mut file = tokio::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(&path)
            .await?;

        file.write_all(append.as_bytes()).await?;

        Ok(())
    }

    /// Start watching AI chat files for external changes
    pub async fn start_watching(&self) -> Result<()> {
        let (tx, mut rx) = mpsc::channel(100);

        let mut watcher = notify::recommended_watcher(move |res: Result<Event, notify::Error>| {
            if let Ok(event) = res {
                let _ = tx.try_send(event);
            }
        })?;

        watcher.watch(&self.ai_chats_dir, RecursiveMode::NonRecursive)?;

        let ai_rooms = self.ai_rooms.clone();
        let _store = self._store.clone();
        let _ai_chats_dir = self.ai_chats_dir.clone();

        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                for path in event.paths {
                    if let Some(ext) = path.extension() {
                        if ext == "md" {
                            if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                                // Check if this is a known AI room
                                let rooms = ai_rooms.read().await;
                                if rooms.contains_key(stem) {
                                    debug!("AI chat file changed externally: {:?}", path);
                                    // Could trigger re-sync here
                                }
                            }
                        }
                    }
                }
            }
            drop(watcher);
        });

        info!("[@BraidBot] Started watching AI chat files");
        Ok(())
    }

    /// Get AI chat history as markdown
    pub async fn get_chat_history(&self, room_id: &str) -> Result<String> {
        let path = self.ai_chats_dir.join(format!("{}.md", room_id));

        if path.exists() {
            let content = tokio::fs::read_to_string(&path).await?;
            Ok(content)
        } else {
            Ok(format!("# AI Chat: {}\n\n_No messages yet_", room_id))
        }
    }

    /// List all AI chat rooms
    pub async fn list_ai_rooms(&self) -> Vec<String> {
        self.ai_rooms.read().await.keys().cloned().collect()
    }
}

/// Hook for message processing - call this when new messages arrive
pub async fn on_new_message(
    ai_manager: &Option<Arc<AiChatManager>>,
    room_id: &str,
    message: &Message,
) -> Result<Option<Message>> {
    if let Some(manager) = ai_manager {
        manager.process_message(room_id, message).await
    } else {
        Ok(None)
    }
}
