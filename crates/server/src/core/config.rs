//! Chat server configuration

use std::path::PathBuf;
use std::sync::Arc;

use crate::chat::ai::AiChatManager;
use crate::chat::friends::FriendManager;
use crate::chat::mail::MailManager;
use crate::core::auth::AuthManager;
use crate::core::daemon::DaemonIntegration;
use crate::core::store::JsonChatStore;
use crate::core::pages::{LocalOrgManager, PagesManager};

/// Configuration for the Braid Chat Server
#[derive(Clone, Debug)]
pub struct ChatServerConfig {
    /// Storage directory for chat files
    pub storage_dir: PathBuf,
    /// Blob storage directory
    pub blob_dir: PathBuf,
    /// Local drafts directory
    pub drafts_dir: PathBuf,
    /// Enable daemon integration
    pub enable_daemon: bool,
    /// Daemon port to connect to
    pub daemon_port: u16,
    /// Enable offline drafts
    pub enable_offline: bool,
    /// Max blob size in MB
    pub max_blob_size: usize,
    /// Inline blob threshold in bytes
    pub inline_threshold: usize,
    /// Node ID for CRDT
    pub node_id: String,
}

impl Default for ChatServerConfig {
    fn default() -> Self {
        Self {
            storage_dir: braid_common::peers_dir(),
            blob_dir: braid_common::blobs_dir(),
            drafts_dir: braid_common::sync_dir().join("drafts"),
            enable_daemon: true,
            daemon_port: std::env::var("DAEMON_PORT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(45678),
            enable_offline: true,
            max_blob_size: 50,
            inline_threshold: 10240, // 10KB
            node_id: format!(
                "server-{}",
                uuid::Uuid::new_v4().to_string()[..8].to_string()
            ),
        }
    }
}

impl ChatServerConfig {
    /// Create config with custom base directory
    pub fn with_base_dir(base_dir: impl Into<PathBuf>) -> Self {
        let mut config = Self::default();
        let base = base_dir.into();
        config.storage_dir = base.join("peers");
        config.blob_dir = base.join("blobs");
        config.drafts_dir = base.join("drafts");
        config
    }

    /// Ensure all directories exist
    pub async fn ensure_dirs(&self) -> anyhow::Result<()> {
        tokio::fs::create_dir_all(&self.storage_dir).await?;
        tokio::fs::create_dir_all(&self.blob_dir).await?;
        tokio::fs::create_dir_all(&self.drafts_dir).await?;
        Ok(())
    }
}

/// App state shared across all handlers
#[derive(Clone)]
pub struct AppState {
    pub config: ChatServerConfig,
    pub store: Arc<JsonChatStore>,
    pub auth: Arc<AuthManager>,
    pub friends: Arc<FriendManager>,
    pub ai_manager: Option<Arc<AiChatManager>>,
    pub daemon: Option<Arc<DaemonIntegration>>,
    pub mail_manager: Arc<MailManager>,
    pub pages_manager: Arc<PagesManager>,
    pub local_org_manager: Arc<LocalOrgManager>,
}
