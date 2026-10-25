use super::{debouncer::DebouncedSyncManager, ActivityTracker, PendingWrites};
use crate::core::merge::{MergeType, MergeTypeRegistry};
use crate::core::BraidClient;
use crate::fs::binary_sync::BinarySyncManager;
use crate::fs::config::Config;
use crate::fs::versions::VersionStore;
use parking_lot::Mutex as PMutex;
use rusqlite::Connection;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub enum Command {
    Sync {
        url: String,
    },
    Unsync {
        url: String,
    },
    SetCookie {
        domain: String,
        value: String,
    },
    SetOriginCredentials {
        origin: String,
        credentials: braid_http::client::OriginCredentials,
        result: async_channel::Sender<crate::core::Result<()>>,
    },
    SetIdentity {
        domain: String,
        email: String,
    },
    #[cfg(feature = "nfs")]
    Mount {
        port: u16,
        mount_point: Option<String>,
    },
    #[cfg(feature = "nfs")]
    Unmount,
}
impl std::fmt::Debug for Command {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Sync { .. } => "Sync",
            Self::Unsync { .. } => "Unsync",
            Self::SetIdentity { .. } => "SetIdentity",
            Self::SetCookie { .. } => "SetCookie { credentials: [redacted] }",
            Self::SetOriginCredentials { .. } => "SetOriginCredentials { credentials: [redacted] }",
            #[cfg(feature = "nfs")]
            Self::Mount { .. } => "Mount",
            #[cfg(feature = "nfs")]
            Self::Unmount => "Unmount",
        })
    }
}

/// Unified state for the BraidFS daemon.
#[derive(Clone)]
pub struct DaemonState {
    /// The shared GET/PUT owner for each active text resource.
    pub text_sessions:
        Arc<tokio::sync::Mutex<HashMap<String, Arc<crate::simpleton::SimpletonClient>>>>,
    pub config: Arc<RwLock<Config>>,
    pub content_cache: Arc<RwLock<HashMap<String, String>>>,
    pub version_store: Arc<RwLock<VersionStore>>,
    pub tracker: ActivityTracker,
    pub merge_registry: Arc<MergeTypeRegistry>,
    pub active_merges: Arc<RwLock<HashMap<String, Box<dyn MergeType>>>>,
    pub pending: PendingWrites,
    pub client: BraidClient,
    pub failed_syncs: Arc<RwLock<HashMap<String, (u16, std::time::Instant)>>>,
    pub binary_sync: Arc<BinarySyncManager>,
    pub inode_db: Arc<PMutex<Connection>>,
    pub tx_cmd: async_channel::Sender<Command>,
    pub debouncer: Arc<DebouncedSyncManager>,
    /// URLs being managed by the local HTTP 209 server (polling)
    pub local_server_managed: Arc<RwLock<std::collections::HashSet<String>>>,
}
