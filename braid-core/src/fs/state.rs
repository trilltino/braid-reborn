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

#[derive(Debug, Clone)]
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

/// Unified state for the BraidFS daemon.
#[derive(Clone)]
pub struct DaemonState {
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
