//! Server-side Braid Multiplexing implementation.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex};

/// Information about a multiplexed connection on the server.
pub struct MultiplexerConnection {
    /// Channel to send data to the main multiplexer connection.
    pub sender: mpsc::Sender<Vec<u8>>,
    /// Active request IDs to their responders (not used in v1.0 simple impl yet).
    pub active_requests: Mutex<HashMap<String, mpsc::Sender<()>>>,
    /// Last activity timestamp for cleanup.
    pub last_activity: Mutex<Instant>,
}

impl MultiplexerConnection {
    /// Update last activity timestamp.
    pub async fn touch(&self) {
        *self.last_activity.lock().await = Instant::now();
    }
}

/// Registry of active multiplexers on the server.
#[derive(Default)]
pub struct MultiplexerRegistry {
    /// Active multiplexer connections by ID.
    pub multiplexers: Mutex<HashMap<String, Arc<MultiplexerConnection>>>,
}

impl MultiplexerRegistry {
    /// Creates a new multiplexer registry.
    pub fn new() -> Self {
        Self {
            multiplexers: Mutex::new(HashMap::new()),
        }
    }

    /// Tracks a new multiplexer connection.
    pub async fn add(
        &self,
        id: String,
        sender: mpsc::Sender<Vec<u8>>,
    ) -> Arc<MultiplexerConnection> {
        let conn = Arc::new(MultiplexerConnection {
            sender,
            active_requests: Mutex::new(HashMap::new()),
            last_activity: Mutex::new(Instant::now()),
        });
        let mut multiplexers = self.multiplexers.lock().await;
        multiplexers.insert(id, conn.clone());
        conn
    }

    /// Removes a multiplexer connection.
    pub async fn remove(&self, id: &str) {
        let mut multiplexers = self.multiplexers.lock().await;
        multiplexers.remove(id);
    }

    /// Gets a multiplexer connection by ID.
    pub async fn get(&self, id: &str) -> Option<Arc<MultiplexerConnection>> {
        let multiplexers = self.multiplexers.lock().await;
        let conn = multiplexers.get(id).cloned();
        if let Some(ref c) = conn {
            c.touch().await;
        }
        conn
    }

    /// Clean up stale multiplexers.
    pub async fn cleanup_stale(&self, timeout: Duration) {
        let mut multiplexers = self.multiplexers.lock().await;
        let now = Instant::now();

        let mut to_remove = Vec::new();
        for (id, conn) in multiplexers.iter() {
            let last_activity = *conn.last_activity.lock().await;
            if now.duration_since(last_activity) > timeout {
                to_remove.push(id.clone());
            }
        }

        for id in to_remove {
            multiplexers.remove(&id);
        }
    }
}
