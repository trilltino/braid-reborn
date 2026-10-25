//! Local HTTP 209 server for braidfs-daemon
//!
//! This provides a bridge between braid.org (which doesn't support HTTP 209)
//! and local clients (IDEs) that want live updates.
//!
//! Architecture:
//! - Polls braid.org periodically for changes
//! - Serves HTTP 209 subscriptions to local clients
//! - Broadcasts updates when changes detected

use crate::fs::state::DaemonState;
use axum::{body::Body, extract::Path, response::Response, routing::get, Router};
use braid_http::types::BraidRequest;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, RwLock};
use tokio::time::interval;
use tracing::{info, warn};

/// Subscription state for a URL
struct SubscriptionState {
    /// Last known version from braid.org
    last_version: String,
    /// Last known content
    last_content: String,
    /// Broadcast channel for updates
    tx: broadcast::Sender<BraidUpdate>,
    /// Last time there were active subscribers
    last_active: std::time::Instant,
}

/// Update sent to subscribers
#[derive(Clone, Debug)]
struct BraidUpdate {
    version: String,
    content: String,
}

/// Shared state for the local server
pub struct LocalBraidServer {
    /// Map of URL to subscription state
    subscriptions: Arc<RwLock<HashMap<String, SubscriptionState>>>,
    /// Daemon state for making requests
    daemon_state: DaemonState,
    /// Poll interval
    poll_interval: Duration,
}

impl LocalBraidServer {
    pub fn new(daemon_state: DaemonState, poll_interval_secs: u64) -> Self {
        Self {
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            daemon_state,
            poll_interval: Duration::from_secs(poll_interval_secs),
        }
    }

    /// Start the HTTP server and polling loop
    pub async fn start(self, port: u16) -> Result<(), Box<dyn std::error::Error>> {
        let state = Arc::new(self);

        // Start polling loop (only active when subscribers connected)
        let state_clone = state.clone();
        tokio::spawn(async move {
            state_clone.poll_loop().await;
        });

        // Build router
        let app = Router::new()
            .route("/subscribe/{*url}", get(subscribe_handler))
            .with_state(state);

        let addr = format!("127.0.0.1:{}", port);
        info!("[LocalBraidServer] Starting on http://{}", addr);

        let listener = tokio::net::TcpListener::bind(&addr).await?;
        axum::serve(listener, app).await?;

        Ok(())
    }

    /// Poll loop - only polls when there are active subscribers
    async fn poll_loop(&self) {
        let mut interval = interval(self.poll_interval);
        let grace_period = Duration::from_secs(30); // Keep managing for 30s after disconnect

        loop {
            interval.tick().await;

            // Find URLs with active subscribers and update last_active
            let urls_to_poll: Vec<String> = {
                let mut subs = self.subscriptions.write().await;
                let mut active_urls = Vec::new();

                for (url, state) in subs.iter_mut() {
                    if state.tx.receiver_count() > 0 {
                        state.last_active = std::time::Instant::now();
                        active_urls.push(url.clone());
                    }
                }
                active_urls
            };

            // Update local_server_managed set (include URLs within grace period)
            {
                let mut managed = self.daemon_state.local_server_managed.write().await;
                let subs = self.subscriptions.read().await;

                // Add URLs that have subscribers or are within grace period
                for (url, state) in subs.iter() {
                    if state.tx.receiver_count() > 0 || state.last_active.elapsed() < grace_period {
                        managed.insert(url.clone());
                    }
                }

                // Remove URLs that are outside grace period
                managed.retain(|url| {
                    if let Some(state) = subs.get(url) {
                        state.tx.receiver_count() > 0 || state.last_active.elapsed() < grace_period
                    } else {
                        false
                    }
                });
            }

            if !urls_to_poll.is_empty() {
                info!(
                    "[LocalBraidServer] Polling {} URLs with active subscribers",
                    urls_to_poll.len()
                );
                for url in urls_to_poll {
                    if let Err(e) = self.check_for_updates(&url).await {
                        warn!("[LocalBraidServer] Error checking {}: {}", url, e);
                    }
                }
            }
        }
    }

    /// Check a single URL for updates
    async fn check_for_updates(&self, url: &str) -> Result<(), Box<dyn std::error::Error>> {
        // Build request with auth
        let req = super::sync::authenticate(
            BraidRequest::new().with_header("Accept", "text/plain"),
            url,
            &self.daemon_state,
        )
        .await?;

        // Fetch current state
        let response = self.daemon_state.client.fetch(url, req).await?;

        let current_version = response.header("version").unwrap_or("").to_string();

        if !(200..300).contains(&response.status) {
            return Err(format!("Polling GET failed: {}", response.status).into());
        }
        let current_content = std::str::from_utf8(&response.body)?.to_owned();
        self.write_to_local_file(url, &current_content, &current_version)
            .await?;

        // Check if changed
        let mut subs = self.subscriptions.write().await;
        if let Some(state) = subs.get_mut(url) {
            if state.last_version != current_version {
                info!(
                    "[LocalBraidServer] Update detected for {}: {} → {}",
                    url, state.last_version, current_version
                );

                // Broadcast update
                let update = BraidUpdate {
                    version: current_version.clone(),
                    content: current_content.clone(),
                };

                let _ = state.tx.send(update);

                // Update stored state
                state.last_version = current_version.clone();
                state.last_content = current_content.clone();
            }
        }

        Ok(())
    }

    /// Write updated content to local file and update caches
    async fn write_to_local_file(
        &self,
        url: &str,
        content: &str,
        version: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let versions = braid_http::protocol::parse_version_header(version)?;
        let update = braid_http::types::Update {
            version: versions,
            body: Some(content.to_owned().into()),
            ..Default::default()
        };
        crate::fs::subscription::materialize_update(url, &self.daemon_state, &update).await?;
        Ok(())
    }

    /// Register a URL for polling
    pub async fn register_url(
        &self,
        url: String,
        initial_version: String,
        initial_content: String,
    ) {
        let (tx, _rx) = broadcast::channel(16);

        let mut subs = self.subscriptions.write().await;
        subs.insert(
            url.clone(),
            SubscriptionState {
                last_version: initial_version,
                last_content: initial_content,
                tx,
                last_active: std::time::Instant::now(),
            },
        );

        info!("[LocalBraidServer] Registered URL for polling: {}", url);
    }
}

/// HTTP 209 subscription handler
async fn subscribe_handler(
    Path(url): Path<String>,
    axum::extract::State(state): axum::extract::State<Arc<LocalBraidServer>>,
) -> Response {
    info!("[LocalBraidServer] New subscription request for: {}", url);

    // Ensure URL is registered
    {
        let subs = state.subscriptions.read().await;
        if !subs.contains_key(&url) {
            drop(subs);
            // Register with empty state - will be populated on first poll
            state
                .register_url(url.clone(), String::new(), String::new())
                .await;
        }
    }

    // Get broadcast receiver
    let mut rx = {
        let subs = state.subscriptions.read().await;
        match subs.get(&url) {
            Some(sub_state) => sub_state.tx.subscribe(),
            None => {
                return Response::builder()
                    .status(500)
                    .body(Body::from("Failed to create subscription"))
                    .unwrap_or_else(crate::core::server::response_error);
            }
        }
    };

    // A lagged receiver closes the stream so the peer repairs from its frontier.
    let stream = async_stream::stream! {
        while let Ok(update) = rx.recv().await {
            let versions = match braid_http::protocol::parse_version_header(&update.version) {
                Ok(versions) => versions,
                Err(error) => { yield Err(std::io::Error::other(error.to_string())); break; }
            };
            let update = braid_http::types::Update { version: versions, body: Some(update.content.into()), ..Default::default() };
            match braid_http::protocol::format_update(&update) {
                Ok(bytes) => yield Ok::<_, std::io::Error>(bytes),
                Err(error) => { yield Err(std::io::Error::other(error.to_string())); break; }
            }
        }
    };

    // Return HTTP 209 Subscription
    Response::builder()
        .status(209)
        .header("Content-Type", "text/plain")
        .header("Subscribe", "true")
        .header("Merge-Type", "simpleton")
        .body(Body::from_stream(stream))
        .unwrap_or_else(crate::core::server::response_error)
}
