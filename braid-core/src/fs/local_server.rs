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
        let mut req = BraidRequest::new().with_header("Accept", "text/plain");

        if let Ok(u) = url::Url::parse(url) {
            if let Some(domain) = u.domain() {
                let cfg = self.daemon_state.config.read().await;
                if let Some(token) = cfg.cookies.get(domain) {
                    let cookie_str = if token.contains('=') {
                        token.clone()
                    } else {
                        format!("client={}", token)
                    };
                    req = req.with_header("Cookie", cookie_str);
                }
            }
        }

        // Fetch current state
        let response = self.daemon_state.client.fetch(url, req).await?;

        let current_version = response
            .header("version")
            .or(response.header("current-version"))
            .unwrap_or("")
            .to_string();

        let current_content = String::from_utf8_lossy(&response.body).to_string();

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

                // Write to local file (and update caches)
                if let Err(e) = self
                    .write_to_local_file(url, &current_content, &current_version)
                    .await
                {
                    warn!("[LocalBraidServer] Failed to write local file: {}", e);
                }
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
        use crate::fs::mapping::url_to_path;

        let path = url_to_path(url)?;

        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        // Mark this write as pending so file watcher doesn't trigger sync
        self.daemon_state.pending.add(path.clone());

        // Write to file
        tokio::fs::write(&path, content).await?;
        info!("[LocalBraidServer] Updated local file: {:?}", path);

        // Update content cache so file watcher doesn't trigger unnecessary sync
        {
            let mut cache = self.daemon_state.content_cache.write().await;
            cache.insert(url.to_string(), content.to_string());
        }

        // Update version store so future syncs use correct parent version
        if !version.is_empty() {
            let mut store = self.daemon_state.version_store.write().await;
            // Parse version to extract just the version string (remove quotes if present)
            let clean_version = version.trim_matches('"').to_string();
            use braid_http::types::Version;
            store.update(url, vec![Version::from(clean_version)], vec![]);
            let _ = store.save().await;
            info!(
                "[LocalBraidServer] Updated version store for {}: {}",
                url, version
            );
        }

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
                    .unwrap();
            }
        }
    };

    // Create streaming response
    let stream = async_stream::stream! {
        loop {
            match rx.recv().await {
                Ok(update) => {
                    // Format as Braid message
                    let message = format!(
                        "Version: {}\r\n\r\n{}",
                        update.version,
                        update.content
                    );
                    yield Ok::<_, std::convert::Infallible>(message);
                }
                Err(_) => {
                    // Channel closed or lagged
                    break;
                }
            }
        }
    };

    // Return HTTP 209 Subscription
    Response::builder()
        .status(209)
        .header("Content-Type", "text/plain")
        .header("Connection", "keep-alive")
        .body(Body::from_stream(stream))
        .unwrap()
}
