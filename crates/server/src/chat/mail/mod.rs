//! Braid Mail Module - Server Side
//!
//! The server subscribes to external mail feeds and caches them locally.
//! Clients fetch feed data from the server via HTTP API.
//! When user clicks subscribe, messages appear in the UI via Braid protocol.

use crate::core::config::AppState;
use crate::core::store::json_store::JsonChatStore;
use anyhow::Result;
use axum::{
    body::Body,
    extract::{Path, State},
    http::{header, HeaderMap, StatusCode},
    response::{Json, Response},
};
use braid_http::protocol::constants::headers;
use braid_http::{BraidClient, BraidRequest};
use futures::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tracing::{debug, error, info, warn};

/// Mail feed item with Braid protocol metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MailFeedItem {
    pub id: String,
    pub url: String,
    pub subject: Option<String>,
    pub from: Option<Vec<String>>,
    pub to: Option<Vec<String>>,
    pub cc: Option<Vec<String>>,
    pub date: Option<u64>,
    pub body: Option<String>,
    pub is_network: bool,
    /// Braid version header
    pub version: Option<String>,
    /// Braid parents header
    pub parents: Option<String>,
    /// Braid merge-type header
    pub merge_type: Option<String>,
}

/// Mail post content with Braid protocol metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MailPost {
    pub url: String,
    pub subject: Option<String>,
    pub from: Option<Vec<String>>,
    pub to: Option<Vec<String>>,
    pub cc: Option<Vec<String>>,
    pub date: Option<u64>,
    pub body: Option<String>,
    /// Braid version header
    pub version: Option<String>,
    /// Braid parents header
    pub parents: Option<String>,
    /// Braid merge-type header
    pub merge_type: Option<String>,
    /// Alternative URL field (some feeds use 'link')
    pub link: Option<String>,
}

/// Mail subscription state
#[derive(Debug, Clone)]
struct FeedSubscription {
    feed_url: String,
    last_version: Option<String>,
}

/// Mail manager - handles external feed subscriptions
pub struct MailManager {
    _store: Arc<JsonChatStore>,
    /// Feed URL -> Subscription state
    subscriptions: Arc<RwLock<HashMap<String, FeedSubscription>>>,
    /// Cached feed items
    feed_items: Arc<RwLock<Vec<MailFeedItem>>>,
    /// Cached posts
    posts: Arc<RwLock<HashMap<String, MailPost>>>,
    /// Notification channel for updates
    update_tx: broadcast::Sender<()>,
    /// User authentication cookie for posting
    user_cookie: Arc<RwLock<Option<String>>>,
    /// User email identity
    user_email: Arc<RwLock<Option<String>>>,
}

impl MailManager {
    pub fn new(store: Arc<JsonChatStore>) -> Self {
        let (update_tx, _) = broadcast::channel(16);
        Self {
            _store: store,
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            feed_items: Arc::new(RwLock::new(Vec::new())),
            posts: Arc::new(RwLock::new(HashMap::new())),
            update_tx,
            user_cookie: Arc::new(RwLock::new(None)),
            user_email: Arc::new(RwLock::new(None)),
        }
    }

    /// Set authentication cookie for posting
    pub async fn set_cookie(&self, cookie: String) {
        info!("[MailManager] Setting authentication cookie");
        *self.user_cookie.write().await = Some(cookie);
    }

    /// Set user email identity
    pub async fn set_email(&self, email: String) {
        info!("[MailManager] Setting user email: {}", email);
        *self.user_email.write().await = Some(email);
    }

    /// Get the current user email
    pub async fn get_email(&self) -> Option<String> {
        self.user_email.read().await.clone()
    }

    /// Subscribe to an external mail feed
    pub async fn subscribe_feed(&self, feed_url: &str) -> Result<()> {
        info!("[MailManager] Subscribing to feed: {}", feed_url);

        let mut subs = self.subscriptions.write().await;
        if subs.contains_key(feed_url) {
            info!("[MailManager] Already subscribed to {}", feed_url);
            return Ok(());
        }

        subs.insert(
            feed_url.to_string(),
            FeedSubscription {
                feed_url: feed_url.to_string(),
                last_version: None,
            },
        );
        drop(subs);

        // Start background task to sync feed
        let feed_url = feed_url.to_string();
        let subscriptions = self.subscriptions.clone();
        let feed_items = self.feed_items.clone();
        let posts = self.posts.clone();
        let update_tx = self.update_tx.clone();

        tokio::spawn(async move {
            if let Err(e) =
                Self::feed_sync_task(feed_url, subscriptions, feed_items, posts, update_tx).await
            {
                error!("[MailManager] Feed sync task failed: {}", e);
            }
        });

        info!("[MailManager] Subscription started");
        Ok(())
    }

    /// Background task to sync feed
    async fn feed_sync_task(
        feed_url: String,
        subscriptions: Arc<RwLock<HashMap<String, FeedSubscription>>>,
        feed_items: Arc<RwLock<Vec<MailFeedItem>>>,
        posts: Arc<RwLock<HashMap<String, MailPost>>>,
        update_tx: broadcast::Sender<()>,
    ) -> Result<()> {
        let client = BraidClient::new()?;

        loop {
            // Check if still subscribed
            let sub = {
                let subs = subscriptions.read().await;
                match subs.get(&feed_url) {
                    Some(s) => FeedSubscription {
                        feed_url: s.feed_url.clone(),
                        last_version: s.last_version.clone(),
                    },
                    None => {
                        info!("[MailManager] Subscription ended for {}", feed_url);
                        break;
                    }
                }
            };

            // Fetch feed
            match Self::fetch_feed(&client, &feed_url, sub.last_version.clone()).await {
                Ok((items, new_version)) => {
                    // 2. Hydrate items (fetch details from mail.braid.org)
                    // 2. Hydrate items (fetch details from mail.braid.org)
                    let hydrated_items = {
                        info!("[MailManager] Starting hydration for {} items", items.len());

                        let futures = items.into_iter().map(|mut item| {
                            // client is created fresh inside
                            let posts = posts.clone();
                            // We need to move `item` into the future.

                            async move {
                                let full_url = if item.url.starts_with("http") {
                                    item.url.clone()
                                } else {
                                    if item.url.starts_with("/") {
                                        format!("https://mail.braid.org{}", item.url)
                                    } else {
                                        item.url.clone()
                                    }
                                };

                                // 1. Check Cache First
                                let cached_post = {
                                    let posts_guard = posts.read().await;
                                    posts_guard.get(&full_url).cloned()
                                };

                                if let Some(post) = cached_post {
                                    item.subject = post.subject;
                                    item.from = post.from;
                                    item.to = post.to;
                                    item.date = post.date;
                                    item.body = post.body;
                                    return (item, true); // true = cached
                                }

                                // 2. Fetch if missing
                                // We create a new client inside if needed, or better, we need `client` to be Clonable.
                                // Let's check `braid_http` earlier view. It was just a mod file.
                                // Assuming we can't easily clone client, let's create new one or use `reqwest` directly?
                                // No, use `BraidClient::new()`. It returns Result.
                                let client = match BraidClient::new() {
                                    Ok(c) => c,
                                    Err(_) => return (item, false),
                                };

                                match client.fetch(&full_url, BraidRequest::new()).await {
                                    Ok(resp) => {
                                        let body = String::from_utf8_lossy(&resp.body);
                                        if let Ok(json) =
                                            serde_json::from_str::<serde_json::Value>(&body)
                                        {
                                            item.subject = json
                                                .get("subject")
                                                .and_then(|v| v.as_str())
                                                .map(|s| s.to_string());
                                            item.from = json.get("from").and_then(|v| {
                                                serde_json::from_value(v.clone()).ok()
                                            });
                                            item.date =
                                                json.get("date").and_then(|v| v.as_u64()).map(
                                                    |v| if v < 10000000000 { v * 1000 } else { v },
                                                );
                                            item.body = json
                                                .get("body")
                                                .and_then(|v| v.as_str())
                                                .map(|s| s.to_string());

                                            // Extract Braid protocol headers
                                            let version = resp
                                                .headers
                                                .get("version")
                                                .or(resp.headers.get("Version"))
                                                .or(resp.headers.get("Current-Version"))
                                                .map(|s| s.to_string());
                                            let parents = resp
                                                .headers
                                                .get("parents")
                                                .or(resp.headers.get("Parents"))
                                                .map(|s| s.to_string());
                                            let merge_type = resp
                                                .headers
                                                .get("merge-type")
                                                .or(resp.headers.get("Merge-Type"))
                                                .map(|s| s.to_string());

                                            // Update item with Braid metadata
                                            item.version = version.clone();
                                            item.parents = parents.clone();
                                            item.merge_type = merge_type.clone();
                                            item.cc = json.get("cc").and_then(|v| {
                                                serde_json::from_value(v.clone()).ok()
                                            });

                                            // 3. Update Cache
                                            let new_post = MailPost {
                                                url: full_url.clone(),
                                                subject: item.subject.clone(),
                                                from: item.from.clone(),
                                                to: item.to.clone(),
                                                cc: item.cc.clone(),
                                                date: item.date,
                                                body: item.body.clone(),
                                                version,
                                                parents,
                                                merge_type,
                                                link: None,
                                            };
                                            posts.write().await.insert(full_url.clone(), new_post);
                                            return (item, false); // false = fetched
                                        }
                                    }
                                    Err(e) => {
                                        warn!(
                                            "[MailManager] Failed to hydrate item {}: {}",
                                            full_url, e
                                        );
                                    }
                                }
                                (item, false)
                            }
                        });

                        let results: Vec<(MailFeedItem, bool)> = stream::iter(futures)
                            .buffer_unordered(20) // Parallel fetch 20 items
                            .collect()
                            .await;

                        let cached_count = results.iter().filter(|(_, c)| *c).count();
                        let fetched_count = results.len() - cached_count;

                        info!(
                            "[MailManager] Hydration complete: {} cached, {} fetched",
                            cached_count, fetched_count
                        );

                        results.into_iter().map(|(i, _)| i).collect::<Vec<_>>()
                    };

                    // 3. Only add items that have SOME data (subject OR body)
                    let hydrated_items: Vec<_> = hydrated_items
                        .into_iter()
                        .filter(|item| {
                            let has_data = item.subject.is_some()
                                || item.body.is_some()
                                || item.from.is_some();
                            if !has_data {
                                warn!("[MailManager] Skipping unhydrated post: {}", item.url);
                            }
                            has_data
                        })
                        .collect();

                    info!(
                        "[MailManager] Adding {} hydrated items to feed (filtered from {})",
                        hydrated_items.len(),
                        hydrated_items.len()
                    );

                    let has_hydrated_items = !hydrated_items.is_empty();
                    let mut feed_guard = feed_items.write().await;
                    for item in hydrated_items {
                        // Check if already exists
                        if let Some(pos) = feed_guard.iter().position(|i| i.id == item.id) {
                            // If the new item is hydrated (has subject OR body), update the cache
                            // Even if subject is missing, if we fetched a body, we know more than before.
                            if item.subject.is_some() || item.body.is_some() {
                                feed_guard[pos] = item;
                            }
                        } else {
                            feed_guard.push(item);
                        }
                    }
                    // Sort by date descending
                    feed_guard.sort_by(|a, b| b.date.unwrap_or(0).cmp(&a.date.unwrap_or(0)));
                    drop(feed_guard);

                    // Update last version
                    if let Some(ver) = new_version {
                        let mut subs = subscriptions.write().await;
                        if let Some(s) = subs.get_mut(&feed_url) {
                            s.last_version = Some(ver);
                        }
                    }

                    // 4. Notify subscribers of new data
                    if has_hydrated_items {
                        let _ = update_tx.send(());
                        info!("[MailManager] Broadcast update for {}", feed_url);
                    }
                }
                Err(e) => {
                    warn!("[MailManager] Failed to fetch feed: {}", e);
                }
            }

            // Sleep before next sync
            tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
        }

        Ok(())
    }

    /// Fetch feed from external source
    async fn fetch_feed(
        client: &BraidClient,
        feed_url: &str,
        since_version: Option<String>,
    ) -> Result<(Vec<MailFeedItem>, Option<String>)> {
        let mut req = BraidRequest::new();
        req = req.subscribe();

        if let Some(ver) = since_version {
            req.parents = Some(vec![braid_http::types::Version::new(&ver)]);
        }

        let mut subscription = client.subscribe(feed_url, req).await?;

        let mut items = Vec::new();
        let mut last_version = None;

        // Process first batch of updates
        while let Some(update_result) = subscription.next().await {
            match update_result {
                Ok(update) => {
                    // Get version from extra_headers if available
                    last_version = update.extra_headers.get("version").map(|v| v.to_string());

                    if let Some(body) = &update.body {
                        let body_str = String::from_utf8_lossy(body);
                        let parsed_items = Self::parse_feed_items(&body_str);
                        items.extend(parsed_items);
                    }

                    // Only process first update for now
                    break;
                }
                Err(e) => {
                    warn!("[MailManager] Subscription error: {}", e);
                    break;
                }
            }
        }

        Ok((items, last_version))
    }

    /// Parse feed items from JSON
    fn parse_feed_items(body: &str) -> Vec<MailFeedItem> {
        // Try array of links: ["/post/1", "/post/2"] or objects with {link: "..."}
        if let Ok(links) = serde_json::from_str::<Vec<Option<serde_json::Value>>>(body) {
            return links
                .into_iter()
                .filter_map(|item| item) // Skip null entries like braidmail does
                .filter_map(|item| {
                    // Handle both string links and {link: "..."} objects
                    let link = if let Some(s) = item.as_str() {
                        s.to_string()
                    } else if let Some(obj) = item.as_object() {
                        obj.get("link")
                            .and_then(|l| l.as_str())
                            .map(|s| s.to_string())?
                    } else {
                        return None;
                    };

                    Some(MailFeedItem {
                        id: link.clone(),
                        url: link,
                        subject: None,
                        from: None,
                        to: None,
                        cc: None,
                        date: None,
                        body: None,
                        is_network: true,
                        version: None,
                        parents: None,
                        merge_type: None,
                    })
                })
                .collect();
        }

        warn!("[MailManager] Failed to parse feed body");
        Vec::new()
    }

    /// Fetch a specific post
    pub async fn fetch_post(&self, url: &str) -> Result<MailPost> {
        // Check cache first
        {
            let posts = self.posts.read().await;
            if let Some(post) = posts.get(url) {
                return Ok(post.clone());
            }
        }

        // Fetch from network
        let client = BraidClient::new()?;
        let req = BraidRequest::new();
        let resp = client.fetch(url, req).await?;
        let body = String::from_utf8_lossy(&resp.body);

        let json: serde_json::Value = serde_json::from_str(&body)?;

        // Extract Braid protocol headers
        let version = resp
            .headers
            .get("version")
            .or(resp.headers.get("Version"))
            .or(resp.headers.get("Current-Version"))
            .map(|s| s.to_string());
        let parents = resp
            .headers
            .get("parents")
            .or(resp.headers.get("Parents"))
            .map(|s| s.to_string());
        let merge_type = resp
            .headers
            .get("merge-type")
            .or(resp.headers.get("Merge-Type"))
            .map(|s| s.to_string());

        let post = MailPost {
            url: url.to_string(),
            date: json.get("date").and_then(|v| v.as_u64()).map(|v| v * 1000),
            from: json
                .get("from")
                .and_then(|v| serde_json::from_value(v.clone()).ok()),
            to: json
                .get("to")
                .and_then(|v| serde_json::from_value(v.clone()).ok()),
            cc: json
                .get("cc")
                .and_then(|v| serde_json::from_value(v.clone()).ok()),
            subject: json
                .get("subject")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            body: json
                .get("body")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            version,
            parents,
            merge_type,
            link: json
                .get("link")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
        };

        // Cache it
        self.posts
            .write()
            .await
            .insert(url.to_string(), post.clone());

        Ok(post)
    }

    /// Get update receiver
    pub fn subscribe_updates(&self) -> broadcast::Receiver<()> {
        self.update_tx.subscribe()
    }

    /// Get all feed items
    pub async fn get_feed_items(&self) -> Vec<MailFeedItem> {
        self.feed_items.read().await.clone()
    }

    /// Get cached posts
    pub async fn get_posts(&self) -> Vec<MailPost> {
        self.posts.read().await.values().cloned().collect()
    }

    /// Check if subscribed to any feed
    pub async fn is_subscribed(&self) -> bool {
        !self.subscriptions.read().await.is_empty()
    }

    /// Send a mail post to mail.braid.org
    pub async fn send_mail(&self, mut post: MailPost) -> Result<String> {
        let client = BraidClient::new()?;

        // Generate URL matching xfmail reference: https://mail.braid.org/post/{random_id}
        let url = if post.url.is_empty() {
            // Generate 8-char lowercase ID using UUID (already available as dependency)
            let id = uuid::Uuid::new_v4().to_string();
            let short_id: String = id.chars().filter(|c| c.is_alphanumeric()).take(8).collect();
            format!("https://mail.braid.org/post/{}", short_id.to_lowercase())
        } else {
            post.url.clone()
        };

        // Update post URL
        post.url = url.clone();

        // Get user email for 'from' field if not set
        let from =
            if post.from.is_none() || post.from.as_ref().map(|f| f.is_empty()).unwrap_or(true) {
                let email = self.user_email.read().await.clone();
                vec![email.unwrap_or_else(|| "anonymous".to_string())]
            } else {
                post.from.clone().unwrap()
            };
        post.from = Some(from.clone());

        // Build the post body matching xfmail's BraidPostRequest format
        let body_json = serde_json::json!({
            "from": from,
            "to": post.to.clone().unwrap_or_else(|| vec!["public".to_string()]),
            "cc": post.cc.clone().unwrap_or_default(),
            "date": post.date.unwrap_or_else(|| chrono::Utc::now().timestamp_millis() as u64),
            "body": post.body.clone().unwrap_or_default(),
            "subject": post.subject.clone(),
        });

        info!("[Mail] Sending post to: {}", url);

        let mut req = BraidRequest::new()
            .with_method("PUT")
            .with_content_type("application/json")
            .with_body(body_json.to_string());

        // Add authentication cookie if available
        if let Some(cookie) = self.user_cookie.read().await.clone() {
            let cookie_header = if cookie.contains('=') {
                cookie
            } else {
                format!("client={}", cookie)
            };
            req = req.with_header("Cookie", cookie_header);
        }

        match client.fetch(&url, req).await {
            Ok(resp) => {
                if resp.status >= 200 && resp.status < 300 {
                    info!("[Mail] Post sent successfully to {}", url);

                    // Cache the sent post
                    self.posts.write().await.insert(url.clone(), post.clone());

                    // Optimistic update: Add to feed immediately
                    let feed_item = MailFeedItem {
                        id: url.clone(),
                        url: url.clone(),
                        subject: post.subject.clone(),
                        from: post.from.clone(),
                        to: post.to.clone(),
                        cc: post.cc.clone(),
                        date: post.date,
                        body: post.body.clone(),
                        is_network: false, // Local post
                        version: None,
                        parents: None,
                        merge_type: Some("sync9".to_string()),
                    };

                    // Insert at the beginning (newest first)
                    let mut feed = self.feed_items.write().await;
                    feed.insert(0, feed_item);
                    drop(feed);

                    // Notify subscribers of update
                    let _ = self.update_tx.send(());

                    Ok(url)
                } else {
                    let body = String::from_utf8_lossy(&resp.body);
                    tracing::error!("[Mail] Send failed with status {}: {}", resp.status, body);
                    Err(anyhow::anyhow!("Server returned status {}: {}", resp.status, body).into())
                }
            }
            Err(e) => {
                tracing::error!("[Mail] Network error sending mail: {}", e);
                Err(e.into())
            }
        }
    }
}

/// API: Subscribe to mail feed
pub async fn subscribe_mail(
    State(state): State<AppState>,
    Json(req): Json<SubscribeMailRequest>,
) -> std::result::Result<Json<SubscribeMailResponse>, axum::http::StatusCode> {
    let feed_url = req
        .feed_url
        .unwrap_or_else(|| "https://mail.braid.org/feed".to_string());

    match state.mail_manager.subscribe_feed(&feed_url).await {
        Ok(_) => Ok(Json(SubscribeMailResponse {
            success: true,
            message: "Subscribed to mail feed".to_string(),
        })),
        Err(e) => {
            error!("[MailAPI] Subscribe failed: {}", e);
            Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// API: Get mail feed
pub async fn get_mail_feed(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> std::result::Result<Response<Body>, StatusCode> {
    info!("[MailAPI] GET /mail/feed");

    // Check if this is a subscription request
    if headers.get(&headers::SUBSCRIBE).is_some() {
        info!("[MailAPI] Establishing Braid subscription for mail feed");

        let mut rx = state.mail_manager.subscribe_updates();
        let mail_manager = state.mail_manager.clone();

        let stream = async_stream::stream! {
            // 1. Send initial feed state
            let initial_items = mail_manager.get_feed_items().await;
            let body = serde_json::to_string(&initial_items).unwrap_or_default();

            let mut update = String::new();
            update.push_str(&format!("Content-Length: {}\r\n", body.len()));
            update.push_str("\r\n");
            update.push_str(&body);
            update.push_str("\r\n\r\n");
            yield Ok::<_, Infallible>(bytes::Bytes::from(update));

            // 2. Stream updates
            loop {
                match rx.recv().await {
                    Ok(_) => {
                        let items = mail_manager.get_feed_items().await;
                        let body = serde_json::to_string(&items).unwrap_or_default();

                        let mut update = String::new();
                        update.push_str(&format!("Content-Length: {}\r\n", body.len()));
                        update.push_str("\r\n");
                        update.push_str(&body);
                        update.push_str("\r\n\r\n");
                        yield Ok::<_, Infallible>(bytes::Bytes::from(update));
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        warn!("[MailAPI] Subscription lagged, continuing...");
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        };

        return Response::builder()
            .status(StatusCode::from_u16(209).unwrap())
            .header(header::CONTENT_TYPE, "application/json")
            .header(headers::SUBSCRIBE.as_str(), "true")
            .header("Merge-Type", "simpleton")
            .body(Body::from_stream(stream))
            .map_err(|e| {
                error!("[MailAPI] Failed to build subscription response: {}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            });
    }

    // Standard JSON response
    let items = state.mail_manager.get_feed_items().await;
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_string(&items).unwrap_or_default(),
        ))
        .unwrap())
}

/// API: Get a specific mail post
pub async fn get_mail_post(
    Path(url): Path<String>,
    State(state): State<AppState>,
) -> std::result::Result<Json<MailPost>, axum::http::StatusCode> {
    // Note: URL is passed as path parameter, may need decoding on client side
    match state.mail_manager.fetch_post(&url).await {
        Ok(post) => Ok(Json(post)),
        Err(e) => {
            error!("[MailAPI] Fetch post failed: {}", e);
            Err(axum::http::StatusCode::NOT_FOUND)
        }
    }
}

/// API: Check subscription status
pub async fn is_subscribed(
    State(state): State<AppState>,
) -> std::result::Result<Json<bool>, axum::http::StatusCode> {
    Ok(Json(state.mail_manager.is_subscribed().await))
}

/// API: Send mail
pub async fn send_mail(
    State(state): State<AppState>,
    Json(req): Json<SendMailRequest>,
) -> std::result::Result<Json<SendMailResponse>, axum::http::StatusCode> {
    let post = MailPost {
        url: String::new(),
        subject: req.subject,
        from: Some(vec![req.from]),
        to: Some(req.to),
        cc: None,
        date: Some(chrono::Utc::now().timestamp_millis() as u64),
        body: req.body,
        version: None,
        parents: None,
        merge_type: None,
        link: None,
    };

    match state.mail_manager.send_mail(post).await {
        Ok(url) => Ok(Json(SendMailResponse { success: true, url })),
        Err(e) => {
            error!("[MailAPI] Send mail failed: {}", e);
            Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

// Request/Response types

#[derive(Debug, Serialize, Deserialize)]
pub struct SubscribeMailRequest {
    pub feed_url: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SubscribeMailResponse {
    pub success: bool,
    pub message: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SendMailRequest {
    pub subject: Option<String>,
    pub from: String,
    pub to: Vec<String>,
    pub body: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SendMailResponse {
    pub success: bool,
    pub url: String,
}

/// API: Set authentication credentials
pub async fn set_mail_auth(
    State(state): State<AppState>,
    Json(req): Json<SetAuthRequest>,
) -> std::result::Result<Json<serde_json::Value>, StatusCode> {
    if let Some(cookie) = req.cookie {
        state.mail_manager.set_cookie(cookie).await;
    }
    if let Some(email) = req.email {
        state.mail_manager.set_email(email).await;
    }
    Ok(Json(serde_json::json!({ "success": true })))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SetAuthRequest {
    pub cookie: Option<String>,
    pub email: Option<String>,
}
