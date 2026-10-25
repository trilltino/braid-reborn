//! Thin wasm-bindgen surface over the production browser journal, transport,
//! channel and Simpleton client. It adds no protocol logic of its own; the
//! Playwright suites under `tests/browser` drive it in Chromium, Firefox and WebKit.
#![cfg(target_arch = "wasm32")]

use braid_http::{client::browser::BrowserJournalOptions, client::ChannelJournal, BraidError};
use wasm_bindgen::prelude::*;

/// A JavaScript `Error` whose `code` is stable and whose message is the library's
/// own redacted text.
fn js_error(error: BraidError) -> JsValue {
    let code = match &error {
        BraidError::CapabilityUnavailable { .. } => "capability",
        BraidError::Io(io) if io.kind() == std::io::ErrorKind::WouldBlock => "competing-writer",
        BraidError::Io(io) if io.kind() == std::io::ErrorKind::StorageFull => "quota",
        BraidError::Io(_) => "io",
        BraidError::ResourceLimit { .. } => "resource-limit",
        BraidError::Config(_) => "config",
        BraidError::Aborted => "aborted",
        BraidError::Timeout => "timeout",
        BraidError::Protocol(_) => "protocol",
        _ => "error",
    };
    let value = js_sys::Error::new(&error.to_string());
    let _ = js_sys::Reflect::set(&value, &"code".into(), &code.into());
    value.into()
}

/// The production `ChannelJournal` opened through `open_browser`.
#[wasm_bindgen]
pub struct JournalHarness {
    journal: ChannelJournal,
}
#[wasm_bindgen]
impl JournalHarness {
    pub async fn open(resource: String, context: String) -> Result<JournalHarness, JsValue> {
        let options = BrowserJournalOptions::new(&resource, &context).map_err(js_error)?;
        let journal = ChannelJournal::open_browser(&options).await.map_err(js_error)?;
        Ok(Self { journal })
    }
    pub fn checkpoint(&self) -> Result<String, JsValue> {
        serde_json::to_string(self.journal.checkpoint()).map_err(|e| js_error(e.into()))
    }
    /// Commit `application` (a JSON document) as the first application state.
    pub async fn initialize(&mut self, application: String) -> Result<(), JsValue> {
        let value: serde_json::Value = serde_json::from_str(&application).map_err(|e| js_error(e.into()))?;
        self.journal.initialize_application(vec![], value).await.map_err(js_error)
    }
    pub async fn backup_original(&mut self, label: String) -> Result<(), JsValue> {
        self.journal.backup_original(&label).await.map_err(js_error)
    }
}

/// Fails with `capability` before anything is generated when a facility is missing.
#[wasm_bindgen]
pub fn check_capabilities() -> Result<(), JsValue> {
    braid_http::client::browser::check_runtime_capabilities().map_err(js_error)
}

/// A checked UUID identity; missing crypto is an error, never a panic.
#[wasm_bindgen]
pub fn peer_id() -> Result<String, JsValue> {
    braid_http::client::browser::checked_peer_id().map_err(js_error)
}

/// No network at all: every request fails like a lost connection. The channel
/// keeps queuing durably, which is exactly the offline-editing path.
struct OfflineNetwork;
#[async_trait::async_trait(?Send)]
impl braid_http::traits::BraidNetwork for OfflineNetwork {
    async fn fetch(&self, _: &str, _: braid_http::BraidRequest) -> braid_http::Result<braid_http::BraidResponse> {
        Err(BraidError::Http("offline".into()))
    }
    async fn subscribe(
        &self,
        _: &str,
        _: braid_http::BraidRequest,
    ) -> braid_http::Result<async_channel::Receiver<braid_http::Result<braid_http::types::Update>>> {
        Err(BraidError::Http("offline".into()))
    }
}

/// A production `ReliableChannel` over an IndexedDB journal and no network.
#[wasm_bindgen]
pub struct ChannelHarness {
    channel: Option<braid_http::client::ReliableChannel>,
}
#[wasm_bindgen]
impl ChannelHarness {
    pub async fn open(resource: String, context: String) -> Result<ChannelHarness, JsValue> {
        let options = BrowserJournalOptions::new(&resource, &context).map_err(js_error)?;
        let journal = ChannelJournal::open_browser(&options).await.map_err(js_error)?;
        let channel = braid_http::client::ReliableChannel::start(
            std::sync::Arc::new(OfflineNetwork),
            options.resource_url().to_owned(),
            braid_http::BraidRequest::new(),
            journal,
            braid_http::client::ChannelConfig::default(),
        )
        .map_err(js_error)?;
        Ok(Self { channel: Some(channel) })
    }
    fn channel(&self) -> Result<&braid_http::client::ReliableChannel, JsValue> {
        self.channel.as_ref().ok_or_else(|| js_error(BraidError::Aborted))
    }
    /// Queue a snapshot update durably; resolves after the IndexedDB transaction completes.
    pub async fn queue(&self, version: String, body: String, application: Option<String>) -> Result<(), JsValue> {
        let application = application
            .map(|text| serde_json::from_str(&text))
            .transpose()
            .map_err(|e| js_error(e.into()))?;
        let update = braid_http::types::Update::snapshot(braid_http::types::Version::new(&version), body);
        self.channel()?.queue(update, application).await.map_err(js_error)
    }
    pub async fn save_application(&self, application: String) -> Result<(), JsValue> {
        let value = serde_json::from_str(&application).map_err(|e| js_error(e.into()))?;
        self.channel()?.save_application(value).await.map_err(js_error)
    }
    /// The unacknowledged entries as `[{"version": ..., "body": ...}]`.
    pub async fn pending(&self) -> Result<String, JsValue> {
        let entries = self.channel()?.pending_updates().await.map_err(js_error)?;
        let entries: Vec<_> = entries
            .iter()
            .map(|u| serde_json::json!({"version": u.version.iter().map(ToString::to_string).collect::<Vec<_>>(), "body": u.body_str()}))
            .collect();
        serde_json::to_string(&entries).map_err(|e| js_error(e.into()))
    }
    /// Graceful shutdown: the journal and its Web Lock are released before this resolves.
    pub async fn close(&mut self) {
        if let Some(channel) = self.channel.take() {
            channel.close().await;
        }
    }
}

/// Production Simpleton over the production Fetch transport and IndexedDB journal.
/// A token is used only to authenticate the exact resource origin and is never
/// included in status, diagnostics or application recovery state.
#[wasm_bindgen]
pub struct SimpletonHarness {
    client: Option<braid_core::simpleton::SimpletonClient>,
}
#[wasm_bindgen]
impl SimpletonHarness {
    pub async fn open(resource: String, context: String, bearer: String) -> Result<SimpletonHarness, JsValue> {
        braid_http::client::browser::check_runtime_capabilities().map_err(js_error)?;
        let options = BrowserJournalOptions::new(&resource, &context).map_err(js_error)?;
        let journal = ChannelJournal::open_browser(&options).await.map_err(js_error)?;
        let credentials = braid_http::client::ScopedCredentials::new(
            &braid_http::client::http_origin(options.resource_url()).map_err(js_error)?,
            braid_http::client::OriginCredentials { bearer_token: Some(bearer), cookie: None },
        ).map_err(js_error)?;
        let request = credentials.authenticate(options.resource_url(), braid_http::BraidRequest::new()).map_err(js_error)?;
        let transport = braid_http::BraidClient::new().map_err(js_error)?;
        let client = braid_core::simpleton::SimpletonClient::connect_with_request(
            &transport, options.resource_url(), journal, request,
        ).await.map_err(js_error)?;
        Ok(Self { client: Some(client) })
    }
    pub async fn changed(&self, text: String) -> Result<(), JsValue> {
        self.client.as_ref().ok_or_else(|| js_error(BraidError::Aborted))?.changed(text).await.map_err(js_error)
    }
    pub fn status(&self) -> Result<String, JsValue> {
        let client = self.client.as_ref().ok_or_else(|| js_error(BraidError::Aborted))?;
        let status = client.status().borrow().clone();
        let diagnostics = client.diagnostics().borrow().clone();
        serde_json::to_string(&serde_json::json!({
            "text": status.text, "version": status.version, "online": status.online,
            "pending_count": status.outstanding_puts, "has_pending_edit": status.has_pending_edit,
            "initialized": status.initialized, "error": status.error, "revision": status.revision,
            "recovery": format!("{:?}", status.recovery), "state": format!("{:?}", diagnostics.state),
            "receive_frontier": diagnostics.receive_frontier,
            "retry_reason": diagnostics.retry_reason.map(|reason| format!("{reason:?}")),
            "repair_cause": diagnostics.repair_cause.map(|cause| format!("{cause:?}")),
        })).map_err(|error| js_error(error.into()))
    }
    pub async fn close(&mut self) {
        if let Some(client) = self.client.take() { client.close().await; }
    }
}

/// Direct production Fetch/codec access for cancellation and slow-reader tests.
#[wasm_bindgen]
pub struct FetchHarness {
    receiver: Option<async_channel::Receiver<braid_http::Result<braid_http::types::Update>>>,
}
#[wasm_bindgen]
impl FetchHarness {
    pub async fn subscribe(resource: String, bearer: String) -> Result<FetchHarness, JsValue> {
        use braid_http::traits::BraidNetwork;
        let credentials = braid_http::client::ScopedCredentials::new(&braid_http::client::http_origin(&resource).map_err(js_error)?,
            braid_http::client::OriginCredentials { bearer_token: Some(bearer), cookie: None }).map_err(js_error)?;
        let request = credentials.authenticate(&resource, braid_http::BraidRequest::new()).map_err(js_error)?;
        let receiver = braid_http::client::wasm_network::WasmNetwork.subscribe(&resource, request).await.map_err(js_error)?;
        Ok(Self { receiver: Some(receiver) })
    }
    pub async fn next(&self) -> Result<Option<String>, JsValue> {
        let Some(receiver) = &self.receiver else { return Err(js_error(BraidError::Aborted)); };
        match receiver.recv().await {
            Ok(update) => serde_json::to_string(&update.map_err(js_error)?).map(Some).map_err(|error| js_error(error.into())),
            Err(_) => Ok(None),
        }
    }
    pub fn close(&mut self) { self.receiver = None; }
    pub async fn finite(resource: String, bearer: String, version: Option<String>) -> Result<String, JsValue> {
        use braid_http::traits::BraidNetwork;
        let credentials = braid_http::client::ScopedCredentials::new(&braid_http::client::http_origin(&resource).map_err(js_error)?,
            braid_http::client::OriginCredentials { bearer_token: Some(bearer), cookie: None }).map_err(js_error)?;
        let mut request = credentials.authenticate(&resource, braid_http::BraidRequest::new()).map_err(js_error)?;
        if let Some(version) = version { request.version = Some(vec![braid_http::types::Version::new(version)]); }
        let response = braid_http::client::wasm_network::WasmNetwork.fetch(&resource, request).await.map_err(js_error)?;
        serde_json::to_string(&serde_json::json!({
            "status": response.status, "headers": response.headers,
            "body": String::from_utf8_lossy(&response.body),
        })).map_err(|error| js_error(error.into()))
    }
}
