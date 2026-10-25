//! Browser capabilities and a single-writer IndexedDB channel journal.
use super::{ChannelJournal, JournalBackend, MAX_JOURNAL_BYTES};
use crate::{BraidError, Result};
use async_trait::async_trait;
use wasm_bindgen::prelude::*;

#[wasm_bindgen(module = "/src/client/browser_runtime.js")]
extern "C" {
    #[wasm_bindgen(catch, js_name = browserCheckCapabilities)]
    fn check_capabilities(storage: bool) -> std::result::Result<(), JsValue>;
    #[wasm_bindgen(catch, js_name = browserRandomBytes)]
    fn random_bytes() -> std::result::Result<js_sys::Uint8Array, JsValue>;
    #[wasm_bindgen(js_name = browserErrorCode)]
    pub(super) fn error_code(error: &JsValue) -> String;
    #[wasm_bindgen(catch, js_name = browserJournalStart)]
    fn journal_start(resource: &str, context: &str) -> std::result::Result<JsValue, JsValue>;
    #[wasm_bindgen(catch, js_name = browserJournalReady)]
    async fn journal_ready(state: &JsValue) -> std::result::Result<JsValue, JsValue>;
    #[wasm_bindgen(catch, js_name = browserJournalLoad)]
    async fn journal_load(state: &JsValue) -> std::result::Result<JsValue, JsValue>;
    #[wasm_bindgen(catch, js_name = browserJournalStore)]
    async fn journal_store(state: &JsValue, payload: &[u8]) -> std::result::Result<JsValue, JsValue>;
    #[wasm_bindgen(catch, js_name = browserJournalBackup)]
    async fn journal_backup(state: &JsValue, label: &str, payload: &[u8]) -> std::result::Result<JsValue, JsValue>;
    #[wasm_bindgen(catch, js_name = browserJournalClose)]
    fn journal_close(state: &JsValue) -> std::result::Result<(), JsValue>;
}

/// Check before creating identities, starting requests or spawning browser work.
pub fn check_runtime_capabilities() -> Result<()> {
    check_capabilities(false).map_err(browser_error)
}

/// A checked v4 identity: missing or failing crypto never reaches UUID's panic path.
pub fn checked_peer_id() -> Result<String> {
    let bytes = random_bytes().map_err(browser_error)?;
    let mut identity = [0; 16];
    if bytes.length() != identity.len() as u32 {
        return Err(BraidError::CapabilityUnavailable { capability: "browser crypto" });
    }
    bytes.copy_to(&mut identity);
    Ok(uuid::Builder::from_random_bytes(identity).into_uuid().to_string())
}

/// The namespace is a stable authenticated principal identity, never a token.
#[derive(Clone)]
pub struct BrowserJournalOptions {
    resource_url: String,
    authorization_context: String,
}
impl BrowserJournalOptions {
    pub fn new(resource_url: &str, authorization_context: &str) -> Result<Self> {
        let parsed = super::validate_resource_url(resource_url)?;
        if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() || parsed.fragment().is_some() || resource_url.len() > 8192 {
            return Err(BraidError::Config("Browser journal needs a bounded HTTP(S) resource URL without a fragment".into()));
        }
        if authorization_context.is_empty() || authorization_context.len() > 512 || authorization_context.chars().any(char::is_control) {
            return Err(BraidError::Config("Browser journal needs a bounded authorization context without control characters".into()));
        }
        Ok(Self { resource_url: parsed.to_string(), authorization_context: authorization_context.to_owned() })
    }
    pub fn resource_url(&self) -> &str { &self.resource_url }
}

/// The exclusive Web Lock is owned until the last IndexedDB transaction ends.
pub struct BrowserJournal {
    state: JsValue,
}
impl BrowserJournal {
    pub async fn open(options: &BrowserJournalOptions) -> Result<Self> {
        let journal = Self { state: journal_start(&options.resource_url, &options.authorization_context).map_err(browser_error)? };
        journal_ready(&journal.state).await.map_err(browser_error)?;
        Ok(journal)
    }
}
impl Drop for BrowserJournal {
    fn drop(&mut self) { let _ = journal_close(&self.state); }
}
#[async_trait(?Send)]
impl JournalBackend for BrowserJournal {
    async fn load(&mut self) -> Result<Option<Vec<u8>>> {
        let payload = journal_load(&self.state).await.map_err(browser_error)?;
        if payload.is_undefined() { return Ok(None); }
        let payload = js_sys::Uint8Array::new(&payload);
        if payload.length() as usize > MAX_JOURNAL_BYTES { return Err(journal_limit()); }
        Ok(Some(payload.to_vec()))
    }
    async fn store(&mut self, bytes: &[u8]) -> Result<()> {
        if bytes.len() > MAX_JOURNAL_BYTES { return Err(journal_limit()); }
        journal_store(&self.state, bytes).await.map_err(browser_error)?;
        Ok(())
    }
    /// Write-once: the first backup of a label is the original and is never replaced.
    async fn backup(&mut self, label: &str, bytes: &[u8]) -> Result<()> {
        if bytes.len() > MAX_JOURNAL_BYTES { return Err(journal_limit()); }
        journal_backup(&self.state, label, bytes).await.map_err(browser_error)?;
        Ok(())
    }
}
impl ChannelJournal {
    pub async fn open_browser(options: &BrowserJournalOptions) -> Result<Self> {
        let journal = Self::from_backend(BrowserJournal::open(options).await?).await?;
        if journal.checkpoint().resource_url.as_ref().is_some_and(|resource| resource != &options.resource_url) {
            return Err(BraidError::Config("Browser journal is bound to a different resource".into()));
        }
        Ok(journal)
    }
}

fn journal_limit() -> BraidError { BraidError::ResourceLimit { resource: "channel journal", limit: MAX_JOURNAL_BYTES as u64 } }

pub(super) fn browser_error(error: JsValue) -> BraidError {
    match error_code(&error).as_str() {
        "secure-context" => BraidError::CapabilityUnavailable { capability: "secure browser context" },
        "fetch" => BraidError::CapabilityUnavailable { capability: "browser Fetch" },
        "abort-controller" => BraidError::CapabilityUnavailable { capability: "browser AbortController" },
        "readable-stream" => BraidError::CapabilityUnavailable { capability: "browser ReadableStream" },
        "crypto" => BraidError::CapabilityUnavailable { capability: "browser crypto" },
        "text-encoder" => BraidError::CapabilityUnavailable { capability: "browser UTF-8 encoder" },
        "promise" => BraidError::CapabilityUnavailable { capability: "browser Promise" },
        "microtasks" => BraidError::CapabilityUnavailable { capability: "browser microtasks" },
        "timers" => BraidError::CapabilityUnavailable { capability: "browser timers" },
        "indexed-db" => BraidError::CapabilityUnavailable { capability: "IndexedDB" },
        "web-locks" => BraidError::CapabilityUnavailable { capability: "Web Locks" },
        "journal-busy" => std::io::Error::new(std::io::ErrorKind::WouldBlock, "Another writer owns this browser journal (competing writer)").into(),
        "database-blocked" => std::io::Error::new(std::io::ErrorKind::WouldBlock, "IndexedDB schema opening is blocked").into(),
        "quota" => std::io::Error::new(std::io::ErrorKind::StorageFull, "IndexedDB quota exceeded").into(),
        "aborted" => BraidError::Aborted,
        "transaction-aborted" => std::io::Error::other("IndexedDB transaction aborted before commit").into(),
        "timeout" => BraidError::Timeout,
        "journal-schema" => BraidError::Config("Unsupported or invalid browser journal schema".into()),
        "journal-binding" => BraidError::Config("Browser journal authorization/resource binding mismatch".into()),
        "journal-limit" => journal_limit(),
        "opaque-response" => BraidError::Protocol("Browser Fetch returned an opaque response".into()),
        _ => BraidError::Io(std::io::Error::other("Browser operation failed")),
    }
}
