use crate::core::error::Result;
use crate::core::{BraidRequest, BraidResponse, Update};
use async_trait::async_trait;
pub use braid_http::traits::BraidRuntime;

/// Abstraction for network operations.
#[async_trait]
#[allow(clippy::double_must_use)] // Generated async-trait attribute.
pub trait BraidNetwork: Send + Sync + 'static {
    async fn fetch(&self, url: &str, req: BraidRequest) -> Result<BraidResponse>;
    async fn subscribe(
        &self,
        url: &str,
        req: BraidRequest,
    ) -> Result<async_channel::Receiver<Result<Update>>>;
}

/// Abstraction for persistent storage.
#[async_trait]
#[allow(clippy::double_must_use)] // Generated async-trait attribute.
pub trait BraidStorage: Send + Sync + 'static {
    async fn put(&self, key: &str, data: bytes::Bytes, meta: String) -> Result<()>;
    async fn get(&self, key: &str) -> Result<Option<(bytes::Bytes, String)>>;
    async fn delete(&self, key: &str) -> Result<()>;
    async fn list_keys(&self) -> Result<Vec<String>>;
}

#[cfg(all(feature = "native", not(target_arch = "wasm32")))]
pub use braid_http::traits::NativeRuntime;
