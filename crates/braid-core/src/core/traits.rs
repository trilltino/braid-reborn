use crate::core::error::Result;
use crate::core::{BraidRequest, BraidResponse, Update};
use async_trait::async_trait;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

/// Abstraction for asynchronous runtime operations.
pub trait BraidRuntime: Send + Sync + 'static {
    fn spawn(&self, future: Pin<Box<dyn Future<Output = ()> + Send + 'static>>);
    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
    fn now_ms(&self) -> u64;
}

/// Abstraction for network operations.
#[async_trait]
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
pub trait BraidStorage: Send + Sync + 'static {
    async fn put(&self, key: &str, data: bytes::Bytes, meta: String) -> Result<()>;
    async fn get(&self, key: &str) -> Result<Option<(bytes::Bytes, String)>>;
    async fn delete(&self, key: &str) -> Result<()>;
    async fn list_keys(&self) -> Result<Vec<String>>;
}

#[cfg(feature = "native")]
pub struct NativeRuntime;

#[cfg(feature = "native")]
impl BraidRuntime for NativeRuntime {
    fn spawn(&self, future: Pin<Box<dyn Future<Output = ()> + Send + 'static>>) {
        tokio::spawn(future);
    }

    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
        Box::pin(tokio::time::sleep(duration))
    }

    fn now_ms(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }
}
