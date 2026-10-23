use crate::error::Result;
use crate::types::{BraidRequest, BraidResponse, Update};
use async_trait::async_trait;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

/// Browser futures run on the local executor; native futures may move between threads.
#[cfg(not(target_arch = "wasm32"))]
pub type RuntimeFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
#[cfg(target_arch = "wasm32")]
pub type RuntimeFuture = Pin<Box<dyn Future<Output = ()> + 'static>>;

/// Abstraction for asynchronous runtime operations.
pub trait BraidRuntime: Send + Sync + 'static {
    fn spawn(&self, future: RuntimeFuture);
    fn sleep(&self, duration: Duration) -> RuntimeFuture;
    fn now_ms(&self) -> u64;
}

/// Abstraction for network operations.
#[cfg_attr(target_arch="wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[allow(clippy::double_must_use)] // async-trait emits must_use on an already must_use boxed Future.
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
#[allow(clippy::double_must_use)] // Same async-trait macro expansion.
pub trait BraidStorage: Send + Sync + 'static {
    async fn put(&self, key: &str, data: bytes::Bytes, meta: String) -> Result<()>;
    async fn get(&self, key: &str) -> Result<Option<(bytes::Bytes, String)>>;
    async fn delete(&self, key: &str) -> Result<()>;
    async fn list_keys(&self) -> Result<Vec<String>>;
}

#[cfg(all(feature = "native", not(target_arch = "wasm32")))]
pub struct NativeRuntime;

#[cfg(all(feature = "native", not(target_arch = "wasm32")))]
impl BraidRuntime for NativeRuntime {
    fn spawn(&self, future: RuntimeFuture) {
        tokio::spawn(future);
    }

    fn sleep(&self, duration: Duration) -> RuntimeFuture {
        Box::pin(tokio::time::sleep(duration))
    }

    fn now_ms(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }
}

#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
pub struct WasmRuntime;

#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
impl BraidRuntime for WasmRuntime {
    fn spawn(&self, future: RuntimeFuture) {
        wasm_bindgen_futures::spawn_local(future);
    }

    fn sleep(&self, duration: Duration) -> RuntimeFuture {
        Box::pin(gloo_timers::future::sleep(duration))
    }

    fn now_ms(&self) -> u64 {
        js_sys::Date::now() as u64
    }
}
