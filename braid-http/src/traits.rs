use crate::error::Result;
use crate::types::{BraidRequest, BraidResponse, Update};
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

#[cfg(all(feature = "native", not(target_arch = "wasm32")))]
pub struct NativeRuntime;

#[cfg(all(feature = "native", not(target_arch = "wasm32")))]
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

#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
pub struct WasmRuntime;

#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
struct SendFuture<F>(F);
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
unsafe impl<F> Send for SendFuture<F> {}
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
impl<F: Future> Future for SendFuture<F> {
    type Output = F::Output;
    fn poll(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let inner = unsafe { self.map_unchecked_mut(|s| &mut s.0) };
        inner.poll(cx)
    }
}

#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
impl BraidRuntime for WasmRuntime {
    fn spawn(&self, future: Pin<Box<dyn Future<Output = ()> + Send + 'static>>) {
        wasm_bindgen_futures::spawn_local(future);
    }

    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
        let f = gloo_timers::future::sleep(duration);
        Box::pin(SendFuture(f))
    }

    fn now_ms(&self) -> u64 {
        js_sys::Date::now() as u64
    }
}
