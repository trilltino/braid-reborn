//! Owned task cancellation and timers without a filesystem or native-only timer.
use crate::{BraidError, Result};
use futures::future::{AbortHandle, Abortable};
use std::{future::Future, time::Duration};

pub struct OwnedTask {
    abort: AbortHandle,
    completed: async_channel::Receiver<()>,
}
impl OwnedTask {
    #[cfg(not(target_arch = "wasm32"))]
    pub fn spawn<F: Future<Output = ()> + Send + 'static>(future: F) -> Result<Self> {
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| {
            BraidError::CapabilityUnavailable {
                capability: "Tokio runtime",
            }
        })?;
        let (abort, registration) = AbortHandle::new_pair();
        let (completed, receiver) = async_channel::bounded(1);
        runtime.spawn(async move {
            let _completed = completed;
            let _ = Abortable::new(future, registration).await;
        });
        Ok(Self {
            abort,
            completed: receiver,
        })
    }
    #[cfg(target_arch = "wasm32")]
    pub fn spawn<F: Future<Output = ()> + 'static>(future: F) -> Result<Self> {
        let (abort, registration) = AbortHandle::new_pair();
        let (completed, receiver) = async_channel::bounded(1);
        wasm_bindgen_futures::spawn_local(async move {
            let _completed = completed;
            let _ = Abortable::new(future, registration).await;
        });
        Ok(Self {
            abort,
            completed: receiver,
        })
    }
    pub fn abort(&self) {
        self.abort.abort();
    }
    pub async fn join(&mut self) {
        let _ = self.completed.recv().await;
    }
}
impl Drop for OwnedTask {
    fn drop(&mut self) {
        self.abort();
    }
}

pub(super) async fn timeout<T>(duration: Duration, future: impl Future<Output = T>) -> Result<T> {
    let timer = super::utils::sleep(duration);
    futures::pin_mut!(timer, future);
    match futures::future::select(future, timer).await {
        futures::future::Either::Left((value, _)) => Ok(value),
        futures::future::Either::Right(_) => Err(BraidError::Timeout),
    }
}
