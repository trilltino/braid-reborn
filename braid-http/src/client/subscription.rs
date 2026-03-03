//! Subscription handling for Braid protocol.

use crate::error::{BraidError, Result};
use crate::types::Update;
use futures::Stream;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

pub struct ReceiverStream<T> {
    receiver: async_channel::Receiver<T>,
}

impl<T> ReceiverStream<T> {
    pub fn new(receiver: async_channel::Receiver<T>) -> Self {
        Self { receiver }
    }
}

impl<T: Unpin> Stream for ReceiverStream<T> {
    type Item = T;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut fut = self.receiver.recv();
        unsafe { Pin::new_unchecked(&mut fut) }
            .poll(cx)
            .map(|res| res.ok())
    }
}

#[cfg(not(target_arch = "wasm32"))]
use std::time::Instant;

#[cfg(target_arch = "wasm32")]
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct Instant {
    start_ms: f64,
}

#[cfg(target_arch = "wasm32")]
impl Instant {
    pub fn now() -> Self {
        let start_ms = web_sys::window()
            .and_then(|w| w.performance())
            .map(|p| p.now())
            .unwrap_or(0.0);
        Self { start_ms }
    }
    pub fn elapsed(&self) -> Duration {
        let now = web_sys::window()
            .and_then(|w| w.performance())
            .map(|p| p.now())
            .unwrap_or(0.0);
        Duration::from_secs_f64(((now - self.start_ms).max(0.0)) / 1000.0)
    }
}

#[cfg(target_arch = "wasm32")]
impl std::ops::Add<Duration> for Instant {
    type Output = Instant;
    fn add(self, other: Duration) -> Instant {
        Instant {
            start_ms: self.start_ms + other.as_secs_f64() * 1000.0,
        }
    }
}

/// Configuration for heartbeat timeout detection.
#[derive(Debug, Clone)]
pub struct HeartbeatConfig {
    pub interval_secs: f64,
    pub timeout: Duration,
}

impl HeartbeatConfig {
    pub fn new(interval_secs: f64) -> Self {
        let timeout_secs = 1.2 * interval_secs + 3.0;
        Self {
            interval_secs,
            timeout: Duration::from_secs_f64(timeout_secs),
        }
    }

    pub fn from_header(value: &str) -> Option<Self> {
        value
            .trim()
            .strip_suffix('s')
            .unwrap_or(value)
            .parse::<f64>()
            .ok()
            .map(Self::new)
    }
}

pub struct Subscription {
    receiver: async_channel::Receiver<Result<Update>>,
    heartbeat_config: Option<HeartbeatConfig>,
    last_activity: Instant,
}

impl Subscription {
    pub fn new(receiver: async_channel::Receiver<Result<Update>>) -> Self {
        Subscription {
            receiver,
            heartbeat_config: None,
            last_activity: Instant::now(),
        }
    }

    pub fn with_heartbeat(
        receiver: async_channel::Receiver<Result<Update>>,
        heartbeat_config: HeartbeatConfig,
    ) -> Self {
        Subscription {
            receiver,
            heartbeat_config: Some(heartbeat_config),
            last_activity: Instant::now(),
        }
    }

    pub async fn next(&mut self) -> Option<Result<Update>> {
        if let Some(ref config) = self.heartbeat_config {
            let timeout = config.timeout;
            #[cfg(not(target_arch = "wasm32"))]
            {
                let deadline = self.last_activity + timeout;
                tokio::select! {
                    result = self.receiver.recv() => {
                        self.last_activity = Instant::now();
                        result.ok()
                    }
                    _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                        Some(Err(BraidError::Timeout))
                    }
                }
            }
            #[cfg(target_arch = "wasm32")]
            {
                use futures::{future::FutureExt, pin_mut, select};
                let recv_fut = self.receiver.recv().fuse();
                let timer_fut =
                    gloo_timers::future::TimeoutFuture::new(timeout.as_millis() as u32).fuse();
                pin_mut!(recv_fut, timer_fut);
                select! {
                    result = recv_fut => {
                        self.last_activity = Instant::now();
                        result.ok()
                    }
                    _ = timer_fut => Some(Err(BraidError::Timeout))
                }
            }
        } else {
            let result = self.receiver.recv().await.ok();
            self.last_activity = Instant::now();
            result
        }
    }

    pub fn is_heartbeat_timeout(&self) -> bool {
        if let Some(ref config) = self.heartbeat_config {
            self.last_activity.elapsed() > config.timeout
        } else {
            false
        }
    }
}

impl Stream for Subscription {
    type Item = Result<Update>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.get_unchecked_mut() };
        let mut fut = this.receiver.recv();
        match unsafe { Pin::new_unchecked(&mut fut) }.poll(cx) {
            Poll::Ready(result) => {
                this.last_activity = Instant::now();
                Poll::Ready(result.ok())
            }
            Poll::Pending => {
                if this.is_heartbeat_timeout() {
                    Poll::Ready(Some(Err(BraidError::Timeout)))
                } else {
                    Poll::Pending
                }
            }
        }
    }
}

pub struct SubscriptionStream {
    receiver: ReceiverStream<Result<Update>>,
}

impl SubscriptionStream {
    pub fn new(receiver: async_channel::Receiver<Result<Update>>) -> Self {
        SubscriptionStream {
            receiver: ReceiverStream::new(receiver),
        }
    }
}

impl Stream for SubscriptionStream {
    type Item = Result<Update>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.get_unchecked_mut() };
        unsafe { Pin::new_unchecked(&mut this.receiver) }.poll_next(cx)
    }
}
