//! Subscription streams retain their pending receive future across polls.
use crate::error::{BraidError, Result};
use crate::types::Update;
use futures::Stream;
use std::{
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

pub struct ReceiverStream<T> {
    receiver: Pin<Box<async_channel::Receiver<T>>>,
}
impl<T> ReceiverStream<T> {
    pub fn new(receiver: async_channel::Receiver<T>) -> Self {
        Self {
            receiver: Box::pin(receiver),
        }
    }
}
impl<T> Stream for ReceiverStream<T> {
    type Item = T;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        self.get_mut().receiver.as_mut().poll_next(cx)
    }
}
#[derive(Debug, Clone)]
pub struct HeartbeatConfig {
    pub interval_secs: f64,
    pub timeout: Duration,
}
impl HeartbeatConfig {
    pub fn new(interval_secs: f64) -> Self {
        let interval_secs = if interval_secs.is_finite() && interval_secs > 0.0 {
            interval_secs.min(86400.0)
        } else {
            30.0
        };
        Self {
            interval_secs,
            timeout: Duration::from_secs_f64(1.2 * interval_secs + 3.0),
        }
    }
    pub fn from_header(value: &str) -> Option<Self> {
        crate::protocol::parse_heartbeat_duration(value)
            .ok()
            .map(|duration| Self::new(duration.as_secs_f64()))
    }
}

pub struct Subscription {
    receiver: ReceiverStream<Result<Update>>,
    heartbeat_config: Option<HeartbeatConfig>,
    #[cfg(not(target_arch = "wasm32"))]
    timer: Option<Pin<Box<tokio::time::Sleep>>>,
    #[cfg(target_arch = "wasm32")]
    timer: Option<Pin<Box<gloo_timers::future::TimeoutFuture>>>,
    timed_out: bool,
}
impl Subscription {
    pub fn new(receiver: async_channel::Receiver<Result<Update>>) -> Self {
        Self {
            receiver: ReceiverStream::new(receiver),
            heartbeat_config: None,
            timer: None,
            timed_out: false,
        }
    }
    pub fn with_heartbeat(
        receiver: async_channel::Receiver<Result<Update>>,
        config: HeartbeatConfig,
    ) -> Self {
        let mut subscription = Self::new(receiver);
        subscription.heartbeat_config = Some(config);
        subscription.reset_timer();
        subscription
    }
    fn reset_timer(&mut self) {
        if let Some(config) = &self.heartbeat_config {
            #[cfg(not(target_arch = "wasm32"))]
            {
                self.timer = Some(Box::pin(tokio::time::sleep(config.timeout)));
            }
            #[cfg(target_arch = "wasm32")]
            {
                self.timer = Some(Box::pin(gloo_timers::future::TimeoutFuture::new(
                    config.timeout.as_millis().min(u32::MAX as u128) as u32,
                )));
            }
        }
    }
    pub async fn next(&mut self) -> Option<Result<Update>> {
        futures::StreamExt::next(self).await
    }
    pub fn is_heartbeat_timeout(&self) -> bool {
        self.timed_out
    }
}
impl Stream for Subscription {
    type Item = Result<Update>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use std::future::Future;
        let this = self.get_mut();
        if this.timed_out {
            return Poll::Ready(None);
        }
        match Pin::new(&mut this.receiver).poll_next(cx) {
            Poll::Ready(result) => {
                this.reset_timer();
                Poll::Ready(result)
            }
            Poll::Pending => {
                if let Some(timer) = &mut this.timer {
                    if timer.as_mut().poll(cx).is_ready() {
                        this.timed_out = true;
                        return Poll::Ready(Some(Err(BraidError::Timeout)));
                    }
                }
                Poll::Pending
            }
        }
    }
}
pub struct SubscriptionStream {
    receiver: ReceiverStream<Result<Update>>,
}
impl SubscriptionStream {
    pub fn new(receiver: async_channel::Receiver<Result<Update>>) -> Self {
        Self {
            receiver: ReceiverStream::new(receiver),
        }
    }
}
impl Stream for SubscriptionStream {
    type Item = Result<Update>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.get_mut().receiver).poll_next(cx)
    }
}
