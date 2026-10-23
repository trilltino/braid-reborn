//! Server-side subscription utilities.

use axum::body::Bytes;
use futures::{Stream, StreamExt};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::time::{interval, Duration, Interval};

/// A stream wrapper that injects heartbeat blank lines (\r\n).
pub struct HeartbeatStream<S> {
    inner: S,
    heartbeat: Interval,
}

impl<S> HeartbeatStream<S> {
    pub fn new(inner: S, delay: Duration) -> Self {
        let mut heartbeat = interval(delay);
        heartbeat.reset();
        Self { inner, heartbeat }
    }
}

impl<S, T, E> Stream for HeartbeatStream<S>
where
    S: Stream<Item = Result<T, E>> + Unpin,
    T: From<Bytes>,
{
    type Item = Result<T, E>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.poll_next_unpin(cx) {
            Poll::Ready(Some(item)) => {
                self.heartbeat.reset();
                return Poll::Ready(Some(item));
            }
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Pending => {}
        }
        match self.heartbeat.poll_tick(cx) {
            Poll::Ready(_) => Poll::Ready(Some(Ok(T::from(Bytes::from("\r\n"))))),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn test_heartbeats() {
        let (_tx, rx) = futures::channel::mpsc::unbounded::<Result<Bytes, std::io::Error>>();
        let mut hb_stream = HeartbeatStream::new(rx, Duration::from_millis(10));
        tokio::time::sleep(Duration::from_millis(20)).await;
        let item = hb_stream.next().await.unwrap().unwrap();
        assert_eq!(item, Bytes::from("\r\n"));
    }
}
