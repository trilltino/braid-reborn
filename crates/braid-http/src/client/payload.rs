//! Immutable payload references for large outgoing bodies.
//!
//! A durable outbox entry names content by digest and length instead of copying
//! it into the 32 MiB checkpoint. The bytes must stay available, unchanged, until
//! the server acknowledges the PUT; the transport verifies both while streaming.
use crate::{BraidError, Result};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    pin::Pin,
    task::{Context, Poll},
};

#[cfg(not(target_arch = "wasm32"))]
pub type PayloadStream = futures::stream::BoxStream<'static, Result<Bytes>>;
#[cfg(target_arch = "wasm32")]
pub type PayloadStream = futures::stream::LocalBoxStream<'static, Result<Bytes>>;

/// Content identity of an outgoing body: lowercase hex SHA-256 and exact length.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PayloadRef {
    pub sha256: String,
    pub length: u64,
    pub media_type: String,
}
impl PayloadRef {
    /// Describe in-memory content. Large producers compute the same fields while
    /// writing their immutable object.
    pub fn for_bytes(media_type: impl Into<String>, bytes: &[u8]) -> Self {
        Self {
            sha256: hex(&Sha256::digest(bytes)),
            length: bytes.len() as u64,
            media_type: media_type.into(),
        }
    }
    pub(crate) fn validate(&self) -> Result<()> {
        if self.sha256.len() != 64
            || !self
                .sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(BraidError::Config(
                "Payload digest must be 64 lowercase hex characters".into(),
            ));
        }
        if self.media_type.is_empty() || self.media_type.chars().any(char::is_control) {
            return Err(BraidError::Config("Invalid payload media type".into()));
        }
        Ok(())
    }
}
fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes
        .iter()
        .fold(String::with_capacity(64), |mut text, byte| {
            let _ = write!(text, "{byte:02x}");
            text
        })
}

/// Opens immutable content on demand. Implementations must return the same bytes
/// for the same reference for as long as any outbox entry names it.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[allow(clippy::double_must_use)]
pub trait PayloadSource: Send + Sync + 'static {
    async fn open(&self, payload: &PayloadRef) -> Result<PayloadStream>;
}

/// Fails the stream, and therefore the request body, on any length or digest
/// difference. A truncated or substituted body can never look complete.
pub(crate) struct VerifiedPayload {
    inner: PayloadStream,
    expected: PayloadRef,
    hasher: Sha256,
    seen: u64,
    finished: bool,
}
impl VerifiedPayload {
    pub(crate) fn new(inner: PayloadStream, expected: PayloadRef) -> Self {
        Self {
            inner,
            expected,
            hasher: Sha256::new(),
            seen: 0,
            finished: false,
        }
    }
}
impl Stream for VerifiedPayload {
    type Item = Result<Bytes>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }
        match self.inner.poll_next_unpin(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(Ok(chunk))) => {
                self.seen = self.seen.saturating_add(chunk.len() as u64);
                if self.seen > self.expected.length {
                    self.finished = true;
                    return Poll::Ready(Some(Err(corrupt("longer than declared"))));
                }
                self.hasher.update(&chunk);
                Poll::Ready(Some(Ok(chunk)))
            }
            Poll::Ready(Some(Err(error))) => {
                self.finished = true;
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(None) => {
                self.finished = true;
                if self.seen != self.expected.length {
                    return Poll::Ready(Some(Err(corrupt("shorter than declared"))));
                }
                let actual = hex(&self.hasher.clone().finalize());
                if actual != self.expected.sha256 {
                    return Poll::Ready(Some(Err(corrupt("digest differs from its reference"))));
                }
                Poll::Ready(None)
            }
        }
    }
}
fn corrupt(reason: &str) -> BraidError {
    BraidError::Payload(format!("Immutable payload is {reason}"))
}

/// Buffer an unverified stream for the default `BraidNetwork::put_stream` adapter.
pub async fn collect_payload(stream: PayloadStream, limit: usize) -> Result<Bytes> {
    let mut stream = stream;
    let mut body = bytes::BytesMut::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if body.len().saturating_add(chunk.len()) > limit {
            return Err(BraidError::ResourceLimit {
                resource: "buffered payload",
                limit: limit as u64,
            });
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body.freeze())
}
