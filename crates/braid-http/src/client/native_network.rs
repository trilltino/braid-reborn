use crate::client::parser::MessageParser;
use crate::error::{BraidError, Result};
use crate::traits::BraidNetwork;
use crate::types::{BraidRequest, BraidResponse, Update};
use async_trait::async_trait;
use futures::StreamExt;
use reqwest::Client;

pub struct NativeNetwork {
    client: Client,
    subscription_client: Client,
    request_timeout: std::time::Duration,
}
impl NativeNetwork {
    pub fn new(client: Client) -> Self {
        Self {
            subscription_client: client.clone(),
            client,
            request_timeout: std::time::Duration::from_secs(30),
        }
    }
    pub fn with_subscription_client(client: Client, subscription_client: Client) -> Self {
        Self {
            client,
            subscription_client,
            request_timeout: std::time::Duration::from_secs(30),
        }
    }
    pub fn with_request_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.request_timeout = timeout;
        self
    }
    pub fn client(&self) -> &Client {
        &self.client
    }
    fn request(
        &self,
        client: &Client,
        url: &str,
        request: &BraidRequest,
    ) -> Result<reqwest::RequestBuilder> {
        super::validate_resource_url(url)?;
        let method = reqwest::Method::from_bytes(request.method.as_bytes())
            .map_err(|e| BraidError::Protocol(e.to_string()))?;
        let (headers, body) = crate::protocol::encode_request(request)?;
        let mut builder = client.request(method, url);
        for (k, v) in headers {
            let mut value = http::HeaderValue::from_str(&v).map_err(|_| {
                BraidError::HeaderParse("Invalid outgoing HTTP header value".into())
            })?;
            if crate::protocol::headers::is_credential_header(&k) {
                value.set_sensitive(true);
            }
            builder = builder.header(k, value);
        }
        Ok(builder.body(body))
    }
}
#[async_trait]
impl BraidNetwork for NativeNetwork {
    async fn fetch(&self, url: &str, request: BraidRequest) -> Result<BraidResponse> {
        let response = self
            .request(&self.client, url, &request)?
            .timeout(self.request_timeout)
            .send()
            .await
            .map_err(|e| BraidError::Http(e.to_string()))?;
        let status = response.status().as_u16();
        let headers = crate::protocol::headers_from_http(response.headers())?;
        let mut chunks = response.bytes_stream();
        let mut body = bytes::BytesMut::new();
        while let Some(chunk) = chunks.next().await {
            let chunk = chunk.map_err(|e| BraidError::Http(e.to_string()))?;
            if body.len().saturating_add(chunk.len()) > 32 * 1024 * 1024 {
                return Err(BraidError::Protocol("HTTP entity limit exceeded".into()));
            }
            body.extend_from_slice(&chunk);
        }
        let body = body.freeze();
        Ok(BraidResponse {
            status,
            headers,
            body,
            is_subscription: status == 209,
        })
    }
    /// Streams the verified payload; memory use stays bounded by the chunk size.
    async fn put_stream(
        &self,
        url: &str,
        mut request: BraidRequest,
        body: crate::client::PayloadStream,
        length: u64,
    ) -> Result<BraidResponse> {
        request.body = bytes::Bytes::new();
        request.patches = None;
        let builder = self.request(&self.client, url, &request)?;
        // Streams are not retried by reqwest; the channel replays the whole immutable PUT.
        let timeout = self
            .request_timeout
            .saturating_add(std::time::Duration::from_secs(length / (1024 * 1024)));
        let response = builder
            .header(http::header::CONTENT_LENGTH, length)
            .body(reqwest::Body::wrap_stream(body))
            .timeout(timeout)
            .send()
            .await
            .map_err(|e| BraidError::Http(e.to_string()))?;
        let status = response.status().as_u16();
        let headers = crate::protocol::headers_from_http(response.headers())?;
        let mut chunks = response.bytes_stream();
        let mut response_body = bytes::BytesMut::new();
        while let Some(chunk) = chunks.next().await {
            let chunk = chunk.map_err(|e| BraidError::Http(e.to_string()))?;
            if response_body.len().saturating_add(chunk.len()) > 1024 * 1024 {
                return Err(BraidError::Protocol("PUT response limit exceeded".into()));
            }
            response_body.extend_from_slice(&chunk);
        }
        Ok(BraidResponse {
            status,
            headers,
            body: response_body.freeze(),
            is_subscription: false,
        })
    }
    async fn subscribe(
        &self,
        url: &str,
        mut request: BraidRequest,
    ) -> Result<async_channel::Receiver<Result<Update>>> {
        request.subscribe = true;
        request.method = "GET".into();
        let response = tokio::time::timeout(
            self.request_timeout,
            self.request(&self.subscription_client, url, &request)?
                .send(),
        )
        .await
        .map_err(|_| BraidError::Timeout)?
        .map_err(|e| BraidError::Http(e.to_string()))?;
        if response.status().as_u16() != 209 {
            return Err(BraidError::SubscriptionStatus {
                status: response.status().as_u16(),
                retry_after: response
                    .headers()
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .and_then(crate::client::parse_retry_after),
            });
        }
        let headers = crate::protocol::headers_from_http(response.headers())?;
        let heartbeat = headers
            .get("heartbeats")
            .map(|value| {
                crate::client::subscription::HeartbeatConfig::from_header(value)
                    .ok_or_else(|| BraidError::HeaderParse("Invalid Heartbeats response".into()))
            })
            .transpose()?;
        let merge_type = headers.get("merge-type").cloned();
        let repr_type = headers.get("repr-type").cloned();
        let (tx, rx) = async_channel::bounded(100);
        let mut stream = response.bytes_stream();
        tokio::spawn(async move {
            let mut parser = MessageParser::new();
            loop {
                let next = async {
                    if let Some(config) = &heartbeat {
                        tokio::time::timeout(config.timeout, stream.next())
                            .await
                            .map_err(|_| BraidError::Timeout)
                    } else {
                        Ok(stream.next().await)
                    }
                };
                let chunk = tokio::select! { _ = tx.closed() => return, result = next => result };
                match chunk {
                    Ok(Some(Ok(bytes))) => match parser.feed(&bytes) {
                        Ok(messages) => {
                            for msg in messages {
                                match crate::client::utils::message_to_update(msg) {
                                    Ok(mut update) => {
                                        if update.merge_type.is_none() {
                                            update.merge_type = merge_type.clone();
                                        }
                                        if let Some(repr) = &repr_type {
                                            update
                                                .extra_headers
                                                .entry("repr-type".into())
                                                .or_insert_with(|| repr.clone());
                                        }
                                        if tx.send(Ok(update)).await.is_err() {
                                            return;
                                        }
                                    }
                                    Err(e) => {
                                        let _ = tx.send(Err(e)).await;
                                        return;
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            let _ = tx.send(Err(e)).await;
                            return;
                        }
                    },
                    Ok(None) => {
                        if let Err(e) = parser.finish() {
                            let _ = tx.send(Err(e)).await;
                        }
                        return;
                    }
                    Ok(Some(Err(e))) => {
                        let _ = tx.send(Err(BraidError::Http(e.to_string()))).await;
                        return;
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                }
            }
        });
        Ok(rx)
    }
}
