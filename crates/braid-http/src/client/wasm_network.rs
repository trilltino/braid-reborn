//! Browser Fetch transport using the same byte codec as the native client.
use crate::{
    client::{HeartbeatConfig, MessageParser},
    error::{BraidError, Result},
    traits::BraidNetwork,
    types::{BraidRequest, BraidResponse, Update},
};
use async_trait::async_trait;
use futures::{FutureExt, StreamExt};
pub struct WasmNetwork;
fn request(url: &str, req: &BraidRequest) -> Result<reqwest::RequestBuilder> {
    let method = reqwest::Method::from_bytes(req.method.as_bytes())
        .map_err(|e| BraidError::Protocol(e.to_string()))?;
    let (headers, body) = crate::protocol::encode_request(req)?;
    let mut builder = reqwest::Client::new().request(method, url);
    for (key, value) in headers {
        builder = builder.header(key, value);
    }
    Ok(builder.body(body))
}
#[async_trait(?Send)]
impl BraidNetwork for WasmNetwork {
    async fn fetch(&self, url: &str, req: BraidRequest) -> Result<BraidResponse> {
        let response = request(url, &req)?
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
    async fn subscribe(
        &self,
        url: &str,
        mut req: BraidRequest,
    ) -> Result<async_channel::Receiver<Result<Update>>> {
        req.subscribe = true;
        req.method = "GET".into();
        let response = request(url, &req)?
            .send()
            .await
            .map_err(|e| BraidError::Http(e.to_string()))?;
        if response.status().as_u16() != 209 {
            return Err(BraidError::InvalidSubscriptionStatus(
                response.status().as_u16(),
            ));
        }
        let heartbeat = response
            .headers()
            .get("heartbeats")
            .and_then(|v| v.to_str().ok())
            .and_then(HeartbeatConfig::from_header);
        let merge_type = response
            .headers()
            .get("merge-type")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let (tx, rx) = async_channel::bounded(100);
        wasm_bindgen_futures::spawn_local(async move {
            let mut stream = response.bytes_stream();
            let mut parser = MessageParser::new();
            loop {
                let next = async {
                    if let Some(config) = &heartbeat {
                        let timer = gloo_timers::future::TimeoutFuture::new(
                            config.timeout.as_millis() as u32,
                        )
                        .fuse();
                        let read = stream.next().fuse();
                        futures::pin_mut!(timer, read);
                        futures::select! {_=timer=>Err(BraidError::Timeout),chunk=read=>Ok(chunk)}
                    } else {
                        Ok(stream.next().await)
                    }
                }
                .fuse();
                let closed = tx.closed().fuse();
                futures::pin_mut!(next, closed);
                let chunk = futures::select! {_=closed=>return,result=next=>result};
                match chunk {
                    Ok(Some(Ok(bytes))) => match parser.feed(&bytes) {
                        Ok(messages) => {
                            for message in messages {
                                match crate::client::message_to_update(message) {
                                    Ok(mut update) => {
                                        if update.merge_type.is_none() {
                                            update.merge_type = merge_type.clone();
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
