//! Axum middleware for Braid protocol support.

use super::resource_state::ResourceStateManager;
use crate::core::protocol_mod as protocol;
use crate::core::protocol_mod::constants::headers;
use crate::core::Version;
use axum::{extract::Request, middleware::Next, response::Response};
use futures::StreamExt;
use std::collections::BTreeMap;
use std::sync::Arc;

/// Newtype wrapper indicating Firefox browser detection.
#[derive(Clone, Copy, Debug)]
pub struct IsFirefox(pub bool);

async fn braid_middleware_handler(
    axum::extract::State(state): axum::extract::State<BraidLayer>,
    req: Request,
    next: Next,
) -> Response {
    state.handle_middleware(req, next).await
}

/// Braid protocol state extracted from HTTP request headers.
#[derive(Clone, Debug)]
pub struct BraidState {
    pub subscribe: bool,
    pub version: Option<Vec<Version>>,
    pub parents: Option<Vec<Version>>,
    pub peer: Option<String>,
    pub heartbeat: Option<u64>,
    pub merge_type: Option<String>,
    pub content_range: Option<String>,
    pub multiplex_through: Option<String>,
    pub ack: Option<Vec<crate::core::Version>>,

    pub headers: BTreeMap<String, String>,
}

impl BraidState {
    #[must_use]
    pub fn from_headers(headers: &axum::http::HeaderMap) -> Self {
        let mut braid_state = BraidState {
            subscribe: false,
            version: None,
            parents: None,
            peer: None,
            heartbeat: None,
            merge_type: None,
            content_range: None,
            multiplex_through: None,
            ack: None,

            headers: BTreeMap::new(),
        };

        for (name, value) in headers.iter() {
            if let Ok(value_str) = value.to_str() {
                let name_lower = name.to_string().to_lowercase();
                braid_state
                    .headers
                    .insert(name_lower.clone(), value_str.to_string());

                if name_lower == headers::SUBSCRIBE.as_str() {
                    braid_state.subscribe = value_str.to_lowercase() == "true";
                } else if name_lower == headers::VERSION.as_str() {
                    braid_state.version = protocol::parse_version_header(value_str).ok();
                } else if name_lower == headers::PARENTS.as_str() {
                    braid_state.parents = protocol::parse_version_header(value_str).ok();
                } else if name_lower == headers::PEER.as_str() {
                    braid_state.peer = Some(value_str.to_string());
                } else if name_lower == headers::HEARTBEATS.as_str() {
                    braid_state.heartbeat = protocol::parse_heartbeat(value_str).ok();
                } else if name_lower == headers::MERGE_TYPE.as_str() {
                    braid_state.merge_type = Some(value_str.to_string());
                } else if name_lower == headers::CONTENT_RANGE.as_str() {
                    braid_state.content_range = Some(value_str.to_string());
                } else if name_lower == protocol::constants::headers::MULTIPLEX_THROUGH.as_str() {
                    braid_state.multiplex_through = Some(value_str.to_string());
                }
            }
        }
        braid_state
    }
}

/// Axum middleware layer for Braid protocol support.
#[derive(Clone)]
pub struct BraidLayer {
    config: super::config::ServerConfig,
    pub resource_manager: Arc<ResourceStateManager>,
    pub multiplexer_registry: Arc<super::multiplex::MultiplexerRegistry>,
}

impl BraidLayer {
    #[must_use]
    pub fn new() -> Self {
        Self {
            config: super::config::ServerConfig::default(),
            resource_manager: Arc::new(ResourceStateManager::new()),
            multiplexer_registry: Arc::new(super::multiplex::MultiplexerRegistry::new()),
        }
    }

    #[must_use]
    pub fn with_config(config: super::config::ServerConfig) -> Self {
        Self {
            config,
            resource_manager: Arc::new(ResourceStateManager::new()),
            multiplexer_registry: Arc::new(super::multiplex::MultiplexerRegistry::new()),
        }
    }

    #[inline]
    #[must_use]
    pub fn config(&self) -> &super::config::ServerConfig {
        &self.config
    }

    #[must_use]
    pub fn middleware(
        &self,
    ) -> impl tower::Layer<
        axum::routing::Route,
        Service = impl tower::Service<
            Request,
            Response = Response,
            Error = std::convert::Infallible,
            Future = impl Send + 'static,
        > + Clone
                      + Send
                      + Sync
                      + 'static,
    > + Clone {
        axum::middleware::from_fn_with_state(self.clone(), braid_middleware_handler)
    }

    async fn handle_middleware(&self, mut req: Request, next: Next) -> Response {
        let resource_manager = self.resource_manager.clone();
        let multiplexer_registry = self.multiplexer_registry.clone();

        if req.method().as_str() == "MULTIPLEX" {
            let version = req
                .headers()
                .get(crate::core::protocol_mod::constants::headers::MULTIPLEX_VERSION)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("1.0");
            if version == "1.0" {
                let (tx, mut rx) = tokio::sync::mpsc::channel(1024);
                let id = format!("{:x}", rand::random::<u64>());
                multiplexer_registry.add(id.clone(), tx).await;
                let stream = async_stream::stream! {
                    while let Some(data) = rx.recv().await { yield Ok::<_, std::io::Error>(axum::body::Bytes::from(data)); }
                };
                let body = axum::body::Body::from_stream(stream);
                return Response::builder()
                    .status(200)
                    .header(
                        crate::core::protocol_mod::constants::headers::MULTIPLEX_VERSION,
                        "1.0",
                    )
                    .body(body)
                    .unwrap();
            }
        }

        let braid_state = Arc::new(BraidState::from_headers(req.headers()));
        let multiplex_through = braid_state.multiplex_through.clone();
        let m_registry = multiplexer_registry.clone();

        let is_firefox = req
            .headers()
            .get("user-agent")
            .and_then(|v| v.to_str().ok())
            .map(|ua| ua.to_lowercase().contains("firefox"))
            .unwrap_or(false);

        req.extensions_mut().insert(braid_state.clone());
        req.extensions_mut().insert(resource_manager.clone());
        req.extensions_mut().insert(multiplexer_registry);
        req.extensions_mut().insert(IsFirefox(is_firefox));

        let mut response = next.run(req).await;
        let headers = response.headers_mut();
        headers.insert(
            axum::http::header::HeaderName::from_static("range-request-allow-methods"),
            axum::http::header::HeaderValue::from_static("PATCH, PUT"),
        );
        headers.insert(
            axum::http::header::HeaderName::from_static("range-request-allow-units"),
            axum::http::header::HeaderValue::from_static("json"),
        );

        if let Some(through) = multiplex_through {
            let parts: Vec<&str> = through.split('/').collect();
            if parts.len() >= 5 && parts[1] == ".well-known" && parts[2] == "multiplexer" {
                let m_id = parts[3];
                let r_id = parts[4];

                if let Some(conn) = m_registry.get(m_id).await {
                    let sender = conn.sender.clone();
                    let r_id = r_id.to_string();
                    let mut cors_headers = axum::http::HeaderMap::new();
                    for (k, v) in response.headers() {
                        if k.as_str().starts_with("access-control-") {
                            cors_headers.insert(k.clone(), v.clone());
                        }
                    }

                    tokio::spawn(async move {
                        let mut header_block =
                            format!(":status: {}\r\n", response.status().as_u16());
                        for (name, value) in response.headers() {
                            header_block.push_str(&format!(
                                "{}: {}\r\n",
                                name,
                                value.to_str().unwrap_or("")
                            ));
                        }
                        header_block.push_str("\r\n");
                        let _ = sender
                            .send(
                                crate::core::protocol_mod::multiplex::MultiplexEvent::Data(
                                    r_id.clone(),
                                    header_block.clone().into_bytes(),
                                )
                                .to_string()
                                .into_bytes(),
                            )
                            .await;
                        let _ = sender.send(header_block.into_bytes()).await;
                        let mut body_stream = response.into_body().into_data_stream();
                        while let Some(Ok(chunk)) = body_stream.next().await {
                            let _ = sender
                                .send(
                                    crate::core::protocol_mod::multiplex::MultiplexEvent::Data(
                                        r_id.clone(),
                                        chunk.to_vec(),
                                    )
                                    .to_string()
                                    .into_bytes(),
                                )
                                .await;
                            let _ = sender.send(chunk.to_vec()).await;
                        }
                        let _ = sender
                            .send(
                                crate::core::protocol_mod::multiplex::MultiplexEvent::CloseResponse(
                                    r_id,
                                )
                                .to_string()
                                .into_bytes(),
                            )
                            .await;
                    });

                    let mut builder = Response::builder().status(293).header(
                        crate::core::protocol_mod::constants::headers::MULTIPLEX_VERSION,
                        "1.0",
                    );
                    if let Some(headers) = builder.headers_mut() {
                        headers.extend(cors_headers);
                    }
                    return builder.body(axum::body::Body::empty()).unwrap();
                }
            }
        }
        response
    }
}

impl Default for BraidLayer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_parse_headers() {
        let result = protocol::parse_version_header("\"v1\", \"v2\"");
        assert_eq!(result.unwrap().len(), 2);
    }
}
