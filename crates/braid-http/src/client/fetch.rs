//! Main Braid HTTP client implementation.

use crate::client::config::ClientConfig;
#[cfg(not(target_arch = "wasm32"))]
use crate::client::native_network::NativeNetwork;
#[cfg(target_arch = "wasm32")]
use crate::client::wasm_network::WasmNetwork;
use crate::error::{BraidError, Result};
use crate::traits::BraidNetwork;
use crate::types::{BraidRequest, BraidResponse};
use std::sync::Arc;

/// The main Braid HTTP client
#[derive(Clone)]
pub struct BraidClient {
    #[cfg(not(target_arch = "wasm32"))]
    pub network: Arc<NativeNetwork>,
    #[cfg(target_arch = "wasm32")]
    pub network: Arc<WasmNetwork>,
    pub config: Arc<ClientConfig>,
    /// Active multiplexers by origin.
    #[cfg(not(target_arch = "wasm32"))]
    pub multiplexers: Arc<
        tokio::sync::Mutex<
            std::collections::HashMap<String, Arc<crate::client::multiplex::Multiplexer>>,
        >,
    >,
}

impl BraidClient {
    #[cfg(not(target_arch = "wasm32"))]
    pub fn network(&self) -> &Arc<NativeNetwork> {
        &self.network
    }

    #[cfg(target_arch = "wasm32")]
    pub fn network(&self) -> &Arc<WasmNetwork> {
        &self.network
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn client(&self) -> &reqwest::Client {
        self.network.client()
    }

    pub fn new() -> Result<Self> {
        Self::with_config(ClientConfig::default())
    }

    pub fn with_config(config: ClientConfig) -> Result<Self> {
        #[cfg(not(target_arch = "wasm32"))]
        {
            let mut builder = reqwest::Client::builder()
                // A redirect must never change a queued PUT into a successful
                // GET acknowledgment or forward a scoped credential elsewhere.
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(std::time::Duration::from_secs(
                    config.connection_timeout_secs,
                ))
                .pool_idle_timeout(std::time::Duration::from_secs(90))
                .pool_max_idle_per_host(config.max_total_connections as usize);

            if !config.proxy_url.is_empty() {
                builder = builder.proxy(
                    reqwest::Proxy::all(&config.proxy_url)
                        .map_err(|_| BraidError::Config("Invalid proxy configuration".into()))?,
                );
            }

            let client = builder
                .user_agent("braid-http/0.1")
                .build()
                .map_err(|e| BraidError::Config(e.to_string()))?;
            let subscription_client = client.clone();
            let network = Arc::new(
                NativeNetwork::with_subscription_client(client, subscription_client)
                    .with_request_timeout(std::time::Duration::from_millis(
                        config.request_timeout_ms,
                    )),
            );

            Ok(BraidClient {
                network,
                config: Arc::new(config),
                multiplexers: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
            })
        }

        #[cfg(target_arch = "wasm32")]
        {
            let network = Arc::new(WasmNetwork);
            Ok(BraidClient {
                network,
                config: Arc::new(config),
            })
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    /// Preserve custom TLS/proxy settings while enforcing explicit redirects.
    pub fn with_client_builder(builder: reqwest::ClientBuilder) -> Result<Self> {
        let client = builder
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| BraidError::Config("Unable to build HTTP transport".into()))?;
        Self::with_client(client)
    }

    #[cfg(not(target_arch = "wasm32"))]
    /// Compatibility boundary for a caller-owned transport. Its builder's
    /// redirect/default-header policies remain the caller's responsibility.
    /// Use with_client_builder for the library's redirect enforcement.
    pub fn with_client(client: reqwest::Client) -> Result<Self> {
        Ok(BraidClient {
            network: Arc::new(NativeNetwork::new(client)),
            config: Arc::new(ClientConfig::default()),
            multiplexers: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
        })
    }

    pub async fn get(&self, url: &str) -> Result<BraidResponse> {
        self.fetch(url, BraidRequest::new()).await
    }

    pub async fn put(
        &self,
        url: &str,
        body: &str,
        mut request: BraidRequest,
    ) -> Result<BraidResponse> {
        request = request.with_method("PUT").with_body(body.to_string());

        if request.content_type.is_none() {
            request = request.with_content_type("application/json");
        }

        if request.version.is_none() {
            let random_version = uuid::Uuid::new_v4().to_string();
            request.version = Some(vec![crate::types::Version::new(&random_version)]);
        }

        self.fetch(url, request).await
    }

    pub async fn post(
        &self,
        url: &str,
        body: &str,
        mut request: BraidRequest,
    ) -> Result<BraidResponse> {
        request = request.with_method("POST").with_body(body.to_string());
        self.fetch(url, request).await
    }

    pub async fn poke(&self, recipient_endpoint: &str, post_url: &str) -> Result<BraidResponse> {
        let request = BraidRequest::new()
            .with_method("POST")
            .with_body(post_url.to_string())
            .with_content_type("text/plain");

        self.fetch(recipient_endpoint, request).await
    }

    pub async fn fetch(&self, url: &str, request: BraidRequest) -> Result<BraidResponse> {
        self.fetch_with_retries(url, request).await
    }

    pub async fn subscribe(
        &self,
        url: &str,
        request: BraidRequest,
    ) -> Result<crate::client::Subscription> {
        self.log_request(url, &request);
        let rx = self.network.subscribe(url, request).await?;
        Ok(crate::client::Subscription::new(rx))
    }

    async fn fetch_with_retries(&self, url: &str, request: BraidRequest) -> Result<BraidResponse> {
        let retry_config = request.retry.clone().unwrap_or_else(|| {
            if self.config.max_retries == 0 {
                crate::client::retry::RetryConfig::no_retry()
            } else {
                crate::client::retry::RetryConfig::default()
                    .with_max_retries(self.config.max_retries)
                    .with_initial_backoff(std::time::Duration::from_millis(
                        self.config.retry_delay_ms,
                    ))
            }
        });

        let mut retry_state = crate::client::retry::RetryState::new(retry_config);

        loop {
            self.log_request(url, &request);

            match self.fetch_internal(url, &request).await {
                Ok(response) => {
                    self.log_response(url, &response);

                    let status = response.status;
                    if (400..600).contains(&status) {
                        let retry_after = response
                            .headers
                            .get("retry-after")
                            .and_then(|v| crate::client::retry::parse_retry_after(v));

                        match retry_state.should_retry_status(status, retry_after) {
                            crate::client::retry::RetryDecision::Retry(delay) => {
                                if self.config.enable_logging {
                                    tracing::warn!(
                                        "Request status {} (attempt {}), retrying in {:?}",
                                        status,
                                        retry_state.attempts,
                                        delay
                                    );
                                }
                                crate::client::utils::sleep(delay).await;
                                continue;
                            }
                            crate::client::retry::RetryDecision::DontRetry => {
                                return Ok(response);
                            }
                        }
                    }
                    retry_state.reset();
                    return Ok(response);
                }
                Err(e) => {
                    let is_abort = !matches!(&e, BraidError::Http(_) | BraidError::Timeout);

                    match retry_state.should_retry_error(is_abort) {
                        crate::client::retry::RetryDecision::Retry(delay) => {
                            if self.config.enable_logging {
                                tracing::warn!(
                                    "Request failed (attempt {}), retrying in {:?}: {}",
                                    retry_state.attempts,
                                    delay,
                                    e
                                );
                            }
                            crate::client::utils::sleep(delay).await;
                            continue;
                        }
                        crate::client::retry::RetryDecision::DontRetry => {
                            return Err(e);
                        }
                    }
                }
            }
        }
    }

    async fn fetch_internal(&self, url: &str, request: &BraidRequest) -> Result<BraidResponse> {
        self.network.fetch(url, request.clone()).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub async fn fetch_multiplexed(
        &self,
        url: &str,
        mut request: BraidRequest,
    ) -> Result<BraidResponse> {
        let origin = self.origin_from_url(url)?;

        let mut multiplexers = self.multiplexers.lock().await;
        let multiplexer = if let Some(m) = multiplexers.get(&origin) {
            m.clone()
        } else {
            let multiplex_url = format!("{}/.multiplex", origin);
            let m_id = format!("{:x}", rand::random::<u64>());
            let m = Arc::new(crate::client::multiplex::Multiplexer::new(
                origin.clone(),
                m_id,
            ));

            let client = self.clone();
            let m_inner = m.clone();
            let origin_task = origin.clone();
            crate::client::utils::spawn_task(async move {
                let run_multiplex = async {
                    let multiplex_method =
                        reqwest::Method::from_bytes(b"MULTIPLEX").map_err(|e| {
                            BraidError::Protocol(format!("Invalid multiplex method: {}", e))
                        })?;
                    let multiplex_header_name = reqwest::header::HeaderName::from_bytes(
                        crate::protocol::constants::headers::MULTIPLEX_VERSION
                            .as_str()
                            .as_bytes(),
                    )
                    .map_err(|e| {
                        BraidError::Protocol(format!("Invalid multiplex header: {}", e))
                    })?;

                    let resp = client
                        .network
                        .client()
                        .request(multiplex_method, &multiplex_url)
                        .header(multiplex_header_name, "1.0")
                        .send()
                        .await
                        .map_err(|e| {
                            BraidError::Http(format!(
                                "Failed to establish multiplexed connection to {}: {}",
                                multiplex_url, e
                            ))
                        })?;

                    m_inner.run_stream(resp).await
                };

                if let Err(e) = run_multiplex.await {
                    tracing::error!("Multiplexer task failed for {}: {}", origin_task, e);
                }
            });

            multiplexers.insert(origin.clone(), m.clone());
            m
        };
        drop(multiplexers);

        let r_id = format!("{:x}", rand::random::<u32>());
        let (tx, rx) = async_channel::bounded(100);
        multiplexer.add_request(r_id.clone(), tx).await;

        request.extra_headers.insert(
            crate::protocol::constants::headers::MULTIPLEX_THROUGH.to_string(),
            format!("/.well-known/multiplexer/{}/{}", multiplexer.id, r_id),
        );

        self.log_request(url, &request);
        let initial_response = self.fetch_internal(url, &request).await?;
        self.log_response(url, &initial_response);

        if initial_response.status == 293 {
            let mut response_buffer = Vec::new();
            let mut headers_parsed = None;

            while let Ok(chunk) = rx.recv().await {
                if response_buffer.len().saturating_add(chunk.len()) > 32 * 1024 * 1024 {
                    return Err(BraidError::Protocol(
                        "Multiplex response limit exceeded".into(),
                    ));
                }
                response_buffer.extend_from_slice(&chunk);

                if headers_parsed.is_none() {
                    if let Ok((status, headers, body_start)) =
                        crate::protocol::parse_tunneled_response(&response_buffer)
                    {
                        headers_parsed = Some((status, headers, body_start));
                    }
                }
            }

            if let Some((status, headers, body_start)) = headers_parsed {
                let body = bytes::Bytes::copy_from_slice(&response_buffer[body_start..]);
                if let Some(length) = headers.get("content-length") {
                    if length.parse::<usize>().ok() != Some(body.len()) {
                        return Err(BraidError::Protocol("Truncated multiplex response".into()));
                    }
                }
                return Ok(BraidResponse {
                    status,
                    headers,
                    body,
                    is_subscription: false,
                });
            } else {
                return Err(crate::error::BraidError::Protocol(
                    "Multiplexed response ended before headers received".to_string(),
                ));
            }
        }

        Ok(initial_response)
    }

    pub fn config(&self) -> &ClientConfig {
        &self.config
    }

    fn log_request(&self, _url: &str, _request: &BraidRequest) {}

    fn log_response(&self, _url: &str, _response: &BraidResponse) {}

    #[cfg(not(target_arch = "wasm32"))]
    fn origin_from_url(&self, url: &str) -> Result<String> {
        let parsed_url = url::Url::parse(url).map_err(|e| BraidError::Config(e.to_string()))?;
        if !matches!(parsed_url.scheme(), "http" | "https") || parsed_url.host_str().is_none() {
            return Err(BraidError::Config("Expected an HTTP origin".into()));
        }
        Ok(parsed_url.origin().ascii_serialization())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::BraidRequest;

    #[test]
    fn test_client_init() {
        let client = BraidClient::new().unwrap();
        assert_eq!(client.config().max_retries, 3);
    }

    #[test]
    fn test_origin_extraction() {
        let client = BraidClient::new().unwrap();
        assert_eq!(
            client.origin_from_url("http://example.com/foo").unwrap(),
            "http://example.com"
        );
    }

    #[test]
    fn test_put_request_prep() {
        let mut req = BraidRequest::new();
        req = req.with_method("PUT").with_body("test".to_string());
        if req.content_type.is_none() {
            req = req.with_content_type("application/json");
        }
        if req.version.is_none() {
            req.version = Some(vec![crate::types::Version::new("test-version")]);
        }
        assert_eq!(req.method, "PUT");
        assert_eq!(req.version.unwrap()[0].to_string(), "test-version");
    }

    #[test]
    fn test_poke_request_prep() {
        let req = BraidRequest::new()
            .with_method("POST")
            .with_body("http://example.com/post")
            .with_content_type("text/plain");
        assert_eq!(req.method, "POST");
        assert_eq!(
            String::from_utf8_lossy(&req.body),
            "http://example.com/post"
        );
    }
}
