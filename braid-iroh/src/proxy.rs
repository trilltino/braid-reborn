//! Optional TCP proxy bridge for legacy HTTP clients.
//!
//! Lets a browser or curl talk to the P2P Braid network through a local
//! TCP listener. Requests to `http://localhost:<port>/resource` get
//! forwarded over iroh to the target peer.
//!
//! Requires the `proxy` feature flag.

#[cfg(feature = "proxy")]
pub mod bridge {
    use axum::{
        body::Body,
        extract::{Request, State},
        response::Response,
        routing::any,
        Router,
    };
    use iroh::{Endpoint, EndpointId};
    use iroh_h3_client::IrohH3Client;
    use std::net::SocketAddr;
    use tower_http::trace::TraceLayer;

    /// State shared across proxy request handlers.
    #[derive(Clone)]
    pub struct ProxyState {
        client: IrohH3Client,
        default_peer: EndpointId,
    }

    impl ProxyState {
        /// Create a new proxy state with the given client and default peer.
        pub fn new(client: IrohH3Client, default_peer: EndpointId) -> Self {
            Self {
                client,
                default_peer,
            }
        }
    }

    /// Start a local TCP proxy that forwards HTTP/1.1 requests to a target Braid peer
    /// over HTTP/3.
    ///
    /// # Arguments
    /// * `endpoint` - The iroh endpoint to use for P2P connections
    /// * `listen_addr` - Local socket address to bind the HTTP/1.1 listener
    /// * `default_peer` - The target peer ID to forward requests to
    ///
    /// # Returns
    /// Returns `Ok(())` when the server shuts down gracefully, or an error if binding fails.
    pub async fn start_proxy(
        endpoint: &Endpoint,
        listen_addr: SocketAddr,
        default_peer: EndpointId,
    ) -> anyhow::Result<()> {
        let alpn = super::super::node::BRAID_H3_ALPN.to_vec();
        let client = IrohH3Client::new(endpoint.clone(), alpn);

        let state = ProxyState::new(client, default_peer);

        let app = Router::new()
            .route("/{*path}", any(proxy_handler))
            .with_state(state)
            .layer(TraceLayer::new_for_http());

        tracing::info!("Starting TCP Proxy Bridge on http://{}", listen_addr);

        let listener = tokio::net::TcpListener::bind(listen_addr).await?;
        axum::serve(listener, app).await?;

        Ok(())
    }

    /// Main proxy handler that forwards HTTP/1.1 requests to the P2P network via HTTP/3.
    ///
    /// This handler:
    /// 1. Reconstructs the target URL from the default peer and request path
    /// 2. Converts the incoming axum body to bytes for forwarding
    /// 3. Sends the request via IrohH3Client
    /// 4. Streams the response back to the HTTP/1.1 client
    async fn proxy_handler(
        State(state): State<ProxyState>,
        req: Request,
    ) -> Result<Response, String> {
        let path = req.uri().path();
        let query = req
            .uri()
            .query()
            .map(|q| format!("?{}", q))
            .unwrap_or_default();

        // Construct target URL: https://<peer_id>/<path>?<query>
        // The peer_id is used as the authority/host in the URL
        let target_url = format!("https://{}{}{}", state.default_peer, path, query);

        tracing::info!(
            method = %req.method(),
            path = %path,
            target = %target_url,
            "Proxying request"
        );

        // Convert incoming axum body to bytes
        // Note: We collect the entire body for simplicity. For large uploads,
        // streaming would be more efficient but requires more complex handling.
        let (parts, body) = req.into_parts();
        let body_bytes = axum::body::to_bytes(body, 10 * 1024 * 1024) // 10MB limit
            .await
            .map_err(|e| format!("Failed to read request body: {}", e))?;

        // Build the request using IrohH3Client
        let mut builder = state
            .client
            .request(parts.method.clone(), &target_url);

        // Copy headers from the original request, skipping hop-by-hop headers
        for (key, value) in &parts.headers {
            let key_str = key.as_str().to_lowercase();
            // Skip hop-by-hop headers that shouldn't be forwarded
            if matches!(
                key_str.as_str(),
                "host"
                    | "connection"
                    | "keep-alive"
                    | "proxy-authenticate"
                    | "proxy-authorization"
                    | "te"
                    | "trailers"
                    | "transfer-encoding"
                    | "upgrade"
            ) {
                continue;
            }
            builder = builder.header(key, value);
        }

        // Set the body and build the request
        // Use the .bytes() method on RequestBuilder to set body with octet-stream content-type
        let client_req = if body_bytes.is_empty() {
            builder.build().map_err(|e| format!("Failed to build request: {}", e))?
        } else {
            builder.bytes(body_bytes).map_err(|e| format!("Failed to build request: {}", e))?
        };

        let resp = client_req
            .send()
            .await
            .map_err(|e| format!("Request failed: {}", e))?;

        // Extract response parts
        let status = resp.status;
        let headers = resp.headers.clone();

        // Collect response body
        let response_bytes = resp
            .bytes()
            .await
            .map_err(|e| format!("Failed to read response body: {}", e))?;

        tracing::debug!(
            status = %status,
            body_len = response_bytes.len(),
            "Received response from P2P network"
        );

        // Build the response
        let mut response_builder = Response::builder().status(status);

        // Copy response headers
        let response_headers = response_builder
            .headers_mut()
            .ok_or_else(|| "Failed to get response headers mut".to_string())?;

        for (key, value) in headers.iter() {
            let key_str = key.as_str().to_lowercase();
            // Skip hop-by-hop headers in response
            if matches!(
                key_str.as_str(),
                "connection"
                    | "keep-alive"
                    | "transfer-encoding"
                    | "upgrade"
            ) {
                continue;
            }
            response_headers.insert(key, value.clone());
        }

        response_builder
            .body(Body::from(response_bytes))
            .map_err(|e| format!("Failed to build response: {}", e))
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_proxy_state_new() {
            // This is a compile-time test to verify the API
            fn assert_proxy_state<T: Clone + Send + Sync + 'static>() {}
            assert_proxy_state::<ProxyState>();
        }
    }
}

#[cfg(not(feature = "proxy"))]
pub mod bridge {
    //! Stub implementation when proxy feature is disabled.
    use iroh::{Endpoint, EndpointId};
    use std::net::SocketAddr;

    /// Stub function that returns an error when proxy feature is not enabled.
    pub async fn start_proxy(
        _endpoint: &Endpoint,
        _listen_addr: SocketAddr,
        _default_peer: EndpointId,
    ) -> anyhow::Result<()> {
        Err(anyhow::anyhow!(
            "Proxy feature is not enabled. Rebuild with --features proxy"
        ))
    }
}
