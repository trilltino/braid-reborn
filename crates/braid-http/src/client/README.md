# Braid-HTTP Client Module (`client`)

The `client` module provides the high-level API for interacting with Braid-HTTP servers. It is designed to be highly portable, supporting both native (Tokio/reqwest) and WebAssembly (browser fetch) runtimes.

## Key Sub-modules

### Network Implementations (`native_network.rs`, `wasm_network.rs`)
Implements the `BraidNetwork` trait for different backends.
- **Native**: Powered by `reqwest` and `tokio`, offering full support for pooling, proxies, and streaming.
- **WASM**: Designed for the browser, utilizing `js-sys` and `wasm-bindgen-futures` to bridge to the browser's native `fetch` API.

### Subscription Management (`subscription.rs`, `multiplex.rs`)
Handles long-lived Braid subscriptions. It manages the connection lifecycle, heartbeats, and ensures that incoming updates are correctly parsed and delivered to the application via async channels.

### Request/Response Engine (`fetch.rs`, `parser.rs`)
- **`fetch.rs`**: The low-level execution engine that constructs `BraidRequest` objects and dispatches them through the selected network backend.
- **`parser.rs`**: A robust parser for Braid-HTTP response bodies. It handles both full state snapshots and the Braid-specific patch format (including multipart/related and SFV-encoded patches).

### Reliability (`retry.rs`)
Implements customizable retry policies and exponential backoff, ensuring that Braid subscriptions remain resilient even in the face of intermittent network instability.

### Headers and Utilities (`headers.rs`, `utils.rs`)
Helpers for extracting and validating Braid-specific headers (Version, Parents, Merge-Type) from standard HTTP responses.

## Usage Pattern

Most users will interact with this module through the `BraidClient` re-exported at the crate root.

```rust
let client = BraidClient::new(Config::default());
let (update_stream, subscription_handle) = client.subscribe("/my-resource").await?;
```
