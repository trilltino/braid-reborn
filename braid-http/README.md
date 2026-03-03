# Braid-HTTP

A pure Rust implementation of the Braid-HTTP protocol, enabling direct, serverless state synchronization over standard HTTP connections. This crate provides the types, parsers, and client logic for building Braid-compliant applications.

## What is Braid-HTTP?

Braid-HTTP extends the HTTP protocol with headers and semantics for real-time state synchronization:

- **Versioning**: Every resource state has a version identifier, enabling precise history tracking and conflict detection.
- **Subscriptions**: Clients subscribe to resources with `Subscribe: true`, receiving a persistent stream of updates as the state evolves.
- **Patches**: Updates are transmitted as differential patches rather than full state, minimizing bandwidth and enabling fine-grained conflict resolution.
- **Parents**: Each version references its parent versions, forming a directed acyclic graph (DAG) of document history.

## Key Types

| Type | Description |
|---|---|
| `BraidClient` | Async HTTP client for subscribing to and updating resources |
| `BraidRequest` | Outbound request builder with Braid headers (Version, Parents, Subscribe) |
| `BraidResponse` | Parsed response with extracted Braid metadata |
| `Update` | A versioned state change containing body bytes, version, and parent references |
| `Version` | Version identifier -- supports string and structured forms |
| `Patch` | A content delta (body, content-range, content-type) |
| `BraidError` | Error types for protocol violations, network failures, and parsing errors |

## Module Structure

```
braid-http
  +-- client.rs      BraidClient -- async HTTP client with subscription support
  +-- types/         Core protocol types (Update, Version, Patch, Request, Response)
  +-- protocol.rs    Header parsing and formatting (SFV, Version, Parents)
  +-- traits.rs      Trait definitions for extensible Braid implementations
  +-- error.rs       Error types and Result alias
```

## Feature Flags

| Feature | Description |
|---|---|
| `native` (default) | Full native runtime with Tokio and reqwest |
| `wasm` | WebAssembly support via gloo-timers, wasm-bindgen-futures, js-sys |
| `fuzzing` | Enables fuzzing-friendly configurations |

The `wasm` feature replaces Tokio-based timers and futures with browser-compatible equivalents, allowing this crate to run in WebAssembly environments.

## Usage

Add `braid-http` to your dependencies:

```toml
[dependencies]
braid-http = { path = "../braid-http" }
```

```rust
use braid_http::{BraidClient, BraidRequest, BraidResponse};
use braid_http::types::{Update, Version, Patch};

// Parse Braid headers from an HTTP response
let version = Version::String("v1".into());
let update = Update::snapshot(version, bytes::Bytes::from("hello"));

// Access version history
for v in &update.version {
    println!("Version: {}", v);
}
```

## Protocol Compliance

This crate implements the Braid-HTTP specification with support for:

- Structured Field Values (SFV) parsing for version and parent headers
- HTTP 209 (Subscription) response code handling
- Content-Range based partial updates
- Long-lived streaming responses for subscriptions

## Technology Stack

- **Language**: Rust
- **HTTP Client**: reqwest (with JSON, streaming, and multipart support)
- **Header Parsing**: SFV (Structured Field Values), regex
- **Serialization**: Serde, JSON
- **Async**: Tokio (native), gloo-timers (WASM)

## Testing

```bash
cargo test -p braid-http
```

## License

MIT OR Apache-2.0
