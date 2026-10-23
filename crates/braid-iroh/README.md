# Braid-Iroh

Braid-HTTP over Iroh P2P -- a pure Rust library that layers the Braid state synchronization protocol on top of Iroh's QUIC-based peer-to-peer transport. Every node is both a client and a server, enabling direct, serverless state synchronization between peers without any central infrastructure.

## Architecture

The crate is organized into five modules, each responsible for a distinct layer of the stack:

```
braid-iroh
  +-- node.rs           BraidIrohNode -- the primary peer entry point
  +-- protocol.rs       Axum routes served over HTTP/3 (GET, PUT with Braid headers)
  +-- subscription.rs   Gossip-backed pub/sub keyed by resource URL
  +-- discovery.rs      Pluggable peer discovery (mock for tests, real DNS/Pkarr for production)
  +-- proxy.rs          Optional HTTP/1.1 -> HTTP/3 TCP bridge for legacy clients
```

### Node (`node.rs`)

`BraidIrohNode` is the main entry point. Calling `BraidIrohNode::spawn(config)` sets up an Iroh `Endpoint`, starts the gossip protocol, mounts the Braid-HTTP Axum router via `IrohAxum`, and begins accepting incoming QUIC connections.

Key methods on `BraidIrohNode`:

| Method | Description |
|---|---|
| `spawn(config)` | Create and start a new peer with the given configuration |
| `subscribe(url, bootstrap)` | Subscribe to a resource URL on the gossip network |
| `put(url, update)` | Store an update locally and broadcast it to all subscribers |
| `get(url)` | Retrieve the latest state of a resource from local storage |
| `get_version(url, version_id)` | Retrieve a specific historical version of a resource |
| `get_history(url)` | List all version IDs for a resource (oldest to newest) |
| `join_peers(url, peers)` | Add additional peers to an existing gossip topic |
| `shutdown()` | Gracefully shut down the node and its endpoint |

### Protocol (`protocol.rs`)

Wraps an Axum router in `IrohAxum` so that standard Braid-HTTP semantics (versioned GET, PUT with `Version`/`Parents` headers) are served over HTTP/3 on Iroh QUIC connections. Routes:

- `GET /:resource` -- Returns the latest snapshot, a specific version (`?version=...`), or full history (`?history=true`)
- `GET /:resource` with `Subscribe: true` header -- Returns HTTP 209 with the current state, establishing a Braid subscription
- `PUT /:resource` -- Accepts a new versioned update, stores it locally, and broadcasts to gossip subscribers

### Subscription (`subscription.rs`)

`SubscriptionManager` maps resource URLs to iroh-gossip topics. Topic IDs are derived deterministically from the URL via blake3 hashing, so any peer that knows the URL can join the correct topic without out-of-band coordination.

- URL normalization ensures `/resource` and `resource/` resolve to the same topic
- Subscriptions return a `(GossipSender, GossipReceiver)` pair for bidirectional communication
- Broadcasting serializes `braid_http_rs::Update` to JSON and sends it over gossip

### Discovery (`discovery.rs`)

`DiscoveryConfig` supports two modes:

- **Mock**: An in-memory `MockDiscoveryMap` shared across endpoints on the same machine. Ideal for tests and demos where multiple peers run in the same process.
- **Real**: Uses Iroh's production discovery stack (DNS, Pkarr, mDNS) for cross-network peer resolution.

### Proxy (`proxy.rs`)

An optional feature-gated (`proxy`) TCP bridge that lets legacy HTTP/1.1 clients (browsers, curl) talk to the P2P Braid network through a local TCP listener. Requests to `http://localhost:<port>/resource` are transparently forwarded over HTTP/3 via `IrohH3Client` to the target peer.

## Technology Stack

- **Language**: Rust
- **P2P Transport**: Iroh (QUIC, NAT traversal, relay)
- **Gossip**: iroh-gossip for decentralized pub/sub
- **HTTP/3**: iroh-h3 (h3-axum for server routes, h3-client for outbound requests)
- **Web Framework**: Axum (routes served over both H3 and optional TCP)
- **Braid Protocol**: braid-core (Update, Version, Patch types and server/client middleware)
- **Hashing**: blake3 (deterministic topic derivation, key generation)

## Usage

Add `braid-iroh` to your dependencies:

```toml
[dependencies]
braid-iroh = { path = "../braid-iroh" }
```

Spawn a node and interact with the P2P network:

```rust
use braid_iroh::{spawn_node, DiscoveryConfig};
use braid_http_rs::Update;

// Spawn a peer with mock discovery (for local testing)
let discovery = DiscoveryConfig::mock();
let (state, rx) = spawn_node("alice", Some(8080), None, discovery).await?;

// Subscribe to a resource
let rx = state.peer.subscribe("/my-resource", vec![]).await?;

// PUT an update (stores locally + broadcasts to gossip)
let update = Update::snapshot(
    Version::String("v1".into()),
    bytes::Bytes::from("hello world"),
);
state.peer.put("/my-resource", update).await?;

// GET the latest state
if let Some(latest) = state.peer.get("/my-resource") {
    println!("Latest: {:?}", latest);
}
```

## Testing

The crate includes unit tests across all modules:

```bash
cargo test -p braid-iroh
```

Tests cover discovery map operations, topic derivation determinism, protocol handler compilation checks, and proxy state API surface validation.

## License

MIT OR Apache-2.0
