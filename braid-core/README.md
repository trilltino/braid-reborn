# Braid-Core

The unified Braid Protocol implementation in Rust. This crate consolidates the core state synchronization engine, conflict resolution, blob storage, and filesystem synchronization into a single high-performance library.

## Architecture

Braid-Core acts as the central standard library for the Braid ecosystem. It re-exports and unifies several subsystems under a single API surface:

```
braid-core
  +-- core/         Braid-HTTP protocol (types, parser, client, server middleware)
  +-- vendor/       Vendored third-party code (Diamond Types CRDT)
  +-- fs/           Filesystem synchronization client logic
  +-- blob           Re-export of braid-blob (binary storage)
```

### Core Protocol

The `core` module provides the fundamental Braid-HTTP types and middleware:

| Type | Description |
|---|---|
| `Update` | A versioned state snapshot or patch with body, version, and parents |
| `Version` | A version identifier (string or structured) for tracking document history |
| `Patch` | A content delta applied to a resource |
| `BraidRequest` / `BraidResponse` | HTTP request/response wrappers with Braid headers |
| `BraidClient` | Async HTTP client for subscribing to and updating Braid resources |
| `BraidLayer` | Tower middleware that injects Braid protocol handling into Axum servers |
| `BraidState` | Server-side state container for managing subscriptions and version graphs |

### Conflict Resolution

Formerly powered by the Antimatter CRDT, conflict resolution now uses Diamond Types for operational transformation. The `merge` module provides deterministic merging of concurrent edits across decentralized nodes.

### Filesystem Synchronization

The `fs` module implements BraidFS -- synchronization of physical file boundaries across nodes. It watches local directories for changes (via `notify`), computes diffs (via `dissimilar`), and emits Braid patches to keep remote peers consistent.

## Feature Flags

| Feature | Description | Key Dependencies |
|---|---|---|
| `server` | Axum-based server middleware (BraidLayer, BraidState) | axum, tower, tower-http |
| `client` | HTTP client for subscribing to Braid resources | reqwest |
| `blob` | Binary storage via braid-blob | braid-blob, rusqlite |
| `fs` | Filesystem watching and synchronization | notify, dissimilar |
| `nfs` | NFS server bridge for mounting Braid resources | nfsserve |
| `native` | Full native runtime (Tokio, filesystem, SQLite) | tokio, rusqlite, notify |
| `lz4` | LZ4 compression for storage and transfer | lz4_flex |

All features are enabled by default. For embedded or WASM targets, disable `native` and select only the features you need.

## Usage

Add `braid-core` to your dependencies with the features you need:

```toml
[dependencies]
braid-core = { path = "../braid-core", features = ["server", "client"] }
```

Server example:

```rust
use braid_core::{BraidLayer, BraidState, ServerConfig};

let state = BraidState::new(ServerConfig::default());
let app = axum::Router::new()
    .layer(BraidLayer::new(state));
```

Client example:

```rust
use braid_core::{BraidClient, ClientConfig, Update, Version};

let client = BraidClient::new(ClientConfig::default());
let subscription = client.subscribe("https://example.com/resource").await?;
```

## Testing

The crate includes rigorous integration and unit tests:

```bash
cargo test -p braid-core
```

Key test suites:
- `tests/sync_integrity_tests.rs`: Atomic file writing guarantees, race condition prevention
- Checksum generation and validation for corrupted disk data
- Braid-HTTP header parsing and formatting

## Technology Stack

- **Language**: Rust
- **Async Runtime**: Tokio
- **CRDT**: Diamond Types (operational transformation)
- **HTTP**: reqwest (client), Axum + Tower (server)
- **Storage**: rusqlite (metadata), native filesystem (blobs)
- **Serialization**: Serde, JSON, smallvec

## License

MIT OR Apache-2.0
