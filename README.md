# Braid Reborn

A modular Rust workspace implementing the Braid protocol for decentralized state synchronization. The workspace spans the full stack -- from low-level HTTP protocol types to P2P transport, filesystem synchronization, NFS bridging, and a centralized relay server.

## Workspace Crates

| Crate | Description |
|---|---|
| [braid-http](braid-http/) | Pure Rust implementation of the Braid-HTTP protocol (types, parser, client) with WASM support |
| [braid-core](braid-core/) | Unified Braid engine: CRDT conflict resolution, versioned state, blob storage, and filesystem sync |
| [braid-common](braid-common/) | Shared utilities, error types, directory management, and constants used across all crates |
| [braid-blob](braid-blob/) | Binary storage engine with SHA-256 integrity and SQLite metadata |
| [braid-iroh](braid-iroh/) | Braid-HTTP over Iroh P2P (QUIC transport, gossip subscriptions, HTTP/3 routes) |
| [braidfs-daemon](braidfs-daemon/) | Background daemon for real-time filesystem replication via Braid-HTTP |
| [braidfs-nfs](braidfs-nfs/) | NFS bridge that exposes Braid-synchronized state as network-attached storage |
| [server](server/) | Centralized relay server with auth, WebSocket support, AI integration, and state caching |

## Dependency Graph

```
braid-iroh -----> braid-core -----> braid-http
                     |                  ^
                     +-> braid-blob ----+
                     |
                     +-> braid-common
                     |
braidfs-daemon ------+
braidfs-nfs ---------+
server --------------+-----> braid-blob
                     +-----> braid-common
                     +-----> braid-http
```

## What is Braid?

Braid extends HTTP with headers and semantics for real-time state synchronization. Every resource has a version history forming a DAG, updates are transmitted as differential patches, and clients subscribe to receive continuous state updates. Combined with CRDT conflict resolution, Braid enables decentralized collaborative editing without a central server.

## Building

```bash
# Build the entire workspace
cargo build --workspace

# Run all tests
cargo test --workspace

# Build a specific crate
cargo build -p braid-iroh
```

## References

The [references/](references/) directory contains vendored forks of upstream Iroh libraries used by `braid-iroh`:

- **iroh-gossip**: Gossip protocol for decentralized pub/sub
- **iroh-h3**: HTTP/3 over Iroh QUIC (server, client, and Axum integration)

These are included as path dependencies and may contain local modifications.

## Technology Stack

- **Language**: Rust (2021 edition)
- **Async Runtime**: Tokio
- **P2P Transport**: Iroh (QUIC, NAT traversal, relay)
- **Web Framework**: Axum + Tower
- **CRDT**: Diamond Types
- **Storage**: SQLite (rusqlite/SQLx), native filesystem
- **Serialization**: Serde, JSON

## License

MIT OR Apache-2.0
