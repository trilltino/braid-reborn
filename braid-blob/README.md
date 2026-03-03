# Braid-Blob

A high-performance binary storage engine and synchronization service for the Braid ecosystem. This crate extends the textual state synchronization capabilities of Braid-HTTP to manage large immutable and mutable blob assets with cryptographic integrity guarantees.

## How It Works

Braid-Blob provides a `BlobStore` abstraction that persists arbitrary byte streams and links them to Braid version hashes. The storage architecture uses a two-tier approach:

- **Metadata Layer** (SQLite via `rusqlite`): Tracks blob identifiers, version associations, checksums, and synchronization state in a relational store for fast lookups and atomic transactions.
- **Data Layer** (Native Filesystem): Binary chunk data is written directly to disk, avoiding the overhead of encoding large payloads into a database. Files are organized by content-addressable hash for deduplication.

Every blob is validated with a SHA-256 integrity check on write and on read. If a checksum mismatch is detected during retrieval, the store returns an integrity error rather than serving corrupt data.

## Integration with the Braid Ecosystem

This crate sits below `braid-core` in the dependency graph:

```
braid-core (re-exports braid-blob via the "blob" feature)
  +-- braid-blob
        +-- braid-http (shared types: Version, Update)
```

When `braid-core` is compiled with `features = ["blob"]`, the `BlobStore` is available at `braid_core::BlobStore` for direct use. The `server` and `braidfs-daemon` crates use this integration to persist file content alongside Braid version metadata.

## Key Types

| Type | Description |
|---|---|
| `BlobStore` | Primary API for reading, writing, and synchronizing binary content |
| `BlobMetadata` | Metadata record including SHA-256 checksum, size, and timestamps |
| `BlobChunk` | A segment of binary data associated with a parent blob |

## Applications

- Decentralized file sharing where blob data must be validated before consumption
- Avatar and media synchronization for chat platforms
- Version-controlled binary data transfer alongside semantic Braid state

## Technology Stack

- **Language**: Rust
- **Storage**: rusqlite (metadata), native filesystem (binary chunks)
- **Cryptography**: SHA-256 for integrity hashing
- **Serialization**: Serde, JSON
- **Async Runtime**: Tokio

## Testing

Implicit testing coverage through the `braid-core` integration tests, specifically verifying checksum verification, atomic writes, and data integrity over disk:

```bash
cargo test -p braid-core
```

## License

MIT OR Apache-2.0
