# Braid Core Module (`core`)

The `core` module contains the fundamental building blocks of the Braid protocol implementation. It defines the core abstractions, server-side state machines, and conflict resolution algorithms used throughout the workspace.

## Module Structure

### Abstractions (`traits.rs`)
Defines the `BraidRuntime`, `BraidNetwork`, and `BraidStorage` traits. These abstractions decouple the protocol logic from the underlying execution environment (e.g., allowing the same logic to run in a browser via WASM or on a server via Tokio).

### Server Implementation (`server/`)
Provides a complete Braid-HTTP server implementation, designed to be used as a middleware layer in `axum` or other `tower`-compatible frameworks.
- **`BraidLayer`**: A Tower middleware that intercepts requests and handles Braid-specific headers.
- **`BraidState`**: Manages the version graphs and active subscriptions for all resources on the server.
- **`ConflictResolver`**: A pluggable component that determines how concurrent updates are merged.
- **`ResourceState`**: Encapsulates the history and current state of a single Braid resource.

### Conflict Resolution (`merge/`)
Implements the algorithms for reconciling concurrent updates:
- **`diamond.rs`**: Integration with Diamond Types, providing powerful operational transformation (OT) and CRDT capabilities for high-concurrency collaborative environments.
- **`simpleton.rs`**: A simpler merge strategy for use cases where full OT isn't required (e.g., immutable blobs or basic state sync).

### Primitives and Errors (`error.rs`, `mod.rs`)
Defines the internal error types (`BraidError`) and provides top-level re-exports for the most commonly used types in the `core` module.

## Integration Flow

1. **Request In**: A `BraidRequest` arrives at the `BraidLayer`.
2. **Context**: The `BraidState` identifies the target resource and its current version graph.
3. **Merge**: If the request is a `PUT`, the `ConflictResolver` uses the logic in `merge/` to integrate the new version.
4. **Broadcast**: Subscribed clients are notified of the new version via their respective transport channels.
5. **Response Out**: The server returns a `BraidResponse` with updated version and parent headers.
