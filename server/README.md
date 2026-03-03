# Braid LocalLink Server

A centralized bridging server for the Braid ecosystem, providing data aggregation, authentication, persistent storage, and real-time state dissemination. Designed for hybrid P2P/Server topologies where direct peer connections may be restricted or where a trusted relay is needed.

## Architecture

```
Clients (Web, Desktop, Mobile)
    |
    v
Braid LocalLink Server
  +-- Auth Layer (bcrypt, SQLx/SQLite)
  +-- Braid-HTTP Middleware (BraidLayer, subscriptions, versioning)
  +-- WebSocket Gateway (real-time bidirectional updates)
  +-- AI Context Agent (genai integration)
  +-- Storage Layer
      +-- SQLite (user accounts, auth tokens, metadata)
      +-- JSON (Braid-compliant chat/document storage)
      +-- Blob Cache (via braid-blob)
```

## Features

### Authentication
Secure token-based access with bcrypt password hashing and SQLx-managed SQLite user accounts. Supports user registration, login, and session management.

### Braid-HTTP State Synchronization
Full Braid-HTTP middleware via `braid-core`'s `BraidLayer`. Resources are versioned, subscriptions are maintained, and patches are streamed to connected clients in real-time.

### WebSocket Gateway
Native WebSocket support via Axum for clients that require bidirectional messaging beyond standard HTTP streaming. Used for real-time UI updates and interactive sessions.

### AI Integration
Built-in AI context agent powered by `genai`. The server can route conversational queries through AI models, returning contextual responses alongside synchronized state.

### Storage and Caching
- **SQLite** (via SQLx): Persistent storage for user accounts, authentication tokens, and relational metadata.
- **JSON**: Braid-compliant document and chat storage with append-only version history.
- **Blob Cache**: Binary content cached locally via `braid-blob`, reducing load on peer nodes by serving recent versions to new subscribers.

### Daemon Integration
HTTP client (`reqwest`) integration with `braidfs-daemon` instances, allowing the server to relay filesystem synchronization events between daemons that cannot reach each other directly.

## Key Dependencies

| Dependency | Purpose |
|---|---|
| `axum` | Web routing, WebSockets, multipart uploads |
| `tower` / `tower-http` | Middleware stack (CORS, tracing) |
| `sqlx` | Async SQLite driver with migrations |
| `bcrypt` | Password hashing for authentication |
| `genai` | AI model integration for contextual agents |
| `braid-core` | Braid protocol engine (CRDT, versioning, patches) |
| `braid-http` | Braid-HTTP types and header parsing |
| `braid-blob` | Binary content storage and integrity verification |
| `braid-common` | Shared utilities and constants |

## Usage

Run the compiled binary with appropriate environment configuration:

```bash
# Start the server on port 3000
local_link_server --port 3000 --database sqlite://braid.db
```

The server exposes:
- REST endpoints for auth (register, login)
- Braid-HTTP endpoints for resource synchronization
- WebSocket endpoints for real-time updates
- AI query endpoints for contextual responses

## Testing

Extensive testing validates backend data integrity:

```bash
cargo test -p local_link_server
```

Key test suites:
- `tests/chat_sync_integrity.rs`: Multi-peer chat context storage, retrieval, and corrupt data handling

## Technology Stack

- **Language**: Rust
- **Web Framework**: Axum (with WebSocket and multipart support)
- **Database**: SQLx + SQLite (with migrations)
- **Authentication**: bcrypt
- **AI**: genai
- **Middleware**: Tower, tower-http (CORS, tracing)
- **Braid Protocol**: braid-core, braid-http, braid-blob, braid-common

## License

MIT OR Apache-2.0
