# Server Core Module (`core`)

The `core` module provides the foundational infrastructure for the Braid LocalLink Server, handling authentication, data persistence, and protocol routing.

## Key Sub-modules

### Authentication (`auth/`)
Manages the user lifecycle and security:
- Token-based authentication for Braid-HTTP and WebSocket connections.
- Password hashing via `bcrypt`.
- SQLite-backed user account management.

### Protocol Routing (`protocol/`, `router.rs`)
Defines the server's API surface, including:
- Standard Braid-HTTP resource routes.
- RPC-style endpoints for administrative operations.
- WebSocket upgrade logic for real-time bidirectional state sync.

### Data Storage (`store/`, `models/`, `blobs.rs`)
- **`store/`**: Relational database integration using SQLx (SQLite).
- **`models/`**: Schema definitions for users, sessions, and resource metadata.
- **`blobs.rs`**: Server-side blob management, caching frequently accessed binary assets for the P2P network.

### Daemon Orchestration (`daemon/`)
Handles communication with `braidfs-daemon` instances. This sub-module allows the server to act as a bridge and discovery coordinator for filesystem sync nodes.

### Web Pages (`pages/`)
Contains the static and dynamic templates for the server's web dashboard and management console.
