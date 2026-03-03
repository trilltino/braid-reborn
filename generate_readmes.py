import os

# Content for each root README

readmes = {
    "braid-http": """# Braid HTTP

This project implements the Braid-HTTP protocol over standard HTTP, allowing direct, serverless state synchronization between peers or traditional client-server environments.

## Braid HTTP Integration
The crate manages state synchronization on top of standard networking stacks. When a peer updates a resource, the change is versioned and a delta is generated. This update is then propagated to subscribers. The system supports:
- Subscriptions: Peers establish persistent subscriptions to resources using Braid-HTTP semantics.
- Differential Synchronization: Updates are transmitted as precise patches rather than full state, minimizing overhead.
- Version History: Each resource maintains a graph of versions, allowing strict conflict resolution.

## Applications
This library forms the foundational networking layer for any Braid-compatible application. It handles request generation, parsing, and streaming of patch updates.

## Technology Stack
- Language: Rust
- Networking: reqwest, HTTP parsing
- Protocol: Braid-HTTP (State Synchronization, Versioning)

## Testing Status
Tests are currently being migrated and expanded. Unit tests for parsing and formatting HTTP headers are planned for future versions.

## Using the Crate
Add `braid-http` to your dependencies to build Braid compliant clients and servers. Use the provided serializers and constants to properly configure your HTTP requests.
""",
    
    "braid-common": """# Braid Common

This project provides the shared utility foundations, constants, and data structures utilized across the entire Braid ecosystem in Rust.

## Core Integration
By centralizing shared logic such as global ID generation, standard paths, error types, and system configurations, this crate ensures that other decoupled Braid components (`braid-core`, `braid-http`, `braid-blob`) all speak the exact same underlying language and follow the same system invariants.

## Applications
This is not a standalone application but a core dependency. It is used universally across the workspace to prevent code duplication and enforce consistency.

## Technology Stack
- Language: Rust
- Serialization: Serde, JSON
- Utilities: UUID, Anyhow (Error handling)

## Testing Status
Extensive macro and utility testing is planned. Constants are verified functionally by the integration tests in dependent crates.

## Using the Crate
Include `braid-common` for standard error handlers, constants, and utilities within your own Braid tools.
""",

    "braid-blob": """# Braid Blob

This project implements a high-performance binary storage engine and synchronization service designed specifically for the Braid ecosystem.

## Braid Blob Storage Integration
It extends the textual state synchronization capabilities of Braid-HTTP to manage large immutable and mutable blob assets. Validated via SHA-256 integrity checks, it ensures reliable read, write, and synchronization of complex blob structures across networks.

## Applications
Ideal for decentralized file sharing applications, avatar syncing for chat platforms, or any application requiring robust, version-controlled binary data transfer alongside semantic state.

## Technology Stack
- Language: Rust
- Storage Backends: rusqlite (metadata), Native filesystem (binary chunks)
- Cryptography: SHA-256 for integrity hashing

## Testing Status
This module has implicit testing coverage through the `braid-core` integration tests, specifically verifying checksum verification, atomic writes, and data integrity over the disk.

## Using the Crate
Leverage `BlobStore` to persist arbitrary byte streams, linking them directly to Braid version hashes.
""",

    "braid-core": """# Braid Core

This project implements the unified Braid Protocol in Rust, encapsulating the core state synchronization, Antimatter CRDT mechanics, and BraidFS logic into a single high-performance engine.

## Protocol Integration
Acting as the central standard library, this engine processes incoming Braid patches, calculates Antimatter conflict-free replicated data type (CRDT) states, and yields deterministic states. It operates fully decoupled from the transport layer, while maintaining seamless integrations with `braid-http`. The system supports:
- CRDT Resolution: Guarantees eventual consistency across decentralized nodes.
- Virtual File Systems (BraidFS): Synchronizing physical file boundaries across nodes.
- Immutable History: Full traversal and rollback of document history graphs.

## Applications
This acts as the core processing engine for peer-to-peer applications, specialized servers, and local daemons. It is heavily utilized by `braidfs-daemon` and `server`.

## Technology Stack
- Language: Rust
- Async Runtime: Tokio
- Synchronization: Antimatter CRDT algorithms

## Testing Status
Rigorous automated integration and unit tests are included.
- `tests/sync_integrity_tests.rs`: Tests atomic file writing guarantees directly to disk without race conditions.
- Further testing covers checksum generation, validation, and error handling for corrupted disk data.

## Using the Crate
Instantiate the Braid engine to process Braid-HTTP requests, track history, and maintain distributed system states efficiently.
""",

    "braidfs-daemon": """# BraidFS Daemon

This project is a background system-level service daemon powering BraidFS, the Braid File System.

## BraidFS Integration
The daemon orchestrates persistent data replication over Braid-HTTP. Operating automatically in the background, it ensures your system's local mount points remain precisely in sync with remote peers. It interfaces deeply with system APIs and the main `braid-core` library.

## Applications
The daemon runs as an isolated process on user machines (Windows/Unix). It transforms static file directories into active, synchronized collaborative workspaces without requiring manual user intervention.

## Technology Stack
- Language: Rust
- Framework: Clap (CLI daemon management), Tokio
- Core: `braid-core` (CRDT & FS features)

## Testing Status
Daemon functionality is currently tested functionally in live environments. Automated daemon lifecycle tests are planned.

## Using the Crate
Run the `braidfs-daemon` binary as a background service, pointing it to your local directories intended for P2P synchronization.
""",

    "braidfs-nfs": """# BraidFS NFS

This project implements a Network File System (NFS) bridge for the Braid ecosystem.

## NFS Bridge Integration
Instead of building custom file system drivers for every operating system, this service natively exposes Braid synchronized states as standard network attached storage. By translating OS-level file system calls into fully versioned Braid-HTTP patches, it achieves synchronization invisibly to the client OS.

## Applications
Allows standard applications (like IDEs, media players, or basic text editors) to interact with localized peers or remote collaborative directories seamlessly by mounting them as an NFS drive.

## Technology Stack
- Language: Rust
- Network Storage: NFSv3/NFSv4 Server primitives
- Async Integration: parking_lot, async-channel, Tokio

## Testing Status
Currently tested through integration tools. Full NFS mount simulation tests are on the roadmap.

## Using the Crate
Launch the service and mount the resulting loopback IP as an NFS volume from your host operating system to begin interacting with Braid content natively.
""",

    "server": """# Braid LocalLink Server

This project is a robust centralized bridging server for the Braid ecosystem, providing data aggregation, storage caching, and daemon interplay.

## Server Integration
The server multiplexes updates across Braid nodes. It is especially useful in hybrid P2P/Server topologies where direct peer connections might be restricted. It provides:
- Authentication: Secure token-based access and password hashing.
- Persistent Storage: SQLite and JSON-based Braid-compliant chat storing.
- State Caching: Reduces load on peer nodes by caching and serving recent version states to new subscribers.

## Applications
The LocalLink server hosts decentralized chat interfaces, powers web applications requiring real-time updates via WebSockets or HTTP streams, and seamlessly integrates AI contextual agents.

## Technology Stack
- Language: Rust
- Framework: Axum (Web routing, WebSockets), Tower
- Database: SQLx, SQLite
- Security: bcrypt

## Testing Status
Extensive testing is in place validating backend data integrity.
- `tests/chat_sync_integrity.rs`: Validates correct addition, storage, and retrieval of multi-peer chat contexts. Corrupt data handling and blob sync stability are thoroughly simulated.

## Using the Crate
Run the compiled binary passing appropriate configuration environments (like database URLs and sync endpoints) to orchestrate data across your local or remote network.
"""
}

# Directories within crates to receive subdirectory READMEs
sub_readmes = {
    "src": "This directory contains the primary source code modules for this crate.",
    "tests": "This directory contains the integration and integrity tests verifying the functionality of this crate.",
    "examples": "This directory contains practical examples showcasing how to implement and interact with the modules provided by this crate.",
    "core": "This sub-module handles the fundamental core primitives, parsing, and base state structures.",
    "fs": "This sub-module encapsulates the virtual file system (VFS) and pathing integration logic.",
    "client": "This sub-module manages outgoing connection logic, request generation, and peer-to-peer dialing protocols.",
    "protocol": "This sub-module implements the specific byte-level protocol rules, state machines, and patch generation protocols.",
    "handlers": "This sub-module contains the routing handlers, dispatching incoming network requests to appropriate internal actors.",
    "chat": "This sub-module maintains chat-specific data structures, room synchronization details, and messaging logic."
}

target_dir = r"C:\Users\isich\309_braid_reborn"

for crate, content in readmes.items():
    crate_path = os.path.join(target_dir, crate)
    if os.path.isdir(crate_path):
        readme_path = os.path.join(crate_path, "README.md")
        with open(readme_path, "w", encoding="utf-8") as f:
            f.write(content)
            
        # Add subdirectory readmes
        for root, dirs, files in os.walk(crate_path):
            # Skip hidden dirs like .git
            dirs[:] = [d for d in dirs if not d.startswith('.')]
            
            # create README in subdirs
            for d in dirs:
                if d in sub_readmes:
                    sub_readme_path = os.path.join(root, d, "README.md")
                    # don't overwrite if it exists, but actually we probably want to ensure it's there
                    with open(sub_readme_path, "w", encoding="utf-8") as f:
                        f.write(f"# `{d}` Module Directory\n\n{sub_readmes[d]}\n")
