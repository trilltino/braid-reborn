# BraidFS Daemon

A background system-level service daemon powering BraidFS, the Braid File System. This daemon orchestrates persistent, real-time data replication over Braid-HTTP, transforming static file directories into active, synchronized collaborative workspaces.

## How It Works

The daemon runs as an isolated background process on user machines (Windows, macOS, Linux). Once started, it:

1. **Watches local directories** for file changes using the `notify` crate's filesystem event API.
2. **Computes diffs** between the current and previous file states using the `dissimilar` crate for efficient text diffing.
3. **Generates Braid patches** from those diffs and publishes them via `braid-core`'s client API.
4. **Subscribes to remote peers** and applies incoming patches to the local filesystem, keeping directories in sync bidirectionally.
5. **Manages blob storage** for binary files via `braid-blob`, ensuring large assets are transferred with SHA-256 integrity checks.

All synchronization happens automatically in the background without requiring manual user intervention.

## Architecture

```
braidfs-daemon
  +-- main.rs        CLI entry point (Clap-derived argument parser)
  |
  +-- braid-core     (with features: fs, server, client, blob)
      +-- fs/        Filesystem watcher and patch generation
      +-- blob       Binary storage and integrity verification
      +-- client     Braid-HTTP subscription and update client
      +-- server     Local Braid-HTTP server for accepting incoming patches
```

## CLI Usage

The daemon is built as a standalone binary with Clap-derived command-line arguments:

```bash
# Start the daemon, watching a local directory for changes
braidfs-daemon --watch /path/to/sync/directory

# Connect to a remote peer for bidirectional synchronization
braidfs-daemon --watch /path/to/sync/directory --peer https://peer-address
```

## Configuration

| Argument | Description |
|---|---|
| `--watch` | Local directory path to monitor for changes |
| `--peer` | Remote Braid-HTTP endpoint to synchronize with |
| `--port` | Local port for the embedded Braid-HTTP server |
| `--log-level` | Logging verbosity (trace, debug, info, warn, error) |

## Technology Stack

- **Language**: Rust
- **CLI Framework**: Clap (derive mode)
- **Async Runtime**: Tokio (full features)
- **Core Engine**: braid-core (CRDT, filesystem sync, blob storage)
- **Logging**: tracing + tracing-subscriber with env-filter

## Testing

Daemon functionality is tested functionally in live environments. Automated daemon lifecycle tests are planned.

```bash
cargo test -p braidfs-daemon
```

## License

MIT OR Apache-2.0
