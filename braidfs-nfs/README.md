# BraidFS NFS

A Network File System (NFS) bridge for the Braid ecosystem. Instead of building custom file system drivers for every operating system, this service natively exposes Braid-synchronized states as standard network-attached storage.

## How It Works

The NFS bridge translates OS-level file system calls into fully versioned Braid-HTTP patches, achieving synchronization invisibly to the client operating system. When an application reads or writes a file on the mounted NFS volume, the following flow executes:

```
Application (IDE, editor, media player)
    |
    v
NFS Client (OS built-in)
    |
    v
braidfs-nfs (this crate -- NFSv3/v4 server)
    |
    v
braid-core (Braid-HTTP patches, CRDT merge, blob storage)
    |
    v
Remote Peers (via Braid-HTTP subscriptions)
```

1. **NFS Read**: The OS reads a file path. The NFS server resolves the path against the local Braid state, retrieves the latest version from `braid-core`, and returns the content.
2. **NFS Write**: The OS writes to a file. The NFS server captures the write, computes a Braid patch (diff), publishes it via `braid-core`'s client, and broadcasts to subscribed peers.
3. **Remote Update**: An incoming Braid patch from a remote peer triggers an update to the local filesystem state. The next NFS read from the OS will see the updated content.

## Key Components

| Component | Description |
|---|---|
| `nfsserve` integration | Provides the NFSv3/v4 server primitives for handling mount, read, write, and directory listing operations |
| `braid-core` (nfs feature) | Supplies the NFS VFS adapter that maps file paths to Braid resources |
| Async event channels | `async-channel` bridges the NFS synchronous call model with Braid's async event-driven reconciliation loop |
| `parking_lot` | Fast, low-overhead mutexes for shared state between the NFS server thread and the async Braid runtime |

## Applications

- Mount a Braid-synchronized directory as a native drive on any OS
- Allow standard applications (IDEs, text editors, media players) to interact with collaborative content without any Braid-specific tooling
- Bridge legacy workflows into the Braid P2P ecosystem

## CLI Usage

```bash
# Start the NFS server and mount Braid resources
braidfs-nfs --mount /braid --peer https://peer-address --port 2049

# The mounted path is then accessible as a standard network drive
# On Linux/macOS: mount -t nfs localhost:/braid /mnt/braid
# On Windows: mount \\localhost\braid Z:
```

## Technology Stack

- **Language**: Rust
- **NFS Server**: nfsserve (NFSv3/v4 primitives)
- **Async Runtime**: Tokio (full features)
- **Core Engine**: braid-core (with `nfs`, `fs`, `client`, `server` features)
- **Concurrency**: parking_lot, async-channel
- **Storage**: rusqlite (metadata), native filesystem (blobs)

## Testing

Integration tested through NFS mount simulation tools. Full automated mount/unmount lifecycle tests are on the roadmap.

```bash
cargo test -p braidfs-nfs
```

## License

MIT OR Apache-2.0
