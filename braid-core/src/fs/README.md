# BraidFS Module (`fs`)

The `fs` module is the engine room of the Braid File System (BraidFS). It implements the bidirectional synchronization logic that connects the local filesystem to the global Braid network.

## Key Components

### Synchronization Daemon (`mod.rs`)
The heart of the module is `run_daemon`, which orchestrates several sub-systems:
- **Filesystem Watcher**: Monitors local file changes in real-time.
- **State Manager**: Maintains the "truth" about local versions, remote peers, and synchronization progress.
- **Subscription Loop**: Listens for Braid-HTTP updates from peers.
- **Activity Tracker**: Prevents redundant sync cycles by tracking which resources are currently being updated.

### Internal API Server (`api.rs`)
Starts an Axum-based server (typically on a local port) to handle administrative and sync-specific requests:
- `/.braidfs/sync` / `unmount`: Manage sync endpoints.
- `/.braidfs/push`: Force a manual push of local content.
- `/.braidfs/config`: Remote-accessible node configuration.
- `/.braidfs/errors`: Exposes internal error logs for diagnostics.

### Filesystem Driver (`watcher.rs`, `debouncer.rs`)
Interfaces with the OS to detect changes. A `debouncer` is used to ensure that a single "save" operation (which might trigger multiple OS events) results in exactly one Braid sync event.

### Synchronization Engine (`sync.rs`, `binary_sync.rs`)
- **Text Sync**: Uses the `diff.rs` logic to generate minimal Braid patches for text files.
- **Binary Sync**: Handles large files efficiently by hashing and chunking, often bypassing the standard Braid text-diffing pipeline for performance.

### Mounting and Mapping (`mount.rs`, `mapping.rs`)
Responsible for mapping local file paths to Braid URLs (e.g., `C:\Documents\project` -> `/braid/project`). It handles path normalization and cross-platform compatibility.

### NFS Integration (`nfs.rs`)
Provides the backend logic for the `braidfs-nfs` crate, allowing Braid states to be served as an NFS volume.

## Logic Flow

1. **Change Detection**: `watcher` detects a file modification.
2. **Debounce**: `debouncer` waits for the file system to settle.
3. **Diffing**: `sync` reads the file and generates a Braid `Update`.
4. **Persistence**: The version is stored in the local registry (`state.rs`).
5. **Broadcast**: The update is pushed to all subscribed peers via the Braid network.

## Technology Stack
- **Filesystem Events**: `notify`
- **Diffing Engine**: `dissimilar`
- **Core Protocol**: `braid-http`, `braid-core`
- **Web Interface**: `axum`
