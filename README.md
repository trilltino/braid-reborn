# Braid Reborn

A Rust workspace for Braid state synchronization over HTTP and Iroh, with
versioned text storage, blob storage, filesystem synchronization and server
applications.

Current verification and remaining release work are recorded in the
[engineering status](docs/audit/engineering-status.md), with
[architecture](docs/architecture.md), [conformance](docs/conformance.md) and
[migration notes](docs/migration.md). The earlier
[audit status](docs/audits/AUDIT_IMPLEMENTATION_STATUS.md) is historical.

## Repository layout

```text
crates/             Project libraries and applications
docs/               Workspace guide and protocol audits
scripts/            Conformance runner and archived tooling
.github/workflows/  Native, WASM and conformance CI
Cargo.toml          Workspace definition
Cargo.lock          Shared, tracked dependency lockfile
target/             Ignored build output and generated audit reports
```

## Packages

| Package | Purpose |
| --- | --- |
| [braid-http](crates/braid-http/) | Protocol types, byte codec, HTTP clients and subscriptions |
| [braid-core](crates/braid-core/) | Versioned replicas, merge engines, HTTP adapters and filesystem synchronization |
| [braid-blob](crates/braid-blob/) | Content-addressed binary storage with SQLite metadata |
| [braid-common](crates/braid-common/) | Application data directory helpers used by the server |
| [braid-iroh](crates/braid-iroh/) | Iroh transport, gossip announcements and HTTP/3 integration |
| [braidfs-daemon](crates/braidfs-daemon/) | Filesystem synchronization daemon |
| [braidfs-nfs](crates/braidfs-nfs/) | NFS bridge for Braid filesystem state |
| [local_link_server](crates/server/) | LocalLink server, page routes, authentication and chat |

## Development

Run Cargo commands from the repository root using pinned Rust 1.99.0:

```sh
cargo check --workspace --locked
cargo build -p braid-iroh --locked
cargo run -p local_link_server --locked
```

Plain Cargo commands and `--workspace` select the eight project packages.
Iroh/Gossip/H3 come from pinned registry releases. Their old source copies were
removed after patch comparison and local HTTP/3 integration tests. Build artifacts
share the root `target/`.

## Capability matrix

| Capability | Status |
| --- | --- |
| Native snapshots, binary patch framing, 209 subscriptions | Tested with Rust and pinned upstream JS |
| Durable history, duplicate retries, lost ACK recovery | Tested for the documented single-writer store |
| Historical-parent Diamond concurrency | Opposite-order delivery, seeded schedules and restarts tested |
| Simpleton | Coordinated writes; stale writes conflict; full JS convergence open |
| Blob streaming, limits, digests and durable references | Tested; historical objects retained; no automatic GC |
| Chat persistence and authentication | Tested regressions; per-resource ACLs incomplete |
| Iroh 1.4.1 HTTP/3 | Local GET/PUT/209/revocation tested; relay and anti-entropy open |
| Browser/WASM | Compile checked; browser behavior remains experimental |
| NFS | Experimental; complete rename/truncate/tombstone guarantees incomplete |
| Linked JSON, range queries, transport multiplexing | Unsupported/incomplete |
| Dependency security | Unmaintained findings and inactive lockfile RSA advisory remain |

This is an engineering checkpoint, not completion of every release criterion.

See the [workspace guide](docs/workspace.md) for validation commands, local data
paths and generated-file policy, and the [script guide](scripts/README.md) for
upstream conformance testing. The
[original protocol and Rust audit](docs/audits/BRAID_REBORN_PROTOCOL_AND_RUST_AUDIT_2026-10-22.md)
is preserved alongside its implementation status.

## License

Project packages declare MIT OR Apache-2.0. Vendored dependencies retain their
own license files and declarations.
