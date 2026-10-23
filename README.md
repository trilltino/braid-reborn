# Braid Reborn

A Rust workspace for Braid state synchronization over HTTP and Iroh, with
versioned text storage, blob storage, filesystem synchronization and server
applications.

Protocol guarantees, test results and remaining work are recorded in the
[audit implementation status](docs/audits/AUDIT_IMPLEMENTATION_STATUS.md).

## Repository layout

```text
crates/             Project libraries and applications
references/         Vendored Iroh dependencies used by braid-iroh
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

Run Cargo commands from the repository root using stable Rust:

```sh
cargo check --workspace --locked
cargo build -p braid-iroh --locked
cargo run -p local_link_server --locked
```

Plain Cargo commands select the eight project packages. `--workspace` also
selects the vendored Iroh packages. Build artifacts share the root `target/`.

See the [workspace guide](docs/workspace.md) for validation commands, local data
paths and generated-file policy, and the [script guide](scripts/README.md) for
upstream conformance testing. The
[original protocol and Rust audit](docs/audits/BRAID_REBORN_PROTOCOL_AND_RUST_AUDIT_2026-10-22.md)
is preserved alongside its implementation status.

## License

Project packages declare MIT OR Apache-2.0. Vendored dependencies retain their
own license files and declarations.
