# Working in this workspace

## Layout

All eight project packages live in `crates/`, including the server and filesystem
binaries. Their package names and `cargo -p` selectors are unchanged. The server's
package name is `local_link_server`.

`references/` contains source dependencies used by `braid-iroh`: the Iroh gossip
fork and HTTP/3 adapter packages. These are active Cargo workspace members, not
disposable reference material. Their upstream examples, licenses and manifests
are retained. The HTTP/3 example is also a workspace member because the adapters
use it in their development dependencies.

The root `Cargo.toml` defines the workspace, and the root `Cargo.lock` is the
shared dependency lockfile. Plain Cargo commands select the project packages;
`--workspace` includes the vendored members as well. Run commands from the root
to share `target/` and keep relative application data paths consistent.

## Build and check

Install the stable Rust toolchain and the native build prerequisites required by
your platform. The project crates use Rust 2021; the HTTP/3 forks use Rust 2024.
For browser builds, install the WASM target:

```sh
rustup target add wasm32-unknown-unknown
cargo check --workspace --locked
cargo build -p braidfs-daemon --locked
cargo run -p local_link_server --locked
```

CI runs native checks on Linux and Windows, focused protocol/filesystem/server
regressions, a WASM compile check, and the upstream HTTP/subscription suites. To
reproduce the focused Rust checks:

```sh
cargo test -p braid-http --locked --lib --test audit_conformance
cargo test -p braid-core --locked --test audit_replica --test audit_filesystem --test sync_integrity_tests
cargo test -p local_link_server --locked --test audit_routes
cargo check -p braid-http --locked --target wasm32-unknown-unknown --no-default-features --features wasm
```

The [script guide](../scripts/README.md) covers the independent upstream harness.
The [audit status](audits/AUDIT_IMPLEMENTATION_STATUS.md) records protocol
guarantees, validation results and remaining work.

## Generated files and local data

- `target/` holds Cargo artifacts and may be removed with `cargo clean` when a
  rebuild is wanted. It is ignored by Git.
- `target/audit/` holds downloaded conformance tooling and machine reports.
  Existing root audit logs were preserved in `target/audit/logs/`.
- `braid_data/` is the default local application data directory. Set `BRAID_ROOT`
  to use another location. Local data and `.env` files are ignored.
- Durable audit documents live in `docs/audits/`; generated logs stay out of the
  source tree. The original audit keeps its historical paths and revision links.
- `scripts/legacy/` holds the old generation and publishing scripts for a
  different checkout. They are retained for reference and are not CI inputs.

Add new project packages under `crates/`; the workspace member and default-member
globs include them automatically. Keep shared build and ignore rules at the root.
