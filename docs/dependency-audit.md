# Direct dependency audit, 2026-10-23

Baseline registry observations were queried from the official crates.io API.
The current inventory records 140 direct declarations across eight packages;
resolved versions below are each declaration's actual Cargo graph edges, not
every transitive version with the same package name. This is a complete declaration and
call-site inventory, not a claim that every dependency implementation has been
exhaustively reviewed. Selected API sources were read for the transport upgrade,
SQLite blocking boundaries, SFV values, durable writes and text merge adapter.
Exact requirements, optional/default features, target conditions, licenses and
matching Rust source files are recorded in [dependencies.json](audit/dependencies.json).
Versions below come from the shared Cargo.lock; a package name may have several
transitive major/minor versions even when a particular direct edge selects one.
Latest releases are observations as of the audit, not upgrade instructions.

## Decisions

- Upgrade Iroh to 1.4.1, Gossip to 0.101.0, H3 adapter/axum/client to
  0.3.0/0.4.0/0.5.0 as one compatible set. Registry source APIs and an actual
  authenticated two-peer H3 exchange were checked. No test-utils production flag.
- Remove 22 unused first-party manifest declarations only after searching every
  Rust source in the owning crate (including examples, tests and copied modules).
  Keep transitive dependencies where their remaining owner uses them.
- Replace the SQLx umbrella dependency with sqlx-core/sqlx-sqlite 0.8.6 and
  explicit bundled SQLite/Tokio features. This removes unused MySQL/Postgres
  macro backends and their inactive RSA advisory from the lockfile. Existing
  database values and schema stay unchanged. Auth/friends share a bounded pool.
- Keep rusqlite 0.32.1 for its current API and libsqlite3-sys compatibility with
  SQLx. Blob transactions run off the async executor. NFS operations run on blocking workers and use WAL/FULL
  transactions. Server auth/friends use the shared async pool.
- Apply compatible security updates to TLS/HTTP/DNS/QUIC/compression/runtime
  dependencies and their coupled WASM bindgen family; do not blanket-update the
  whole graph. crypto-common 0.2.0 was yanked/incompatible with digest 0.11.3;
  0.2.2 is selected. Cargo.lock records reproducible final versions.
- Keep distinct rand 0.8/0.9/0.10 API users where migrations are unnecessary, but
  update each to patched compatible releases. A duplicate-major count by itself
  does not establish a defect or justify unsafe forced unification.
- Replace copied Diamond smartstring storage with compact_str 0.10.0 and
  preserve string serde representation. Replace the netlink 0.8 macro backend
  using a source-identical registry copy plus a manifest-only pastey 0.2.3 alias.
  [The patch record](../vendor/netlink-packet-core/BRAID_PATCH.json) pins source,
  archive checksum, license and reason; netlink 0.9 removes an API still required
  by its consumers, so it cannot be upgraded independently.
- Reuse futures/async-channel/Tokio where APIs actually require them. Target-only browser/macOS dependencies now live under the correct
  Cargo target sections. Udeps found unused server tokio-test, which was removed.
  Cargo outdated failed on its temporary path-patch copy; Geiger timed out after
  300 seconds. Neither is represented as a passing audit.

- NFS mount/RPC evidence requires a narrow nfsserve 0.10.2 patch. Its plain
  READDIR handler discarded the continuation cookie; both directory procedures
  subtracted unsigned framing budgets without checking tiny counts. An independent
  wire client reproduces the first failure. The inspected 0.11.0 source retains
  both defects, so changing version alone is insufficient. The archive hash,
  BSD license and precise patch are recorded under vendor/nfsserve.

## Security and licenses

Baseline cargo audit reported 30 vulnerability entries, plus unsound, unmaintained
and yanked warnings. Intermediate transport/security updates reduced this to one
RSA entry. This continuation removes that inactive backend and the two
unmaintained dependencies, and upgrades Tokio 1.49 to patched 1.54.1 with its
coupled libc/mio/socket2/macros graph. Final counts are recorded in
engineering-status.md after the final gate.
Do not call the graph clean while any configured check fails.

The committed deny.toml explicitly permits the SPDX licenses present in reviewed
metadata, including Unicode/data licenses and MPL-2.0 dependencies. Keep upstream
license notices and source obligations with distributions. First-party manifests
use their existing MIT OR Apache-2.0 project license; root copies of those texts
are supplied. A policy file is not a substitute for a distribution license review.
No vulnerability ignore list was added.

## Every current direct declaration

The last column names an observed source file, or a feature/derive boundary that
requires review. See the JSON record for all matching files and exact features.

| Owner | Dependency | Locked / latest stable | Scope and features | Call-site evidence |
| --- | --- | --- | --- | --- |
| braid-blob | [tempfile](https://crates.io/crates/tempfile) | 3.25.0 / 3.27.0 | dev; all; default | braid-blob/tests/streaming.rs:14 |
| braid-blob | [async-trait](https://crates.io/crates/async-trait) | 0.1.89 / 0.1.92 | normal; all; default | braid-blob/src/store.rs:1 |
| braid-blob | braid-http | workspace | normal; all; default | braid-blob/src/objects.rs:3 |
| braid-blob | [bytes](https://crates.io/crates/bytes) | 1.11.1 / 1.12.1 | normal; all; serde | braid-blob/src/objects.rs:99 |
| braid-blob | [rusqlite](https://crates.io/crates/rusqlite) | 0.32.1 / 0.40.2 | normal; all; bundled | braid-blob/src/store.rs:7 |
| braid-blob | [serde](https://crates.io/crates/serde) | 1.0.228 / 1.0.229 | normal; all; derive | braid-blob/src/store.rs:8 |
| braid-blob | [serde_json](https://crates.io/crates/serde_json) | 1.0.149 / 1.0.154 | normal; all; default | braid-blob/src/store.rs:116 |
| braid-blob | [sha2](https://crates.io/crates/sha2) | 0.10.9 / 0.11.1 | normal; all; default | braid-blob/src/objects.rs:4 |
| braid-blob | [tokio](https://crates.io/crates/tokio) | 1.54.1 / 1.54.1 | normal; all; full | braid-blob/src/objects.rs:6 |
| braid-blob | [tracing](https://crates.io/crates/tracing) | 0.1.44 / 0.1.44 | normal; all; default | braid-blob/src/objects.rs:25 |
| braid-blob | [uuid](https://crates.io/crates/uuid) | 1.21.0 / 1.29.0 | normal; all; v4 | braid-blob/src/objects.rs:44 |
| braid-core | [lz4_flex](https://crates.io/crates/lz4_flex) | 0.12.2 / 0.14.0 | dev; all; default | braid-core/src/vendor/diamond_types/encoding/parseerror.rs:23 |
| braid-core | [tempfile](https://crates.io/crates/tempfile) | 3.25.0 / 3.27.0 | dev; all; default | braid-core/tests/audit_filesystem.rs:17 |
| braid-core | [anyhow](https://crates.io/crates/anyhow) | 1.0.104 / 1.0.104 | normal; all; default | braid-core/src/core/error.rs:78 |
| braid-core | [async-channel](https://crates.io/crates/async-channel) | 2.5.0 / 2.5.0 | normal; all; default | braid-core/tests/audit_filesystem.rs:60 |
| braid-core | [async-stream](https://crates.io/crates/async-stream) | 0.3.6 / 0.3.6 | normal; all; default | braid-core/src/fs/local_server.rs:273 |
| braid-core | [async-trait](https://crates.io/crates/async-trait) | 0.1.89 / 0.1.92 | normal; all; default | braid-core/src/core/traits.rs:3 |
| braid-core | [axum](https://crates.io/crates/axum) | 0.8.8 / 0.8.9 | normal; all; default; optional | braid-core/tests/audit_filesystem.rs:25 |
| braid-core | [base64](https://crates.io/crates/base64) | 0.22.1 / 0.23.1 | normal; all; default | braid-core/src/replica.rs:191 |
| braid-core | braid-blob | workspace | normal; all; default; optional | braid-core/src/lib.rs:30 |
| braid-core | braid-http | workspace | normal; all; default | braid-core/examples/simpleton_controller.rs:5 |
| braid-core | [bumpalo](https://crates.io/crates/bumpalo) | 3.20.2 / 3.20.3 | normal; all; collections | braid-core/src/vendor/diamond_types/oplog.rs:505 |
| braid-core | [bytes](https://crates.io/crates/bytes) | 1.11.1 / 1.12.1 | normal; all; serde | braid-core/tests/audit_filesystem.rs:27 |
| braid-core | [chrono](https://crates.io/crates/chrono) | 0.4.43 / 0.4.45 | normal; all; serde | braid-core/src/fs/api.rs:312 |
| braid-core | [compact_str](https://crates.io/crates/compact_str) | 0.10.0 / 0.10.0 | normal; all; serde | braid-core/src/vendor/diamond_types/branch.rs:4 |
| braid-core | [crc](https://crates.io/crates/crc) | 3.4.0 / 3.4.0 | normal; all; default | braid-core/src/vendor/diamond_types/encoding/tools.rs:112 |
| braid-core | [dissimilar](https://crates.io/crates/dissimilar) | 1.0.10 / 1.0.11 | normal; all; default; optional | braid-core/src/fs/diff.rs:2 |
| braid-core | [futures](https://crates.io/crates/futures) | 0.3.32 / 0.3.34 | normal; all; default | braid-core/src/core/server/middleware.rs:8 |
| braid-core | [headers](https://crates.io/crates/headers) | 0.4.1 / 0.4.2 | normal; all; default; optional | braid-core/src/replica.rs:120 |
| braid-core | [http](https://crates.io/crates/http) | 1.4.0 / 1.5.0 | normal; all; default | braid-core/examples/simpleton_controller.rs:2 |
| braid-core | [humansize](https://crates.io/crates/humansize) | 2.1.3 / 2.1.3 | normal; all; default | braid-core/src/vendor/diamond_types/list/list.rs:2 |
| braid-core | [jumprope](https://crates.io/crates/jumprope) | 1.1.2 / 1.1.2 | normal; all; default | braid-core/src/vendor/diamond_types/branch.rs:221 |
| braid-core | [lazy_static](https://crates.io/crates/lazy_static) | 1.5.0 / 1.5.1 | normal; all; default | braid-core/src/fs/mod.rs:48 |
| braid-core | [libc](https://crates.io/crates/libc) | 0.2.191 / 0.2.191 | normal; cfg(target_os = "macos"); default | braid-core/src/vendor/diamond_types/storage/file.rs:64 |
| braid-core | [lz4_flex](https://crates.io/crates/lz4_flex) | 0.12.2 / 0.14.0 | normal; all; default; optional | braid-core/src/vendor/diamond_types/encoding/parseerror.rs:23 |
| braid-core | [nfsserve](https://crates.io/crates/nfsserve) | 0.10.2 / 0.11.0 | normal; all; default; optional | braid-core/tests/audit_shared_channel.rs:246 |
| braid-core | [notify](https://crates.io/crates/notify) | 8.2.0 / 8.2.0 | normal; all; default; optional | braid-core/src/core/error.rs:69 |
| braid-core | [num_enum](https://crates.io/crates/num_enum) | 0.7.5 / 0.7.6 | normal; all; default | braid-core/src/vendor/diamond_types/encoding/mod.rs:4 |
| braid-core | [parking_lot](https://crates.io/crates/parking_lot) | 0.12.5 / 0.13.0 | normal; all; default | braid-core/tests/audit_filesystem.rs:87 |
| braid-core | [percent-encoding](https://crates.io/crates/percent-encoding) | 2.3.2 / 2.3.2 | normal; all; default | braid-core/src/fs/api.rs:336 |
| braid-core | [rand](https://crates.io/crates/rand) | 0.9.5 / 0.10.3 | normal; all; default | braid-core/src/vendor/diamond_types/fuzzer.rs:1 |
| braid-core | [rusqlite](https://crates.io/crates/rusqlite) | 0.32.1 / 0.40.2 | normal; all; default; optional | braid-core/tests/audit_filesystem.rs:88 |
| braid-core | [serde](https://crates.io/crates/serde) | 1.0.228 / 1.0.229 | normal; all; derive | braid-core/src/lib.rs:22 |
| braid-core | [serde_json](https://crates.io/crates/serde_json) | 1.0.149 / 1.0.154 | normal; all; default | braid-core/examples/simpleton_controller.rs:6 |
| braid-core | [sha2](https://crates.io/crates/sha2) | 0.10.9 / 0.11.1 | normal; all; default | braid-core/src/replica.rs:15 |
| braid-core | [smallvec](https://crates.io/crates/smallvec) | 1.15.1 / 1.16.3 | normal; all; serde | braid-core/src/lib.rs:15 |
| braid-core | [str_indices](https://crates.io/crates/str_indices) | 0.4.4 / 0.4.4 | normal; all; default | braid-core/src/vendor/diamond_types/unicount.rs:11 |
| braid-core | [thiserror](https://crates.io/crates/thiserror) | 2.0.18 / 2.0.21 | normal; all; default | braid-core/src/replica.rs:23 |
| braid-core | [tokio](https://crates.io/crates/tokio) | 1.54.1 / 1.54.1 | normal; all; default; optional | braid-core/examples/simpleton_controller.rs:7 |
| braid-core | [tower](https://crates.io/crates/tower) | 0.5.3 / 0.5.3 | normal; all; default; optional | braid-core/src/core/server/middleware.rs:123 |
| braid-core | [tracing](https://crates.io/crates/tracing) | 0.1.44 / 0.1.44 | normal; all; default | braid-core/src/fs/api.rs:84 |
| braid-core | [url](https://crates.io/crates/url) | 2.5.8 / 2.5.8 | normal; all; default | braid-core/examples/simpleton_controller.rs:25 |
| braid-core | [uuid](https://crates.io/crates/uuid) | 1.21.0 / 1.29.0 | normal; all; v4,serde,js | braid-core/src/simpleton.rs:33 |
| braid-http | [arbitrary](https://crates.io/crates/arbitrary) | 1.5.0 / unrecorded | normal; all; derive; optional | braid-http/src/client/fuzzer.rs:6 |
| braid-http | [async-channel](https://crates.io/crates/async-channel) | 2.5.0 / 2.5.0 | normal; all; default | braid-http/src/traits.rs:31 |
| braid-http | [async-trait](https://crates.io/crates/async-trait) | 0.1.89 / 0.1.92 | normal; all; default | braid-http/src/traits.rs:3 |
| braid-http | [bytes](https://crates.io/crates/bytes) | 1.11.1 / 1.12.1 | normal; all; serde | braid-http/src/error.rs:13 |
| braid-http | [futures](https://crates.io/crates/futures) | 0.3.32 / 0.3.34 | normal; all; default | braid-http/src/traits.rs:8 |
| braid-http | [getrandom](https://crates.io/crates/getrandom) | 0.2.17 / 0.4.3 | normal; cfg(target_arch = "wasm32"); js | local/feature boundary |
| braid-http | [gloo-timers](https://crates.io/crates/gloo-timers) | 0.3.0 / 0.4.0 | normal; cfg(target_arch = "wasm32"); futures; optional | braid-http/src/traits.rs:75 |
| braid-http | [http](https://crates.io/crates/http) | 1.4.0 / 1.5.0 | normal; all; default | braid-http/examples/fuzz_controller.rs:77 |
| braid-http | [httpdate](https://crates.io/crates/httpdate) | 1.0.3 / 1.0.3 | normal; all; default | braid-http/src/client/retry.rs:165 |
| braid-http | [js-sys](https://crates.io/crates/js-sys) | 0.3.106 / 0.3.106 | normal; cfg(target_arch = "wasm32"); default; optional | braid-http/src/traits.rs:79 |
| braid-http | [rand](https://crates.io/crates/rand) | 0.8.8 / 0.10.3 | normal; all; small_rng | braid-http/src/client/channel.rs:531 |
| braid-http | [reqwest](https://crates.io/crates/reqwest) | 0.13.2 / 0.13.5 | normal; cfg(not(target_arch = "wasm32")); json,stream,multipart | braid-http/src/client/fetch.rs:42 |
| braid-http | [reqwest](https://crates.io/crates/reqwest) | 0.13.2 / 0.13.5 | normal; cfg(target_arch = "wasm32"); json,stream | braid-http/src/client/fetch.rs:42 |
| braid-http | [serde](https://crates.io/crates/serde) | 1.0.228 / 1.0.229 | normal; all; derive | braid-http/src/client/channel.rs:8 |
| braid-http | [serde_json](https://crates.io/crates/serde_json) | 1.0.149 / 1.0.154 | normal; all; default | braid-http/examples/codec_fixtures.rs:6 |
| braid-http | [sfv](https://crates.io/crates/sfv) | 0.14.0 / 0.16.0 | normal; all; default | braid-http/src/protocol/headers.rs:46 |
| braid-http | [thiserror](https://crates.io/crates/thiserror) | 2.0.18 / 2.0.21 | normal; all; default | braid-http/src/error.rs:4 |
| braid-http | [tokio](https://crates.io/crates/tokio) | 1.54.1 / 1.54.1 | normal; cfg(not(target_arch = "wasm32")); full | braid-http/examples/fuzz_controller.rs:8 |
| braid-http | [tokio](https://crates.io/crates/tokio) | 1.54.1 / 1.54.1 | normal; cfg(target_arch = "wasm32"); sync | braid-http/examples/fuzz_controller.rs:8 |
| braid-http | [tracing](https://crates.io/crates/tracing) | 0.1.44 / 0.1.44 | normal; all; default | braid-http/src/client/fetch.rs:194 |
| braid-http | [url](https://crates.io/crates/url) | 2.5.8 / 2.5.8 | normal; all; default | braid-http/examples/fuzz_controller.rs:101 |
| braid-http | [uuid](https://crates.io/crates/uuid) | 1.21.0 / 1.29.0 | normal; all; v4,serde,js | braid-http/src/durable.rs:68 |
| braid-http | [wasm-bindgen-futures](https://crates.io/crates/wasm-bindgen-futures) | 0.4.79 / 0.4.79 | normal; cfg(target_arch = "wasm32"); default; optional | braid-http/src/traits.rs:71 |
| braid-iroh | [tempfile](https://crates.io/crates/tempfile) | 3.25.0 / 3.27.0 | dev; all; default | braid-iroh/tests/transport.rs:14 |
| braid-iroh | [anyhow](https://crates.io/crates/anyhow) | 1.0.104 / 1.0.104 | normal; all; default | braid-iroh/src/lib.rs:31 |
| braid-iroh | [async-stream](https://crates.io/crates/async-stream) | 0.3.6 / 0.3.6 | normal; all; default | braid-iroh/src/protocol.rs:62 |
| braid-iroh | [axum](https://crates.io/crates/axum) | 0.8.8 / 0.8.9 | normal; all; ws | braid-iroh/src/protocol.rs:2 |
| braid-iroh | [blake3](https://crates.io/crates/blake3) | 1.8.3 / 1.8.7 | normal; all; default | braid-iroh/src/lib.rs:95 |
| braid-iroh | braid-core | workspace | normal; all; server,client | braid-iroh/src/lib.rs:96 |
| braid-iroh | [bytes](https://crates.io/crates/bytes) | 1.11.1 / 1.12.1 | normal; all; default | braid-iroh/src/lib.rs:118 |
| braid-iroh | [futures](https://crates.io/crates/futures) | 0.3.32 / 0.3.34 | normal; all; default | braid-iroh/src/protocol.rs:63 |
| braid-iroh | [http](https://crates.io/crates/http) | 1.4.0 / 1.5.0 | normal; all; default | braid-iroh/src/protocol.rs:4 |
| braid-iroh | [iroh](https://crates.io/crates/iroh) | 1.4.1 / 1.4.1 | normal; all; default | braid-iroh/src/discovery.rs:5 |
| braid-iroh | [iroh-gossip](https://crates.io/crates/iroh-gossip) | 0.101.0 / unrecorded | normal; all; net | braid-iroh/src/lib.rs:31 |
| braid-iroh | [iroh-h3-axum](https://crates.io/crates/iroh-h3-axum) | 0.4.0 / unrecorded | normal; all; default | braid-iroh/src/protocol.rs:12 |
| braid-iroh | [iroh-h3-client](https://crates.io/crates/iroh-h3-client) | 0.5.0 / unrecorded | normal; all; default | braid-iroh/src/proxy.rs:19 |
| braid-iroh | [n0-future](https://crates.io/crates/n0-future) | 0.3.2 / 0.3.2 | normal; all; default | braid-iroh/src/discovery.rs:15 |
| braid-iroh | [parking_lot](https://crates.io/crates/parking_lot) | 0.12.5 / 0.13.0 | normal; all; default | braid-iroh/src/discovery.rs:7 |
| braid-iroh | [rand](https://crates.io/crates/rand) | 0.10.3 / 0.10.3 | normal; all; default | braid-iroh/src/lib.rs:133 |
| braid-iroh | [serde](https://crates.io/crates/serde) | 1.0.228 / 1.0.229 | normal; all; derive | braid-iroh/src/subscription.rs:14 |
| braid-iroh | [serde_json](https://crates.io/crates/serde_json) | 1.0.149 / 1.0.154 | normal; all; default | braid-iroh/src/subscription.rs:89 |
| braid-iroh | [tokio](https://crates.io/crates/tokio) | 1.54.1 / 1.54.1 | normal; all; full | braid-iroh/src/lib.rs:100 |
| braid-iroh | [tower-http](https://crates.io/crates/tower-http) | 0.6.8 / 0.7.1 | normal; all; cors,trace,fs | braid-iroh/src/proxy.rs:21 |
| braid-iroh | [tracing](https://crates.io/crates/tracing) | 0.1.44 / 0.1.44 | normal; all; default | braid-iroh/src/lib.rs:71 |
| braidfs-daemon | [anyhow](https://crates.io/crates/anyhow) | 1.0.104 / 1.0.104 | normal; all; default | braidfs-daemon/src/main.rs:19 |
| braidfs-daemon | braid-core | workspace | normal; all; fs,server,client,blob,serde,smallvec | braidfs-daemon/src/main.rs:6 |
| braidfs-daemon | [clap](https://crates.io/crates/clap) | 4.5.60 / 4.6.7 | normal; all; derive | braidfs-daemon/src/main.rs:7 |
| braidfs-daemon | [tokio](https://crates.io/crates/tokio) | 1.54.1 / 1.54.1 | normal; all; full | braidfs-daemon/src/main.rs:18 |
| braidfs-daemon | [tracing](https://crates.io/crates/tracing) | 0.1.44 / 0.1.44 | normal; all; default | braidfs-daemon/src/main.rs:8 |
| braidfs-daemon | [tracing-subscriber](https://crates.io/crates/tracing-subscriber) | 0.3.22 / 0.3.23 | normal; all; env-filter | braidfs-daemon/src/main.rs:20 |
| braidfs-nfs | [anyhow](https://crates.io/crates/anyhow) | 1.0.104 / 1.0.104 | normal; all; default | braidfs-nfs/src/main.rs:25 |
| braidfs-nfs | braid-core | workspace | normal; all; fs,client,server,nfs | braidfs-nfs/src/main.rs:6 |
| braidfs-nfs | [clap](https://crates.io/crates/clap) | 4.5.60 / 4.6.7 | normal; all; derive | braidfs-nfs/src/main.rs:7 |
| braidfs-nfs | [nfsserve](https://crates.io/crates/nfsserve) | 0.10.2 / 0.11.0 | normal; all; default | braidfs-nfs/src/main.rs:8 |
| braidfs-nfs | [parking_lot](https://crates.io/crates/parking_lot) | 0.12.5 / 0.13.0 | normal; all; default | braidfs-nfs/src/main.rs:9 |
| braidfs-nfs | [rusqlite](https://crates.io/crates/rusqlite) | 0.32.1 / 0.40.2 | normal; all; bundled | braidfs-nfs/src/main.rs:10 |
| braidfs-nfs | [tokio](https://crates.io/crates/tokio) | 1.54.1 / 1.54.1 | normal; all; full | braidfs-nfs/src/main.rs:24 |
| braidfs-nfs | [tracing](https://crates.io/crates/tracing) | 0.1.44 / 0.1.44 | normal; all; default | braidfs-nfs/src/main.rs:12 |
| braidfs-nfs | [tracing-subscriber](https://crates.io/crates/tracing-subscriber) | 0.3.22 / 0.3.23 | normal; all; env-filter | braidfs-nfs/src/main.rs:26 |
| local_link_server | [tempfile](https://crates.io/crates/tempfile) | 3.25.0 / 3.27.0 | dev; all; default | server/examples/ai_context_test.rs:6 |
| local_link_server | [anyhow](https://crates.io/crates/anyhow) | 1.0.104 / 1.0.104 | normal; all; default | server/examples/ai_context_test.rs:10 |
| local_link_server | [async-stream](https://crates.io/crates/async-stream) | 0.3.6 / 0.3.6 | normal; all; default | server/src/chat/handlers/braid_subscribe.rs:211 |
| local_link_server | [axum](https://crates.io/crates/axum) | 0.8.8 / 0.8.9 | normal; all; ws,multipart | server/src/lib.rs:20 |
| local_link_server | [bcrypt](https://crates.io/crates/bcrypt) | 0.18.0 / 0.19.3 | normal; all; default | server/src/core/auth/mod.rs:11 |
| local_link_server | braid-blob | workspace | normal; all; default | server/src/core/store/json_store.rs:12 |
| local_link_server | braid-common | workspace | normal; all; default | server/examples/ai_context_test.rs:19 |
| local_link_server | braid-core | workspace | normal; all; default | server/src/core/pages/handlers.rs:28 |
| local_link_server | braid-http | workspace | normal; all; default | server/tests/chat_sync_integrity.rs:60 |
| local_link_server | [bytes](https://crates.io/crates/bytes) | 1.11.1 / 1.12.1 | normal; all; serde | server/src/lib.rs:218 |
| local_link_server | [chrono](https://crates.io/crates/chrono) | 0.4.43 / 0.4.45 | normal; all; serde | server/src/chat/crdt.rs:7 |
| local_link_server | [futures](https://crates.io/crates/futures) | 0.3.32 / 0.3.34 | normal; all; default | server/src/chat/mail/mod.rs:18 |
| local_link_server | [genai](https://crates.io/crates/genai) | 0.5.3 / 0.7.0 | normal; all; default | server/src/chat/ai/mod.rs:4 |
| local_link_server | [headers](https://crates.io/crates/headers) | 0.4.1 / 0.4.2 | normal; all; default | server/src/lib.rs:178 |
| local_link_server | [http](https://crates.io/crates/http) | 1.4.0 / 1.5.0 | normal; all; default | server/src/lib.rs:134 |
| local_link_server | [notify](https://crates.io/crates/notify) | 8.2.0 / 8.2.0 | normal; all; default | server/src/core/daemon/mod.rs:12 |
| local_link_server | [reqwest](https://crates.io/crates/reqwest) | 0.13.2 / 0.13.5 | normal; all; json | server/src/core/daemon/mod.rs:25 |
| local_link_server | [serde](https://crates.io/crates/serde) | 1.0.228 / 1.0.229 | normal; all; derive | server/src/chat/crdt.rs:8 |
| local_link_server | [serde_json](https://crates.io/crates/serde_json) | 1.0.149 / 1.0.154 | normal; all; default | server/src/lib.rs:252 |
| local_link_server | [sha2](https://crates.io/crates/sha2) | 0.10.9 / 0.11.1 | normal; all; default | server/tests/chat_sync_integrity.rs:56 |
| local_link_server | [sqlx-core](https://crates.io/crates/sqlx-core) | 0.8.6 / 0.9.0 | normal; all; _rt-tokio | server/src/core/database.rs:3 |
| local_link_server | [sqlx-sqlite](https://crates.io/crates/sqlx-sqlite) | 0.8.6 / 0.9.0 | normal; all; bundled | server/src/core/database.rs:4 |
| local_link_server | [tokio](https://crates.io/crates/tokio) | 1.54.1 / 1.54.1 | normal; all; full | server/examples/ai_context_test.rs:7 |
| local_link_server | [tower](https://crates.io/crates/tower) | 0.5.3 / 0.5.3 | normal; all; default | server/tests/engineering_auth.rs:15 |
| local_link_server | [tower-http](https://crates.io/crates/tower-http) | 0.6.8 / 0.7.1 | normal; all; cors,trace | server/src/lib.rs:164 |
| local_link_server | [tracing](https://crates.io/crates/tracing) | 0.1.44 / 0.1.44 | normal; all; default | server/src/lib.rs:30 |
| local_link_server | [tracing-subscriber](https://crates.io/crates/tracing-subscriber) | 0.3.22 / 0.3.23 | normal; all; env-filter | server/src/lib.rs:31 |
| local_link_server | [uuid](https://crates.io/crates/uuid) | 1.21.0 / 1.29.0 | normal; all; v4,serde | server/src/chat/crdt.rs:10 |

## Resource-policy continuation (2026-10-25)

The policy store, local administrator, shared HTTP/H3 authorization and application
route boundary use existing serde, SHA-256, UUID, Tokio and durable-file edges.
No registry dependency or Cargo.lock revision is added. Policy JSON is a separate
versioned format, not an engine storage-format change. Existing trusted local
replica APIs remain available; network mutation and read adapters add explicit
principal checks. The old Iroh error-hiding conveniences remain deprecated
adapters alongside fallible reads/history APIs. Full public-API, dependency and
provenance review remains an open release gate.

Individual fs compilation reproduced undeclared server/client feature needs.
The fs feature now enables those existing edges and nfs enables fs. The
13-profile runner checks each profile independently, including library tests.

Origin credentials, configuration migration and control authorization use existing
URL, HTTP, serde, SHA-256 and async-channel dependencies. No package or lockfile
version changes are introduced. Existing constructors remain; the additional
with_client_builder adapter applies library redirect policy while the older
with_client constructor retains its caller-owned transport contract.
