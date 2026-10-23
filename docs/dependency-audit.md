# Direct dependency audit, 2026-10-23

Registry metadata for 90 unique direct dependency package names in the baseline
was queried from the official crates.io API. This is a complete declaration and
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
- Disable SQLx defaults: only SQLite/runtime/TLS/migrations are needed. Replace a
  single enum derive with explicit lowercase database values, avoiding database
  macro backends. Stored pending/accepted/rejected values are unchanged.
- Keep rusqlite 0.32.1 for its current API and libsqlite3-sys compatibility with
  SQLx. Blob transactions run off the async executor. Shared server pool ownership
  and NFS database blocking work still need review.
- Apply compatible security updates to TLS/HTTP/DNS/QUIC/compression/runtime
  dependencies and their coupled WASM bindgen family; do not blanket-update the
  whole graph. crypto-common 0.2.0 was yanked/incompatible with digest 0.11.3;
  0.2.2 is selected. Cargo.lock records reproducible final versions.
- Keep distinct rand 0.8/0.9/0.10 API users where migrations are unnecessary, but
  update each to patched compatible releases. A duplicate-major count by itself
  does not establish a defect or justify unsafe forced unification.
- Retain smartstring used by the copied Diamond engine, and paste used by
  Iroh's netlink dependencies, while compatible maintained replacements remain
  unresolved. They carry unmaintained advisories.
  These are explicit open maintenance risks, not blanket ignored advisories.
- Reuse futures/async-channel/Tokio where APIs actually require them. A full task
  ownership/pool/feature minimization and udeps/geiger audit remains outstanding.

## Security and licenses

Baseline cargo audit reported 30 vulnerability entries, plus unsound, unmaintained
and yanked warnings. Intermediate transport/security updates reduced this to one
RSA entry; final counts are recorded in engineering-status.md after the final gate.
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
| braid-blob | [async-trait](https://crates.io/crates/async-trait) | 0.1.89 / 0.1.92 | normal; all; default | src/store.rs:1 |
| braid-blob | braid-http | workspace `../braid-http` | normal | local |
| braid-blob | [bytes](https://crates.io/crates/bytes) | 1.11.1 / 1.12.1 | normal; all; serde | src/objects.rs:99 |
| braid-blob | [rusqlite](https://crates.io/crates/rusqlite) | 0.32.1 / 0.40.2 | normal; all; bundled | src/store.rs:7 |
| braid-blob | [serde](https://crates.io/crates/serde) | 1.0.228 / 1.0.229 | normal; all; derive | src/store.rs:8 |
| braid-blob | [serde_json](https://crates.io/crates/serde_json) | 1.0.149 / 1.0.154 | normal; all; default | src/store.rs:116 |
| braid-blob | [sha2](https://crates.io/crates/sha2) | 0.10.9, 0.11.1 / 0.11.1 | normal; all; default | src/objects.rs:4 |
| braid-blob | [tokio](https://crates.io/crates/tokio) | 1.49.0 / 1.54.1 | normal; all; full | src/objects.rs:6 |
| braid-blob | [tracing](https://crates.io/crates/tracing) | 0.1.44 / 0.1.44 | normal; all; default | src/objects.rs:25 |
| braid-blob | [uuid](https://crates.io/crates/uuid) | 1.21.0 / 1.29.0 | normal; all; v4 | src/objects.rs:44 |
| braid-blob | [tempfile](https://crates.io/crates/tempfile) | 3.25.0 / 3.27.0 | dev; all; default | tests/streaming.rs:14 |
| braid-core | [anyhow](https://crates.io/crates/anyhow) | 1.0.104 / 1.0.104 | normal; all; default | src/core/error.rs:78 |
| braid-core | [async-channel](https://crates.io/crates/async-channel) | 2.5.0 / 2.5.0 | normal; all; default | tests/audit_filesystem.rs:60 |
| braid-core | [async-stream](https://crates.io/crates/async-stream) | 0.3.6 / 0.3.6 | normal; all; default | src/fs/local_server.rs:273 |
| braid-core | [async-trait](https://crates.io/crates/async-trait) | 0.1.89 / 0.1.92 | normal; all; default | src/core/traits.rs:3 |
| braid-core | [axum](https://crates.io/crates/axum) | 0.8.8 / 0.8.9 | normal; all; default; optional | tests/audit_filesystem.rs:25 |
| braid-core | [base64](https://crates.io/crates/base64) | 0.22.1, 0.23.1 / 0.23.1 | normal; all; default | src/replica.rs:191 |
| braid-core | braid-blob | workspace `../braid-blob` | normal | local |
| braid-core | braid-http | workspace `../braid-http` | normal | local |
| braid-core | [bumpalo](https://crates.io/crates/bumpalo) | 3.20.2 / 3.20.3 | normal; all; collections | src/vendor/diamond_types/oplog.rs:505 |
| braid-core | [bytes](https://crates.io/crates/bytes) | 1.11.1 / 1.12.1 | normal; all; serde | tests/audit_filesystem.rs:27 |
| braid-core | [chrono](https://crates.io/crates/chrono) | 0.4.43 / 0.4.45 | normal; all; serde | src/fs/api.rs:306 |
| braid-core | [crc](https://crates.io/crates/crc) | 3.4.0 / 3.4.0 | normal; all; default | src/vendor/diamond_types/encoding/tools.rs:112 |
| braid-core | [dissimilar](https://crates.io/crates/dissimilar) | 1.0.10 / 1.0.11 | normal; all; default; optional | src/fs/diff.rs:2 |
| braid-core | [futures](https://crates.io/crates/futures) | 0.3.32 / 0.3.34 | normal; all; default | src/core/server/middleware.rs:8 |
| braid-core | [headers](https://crates.io/crates/headers) | 0.4.1 / 0.4.2 | normal; all; default; optional | src/replica.rs:120 |
| braid-core | [http](https://crates.io/crates/http) | 1.4.0 / 1.5.0 | normal; all; default | tests/audit_filesystem.rs:27 |
| braid-core | [humansize](https://crates.io/crates/humansize) | 2.1.3 / 2.1.3 | normal; all; default | src/vendor/diamond_types/list/list.rs:2 |
| braid-core | [jumprope](https://crates.io/crates/jumprope) | 1.1.2 / 1.1.2 | normal; all; default | src/vendor/diamond_types/branch.rs:221 |
| braid-core | [lazy_static](https://crates.io/crates/lazy_static) | 1.5.0 / 1.5.1 | normal; all; default | src/fs/mod.rs:46 |
| braid-core | [libc](https://crates.io/crates/libc) | 0.2.182 / 0.2.191 | normal; all; default | src/vendor/diamond_types/storage/file.rs:64 |
| braid-core | [lz4_flex](https://crates.io/crates/lz4_flex) | 0.12.2 / 0.14.0 | normal; all; default; optional | src/vendor/diamond_types/encoding/parseerror.rs:23 |
| braid-core | [nfsserve](https://crates.io/crates/nfsserve) | 0.10.2 / 0.11.0 | normal; all; default; optional | src/fs/mod.rs:505 |
| braid-core | [notify](https://crates.io/crates/notify) | 8.2.0 / 8.2.0 | normal; all; default; optional | src/core/error.rs:69 |
| braid-core | [num_enum](https://crates.io/crates/num_enum) | 0.7.5 / 0.7.6 | normal; all; default | src/vendor/diamond_types/encoding/mod.rs:4 |
| braid-core | [parking_lot](https://crates.io/crates/parking_lot) | 0.12.5 / 0.13.0 | normal; all; default | tests/audit_filesystem.rs:86 |
| braid-core | [percent-encoding](https://crates.io/crates/percent-encoding) | 2.3.2 / 2.3.2 | normal; all; default | src/fs/api.rs:330 |
| braid-core | [rand](https://crates.io/crates/rand) | 0.8.8, 0.9.5, 0.10.3 / 0.10.3 | normal; all; default | src/vendor/diamond_types/fuzzer.rs:1 |
| braid-core | [rusqlite](https://crates.io/crates/rusqlite) | 0.32.1 / 0.40.2 | normal; all; default; optional | tests/audit_filesystem.rs:87 |
| braid-core | [serde](https://crates.io/crates/serde) | 1.0.228 / 1.0.229 | normal; all; derive | src/lib.rs:15 |
| braid-core | [serde_json](https://crates.io/crates/serde_json) | 1.0.149 / 1.0.154 | normal; all; default | src/replica.rs:14 |
| braid-core | [sha2](https://crates.io/crates/sha2) | 0.10.9, 0.11.1 / 0.11.1 | normal; all; default | src/replica.rs:15 |
| braid-core | [smallvec](https://crates.io/crates/smallvec) | 1.15.1 / 1.16.3 | normal; all; serde | src/lib.rs:10 |
| braid-core | [smartstring](https://crates.io/crates/smartstring) | 1.0.1 / 1.0.1 | normal; all; serde | src/vendor/diamond_types/branch.rs:4 |
| braid-core | [str_indices](https://crates.io/crates/str_indices) | 0.4.4 / 0.4.4 | normal; all; default | src/vendor/diamond_types/unicount.rs:11 |
| braid-core | [thiserror](https://crates.io/crates/thiserror) | 1.0.69, 2.0.18 / 2.0.21 | normal; all; default | src/replica.rs:23 |
| braid-core | [tokio](https://crates.io/crates/tokio) | 1.49.0 / 1.54.1 | normal; all; default; optional | src/replica.rs:21 |
| braid-core | [tower](https://crates.io/crates/tower) | 0.5.3 / 0.5.3 | normal; all; default; optional | src/core/server/middleware.rs:123 |
| braid-core | [tracing](https://crates.io/crates/tracing) | 0.1.44 / 0.1.44 | normal; all; default | src/fs/api.rs:84 |
| braid-core | [url](https://crates.io/crates/url) | 2.5.8 / 2.5.8 | normal; all; default | tests/audit_filesystem.rs:49 |
| braid-core | [uuid](https://crates.io/crates/uuid) | 1.21.0 / 1.29.0 | normal; all; v4,serde,js | src/fs/api.rs:369 |
| braid-core | [lz4_flex](https://crates.io/crates/lz4_flex) | 0.12.2 / 0.14.0 | dev; all; default | src/vendor/diamond_types/encoding/parseerror.rs:23 |
| braid-core | [tempfile](https://crates.io/crates/tempfile) | 3.25.0 / 3.27.0 | dev; all; default | tests/audit_filesystem.rs:17 |
| braid-http | [arbitrary](https://crates.io/crates/arbitrary) | 1.5.0 / unrecorded | normal; all; derive; optional | src/client/fuzzer.rs:6 |
| braid-http | [async-channel](https://crates.io/crates/async-channel) | 2.5.0 / 2.5.0 | normal; all; default | src/traits.rs:31 |
| braid-http | [async-trait](https://crates.io/crates/async-trait) | 0.1.89 / 0.1.92 | normal; all; default | src/traits.rs:3 |
| braid-http | [bytes](https://crates.io/crates/bytes) | 1.11.1 / 1.12.1 | normal; all; serde | src/error.rs:13 |
| braid-http | [futures](https://crates.io/crates/futures) | 0.3.32 / 0.3.34 | normal; all; default | src/traits.rs:8 |
| braid-http | [gloo-timers](https://crates.io/crates/gloo-timers) | 0.3.0 / 0.4.0 | normal; all; futures; optional | src/traits.rs:75 |
| braid-http | [http](https://crates.io/crates/http) | 1.4.0 / 1.5.0 | normal; all; default | examples/fuzz_controller.rs:76 |
| braid-http | [js-sys](https://crates.io/crates/js-sys) | 0.3.106 / 0.3.106 | normal; all; default; optional | src/traits.rs:79 |
| braid-http | [rand](https://crates.io/crates/rand) | 0.8.8, 0.9.5, 0.10.3 / 0.10.3 | normal; all; small_rng | src/client/fetch.rs:254 |
| braid-http | [serde](https://crates.io/crates/serde) | 1.0.228 / 1.0.229 | normal; all; derive | src/types/content_range.rs:7 |
| braid-http | [serde_json](https://crates.io/crates/serde_json) | 1.0.149 / 1.0.154 | normal; all; default | examples/codec_fixtures.rs:6 |
| braid-http | [sfv](https://crates.io/crates/sfv) | 0.14.0 / 0.16.0 | normal; all; default | src/protocol/headers.rs:46 |
| braid-http | [thiserror](https://crates.io/crates/thiserror) | 1.0.69, 2.0.18 / 2.0.21 | normal; all; default | src/error.rs:4 |
| braid-http | [tracing](https://crates.io/crates/tracing) | 0.1.44 / 0.1.44 | normal; all; default | src/client/fetch.rs:194 |
| braid-http | [url](https://crates.io/crates/url) | 2.5.8 / 2.5.8 | normal; all; default | examples/fuzz_controller.rs:100 |
| braid-http | [uuid](https://crates.io/crates/uuid) | 1.21.0 / 1.29.0 | normal; all; v4,serde,js | src/durable.rs:15 |
| braid-http | [wasm-bindgen-futures](https://crates.io/crates/wasm-bindgen-futures) | 0.4.79 / 0.4.79 | normal; all; default; optional | src/traits.rs:71 |
| braid-http | [reqwest](https://crates.io/crates/reqwest) | 0.13.2 / 0.13.5 | normal; cfg(not(target_arch = "wasm32")); json,stream,multipart | src/client/fetch.rs:42 |
| braid-http | [tokio](https://crates.io/crates/tokio) | 1.49.0 / 1.54.1 | normal; cfg(not(target_arch = "wasm32")); full | examples/fuzz_controller.rs:8 |
| braid-http | [getrandom](https://crates.io/crates/getrandom) | 0.2.17, 0.3.4, 0.4.1 / 0.4.3 | normal; cfg(target_arch = "wasm32"); js | derive/feature/platform boundary |
| braid-http | [reqwest](https://crates.io/crates/reqwest) | 0.13.2 / 0.13.5 | normal; cfg(target_arch = "wasm32"); json,stream; defaults off | src/client/fetch.rs:42 |
| braid-http | [tokio](https://crates.io/crates/tokio) | 1.49.0 / 1.54.1 | normal; cfg(target_arch = "wasm32"); sync | examples/fuzz_controller.rs:8 |
| braid-iroh | [anyhow](https://crates.io/crates/anyhow) | 1.0.104 / 1.0.104 | normal; all; default | src/lib.rs:26 |
| braid-iroh | [async-stream](https://crates.io/crates/async-stream) | 0.3.6 / 0.3.6 | normal; all; default | src/protocol.rs:62 |
| braid-iroh | [axum](https://crates.io/crates/axum) | 0.8.8 / 0.8.9 | normal; all; ws | src/protocol.rs:2 |
| braid-iroh | [blake3](https://crates.io/crates/blake3) | 1.8.3 / 1.8.7 | normal; all; default | src/lib.rs:90 |
| braid-iroh | braid_http_rs | workspace `../braid-core` | normal | local |
| braid-iroh | [bytes](https://crates.io/crates/bytes) | 1.11.1 / 1.12.1 | normal; all; default | src/lib.rs:113 |
| braid-iroh | [futures](https://crates.io/crates/futures) | 0.3.32 / 0.3.34 | normal; all; default | src/protocol.rs:63 |
| braid-iroh | [http](https://crates.io/crates/http) | 1.4.0 / 1.5.0 | normal; all; default | src/protocol.rs:4 |
| braid-iroh | [iroh](https://crates.io/crates/iroh) | 1.4.1 / 1.4.1 | normal; all; default | src/discovery.rs:5 |
| braid-iroh | [iroh-gossip](https://crates.io/crates/iroh-gossip) | 0.101.0 / unrecorded | normal; all; net | src/lib.rs:26 |
| braid-iroh | [iroh-h3-axum](https://crates.io/crates/iroh-h3-axum) | 0.4.0 / unrecorded | normal; all; default | src/protocol.rs:12 |
| braid-iroh | [iroh-h3-client](https://crates.io/crates/iroh-h3-client) | 0.5.0 / unrecorded | normal; all; default | src/proxy.rs:19 |
| braid-iroh | [n0-future](https://crates.io/crates/n0-future) | 0.3.2 / 0.3.2 | normal; all; default | src/discovery.rs:15 |
| braid-iroh | [parking_lot](https://crates.io/crates/parking_lot) | 0.12.5 / 0.13.0 | normal; all; default | src/discovery.rs:7 |
| braid-iroh | [rand](https://crates.io/crates/rand) | 0.8.8, 0.9.5, 0.10.3 / 0.10.3 | normal; all; default | src/lib.rs:128 |
| braid-iroh | [serde](https://crates.io/crates/serde) | 1.0.228 / 1.0.229 | normal; all; derive | src/subscription.rs:14 |
| braid-iroh | [serde_json](https://crates.io/crates/serde_json) | 1.0.149 / 1.0.154 | normal; all; default | src/subscription.rs:89 |
| braid-iroh | [tokio](https://crates.io/crates/tokio) | 1.49.0 / 1.54.1 | normal; all; full | src/lib.rs:95 |
| braid-iroh | [tower-http](https://crates.io/crates/tower-http) | 0.6.8 / 0.7.1 | normal; all; cors,trace,fs | src/proxy.rs:21 |
| braid-iroh | [tracing](https://crates.io/crates/tracing) | 0.1.44 / 0.1.44 | normal; all; default | src/lib.rs:66 |
| braid-iroh | [tempfile](https://crates.io/crates/tempfile) | 3.25.0 / 3.27.0 | dev; all; default | tests/transport.rs:14 |
| braidfs-daemon | [anyhow](https://crates.io/crates/anyhow) | 1.0.104 / 1.0.104 | normal; all; default | src/main.rs:14 |
| braidfs-daemon | braid-core | workspace `../braid-core` | normal | local |
| braidfs-daemon | [clap](https://crates.io/crates/clap) | 4.5.60 / 4.6.7 | normal; all; derive | src/main.rs:2 |
| braidfs-daemon | [tokio](https://crates.io/crates/tokio) | 1.49.0 / 1.54.1 | normal; all; full | src/main.rs:13 |
| braidfs-daemon | [tracing](https://crates.io/crates/tracing) | 0.1.44 / 0.1.44 | normal; all; default | src/main.rs:3 |
| braidfs-daemon | [tracing-subscriber](https://crates.io/crates/tracing-subscriber) | 0.3.22 / 0.3.23 | normal; all; env-filter | src/main.rs:15 |
| braidfs-nfs | [anyhow](https://crates.io/crates/anyhow) | 1.0.104 / 1.0.104 | normal; all; default | src/main.rs:23 |
| braidfs-nfs | [async-channel](https://crates.io/crates/async-channel) | 2.5.0 / 2.5.0 | normal; all; default | src/main.rs:69 |
| braidfs-nfs | braid-core | workspace `../braid-core` | normal | local |
| braidfs-nfs | [clap](https://crates.io/crates/clap) | 4.5.60 / 4.6.7 | normal; all; derive | src/main.rs:3 |
| braidfs-nfs | [nfsserve](https://crates.io/crates/nfsserve) | 0.10.2 / 0.11.0 | normal; all; default | src/main.rs:4 |
| braidfs-nfs | [parking_lot](https://crates.io/crates/parking_lot) | 0.12.5 / 0.13.0 | normal; all; default | src/main.rs:5 |
| braidfs-nfs | [rusqlite](https://crates.io/crates/rusqlite) | 0.32.1 / 0.40.2 | normal; all; bundled | src/main.rs:6 |
| braidfs-nfs | [tokio](https://crates.io/crates/tokio) | 1.49.0 / 1.54.1 | normal; all; full | src/main.rs:9 |
| braidfs-nfs | [tracing](https://crates.io/crates/tracing) | 0.1.44 / 0.1.44 | normal; all; default | src/main.rs:10 |
| braidfs-nfs | [tracing-subscriber](https://crates.io/crates/tracing-subscriber) | 0.3.22 / 0.3.23 | normal; all; env-filter | src/main.rs:24 |
| local_link_server | [anyhow](https://crates.io/crates/anyhow) | 1.0.104 / 1.0.104 | normal; all; default | examples/ai_context_test.rs:10 |
| local_link_server | [async-stream](https://crates.io/crates/async-stream) | 0.3.6 / 0.3.6 | normal; all; default | src/chat/handlers/braid_subscribe.rs:211 |
| local_link_server | [axum](https://crates.io/crates/axum) | 0.8.8 / 0.8.9 | normal; all; ws,multipart | src/lib.rs:15 |
| local_link_server | [bcrypt](https://crates.io/crates/bcrypt) | 0.18.0 / 0.19.3 | normal; all; default | src/core/auth/mod.rs:10 |
| local_link_server | braid-blob | workspace `../braid-blob` | normal | local |
| local_link_server | braid-common | workspace `../braid-common` | normal | local |
| local_link_server | braid-core | workspace `../braid-core` | normal | local |
| local_link_server | braid-http | workspace `../braid-http` | normal | local |
| local_link_server | [bytes](https://crates.io/crates/bytes) | 1.11.1 / 1.12.1 | normal; all; serde | src/lib.rs:213 |
| local_link_server | [chrono](https://crates.io/crates/chrono) | 0.4.43 / 0.4.45 | normal; all; serde | src/chat/crdt.rs:7 |
| local_link_server | [futures](https://crates.io/crates/futures) | 0.3.32 / 0.3.34 | normal; all; default | src/chat/mail/mod.rs:18 |
| local_link_server | [genai](https://crates.io/crates/genai) | 0.5.3 / 0.7.0 | normal; all; default | src/chat/ai/mod.rs:4 |
| local_link_server | [headers](https://crates.io/crates/headers) | 0.4.1 / 0.4.2 | normal; all; default | src/lib.rs:173 |
| local_link_server | [http](https://crates.io/crates/http) | 1.4.0 / 1.5.0 | normal; all; default | src/lib.rs:129 |
| local_link_server | [notify](https://crates.io/crates/notify) | 8.2.0 / 8.2.0 | normal; all; default | src/core/daemon/mod.rs:12 |
| local_link_server | [reqwest](https://crates.io/crates/reqwest) | 0.13.2 / 0.13.5 | normal; all; json | src/core/daemon/mod.rs:25 |
| local_link_server | [serde](https://crates.io/crates/serde) | 1.0.228 / 1.0.229 | normal; all; derive | src/chat/crdt.rs:8 |
| local_link_server | [serde_json](https://crates.io/crates/serde_json) | 1.0.149 / 1.0.154 | normal; all; default | src/lib.rs:247 |
| local_link_server | [sha2](https://crates.io/crates/sha2) | 0.10.9, 0.11.1 / 0.11.1 | normal; all; default | tests/chat_sync_integrity.rs:56 |
| local_link_server | [sqlx](https://crates.io/crates/sqlx) | 0.8.6 / 0.9.0 | normal; all; sqlite,runtime-tokio,tls-native-tls,migrate; defaults off | src/core/error.rs:58 |
| local_link_server | [tokio](https://crates.io/crates/tokio) | 1.49.0 / 1.54.1 | normal; all; full | examples/ai_context_test.rs:7 |
| local_link_server | [tower](https://crates.io/crates/tower) | 0.5.3 / 0.5.3 | normal; all; default | tests/engineering_auth.rs:15 |
| local_link_server | [tower-http](https://crates.io/crates/tower-http) | 0.6.8 / 0.7.1 | normal; all; cors,trace | src/lib.rs:159 |
| local_link_server | [tracing](https://crates.io/crates/tracing) | 0.1.44 / 0.1.44 | normal; all; default | src/lib.rs:25 |
| local_link_server | [tracing-subscriber](https://crates.io/crates/tracing-subscriber) | 0.3.22 / 0.3.23 | normal; all; env-filter | src/lib.rs:26 |
| local_link_server | [uuid](https://crates.io/crates/uuid) | 1.21.0 / 1.29.0 | normal; all; v4,serde | src/chat/crdt.rs:10 |
| local_link_server | [tempfile](https://crates.io/crates/tempfile) | 3.25.0 / 3.27.0 | dev; all; default | examples/ai_context_test.rs:6 |
| local_link_server | [tokio-test](https://crates.io/crates/tokio-test) | 0.4.6 / 0.4.6 | dev; all; default | derive/feature/platform boundary |
