# Audit implementation, 23 October 2026

This records the changes made from `BRAID_REBORN_PROTOCOL_AND_RUST_AUDIT_2026-10-22.md`.
It is an implementation and validation record, not a claim that every roadmap stage is complete.

## Implemented

- **F01/F03:** one byte codec for native and browser requests, finite HTTP responses, subscriptions, wiki/local.org/v2 pages and Iroh. `Patches` is a decimal count with individually framed bodies, including a single patch and zero patches. Ambiguous bodies, framing overrides and header injection fail explicitly.
- **F04:** the server is back in the workspace. The small directory helper dependency is now ordinary source in `crates/braid-common`; builds do not depend on the orphaned gitlink.
- **F05/F08:** preserve complete frontiers, separate `Current-Version`, preserve binary payloads, handle single `Content-Range` patches, and reject malformed lengths, duplicate headers and partial EOF. Defaults: 64 KiB headers, 16 MiB individual bodies, 1,024 patches, 32 MiB updates/entities.
- **F02/F10/F11:** journal immutable text PUT identities, bases, content and parents before sending. Replay pending requests after restart, including a lost ACK. Persist acknowledged content for offline diffs. Serialize each local document's network/materialization operations. Dirty local files cause an explicit conflict instead of a remote overwrite. Initial subscriptions and polling use the same materialization path. Save version/hash/content only after successful atomic disk replacement.
- **F06/F07:** correct Diamond Unicode positions and predecessor parents, reject unknown ancestry, validate ranges before mutation, clone candidate state independently, and persist the actual operation checkpoint and wire-version mappings. Durable replicas retain original updates, identity fingerprints and the version graph in the same atomic document. Duplicate identities with different semantics fail; failed multi-patch updates leave the committed document untouched. Reconnect can replay retained updates or receive a replacement snapshot. Corrupt JSON is an error, never an empty document.
- **F09:** TCP HTTP and Iroh HTTP/3 share the replica adapter, continuing 209 frames, replay and heartbeat handling. Iroh stores history on disk; display names select persisted random key files rather than deriving public, predictable secrets.
- **F10:** `subscribe_reliable` owns reconnect/cancellation and waits for the caller's explicit acknowledgment before advancing its resume frontier. HTTP errors, stream failures, heartbeats and partial EOF are handled separately. Dropping the subscription cancels its worker. The filesystem acknowledges only after materialization and cursor persistence.
- **F12/F13:** browser Fetch and streaming transport replace the WASM stubs. Native clients support normal protocol negotiation and pooling; multiplex origins include the port. Multiplex framing/buffering is bounded and delivery does not await under the request registry lock.
- **F14:** blobs are immutable content-addressed objects, made durable before SQLite publishes their references. Reference/event commits are transactional and cancellation rolls back. Parent validation replaces lexical ordering. Replayed old events do not revert newer content. Existing file-based blobs remain readable.
- **F15:** native/WASM CI, focused regression tests, an upstream JSON-lines controller, and a pinned upstream HTTP/subscription runner.

## Current guarantees and boundaries

Versioned text mutations require one string `Version` event and the complete `Parents` frontier (empty for a new document). Older JSON-valued patch headers are rejected.

The shared positional replica accepts updates against its current frontier. Stale/concurrent positional writes receive **409**, preserving the inputs for explicit reconciliation. The persisted Diamond checkpoint is real causal history, but automatic merging of arbitrary concurrent historical positional edits is not advertised. Existing snapshot-only v2 JSON is imported on the first commit; the old graph remains available and replay can fall back to a snapshot. Legacy wiki files seed a separate authoritative replica on first access; subsequent HTTP writes materialize that replica back to the file. External file edits after import need explicit reconciliation rather than silently replacing authoritative history.

The filesystem has one pending text request per URL, at most 128 URLs and a 16 MiB edit limit. It retains conflicts for resolution. It does not provide an automatic conflict-resolution UI, a fully transactional NFS layer, or a durable binary network outbox. OS editor writes are outside the daemon's process-local locks. JSON replica directories require a single writer process; SQLite blob writers use database transactions. The file helper syncs file data, renames within the destination directory and syncs the directory on Unix. Power-loss behavior on each filesystem remains to be fault-tested.

Iroh gossip carries bounded resource/frontier announcements (16 KiB maximum), not document bodies. Iroh peers can request authoritative replay/snapshots over HTTP/3. Automatic discovery-driven anti-entropy after missed gossip, authorization policy, and multi-peer partition testing remain follow-up work. Gossip is not treated as a commit acknowledgment.

Browser transport is compile-checked for `wasm32-unknown-unknown`; browser runtime/CORS interoperability is not yet certified. Multiplex remains experimental. Cursor sharing, Webindex, Yjs, performance benchmarks, crash injection, and the roadmap's broader property/model tests remain open.

Local results: **97 Rust tests passed**, with one pre-existing ignored parser stress test. The locked workspace and WASM checks passed, including server route construction and the filesystem recovery regressions.

Validation platform: Windows, `rustc 1.99.0 (b940084d7 2026-09-28)`. Linux CI is configured but has not run in this session.

## Reproduce validation

```text
cargo check --workspace --locked
cargo test -p braid-http --locked --lib --test audit_conformance
cargo test -p braid-core --locked --test audit_replica --test audit_filesystem --test sync_integrity_tests
cargo test -p local_link_server --locked --test audit_routes
cargo check -p braid-http --locked --target wasm32-unknown-unknown --no-default-features --features wasm
cargo build -p braid-http --locked --example fuzz_controller
node scripts/run-upstream-conformance.mjs
```

Upstream harness revision: `b79c23f877d2e6807e3925bc0165a4da139a4458`; protocol dependencies pinned to `braid-http@1.3.125` and `braid-text@0.5.3` rather than moving ranges. Reports are written under `target/audit/`.

The upstream HTTP suite passes **2/2**, subscriptions **7/7**. An additional run with `--reliability` passes **13/15**: tests 14 and 15 require the Simpleton editing controller (`sync-text`, `edit`, `ack`), which the HTTP adapter does not implement. They are reported as failures, not omitted from the reliability result. The complete Simpleton, cursors and Webindex suites have not passed. These results do not establish completion of the audit's six-stage roadmap.
