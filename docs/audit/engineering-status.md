# Engineering checkpoint, 2026-10-23

**The full refactor prompt is not complete.** This checkpoint implements tested
correctness/security repairs and the compatible Iroh stack. Open release criteria
are listed below rather than reported as supported behavior.

Baseline: `4f5275b9ab83bc3cff1ef91ff068be6539edbfa2` on master.
Implementation commit: `89a70ce1950d9a1ad5214cbd9161b714fdcaa368`. No deployed data was
migrated. Both user-supplied root documents are preserved.

## Verified changes

| Checkpoint | Changes and regression evidence | Remaining boundary |
| --- | --- | --- |
| 0: build/reproducibility | Fix server example/test imports, feature activation and arbitrary derives; pin tested Rust 1.99.0; minimal core check passes | Complete function inventory, actual MSRV and Linux runtime validation open |
| 1: canonical wire | RFC structured string/display-string frontiers; duplicate-header rejection; bounded incremental headers; scalar patch coordinates; numeric patch media type; shared runtime trait; no HTTP unsafe wrapper | Historical GET, continuous fuzz coverage, browser behavior and transport multiplexing open |
| 2: causal engine | Declared historical parent checkout, stable remote-ID checkpoint mappings, concurrent heads, idempotent retries, 24 seeded restart/duplicate schedules | Legacy public wrappers, independent CRDT oracle and complete old checkpoint corpus/provenance open |
| 3: filesystem/blob durability | Existing lost-ACK/outbox tests retained; binary import streams; bounded CAS staging, digest verification, blocking-pool SQLite transactions; safe failed-upload behavior | One unified sync state machine, cross-process JSON writer ownership, full cancellation/power-loss matrix and NFS lifecycle open |
| 4: applications | Chat persists before publishing memory; complete edit history and graph; additive event ledger for batch sync; corruption/path errors fail closed; authenticated sender and catch-all/blob routes | Per-resource ACLs, lifecycle/task ownership, durable drafts/reactions across all routes, complete legacy migration validation open |
| 5: transport modernization | Iroh 1.4.1/Gossip 0.101/H3 0.3/Axum 0.4/client 0.5; explicit endpoint allowlist/default deny; active response revocation; random persisted OS-locked identity; owned proxy shutdown; old references removed after patch comparison | Relay/NAT, partitions, anti-entropy, private gossip membership and resource quotas open |
| 6: dependencies/quality | Remove 22 unused declarations with source proof; patched compatible TLS/QUIC/compression/WASM/runtime releases; Clippy warnings denied; source-pinned JS differential fixtures; measured header scan workload | Every-dependency deep API/changelog review, maintained vendor replacement, security findings, udeps/geiger and representative performance suite open |
| 7: extensions | Capability matrix explicitly marks unfinished extensions | Linked JSON/range query/network multiplexing unsupported |

Specifications: BHTTP-04, Versions-05, Range-Patch-01 and Merge-Types-00 at the
[recorded upstream revision](../upstream-compatibility.md). The
[conformance matrix](../conformance.md) maps behaviors to actual tests.

## Commands actually run

- `cargo test --workspace --locked`: **356 passed**, 27 ignored across unit,
  integration and documentation tests. Includes copied vendor tests; it is not
  a count of first-party cases alone. Earlier runs found and fixed stale doctest
  imports and an incorrect genesis operation-count assertion.
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`:
  passed. This also checks examples and optional native/NFS/fuzzing features.
- `cargo check -p braid-core --no-default-features --locked`: passed.
- `cargo fmt --all -- --check`: passed after the final source formatting.
- `cargo machete`: passed after removing inactive reference source.
- `node scripts/run-upstream-conformance.mjs`: HTTP 2/2 and subscriptions 7/7
  passed against the pinned external runner. Reliable-update tests 14/15 still
  require a Simpleton controller; the previous recorded result is 13/15 and is
  not promoted to full conformance.
- `node tests/interoperability/codec.mjs`: four upstream JS-encoded fixtures and
  four Rust-encoded frames passed bidirectionally at braid-http SHA 639b811ec91e5d1aae4a4a7d162616b499f95b07.
  Checked-in Rust tests decode JS bytes at every split and run 2048 deterministic
  malformed-input mutations. The local Node 22.18 fallback warning concerns H2
  fetch; these differential tests use the pure parser and local Node HTTP server.
- `cargo check -p braid-http --locked --target wasm32-unknown-unknown
  --no-default-features --features wasm`: passed on the pinned Rust 1.99.0
  toolchain after installing its target. Browser behavior remains unverified.
- `cargo deny check`: licenses/bans/sources passed; **advisories failed**.
- `cargo audit --json`: **one vulnerability entry**, two unmaintained warnings;
  no remaining unsound/yanked warnings after compatible updates. The RSA entry
  is present in the optional SQLx dependency lock graph; `cargo tree --locked
  --invert rsa --target all` reports no active build path. No ignore was added.
  Unmaintained: paste (through Iroh netlink dependencies), smartstring (copied
  Diamond engine). The security workflow deliberately exposes these failures.

Reports under target/ are disposable and may be cleaned; earlier raw reports
are no longer present in the ignored build-output directory. The observations above were captured
from completed tool output, and current rechecks regenerate fresh reports. Committed baseline
hashes, dependency/provenance records and fixture bytes preserve the durable
review evidence. CI defines Windows/Linux full tests, quality/minimal profiles,
WASM compile, JS interoperability and a separate advisory workflow. Local passes
are not a claim that remote CI or Linux tests have already run.

## Safety and performance evidence

HTTP production code forbids unsafe and denies explicit unwrap/expect/panic via
Clippy. A lexical scan found no explicit unwrap/expect/panic/todo/unimplemented/
unsafe site in the maintained source sections preceding test modules. Its
classifier does not establish a complete AST/reachability proof; vendor assertions,
fallbacks and production items following test modules still require review.
Baseline lexical counts and their limitations are in baseline.md.

The same-binary debug parser comparison took 37,571,873 versus 85,565 microseconds
for 20 repetitions of an 8192-byte header in one-byte fragments. This is one
adversarial workload, not broad production speed or memory evidence. See
../../tests/benchmarks/README.md. No RSS/allocation/CRDT/blob-throughput/cold-build
improvement is claimed.

## Operations that still need care

- JSON replica and chat stores assume a single writer process per data root.
  Cross-process concurrent mutation/crash durability is not established.
- NFS is experimental. SETATTR returns NOTSUPP instead of acknowledging a no-op.
  Rename still needs persistent inode/descendant-path transaction/recovery; remove
  still needs synced tombstone policy. Do not treat its read/write support as a
  complete mounted filesystem guarantee.
- Simpleton current-frontier writes are safe to reject when stale; complete
  coordinated upstream Simpleton convergence remains unimplemented.
- Legacy chat snapshots preserve surviving messages/edits but cannot reconstruct
  missing original event payloads. Sync returns an explicit history error rather
  than inventing events; a full ChatCrdtState export including tombstones is
  needed for those rooms. Automatic snapshot fallback remains open.
- Blob cancellation can leave safe unreferenced/staging files. There is no automatic
  GC or complete recovery/inspection tool yet. Back up data before migrations.
- Public pages retain their prior exposure; authenticated chat/blob gates do not
  establish per-resource ACLs. Legacy convenience APIs that erase load errors and
  several app task lifetimes still need consolidation.
- Coverage-guided fuzzing of all trust boundaries, exhaustive process-kill/IO
  injection, independent model checking, browser runtime and relay tests are open.

Upgrade/rollback and API changes are in ../migration.md; no real data was touched.
Next checkpoint: implement the shared Simpleton GET/PUT synchronization channel
and its independent upstream controller, followed by persistent NFS lifecycle
transactions and the remaining crash/security tests.
