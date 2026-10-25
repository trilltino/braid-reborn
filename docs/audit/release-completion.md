# Release completion registry

Starting revision: a10620eea582dc4d68f6b752ba480e16893ed911. Programme checkpoint
under repair: 22af1e5. The two original root documents (hashes in
[release-inputs.json](release-inputs.json)) and
[the completion evaluation](completion-evaluation-2026-10-24.md) are the inputs.
No deployed application data is used for implementation or migration testing.

**A requirement closes only with executable evidence.** An unfinished row stays
unfinished even when unrelated CI is green. Missing provenance, compatibility
inputs or runtime evidence stays a named blocker and prevents full sign-off.
Process-kill tests are never presented as physical power-loss proof.

## Source audit and continuation, 25 October 2026

The shared-contract implementation is committed in `924043b` and `9f5e2d4`.
The recorded Gate 1 run below covers 444 passing tests; it is historical evidence,
not a new check of subsequent source changes. `80d769e` identifies requirements;
`d798f84` and `f89c63e` add provenance measurements and an independent engine oracle.

The current instruction is to continue editing without Cargo commands or builds
while the user runs `cargo clean`. New source and test definitions remain
**Prepared**, with compilation, formatting and runtime gates pending. An earlier
attempt interrupted for the clean is not a failed product test or a passing gate.

| Area | Source already present | Current disposition |
| --- | --- | --- |
| Four defects, history, ACLs, credentials | Committed before `22af1e5` | Retain recorded regressions |
| Shared representations, paging, causal ingress, ACK/payload and text recovery | `924043b`, `9f5e2d4` | Gate 1 recorded; legacy-baseline proof tightened in current source, not rerun |
| Browser journal, Web Locks and owned Fetch | `completion/browser` worktree | Prepared; harness/runtime evidence still pending |
| H3 transport and streaming proxy | `completion/reliability` worktree | Prepared; replication ownership/partition gates remain open |
| Frozen extension fixtures, linked JSON, finite queries and authorized graph adapter | `completion/protocol` worktree | Prepared; deployed multiplex and interoperability gates remain open |
| File fault injection and SQLite contention | Current source/test definitions | Prepared; no execution under the no-build instruction |
| Engine comparison and independent oracle | `d798f84`, `f89c63e` | Partial; original-copy attribution and historical project corpus remain open |

What is left to do, in priority order, is in [remaining-work.md](remaining-work.md).

## How to read a row

| Status | Meaning |
| --- | --- |
| **Evidenced** | Implemented, and the named test passed in the recorded run on the recorded revision |
| **Claimed** | An earlier checkpoint asserts closure and the suite still passes, but this registry has not yet re-mapped the item to a named test. Re-verification is a release-audit task |
| **Partial** | Part of the requirement is evidenced; the remainder is listed in *Remaining* |
| **Open** | Not implemented. *Blocker* names anything external |
| **Prepared** | Source/test definitions exist; compilation or the required behavioral gate has not run |

Owners: **INT** integrator (shared contracts, compatibility, provenance, CI,
release evidence), **BRW** browser agent, **REL** reliability agent, **PRO**
protocol agent. Evidence names a test file or script under the repository root;
a `::` selects a test function.

## Baseline (fresh, before this programme's changes)

| Item | Result |
| --- | --- |
| Revision / platform | `22af1e5` checked out in a clean worktree; Windows 11 x86_64 MSVC; Rust 1.99.0; Cargo 1.99.0; Node 22.18.0 |
| `cargo test --workspace --all-features --locked --no-fail-fast` | **418 passed, 0 failed, 26 ignored** (raw log: `target/release-evidence/baseline-22af1e5-workspace.log`, not committed) |
| Release-profile measurements | **Not yet captured.** Required before any performance claim; tracked as `INT-PERF-*` below |

A first baseline attempt raced with in-progress edits and failed to compile
`braid-iroh`. It is not evidence and was discarded; the clean-worktree run above
replaces it.

## A. Original audit findings (F01–F15) and evaluation findings (E1–E4)

| ID | Requirement (source) | Evidence | Owner | Status | Remaining / blocker |
| --- | --- | --- | --- | --- | --- |
| F01 | Native PUT serializes `with_patches` (audit §1.1) | braid-http `engineering_wire`, `js_fixtures`; upstream codec runner | INT | Claimed | Map to named test at release audit |
| F02 | Offline edits are never overwritten by a server copy | core `audit_filesystem`, `audit_shared_channel`; `completion_recoverable_text::digest_failure_on_the_first_snapshot_repairs_without_overwriting_pending_text` | INT | Partial | Filesystem/NFS external-write races (REL-FS-*) |
| F03 | `Patches: N` framing in every handler | server `release_streams`; `js_fixtures`; pinned upstream runner | INT | Claimed | Real app-route interop for chat (PRO-MUX-*) |
| F04 | Reproducible build; `braid-common` provenance | Fresh-workspace builds; [dead-code.md](dead-code.md) | INT | Partial | Original helper source is unavailable. The agreed disposition is independently maintained first-party code, without a recovery claim; complete its dependency/API review |
| F05 | `Version`/`Parents`/`Current-Version` never mixed | core `release_regressions`, braid-http `js_fixtures` | INT | Claimed | |
| F06 | Diamond wrapper parent rules and Unicode indexes | core `engineering_causality`; `simpleton::tests` | INT | Claimed | Engine provenance blocker: INT-PROV-* |
| F07 | Durable causal history; reconnect replay | `release_regressions`, `audit_replica`; `completion_shared_contracts::causal_pages_*` (1,100 events) | INT | Partial | Legacy chat event recovery (INT-LEG-1) |
| F08 | Bounded incremental parser; lossless binary | braid-http `engineering_wire`; fuzz target `wire` | INT | Claimed | Extended fuzz targets (INT-FUZZ-*) |
| F09 | Iroh is a convergent replica, not one 209 JSON blob | `braid-iroh/tests/replication_history.rs` (authenticated paged ingestion); `transport.rs` | REL | Partial | `ReplicationManager`, reconciliation schedule, relay-only (REL-REP-*) |
| F10 | One owner for subscription, PUT queue, ACK, cursor | braid-http `reliable_channel` (11), `application_acknowledgments` (9) | INT | Evidenced (native) | Browser transport (BRW-*) |
| F11 | Atomic saves; per-page serialization | server `engineering_storage`; core `audit_replica` | INT | Claimed | Fault injection at every boundary (INT-FAULT-*) |
| F12 | WASM network is real | `cargo check` wasm only | BRW | **Open** | Browser transport and runtime tests (BRW-*). Runtime engines are installed locally (Playwright Chromium/Firefox/WebKit) |
| F13 | HTTP/2/3 pooling; multiplex key includes port | `multiplex` unit coverage | PRO | Partial | Deployed multiplex owner (PRO-MUX-*) |
| F14 | Blob file+metadata transaction; conflicts retained | `braid-blob/tests/streaming` | REL | Partial | Binary conflicts/outbox (REL-BIN-*) |
| F15 | Fuzz, fault-injection, CI, upstream Braid-Fuzz | CI workflows; upstream runner (HTTP 2/2, subs 7/7, reliable 15/15, Simpleton 12/12) | INT | Partial | Fault injection, extended fuzz, seeded schedules (INT-*) |
| E1 | Heartbeat duration never panics | server `release_heartbeat`, `release_streams` | INT | Evidenced | |
| E2 | Legacy resolver is atomic | core `release_regressions` | INT | Evidenced | |
| E3 | Historical GET returns the requested version or 432 | core `release_regressions` | INT | Evidenced | |
| E4 | `Subscribe`+`Version` rejected | core `release_regressions` | INT | Evidenced | |

## B. Original acceptance criteria

| ID | Criterion (refactor prompt, *Exact release acceptance criteria*) | Status | Where it is tracked |
| --- | --- | --- | --- |
| P1 | Raw PUT patch bytes cross-checked with JS | Claimed | F01, `tests/interoperability/codec.mjs` |
| P2 | Every handler decodes its own frames | Partial | F03; chat app routes (PRO) |
| P3 | Version/Parents/Current-Version retain every head | Claimed | F05 |
| P4 | No self-parent, no unknown-parent root fallback | Claimed | F06 |
| P5 | Empty snapshot, Unicode, binary, multipatch | Claimed | `engineering_wire`, `simpleton::tests` |
| P6 | History/reconnect for advertised merges or surfaced unsupported | Partial | F07, G-06 |
| P7 | Status, heartbeat, framing, cancel, backpressure | Partial | F10; browser cancel (BRW-FETCH-*) |
| P8 | Independent JS conformance with pinned revisions and captures | Evidenced | `scripts/run-upstream-conformance.mjs` |
| D1 | Dirty local edits never overwritten | Partial | F02 |
| D2 | Cursor persisted only after durable apply | Evidenced | `reliable_channel::cursor_advances_only_after_application_commit…` |
| D3 | Outbound edits survive restart under stable IDs | Evidenced | `reliable_channel`, `application_acknowledgments`, `completion_recoverable_text` |
| D4 | Multi-operation updates all-or-nothing | Claimed | E2, `audit_replica` |
| D5 | Crash/fault injection across history, blob/file, SQLite | **Open** | INT-FAULT-* |
| D6 | Path, secret, exhaustion, unauthenticated access blocked | Partial | `release_access`, `origin_*`; path confinement hardening REL-FS-* |
| D7 | Production panic/`unsafe` elimination | **Open** | INT-REVIEW-* |
| Q1 | Fresh checkout reproducible; `braid-common` provenance | Partial | F04; independently maintained helper, no claim of original-source recovery |
| Q2 | Crate directions and public APIs documented | Partial | [architecture.md](../architecture.md); API review INT-REVIEW-* |
| Q3 | Module/function boundaries | Open | INT-REVIEW-* |
| Q4 | Every direct dependency audited | Partial | [dependency-audit.md](../dependency-audit.md) |
| Q5 | Vendored code SHA, license, diff tracking | **Blocked** | INT-PROV-* (copied Diamond engine origin unestablished; H3-Axum 0.2.0 archive returned 403) |
| Q6 | Deletions with call-site proof | Partial | [dead-code.md](dead-code.md) |
| Q7 | CI covers targets/features/interop/fuzz/security/Iroh E2E | Partial | `.github/workflows/ci.yml` |
| Q8 | No new warnings, dead stubs or false README claims | Open | INT-REVIEW-* |
| Q9 | Performance report with baseline and correctness comparison | **Open** | INT-PERF-* |

## C. Gate 1 — shared contracts (must pass before downstream streams)

| ID | Requirement | Implementation | Executable evidence | Compatibility | Status |
| --- | --- | --- | --- | --- | --- |
| G-01 | Completion registry with identifiable requirements | this document | n/a (document) | replaces the prior table; checkpoint log retained below | Evidenced |
| G-02 | Fresh baseline before modification | clean worktree at `22af1e5` | 418/0/26, see *Baseline* | none | Evidenced |
| G-03 | `RepresentationAt { bytes, media_type, frontier }` and authorized historical reader | `braid_http::types::RepresentationAt`; `ReplicaStore::{representation_at, representation_at_authorized}` | `completion_shared_contracts::json_snapshots_keep_declared_type…`, `…::causal_ingestion_uses_real_authority…` | text `snapshot`/`snapshot_at` unchanged | Evidenced |
| G-04 | JSON resources: validated versioned snapshots, parent-checked writes, stale write conflicts | `replica/representation.rs`, `json-snapshot` profile (schema 1) | `json_snapshots_keep_declared_type…` (stale write, invalid JSON leaves bytes unchanged, profile mismatch refused) | text resources untouched; new profile only | Evidenced |
| G-05 | Declared media types persisted; JSON never inferred from legacy text | `merge_state.media_type`; creation without a type refused | `linked_json_is_explicit_and_invalid_creation_never_publishes` | legacy text keeps `text/plain; charset=utf-8` | Evidenced |
| G-06 | `history_page(resource, from, target, limits)`: original events, fixed target, causally closed `through`, completion; 128 events / 32 MiB default; oversized event fails; finite GET unchanged | `replica/history.rs`; `ReplicaStore::history_page[_authorized]` | `causal_pages_cross_the_finite_get_cap…` (1,100 events across pages, events stored in reversed order), `history_pages_reject_missing_events_and_never_emit_an_oversized_singleton` | `causal_history` and finite GET semantics unchanged | Evidenced |
| G-07 | Versioned internal H3 endpoint `POST /.well-known/braid/replication/v1/history` | `replica_http::history_authorized`, Iroh route | `braid-iroh/tests/replication_history::paged_history_preserves_original_identities_and_is_authorized` over real H3: 3 pages, 403 for transport-admitted stranger, 403 for missing resource, 400 malformed/namespace/over-limit | catch-all resource routes unchanged | Evidenced |
| G-08 | Detect legacy namespace collisions before enabling | `ReplicaStore::replication_namespace_collisions`, `BraidIrohNode::replication_history_enabled`; creation inside the namespace refused | `reserved_replication_namespace_blocks_new_resources…`, `legacy_namespace_collisions_disable_the_endpoint_without_modifying_data` (503, legacy file bytes unchanged) | existing colliding data is reported, never renamed, deleted or adopted | Evidenced |
| G-09 | Authorized causal ingestion preserving identities and parents, no fabricated `Peer` | `ReplicaStore::commit_causal_authorized` | `causal_ingestion_uses_real_authority…` (concurrent events from two principals, stored events byte-identical, no `peer` header) | additive | Evidenced |
| G-10 | Recoverable text state: verified base (text, frontier, digest) + optimistic state + pending intent + immutable outbox | `simpleton.rs` `VerifiedBase`, state schema 2 | `completion_recoverable_text` (7 tests): base advances only through the acknowledged edit's own digest; unacknowledged edit keeps the old base and replays an identical PUT after restart | schema field and base are `serde(default)`; older readers ignore them | Evidenced |
| G-11 | Opt-in application-owned acknowledgments: `ChannelEvent::PutAcknowledged`, `commit_ack` atomically advances state and removes the entry; generic callers keep automatic ack | `client/channel.rs`, `ChannelConfig::application_acknowledgments` (default off) | `application_acknowledgments` (failed ack persistence is retryable and does not poison the channel; restart replays the identical PUT; generic channels reject `commit_ack`); `reliable_channel` unchanged | `next()` and `queue()` unchanged | Evidenced |
| G-12 | Immutable payload references, streaming transport, `queue_payload`; `queue(Update)` stays the inline adapter | `client/payload.rs`, `BraidNetwork::put_stream` (default buffering adapter; native reqwest streaming override), `ReliableChannel::queue_payload` | `application_acknowledgments::payloads_stream_by_reference…`, `substituted_or_truncated_payloads_fail_closed…` (mock transport); native reqwest path: `native_put_stream` | journals without payloads round-trip byte-for-byte (`journals_without_payload_references…`); `BraidError::Payload` is a new variant | Evidenced (see note) |
| G-13 | Diagnostics: state, pending count, receive frontier, retry reason, repair cause | `ChannelDiagnostics`, `ReliableChannel::diagnostics`, `SimpletonClient::diagnostics` | `diagnostics_name_retry_reasons…`, `substituted_or_truncated_payloads…` (repair cause `Payload`) | `status()` unchanged | Evidenced |
| G-14 | Back up original journal bytes before migrating; recover legacy baseline by materializing the frontier and replaying unchanged queue; otherwise retain original and report `NeedsLegacyBaseline` | `ChannelJournal::backup_original`, `FileJournal` `.bak` (first backup is never replaced), `Driver::recover_legacy_baseline` | `legacy_journal_without_a_queue_gets_a_verified_base_and_a_byte_exact_backup`, `legacy_queue_is_recovered_by_materializing…`, `unverifiable_legacy_baseline_is_reported_and_the_original_is_retained` | legacy peer identity and queued operation bytes unchanged | Evidenced |
| G-15 | Pending-preserving repair of digest failures | `Driver::repair` | `digest_failure_on_the_first_snapshot_repairs_without_overwriting_pending_text`, `digest_failure_with_queued_edits_rebuilds_from_the_base_and_keeps_every_edit` | none | Evidenced |

Note on G-12: `NativeNetwork::put_stream` is exercised by `native_put_stream`
(see the checkpoint log). The default adapter used by other transports buffers
and is bounded at 32 MiB.

## D. Parallel streams (open until each gate passes)

### Browser stream (BRW)

| ID | Requirement | Owner | Status | Remaining / blocker |
| --- | --- | --- | --- | --- |
| BRW-CAP-1 | `ChannelJournal::open_browser` over IndexedDB with exclusive Web Locks; records bound to canonical URL and stable authorization context; credentials never persisted | BRW | Open | Local Chromium/Firefox/WebKit available through Playwright |
| BRW-CAP-2 | Versioned browser envelope, 32 MiB limit, ownership acquired before load, ACK only after transaction completion, quota/abort never publish a candidate | BRW | Open | |
| BRW-CAP-3 | Required facilities checked before identity generation or storage mutation; explicit errors for missing facilities and competing writers | BRW | Open | |
| BRW-FETCH-1 | Fetch owned with `AbortController` and stream readers; redirects fail; timeout/reconnect/disconnect/shutdown abort and cancel; bounded parsing and backpressure | BRW | Open | |
| BRW-CORS-1 | Application routes configure approved-origin CORS and explicit protocol-header exposure; exact-origin auth and same-origin cookies retained | BRW/INT | Open | |
| BRW-RT-1 | Production Simpleton/channel in a thin WASM harness; digest repair from verified state; never overwrite pending text | BRW | Open | Depends on G-10/G-15 (done) |
| BRW-GATE | Chromium, Firefox, WebKit: offline edit, reload, persistent-profile restart, lost ACK, Unicode concurrency, slow reader, cancellation, multi-tab exclusion, auth-context isolation, CORS, redirects, real IndexedDB quota/transaction failures | BRW | Open | |

### Reliability stream (REL)

| ID | Requirement | Owner | Status | Remaining / blocker |
| --- | --- | --- | --- | --- |
| REL-H3-1 | `IrohNetwork` over the shared codec/channel; TCP proxy streams H3 responses and propagates cancellation; startup examples use explicit configured resources, never public demo data | REL | Open | |
| REL-REP-1 | `ReplicationManager::{start, stop, status, shutdown}`; one worker per authenticated peer/resource relationship | REL | Open | Builds on G-07/G-09 |
| REL-REP-2 | Reconcile on startup, reconnect, local commit, valid gossip hint, every 30 s ±3 s; positional subscriptions are wakeups; limits 64 relationships / 4 repairs / 128 MiB / 256 missing-parent events (32 MiB); retry 500 ms–30 s | REL | Open | |
| REL-REP-3 | Persist before advancing progress; both partition branches; dedupe; retain uncertain outgoing identities; recheck ACLs; drain before router shutdown | REL | Open | |
| REL-BIN-1 | Durable binary events and outboxes referencing verified immutable payloads; explicit conflicts; validated durable receipt before ack advance | REL | Open | Uses `PayloadRef` (G-12) |
| REL-BIN-2 | Ordinary reads of unresolved conflicts return 409; historical Version reads return the exact branch; resolution parents every head; inspect/export/resolve commands | REL | Open | |
| REL-NFS-1 | Binary event identities and persisted phases; destination confirmed before source removal; later edits preserved; explicit cross-origin recovery | REL | Open | |
| REL-FS-1 | Watcher-overflow rescans, atomic-save handling, delete/recreate generations, external-write races | REL | Open | |
| REL-FS-2 | Confinement for symlinks, junctions, normalization collisions, hardlinks, path-swap races | REL | Open | Windows junction case already covered in `release_access` |
| REL-ADM-1 | Storage inspection, quota admission (aggregate quotas opt-in), mark-and-sweep GC with hashed dry-run plan, explicit apply revalidated under maintenance ownership; conservative retention for unrecoverable references | REL | Open | |
| REL-GATE | 2-/3-peer partitions, multi-page catch-up, missed gossip, relay-only, child-before-parent, duplicates, revocation, dropped ACKs, binary conflicts, restartable binary NFS, quota exhaustion, populated migration/rollback/GC fixtures | REL | Open | **Relay-only** needs a relay reachable without direct paths; **mounted Linux NFS** evidence exists only from GitHub CI (`e7fcd6c`); local WSL distribution must be verified bootable and privileged |

### Protocol stream (PRO)

| ID | Requirement | Owner | Status | Remaining / blocker |
| --- | --- | --- | --- | --- |
| PRO-FIX-1 | Freeze Linked JSON draft at spec `0541922135bea0d60ad6b539e8d1ec25c2527284`, range proposal `"rgcookj27z-512"`, multiplex JS `639b811ec91e5d1aae4a4a7d162616b499f95b07`; commit raw sources, hashes, independent fixtures | PRO | Open | Network access to fetch pinned sources |
| PRO-LJ-1 | `application/linked+json`, escaping, relative URI resolution, lossless unknown metadata; explicit resolution, same-origin default, allowlisted remote traversal | PRO | Open | Media-type plumbing exists (G-03/G-05) |
| PRO-LJ-2 | Authorize every traversal; caches isolated by principal, policy generation, resource, frontier | PRO | Open | |
| PRO-Q-1 | Finite field/index/slice/projection/wildcard/chained/recursive queries; depth 8 / 64 resources / 32 MiB; reject malformed, cyclic, unauthorized, unavailable, over-budget without partial success; per-resource versions only | PRO | Open | |
| PRO-MUX-1 | One shared multiplex connection owner for native and browser; negotiated `MULTIPLEX`, POST fallback, cancel, reconnect, cleanup against pinned JS | PRO | Open | |
| PRO-MUX-2 | Connections and request IDs bound to authenticated principals; every inner request authorized; atomic frame encoding; headers/body exactly once | PRO | Open | |
| PRO-MUX-3 | Fair per-stream scheduling; 64 streams / 1 MiB stream backlog / 32 MiB connection backlog / 64 KiB chunks; bounded connections; explicit overflow cancel | PRO | Open | |
| PRO-GATE | Independent wire fixtures and real routes: bidirectional interop, fragmented frames, duplicate IDs, cancel, reconnect, two-principal isolation, stalled consumer beside a progressing response | PRO | Open | |

### Integration, provenance and release evidence (INT)

| ID | Requirement | Owner | Status | Remaining / blocker |
| --- | --- | --- | --- | --- |
| INT-PROV-1 | Copied-engine source revisions, licenses, file hashes, reproducible patches; similarity to upstream is evidence to investigate, not proof | INT | **Partial / Blocked** | Measured in [engine-provenance.md](engine-provenance.md): closest sampled upstream revision `90227908`, 102 of 105 files edited (2,938 lines), no byte-identical tree, so a reproducible patch set against a pinned revision cannot be reconstructed. **License gap:** upstream is ISC and the copy carried no notice; [NOTICE.md](../../crates/braid-core/src/vendor/NOTICE.md) added, but the upstream copyright line is unavailable and must be obtained from the author |
| INT-PROV-2 | Preserved historical checkpoint corpus and separately pinned upstream oracle; compare decoding, checkout, migration, merges; freeze engine storage format until compatible | INT | **Partial** | Oracle built from upstream `90227908` (`tests/oracle`, `node scripts/run-engine-oracle.mjs`). `engine_oracle::copied_engine_agrees_with_upstream_on_seeded_schedules`: 360 scripted schedules (2/3/10 replicas, 32,400 operations, 7,230 merge points) agree on merged text, operation count, cross-decoding of each engine's encoding, and merge-order independence. **Remaining:** a preserved corpus of checkpoints written by earlier releases of this project, and migration comparison; engine storage format stays frozen |
| INT-LEG-1 | Recover available legacy chat events without inventing events or parents; snapshot-only boundaries explicit | INT | Open | |
| INT-REVIEW-1 | Dependency, feature, public-API, task-lifetime, panic, unsafe, dead-code reviews; removal only with caller and compatibility evidence | INT | Open | |
| INT-FUZZ-1 | Fuzz frontiers, query ranges, multiplex frames, paths, gossip/H3 inputs, checkpoints, remote ingestion | INT | Open | History request/response codecs are new fuzz surfaces |
| INT-FAULT-1 | Deterministic failure injection at every documented persistence boundary: disk full, permissions, SQLite busy, cancellation, subprocess termination, restart | INT | Prepared / partial | `braid-http::durable::fault`, core `failure_injection`, server `failure_injection_sqlite`; execution pending by user instruction. Broader blob, projection, migration, GC and cross-store boundaries remain open |
| INT-SIM-1 | Reproducible 2/3/10-replica schedules with saved seeds and an independent convergence oracle | INT | **Partial** | Engine level done: seeded SplitMix64 schedules, failures save the exact script under `target/oracle-failures/`, replay with `BRAID_ORACLE_SEED`; the upstream engine is the independent oracle and the test asserts the schedules contain real concurrency. **Remaining:** replica-store level schedules (partition/heal through the replication manager, REL-GATE), which need the replication stream; the test is `#[ignore]` by default because it needs the oracle binary and counts as ignored until run in CI |
| INT-PERF-1 | Release-build measurements: parsing, Unicode edits, CRDT history, 10,000-file scans, large blobs, fanout, partition recovery, cold start; record hardware, workload, latency distributions, throughput, allocations, peak memory, checkpoint and build size; equivalent workloads and correctness before any improvement claim | INT | **Open** | Baseline not yet captured |
| INT-REL-1 | Windows+Linux default/all-feature/isolated profiles, warnings-denied, fmt, WASM, preserved examples; upstream suites; three-browser runtime; real relay-only and mounted Linux NFS; populated backup/migration/rollback/privacy/quota/conflict/GC; docs tied to the exact candidate SHA | INT | Open | Depends on all streams |

Explicitly deferred by the user: cursor sharing, Webindex, Yjs, and graph-wide
live query subscriptions. Physical power loss is certified only against actual
filesystem/environment evidence.

## Evidence log (earlier checkpoints, preserved)

## First implementation checkpoint

- Fresh starting all-feature workspace run passed 375 tests, 28 ignored.
- The original source failed all four permanent defect probes. A valid second
  operation and false Subscribe selector were also confirmed failing.
- Repaired workspace run: 384 passed, 0 failed, 26 ignored. The ignored-count
  difference is removal of two obsolete ignored resolver documentation examples,
  not enabling or filtering out failing tests.
- Final focused run after creation/index and stream-race regressions: 12 passed.
- Final all-target/all-feature warnings-denied Clippy passed. Separate minimal
  core, HTTP and Iroh warnings-denied Clippy passed; HTTP WASM compile passed.
- Formatting and diff whitespace checks passed. Raw logs are under target/release-*.
- No data format migration or deployed-data mutation occurred. Dependency graph
  and lockfiles are unchanged. Remaining rows stay open.

## Authorization and feature checkpoint

- Default-feature workspace: 406 passed, 0 failed, 26 ignored. This includes the
  real Windows junction case in the production-router authorization test.
- All-feature workspace: 405 passed, 0 failed, 26 ignored before the final
  outbox-header test was added. The complete wire/channel suites then passed
  12 tests with that new case; one subprocess helper remains intentionally ignored.
- All 13 isolated supported profiles passed warnings-denied Clippy and their
  library tests. WASM compile and final all-target/all-feature Clippy passed.
- The first broad run timed out in existing shared-channel rename recovery.
  Its focused reruns and the subsequent full default/all-feature runs passed;
  the failed observation remains in target/release-acl-workspace.log.
- Common user/peer policies, application/H3 protection, attachment-reference
  validation, local inventory and resumable owner assignment are implemented.
  Policies have schema version 1, immutable creation IDs, grant revisions and
  pre-publication write generations. Rollback refuses changed or used policies.
- Original input hashes are in release-inputs.json; the user-supplied source
  documents remain untracked and unchanged. No deployed data was migrated.
- Per-origin daemon credentials, complete backup/legacy-secret migration, browser
  channels, anti-entropy, binary recovery, extensions and later release gates
  remain open. Local Windows evidence does not certify new Linux CI results.

## Origin credential checkpoint

- Full all-feature workspace: 415 passed, 0 failed, 26 ignored.
- Final all-target/all-feature warnings-denied Clippy passed. The affected HTTP
  minimal, core native and core fs profiles passed isolated Clippy/library tests.
  WASM compilation passed; browser runtime remains a separate open gate.
- Actual daemon IPC/HTTP fixture passed persistence-before-ACK, exact-origin
  routing, journal/status/log isolation, credential rotation and failure to
  publish a configuration candidate. The fixture is included in native CI.
- Production app-route tests deny daemon control to ownerless callers and editors,
  permit its explicit owner, preserve the cookie adapter and propagate pending
  outcomes as HTTP 202. App login headers are not copied into the local RPC.
- Config schema 2 adds bounded per-origin credentials. Original bytes and a
  versioned hash record support dry-run, resume and guarded format rollback;
  unknown fields survive and modified configurations block rollback.
- No dependency/lockfile version or engine storage format changed. Browser
  ownership/journals, Iroh repair, binary durability, extension deployment and
  remaining provenance/performance/chaos evidence are unfinished.

## Portable actor and journal checkpoint

- The channel state machine and production Simpleton owner compile natively and
  for WASM. Native public paths and stored checkpoint fields are preserved.
  `JournalBackend` owns atomic whole-record publication and exclusive writing;
  `FileJournal` adapts the existing bounded file store and kernel lease.
- Full all-feature workspace: 418 passed, 0 failed, 26 ignored. The native
  channel regression suite adds failed queue/cursor stores and failed durable
  acknowledgment removal followed by exact PUT replay after restart.
- Workspace all-target/all-feature Clippy passes with warnings denied. The HTTP
  minimal, core client and core native profiles pass isolated library checks and
  tests. Core WASM warnings-denied Clippy is included in CI.
- The final channel suite passes 11 tests (one process-only helper ignored).
  The independent pinned JavaScript/production Rust Simpleton fixture passes its
  five Unicode, lost-ACK, idempotency, historical-read and replacement scenarios.
- Native task ownership retains Send/Sync and abort/join; a missing Tokio runtime
  returns a capability error before network work. Browser futures remain local.
- No registry package, lockfile version or native journal/engine format changed.
  The original input-document hashes remain identical. Browser capabilities,
  IndexedDB transactions, Web Locks, Fetch cancellation and three-engine runtime
  scenarios still block this milestone's completion. H3 channel wiring is open.

## Gate 1 checkpoint — shared contracts

Windows 11 x86_64 MSVC, Rust 1.99.0, Cargo 1.99.0. The candidate SHA is the commit
that contains this section; it is recorded in the engineering status document.

| Check | Result |
| --- | --- |
| `cargo fmt --all -- --check` | passed |
| `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings` | passed |
| `cargo test --workspace --locked` (default features) | **444 passed, 0 failed, 26 ignored** |
| `cargo test --workspace --all-features --locked` | **444 passed, 0 failed, 26 ignored** |
| Isolated supported profiles (`scripts/check-feature-profiles.mjs`: 13 profiles, warnings-denied Clippy and library tests) | passed |
| `braid-core`, `braid-http`, `braid-iroh` `--no-default-features` checks | passed (provisional run, before the final test-only edit) |
| WASM: `braid-http` check; `braid-core` warnings-denied Clippy | passed (compile only; the browser runtime is BRW-GATE) |
| `braidfs-daemon` build; `tests/interoperability/credentials.mjs` | passed (provisional run, before the final test-only edit) |

Against the 418/26 baseline this adds 26 passing tests: 6 in
`completion_shared_contracts`, 9 in `application_acknowledgments`, 2 in
`native_put_stream`, 7 in `completion_recoverable_text` and 2 in
`braid-iroh/tests/replication_history`. No test was removed or ignored. The
recoverable-text and acknowledgment suites each ran 25 consecutive times without a
failure. Raw logs are under `target/release-evidence/gate1-final-*.log` (generated,
not committed).

Observed failures during the work, kept as evidence rather than hidden:

- A first baseline attempt raced with edits and failed to compile; it was discarded
  and replaced by the clean-worktree run.
- The first full Clippy run failed `large_enum_variant` on `ChannelEvent` (fixed by
  boxing the update) and a test-only `type_complexity`.
- Early recoverable-text tests failed because the fake server dropped the
  subscription when it rejected a PUT and could not hold a request in flight. The
  fixtures were wrong, not the product; no assertion was weakened.
- The native streaming test's sink assumed the client always sends a request head. A
  client that aborts on a failing source stream may send nothing, which is a
  stronger outcome than a truncated body; the assertion now accepts either.

Not rerun at this checkpoint: `scripts/run-upstream-conformance.mjs` and
`tests/interoperability/simpleton-server.mjs`, which need network access to fetch
pinned upstream sources. The channel and Simpleton changes are covered by the Rust
suites and by `release_simpleton_server` (Rust clients and observer), which passed. The
pinned-JavaScript comparison is not rerun here; re-running the JS suites is part of
the release gate.
