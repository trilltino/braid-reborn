# Remaining work

Snapshot of 2026-10-25, written against `master` at `603349e`. This is the
working list for finishing the programme started from `22af1e5`. The authoritative,
row-by-row requirement list is the [completion registry](release-completion.md); this
document is the plan for what to do next and where things stand. When they disagree,
the registry's rows and their named evidence win.

**The release is not complete and must not be described as complete.** A requirement
closes only with executable evidence. Missing provenance, compatibility inputs or
runtime evidence stays a named blocker. Process-kill tests are never power-loss proof.

## 1. Where things stand

### Verified (a full gate was run and recorded)

The shared-contract gate passed on this Windows machine before the later commits
below: `cargo test --workspace` in default and all-feature profiles gave **444 passed,
0 failed, 26 ignored** (baseline at `22af1e5` was 418/0/26), with `cargo fmt`,
warnings-denied Clippy over all targets and features, the 13 isolated feature
profiles, and both WASM checks passing. Details and the observed failures are in the
registry's *Gate 1 checkpoint*. That covers typed representations, versioned JSON
snapshots, bounded causal history paging, the authenticated replication-history
endpoint, causal ingestion, the verified Simpleton base, application-owned
acknowledgments, immutable payload references, diagnostics, and legacy-journal
migration with byte-exact backup.

Also run and passing individually (not part of the 444 figure):

| Item | Evidence |
| --- | --- |
| Copied engine vs upstream engine | `node scripts/run-engine-oracle.mjs 120`: 360 scripted schedules (2/3/10 replicas), 32,400 operations, 7,230 merge points; text, operation count, cross-decoding and merge order all agree. The test is `#[ignore]` by default (needs the oracle binary) and is wired into the `upstream` CI job |
| Failure injection | `crates/braid-core/tests/failure_injection.rs`: a fault at every step of the atomic-write path for replica documents, resource policies and channel journals (disk full, permission denied, simulated crash, cancellation), plus 12 seeded subprocess kills; `crates/server/tests/failure_injection_sqlite.rs`: a held SQLite write lock |
| Engine provenance | [engine-provenance.md](engine-provenance.md): closest upstream revision and per-file differences measured |

### Present on `master` but NOT verified by me

Ten commits authored "Codex" landed on `master` between the gate and this
snapshot, interleaved with a cherry-picked browser WIP snapshot. They include: file-publication
fault plans, a durable sender channel for causal receive paths, a legacy-baseline
frontier proof, browser origin policies and exposed protocol headers, browser journal
hardening, a WASM channel/route harness, and a registry audit. **None of them has had
a full gate run since they landed**, and the registry still marks every stream row
Open. Treat them as prepared source, not evidence. The first task below is to run the
gate over exactly what is on `master`.

### Unmerged stream branches (work exists, nothing integrated or gated)

| Branch | Commits beyond the gate | Content |
| --- | --- | --- |
| `completion/browser` | 4 (+ WIP) | browser journal hardening and write-once backups, WASM channel and route harness sources, bounded parser drains, redirect rejection and restart scenarios |
| `completion/protocol` | 2 (+ WIP) | frozen pinned sources with executable fixtures; authorized finite Linked JSON queries and version metadata |
| `completion/reliability` | 2 (+ WIP) | Iroh H3 streaming, idle cancellation and proxy lifecycle; durable sender channel |
| `completion/storage` | 1 | binary event history, explicit conflicts and validated receipts |

The four stream agents were stopped by a session rate limit with uncommitted edits in
their worktrees (`../braid-reborn-{browser,protocol,reliability,storage}`). Inspect
those before anything is cleaned up.

## 2. Housekeeping before more feature work

1. **Resolve the stalled cherry-pick on the main checkout.** `master` is mid
   cherry-pick of `44d2e76` (protocol fixtures) with a modify/delete conflict on
   `tests/protocol-fixtures/extension-sources.json`. Whoever started it should finish
   or `--abort` it; the branch's own history already contains the commit.
2. **One writer per branch.** Codex sessions and Claude sessions have both committed to
   `master` and to the stream branches. Pick an owner for `master` and for each stream
   branch, and keep the others out, or merge conflicts and duplicated work will recur.
3. **Run the gate over `master` as it is** (section 5) and record the result in the
   registry before adding anything.
4. **Merge the stream branches** one at a time, each followed by a gate run. Shared
   manifests, `Cargo.lock`, the channel/journal contracts and `braid-http` traits stay
   with the integrator.
5. The two original input documents are untracked on purpose; their hashes are in
   [release-inputs.json](release-inputs.json). Do not stage or edit them.

## 3. Work remaining by stream

Row IDs refer to the [registry](release-completion.md).

### Browser (BRW-*)

All rows are Open on `master`; code exists on `completion/browser`.
- IndexedDB journal with exclusive Web Locks: ownership before load, acknowledgment only
  after `oncomplete`, quota and abort never publish a candidate (BRW-CAP-1/2).
- Capability checks before identity generation or storage mutation, explicit errors for
  missing facilities and competing writers (BRW-CAP-3).
- Fetch owned by `AbortController` and stream readers; redirects fail; timeout, reconnect,
  disconnect and shutdown cancel; bounded parsing and backpressure (BRW-FETCH-1).
- Approved-origin CORS and protocol-header exposure on the real application routes
  (BRW-CORS-1).
- Production Simpleton/channel in a WASM harness (BRW-RT-1).
- **Gate (BRW-GATE):** Chromium, Firefox and WebKit all passing offline editing, reload,
  persistent-profile restart, lost ACK, Unicode concurrency, slow readers, cancellation,
  multi-tab exclusion, authorization-context isolation, CORS, redirects, and real
  IndexedDB quota/transaction failures. The three Playwright engines are installed
  locally; nothing has been run against them yet.

### Reliability: replication, binary, filesystem, administration (REL-*)

- **Iroh (REL-H3-1, REL-REP-1..3):** `IrohNetwork` on the shared codec and channel; a
  streaming TCP proxy that propagates cancellation; `ReplicationManager::{start, stop,
  status, shutdown}` with one worker per authenticated peer/resource; reconcile on
  startup, reconnect, local commit, valid gossip hints and every 30 s ±3 s; default
  limits 64 relationships, 4 repairs, 128 MiB, 256 missing-parent events (32 MiB);
  persist before advancing progress; both partition branches; dedupe; retain uncertain
  outgoing identities; recheck ACLs; drain before router shutdown. Streaming/channel
  pieces exist on `completion/reliability`; the manager does not.
- **Binary (REL-BIN-1/2):** durable binary events and outboxes on verified payloads,
  explicit conflicts, 409 on ordinary reads of unresolved conflicts, exact-branch
  historical reads, resolution parenting every head, inspect/export/resolve commands.
  First slice on `completion/storage`.
- **NFS and filesystem (REL-NFS-1, REL-FS-1/2):** persisted phases and stable binary
  identities, destination confirmed before source removal, watcher-overflow rescans,
  atomic saves, delete/recreate generations, external-write races, and confinement for
  symlinks, junctions, normalization collisions, hardlinks and path-swap races.
- **Administration (REL-ADM-1):** storage inspection, opt-in aggregate quotas, and
  mark-and-sweep GC (hashed dry-run plan, explicit apply revalidated under maintenance
  ownership, conservative retention on unrecoverable references).
- **Gate (REL-GATE):** 2- and 3-peer partitions, multi-page catch-up, missed gossip,
  relay-only connectivity, child-before-parent delivery, duplicates, revocation, dropped
  ACKs, binary conflicts, restartable binary NFS, quota exhaustion, and populated
  migration/rollback/GC fixtures.

### Protocol (PRO-*)

- Frozen sources and fixtures are committed on `completion/protocol` (PRO-FIX-1) but
  not merged to `master`.
- Linked JSON (media type, escaping, relative resolution, lossless metadata, same-origin
  default, allowlisted remote traversal, per-traversal authorization, caches isolated by
  principal/generation/resource/frontier) and finite queries (depth 8, 64 resources,
  32 MiB; no partial success; per-resource versions) have a first implementation on the
  branch and no recorded gate (PRO-LJ-1/2, PRO-Q-1).
- **Multiplex (PRO-MUX-1..3): not started.** One shared connection owner for native and
  browser, negotiated `MULTIPLEX` and POST fallback, principal-bound connections and
  request IDs, atomic frame encoding, fair scheduling (64 streams, 1 MiB per stream,
  32 MiB per connection, 64 KiB chunks), explicit overflow cancellation.
- **Gate (PRO-GATE):** bidirectional interoperability with the pinned JavaScript
  implementation, fragmented frames, duplicate IDs, cancellation, reconnect,
  two-principal isolation, and a stalled consumer beside a progressing response.

### Integration, provenance and release evidence (INT-*)

- **INT-PROV-1 (blocked):** the copied engine's exact origin cannot be reconstructed; it
  differs from the closest upstream revision in 102 of 105 files. Upstream is ISC and the
  copy had no notice; [NOTICE.md](../../crates/braid-core/src/vendor/NOTICE.md) was added
  but the upstream copyright line must be obtained from the author.
- **INT-PROV-2:** build a preserved corpus of checkpoints written by earlier releases of
  this project and compare decoding, checkout, migration and merges against the pinned
  oracle. Engine storage format stays frozen until this passes.
- **INT-LEG-1:** recover available legacy chat events without inventing events or parents;
  mark snapshot-only boundaries explicitly.
- **INT-REVIEW-1:** dependency, feature, public-API, task-lifetime, panic, unsafe and
  dead-code reviews; remove code only with caller and compatibility evidence.
- **INT-FUZZ-1:** fuzz frontiers, query ranges, multiplex frames, paths, gossip/H3 inputs,
  checkpoints and remote ingestion. The new history request/response codecs are unfuzzed.
- **INT-FAULT-1 (partial):** extend injection to the blob store, filesystem projection,
  daemon configuration, migrations and GC; add disk-full against a bounded volume and
  real permission failures. Note that the registry row says execution was pending by
  instruction; the suites named above have since been run and pass.
- **INT-SIM-1 (partial):** engine-level schedules are done; replica-store-level
  partition/heal schedules need the replication manager. Keep saved seeds and an
  independent convergence oracle.
- **INT-PERF-1 (open):** release-build measurements for parsing, Unicode edits, CRDT
  history, 10,000-file scans, large blobs, fanout, partition recovery and cold start,
  recording hardware, workload, latency distributions, throughput, allocations, peak
  memory, checkpoint size and build size. Do this only on a quiet machine with no
  concurrent builds, and compare equivalent workloads and correctness before claiming
  any improvement. No baseline exists yet.
- Rows marked *Claimed* in the registry (F01, F03, F05, F06, F08, F11 and others) need
  their evidence mapped to named tests during the release audit.

## 4. Environment blockers

| Blocker | Effect | Needed |
| --- | --- | --- |
| WSL distribution `Ubuntu-24.04` cannot start: its disk image `D:\WSL\Ubuntu-24.04\rootfs\ext4.vhdx` does not exist and there is no D: drive | No local Linux, so no local mounted-NFS or Linux-only evidence; Docker Desktop depends on the same backend | Restore or reimport the distribution, or run the Linux gates on GitHub Actions |
| Mounted NFS evidence exists only from CI (`tests/chaos/nfs-mount.sh`, commit `e7fcd6c`) | Cannot be rerun for new binary/NFS behavior | Linux host with mount privileges; extend the script to cover binary phases |
| Relay-only connectivity | Needs a path that forces traffic through a relay | A reachable relay (local `iroh-relay` is a candidate) and a way to block direct paths |
| Network access for pinned upstream fetches | `scripts/run-upstream-conformance.mjs` and `tests/interoperability/simpleton-server.mjs` were not rerun at the gate | Network at release time |
| Single disk with about 70 GB free | Several concurrent debug builds are tight; one debug target is 4-8 GB | Keep builds to one per worktree and delete finished worktrees' targets |

Physical power loss can only be certified against actual filesystem/environment
evidence; nothing in this repository claims it.

## 5. Completion gate (unchanged)

Before sign-off, every one of these must pass on the exact candidate SHA, with the
documents updated to name that SHA:

- Windows and Linux: default, all-feature and isolated supported profiles; warnings-denied
  checks; formatting; WASM; preserved examples.
- All independent upstream suites and the three-browser runtime tests.
- Real relay-only and mounted Linux NFS evidence.
- Populated backup, migration, rollback, privacy-change, quota, conflict and GC tests.
- Updated architecture, conformance, migration, capability, dependency and
  engineering-status records tied to the candidate SHA.

Commands used at the last full gate:

```
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --locked
cargo test --workspace --all-features --locked
node scripts/check-feature-profiles.mjs
cargo check -p braid-http --locked --target wasm32-unknown-unknown --no-default-features --features wasm
cargo clippy -p braid-core --lib --locked --target wasm32-unknown-unknown --no-default-features --features wasm -- -D warnings
node scripts/run-engine-oracle.mjs 60
```

## 6. Deferred by decision

Cursor sharing, Webindex, Yjs, and graph-wide live query subscriptions are out of scope
for this programme.
