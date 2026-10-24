# Release completion checklist

Starting revision: a10620eea582dc4d68f6b752ba480e16893ed911. The two original
root audit/refactor documents and completion-evaluation-2026-10-24.md are inputs.
An unchecked item is unfinished, even when unrelated CI passes. No deployed
application data is used for implementation or migration testing.

| Requirement / original scope | Implementation boundary | Required evidence | Status / compatibility |
| --- | --- | --- | --- |
| Baselines and four defect regressions (phases 0, 11, 13) | Workspace and release regression tests | Fresh default/all/minimal/WASM results; initially failing probes | In progress; retain original input documents |
| Checked heartbeat duration (E1) | Shared HTTP header parser and chat stream | release_heartbeat; release_streams, including actual stream polling | Implemented; integer API deprecated with upward rounding |
| Atomic compatibility resolver (E2; F07) | ResourceStateManager transaction | release_regressions: valid batch/retry/collision, bad second operation, rejected creation/index | Implemented; retain public adapter; engine deltas have explicit legacy media type |
| GET selectors and historical state (E3/E4; F05/F07) | ReplicaStore and shared HTTP/H3 adapter | release_regressions: historical branches, interval, 432, true/false Subscribe+Version rejection | Historical reads implemented; full transforming Simpleton/reconnect work remains pending |
| Canonical chat streaming (F03/F05/F08/F10) | Chat store replay/subscribe boundary and codec | release_streams: snapshot/subscribe overlap, lag, drop and invalid headers | Codec/race/lag repairs implemented; broader independent app-route interop and legacy history recovery remain pending |
| Transforming Simpleton (phase 3) | Shared Diamond replica and per-subscriber view | JS/Rust concurrent clients, Unicode, peer echo, restart/lost ACK | Pending; preserve queued identities |
| Resource ACL and migration (phases 5/6/8) | Common policy and all app/transport routes | Two users/peers; grants/revocation; blob references; legacy dry-run/rollback | Pending; private by default, ownerless data assigned locally |
| Browser/native/H3 channel (phase 4; F10/F12) | Portable worker, journal, transport | IndexedDB/Web Locks; reload/quota/cancel/slow read in three browsers | Pending; native API compatibility required |
| Iroh repair (phase 6; F09) | Owned peer/resource reconciliation | Partition, late join, missed gossip, relay, gap, revoke, restart | Pending; gossip never acknowledges commits |
| Binary/NFS recovery (phase 5; F02/F11/F14) | Binary outbox, immutable conflicts, namespace journal | Lost ACK, competing binaries, destination-before-tombstone, mounted NFS | Pending; retain both binary versions |
| Quotas, inspection and safe GC (phases 5/8) | Storage administration and retention roots | Disk full, interrupted GC/migration, roots in history/outbox/conflicts | Pending; dry-run default, no eviction of pending/acknowledged data |
| Linked JSON and range queries (phase 10) | Typed media/selector and authorized resolver | Pinned fixtures, versions, escaping, cycles/depth/resource/output budgets | Pending; finite queries, same-origin resolution default |
| Multiplex v1.0 (phase 10; F13) | Canonical mux codec and deployed connection owners | JS/native/browser, POST fallback, cancel/reconnect, slow consumer/isolation | Pending; distinguish H2/H3 multiplexing |
| Provenance and checkpoint compatibility (phases 3/7/8) | Isolated engine adapter and vendor records | Source/license/hash/diff; legacy corpus; independent oracle | Pending; missing provenance remains a blocker |
| Dependency/API/dead-code/task audit (phases 1/7/8/9) | Existing crate boundaries | Reviewed direct edges, public surfaces, safety and task ownership | Pending; deletion needs caller/migration evidence |
| Performance (phase 12) | Repeatable release workloads | Wire/Unicode/history/files/blobs/fanout/recovery/startup, allocations/RSS/latencies | Pending; no unmeasured claims |
| Release gate (phase 14 and exact acceptance list) | CI, conformance and migration documents | All scoped requirements evidenced; fault/fuzz/security/browser/relay/NFS | Pending; unavailable environments explicitly block claims |

Existing retained guarantees include the byte codec, durable native text queue,
transactional blob references, causal Diamond checkpoints, native regression
tests, and the existing independent suites. They must remain passing throughout.

Explicitly deferred by the user: cursor sharing, Webindex and Yjs. Graph-wide live
query subscriptions are outside the agreed finite-query profile. Physical power
loss is certified only against actual filesystem/environment evidence, never by
equating process termination with a power cut.

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
