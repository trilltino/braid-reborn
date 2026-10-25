# Completion evaluation, 24 October 2026

**Verdict: substantial implementation completed; the full engineering refactor
and audit acceptance criteria are not complete.** Existing test and CI claims
are supported by executable evidence, but additional probes reproduce remaining
correctness defects. Completion should not be inferred from green CI alone.

Evaluated `master` at `a10620eea582dc4d68f6b752ba480e16893ed911`, against both
user-supplied root documents. Windows x86_64 MSVC; Rust 1.99.0; Cargo 1.99.0;
Node 22.18.0. The initial working tree contained only those two untracked input
documents. No application data was accessed or migrated. Implementation source,
manifests and lockfiles were not changed by this evaluation. Temporary probe
targets were removed after execution; this report is the only added project file.

## Reproduced findings

### E1 — P1: chat heartbeat input can panic the subscription task

Location: [braid_subscribe.rs](../../crates/server/src/chat/handlers/braid_subscribe.rs),
heartbeat parsing around line 145 and interval construction at line 219;
[headers.rs](../../crates/braid-http/src/protocol/headers.rs), `parse_heartbeat`.

`Heartbeats: 500ms` becomes zero seconds through integer division, then reaches
`tokio::time::interval(Duration::from_secs(0))`. Polling the response body panics
with `` `period` must be non-zero. `` The probe invoked the production chat
handler with temporary application state and observed a panicked Tokio task.
`Heartbeats: 0` reaches the same unchecked boundary by inspection.

This demonstrates a request-triggered subscription failure, not a proven
whole-process crash. Validate the duration before constructing the stream;
reject unsupported values or preserve a bounded, nonzero duration. Add an
actual stream-polling regression: checking response construction alone misses it.

### E2 — P1: the public legacy resolver still acknowledges partial application

Location: [conflict_resolver.rs](../../crates/braid-core/src/core/server/conflict_resolver.rs),
`handle_text_json` around line 225 and operation helpers around lines 265–340.

Start with `abc` at `seed`. Submit one Diamond update containing an insertion
of `X` at position zero followed by an invalid insertion at position 100.
`resolve_update` returns `Ok`, while committed in-memory content is `Xabc`.
The helpers discard individual `Result`s and do not stage the complete batch.
One identity is also reused across individual operations, so deduplication
is performed at the wrong boundary for a multi-operation request.

This API remains publicly re-exported. Source search found no current application
callers outside its tests, so this finding must not be generalized to the newer
`ReplicaStore::commit` path, whose atomic update tests pass. Nevertheless, the
engineering prompt explicitly included this older surface. Delegate it to a
single validated transaction or explicitly deprecate/restrict its guarantees;
propagating an error after partial mutation alone is insufficient.

### E3 — P1: historical GET returns a different version as a successful response

Location: [replica_http.rs](../../crates/braid-core/src/core/server/replica_http.rs),
`get`, especially its finite response around lines 86–90.

After storing `v1 = old`, followed by `v2 = new`, a GET with `Version: "v1"`
returns `200 OK`, `Version: "v2"`, and body `new`. The shared adapter never
uses the requested version. Pages v2 and Iroh delegate to this adapter.

Historical GET is documented as unsupported, but the wire fallback still needs
to obey the pinned profile. Braid HTTP draft-04 section 2.5 permits ignoring
an unsupported historical Version request **only without a Version response
header**. Implement lookup, explicitly reject unsupported requests, or implement
that fallback. The probe allows the specified fallback and still fails.
[Pinned draft, section 2.5](https://github.com/braid-org/braid-spec/blob/0541922135bea0d60ad6b539e8d1ec25c2527284/draft-toomim-httpbis-braid-http-04.txt).

### E4 — P2: the shared GET adapter accepts an invalid subscription combination

The same adapter returns `209` for `Subscribe: ?1` together with `Version: "v1"`.
Draft-04 section 2.5 requires Subscribe to be absent on a Version GET. Validate
this combination before opening a stream. The probe expects a 400 rejection;
the draft defines the invalid combination rather than prescribing that exact
server error status. Resume subscriptions through Parents.

### Additional source observations

- Chat still builds frames manually in `format_braid_update`, reduces some
  frontiers with `.first()`, fabricates `0@server`, and converts store/history
  errors to empty results. It is separate from the corrected shared adapter.
  Its snapshot is read before broadcast subscription is registered, and its
  stream matches only `Ok(update)` from the broadcast receiver, without the
  shared adapter's explicit lag-repair termination. These are source findings;
  this evaluation did not reproduce every associated concurrency schedule.
- Minimal-profile compilation emits an `unused_mut` warning at
  [node.rs](../../crates/braid-iroh/src/node.rs), line 143. All-feature Clippy
  passing does not establish warning-free compilation in every feature profile.
- The directory helper is now ordinary tracked source, fixing the orphaned
  gitlink build problem. Its original external source provenance is still
  unknown in `dead-code.md`. Copied Diamond engine provenance and legacy binary
  checkpoint coverage also remain incomplete in `upstream-compatibility.md`.

## Verification performed in this evaluation

| Check | Observed result |
| --- | --- |
| `cargo test --workspace --all-features --locked` | **375 passed, 0 failed, 28 ignored**, including doctests; run before adding temporary probes |
| `cargo check --workspace --all-targets --locked` | Passed |
| `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings` | Passed on the evaluated implementation, before temporary probes |
| `cargo fmt --all -- --check` | Passed |
| HTTP WASM compile, no defaults, `wasm` feature | Passed; browser runtime was not tested |
| Separate `cargo check -p <package> --no-default-features --locked` for core, HTTP and Iroh | All three passed; Iroh emits the warning above. Each package was checked independently after an initial combined check |
| Build both Rust upstream controllers | Passed |
| `node scripts/run-upstream-conformance.mjs` | HTTP **2/2**, subscriptions **7/7**, reliable updates **15/15**, Simpleton **12/12** |
| `node tests/interoperability/codec.mjs` | **4 JS-to-Rust fixtures and 4 Rust-to-JS frames** passed |
| Temporary focused probes | **4 failed assertions**, reproducing E1–E4; separate from the existing passing suite |

The upstream runner uses braid-fuzz
`b79c23f877d2e6807e3925bc0165a4da139a4458`, with its locked published protocol
dependencies. The bidirectional codec uses braid-http source
`639b811ec91e5d1aae4a4a7d162616b499f95b07`. Local Node uses the documented
built-in-fetch fallback because optional undici requires Node >=22.19. These
results do not certify deployed HTTP/2 multiplexing or all application routes.

GitHub's connected API independently confirmed success for all six jobs in
[conformance CI run 40005076872](https://github.com/trilltino/braid-reborn/actions/runs/40005076872):
Windows/Linux native, WASM, upstream, mounted Linux NFS, and fuzz smoke campaigns.
The upstream job log also confirms the four suite totals above.
[Security run 40003356386](https://github.com/trilltino/braid-reborn/actions/runs/40003356386)
completed successfully. Those are existing remote observations; this evaluation
did not rerun mounted Linux tests, sanitizer fuzzing or the full security workflow
locally. HEAD differs from the cited validated code commit `5ab2e55` only in
four documentation files, confirmed by Git diff.

Local raw logs and the isolated probe source copies are under ignored `target/`:
`evaluation-workspace-tests.log`, `evaluation-check.log`, `evaluation-clippy.log`,
`evaluation-wasm.log`, `evaluation-minimal*.log`, `evaluation-upstream.log`,
`evaluation-codec.log`, `evaluation-probes.rs`, `evaluation-probes.log`,
`evaluation-chat-probe.rs`, and `evaluation-chat-probe.log`.
PowerShell's log wrapper labels ordinary native stderr as `NativeCommandError`;
the command exit codes and test summaries were used to determine results.

## Completion against the requested work

| Requested area | Assessment |
| --- | --- |
| Reproducible workspace, toolchain, first-party CI | Implemented and verified; original helper provenance and an independently established MSRV remain unresolved |
| One bounded byte-preserving wire codec, actual PUT patch bytes, JS interop | Implemented and tested in shared paths; active chat framing and E3/E4 prevent whole-application sign-off |
| Diamond parents, Unicode, persisted causal history, duplicate/atomic commits | Strong regression evidence for the new replica; public compatibility resolver still fails E2; independent engine oracle/checkpoint campaign incomplete |
| Reliable native GET/PUT ownership and durable outbox | Implemented and independently exercised; browser/H3 ownership is separate |
| Filesystem/NFS recovery and local-edit preservation | Substantial tested implementation, including native lost-ACK/restart and remote Linux mount evidence; full crash/IO matrix and binary remote operations remain open |
| Blob streaming, integrity and transactional references | Implemented with focused tests; operational recovery inspection, quotas, retention/GC and broader fault testing remain open |
| Server/chat/auth integration | Auth and persistence regression coverage exists; active chat streaming defects, lifecycle work and per-resource ACLs remain |
| Iroh/Gossip/H3 modernization | Compatible modern stack and authenticated local H3 exchange verified; relay/NAT, missed-gossip repair and partition reconciliation remain unverified/incomplete |
| Dependency, dead-code and public-API audit | Extensive inventory and passing security CI; full dependency source review, complete public/function inventory and copied-engine provenance remain open |
| Safety/fuzz/chaos coverage | Meaningful tests and sanitizer smoke CI exist; E1/E2 and missing trust-boundary campaigns prevent a full safety claim |
| Performance acceptance | One adversarial parser microbenchmark exists; representative allocation/RSS/throughput, CRDT, blob, fanout and startup measurements are absent |
| Browser and optional extensions | Browser compile verified only; runtime tests open. Linked JSON, range query and deployed multiplexing explicitly unsupported/incomplete |

Most of the original F01–F15 defects have concrete repairs or narrower documented
boundaries. In particular, the missing build source, dropped native patch payload,
destructive text reconnect path, parser limits, durable replica/outbox, and local
H3 streaming now have real implementation and tests. The older audit's assessment
of the original snapshot should not be applied unchanged to this tree.

Conversely, the engineering prompt requires demonstrated acceptance, not just
documents or successful compilation. Its full completion cannot be signed off
while E1–E3, app-wide integration, provenance, fault coverage and measurement gaps
remain. Explicitly unsupported optional extensions need not be implemented merely
to remove that label; their scope should stay distinct from defects in active paths.

The existing [engineering status](engineering-status.md), especially its
remaining-release-work list, is substantially more current than
`docs/audits/AUDIT_IMPLEMENTATION_STATUS.md`. The historical file's 97-test and
13/15 reliability counts have been superseded. The current README already calls
the project an engineering checkpoint, which is consistent with this evaluation.
