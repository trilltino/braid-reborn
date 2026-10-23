# Engineering refactor status, 2026-10-23

The full release acceptance matrix is not complete. This continuation implements
shared native synchronization, filesystem/NFS recovery and application/security
repairs. Unsupported features and unverified runtime environments remain explicit.

The prior implementation is 89a70ce1950d9a1ad5214cbd9161b714fdcaa368; the preceding
documentation checkpoint is 9bb7df92e996cecbe49b259d1264c147885a794d.
Both user-supplied root instructions remain unchanged and outside the commit.
No deployed data was accessed or migrated.

## Implemented boundaries

| Boundary | Result and executable evidence |
| --- | --- |
| Native reliable channel | One actor owns GET and serial PUT; exact immutable identities survive lost ACKs; bounded queues/journal/timeout; joint cancellation/retry; fatal parser/auth/causal errors stop with retained state. reliable_channel tests ownership, cursor commit, queue limits, timeout and actual killed-writer replay. |
| Reconnect frontier | Uses the oldest unacknowledged PUT's parents instead of an optimistic version the server may never have seen. The partitioned NFS regression reproduced HTTP 409 before this repair. |
| Production Simpleton client | Pinned upstream parent/scalar algorithm, stable peer/counter, offline pending text, digest checks and revision comparison. The independent text controller invokes this Rust client directly. The Rust server still rejects stale positional mutations. |
| Filesystem ownership | Per-URL shared client, bounded owned debouncer workers, bounded cancellation-safe scanner, owned shutdown; journaled text before projection; separate optimistic views and acknowledged versions; declared-base merge plus revision check. |
| NFS storage | Separate adapter/backing/operation service; blocking worker serialization; cross-process filesystem lease; binary partial/sparse IO, shell offsets, staged SETATTR, monotonic inodes, persistent rename/descendant mapping and remove recovery. Unsupported ownership/platform requests return NOTSUPP. |
| NFS remote operations | Capture bytes before local namespace mutation, recover prepared transactions, publish text destination before empty source, persist local tombstones, retain immutable snapshots and monotonic operation IDs. Integration covers partition, later local edit, restart, lost ACK, replay and delete. Local success and remote confirmation are distinct. |
| JSON writers | Replica and room stores use kernel leases; stateless in-process replica handles share their owner/broadcasts. Another process cannot create an independent stale writer. |
| Auth/database | Auth and friends share a bounded SQLite pool; WAL/FULL/foreign keys/busy timeout; DB validation observes cross-manager logout/deactivation; bcrypt runs on bounded blocking workers. auth_lifecycle reproduced the stale session cache before its removal. |
| Dependencies | Direct SQLite driver/core replace inactive SQLx backends; maintained compact_str and a documented manifest-only netlink macro patch remove unmaintained dependencies; Tokio 1.54.1 and coupled runtime graph are locked. No advisory ignore added. |
| Reproducible tooling | Eight workspace packages, refreshed direct-edge inventory, Windows/Linux all-feature quality/tests, WASM compile, all four pinned external suites, coverage-guided wire/text targets and privileged Linux NFS mount CI. |

Protocol pins and normative/conventional distinctions are in
[upstream compatibility](../upstream-compatibility.md); exact public/on-disk
changes and rollback requirements are in [migration](../migration.md).

## Validation evidence

Focused core validation completed with 198 passing unit tests, 20 ignored copied
engine diagnostic/long-running tests, and the shared-channel filesystem/NFS
integration passing. The new regression was first observed failing on optimistic
reconnect and passed after the repair. Auth revocation and JSON writer tests were
also run during implementation. These intermediate observations do not substitute
for the final workspace gate recorded below.

Completed local gates:

| Command | Observed result |
| --- | --- |
| `cargo test --workspace --all-features --locked` | 374 passed, 0 failed, 28 ignored (includes copied diagnostics/long-running tests and one subprocess helper) |
| `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings` | Passed; an initial needless-borrow finding was repaired and the gate rerun |
| Minimal `cargo check --no-default-features --locked` for core, HTTP and Iroh | All three passed |
| HTTP `wasm32-unknown-unknown`, no defaults, `wasm` feature | Passed on Rust 1.99.0 |
| `cargo fmt --all -- --check` and `cargo machete crates tests/fuzz` | Passed |
| `cargo audit --json`, `cargo deny check` | Passed, zero vulnerabilities and no advisory warnings; duplicate-version notices remain informational |
| `cargo audit --file tests/fuzz/Cargo.lock --json` | Passed, zero vulnerabilities/warnings |
| Independent pinned upstream runner | HTTP 2/2, subscriptions 7/7, reliable updates 15/15, Simpleton 12/12 |
| Bidirectional JS codec runner | Four JS-to-Rust fixtures and four Rust-to-JS frames passed |

Final focused reruns cover changes after the full-workspace build, including
encoded URL round trips, absent-path canonicalization, NFS storage/recovery and
the reconnect parent assertion. Raw reports are under
`target/engineering/continuation/`; generated reports are ignored artifacts.
The independent npm harness is test tooling outside the shipped Rust graph.
Its initial audit reported four dependent entries for one send XSS advisory.
A compatible send 0.19.0 override and the full committed npm lock remove those
findings; npm audit reports zero vulnerabilities. The locked harness rerun
preserves the four suite results above. Local Node 22.18
uses the upstream built-in-fetch fallback because its optional undici requires
22.19; CI installs the current Node 22 series. No H2 multiplex claim follows
from these HTTP/1.1 interoperability results.

Udeps was run with child-only RUSTC_BOOTSTRAP=1. It found unused server tokio-test
(removed), plus target-only libc and WASM dependencies (moved to their target
sections after source review). Cargo outdated failed when its temporary copy
omitted the local netlink path patch. Cargo geiger's absolute-manifest attempt
timed out after 300 seconds; no unsafe-count result is claimed. Cargo machete is
corroborating source evidence rather than proof of dead public APIs.

The local WSL Ubuntu registration points at a missing ext4.vhdx and cannot boot
(ERROR_PATH_NOT_FOUND). It was not modified. Real Linux mounting and sanitizing
fuzz runs are assigned to their CI jobs; creating jobs is not a passing result.
WASM is compile checked, with browser runtime behavior still unverified.

## Remaining release work

- Browser runtime/backpressure/cancellation tests and shared browser-channel ownership.
- Iroh relay/NAT, partition repair and reliable anti-entropy scheduling; local
  authenticated H3 GET/PUT/209 and revocation have tests, not those broader claims.
- Full transforming Simpleton server interoperability, historical version GET,
  linked JSON/range query and deployed transport multiplexing remain unsupported.
- Exhaustive crash/cancellation/IO injection and physical power-loss testing,
  especially Windows directory durability. Kernel leases cover cooperating new
  binaries, not mixed-version writers or arbitrary external editors.
- Complete copied-engine provenance, legacy checkpoint corpus, malformed binary
  checkpoint ingestion campaign and an independent CRDT oracle. Copied assertions
  remain isolated; first-party lint restrictions do not prove the engine panic-free.
- Per-resource application ACLs, remaining app task lifetimes and complete legacy
  chat history recovery. Legacy surviving messages cannot fabricate missing events.
- Operational snapshot/staging inspection and safe GC/global storage quotas.
  NFS snapshots and blob history are deliberately retained; binary remote rename
  and ambiguous/cross-origin mapping errors keep their recovery records.
- Full public-API/function inventory follow-through, every-dependency deep source
  review and representative allocations/RSS/throughput/cold-build benchmarks.
  The earlier measured header scan improvement is one adversarial microbenchmark,
  not a broad application performance claim.

The conformance matrix separates these gaps from the implemented subset. Do not
promote this status to full completion until the corresponding evidence exists.

## Remote CI follow-up

The first pushed continuation (7e198585cdefb1773d798dd8957086b90639b652)
passed GitHub security, WASM and the Linux native workspace gate. Its Linux
mount test exposed directory listing loss; inspection also found that registry
nfsserve 0.10.2 ignores plain READDIR cookies and underflows tiny directory byte
budgets. A new independent TCP RPC test reproduces repeated pages on the original
library. The narrow archive-pinned patch fixes cookie forwarding and returns
NFS3ERR_TOOSMALL for both zero-count directory procedures. Registry 0.11.0 was
compared and retains these bugs. Adapter-only cookie tests could not detect a
handler discarding the cookie before the call.

The first fuzz job also used an unavailable dated action ref; the date is now
passed as the toolchain input to the action. Cached upstream source metadata
lacked its pinned tree in CI, so both scripts now refetch the exact pinned
revision before checkout. These failures are retained as evidence rather than
being reclassified as passing jobs; the follow-up push reruns the repaired gates.
