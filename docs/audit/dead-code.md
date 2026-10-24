# Removed and consolidated code

Release continuation: ConflictResolver's per-operation insert/delete helpers,
ad hoc merged-response builder and silent string extraction fallback were private
and only called inside that module. Their replacement is one ResourceStateManager
candidate transaction, retaining the public resolver interface. Regression tests
cover failed second operations, multiple valid operations, identity replay and
collision. Chat's private manual frame formatter is replaced by WireCodec-backed
format_update; existing message JSON payloads remain available.

Baseline: 4f5275b9ab83bc3cff1ef91ff068be6539edbfa2. Removal decisions combine
source search, manifests, feature edges and regression checks. The earlier audit
was not treated as a command to delete every mentioned module.

| Removed/merged surface | Evidence and replacement |
| --- | --- |
| references/iroh-gossip and references/iroh-h3 (78 files) | No remaining path dependency. Available old archives compared; source fixes already in selected releases. Hashes and comparison gaps in vendor-provenance.json; source recoverable at baseline SHA. Local authenticated H3 integration passes. |
| Duplicate core BraidRuntime/NativeRuntime | Re-export canonical braid-http trait/runtime; both native Send and browser local future types retained. |
| Unsafe WASM SendFuture / unchecked pinning | Local runtime future type removes need to assert Send. HTTP crate now forbids unsafe code. |
| BraidClient panic-prone Default | No workspace caller found; replace with fallible BraidClient::new. Public source change documented in migration.md. |
| ChatCrdt unused Diamond text engine field | No operations wrote it; export/import now handle actual structured message JSON with explicit errors. Chat causal graph/ledger preserved. |
| AI room-state placeholder fields | All fields were written only at registration, never read. Keep membership as HashSet. |
| AI append_to_markdown duplicate private function | No call sites; existing append_to_markdown_static remains in the actual response task. |
| gen_test_data feature | No app/example caller or README-supported workflow. Upstream ignored index trace generator used absent private instrumentation and failed all-features compilation. Copied source is retained as reference. |
| Duplicate scanner metadata.is_file branch | First empty branch made real file-change branch unreachable. Remove empty branch to restore file scanning. |
| Obsolete patch response assertion | application/braid-patch was an invented expectation. Replacement test decodes the actual numeric patch HTTP entity and checks selector/content bytes. |

## Manifest declaration removals

Every candidate below had zero Rust identifier references across its owning crate,
including examples, tests and copied modules, before deleting the declaration.
Associated feature edges were removed together. No public route was deleted. The vendor causal-storage test now uses a
temporary directory, avoiding a generated test.cg in the crate root.
Cargo machete passes after old reference source is removed; it is corroborating
evidence rather than the sole removal authority.

| Owner directory | Dependency removed | Observed Rust references |
| --- | --- | --- |
| braid-core | clap | 0 |
| braid-core | dirs | 0 |
| braid-core | once_cell | 0 |
| braid-core | regex | 0 |
| braid-core | reqwest | 0 |
| braid-core | tokio-stream | 0 |
| braid-core | tower-http | 0 |
| braid-core | tracing-subscriber | 0 |
| braid-http | once_cell | 0 |
| braid-http | percent-encoding | 0 |
| braid-http | regex | 0 |
| braid-iroh | clap | 0 |
| braid-iroh | thiserror | 0 |
| braid-iroh | tokio-stream | 0 |
| braid-iroh | tower | 0 |
| braid-iroh | tracing-subscriber | 0 |
| braid-iroh | uuid | 0 |
| braidfs-nfs | reqwest | 0 |
| braidfs-nfs | serde_json | 0 |
| server | async-trait | 0 |
| server | parking_lot | 0 |
| server | thiserror | 0 |

The standalone NFS binary now constructs the backing service directly from its
root, inode database and blob store. Its dummy daemon clients/merge/config/cache
state and now-unused async-channel declaration are removed. Public daemon-aware
backend constructors remain as compatibility delegates.

## Retained with known limits

- Multiplex parsing/protocol types remain; native transport negotiation is not
  represented as complete. No apparently redundant module was dropped just
  because it does not participate in the current deployed path.
- NFS adapter/backing/reconciliation now have separate ownership and regression
  seams. Supported SETATTR requests perform real staged changes; unsupported
  ownership/platform changes return NOTSUPP. Mounted Linux CI remains a separate
  acceptance gate.
- The copied Diamond engine and compatibility ResourceStateManager/ConflictResolver
  APIs remain. They still require provenance, public API and state-ownership work.
- AI, mail, drafts, presence and typing features remain. Their lifecycle/ownership,
  bounds and durability require separate verification; unused-looking placeholders
  were not treated as permission to remove product features.
- braid-common is the local directory helper used by the server. Original external
  implementation provenance is unknown; this work does not claim otherwise.

The method is lexical, not a proof about downstream consumers outside this
repository. Public source/API changes are listed in migration.md. A complete
function-size/dead-public-API audit remains open. Udeps found one additional
unused server dev dependency (tokio-test), now removed. Target-only libc (macOS)
and gloo/js/wasm dependencies were moved to their owning targets after source
review. Geiger did not complete within the recorded timeout; no unsafe count
is claimed from that attempt.

The maintained-code dependency scan is `cargo machete crates tests/fuzz`.
The narrowly patched registry NFS archive keeps its upstream manifests, including
optional demo declarations whose example files were not published in the archive.
Running machete recursively over vendor/ therefore reports those declarations;
this is not hidden by package ignore lists or by changing the library API.
Security/license/source gates still cover the complete resolved dependency graph.
