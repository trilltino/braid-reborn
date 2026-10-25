# Migration and rollback

Stop writers and make a byte-for-byte backup of the configured BRAID_ROOT before
upgrading a deployment. Implementation tests use temporary data only. The local
administration tool mutates only its explicitly supplied policy directory.
Exercise the upgrade against a copy and compare content, heads, graph and history.
Keep the old executable, Cargo.lock and backup together. Roll back by stopping the
new executable and restoring the whole backup; do not mix old metadata with newer
immutable payload references or checkpoints.

## Storage

Replica checkpoints add stable `version_ids` remote agent/sequence mappings and
retain legacy `versions` positions. Existing snapshots without checkpoints are
seeded from their actual content; only known head mappings are reconstructed.
Unknown historical bases fail explicitly. Test legacy documents before promotion.
JSON file formats remain additive; preservation of every historical external
checkpoint encoding has not been established.

Chat rooms add a default-empty `events` array. Old rooms with messages and no graph
are recovered without dropping messages. A missing graph can be reconstructed
from surviving message/edit metadata; missing complete update payloads cannot.
On the explicit legacy-history error, recover with a full ChatCrdtState
export including tombstones. An automatic protocol fallback still needs integration. Older binaries ignore additive JSON fields but cannot preserve the new
ledger on a subsequent write, so rollback requires the pre-upgrade backup.

Blob payloads remain under objects/<sha256> with SQLite meta/events tables. Legacy
encoded filenames remain readable and are validated against stored hashes/sizes.
Uploads and buffered reads default to 64 MiB. Large applications should configure
BlobUpload.max_bytes explicitly and use open/put_stream. No old immutable payload
is deleted implicitly. A cancelled upload may leave a .upload-* staging file;
inspect references and stop writers before manual cleanup. No automated GC is
provided.

Filesystem `versions.json` adds default-empty `materialized` and `deleted` maps;
existing acknowledged versions and legacy `outbox` edits are retained. Legacy
outbox requests replay before a shared channel starts. New private per-URL
journals under `.braidfs/channels/<sha256(url)>.json` contain resource binding,
frontiers, immutable PUTs and complete Simpleton application state. Keep the
whole directory with `versions.json`; deleting a journal can destroy the only
unacknowledged edit. A missing acknowledged text base fails closed for repair.

NFS adds `nfs_sequence` and `nfs_changes` tables plus additive `plan`/`last_error`
columns. Existing inode IDs survive. Inodes are never reused after unlink. Rename
and remove preserve private snapshots under `.braidfs/nfs-ops/<id>/`; unlink also
moves bytes to `.braidfs/nfs-trash/<id>`. No automatic deletion/GC runs. Interrupted
staging is marked failed before mutation; prepared namespace changes finish on
NFS startup. Old pending changes without captured content become explicit
`repair_required` records. Inspect the saved paths/bytes before manual resolution.

For configured text resources, unlink is an empty Simpleton replacement plus a
local namespace tombstone; rename publishes the destination before emptying the
source. Neither operation promises an atomic transaction across remote URLs.
Remote updates to a removed local path remain journaled without recreating it;
explicit local recreation clears the tombstone through a new durable edit.
Binary remote changes and ambiguous/cross-origin mappings require explicit
configuration/repair. NFS local success means the local intention is durable;
only `remote_confirmed` means all its text requests received ACKs.

Existing encoded URL-segment filenames are preserved. Reverse mapping no longer
encodes percent signs twice, and deleted paths resolve their existing parent
prefix (including Windows extended path prefixes). Two enabled URLs mapping to
one local pathname now fail explicitly; watchers prefer the configured origin
instead of guessing another scheme. Unrepresentable filenames return an error
rather than silently uploading to a different URL.

New stores reject a competing writer process. Stop stale instances before
restarting; kernel locks release on process death and lock files need no manual
removal. Old binaries do not enforce these leases, so mixed-version concurrent
writers remain unsupported.

## Public Rust/configuration changes

### Release continuation

`protocol::parse_heartbeat_duration` is the shared checked duration API. The
deprecated integer `protocol::parse_heartbeat` rounds positive fractions upward;
`500ms` is no longer zero. Server intervals are limited to 100ms through 24h.
Chat streams use the canonical codec, reject invalid headers before streaming,
and terminate on lag for parent-based reconnection. `Version` and `Subscribe`
cannot appear together, even for a false subscription value.

`ReplicaDocument::snapshot_at` / `ReplicaStore::snapshot_at` materialize retained
historical frontiers. `causal_history` returns original events between ordered
causal boundaries, capped at 1024 events / 32 MiB of serialized events per call.
Finite history GET is encoded as a sequence of update frames. New ReplicaError
variants distinguish unavailable history (HTTP 432) and response limits (413);
downstream exhaustive matches must handle these variants. Legacy snapshot-only
documents remain readable at their current frontier; absent history is not invented.

The compatibility ConflictResolver keeps its entry point and JSON batch syntax,
but validates all operations before publishing one candidate. Invalid batches,
unknown/stale bases and reused identities fail without partial publication. Engine
deltas from its legacy `get_history` method carry the explicit
`application/vnd.braid.legacy-operations+json` media type and no fabricated Braid
event ID; use ReplicaStore for causal event replay. The unmeasured merge-quality
field is no longer emitted from structured batch responses.

- BraidClient construction is fallible: use `BraidClient::new()?`; its panic-prone
  Default implementation was removed.
- RuntimeFuture is target-specific: Send on native, local on WASM. Unsafe future
  wrappers were removed. Core re-exports the canonical HTTP runtime trait.
- ParserLimits adds header-field and header-value limits; use `..Default::default()`
  in struct literals. BraidError adds ResourceLimit (the enum is non-exhaustive).
- BraidNfsBackend::new returns Result and fails on corrupt inode metadata.
  `with_root` supports embedded/test instances without global environment changes.
- BraidNfsBackend::open accepts just the root, inode DB and blob store; the
  standalone binary no longer loads/writes daemon configuration or creates dummy
  sync clients. Existing daemon-aware constructors delegate to this boundary.
- ReplicaStore::new is now fallible and resolves an absolute path; use `?`.
- DaemonState adds the shared `text_sessions` registry; initialize it with Default.
- New SimpletonClient and ReliableChannel APIs retain offline edits durably.
  Use status/ACK completion to report synchronization, rather than enqueue success.
- AuthManager no longer relies on a process-local session-validity cache. Friends
  can use `with_pool` to share its pool; app startup uses that path.
- Chat import/export and batch merge/sync APIs return Result. AddMessage patches
  include defaulted reply_to/blob_refs. A batch requires valid causal parents and
  matching author; stale conflicting mutations are rejected without publication.
- Chat PUT handlers receive authenticated Ctx. Sender identity comes from the
  session, never x-user. Catch-all chat and blob routes now require credentials.
  Public page routes retain their existing behavior; per-resource ACLs remain open.
- BraidIrohConfig adds storage_dir and AccessPolicy. Use `..Default::default()` and
  authorize peer IDs explicitly with allow_peer. The default accepts no peers.
  Public policy is an explicit deployment choice. Revocation closes active streams.
- A proxy port in spawn_node requires BRAID_PROXY_PEER; None disables the proxy.
  Persisted random endpoint keys are generated under keys/ with an OS lock.
  Corrupt keys return an error rather than replacing identity.
- Iroh/Gossip/H3 registry releases replace the old path dependencies. Application
  packages remain under crates/. Third-party source provenance is documented in
  upstream-compatibility.md. The unused upstream index trace generator feature
  gen_test_data was removed; it depended on absent private instrumentation.
  Experimental vendor storage is Unix-only and is not the replica persistence path.

Do not run legacy publishing scripts as a migration tool. Refer to conformance.md
and audit/engineering-status.md for the actual tested subset and remaining risks.

## Transforming Simpleton profile

HTTP/H3 Simpleton writes carrying a valid Peer use historical-parent transformation.
The direct commit API and writes without Peer keep the conservative current-base
contract. Existing caller signatures and persisted event identities are unchanged.
All Simpleton subscriptions produce positional updates relative to that subscriber's
view. Own-peer echoes are suppressed; concurrent corrections wait for the peer's
quiet interval. Finite Parents history GET transfers original causal events instead.
A lagged subscription terminates so its owner can reconnect and repair.

Chat historical GET reconstructs retained structured events and returns the selected
Version and the current frontier separately. Snapshot-only legacy rooms remain
readable at their current frontier. Missing original events return HTTP 432; edit
summaries are never converted into fabricated original event payloads.

Independent evidence uses braid-text 1b50ed357dcbbf59a1f4554690f69d0458bc967e and
braid-http 639b811ec91e5d1aae4a4a7d162616b499f95b07. Run
node tests/interoperability/simpleton-server.mjs after building the
simpleton_interop_server example. The test forces same-base Unicode edits between
the pinned JavaScript client and the production Rust client, loses one accepted
PUT acknowledgment, verifies historical content, and checks empty replacement.
This does not close the browser runtime, partition repair or provenance gates.

Resource policies and their local assignment commands are documented in
[resource access](resource-access.md). Existing ownerless data now denies network
access until an explicit local assignment; names and old created_by fields never
establish ownership. Back up the whole data root before enabling writers.

Daemon configuration now has schema version 2 with origin-scoped credentials.
The format-only migration/rollback commands, original-byte backup, unknown-field
preservation, live-daemon exclusion and status redaction are documented in
[origin credentials](origin-credentials.md). The app settings bridge requires
explicit owner management of the control/daemon policy. Legacy cookie routes
remain HTTPS/default-port adapters and do not grant ordinary users control.
