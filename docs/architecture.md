# Architecture and storage contracts

All application crates are members of the root workspace. Dependency direction:

```mermaid
flowchart TD
  server[LocalLink server] --> core[braid-core]
  daemon[braidfs-daemon / braidfs-nfs] --> core
  iroh[braid-iroh HTTP/3] --> core
  core --> http[braid-http: wire types, codec, client, durable file primitive]
  core --> blob[braid-blob: immutable payloads / SQLite references]
  blob --> http
  server --> common[braid-common: data directories]
```

`braid-http` owns Version, Update, Patch, request types and wire framing. Both
native and WASM HTTP paths use its encoder and incremental parser. A native
runtime future must be Send; browser runtime futures may be local. The HTTP
crate forbids unsafe code. OS/runtime adapters own IO and subscription tasks.

`braid-core::replica` validates event identity, parents, merge type, ranges and
representation digest. A per-document process lock serializes load, candidate
mutation, atomic file publication and notification. Event fingerprints make
retries idempotent. Diamond text changes use the declared historical parent
frontier and preserve concurrent heads. Checkpoints contain operation history
and both legacy local positions and stable remote agent/sequence mappings.
Simpleton accepts coordinated writes at the current frontier; a stale positional
write conflicts. The public legacy merge wrappers are a separate compatibility
surface and have not acquired every ReplicaStore guarantee.

HTTP and Iroh H3 call the same replica adapter. Subscriptions subscribe before
loading replay state, send an initial snapshot or valid linear replay, then
publish committed updates through a bounded broadcast. Lag terminates the stream
for frontier-based repair. Concurrent histories use an authoritative merged
snapshot rather than replaying positional patches at the wrong base.

The native `braid-http::client::ReliableChannel` actor owns a resource's GET and
serial PUT queue. A durable journal separates received, optimistic and pending
state; reconnect starts at the oldest unacknowledged update's parents. A PUT
failure cancels the GET and vice versa. Retryable network/status failures back
off together; malformed data, authorization and causal conflicts retain state
and stop for repair. There are bounded command, receive and outbox queues, a PUT
timeout, and a kernel writer lease. A queued identity and its exact bytes survive
an ambiguous ACK. Application state and its queue/cursor commit together.

`braid-core::simpleton` owns scalar text diffs, peer counters, parent checks,
representation digests and offline pending text, following the pinned upstream
client. It uses this channel rather than an independent retry loop. Its revision
comparison guards filesystem decisions against a concurrent receive. The Rust
server's conservative stale-Simpleton rejection is a separate capability: it
does not implement the upstream transforming multi-writer server algorithm.

The filesystem registry shares one Simpleton client per URL. The daemon owns
scanner, replay, subscription and debouncer workers and cancels/drains them at
shutdown. Debouncing has bounded pending URLs and at most eight workers; the
scanner has entry/depth/queue limits and a cancellation-safe overlap permit.
The channel journal holds a complete application recovery record before local
projection. `file_service` merges the disk and channel changes against the last
materialized view using declared-base Diamond operations. It queues the result
with a revision check before atomic file publication. `VersionStore.materialized`
keeps optimistic bytes separate from server-acknowledged `file_versions`.

A root filesystem lease serializes daemon projections and NFS backing operations
across cooperating processes, without holding it while waiting for a network ACK.
A separate daemon lease prevents two independent JSON/config owners. NFS adapter
calls run on blocking workers under a single permit. Persistent inode IDs are
monotonic. Rename/remove journal a bounded content snapshot before changing the
local namespace, then update descendant inode mappings in one SQLite transaction.
Prepared changes recover on NFS startup; interrupted staging never changes paths.
The daemon reconciles `remote_pending` operations through the same text channels,
using persistent monotonic operation identities. Rename publishes destination
text before an empty source replacement. Local tombstones suppress resurrection;
removed bytes and operation snapshots remain private for recovery. This is an
explicit filesystem policy, not a Braid DELETE extension or atomic remote rename.
Cross-origin destinations require explicit configuration. Unsupported binary
remote operations retain their snapshot and error rather than claiming sync.

Blob uploads stage and hash with a heap-backed 64 KiB buffer, enforce limits,
fsync payloads and publish immutable CAS files before an SQLite transaction names
them. Database work runs on the blocking pool, with a busy timeout. Cancellation
may leave safe unreferenced objects, not a committed missing payload. Historical
payloads are retained; no automatic garbage collection runs. `open` verifies and
rewinds a payload for streaming, while `get` caps buffering at 64 MiB.

Chat rooms keep structured messages, a causal graph and an additive event ledger.
They do not use Diamond text operations to merge message objects. Local and remote
candidate changes are saved before replacing visible memory. Legacy snapshots
retain messages/edits but cannot invent absent event history. A request for that
missing history returns an error. Concurrent edits of the same chat message
require explicit coordination/resolution.

Iroh 1.4.1 owns authenticated QUIC identity and connection lifecycle. H3 handlers
check the authenticated RemoteId against an explicit peer policy (default deny).
Revocation wakes active response streams. Private nodes do not automatically
broadcast resource frontiers to deterministic public gossip topics. Proxy tasks
are owned and cancelled at shutdown. Gossip remains a bounded hint mechanism;
reliable anti-entropy scheduling and relay/network-partition behavior are not yet
proven. Callers must authorize application-level remote chat authors separately.

JSON replica and room stores hold kernel writer leases for their lifetime.
Stateless replica handles in one process share ownership and subscription
broadcasts; another process fails explicitly instead of keeping a stale cache.
Blob metadata and the NFS intention registry use SQLite transactions. Auth and
friends share a bounded SQLite pool using WAL, FULL synchronization, foreign
keys and a busy timeout. Session validation consults the database for every call,
so logout and account deactivation are visible across manager instances; bcrypt
runs on a bounded blocking pool.

These locks cover cooperating services, not arbitrary external editors or
physical power loss. Unix parent directories are synced after atomic replacement;
Windows directory durability and exhaustive crash points require separate tests.
