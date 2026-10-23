# Migration and rollback

Stop writers and make a byte-for-byte backup of the configured BRAID_ROOT before
upgrading a deployment. No command in this implementation mutates deployed data.
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

## Public Rust/configuration changes

- BraidClient construction is fallible: use `BraidClient::new()?`; its panic-prone
  Default implementation was removed.
- RuntimeFuture is target-specific: Send on native, local on WASM. Unsafe future
  wrappers were removed. Core re-exports the canonical HTTP runtime trait.
- ParserLimits adds header-field and header-value limits; use `..Default::default()`
  in struct literals. BraidError adds ResourceLimit (the enum is non-exhaustive).
- BraidNfsBackend::new returns Result and fails on corrupt inode metadata.
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
