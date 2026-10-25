# Resource ownership and local administration

Authentication supplies a user ID or cryptographically authenticated Iroh endpoint
ID. These are separate principal namespaces. New resources are private. A reader
can read snapshots, retained history, indexes and subscriptions; an editor can
also write. Owners can change grants, public visibility and the primary owner.
Grants and policy revisions live outside replicated content.

Ownerless existing resources deny network access, including authenticated writes.
The server never infers ownership from a room's name or old `created_by` field.
Use the local administration binary to assign a verified principal explicitly.

Application policies live at `BRAID_ROOT/users.policies`. Iroh policies live under
`BRAID_ROOT/iroh/ENDPOINT_ID/.resource-policies`. The application uses canonical
`chat/ID`, `wiki/PATH`, `local/PATH` and `v2/PATH` keys. Windows file-backed keys
are lowercase to share policy across case aliases. Iroh uses normalized resource
names such as `/doc`, isolated by node storage directory.

Build the administrator with `cargo build -p braid-core --bin braid-admin --locked`.
The following are command forms, not commands run against deployed data:

```text
braid-admin inventory POLICY_ROOT
braid-admin scan POLICY_ROOT chat CHAT_DATA_ROOT
braid-admin scan POLICY_ROOT wiki PAGE_DATA_ROOT
braid-admin scan POLICY_ROOT local LOCAL_ORG_ROOT
braid-admin scan POLICY_ROOT v2 PAGE_DATA_ROOT
braid-admin scan POLICY_ROOT iroh NODE_DATA_ROOT
braid-admin plan POLICY_ROOT assignments.json adoption-plan.json
braid-admin apply POLICY_ROOT adoption-plan.json
braid-admin apply POLICY_ROOT adoption-plan.json --apply
braid-admin rollback POLICY_ROOT adoption-plan.json
braid-admin rollback POLICY_ROOT adoption-plan.json --apply
```

Inventory is read-only, includes hashes, and reports absent policies. A legacy
hashed checkpoint without its original resource name is reported with a null
resource; supply its independently verified mapping before assignment. Inventory
rejects links/reparse points, excessive depth and oversized JSON rather than
guessing. Internal directories are excluded; retain a full filesystem backup,
including hidden journals and SQLite/WAL files, separately.

Assignments are explicit JSON, for example:

```json
[{"resource":"chat/room","owner":{"kind":"user","id":"verified-user-id"}}]
```

Stop writers and take a full data backup before migration. `plan` writes a new
review artifact without changing policy or payloads; save it outside the policy
directory. Its exact policy identities record the previously absent policies.
`apply` and `rollback` are read-only unless `--apply` is present. Execution is
atomic per resource and resumes using the same plan. Competing ownership is
never overwritten. Rollback removes only policies that still exactly match the
plan. A grant change or a protected data write blocks rollback. After data writes,
stop writers and use the full pre-upgrade backup for data rollback.

Only trusted local administrators run these commands. An actor argument is an
explicit ownership check, not remote authentication:

```text
braid-admin grant POLICY_ROOT chat/room user OWNER_ID user READER_ID reader
braid-admin grant POLICY_ROOT /doc peer OWNER_ENDPOINT peer EDITOR_ENDPOINT editor
braid-admin revoke POLICY_ROOT chat/room user OWNER_ID user READER_ID
braid-admin public POLICY_ROOT chat/room user OWNER_ID true
braid-admin transfer POLICY_ROOT chat/room user OWNER_ID user NEW_OWNER_ID
```

A policy reservation can survive a failed new-resource write. It remains private
and inspectable; the same owner can retry. Neither data errors nor reservations
produce successful ACKs. Unix policy writes sync the file and containing directory;
Windows uses the existing synced-file replacement boundary and inherits directory
ACLs. No physical power-loss guarantee is claimed for untested filesystems.

Active subscriptions recheck access per frame and at least every second. A policy
error or revocation terminates the stream and drops its upstream receiver. Blob
hash possession does not authorize reads: an existing readable resource reference
or private upload reference must authorize the blob. A new message must authorize
its attachments before insertion, so it cannot grant access to a guessed hash.
Public resource visibility can explicitly permit anonymous reads and referenced
attachments; it never permits anonymous writes.

Iroh transport allowlists and resource grants are separate. Public transport mode
does not publish every resource. Deterministic gossip is restricted to resources
explicitly marked public; private subscriptions use authenticated H3. Fallible
node read/history methods preserve storage errors; older convenience APIs remain
deprecated adapters for trusted local callers.

The wire boundary excludes credentials and hop-by-hop transport fields from event
metadata. Request credentials still reach HTTP authentication, but cannot enter
new persisted history or replicated update frames. Older stored metadata is
filtered on wire output. Old outboxes containing transport headers stop for
explicit local migration, retaining their original bytes and uncertain identities.
Existing token-bearing checkpoints and fingerprint migration still need a complete
legacy corpus audit; these repairs do not certify arbitrary old deployment data.

The full migration/fault matrix, secret-store integration, per-origin daemon
credentials, multiplex principal binding and multi-resource attachment indexes
remain separate release work. Attachment reference scans currently stop above
10,000 rooms; list filtering bounds materialized JSON to 32 MiB.
