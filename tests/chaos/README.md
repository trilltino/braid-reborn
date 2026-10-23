# Durability regression workloads

Run from the workspace root:

```sh
cargo test -p braid-core --all-features --test audit_filesystem --test engineering_causality --test audit_shared_channel
cargo test -p braid-http --test reliable_channel
cargo test -p braid-blob --test streaming
cargo test -p local_link_server --test engineering_storage
```

The executable tests cover lost acknowledgements, replay after restart, 24 fixed
causal schedules, duplicate delivery, failed materialization, failed room
publication, partial upload IO, digest failures and corrupt payloads.
The reliable-channel test also kills an actual journal writer process, checks
exclusive ownership, then reopens and replays the same identity. Shared-channel
filesystem tests cover offline NFS rename, later local edits, restart, lost ACK
and delete reconciliation. Kernel leases protect cooperating writer processes.
These cases do not establish physical power-loss behavior or exhaustive
cancellation/crash-point coverage.

`nfs-mount.sh` is a privileged Linux-only NFSv3 mount test with an isolated
temporary data root. It tests real file offsets, binary writes, truncation,
sparse writes, open handles across rename, cookie pagination and removal. CI
installs nfs-common and runs it separately from ordinary unit tests.
