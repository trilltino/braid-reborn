# Durability regression workloads

Run from the workspace root:

```sh
cargo test -p braid-core --test audit_filesystem --test engineering_causality
cargo test -p braid-blob --test streaming
cargo test -p local_link_server --test engineering_storage
```

The executable tests cover lost acknowledgements, replay after restart, 24 fixed
causal schedules, duplicate delivery, failed materialization, failed room
publication, partial upload IO, digest failures and corrupt payloads.
These simulate failures at application boundaries. They do not establish
power-loss behavior, filesystem cache guarantees, cross-process writer safety,
or exhaustive cancellation/crash-point coverage.
