# Narrow compatibility patches

`netlink-packet-core` is registry release 0.8.1, MIT licensed. Its
[patch record](netlink-packet-core/BRAID_PATCH.json) records the registry archive
hash and original source commit. Only the manifests replace the unmaintained
`paste` implementation with `pastey = 0.2.3`, retaining the import name `paste`.
Rust source and the public netlink macro API are unchanged. Cargo's root patch
applies it to Iroh's existing netlink 0.8 dependency edges. Netlink 0.9 removes
that macro API and cannot be substituted independently into those consumers.
Recheck the upstream consumers before removing this patch. Root Cargo patches
apply to this workspace; publishing individual crates does not propagate them
to downstream users. A release must carry an equivalent reviewed graph or wait
for the compatible upstream consumer upgrade.

Diamond Types stays under `crates/braid-core/src/vendor`, with its existing
notices and [provenance limitations](../docs/upstream-compatibility.md).
Thirteen copied modules now import maintained `compact_str::CompactString`
instead of unmaintained `smartstring::alias::String`. Empty construction uses
`default()`; serde still encodes ordinary strings. The adapter bounds/hashes
agent names strictly below the engine's 50-byte limit and reserves `ROOT`.
The copied algorithm's internal assertions and unsafe implementation are
isolated from the first-party lint guarantee. This is not a claim of a complete
engine replacement or complete historical binary-format compatibility.
