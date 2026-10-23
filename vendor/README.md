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

`nfsserve` is registry release 0.10.2, BSD-3-Clause licensed. Its
[patch record](nfsserve/BRAID_PATCH.json) pins the exact archive SHA-256;
the published archive supplies no source commit metadata. The only modified
source file is `src/nfs_handlers.rs`: READDIR now forwards the actual cookie
to the existing `readdir` adapter, and both directory procedures reject tiny
framing budgets before subtracting overhead. Public adapter APIs are unchanged. Manifests use num-derive 0.4 (also used
by upstream 0.11) instead of 0.3 to avoid obsolete generated non-local impls.
The independent raw TCP RPC regression first failed by repeating page one,
then passes with the patch and checks both zero-count error responses. Registry
0.11.0 was inspected (source af4f1176dd709847f3dda4f1a7cd093edaadde63)
and retains these defects, so upgrading it would not repair them.
