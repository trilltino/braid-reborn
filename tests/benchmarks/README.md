# Parser comparison workload

```sh
cargo run --locked -p braid-http --example codec_benchmark
# Use --release for optimized measurements; keep profile identical for both parsers.
```

The example sources live in crates/braid-http/examples/codec_benchmark/.
The same binary includes the frozen parser from baseline commit
4f5275b9ab83bc3cff1ef91ff068be6539edbfa2 and calls the current parser. The
baseline file is a benchmark-only reference, excluded from application paths.
Both receive the same valid 8192-byte header in one-byte fragments for 20 iterations.
Both use the current bytes/error/Patch dependency versions, so this isolates
parser source changes rather than reconstructing a complete historical executable.

One Windows x86_64 debug-profile observation on Rust 1.99.0:

| Parser | Total elapsed microseconds, 20 iterations |
| --- | ---: |
| Baseline source | 37,571,873 |
| Current source | 85,565 |

This adversarial header workload checks the intended reduction in repeated scans.
It is an exploratory single run, not a production throughput forecast or an
independent benchmark study. No broad speed, RSS, allocation, blob-throughput,
CRDT-merge, binary-size or cold-build improvement is claimed. Repeated optimized
runs and representative application workloads remain release work.
