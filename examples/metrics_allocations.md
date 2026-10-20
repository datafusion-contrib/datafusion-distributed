# Task-metric decoding allocations

From the repository root, run:

```sh
cargo run --locked --example metrics_allocations
```

Fixtures are generated in memory, and both decoders run in one process. Cargo
uses the repository's Rust 1.95 toolchain and lockfile. The command uses the default
dev profile (unoptimized, with debug information); the first build can take
several minutes.

The example compares the production decoder with the pre-change loop that
clones each protobuf metric before conversion. Both use the same unchanged
per-metric converter and checked-in protobuf definitions.

Two in-memory fixtures each contain 128 metrics with two labels per metric:

- `counters`: 128 named counters.
- `mixed16`: 112 named counters and 16 P95 latency sketches.

Only conversion allocations are counted. Fixture construction, input cloning,
output serialization, and equality checks are outside the measured region.
Each fixture runs 32 times, asserting stable allocation counts and protobuf
roundtrips identical to the input.

With Rust 1.95.0 on macOS arm64, the output is:

```text
counters: 32 identical repeated roundtrips, calls 1542 -> 774 (49.8% reduction), bytes 51716 -> 33760 (34.7% reduction)
mixed16: 32 identical repeated roundtrips, calls 1590 -> 806 (49.3% reduction), bytes 122836 -> 69856 (43.1% reduction)
```

`calls` counts allocation and reallocation calls. `bytes` sums requested
allocation sizes, counting reallocations at their full new size; it does not
measure peak or retained memory. Exact totals can vary by platform. These
synthetic fixtures demonstrate decoding allocation savings, not an end-to-end
query speedup.
