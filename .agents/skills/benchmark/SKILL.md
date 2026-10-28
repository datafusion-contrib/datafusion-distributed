---
name: benchmark
description: Run, compare and profile the local dfbench benchmarks (TPC-H, TPC-DS, ClickBench, over Parquet or Iceberg). Use when asked to benchmark a change, compare branches or flag configurations, verify a performance claim, or find where time goes.
---

# Benchmarking DataFusion Distributed

What counts as evidence for a performance claim is defined in
`docs/source/contributor-guide/04-benchmarks.md`.

## Running

```shell
cargo build -p datafusion-distributed-benchmarks --release
target/release/dfbench run --dataset tpch/sf10 --workers 8 -i 5
```

`dfbench run --help` lists all options.

## Iceberg

The same benchmarks run over Iceberg tables with the `dfbench-iceberg` binary, which takes the
same `run` options:

```shell
cargo build -p datafusion-distributed-iceberg-benchmarks --release
target/release/dfbench-iceberg prepare --input testdata/tpch/sf10   # writes testdata/tpch/sf10_iceberg
target/release/dfbench-iceberg run --dataset tpch/sf10_iceberg --workers 8 -i 5
target/release/dfbench compare tpch/sf10 tpch/sf10_iceberg          # Parquet vs Iceberg results
```

`prepare` converts an existing Parquet dataset and needs an empty output directory. Iceberg runs
always load manifest column statistics. See `benchmarks/README.md` for S3 output and other options.

## Things worth knowing

- `--dataset tpch/sf10` reads `testdata/tpch/sf10`; generate missing data with the
  `benchmarks/gen-*.sh` scripts.
- Query ids are `q1`, `q18`, … (`-q q1,q18`). An unknown id silently runs nothing.
- `--workers N` runs N localhost workers in the same process and Tokio runtime as the runner,
  so a profiler attached to `dfbench` sees everything.
- Each run stores results under `testdata/<dataset>/.results/<current git branch>/` and compares
  against the previous run. Running on `main` overwrites `main`'s baseline.
- Background load (other builds or tests, indexing) skews results heavily. Check it before
  trusting numbers, and alternate the sides of a comparison over several rounds.
- CPU profiles: record with [samply](https://github.com/mstange/samply) and query them from the
  terminal with Mozilla's [profiler-cli](https://github.com/firefox-devtools/profiler/tree/main/profiler-cli)
  (`npm install -g @firefox-devtools/profiler-cli@latest`; run `profiler-cli guide` first):

  ```shell
  samply record --save-only -o /tmp/x.json.gz "$PWD"/target/release/dfbench run --dataset tpch/sf10 ...
  profiler-cli load /tmp/x.json.gz --with-samply
  ```

  The profile stores the binary path as given, and `--with-samply` resolves symbols from it, so
  use an absolute path (the same applies to `dfbench-iceberg`) or its frames show up as bare
  addresses.

  Workers share Tokio's threads, so select all busy `tokio-rt-worker` threads at once
  (`profiler-cli thread select t-1,t-2,...`) to see the whole process.
