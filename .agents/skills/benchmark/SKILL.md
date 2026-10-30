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
DATASET=$(target/release/dfbench prepare-tpch --scale-factor 10)   # only the path goes to stdout
target/release/dfbench run --dataset $DATASET --workers 8 -i 5
```

`dfbench run --help` lists all options.

## Iceberg

The same benchmarks run over Iceberg tables with the `dfbench-iceberg` binary, which takes the
same `run` options:

```shell
cargo build -p datafusion-distributed-iceberg-benchmarks --release
ICEBERG=$(target/release/dfbench-iceberg prepare --input $DATASET)
target/release/dfbench-iceberg run --dataset $ICEBERG --workers 8 -i 5
target/release/dfbench compare $DATASET $ICEBERG                     # Parquet vs Iceberg results
```

`prepare` converts an existing Parquet dataset; converting into a complete dataset is a no-op, so
remove the Iceberg copy after regenerating its source. Iceberg runs always load manifest column
statistics. See `benchmarks/README.md` for S3 output and other options.

## Things worth knowing

- Datasets live in `testdata/<suite>_<format>/<key>=<value>/...` directories that record every
  generation parameter; `prepare-*` prints the absolute path, and `--dataset` takes it absolute
  or relative to the current directory. Generating a complete dataset again is a
  no-op; an interrupted one must be removed before regenerating. Layout options
  (`--max-file-size`, `--dictionary`, `--sorted`) are listed in `benchmarks/README.md`.
- DuckDB and Spark treat the `key=value` directories as Hive partitions and add them as columns;
  pass `hive_partitioning=false` when inspecting files with DuckDB.
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
  samply record --save-only -o /tmp/x.json.gz "$PWD"/target/release/dfbench run --dataset $DATASET ...
  profiler-cli load /tmp/x.json.gz --with-samply
  ```

  The profile stores the binary path as given, and `--with-samply` resolves symbols from it, so
  use an absolute path (the same applies to `dfbench-iceberg`) or its frames show up as bare
  addresses.

  Workers share Tokio's threads, so select all busy `tokio-rt-worker` threads at once
  (`profiler-cli thread select t-1,t-2,...`) to see the whole process.
