# Local DataFusion benchmarks

### Sibling Top-K scan workloads

```shell
cargo bench -p datafusion-distributed-benchmarks --bench sibling_topk -- --save-baseline main
# Run the same workload on the candidate revision:
cargo bench -p datafusion-distributed-benchmarks --bench sibling_topk -- --baseline main
```

This controlled benchmark runs `SELECT score FROM topk_data WHERE included ORDER
BY score DESC LIMIT 100` through localhost gRPC with two or eight scan tasks,
two partitions per task, and four Tokio threads. It uses ordinary task allocation
and execution, with no wait for a dynamic filter. Each task adds one identical
two-million-row Parquet file: the eight-task case has four times the data of the
two-task case, so this is not a constant-work fan-out comparison.

- `pruning`: the first row group has high scores; remaining groups have increasing
  low scores. Natural byte-range allocation separates the high groups from some
  low scans, giving an early remote bound useful work to prune.
- `nonprunable`: included scores increase, but each row group also contains an
  excluded row with the maximum `Int64` score. Its statistics prevent Top-K bounds
  from skipping that group. Parquet row filtering and page indexes are disabled
  in both workloads, so this control measures update work without scan pruning.
  Every iteration checks zero pruned groups or file ranges, all input rows scanned,
  and stable scan bytes; compare those bytes across revisions too.

Every query checks the complete ordered result against values derived from the
fixture. Worker completion observations report actual task assignments, local
and remote filter counts, scan bytes, row counts, and group/range pruning. The
configured `distributed.enable_sibling_topk_filter_pushdown` entry determines whether remote
consumers are required. Effective remote pruning requires a useful received
predicate and fewer scan rows than local pruning could explain. One check uses
groups surviving the strongest local cutoff. Another recognizes file-range stops
when the local filter still admits the whole-file maximum, after discounting all
observed row-group pruning. Both exclude empty byte fragments and require actual
pruning counters. They prove fewer scan output rows; Parquet scan-byte counters
measure reduced reads separately, not physical disk or origin I/O.

Every correct query stays in the timing samples, including queries where delivery
was too late to prove useful pruning. The output counts queries with producer
updates, useful received predicates, and proven remote effects separately. Report
those fractions alongside timings; the pruning workload needs observed effects
before supporting a pruning-benefit claim. Criterion's `observed` totals include
its calibration, warmup, and measurement calls; resource totals cover exactly the
marked queries.

The benchmark compiles before sibling sharing is available. To evaluate an
opt-in implementation, install its configuration at the existing context setup
and verify `configured=true` in the candidate's output and `false` in the control.
Keep the fixture, query, checks, and measurement loop unchanged.

Criterion measures execution through complete result collection. Fixture
generation, worker startup, planning, and checking after collection are excluded;
worker completion observations can overlap collection.
The untimed preflight is also a warmup and prints the worker observations.
For fixed-work CPU and memory comparisons, run the compiled benchmark executable
with, for example, `TOPK_RESOURCE_CASE=pruning/8 TOPK_RESOURCE_QUERIES=20`.
Accepted case names are `pruning/2`, `pruning/8`, `nonprunable/2`, and
`nonprunable/8`; the query count defaults to 20. Flushed `resource_begin` and
`resource_end` lines bracket exactly that many checked queries after the preflight.
This window includes planning and checking; its summed `elapsed` field contains
only query execution. Observing remote predicates adds feature-dependent work,
so these CPU measurements do not isolate production-only sharing overhead.
Observations are consumed after each query, and plans are not retained across
queries.

Use a fresh process per resource run. Measure process user plus system CPU and
sample resident memory inside the marked window; report process high-water RSS
separately because it includes setup. All workers share the process, so summing
per-worker process metrics duplicates resources. Criterion calibrates iteration
counts independently, making whole-process CPU totals from ordinary Criterion
runs unsuitable for fixed-work comparisons.
Sampled RSS can miss short peaks; allocator-accounted Arrow memory does not include
all expression, message, and channel allocations. The reported update count is a
producer-report count, not a measurement of dynamic-filter network bytes.

Use matching release builds, dataset contents, CPU affinity, and configuration;
alternate paired runs on a quiet host and retain failed or rejected pairs. Verify
activation and exact correctness before interpreting timings. These synthetic
cases qualify conditional scan savings and overhead; they do not establish a
general speedup or replace representative full-suite regression measurements.

### Generating Benchmarking data

The `dfbench prepare-*` commands generate datasets under `--root` (by default
the repository's
`testdata/`), in a directory whose path records every parameter the dataset was
generated with, in a fixed order:

```
testdata/tpch_parquet/scale_factor=1/max_file_size=134217728/dictionary=pages/sorted=false/
```

`run --dataset` and `compare` take the dataset's directory, absolute or relative
to the current directory, such as the absolute path that each `prepare-*` command
prints when it finishes. That path is the only output on stdout (progress goes to
stderr), so it can be captured:

```shell
# TPC-H (default: --scale-factor 1)
DATASET=$(cargo run -p datafusion-distributed-benchmarks --release -- prepare-tpch)

# TPC-DS (default: --scale-factor 1)
cargo run -p datafusion-distributed-benchmarks --release -- prepare-tpcds

# ClickBench (defaults: --partition-start 0 --partition-end 100)
cargo run -p datafusion-distributed-benchmarks --release -- prepare-clickbench
```

Each table is generated as one ordered stream, split into `<table>/1.parquet`,
`<table>/2.parquet`, ... whenever `--max-file-size` is reached, so the file size
determines how many files a table has and every file covers a contiguous range
of the generation order. TPC-H and TPC-DS accept the same Parquet layout
options:

| Option            | Path key        | Default               | Meaning                                                                                                                                      |
|-------------------|-----------------|-----------------------|----------------------------------------------------------------------------------------------------------------------------------------------|
| `--scale-factor`  | `scale_factor`  | `1`                   | Scale factor. Use a small value such as `0.01` for a cheap smoke generation                                                                  |
| `--max-file-size` | `max_file_size` | `134217728` (128 MiB) | Approximate maximum bytes per file; a file can exceed it by up to one batch                                                                  |
| `--dictionary`    | `dictionary`    | `pages`               | `none` disables Parquet dictionary pages, `pages` keeps them, `arrow` also stores low-cardinality string columns as Arrow `Dictionary` types |
| `--sorted`        | `sorted`        | `false`               | Write Parquet `sorting_columns` metadata for the columns the generator already emits in order                                                |

With `--sorted`, the sort keys declared for each table are listed in
`table_columns` in
`../datasets/src/tpch.rs` and `tpcds.rs`.

`_SUCCESS` is written once every table is complete, and rerunning a `prepare-*`
command for a complete dataset does nothing. Any other non-empty destination,
such as one left by an interrupted run, is rejected: remove it and generate
again.

### Writing datasets to S3

Pass `--root` to a `prepare-*` command to create the dataset path under another
local directory or an S3 prefix:

```shell
cargo run -p datafusion-distributed-benchmarks --release -- prepare-tpch --root s3://my-bucket
cargo run -p datafusion-distributed-benchmarks --release -- prepare-tpcds --root s3://my-bucket
cargo run -p datafusion-distributed-benchmarks --release -- prepare-clickbench --partition-end 1 \
  --root s3://my-bucket
```

TPC-H and TPC-DS stream generated batches directly to Parquet in the
destination. ClickBench uses temporary storage for one partition at a time to
correct its date column.

Parquet generation resolves credentials once using the official AWS SDK
credential chain, including environment credentials, profiles, SSO, and instance
roles. For SSO, sign in and select the profile before generating:

```shell
aws sso login --profile my-profile
export AWS_PROFILE=my-profile
export AWS_REGION=us-east-1 # use your bucket's region
```

The region is read from the environment or selected profile. Credentials must
remain valid for the whole run; the generator does not refresh them.

Iceberg's storage library supports environment credentials and instance roles.
For an SSO session, export credentials before converting a local dataset to
Iceberg:

```shell
eval "$(aws configure export-credentials --profile my-profile --format env)"
```

The destination bucket must already exist. Generation needs list, read and write
access, including multipart upload access. Iceberg metadata and manifests are
created with their final S3 paths; no metadata rewriting or filesystem sync is
needed.

### Running Benchmarks in single-node mode

After generating the data with the command above, the benchmarks can be run with
the dataset path it printed:

```shell
DATASET=$(cargo run -p datafusion-distributed-benchmarks --release -- prepare-tpch)
cargo run -p datafusion-distributed-benchmarks --release -- run --dataset $DATASET
```

- `--dataset`: Dataset directory, absolute or relative to the current directory. Its
  `<benchmark>_<format>`
  directory selects the queries: `tpch_parquet` and `tpch_iceberg` run the TPC-H
  queries.

### Running benchmarks with local workers

The same command is used for running distributed benchmarks:

```shell
cargo run -p datafusion-distributed-benchmarks --release -- run --workers 8 --dataset $DATASET --file-scan-config-bytes-per-partition 16777216
```

- `--workers`: Amount of localhost workers used in the query. They all run in
  the same process and on the same Tokio runtime as the benchmark runner, so a
  profiler like `samply`
  attached to the benchmark process observes both the coordinator and the
  workers on the same threads.
- `--dataset`: Dataset path, as above.
- `--file-scan-config-bytes-per-partition`: How many bytes each partition is
  expected to scan. Lower values produce more partitions/tasks. Defaults to the
  engine default when unset.

### Iceberg benchmarks

Iceberg preparation and execution live in a separate package so the benchmark
crate used by the root integration tests does not depend on Iceberg. Prepare the
Parquet input, then convert it:

```shell
DATASET=$(cargo run -p datafusion-distributed-benchmarks --release -- prepare-tpch)
ICEBERG=$(cargo run -p datafusion-distributed-iceberg-benchmarks --release -- prepare \
  --input $DATASET)

# Or write the Iceberg representation directly to S3:
cargo run -p datafusion-distributed-iceberg-benchmarks --release -- prepare \
  --input $DATASET \
  --output s3://my-bucket/tpch_iceberg/${DATASET#*tpch_parquet/}/target_file_size=1099511627776
```

Without `--output`, the conversion writes the matching `tpch_iceberg` dataset,
with the source's parameters followed by `target_file_size`, and leaves the
source unchanged:

```
testdata/tpch_iceberg/scale_factor=1/.../sorted=false/target_file_size=1099511627776/
```

It streams one source file at a time into unpartitioned, append-only Iceberg
tables. Source file boundaries are preserved unless `--target-file-size`
requests rolling. As with
`prepare-*`, `_SUCCESS` is written last, converting into a complete dataset does
nothing, and any other non-empty destination is rejected. An existing Iceberg
copy is therefore not updated when the Parquet source is regenerated: remove it
and convert again. When choosing `--output`, keep the
`<benchmark>_iceberg` directory in the path so that `run` can select the
queries.

Run each representation with its format-specific binary, then compare them with
`dfbench`:

```shell
cargo run -p datafusion-distributed-benchmarks --release -- run \
  --workers 2 --dataset $DATASET --partitions 2
cargo run -p datafusion-distributed-iceberg-benchmarks --release -- run \
  --workers 2 --dataset $ICEBERG --partitions 2

dfbench compare $DATASET $ICEBERG
dfbench compare $DATASET@base $ICEBERG@candidate
dfbench compare base candidate --dataset $DATASET
```

`compare` takes two `dataset[@branch]` states, [prev] then [new]. An omitted
branch defaults to the current branch. With `--dataset`, both positional
arguments remain literal branch names. Each dataset uses the existing
`.results/<branch>/` and `previous.json` layout.

Absolute dataset paths are supported when they follow the same layout. Iceberg
runs always load manifest column statistics. Source file boundaries come from
the Parquet dataset's `--max-file-size`. Generated metadata contains absolute
local locations; cloud publication and remote harness support remain separate
work.

### Remote worker

The remote benchmark harness uses the `worker` binary in the
`datafusion-distributed-remote-worker` package. It serves Parquet benchmarks by
default. Build the same binary with the `iceberg` feature to enable Iceberg
session configuration on both the coordinator and every distributed worker:

```shell
cargo build -p datafusion-distributed-remote-worker --release
cargo build -p datafusion-distributed-remote-worker --release --features iceberg
```
