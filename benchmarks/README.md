# Local DataFusion benchmarks

The crate also owns the `worker` binary deployed by the remote benchmark
harness. Keeping that binary here makes API changes to DataFusion Distributed
and its benchmark worker compile together from the same revision.

### Generating Benchmarking data

Generate datasets alongside the integration-test fixtures under `testdata/`.
For example, `tpch/sf1` is stored in `testdata/tpch/sf1`.
TPC-H generation partitions the scalable tables, but writes `nation` (25 rows) and `region`
(5 rows) only once. Regeneration removes surplus numbered Parquet partitions.
Rebuild the Iceberg copy separately after regenerating Parquet; an existing copy is not updated.

```shell
# TPC-H (default: SCALE_FACTOR=1, PARTITIONS=16, SORTED=false - override by setting these environment variables)
./gen-tpch.sh

# Sorted TPC-H (same generators, Parquet sorting_columns metadata)
SORTED=true ./gen-tpch.sh

# TPC-DS (only SCALE_FACTOR=1 is supported)
./gen-tpcds.sh
```

`tpch/sorted_sf1` is written to `testdata/tpch/sorted_sf<scale-factor>`. Use a
small `SCALE_FACTOR` (for example `0.01`) for a cheap smoke generation; SF1 is
the default. Files record the columns tpchgen already emits in order:
`r_regionkey`, `n_nationkey`, `c_custkey`, `s_suppkey`, `p_partkey`,
`ps_partkey`, `o_orderkey`, and `(l_orderkey, l_linenumber)`.

### Running Benchmarks in single-node mode

After generating the data with the command above, the benchmarks can be run with:

```shell
WORKERS=0 ./benchmarks/run.sh --threads 2 --dataset tpch/sf1
```

- `--threads`: This is the physical threads that the Tokio runtime will use for executing the
  binary. It's recommended to set `--threads` to something small, like `2`, for throttling each
  individual process running queries, and simulate how adding throttled workers can speed up the
  queries.
- `--dataset`: Logical dataset name (e.g. `tpch/sf1`, `tpch/sorted_sf1`,
  `tpcds/sf1`). It is resolved to the corresponding
  `testdata/<suite>/<variant>` directory.

### Running benchmarks with local workers

The same script is used for running distributed benchmarks:

```shell
WORKERS=8 ./benchmarks/run.sh --threads 2 --dataset tpch/sf1 --file-scan-config-bytes-per-partition 16777216
```

- `WORKERS`: Env variable that sets the amount of localhost workers used in the query.
- `--threads`: Sets the Tokio runtime threads for each individual worker and for the benchmarking
  binary.
- `--dataset`: Dataset directory name under `testdata`.
- `--file-scan-config-bytes-per-partition`: How many bytes each partition is expected to scan. Lower values
  produce more partitions/tasks. Defaults to the engine default when unset.

### Iceberg benchmarks

Prepare Parquet, then convert it with the same Rust binary (from the repository root):

```shell
cargo run -p datafusion-distributed-benchmarks --release -- prepare-tpch \
  --output testdata/tpch/sf1 --scale-factor 1 --partitions 16
cargo run -p datafusion-distributed-benchmarks --release -- prepare-iceberg \
  --input testdata/tpch/sf1
```

`prepare-iceberg` writes to the sibling `<input>-iceberg/` directory: for example,
`testdata/tpch/sf1-iceberg/`. The source Parquet dataset stays in `testdata/tpch/sf1/`.
Conversion streams one source file at a time into unpartitioned, append-only Iceberg tables using
Parquet writer defaults. Source file boundaries are preserved unless `--target-file-size` requests
rolling. Manifests contain per-file metrics; snapshots contain aggregate record/file-size statistics.
Each table's committed metadata is saved as `<output>/<table>/metadata.json`; `_SUCCESS` is written
last. The output directory must be empty. Interrupted conversion is not resumable and cannot be run.

Run either representation and compare saved states (`dfbench` is `target/release/dfbench`):

```shell
dfbench run --dataset tpch/sf1
dfbench run --dataset tpch/sf1-iceberg --format iceberg
dfbench compare tpch/sf1 tpch/sf1-iceberg
dfbench compare tpch/sf1@base tpch/sf1-iceberg@candidate
dfbench compare base candidate --dataset tpch/sf1

WORKERS=2 ./benchmarks/run.sh --dataset tpch/sf1-iceberg --format iceberg --threads 2 --partitions 2 \
  --file-scan-config-bytes-per-partition 16777216
```

- `run --format` selects the table backend, defaulting to Parquet. Pass the actual dataset
  directory name; running a benchmark never rewrites it.
- `compare` takes exactly two `dataset[@branch]` states, [prev] then [new]. An omitted branch
  defaults to the current branch; the final `@` separates an explicit branch from its dataset.
  Each state independently selects its dataset and branch, without format-specific flags.
- With `compare --dataset`, both positional arguments are literal branch names, preserving the
  existing two-branch shorthand. This also works with an Iceberg dataset such as `tpch/sf1-iceberg`.
- Each dataset uses the existing `.results/<branch>/` and `previous.json` layout. Iceberg results
  live under `sf1-iceberg/`, separate from the Parquet results under `sf1/`. Parquet result storage,
  saved JSON and branch naming are unchanged.
- Timing calculations are unchanged; comparisons do not execute queries or check correctness.

Absolute dataset paths are supported when they follow the same `<suite>/<variant>` convention.
With `--format iceberg`, `--iceberg-column-stats` loads manifest column statistics during planning.
Iceberg preparation, table registration, and session options live behind the Iceberg crate's
`benchmarks` feature, enabled by this runner. The execution loop only receives backend callbacks.

For SF10, SF100, etc., change the generation scale and paths; increase generation `--partitions`
to avoid oversized source files. Conversion is sequential and retains both representations.
Large-scale throughput and TPC-DS/ClickBench Iceberg conversion are not qualified here.
Generated metadata contains absolute local locations. Older experimental `.iceberg/` datasets
are not moved automatically; regenerate at the sibling destination rather than simply moving files.
Uploading files to S3 is not sufficient for remote execution. Cloud publication and harness support remain separate work.
