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

Use the same dataset name for either format (`dfbench` is `target/release/dfbench`):

```shell
dfbench run --dataset tpch/sf1
dfbench run --dataset tpch/sf1 --iceberg
dfbench compare --dataset tpch/sf1 --compare-iceberg
dfbench compare base candidate --dataset tpch/sf1 --iceberg

WORKERS=2 ./benchmarks/run.sh --dataset tpch/sf1 --iceberg --threads 2 --partitions 2 \
  --file-scan-config-bytes-per-partition 16777216
```

- `--iceberg` resolves `<dataset>` to `<dataset>-iceberg` before execution or on both sides of a
  two-branch timing comparison. Continue passing the base dataset name to this flag.
- `--compare-iceberg` compares saved Parquet timings [prev] against Iceberg timings [new] on the
  current branch, or one explicitly named branch. Two branches are rejected: formats are never
  compared across different branches. Combining the two flags is also rejected.
- Each dataset uses the existing `.results/<branch>/` and `previous.json` layout. Iceberg results
  live under `sf1-iceberg/`, separate from the Parquet results under `sf1/`. Parquet result storage,
  saved JSON and branch naming are unchanged.
- Timing calculations are unchanged; comparisons do not execute queries or check correctness.

Absolute dataset paths are supported when they follow the same `<suite>/<variant>` convention.
`--iceberg-column-stats` loads manifest column statistics during planning.

For SF10, SF100, etc., change the generation scale and paths; increase generation `--partitions`
to avoid oversized source files. Conversion is sequential and retains both representations.
Large-scale throughput and TPC-DS/ClickBench Iceberg conversion are not qualified here.
Generated metadata contains absolute local locations. Older experimental `.iceberg/` datasets
are not moved automatically; regenerate at the sibling destination rather than simply moving files.
Uploading files to S3 is not sufficient for remote execution. Cloud publication and harness support remain separate work.
