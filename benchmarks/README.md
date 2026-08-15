# Local DataFusion benchmarks

The crate also owns the `worker` binary deployed by the remote benchmark
harness. Keeping that binary here makes API changes to DataFusion Distributed
and its benchmark worker compile together from the same revision.

### Generating Benchmarking data

Generate datasets alongside the integration-test fixtures under `testdata/`.
For example, `tpch/sf1` is stored in `testdata/tpch/sf1`.

```shell
# TPC-H (default: SCALE_FACTOR=1, PARTITIONS=16 - override by setting these environment variables)
./gen-tpch.sh

# Sorted TPC-H (same scale-factor model, tables globally sorted by TPC-H primary keys)
./gen-tpch-sorted.sh

# TPC-DS (only SCALE_FACTOR=1 is supported)
./gen-tpcds.sh
```

`tpch-sorted` is written to `testdata/tpch-sorted/sf<scale-factor>`. Use a small
`SCALE_FACTOR` (for example `0.01`) for a cheap smoke generation; SF1 is the
default and can take a few minutes. Sort keys are documented in
`testdata/tpch-sorted/README.md`.

### Running Benchmarks in single-node mode

After generating the data with the command above, the benchmarks can be run with:

```shell
WORKERS=0 ./benchmarks/run.sh --threads 2 --dataset tpch/sf1
```

- `--threads`: This is the physical threads that the Tokio runtime will use for executing the
  binary. It's recommended to set `--threads` to something small, like `2`, for throttling each
  individual process running queries, and simulate how adding throttled workers can speed up the
  queries.
- `--dataset`: Logical dataset name (e.g. `tpch/sf1`, `tpch-sorted/sf1`,
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
