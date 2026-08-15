# Local DataFusion benchmarks

The crate also owns the `worker` binary deployed by the remote benchmark
harness. Keeping that binary here makes API changes to DataFusion Distributed
and its benchmark worker compile together from the same revision.

### Generating Benchmarking data

Generate datasets alongside the integration-test fixtures under `testdata/`.
`--dataset` is always `<suite>/<variant>` and maps to
`testdata/<suite>/<variant>`. The suite root (`testdata/tpch`,
`testdata/clickbench`, …) holds shared `queries/` and is not a valid data path.

```shell
# TPC-H (default: SCALE_FACTOR=1, PARTITIONS=16)
./gen-tpch.sh
# writes testdata/tpch/sf1

# TPC-DS (only SCALE_FACTOR=1 is supported)
./gen-tpcds.sh
# writes testdata/tpcds/sf1

# ClickBench (default: partitions 0..100 of the official hits files)
./gen-clickbench.sh
# writes testdata/clickbench/0-100

# Smaller ClickBench slice for a cheap smoke run
PARTITION_START=0 PARTITION_END=1 ./gen-clickbench.sh
# writes testdata/clickbench/0-1
```

### Running Benchmarks in single-node mode

After generating the data with the command above, the benchmarks can be run with:

```shell
WORKERS=0 ./benchmarks/run.sh --threads 2 --dataset tpch/sf1
WORKERS=0 ./benchmarks/run.sh --threads 2 --dataset clickbench/0-100
```

- `--threads`: This is the physical threads that the Tokio runtime will use for executing the
  binary. It's recommended to set `--threads` to something small, like `2`, for throttling each
  individual process running queries, and simulate how adding throttled workers can speed up the
  queries.
- `--dataset`: Logical dataset name (`tpch/sf1`, `tpcds/sf1`, `clickbench/0-100`).
  It is resolved to the corresponding `testdata/<suite>/<variant>` directory.
  Do not pass the suite name alone (`--dataset clickbench`); that directory only
  contains SQL queries.

### Running benchmarks with local workers

The same script is used for running distributed benchmarks:

```shell
WORKERS=8 ./benchmarks/run.sh --threads 2 --dataset tpch/sf1 --file-scan-config-bytes-per-partition 16777216
```

- `WORKERS`: Env variable that sets the amount of localhost workers used in the query.
- `--threads`: Sets the Tokio runtime threads for each individual worker and for the benchmarking
  binary.
- `--dataset`: `<suite>/<variant>` under `testdata` (`tpch/sf1`, `tpcds/sf1`,
  `clickbench/0-100`).
- `--file-scan-config-bytes-per-partition`: How many bytes each partition is expected to scan. Lower values
  produce more partitions/tasks. Defaults to the engine default when unset.
