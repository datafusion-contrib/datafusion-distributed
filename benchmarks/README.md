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

# TPC-DS (same scale-factor model; writes testdata/tpcds/sf<scale-factor>)
./gen-tpcds.sh

# TPC-DS at SF10
SCALE_FACTOR=10 ./gen-tpcds.sh
```

`SCALE_FACTOR` for TPC-DS can be any value in `(0, 100000]`. SF1 uses the
pre-built parquet from [datafusion-benchmarks](https://github.com/apache/datafusion-benchmarks);
other scale factors are generated with `tpcdsgen`. Use a small value such as
`0.01` for a cheap smoke generation.

### Running Benchmarks in single-node mode

After generating the data with the command above, the benchmarks can be run with:

```shell
WORKERS=0 ./benchmarks/run.sh --threads 2 --dataset tpch/sf1
```

- `--threads`: This is the physical threads that the Tokio runtime will use for executing the
  binary. It's recommended to set `--threads` to something small, like `2`, for throttling each
  individual process running queries, and simulate how adding throttled workers can speed up the
  queries.
- `--dataset`: Logical dataset name (e.g. `tpch/sf1`, `tpcds/sf1`). It is
  resolved to the corresponding `testdata/<suite>/<variant>` directory.

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
