# Distributed DataFusion Benchmarks

### Generating Benchmarking data

Generate datasets into `benchmarks/data/`.

```shell
# TPC-H (default: SCALE_FACTOR=1, PARTITIONS=16 - override by setting these environment variables)
./gen-tpch.sh

# TPC-DS (only SCALE_FACTOR=1 is supported)
./gen-tpcds.sh
```

### Running Benchmarks in single-node mode

After generating the data with the command above, the benchmarks can be run with:

```shell
WORKERS=0 ./benchmarks/run.sh --threads 2 --dataset tpch_sf1
```

- `--threads`: This is the physical threads that the Tokio runtime will use for executing the
  binary. It's recommended to set `--threads` to something small, like `2`, for throttling each
  individual process running queries, and simulate how adding throttled workers can speed up the
  queries.
- `--dataset`: Dataset directory name under `benchmarks/data/` (e.g. `tpch_sf1`, `tpcds_sf1`).

### Running Benchmarks benchmarks in distributed mode

The same script is used for running distributed benchmarks:

```shell
WORKERS=8 ./benchmarks/run.sh --threads 2 --dataset tpch_sf1 --files-per-task 2
```

- `WORKERS`: Env variable that sets the amount of localhost workers used in the query.
- `--threads`: Sets the Tokio runtime threads for each individual worker and for the benchmarking
  binary.
- `--dataset`: Dataset directory name under `benchmarks/data/`.
- `--files-per-task`: How many files each distributed task will handle.
