# Distributed DataFusion Benchmarks

### Generating Benchmarking data

Generate TPCH data into the `data/` dir

```shell
./gen-tpch.sh
./gen-tpcds.sh
```

### Running Benchmarks in single-node mode

After generating the data with the command above, the benchmarks can be run with:

```shell
WORKERS=0 ./benchmarks/run.sh --threads 2 --path benchmarks/data/tpch_sf1
```

- `--threads`: This is the physical threads that the Tokio runtime will use for executing the binary.
  It's recommended to set `--threads` to something small, like `2`, for throttling each individual
  process running queries, and simulate how adding throttled workers can speed up the queries.
- `--path`: It can point to any folder containing benchmark datasets.

### Running Benchmarks benchmarks in distributed mode

The same script is used for running distributed benchmarks:

```shell
WORKERS=8 ./benchmarks/run.sh --threads 2 --path ./benchmarks/data/tpch_sf1 --files-per-task 2
```

- `WORKERS`: Env variable that sets the amount of localhost workers used in the query.
- `--threads`: Sets the Tokio runtime threads for each individual worker and for the benchmarking binary.
- `--path`: It can point to any folder containing benchmark datasets.
- `--files-per-task`: How many files each distributed task will handle.
