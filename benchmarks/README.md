# Distributed DataFusion Benchmarks

### Generating Benchmarking data

Generate TPCH data into the `data/` dir

```shell
./gen-tpch.sh
./gen-tpcds.sh
```

### Running TPCH benchmarks in single-node mode

After generating the data with the command above, the benchmarks can be run with

```shell
cargo run -p datafusion-distributed-benchmarks --release -- run --path benchmarks/data/tpch_sf1
```

Note that the `--path` flag can point to any folder containing benchmark datasets.

For running the benchmarks with using just a specific amount of physical threads:

```shell
cargo run -p datafusion-distributed-benchmarks --release -- run --threads 3 --path benchmarks/data/tpch_sf1
```

### Running Benchmarks benchmarks in distributed mode

Running the benchmarks in distributed mode implies:

- running 1 or more workers in separate terminals
- running the benchmarks in an additional terminal

The workers can be spawned by passing the `--spawn <port>` flag, for example, for spawning 3 workers:

```shell
cargo run -p datafusion-distributed-benchmarks --release -- run --spawn 8000
```

```shell
cargo run -p datafusion-distributed-benchmarks --release -- run --spawn 8001
```

```shell
cargo run -p datafusion-distributed-benchmarks --release -- run --spawn 8002
```

With the three workers running in separate terminals, the TPCH benchmarks can be run in distributed mode with:

```shell
cargo run -p datafusion-distributed-benchmarks --release -- run --workers 8000,8001,8002 --path benchmarks/data/tpch_sf1
```

A good way of measuring the impact of distribution is to limit the physical threads each worker can use. For example,
it's expected that running 8 workers with 2 physical threads each one (8 * 2 = 16 total) is faster than running in
single-node with just 2 threads (1 * 3 = 2 total).

```shell
cargo run -p datafusion-distributed-benchmarks --release -- run --threads 2 --spawn 8000 & 
cargo run -p datafusion-distributed-benchmarks --release -- run --threads 2 --spawn 8001 & 
cargo run -p datafusion-distributed-benchmarks --release -- run --threads 2 --spawn 8002 & 
cargo run -p datafusion-distributed-benchmarks --release -- run --threads 2 --spawn 8003 & 
cargo run -p datafusion-distributed-benchmarks --release -- run --threads 2 --spawn 8004 & 
cargo run -p datafusion-distributed-benchmarks --release -- run --threads 2 --spawn 8005 & 
cargo run -p datafusion-distributed-benchmarks --release -- run --threads 2 --spawn 8006 & 
cargo run -p datafusion-distributed-benchmarks --release -- run --threads 2 --spawn 8007 & 
```

```shell
cargo run -p datafusion-distributed-benchmarks --release -- run --threads 2 --workers 8000,8001,8002,8003,8004,8005,8006,8007 --path benchmarks/data/tpch_sf1
```

The `run.sh` script already does this for you in a more ergonomic way:

```shell
WORKERS=8 run.sh --threads 2 --path benchmarks/data/tpch_sf1
```