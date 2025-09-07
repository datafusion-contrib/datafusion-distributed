# Distributed DataFusion Benchmarks

### Generating TPCH data

Generate TPCH data into the `data/` dir

```shell
./gen-tpch.sh
```

### Running TPCH benchmarks in single-node mode

After generating the data with the command above, the benchmarks can be run with

```shell
cargo run -p datafusion-distributed-benchmarks --release -- tpch
```

For preloading the TPCH data in-memory, the `-m` flag can be passed

```shell
cargo run -p datafusion-distributed-benchmarks --release -- tpch -m
```

For running the benchmarks with using just a specific amount of physical threads:

```shell
cargo run -p datafusion-distributed-benchmarks --release -- tpch -m --threads 3
```

### Running TPCH benchmarks in distributed mode

Running the benchmarks in distributed mode implies:

- running 1 or more workers in separate terminals
- running the benchmarks in an additional terminal

The workers can be spawned by passing the `--spawn <port>` flag, for example, for spawning 3 workers:

```shell
cargo run -p datafusion-distributed-benchmarks --release -- tpch --spawn 8000
```

```shell
cargo run -p datafusion-distributed-benchmarks --release -- tpch --spawn 8001
```

```shell
cargo run -p datafusion-distributed-benchmarks --release -- tpch --spawn 8002
```

With the three workers running in separate terminals, the TPCH benchmarks can be run in distributed mode with:

```shell
cargo run -p datafusion-distributed-benchmarks --release -- tpch --workers 8000,8001,8002
```

A good way of measuring the impact of distribution is to limit the physical threads each worker can use. For example,
it's expected that running 8 workers with 2 physical threads each one (8 * 2 = 16 total) is faster than running in
single-node with just 2 threads (1 * 3 = 2 total).

```shell
cargo run -p datafusion-distributed-benchmarks --release -- tpch -m --threads 2 --spawn 8000 & 
cargo run -p datafusion-distributed-benchmarks --release -- tpch -m --threads 2 --spawn 8001 & 
cargo run -p datafusion-distributed-benchmarks --release -- tpch -m --threads 2 --spawn 8002 & 
cargo run -p datafusion-distributed-benchmarks --release -- tpch -m --threads 2 --spawn 8003 & 
cargo run -p datafusion-distributed-benchmarks --release -- tpch -m --threads 2 --spawn 8004 & 
cargo run -p datafusion-distributed-benchmarks --release -- tpch -m --threads 2 --spawn 8005 & 
cargo run -p datafusion-distributed-benchmarks --release -- tpch -m --threads 2 --spawn 8006 & 
cargo run -p datafusion-distributed-benchmarks --release -- tpch -m --threads 2 --spawn 8007 & 
```

```shell
cargo run -p datafusion-distributed-benchmarks --release -- tpch -m --threads 2 --workers 8000,8001,8002,8003,8004,8005,8006,8007
```

The `run.sh` script already does this for you in a more ergonomic way:

```shell
WORKERS=8 THREADS=2 ./run.sh
```