# Cluster Example

Starts an in-memory cluster of DataFusion distributed workers with observability
enabled. This is the fastest way to get a local cluster running for use with the
console TUI or the TPC-DS runner.

## Quick-start

```bash
# Terminal 1 — start 16 workers
cargo run -p datafusion-distributed-console --example cluster

# Terminal 2 — open the console (auto-discovers all workers)
cargo run -p datafusion-distributed-console
```

No flags needed. The cluster connects to a worker on port 9001 by default.

## Usage

```bash
cargo run -p datafusion-distributed-console --example cluster
```

This starts 16 workers on ports 9001 through 9016 and prints the commands to connect.

### Customize the cluster

```bash
cargo run -p datafusion-distributed-console --example cluster -- \
  --workers 8 \
  --base-port 5000
```

| Flag            | Default | Description                                              |
|-----------------|---------|----------------------------------------------------------|
| `--workers`     | 16      | Number of workers to start                               |
| `--base-port`   | 9001    | Starting port; workers bind to consecutive ports         |

Workers bind to consecutive ports starting from `--base-port`
(e.g. `--base-port 5000` gives 5000, 5001, ..., 5015 for 16 workers).

If you change the base port, tell the console which worker to connect to:

```bash
cargo run -p datafusion-distributed-console -- --connect http://localhost:5000
```

## Connecting to the cluster

After starting, the example prints ready-to-use commands. For example:

```
Started 16 workers on ports: 9001,9002,...,9016

Console (connect to any worker for auto-discovery):
    cargo run -p datafusion-distributed-console -- --connect http://localhost:9001
TPC-DS runner:
    cargo run -p datafusion-distributed-console --example tpcds_runner -- --cluster-ports 9001,9002,...,9016
Single query:
    cargo run -p datafusion-distributed-console --example console_run -- --cluster-ports 9001,9002,...,9016 "SELECT 1"
```

Press `Ctrl+C` to stop all workers.
