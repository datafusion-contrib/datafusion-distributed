# Cluster Example

Starts an in-memory cluster of DataFusion distributed workers with observability
enabled. This is the fastest way to get a local cluster running for use with the
console TUI or the TPC-DS runner.

## Usage

### Start a cluster with OS-assigned ports

```bash
cargo run -p datafusion-distributed-console --example cluster
```

This starts 16 workers on random ports and prints the commands to connect.

### Customize the cluster

```bash
cargo run -p datafusion-distributed-console --example cluster -- \
  --workers 8 \
  --base-port 9000
```

| Flag            | Default | Description                                              |
|-----------------|---------|----------------------------------------------------------|
| `--workers`     | 16      | Number of workers to start                               |
| `--base-port`   | 0       | Starting port (0 = OS-assigned random ports)             |

When `--base-port` is set, workers bind to consecutive ports starting from that
value (e.g. 9000, 9001, ..., 9007).

## Connecting to the cluster

After starting, the example prints ready-to-use commands. For example:

```
Started 4 workers on ports: 9000,9001,9002,9003

Console:
    cargo run -p datafusion-distributed-console -- --connect http://localhost:9000
TPC-DS runner:
    cargo run -p datafusion-distributed-console --example tpcds_runner -- --cluster-ports 9000,9001,9002,9003
Single query:
    cargo run -p datafusion-distributed-console --example console_run -- --cluster-ports 9000,9001,9002,9003 "SELECT 1"
```

Note: if one of the workers is on the default port (9001), you can just run
`cargo run -p datafusion-distributed-console` without any arguments.

Press `Ctrl+C` to stop all workers.
