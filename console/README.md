# datafusion-distributed-console

A terminal UI (TUI) for monitoring [DataFusion Distributed](../README.md)
clusters in real time. Built with [ratatui](https://ratatui.rs).

## Quick-start

```bash
# Start a local cluster (16 workers on ports 9001-9016)
cargo run -p datafusion-distributed-console --example cluster

# In another terminal, open the console (connect to any worker port)
cargo run -p datafusion-distributed-console -- 9001
```

The console requires a port argument and auto-discovers all workers in the
cluster via the `GetClusterWorkers` RPC.

## Usage

```
datafusion-distributed-console <PORT> [OPTIONS]
```

| Argument / Flag    | Required | Description                                          |
|--------------------|----------|------------------------------------------------------|
| `PORT`             | Yes      | Port of a seed worker for auto-discovery             |
| `--poll-interval`  | No       | Polling interval in milliseconds (default: 100)      |

## Views

### Cluster Overview (`1`)

A table of all workers showing connection status, active tasks, queries in
flight, CPU usage, memory, and throughput. Columns are sortable.

### Worker Detail (`2`)

Drill into a single worker to see per-task progress (active and completed),
CPU/memory sparklines, and task durations.

## Worker Discovery

The console uses a single seed port to discover the full cluster.
On startup and every 5 seconds, it calls `GetClusterWorkers` on the seed worker,
which returns URLs for all known workers via its `WorkerResolver`. New workers
are added automatically; removed workers are cleaned up.

# Run — connect to the local worker on port 9001
datafusion-distributed-console 9001
```

## Examples

| Example                                          | Description                                    |
|--------------------------------------------------|------------------------------------------------|
| [`cluster`](examples/cluster.md)                 | Start a local multi-worker cluster             |
| [`console_worker`](examples/console.md)          | Start individual workers with observability    |
| [`tpcds_runner`](examples/tpcds_runner.md)       | Run TPC-DS queries with live monitoring        |
