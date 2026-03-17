# datafusion-distributed-console

A terminal UI (TUI) for monitoring [DataFusion Distributed](../README.md)
clusters in real time. Built with [ratatui](https://ratatui.rs).

## Quick-start

```bash
# Start a local cluster (16 workers on ports 9001-9016)
cargo run -p datafusion-distributed-console --example cluster

# In another terminal, open the console
cargo run -p datafusion-distributed-console
```

The console connects to `localhost:9001` by default and auto-discovers all
workers in the cluster via the `GetClusterWorkers` RPC.

## Usage

```
datafusion-distributed-console [OPTIONS]
```

| Flag               | Default                    | Description                                          |
|--------------------|----------------------------|------------------------------------------------------|
| `--connect <URL>`  | `http://localhost:9001`    | Seed worker URL for auto-discovery                   |
| `--poll-interval`  | `100`                      | Polling interval in milliseconds                     |

## Views

### Cluster Overview (`1`)

A table of all workers showing connection status, active tasks, queries in
flight, CPU usage, memory, and throughput. Columns are sortable.

### Worker Detail (`2`)

Drill into a single worker to see per-task progress (active and completed),
CPU/memory sparklines, and task durations.

## Worker Discovery

The console uses a single seed URL (`--connect`) to discover the full cluster.
On startup and every 5 seconds, it calls `GetClusterWorkers` on the seed worker,
which returns URLs for all known workers via its `WorkerResolver`. New workers
are added automatically; removed workers are cleaned up.

## Monitoring an EC2 Benchmark Cluster

The benchmark workers in [`benchmarks/cdk/`](../benchmarks/cdk/README.md) run on
EC2 instances with the observability service enabled. Each worker listens on port
9001 (gRPC/Flight + Observability) and port 9000 (HTTP benchmarks). The
`Ec2WorkerResolver` discovers peers via `DescribeInstances`, so connecting the
console to any single worker exposes the full cluster.

To run the console, SSH into any instance in the cluster and install it there
(the console runs inside the VPC so it can reach all workers on their private IPs):

```bash
cd benchmarks/cdk/
npm run deploy

# Connect to an instance via SSM
aws ssm start-session --target "$INSTANCE_ID" --region "$AWS_REGION"

# Install the Rust toolchain
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source "$HOME/.cargo/env"

# Install the console binary from the repo
cargo install --locked --git https://github.com/datafusion-contrib/datafusion-distributed.git \
  datafusion-distributed-console

# Run — auto-discovers all workers via localhost:9001
datafusion-distributed-console
```

To connect to a specific worker (e.g. one on a different port or IP):

```bash
datafusion-distributed-console --connect http://<worker-private-ip>:9001
```

## Examples

| Example                                          | Description                                    |
|--------------------------------------------------|------------------------------------------------|
| [`cluster`](examples/cluster.md)                 | Start a local multi-worker cluster             |
| [`console_worker`](examples/console.md)          | Start individual workers with observability    |
| [`tpcds_runner`](examples/tpcds_runner.md)       | Run TPC-DS queries with live monitoring        |
