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

# Run — connect to the local worker on port 9001
datafusion-distributed-console 9001
```

## Examples

| Example                                          | Description                                    |
|--------------------------------------------------|------------------------------------------------|
| [`cluster`](examples/cluster.md)                 | Start a local multi-worker cluster             |
| [`console_worker`](examples/console.md)          | Start individual workers with observability    |
| [`tpcds_runner`](examples/tpcds_runner.md)       | Run TPC-DS queries with live monitoring        |
