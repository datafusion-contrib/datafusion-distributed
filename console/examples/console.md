# DataFusion Distributed Console Quick-start

This example demonstrates how to monitor task progress through the console TUI
while running distributed queries.

## Terminal 1: Start workers with observability

The easiest way is to use the cluster example, which starts 16 workers on ports
9001-9016 (see [cluster.md](cluster.md)):

```bash
  cargo run -p datafusion-distributed-console --example cluster
```

Or start individual workers manually:

```bash
  cargo run -p datafusion-distributed-console --example console_worker -- 9001
  cargo run -p datafusion-distributed-console --example console_worker -- 9002
```

## Terminal 2: Start console

```bash
  cargo run -p datafusion-distributed-console -- 9001
```

The console auto-discovers all workers in the cluster via `GetClusterWorkers`.
It will show "Waiting for tasks..." until queries are executed.

To connect to a worker on a different port:

```bash
  cargo run -p datafusion-distributed-console -- 9002
```

## Terminal 3: Run a query

```bash
  cargo run -p datafusion-distributed-console --example console_run -- \
    "SELECT * FROM weather LIMIT 100" \
    --cluster-ports 9001,9002
```

The console will display real-time task progress across all workers.
