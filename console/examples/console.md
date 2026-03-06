# DataFusion Distributed Console Quick-start

This example demonstrates how to monitor task progress through the console TUI
while running distributed queries.

## Terminal 1: Start workers with observability

```bash
  cargo run -p datafusion-distributed-console --example console_worker
  cargo run -p datafusion-distributed-console --example console_worker -- 9002
```

The first worker starts on the default port (9001). The second on 9002.

## Terminal 2: Start console

```bash
  cargo run -p datafusion-distributed-console
```

The console connects to `localhost:9001` by default and auto-discovers all
workers in the cluster via `GetClusterWorkers`. It will show "Waiting for
tasks..." until queries are executed.

To connect to a worker on a non-default port, use `--connect`:

```bash
  cargo run -p datafusion-distributed-console -- --connect http://localhost:9002
```

## Terminal 3: Run a query

```bash
  cargo run -p datafusion-distributed-console --example console_run -- \
    "SELECT * FROM weather LIMIT 100" \
    --cluster-ports 9001,9002
```

The console will display real-time task progress across all workers.
