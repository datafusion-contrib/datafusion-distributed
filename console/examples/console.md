# DataFusion Distributed Console Quick-start

This example demonstrates how to monitor task progress through the console TUI
while running distributed queries.

## Terminal 1: Start workers with observability

```bash
  cargo run -p datafusion-distributed-console --example console_worker -- 8080
  cargo run -p datafusion-distributed-console --example console_worker -- 8081
```

## Terminal 2: Start console

```bash
  cargo run -p datafusion-distributed-console -- --cluster-ports 8080,8081
```

The TUI console will start and connect to the workers. It will show "Waiting for tasks..."
until queries are executed.

## Terminal 3: Run a query

```bash
  cargo run -p datafusion-distributed-console --example console_run -- \
    "SELECT * FROM weather LIMIT 100" \
    --cluster-ports 8080,8081
```

The console will display real-time task progress across all workers.
