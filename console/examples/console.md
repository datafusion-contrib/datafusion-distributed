# DataFusion Distributed Console Quick-start

This example demonstrates how to register workers with the console and execute queries
while monitoring task progress through the console TUI.

Usage:

## Terminal 1: Start console

```bash
  cargo run -p datafusion-distributed-console
```

The TUI console will start with a message saying "Waiting for worker registration..."

## Terminal 2-3: Start workers with observability

```bash
  cargo run -p datafusion-distributed-console --example console_worker -- 8080
  cargo run -p datafusion-distributed-console --example console_worker -- 8081
```

## Terminal 4: Run query with console integration

```bash
  cargo run -p datafusion-distributed-console --example console_run -- \
    "SELECT * FROM weather LIMIT 100" \
    --cluster-ports 8080,8081
```
