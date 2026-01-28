# datafusion distributed console quickstart

Run queries with console integration

This example demonstrates how to register workers with the console and execute queries
while monitoring task progress through the console TUI.

Usage:

# Terminal 1: Start console in IDLE mode

```bash
  cargo run -p datafusion-distributed-console
```

# Terminal 2-5: Start workers with observability

```bash
  cargo run -p datafusion-distributed-console --example console_worker -- 8080
  cargo run -p datafusion-distributed-console --example console_worker -- 8081
  cargo run -p datafusion-distributed-console --example console_worker -- 8082
  cargo run -p datafusion-distributed-console --example console_worker -- 8083
```

# Terminal 6: Run query with console integration

```bash
  cargo run -p datafusion-distributed-console --example console_run -- \
    "SELECT * FROM weather LIMIT 100" \
    --cluster-ports 8080,8081,8082,8083
```
