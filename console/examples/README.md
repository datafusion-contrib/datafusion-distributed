# Console Examples

This directory contains examples for the DataFusion Distributed Console, which
provides real-time monitoring of distributed query execution.

## Examples

### console_worker - Simple Worker with Observability

A basic example that runs a worker instance with observability service enabled.

#### Run the worker

In one terminal from the repository root:

```bash
cargo run -p datafusion-distributed-console --example console_worker
```

The worker listens on `127.0.0.1:8080` by default. You can specify a custom port
as an argument (e.g., `8081`).

#### Run the console

In a second terminal from the repository root:

```bash
# Single worker (default port 8080)
cargo run -p datafusion-distributed-console

# Multiple workers (specify ports)
cargo run -p datafusion-distributed-console -- --cluster-ports 8080,8081,8082,8083
```

The console will display a TUI (Terminal User Interface) with:

### Features

- **Multi-Worker Support**: Monitor multiple workers simultaneously with `--cluster-ports`
- **Real-time Updates**: Console refreshes every 10ms to show live progress
- **Visual Progress Bars**: Each task displays a progress gauge showing
  partition completion
- **Status Indicators**:
  - `CONNECTING` (blue) - Attempting to connect to worker
  - `IDLE` (yellow) - Worker is connected but has no active tasks
  - `ACTIVE` (green) - Worker is processing tasks
  - `DISCONNECTED` (red) - Cannot connect to worker (auto-reconnects every 5 seconds)
- **Cluster Statistics**: View total active, idle, and disconnected workers
- **Task Details**: Shows query ID, stage number, task number, and partition
  progress per worker
- **Parallel Polling**: Non-blocking concurrent updates for all workers
- **Auto-reconnect**: Disconnected workers automatically retry connection
- **Keyboard Controls**: Press `q` or `ESC` to quit the console

---

### tpcds_runner - TPC-DS Query Runner with Observability

A comprehensive example that runs TPC-DS benchmark queries against multiple
workers with full observability, allowing you to monitor distributed query
execution in real-time.

This is the recommended example for seeing the console in action with realistic
workloads.

#### Quick Start

See `tpcds_runner.md` in this directory for detailed documentation, or see `QUICKSTART_CONSOLE.md` in the repository root for a quick start guide.

- Starts workers with observability service enabled
- Runs TPC-DS benchmark queries (single or all)
- Demonstrates real-time task progress monitoring
- Shows partition-level completion tracking
- Supports configurable scale factors and partitions

#### Example Usage

```bash
# Terminal 1-4: Start workers
cargo run -p datafusion-distributed-console --example tpcds_runner -- worker --port 8080
cargo run -p datafusion-distributed-console --example tpcds_runner -- worker --port 8081
cargo run -p datafusion-distributed-console --example tpcds_runner -- worker --port 8082
cargo run -p datafusion-distributed-console --example tpcds_runner -- worker --port 8083

# Terminal 5: Start console (monitor all 4 workers)
cargo run -p datafusion-distributed-console -- --cluster-ports 8080,8081,8082,8083

# Terminal 6: Run query
cargo run -p datafusion-distributed-console --example tpcds_runner --features tpcds -- run \
  --cluster-ports 8080,8081,8082,8083 \
  --query q72
```

Watch the console (Terminal 5) to see real-time task progress as the query executes across workers!
