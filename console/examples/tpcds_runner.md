# TPC-DS Runner with Observability

This example runs TPC-DS queries against workers with observability enabled,
allowing you to monitor task progress in real-time using the console.

## Prerequisites

1. Install git-lfs if not already installed:

```bash
git lfs install
git lfs checkout
```

1. The example will automatically generate TPC-DS test data on first run (this
may take a few minutes).

## Usage

### Step 1: Start Workers with Observability (Terminals 1-4)

Start 4 workers on different ports in different terminals:

```bash
cargo run -p datafusion-distributed-console --example console_worker
cargo run -p datafusion-distributed-console --example console_worker -- 9002
cargo run -p datafusion-distributed-console --example console_worker -- 9003
cargo run -p datafusion-distributed-console --example console_worker -- 9004
```

### Step 2: Start the Console (Terminal 5)

```bash
cargo run -p datafusion-distributed-console
```

The console auto-discovers all workers via the default port (9001).

### Step 3: Run TPC-DS Queries (Terminal 6)

#### Run a single query

```bash
cargo run -p datafusion-distributed-console --example tpcds_runner -- \
  --cluster-ports 9001,9002,9003,9004 \
  --query q99
```

#### Run all TPC-DS queries sequentially

```bash
cargo run -p datafusion-distributed-console --example tpcds_runner -- \
  --cluster-ports 9001,9002,9003,9004
```
