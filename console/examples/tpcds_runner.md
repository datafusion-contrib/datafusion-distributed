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

### Step 1: Start Workers with Observability (4 terminals)

Start 4 workers on different ports:

**Terminal 1:**

```bash
cargo run -p datafusion-distributed-console --example tpcds_runner -- worker --port 8080
```

**Terminal 2:**

```bash
cargo run -p datafusion-distributed-console --example tpcds_runner -- worker --port 8081
```

**Terminal 3:**

```bash
cargo run -p datafusion-distributed-console --example tpcds_runner -- worker --port 8082
```

**Terminal 4:**

```bash
cargo run -p datafusion-distributed-console --example tpcds_runner -- worker --port 8083
```

### Step 2: Start the Console (Terminal 5)

The console monitors worker on port 8080:

```bash
cargo run -p datafusion-distributed-console --cluster-ports 8080,8081,8082,8083
```

You should see the console TUI showing "IDLE" status.

### Step 3: Run TPC-DS Queries (Terminal 6)

#### Run a single query

```bash
cargo run -p datafusion-distributed-console --example tpcds_runner --features tpcds -- run \
  --cluster-ports 8080,8081,8082,8083 \
  --query q1
```

#### Run all TPC-DS queries sequentially

```bash
cargo run -p datafusion-distributed-console --example tpcds_runner --features tpcds -- run \
  --cluster-ports 8080,8081,8082,8083 \
  --query all
```

#### Run with different scale factor

```bash
cargo run -p datafusion-distributed-console --example tpcds_runner --features tpcds -- run \
  --cluster-ports 8080,8081,8082,8083 \
  --scale-factor 0.1 \
  --parquet-partitions 2 \
  --query q1
```
