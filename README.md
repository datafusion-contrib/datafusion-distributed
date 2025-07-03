# Distributed DataFusion

[![Apache licensed][license-badge]][license-url]

[license-badge]: https://img.shields.io/badge/license-Apache%20v2-blue.svg
[license-url]: https://github.com/datafusion-contrib/datafusion-distributed/blob/main/LICENSE

## Overview

Distributed DataFusion is a distributed execution framework that enables DataFusion DataFrame and SQL queries to run in a distributed fashion. This project provides the infrastructure to scale DataFusion workloads across multiple nodes in a cluster.

This is an open source version of the distributed DataFusion prototype, extracted from DataDog's internal implementation and made available to the community.

## Key Features

- **Distributed SQL Execution**: Execute SQL queries across multiple nodes
- **Arrow Flight Integration**: High-performance data transfer using Arrow Flight
- **Streaming Execution**: Pipeline execution model for efficient resource utilization
- **Protocol Buffers**: Efficient serialization for distributed communication
- **Kubernetes Support**: Native integration with Kubernetes for cluster management

## Architecture

DataFusion Distributed implements a master-worker architecture for distributed SQL query execution:

```
      ┌────────────────────────┐
      │ Client (issues SQL)    │
      └────────────────────────┘
                 │
                 ▼
  ┌────────────────────────────────────┐
  │      ┌───────────────┐             │
  │      │     Proxy     │             │
  │      │   (Master)    │             │
  │      └───────────────┘             │
  │        │            │              │
  │        ▼            ▼              │
  │  ┌─────────┐    ┌─────────┐        │
  │  │ Worker  │    │ Worker  │        │
  │  │    1    │    │    2    │        │  
  │  └─────────┘    └─────────┘        │
  │                                    │
  │  DataFusion Distributed Cluster    │
  └────────────────────────────────────┘
```

### Key Components

- **Proxy (Master Node)**: Receives queries from clients, generates distributed execution plans, coordinates worker nodes, and streams results back to clients
- **Workers**: Execute assigned portions of the query plan and return results to the proxy
- **Arrow Flight**: High-performance data transfer protocol between nodes
- **Query Planning**: Distributed query planning that breaks queries into stages
- **Stage Execution**: Individual stages that can run on different nodes
- **Kubernetes Integration**: Cluster management and node discovery

## Getting Started

### Prerequisites

- Rust 1.82 or later
- Protocol Buffers compiler (protoc)
- (Optional) Kubernetes cluster for distributed execution

### Installation

#### Installing Rust

If you don't have Rust installed, you can install it using [rustup](https://www.rust-lang.org/tools/install) (the official Rust installer):

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

After installation, restart your terminal or run:
```bash
source ~/.cargo/env
```

#### Installing Protocol Buffers Compiler ([protoc](https://github.com/protocolbuffers/protobuf/releases))

```bash
brew install protobuf
```

**Verify installation:**
```bash
protoc --version
```

### Building

#### Using the Build Script (Recommended)

```bash
# Build in debug mode
./build.sh

# Build in release mode (optimized)
./build.sh --release
```

**Clean Rebuild**: If you need to completely clean and rebuild (removes all build artifacts):

```bash
# Clean rebuild in debug mode
./clean_and_build.sh

# Clean rebuild in release mode (optimized)
./clean_and_build.sh --release
```

#### Using Cargo Directly

You can also build the project directly with Cargo (the build.rs script will automatically handle Protocol Buffer compilation):

```bash
# Build in debug mode
cargo build
```

```bash
# Build in release mode (optimized)  
cargo build --release
```

**Clean Build Artifacts**: To clean previous build artifacts before rebuilding:

```bash
# Clean all build artifacts (removes target/ directory contents)
cargo clean

# Then rebuild
cargo build --release
```

**Note**: Both commands, `build.sh` script and `cargo` automatically invoke `build.rs`, which handles Protocol Buffer compilation before building the main crate. The main advantage of using `./build.sh` is the user-friendly output and usage examples it provides.

### Running Tests

Run all tests:

```bash
cargo test
```

Run tests with output:

```bash
cargo test -- --nocapture
```

## Usage

With the code now built and ready, the next step is to set up the server and execute queries. To do that, we'll need a schema and some data—so for this example, we'll use the TPC-H schema and queries.

### Setting up TPC-H Data

You need TPC-H data in Parquet format. We recommend using [tpchgen-rs](https://github.com/clflushopt/tpchgen-rs), known as [the world's fastest open-source TPC-H data generator](https://datafusion.apache.org/blog/2025/04/10/fastest-tpch-generator/).

#### Install tpchgen-cli

Ensure you have Rust installed (if not, run: `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`), then:

```bash
cargo install tpchgen-cli
```

#### Generate TPC-H Data

Generate the TPC-H dataset scale factor 1 and save them as parquet files to `/tmp/tpch_s1/`:

```bash
mkdir -p /tmp/tpch_s1 && cd $_
tpchgen-cli -s 1 --format=parquet
```

### Automated Server and Client Setup (Recommended)

```bash
# Launch a server cluster with default settings (2 workers, /tmp/tpch_s1 data, and log files in current folder)
./scripts/launch_tpch_cluster.sh

# Launch a server cluster with custom settings
./scripts/launch_tpch_cluster.sh num_workers=3 tpch_file_path=/path/to/tpch/data log_file_path=./logs
```

Once the cluster is running, use the Python ArrowFlightSQL client to execute queries:

```bash
# In a separate terminal, launch the query client
./scripts/launch_python_arrowflightsql_client.sh

# Or with custom query path
./scripts/launch_python_arrowflightsql_client.sh query_path=./tpch/queries/
```

The client script automatically sets up a Python virtual environment and installs required packages:
- `adbc_driver_manager` - Arrow Database Connectivity driver manager
- `adbc_driver_flightsql` - ArrowFlightSQL driver for ADBC
- `duckdb` - For result display and formatting
- `pyarrow` - Arrow Python bindings

Below are a few common commands for executing your queries:

- `list_queries` : list all queries in `./tpch/queries/`
- `show_query('q1')` : show content of `q1`
- `run_query('q1')` : execute `q1`
- `explain_query('q1')` : show explain of `q1`
- `my_query = "SELECT * FROM nation LIMIT 5"` : assign a custom query
- `run_sql(my_query)` : run a custom query
- `run_sql("SELECT * FROM nation LIMIT 5")` : run a custom query
- `explain_sql(my_query)` : explain a custom query
- `exit()` : quit the client

### Manual Server and Client Setup

#### Manual Server Setup

To manually set up a distributed cluster, start workers first, then the proxy:

**Step 1: Start Workers**

In separate terminal windows, start two workers:

```bash
# Terminal 1 - Start first worker
DATAFUSION_RAY_LOG_LEVEL=trace ./target/release/distributed-datafusion --mode worker --port 20201

# Terminal 2 - Start second worker  
DATAFUSION_RAY_LOG_LEVEL=trace ./target/release/distributed-datafusion --mode worker --port 20202
```

**Step 2: Start Proxy**

In another terminal, start the proxy connecting to both workers:

```bash
# Terminal 3 - Start proxy connected to workers
DATAFUSION_RAY_LOG_LEVEL=trace DFRAY_WORKER_ADDRESSES=worker1/localhost:20201,worker2/localhost:20202 ./target/release/distributed-datafusion --mode proxy --port 20200
```

To make your cluster aware of specific table schemas, you’ll need to define a new environment variable, DFRAY_TABLES, when starting each worker and proxy. This variable should specify tables whose data is stored in Parquet files.For example, the following setup registers two tables—customer and nation—along with their corresponding data sources.

```bash
DFRAY_TABLES=customer:parquet:/tmp/tpch_s1/customer.parquet,nation:parquet:/tmp/tpch_s1/nation.parquet DATAFUSION_RAY_LOG_LEVEL=trace ./target/release/distributed-datafusion --mode worker --port 20201

DFRAY_TABLES=customer:parquet:/tmp/tpch_s1/customer.parquet,nation:parquet:/tmp/tpch_s1/nation.parquet DATAFUSION_RAY_LOG_LEVEL=trace ./target/release/distributed-datafusion --mode worker --port 20202

DFRAY_TABLES=customer:parquet:/tmp/tpch_s1/customer.parquet,nation:parquet:/tmp/tpch_s1/nation.parquet DATAFUSION_RAY_LOG_LEVEL=trace DFRAY_WORKER_ADDRESSES=worker1/localhost:20201,worker2/localhost:20202 ./target/release/distributed-datafusion --mode proxy --port 20200
```

**Using Views:**

To pre-create views that queries can reference (such as for TPC-H q15), you can use the `DFRAY_VIEWS` environment variable:

```bash
# Example: Create a view for TPC-H q15 revenue calculation
DFRAY_VIEWS="CREATE VIEW revenue0 (supplier_no, total_revenue) AS SELECT l_suppkey, sum(l_extendedprice * (1 - l_discount)) FROM lineitem WHERE l_shipdate >= date '1996-08-01' AND l_shipdate < date '1996-08-01' + interval '3' month GROUP BY l_suppkey"

# Use both tables and views in your cluster
DFRAY_TABLES=customer:parquet:/tmp/tpch_s1/customer.parquet,lineitem:parquet:/tmp/tpch_s1/lineitem.parquet,supplier:parquet:/tmp/tpch_s1/supplier.parquet DFRAY_VIEWS="$DFRAY_VIEWS" DATAFUSION_RAY_LOG_LEVEL=trace ./target/release/distributed-datafusion --mode worker --port 20201
```

#### Manual Client Setup

You can now connect a client to the proxy at `localhost:20200` to execute queries across the distributed cluster.

```bash
# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install required packages
pip install adbc_driver_manager adbc_driver_flightsql duckdb pyarrow

# Start Python and connect to the cluster
python3
```

```python
import adbc_driver_flightsql.dbapi as dbapi
import duckdb

# Connect to the distributed cluster
conn = dbapi.connect("grpc://localhost:20200")
cur = conn.cursor()

# Execute a simple query
cur.execute("SELECT 1")
result = cur.fetch_arrow_table()
print(result)

# Execute a query with results displayed via DuckDB
cur.execute("SELECT * FROM nation LIMIT 5")
reader = cur.fetch_record_batch()
duckdb.sql("SELECT * FROM reader").show()
```



## Single Node Usage

For development or testing, you can run individual components:

```bash
# Single worker (no distributed queries)
./target/release/distributed-datafusion --mode worker --port 20201

# Proxy without workers (limited functionality)
./target/release/distributed-datafusion --mode proxy --port 20200
```

View Available Options

```bash
./target/release/distributed-datafusion --help
```

The system supports various configuration options through environment variables:

- `DATAFUSION_RAY_LOG_LEVEL`: Set logging level (default: WARN)
- `DFRAY_TABLES`: Comma-separated list of tables in format `name:format:path`
- `DFRAY_VIEWS`: Semicolon-separated list of CREATE VIEW SQL statements

## TPC-H Query Validation

To validate that your distributed cluster is working correctly, you can use the automated validation script that compares results between DataFusion CLI (single-node) and the distributed system:

```bash
# Run validation with default settings (2 workers, /tmp/tpch_s1 data)
./scripts/validate_tpch_correctness.sh

# Run validation with custom settings
./scripts/validate_tpch_correctness.sh num_workers=3 tpch_file_path=/path/to/tpch/data log_file_path=./logs query_path=./tpch/queries/
```

**Key Features:**
- **Automated Setup**: Installs `datafusion-cli` and `tpchgen-cli` if missing
- **Data Generation**: Creates TPC-H data automatically if not found
- **Smart Validation**: Compares all 22 TPC-H queries with floating-point tolerance
- **Cluster Detection**: Uses existing cluster or launches a new one
- **Detailed Reporting**: Generates comprehensive validation reports

**Example Output:**
```
==============================================================================
TPC-H Correctness Validation
==============================================================================
Configuration:
  - Workers: 2
  - TPC-H Data Directory: /tmp/tpch_s1
  - Query Path: ./tpch/queries/
  - Proxy Port: 20200

[SUCCESS] q1: Results match ✓ (within floating-point tolerance)
[SUCCESS] q6: Results match ✓ (within floating-point tolerance)
...

==============================================================================
Validation Summary
==============================================================================
Total queries tested: 22
Passed: 20
Failed: 2
Success rate: 90%

Detailed report: ./logs/validation_results/validation_report.txt
Result files: ./logs/validation_results
```

The script will warn you if the running cluster has a different number of workers than requested, and automatically handles missing dependencies and data generation.

<!-- TODO: Merge this section into the above -->
## TPC-H Validation Tests

The project includes comprehensive TPC-H validation tests that automatically compare results between regular DataFusion and distributed DataFusion to ensure correctness. These tests are completely self-contained and handle all setup automatically:

```bash
# Run all TPC-H validation tests (fully automated)
cargo test --lib tpch_validation_tests -- --nocapture

# Run single query test for debugging
cargo test --lib test_tpch_validation_single_query -- --ignored --nocapture
```

**What the tests do automatically:**
- ✅ Kill existing processes on ports 40400-40402
- ✅ Install `tpchgen-cli` if not available  
- ✅ Generate TPC-H data at `/tmp/tpch_s1` if not present
- ✅ Start distributed cluster (1 proxy + 2 workers)
- ✅ Run all 22 TPC-H queries on both systems
- ✅ Compare results with floating-point tolerance
- ✅ Clean up cluster processes

**Architecture:**
- **Proxy**: Port 40400
- **Worker 1**: Port 40401
- **Worker 2**: Port 40402
- **TPC-H Data**: `/tmp/tpch_s1` (scale factor 1)

No prerequisites needed - just run `cargo test --lib tpch_validation_tests -- --nocapture` and everything is handled automatically!

## Development

### Project Structure

- `src/`: Main source code
  - `proxy_service.rs`: Proxy service for query distribution
  - `processor_service.rs`: Worker node processing logic
  - `planning.rs`: Query planning and stage creation
  - `flight.rs`: Arrow Flight service implementation
  - `codec.rs`: Serialization/deserialization for distributed execution
  - `k8s.rs`: Kubernetes integration
- `scripts/`: Utility scripts
  - `launch_tpch_cluster.sh`: Launch distributed TPC-H benchmark cluster
  - `launch_python_arrowflightsql_client.sh`: Launch Python query client
  - `build_and_push_docker.sh`: Docker build and push script
  - `python_tests.sh`: Python test runner
- `tpch/queries/`: TPC-H benchmark SQL queries
- `testdata/`: Test data files
- `k8s/`: Kubernetes deployment files

### Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## License

This project is licensed under the Apache License 2.0. See the [LICENSE](LICENSE) file for details.

## Acknowledgments

This project was originally developed at DataDog and has been donated to the open source community.
