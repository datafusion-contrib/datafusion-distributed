#!/bin/bash

set -euo pipefail

# Script to launch a DataFusion Distributed TPC-H cluster with configurable parameters
#
# Usage:
#   ./launch_tpch_cluster.sh [num_workers=N] [tpch_file_path=PATH] [log_file_path=PATH]
#
# Arguments:
#   num_workers      Number of worker nodes (default: 2)
#   tpch_file_path   Path to TPC-H parquet files (default: /tmp/tpch_s1)
#   log_file_path    Directory for log files (default: current directory)
#
# Examples:
#   # Run with all defaults (2 workers, /tmp/tpch_s1, current directory for logs)
#   ./launch_tpch_cluster.sh
#
#   # Run with 3 workers, default TPC-H path and logs
#   ./launch_tpch_cluster.sh num_workers=3
#
#   # Run with custom TPC-H data path
#   ./launch_tpch_cluster.sh tpch_file_path=/path/to/tpch/data
#
#   # Run with all parameters specified
#   ./launch_tpch_cluster.sh num_workers=3 tpch_file_path=/tmp/tpch_s1 log_file_path=./logs
#
# Notes:
#   - TPC-H data must be in parquet format
#   - Required parquet files: customer, lineitem, nation, orders, part, partsupp, region, supplier
#   - Log files will be created in the specified log_file_path directory

# Default values
DEFAULT_NUM_WORKERS=2
DEFAULT_TPCH_PATH="/tmp/tpch_s1"
DEFAULT_LOG_PATH="."

# Parse named arguments
for arg in "$@"; do
    case "$arg" in
    num_workers=*)
        NUM_WORKERS="${arg#*=}"
        ;;
    tpch_file_path=*)
        TPCH_DATA_DIR="${arg#*=}"
        ;;
    log_file_path=*)
        LOG_DIR="${arg#*=}"
        ;;
    *)
        echo "Error: Unknown argument '$arg'"
        echo "Usage: $0 [num_workers=N] [tpch_file_path=PATH] [log_file_path=PATH]"
        exit 1
        ;;
    esac
done

# Set default values if not provided
NUM_WORKERS=${NUM_WORKERS:-$DEFAULT_NUM_WORKERS}
TPCH_DATA_DIR=${TPCH_DATA_DIR:-$DEFAULT_TPCH_PATH}
LOG_DIR=${LOG_DIR:-$DEFAULT_LOG_PATH}

# Validate inputs
if [ "$NUM_WORKERS" -lt 1 ]; then
    echo "Error: Number of workers must be at least 1"
    exit 1
fi

# Check if the binary exists, build if not
if [ ! -f "./target/release/distributed-datafusion" ]; then
    echo "Binary not found, building release version..."
    echo "This may take a few minutes on first run..."
    if [ -f "./build.sh" ]; then
        ./build.sh #--release
    else
        cargo build #--release
    fi

    # Verify the build was successful
    #if [ ! -f "./target/release/distributed-datafusion" ]; then
    if [ ! -f "./target/debug/distributed-datafusion" ]; then
        echo "Error: Failed to build distributed-datafusion binary"
        exit 1
    fi
    echo "✅ Build completed successfully!"
fi

# Check if TPC-H data directory exists
if [ ! -d "$TPCH_DATA_DIR" ]; then
    echo "Error: TPC-H data directory not found: $TPCH_DATA_DIR"
    echo "Please ensure TPC-H data is generated and available at the specified location"
    exit 1
fi

# Check if log directory exists, create if it doesn't
if [ ! -d "$LOG_DIR" ]; then
    echo "Creating log directory: $LOG_DIR"
    mkdir -p "$LOG_DIR"
fi

# Verify required parquet files exist
required_files=("customer.parquet" "lineitem.parquet" "nation.parquet" "orders.parquet"
    "part.parquet" "partsupp.parquet" "region.parquet" "supplier.parquet")

for file in "${required_files[@]}"; do
    if [ ! -f "${TPCH_DATA_DIR}/${file}" ]; then
        echo "Error: Required TPC-H file not found: ${TPCH_DATA_DIR}/${file}"
        exit 1
    fi
done

echo "Setting up TPC-H cluster with:"
echo "  - Workers: $NUM_WORKERS"
echo "  - TPC-H Data Directory: $TPCH_DATA_DIR"
echo "  - Log Directory: $LOG_DIR"
echo

# Define environment variables
export DATAFUSION_RAY_LOG_LEVEL=trace
export DFRAY_TABLES="customer:parquet:${TPCH_DATA_DIR}/customer.parquet,\
lineitem:parquet:${TPCH_DATA_DIR}/lineitem.parquet,\
nation:parquet:${TPCH_DATA_DIR}/nation.parquet,\
orders:parquet:${TPCH_DATA_DIR}/orders.parquet,\
part:parquet:${TPCH_DATA_DIR}/part.parquet,\
partsupp:parquet:${TPCH_DATA_DIR}/partsupp.parquet,\
region:parquet:${TPCH_DATA_DIR}/region.parquet,\
supplier:parquet:${TPCH_DATA_DIR}/supplier.parquet"

# Array to store worker PIDs and addresses
declare -a WORKER_PIDS
declare -a WORKER_ADDRESSES

# Function to cleanup on exit
cleanup() {
    echo "Cleaning up cluster..."
    kill $PROXY_PID ${WORKER_PIDS[*]} 2>/dev/null || true
    exit 0
}

# Set up trap for cleanup
trap cleanup SIGINT SIGTERM EXIT

# Start workers
echo "Starting workers..."
for ((i = 0; i < NUM_WORKERS; i++)); do
    PORT=$((20201 + i))
    WORKER_NAME="worker$((i + 1))"
    LOG_FILE="${LOG_DIR}/${WORKER_NAME}.log"
    echo "  Starting $WORKER_NAME on port $PORT..."
    #env DATAFUSION_RAY_LOG_LEVEL="$DATAFUSION_RAY_LOG_LEVEL" DFRAY_TABLES="$DFRAY_TABLES" ./target/release/distributed-datafusion --mode worker --port $PORT >"$LOG_FILE" 2>&1 &
    env RUST_BACKTRACE=1 DATAFUSION_RAY_LOG_LEVEL="$DATAFUSION_RAY_LOG_LEVEL" DFRAY_TABLES="$DFRAY_TABLES" ./target/debug/distributed-datafusion --mode worker --port $PORT >"$LOG_FILE" 2>&1 &
    WORKER_PIDS[$i]=$!
    WORKER_ADDRESSES[$i]="localhost:${PORT}"
done

# Give workers time to start
echo "Waiting for workers to initialize..."
sleep 5

# Construct worker addresses string for proxy
WORKER_ADDRESSES_STR=$(
    IFS=,
    echo "${WORKER_ADDRESSES[*]}"
)

# Start proxy
echo "Starting proxy on port 20200..."
echo "Connecting to workers: $WORKER_ADDRESSES_STR"
PROXY_LOG="${LOG_DIR}/proxy.log"
#env DATAFUSION_RAY_LOG_LEVEL="$DATAFUSION_RAY_LOG_LEVEL" DFRAY_TABLES="$DFRAY_TABLES" DFRAY_WORKER_ADDRESSES="$WORKER_ADDRESSES_STR" ./target/release/distributed-datafusion --mode proxy --port 20200 >"$PROXY_LOG" 2>&1 &
env RUST_BACKTRACE=1 DATAFUSION_RAY_LOG_LEVEL="$DATAFUSION_RAY_LOG_LEVEL" DFRAY_TABLES="$DFRAY_TABLES" DFRAY_WORKER_ADDRESSES="$WORKER_ADDRESSES_STR" ./target/debug/distributed-datafusion --mode proxy --port 20200 >"$PROXY_LOG" 2>&1 &
PROXY_PID=$!

echo
echo "TPC-H cluster is now running!"
echo
echo "Cluster Information:"
echo "  Proxy: localhost:20200"
for ((i = 0; i < NUM_WORKERS; i++)); do
    echo "  Worker $((i + 1)): localhost:$((20201 + i))"
done
echo
echo "Log files in ${LOG_DIR}:"
echo "  - proxy.log"
for ((i = 0; i < NUM_WORKERS; i++)); do
    echo "  - worker$((i + 1)).log"
done
echo
echo "To monitor logs in real-time:"
echo "  tail -f ${LOG_DIR}/proxy.log   # for proxy logs"
for ((i = 0; i < NUM_WORKERS; i++)); do
    echo "  tail -f ${LOG_DIR}/worker$((i + 1)).log # for worker $((i + 1)) logs"
done
echo
echo "Press Ctrl+C to stop the cluster"

# Wait for user interrupt
wait
