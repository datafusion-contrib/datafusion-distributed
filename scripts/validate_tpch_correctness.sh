#!/bin/bash

set -euo pipefail

# =============================================================================
# TPC-H Correctness Validation Script
# =============================================================================
#
# This script validates the correctness of TPC-H queries by comparing results
# between a non-distributed system (datafusion-cli) and the distributed system.
#
# Usage:
#   ./scripts/validate_tpch_correctness.sh [num_workers=N] [tpch_file_path=PATH] [log_file_path=PATH] [query_path=PATH] [proxy_port=PORT] [maxrows=N]
#
# Arguments:
#   num_workers      Number of worker nodes for distributed system (default: 2)
#   tpch_file_path   Path to TPC-H parquet files (default: /tmp/tpch_s1)
#   log_file_path    Directory for log files (default: current directory)
#   query_path       Path to TPC-H query files (default: ./tpch/queries/)
#   proxy_port       Port number for the distributed system proxy (default: 20200)
#   maxrows          Maximum rows to display in DataFusion CLI (default: 100000)
#
# Examples:
#   # Run with all defaults
#   ./scripts/validate_tpch_correctness.sh
#
#   # Run with custom parameters
#   ./scripts/validate_tpch_correctness.sh num_workers=3 tpch_file_path=/tmp/tpch_s1 log_file_path=./logs query_path=./tpch/queries/ proxy_port=20200 maxrows=100000
#
# Features:
#   - Automatically checks and installs datafusion-cli if needed
#   - Automatically checks and installs tpchgen-cli via cargo if needed
#   - Auto-creates TPC-H data directory if it doesn't exist
#   - Auto-generates TPC-H data (scale factor 1) if missing using tpchgen-cli
#   - Detects if distributed cluster is running, launches if needed  
#   - Warns if existing cluster worker count differs from requested workers
#   - Runs all TPC-H queries on both systems
#   - Compares results with floating-point tolerance and reports differences
#   - Handles DataFusion CLI --maxrows inf bug (uses large finite limit instead)
#   - Handles port conflicts gracefully
#   - Generates detailed validation report with success/failure breakdown
#
# =============================================================================

# ANSI color codes for output formatting
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
DEFAULT_NUM_WORKERS=2
DEFAULT_TPCH_PATH="/tmp/tpch_s1"
DEFAULT_LOG_PATH="."
DEFAULT_QUERY_PATH="./tpch/queries/"
DEFAULT_PROXY_PORT=20200
DEFAULT_MAXROWS=100000

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
        query_path=*)
            QUERY_PATH="${arg#*=}"
            ;;
        proxy_port=*)
            PROXY_PORT="${arg#*=}"
            ;;
        maxrows=*)
            MAXROWS="${arg#*=}"
            ;;
        *)
            echo "Error: Unknown argument '$arg'"
            echo "Usage: $0 [num_workers=N] [tpch_file_path=PATH] [log_file_path=PATH] [query_path=PATH] [proxy_port=PORT] [maxrows=N]"
            exit 1
            ;;
    esac
done

# Set default values if not provided
NUM_WORKERS=${NUM_WORKERS:-$DEFAULT_NUM_WORKERS}
TPCH_DATA_DIR=${TPCH_DATA_DIR:-$DEFAULT_TPCH_PATH}
LOG_DIR=${LOG_DIR:-$DEFAULT_LOG_PATH}
QUERY_PATH=${QUERY_PATH:-$DEFAULT_QUERY_PATH}
PROXY_PORT=${PROXY_PORT:-$DEFAULT_PROXY_PORT}
MAXROWS=${MAXROWS:-$DEFAULT_MAXROWS}

# Global variables
CLUSTER_LAUNCHED_BY_SCRIPT=false
CLUSTER_PID=""
VALIDATION_RESULTS_DIR="${LOG_DIR}/validation_results"
REPORT_FILE="${VALIDATION_RESULTS_DIR}/validation_report.txt"

echo -e "${BLUE}==============================================================================${NC}"
echo -e "${BLUE}TPC-H Correctness Validation${NC}"
echo -e "${BLUE}==============================================================================${NC}"
echo "Configuration:"
echo "  - Workers: $NUM_WORKERS"
echo "  - TPC-H Data Directory: $TPCH_DATA_DIR"
echo "  - Log Directory: $LOG_DIR"
echo "  - Query Path: $QUERY_PATH"
echo "  - Proxy Port: $PROXY_PORT"
echo "  - Max Rows: $MAXROWS"
echo

# Function to print colored messages
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to cleanup on exit
cleanup() {
    print_info "Cleaning up..."
    if [ "$CLUSTER_LAUNCHED_BY_SCRIPT" = true ] && [ -n "$CLUSTER_PID" ]; then
        print_info "Stopping cluster launched by this script..."
        kill $CLUSTER_PID 2>/dev/null || true
        # Wait a bit for graceful shutdown
        sleep 2
        # Force kill any remaining processes
        pkill -f "distributed-datafusion.*--mode" 2>/dev/null || true
    fi
}

# Set up trap for cleanup
trap cleanup SIGINT SIGTERM EXIT

# Function to check if datafusion-cli is installed
check_datafusion_cli() {
    print_info "Checking for datafusion-cli..."
    if command -v datafusion-cli &> /dev/null; then
        print_success "datafusion-cli is already installed"
        return 0
    else
        print_warning "datafusion-cli not found"
        return 1
    fi
}

# Function to install datafusion-cli
install_datafusion_cli() {
    print_info "Installing datafusion-cli via Homebrew..."
    if command -v brew &> /dev/null; then
        brew install datafusion-cli
        if [ $? -eq 0 ]; then
            print_success "datafusion-cli installed successfully"
        else
            print_error "Failed to install datafusion-cli"
            exit 1
        fi
    else
        print_error "Homebrew not found. Please install datafusion-cli manually"
        exit 1
    fi
}

# Function to check if proxy is running
check_proxy_running() {
    print_info "Checking if distributed cluster proxy is running on port $PROXY_PORT..."
    
    # Method 1: Try to connect with timeout using Python
    if command -v python3 &> /dev/null; then
        python3 -c "
import socket
import sys
try:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(2)
    result = sock.connect_ex(('localhost', $PROXY_PORT))
    sock.close()
    sys.exit(0 if result == 0 else 1)
except:
    sys.exit(1)
" 2>/dev/null
        if [ $? -eq 0 ]; then
            print_success "Proxy detected running on port $PROXY_PORT"
            return 0
        fi
    fi
    
    # Method 2: Check if port is bound
    if command -v lsof &> /dev/null; then
        if lsof -i :$PROXY_PORT >/dev/null 2>&1; then
            print_success "Port $PROXY_PORT is bound, assuming proxy is running"
            return 0
        fi
    fi
    
    # Method 3: Check for running processes
    if pgrep -f "distributed-datafusion.*--mode proxy" >/dev/null 2>&1; then
        print_success "Distributed datafusion proxy process detected"
        return 0
    fi
    
    print_warning "No proxy detected on port $PROXY_PORT"
    return 1
}

# Function to check worker count and warn if different
check_worker_count() {
    print_info "Checking if cluster worker count matches requested workers..."
    
    # Count running worker processes (macOS pgrep doesn't have -c flag)
    local running_workers=$(pgrep -f "distributed-datafusion.*--mode worker" 2>/dev/null | wc -l | tr -d ' ')
    
    if [ "$running_workers" -ne "$NUM_WORKERS" ]; then
        print_warning "Cluster has $running_workers workers running, but $NUM_WORKERS workers were requested"
        print_warning "Consider stopping the existing cluster or adjusting the num_workers parameter"
        return 1
    else
        print_success "Cluster worker count ($running_workers) matches requested workers ($NUM_WORKERS)"
        return 0
    fi
}

# Function to check if tpchgen-cli is installed
check_tpchgen_cli() {
    print_info "Checking for tpchgen-cli..."
    if command -v tpchgen-cli &> /dev/null; then
        print_success "tpchgen-cli is already installed"
        return 0
    else
        print_warning "tpchgen-cli not found"
        return 1
    fi
}

# Function to install tpchgen-cli
install_tpchgen_cli() {
    print_info "Installing tpchgen-cli via cargo..."
    if command -v cargo &> /dev/null; then
        cargo install tpchgen-cli
        if [ $? -eq 0 ]; then
            print_success "tpchgen-cli installed successfully"
        else
            print_error "Failed to install tpchgen-cli"
            exit 1
        fi
    else
        print_error "Cargo not found. Please install Rust and Cargo to install tpchgen-cli"
        exit 1
    fi
}

# Function to check if TPC-H data exists
check_tpch_data() {
    local data_dir="$1"
    print_info "Checking TPC-H data in $data_dir..."
    
    # List of required TPC-H tables
    local required_tables=("customer" "lineitem" "nation" "orders" "part" "partsupp" "region" "supplier")
    
    for table in "${required_tables[@]}"; do
        local parquet_file="${data_dir}/${table}.parquet"
        if [ ! -f "$parquet_file" ]; then
            print_warning "Missing TPC-H table: ${table}.parquet"
            return 1
        fi
    done
    
    print_success "All TPC-H data files found"
    return 0
}

# Function to generate TPC-H data
generate_tpch_data() {
    local data_dir="$1"
    print_info "Generating TPC-H data in $data_dir..."
    
    # Check and install tpchgen-cli if needed
    if ! check_tpchgen_cli; then
        install_tpchgen_cli
    fi
    
    # Generate TPC-H data with scale factor 1
    print_info "Running: tpchgen-cli -s 1 --format=parquet --output-dir=$data_dir"
    tpchgen-cli -s 1 --format=parquet --output-dir="$data_dir"
    
    if [ $? -eq 0 ]; then
        print_success "TPC-H data generated successfully"
        
        # Verify the data was created
        if check_tpch_data "$data_dir"; then
            return 0
        else
            print_error "TPC-H data generation completed but files are missing"
            exit 1
        fi
    else
        print_error "Failed to generate TPC-H data"
        exit 1
    fi
}

# Function to launch cluster
launch_cluster() {
    print_info "Launching TPC-H cluster..."
    
    # Check if launch script exists
    if [ ! -f "./scripts/launch_tpch_cluster.sh" ]; then
        print_error "launch_tpch_cluster.sh script not found"
        exit 1
    fi
    
    # Launch cluster in background
    print_info "Starting cluster with $NUM_WORKERS workers on proxy port $PROXY_PORT..."
    # Note: launch_tpch_cluster.sh currently hardcodes proxy port to 20200
    # If PROXY_PORT is not 20200, this may cause issues
    if [ "$PROXY_PORT" -ne 20200 ]; then
        print_warning "launch_tpch_cluster.sh hardcodes proxy port to 20200, but requested port is $PROXY_PORT"
        print_warning "The cluster will start on port 20200, but validation will attempt to connect to port $PROXY_PORT"
    fi
    ./scripts/launch_tpch_cluster.sh num_workers=$NUM_WORKERS tpch_file_path=$TPCH_DATA_DIR log_file_path=$LOG_DIR &
    CLUSTER_PID=$!
    CLUSTER_LAUNCHED_BY_SCRIPT=true
    
    # Wait for cluster to start
    print_info "Waiting for cluster to initialize..."
    local max_wait=30
    local wait_time=0
    
    while [ $wait_time -lt $max_wait ]; do
        sleep 2
        wait_time=$((wait_time + 2))
        if check_proxy_running; then
            print_success "Cluster started successfully"
            return 0
        fi
    done
    
    print_error "Cluster failed to start within ${max_wait} seconds"
    exit 1
}

# Function to validate inputs
validate_inputs() {
    print_info "Validating inputs..."
    
    # Check number of workers
    if [ "$NUM_WORKERS" -lt 1 ]; then
        print_error "Number of workers must be at least 1" 
        exit 1
    fi
    
    # Check proxy port
    if [ "$PROXY_PORT" -lt 1 ] || [ "$PROXY_PORT" -gt 65535 ]; then
        print_error "Proxy port must be between 1 and 65535"
        exit 1
    fi
    
    # Check maxrows
    if [ "$MAXROWS" -lt 1 ]; then
        print_error "Maximum rows must be at least 1"
        exit 1
    fi
    
    # Check/create TPC-H data directory
    if [ ! -d "$TPCH_DATA_DIR" ]; then
        print_warning "TPC-H data directory not found: $TPCH_DATA_DIR"
        print_info "Creating TPC-H data directory..."
        mkdir -p "$TPCH_DATA_DIR"
        if [ $? -eq 0 ]; then
            print_success "Created TPC-H data directory: $TPCH_DATA_DIR"
        else
            print_error "Failed to create TPC-H data directory: $TPCH_DATA_DIR"
            exit 1
        fi
    fi
    
    # Check if TPC-H data exists, generate if missing
    if ! check_tpch_data "$TPCH_DATA_DIR"; then
        print_warning "TPC-H data is missing or incomplete"
        generate_tpch_data "$TPCH_DATA_DIR"
    fi
    
    # Check query path
    if [ ! -d "$QUERY_PATH" ]; then
        print_error "Query path directory not found: $QUERY_PATH"
        exit 1
    fi
    
    # Check for SQL files
    if ! ls "$QUERY_PATH"/*.sql >/dev/null 2>&1; then
        print_error "No SQL query files found in $QUERY_PATH"
        exit 1
    fi
    
    # Create log and results directories
    mkdir -p "$LOG_DIR"
    mkdir -p "$VALIDATION_RESULTS_DIR"
    
    print_success "Input validation completed"
}

# Function to setup datafusion-cli configuration
setup_datafusion_cli() {
    print_info "Setting up datafusion-cli configuration..."
    
    # Create a temporary datafusion-cli config script
    cat > "${VALIDATION_RESULTS_DIR}/datafusion_setup.sql" << EOF
-- Setup TPC-H tables for datafusion-cli
CREATE EXTERNAL TABLE customer STORED AS PARQUET LOCATION '${TPCH_DATA_DIR}/customer.parquet';
CREATE EXTERNAL TABLE lineitem STORED AS PARQUET LOCATION '${TPCH_DATA_DIR}/lineitem.parquet';
CREATE EXTERNAL TABLE nation STORED AS PARQUET LOCATION '${TPCH_DATA_DIR}/nation.parquet';
CREATE EXTERNAL TABLE orders STORED AS PARQUET LOCATION '${TPCH_DATA_DIR}/orders.parquet';
CREATE EXTERNAL TABLE part STORED AS PARQUET LOCATION '${TPCH_DATA_DIR}/part.parquet';
CREATE EXTERNAL TABLE partsupp STORED AS PARQUET LOCATION '${TPCH_DATA_DIR}/partsupp.parquet';
CREATE EXTERNAL TABLE region STORED AS PARQUET LOCATION '${TPCH_DATA_DIR}/region.parquet';
CREATE EXTERNAL TABLE supplier STORED AS PARQUET LOCATION '${TPCH_DATA_DIR}/supplier.parquet';

-- Setup TPC-H views (required for q15)
CREATE VIEW revenue0 (supplier_no, total_revenue) AS
SELECT
    l_suppkey,
    sum(l_extendedprice * (1 - l_discount))
FROM
    lineitem
WHERE
    l_shipdate >= date '1996-08-01'
    AND l_shipdate < date '1996-08-01' + interval '3' month
GROUP BY
    l_suppkey;
EOF
    
    print_success "DataFusion CLI configuration created"
}

# Function to run query with datafusion-cli
run_query_datafusion_cli() {
    local query_file="$1"
    local output_file="$2"
    
    # Create a temporary script that includes setup and query
    local temp_script="${VALIDATION_RESULTS_DIR}/temp_$(basename $query_file .sql).sql"
    cat "${VALIDATION_RESULTS_DIR}/datafusion_setup.sql" > "$temp_script"
    echo "" >> "$temp_script"
    cat "$query_file" >> "$temp_script"
    
    # Run with datafusion-cli (with configurable row limit to avoid inf bug)
    datafusion-cli --maxrows "$MAXROWS" --file "$temp_script" > "$output_file" 2>&1
    local exit_code=$?
    
    # Clean up temp file
    rm -f "$temp_script"
    
    return $exit_code
}

# Function to run query with distributed system
run_query_distributed() {
    local query_file="$1"
    local output_file="$2"
    
    # Create Python script to run the query
    local temp_python="${VALIDATION_RESULTS_DIR}/temp_$(basename $query_file .sql).py"
    cat > "$temp_python" << EOF
import adbc_driver_flightsql.dbapi as dbapi
import duckdb
import sys
import os

try:
    # Connect to the distributed system
    conn = dbapi.connect("grpc://localhost:$PROXY_PORT")
    cur = conn.cursor()
    
    # Read and execute the query
    with open('$query_file', 'r') as f:
        sql = f.read()
    
    cur.execute(sql)
    reader = cur.fetch_record_batch()
    
    # Use DuckDB to process the reader and convert to pandas (following launch_python_arrowflightsql_client.sh pattern)
    import pandas as pd
    df = duckdb.sql("select * from reader").to_df()
    
    # Print results in a format similar to datafusion-cli
    print(df.to_string(index=False))
    
except Exception as e:
    print(f"Error: {e}", file=sys.stderr)
    sys.exit(1)
EOF
    
    # Create virtual environment if it doesn't exist
    if [ ! -d ".venv" ]; then
        python3 -m venv .venv
    fi
    
    # Activate virtual environment and install packages
    source .venv/bin/activate
    pip install -q adbc_driver_manager adbc_driver_flightsql duckdb pandas pyarrow
    
    # Run the Python script
    python3 "$temp_python" > "$output_file" 2>&1
    local exit_code=$?
    
    # Clean up
    rm -f "$temp_python"
    
    return $exit_code
}

# Function to extract data rows from DataFusion CLI output
extract_datafusion_data() {
    local file="$1"
    # Extract data from DataFusion CLI table output, including NULL/empty results
    python3 -c "
import sys
import re

def process_datafusion_output(filename):
    with open(filename, 'r') as f:
        lines = f.readlines()
    
    in_table = False
    header_found = False
    table_data = []
    
    for line in lines:
        line = line.strip()
        
        # Detect table boundaries (+---+)
        if re.match(r'^\s*\+[-\+]*\+\s*$', line):
            if not in_table:
                # Start of table
                in_table = True
                header_found = False
            elif header_found and table_data:
                # End of table (we've seen header and have data)
                for row in table_data:
                    print(row)
                return
            # Otherwise it's the header separator - continue in table
            continue
        
        # Process lines within table
        if in_table and line.startswith('|') and line.endswith('|'):
            # Remove | boundaries and split into fields
            content = line[1:-1]
            fields = [f.strip() for f in content.split('|')]
            
            # Check if this is a header line (column names)
            # Allow SQL expressions with functions, dots, parentheses, etc.
            is_header = all(re.match(r'^[a-zA-Z_][a-zA-Z0-9_().]*$', field) for field in fields if field)
            
            if is_header and not header_found:
                header_found = True
                continue
            elif header_found:
                # This is a data row (could be empty/NULL)
                clean_fields = [f.strip() for f in fields]
                
                # Check if all fields are empty (NULL result)
                if all(not field for field in clean_fields):
                    table_data.append('NULL')
                else:
                    table_data.append('|'.join(clean_fields))
    
    # Handle end of file cases
    if in_table and header_found:
        if table_data:
            # Output accumulated data
            for row in table_data:
                print(row)
        else:
            # Empty table with header but no data = NULL
            print('NULL')

process_datafusion_output('$file')
"
}

# Function to extract data rows from distributed output  
extract_distributed_data() {
    local file="$1"
    # Skip warnings and headers, extract only data rows
    # Data rows typically start with values (not column names)
    grep -v "Warning:\|warnings.warn\|^$" "$file" | python3 -c "
import sys
import re

lines = []
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    lines.append(line)

# Skip the first line if it looks like a header (contains mostly column names)
if lines:
    first_line = lines[0]
    # Check if first line looks like column headers (mostly words separated by spaces)
    fields = first_line.split()
    is_header = len(fields) > 1 and all(re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', field) for field in fields)
    
    start_idx = 1 if is_header else 0
    
    # Output data rows (skip header if detected)
    for line in lines[start_idx:]:
        # Basic check: data rows should contain some numbers
        if re.search(r'\d', line):
            print(line)
"
}

# Function to normalize a data row for comparison
normalize_row() {
    local row="$1"
    # Handle NULL values and convert scientific notation to decimal
    echo "$row" | python3 -c "
import sys
import re
line = sys.stdin.read().strip()

# Handle NULL values directly
if line == 'NULL':
    print('NULL')
    exit()

# Split by multiple spaces or | to get fields
fields = re.split(r'[\s|]+', line)
fields = [f.strip() for f in fields if f.strip()]

# Convert scientific notation and normalize numbers with intelligent precision
normalized_fields = []
for field in fields:
    # Handle NULL field
    if field.upper() == 'NULL' or field == '':
        normalized_fields.append('NULL')
        continue
        
    # Try to convert to float and back to normalize scientific notation
    try:
        if 'e' in field.lower() or '.' in field:
            # It's a number with scientific notation or decimal
            num = float(field)
            # Use relative precision to avoid small differences in large numbers
            if abs(num) >= 1e6:
                # For large numbers, use relative precision (6 significant digits)
                normalized_fields.append(f'{num:.6g}')
            elif abs(num) >= 1e-3 or num == 0:
                # For normal range numbers, use fixed precision
                normalized_fields.append(f'{num:.6f}')
            else:
                # For very small numbers, use scientific notation
                normalized_fields.append(f'{num:.6e}')
        else:
            # Try integer
            num = int(field)
            normalized_fields.append(str(num))
    except:
        # It's a string, keep as is
        normalized_fields.append(field)

print('|'.join(normalized_fields))
"
}

# Function to compare query results with intelligent parsing
compare_results() {
    local query_name="$1"
    local datafusion_file="$2" 
    local distributed_file="$3"
    
    print_info "Comparing results for $query_name..."
    
    # Check if both files exist and have content
    if [ ! -s "$datafusion_file" ]; then
        print_error "DataFusion CLI result file is empty or missing for $query_name"
        return 1
    fi
    
    if [ ! -s "$distributed_file" ]; then
        print_error "Distributed system result file is empty or missing for $query_name"
        return 1
    fi
    
    # Extract and normalize data from both files
    local datafusion_data="${VALIDATION_RESULTS_DIR}/${query_name}_datafusion_normalized.txt"
    local distributed_data="${VALIDATION_RESULTS_DIR}/${query_name}_distributed_normalized.txt"
    
    # Extract data rows
    extract_datafusion_data "$datafusion_file" > "${datafusion_data}.raw"
    extract_distributed_data "$distributed_file" > "${distributed_data}.raw"
    
    # Check if we have data rows (allow NULL values)
    if [ ! -f "${datafusion_data}.raw" ]; then
        print_error "$query_name: DataFusion CLI result file not created"
        return 1
    fi
    
    if [ ! -f "${distributed_data}.raw" ]; then
        print_error "$query_name: Distributed system result file not created"
        return 1
    fi
    
    # Check if files are completely empty (no output at all)
    if [ ! -s "${datafusion_data}.raw" ]; then
        print_error "$query_name: No data rows found in DataFusion CLI output"
        return 1
    fi
    
    if [ ! -s "${distributed_data}.raw" ]; then
        print_error "$query_name: No data rows found in distributed system output"
        return 1
    fi
    
    # Normalize each row
    > "$datafusion_data"
    > "$distributed_data"
    
    while IFS= read -r row; do
        if [ -n "$row" ]; then
            normalize_row "$row" >> "$datafusion_data"
        fi
    done < "${datafusion_data}.raw"
    
    while IFS= read -r row; do
        if [ -n "$row" ]; then
            normalize_row "$row" >> "$distributed_data"
        fi
    done < "${distributed_data}.raw"
    
    # Sort both files to handle potential ordering differences
    sort "$datafusion_data" > "${datafusion_data}.sorted"
    sort "$distributed_data" > "${distributed_data}.sorted"
    
    # Compare normalized data with tolerance for floating-point differences
    local matches=$(python3 -c "
import sys

def compare_files_with_tolerance(file1, file2, rel_tol=1e-5, abs_tol=1e-12):
    try:
        with open('${datafusion_data}.sorted', 'r') as f1, open('${distributed_data}.sorted', 'r') as f2:
            lines1 = [line.strip() for line in f1 if line.strip()]
            lines2 = [line.strip() for line in f2 if line.strip()]
            
        if len(lines1) != len(lines2):
            return False
            
        for line1, line2 in zip(lines1, lines2):
            fields1 = line1.split('|')
            fields2 = line2.split('|')
            
            if len(fields1) != len(fields2):
                return False
                
            for field1, field2 in zip(fields1, fields2):
                # Handle NULL values specially
                if field1.upper() == 'NULL' or field2.upper() == 'NULL':
                    if field1.upper() != field2.upper():
                        return False
                    continue
                
                # Try to compare as numbers with tolerance
                try:
                    num1 = float(field1)
                    num2 = float(field2)
                    
                    # Use relative tolerance for large numbers, absolute for small ones
                    if abs(num1) > 1e-9 and abs(num2) > 1e-9:
                        rel_diff = abs(num1 - num2) / max(abs(num1), abs(num2))
                        if rel_diff > rel_tol:
                            return False
                    else:
                        if abs(num1 - num2) > abs_tol:
                            return False
                except:
                    # Compare as strings
                    if field1 != field2:
                        return False
        return True
    except Exception as e:
        return False

print('1' if compare_files_with_tolerance('${datafusion_data}.sorted', '${distributed_data}.sorted') else '0')
")

    if [ "$matches" = "1" ]; then
        print_success "$query_name: Results match ✓ (within floating-point tolerance)"
        # Clean up temporary files
        rm -f "${datafusion_data}" "${distributed_data}" "${datafusion_data}.raw" "${distributed_data}.raw" "${datafusion_data}.sorted" "${distributed_data}.sorted"
        return 0
    else
        # Try exact diff first
        if diff -q "${datafusion_data}.sorted" "${distributed_data}.sorted" >/dev/null 2>&1; then
            print_success "$query_name: Results match ✓ (exact match)"
            # Clean up temporary files  
            rm -f "${datafusion_data}" "${distributed_data}" "${datafusion_data}.raw" "${distributed_data}.raw" "${datafusion_data}.sorted" "${distributed_data}.sorted"
            return 0
        else
            print_error "$query_name: Results differ ✗"
            echo "  DataFusion CLI output: $datafusion_file"
            echo "  Distributed output: $distributed_file"
            echo "  Normalized comparison files:"
            echo "    DataFusion: ${datafusion_data}.sorted"
            echo "    Distributed: ${distributed_data}.sorted"
            echo "  Use 'diff ${datafusion_data}.sorted ${distributed_data}.sorted' to see differences"
            return 1
        fi
    fi
}

# Function to run all validations
run_validations() {
    print_info "Starting TPC-H query validations..."
    
    local total_queries=0
    local passed_queries=0
    local failed_queries=0
    
    # Initialize report
    echo "TPC-H Correctness Validation Report" > "$REPORT_FILE"
    echo "Generated: $(date)" >> "$REPORT_FILE"
    echo "Configuration:" >> "$REPORT_FILE"
    echo "  - Workers: $NUM_WORKERS" >> "$REPORT_FILE"
    echo "  - TPC-H Data: $TPCH_DATA_DIR" >> "$REPORT_FILE"
    echo "  - Query Path: $QUERY_PATH" >> "$REPORT_FILE"
    echo "  - Proxy Port: $PROXY_PORT" >> "$REPORT_FILE"
    echo "" >> "$REPORT_FILE"
    echo "Results:" >> "$REPORT_FILE"
    echo "========" >> "$REPORT_FILE"
    
    # Get list of query files
    local query_files=($(find "$QUERY_PATH" -name "q*.sql" | sort))
    
    for query_file in "${query_files[@]}"; do
        local query_name=$(basename "$query_file" .sql)
        total_queries=$((total_queries + 1))
        
        print_info "Running validation for $query_name..."
        
        local datafusion_output="${VALIDATION_RESULTS_DIR}/${query_name}_datafusion.out"
        local distributed_output="${VALIDATION_RESULTS_DIR}/${query_name}_distributed.out"
        
        # Run query with DataFusion CLI
        print_info "  Running $query_name with DataFusion CLI..."
        if run_query_datafusion_cli "$query_file" "$datafusion_output"; then
            print_success "  DataFusion CLI execution completed"
        else
            print_error "  DataFusion CLI execution failed"
            echo "$query_name: FAILED (DataFusion CLI error)" >> "$REPORT_FILE"
            failed_queries=$((failed_queries + 1))
            continue
        fi
        
        # Run query with distributed system
        print_info "  Running $query_name with distributed system..."
        if run_query_distributed "$query_file" "$distributed_output"; then
            print_success "  Distributed system execution completed"
        else
            print_error "  Distributed system execution failed"
            echo "$query_name: FAILED (Distributed system error)" >> "$REPORT_FILE"
            failed_queries=$((failed_queries + 1))
            continue
        fi
        
        # Compare results
        if compare_results "$query_name" "$datafusion_output" "$distributed_output"; then
            echo "$query_name: PASSED" >> "$REPORT_FILE"
            passed_queries=$((passed_queries + 1))
        else
            echo "$query_name: FAILED (Results differ)" >> "$REPORT_FILE"
            failed_queries=$((failed_queries + 1))
        fi
        
        echo
    done
    
    # Generate summary
    echo "" >> "$REPORT_FILE"
    echo "Summary:" >> "$REPORT_FILE"
    echo "========" >> "$REPORT_FILE"
    echo "Total queries: $total_queries" >> "$REPORT_FILE"
    echo "Passed: $passed_queries" >> "$REPORT_FILE" 
    echo "Failed: $failed_queries" >> "$REPORT_FILE"
    echo "Success rate: $(( passed_queries * 100 / total_queries ))%" >> "$REPORT_FILE"
    
    # Print summary
    echo -e "${BLUE}==============================================================================${NC}"
    echo -e "${BLUE}Validation Summary${NC}"
    echo -e "${BLUE}==============================================================================${NC}"
    echo "Total queries tested: $total_queries"
    echo -e "Passed: ${GREEN}$passed_queries${NC}"
    echo -e "Failed: ${RED}$failed_queries${NC}"
    echo "Success rate: $(( passed_queries * 100 / total_queries ))%"
    echo
    echo "Detailed report: $REPORT_FILE"
    echo "Result files: $VALIDATION_RESULTS_DIR"
    
    if [ $failed_queries -eq 0 ]; then
        print_success "All validations passed! ✅"
        return 0
    else
        print_error "Some validations failed! ❌"
        return 1
    fi
}

# Main execution
main() {
    validate_inputs
    
    # Check and install datafusion-cli if needed
    if ! check_datafusion_cli; then
        install_datafusion_cli
    fi
    
    # Check if proxy is running, launch cluster if needed
    if ! check_proxy_running; then
        launch_cluster
    else
        print_info "Using existing distributed cluster"
        # Check if worker count matches the requested number
        check_worker_count
    fi
    
    # Setup datafusion-cli configuration
    setup_datafusion_cli
    
    # Run validations
    run_validations
}

# Run main function
main "$@" 