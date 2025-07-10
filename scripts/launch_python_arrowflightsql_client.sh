#!/bin/bash

# =============================================================================
# TPC-H Query Client Launcher
# =============================================================================
#
# This script sets up a Python environment and launches a client for running
# TPC-H queries against a DataFusion Distributed cluster.
#
# Usage:
#   ./launch_python_arrowflightsql_client.sh [query_path=/path/to/queries/]
#
# Examples:
#   1. Use default query path (./tpch/queries/):
#      ./launch_python_arrowflightsql_client.sh
#
#   2. Specify custom query path:
#      ./launch_python_arrowflightsql_client.sh query_path=/path/to/tpch/queries/
#
#   3. Use relative path:
#      ./launch_python_arrowflightsql_client.sh query_path=../queries/
#
# Requirements:
#   - Python 3.x
#   - TPC-H query files (e.g., q1.sql, q5.sql) in the specified directory
#   - DataFusion Distributed cluster running on localhost:20200
#
# Features:
#   - Creates and manages Python virtual environment
#   - Installs required packages (adbc, duckdb, pyarrow)
#   - Provides interactive Python shell with query execution functions
#   - Supports both query execution and EXPLAIN plans
#   - Shows available TPC-H queries
#
# Available Python commands:
#   >>> list_queries()     # Show all available TPC-H queries
#   >>> show_query('q5')              # Show SQL content of query 5
#
#   >>> run_query('q5')              # Run TPC-H query 5
#   >>> explain_query('q5')            # Show EXPLAIN plan for query 5
#   >>> explain_analyze_query('q5')   # Show EXPLAIN ANALYZE plan for query 5
#       Note: EXPLAIN ANALYZE does not work with distributed plans yet
#
#   >>> run_sql('select * from nation')     # Run SQL query
#   >>> explain_sql('select * from nation') # Show EXPLAIN plan for SQL query
#   >>> explain_analyze_sql('select * from nation') # Show EXPLAIN ANALYZE plan for SQL query
#
#   >>> sql="select * from nation"
#   >>> run_sql(sql)                  # Run SQL query from variable
#   >>> explain_sql(sql)              # Show EXPLAIN plan for SQL query from variable
#   >>> explain_analyze_sql(sql)      # Show EXPLAIN ANALYZE plan for SQL query from variable
#
#   >>> exit()                         # Exit the Python shell
#
# Note: The script will verify the query path exists and contains SQL files
# before starting the Python environment.
#
# =============================================================================

# Default TPC-H query path
DEFAULT_QUERY_PATH="./tpch/queries/"

# Parse command line arguments
for arg in "$@"; do
    case $arg in
    query_path=*)
        TPCH_QUERY_PATH="${arg#*=}"
        ;;
    *)
        echo "Usage: $0 [query_path=/path/to/queries/]"
        echo "Example: $0 query_path=./tpch/queries/"
        echo "If no argument is provided, default path will be used: $DEFAULT_QUERY_PATH"
        exit 1
        ;;
    esac
done

# If no query_path argument was provided, use default
if [ -z "${TPCH_QUERY_PATH+x}" ]; then
    TPCH_QUERY_PATH=$DEFAULT_QUERY_PATH
fi

# Check if the query path exists
if [ ! -d "$TPCH_QUERY_PATH" ]; then
    echo "Error: Query path directory does not exist: $TPCH_QUERY_PATH"
    exit 1
fi

# Check if there are any .sql files in the directory
if ! ls "$TPCH_QUERY_PATH"/*.sql >/dev/null 2>&1; then
    echo "Error: No SQL query files found in $TPCH_QUERY_PATH"
    echo "Please ensure the directory contains TPC-H query files (e.g., q1.sql, q5.sql)"
    exit 1
fi

# Create and activate virtual environment if it doesn't exist
if [ ! -d ".venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv .venv
fi

# Activate virtual environment
source .venv/bin/activate

# Install required packages if not already installed
if ! pip show adbc_driver_flightsql >/dev/null 2>&1 || ! pip show rich >/dev/null 2>&1; then
    echo "Installing required packages..."
    pip install adbc_driver_manager adbc_driver_flightsql rich pyarrow
fi

# Create a Python startup script
cat >.python_startup.py <<'EOF'
import adbc_driver_flightsql.dbapi as dbapi
from rich.console import Console
from rich.table import Table
import os
import sys

# Connect to the database
conn = dbapi.connect("grpc://localhost:20200")
cur = conn.cursor()

# Get the TPC-H query path from environment variable
tpch_query_path = os.getenv('TPCH_QUERY_PATH', './tpch/queries/')

def list_queries():
    """
    List all available TPC-H queries in the query directory.
    """
    try:
        files = os.listdir(tpch_query_path)
        queries = sorted([f[:-4] for f in files if f.startswith('q') and f.endswith('.sql')])
        print("Available queries:")
        for q in queries:
            print(f"  {q}")
    except Exception as e:
        print(f"Error listing queries: {str(e)}")

def show_query(query_name):
    """
    Print the SQL content of a TPC-H query by name (e.g., 'q1' for q1.sql).
    """
    query_file = os.path.join(tpch_query_path, f"{query_name}.sql")
    try:
        with open(query_file, 'r') as f:
            sql = f.read()
        print(f"--- {query_file} ---\n{sql}")
    except FileNotFoundError:
        print(f"Error: Query file {query_file} not found")
    except Exception as e:
        print(f"Error reading query: {str(e)}")

def run_query(query_name):
    """
    Run a TPC-H query by name (e.g., 'q5' for q5.sql)
    """
    query_file = os.path.join(tpch_query_path, f"{query_name}.sql")
    try:
        with open(query_file, 'r') as f:
            sql = f.read()
        print(f"Executing query from {query_file}...")
        run_sql(sql)
    except FileNotFoundError:
        print(f"Error: Query file {query_file} not found")
    except Exception as e:
        print(f"Error executing query: {str(e)}")

def run_sql(sql_query):
    """
    Run a given SQL query (passed as a string or variable) and display the results.
    """
    try:
        cur.execute(sql_query)
        table = cur.fetch_arrow_table()

        rich_table = Table(show_header=True, header_style="bold magenta")

        # Add columns based on the PyArrow Table schema
        for field in table.schema:
            rich_table.add_column(field.name)

        # Add rows from the PyArrow Table
        for row_index in range(table.num_rows):
            row_data = [str(table.column(col_index)[row_index].as_py()) for col_index in range(table.num_columns)]
            rich_table.add_row(*row_data)

        console = Console()
        console.print(rich_table)
        
    except Exception as e:
        print(f"Error executing SQL query: {str(e)}")

def explain_query(query_name):
    """
    Run EXPLAIN for a TPC-H query by name (e.g., 'q5' for EXPLAIN q5.sql)
    and display the formatted output
    """
    query_file = os.path.join(tpch_query_path, f"{query_name}.sql")
    try:
        with open(query_file, 'r') as f:
            sql = "explain " + f.read()
        run_sql(sql)
    except FileNotFoundError:
        print(f"Error: Query file {query_file} not found")
    except Exception as e:
        print(f"Error executing query: {str(e)}")

def explain_analyze_query(query_name):
    """
    Run EXPLAIN ANALYZE for a TPC-H query by name (e.g., 'q5' for EXPLAIN ANALYZE q5.sql)
    and display the formatted output with execution statistics
    """
    query_file = os.path.join(tpch_query_path, f"{query_name}.sql")
    try:
        with open(query_file, 'r') as f:
            sql = "explain analyze " + f.read()
        run_sql(sql)
    except FileNotFoundError:
        print(f"Error: Query file {query_file} not found")
    except Exception as e:
        print(f"Error executing EXPLAIN ANALYZE: {str(e)}")
        import traceback
        traceback.print_exc()

sys.ps1 = ">>> "
print("\nWelcome to the TPC-H Query Client!")
print("Available commands:")
print("  list_queries()  # List all available TPC-H queries")
print("  show_query('q5')  # Show SQL content of query 5")
print("  run_query('q5')  # Run TPC-H query 5")
print("  explain_query('q5')   # Show EXPLAIN plan for TPC-H query 5")
print("  explain_analyze_query('q5')   # Show EXPLAIN ANALYZE plan for TPC-H query 5")
print("  run_sql('select * from nation')  # Run SQL query")
print("\nConnected to database at grpc://localhost:20200")
print("Type 'exit()' to quit\n")
EOF

# Set the TPC-H query path environment variable
export TPCH_QUERY_PATH=$TPCH_QUERY_PATH

# Start Python with the startup script
python3 -i .python_startup.py
