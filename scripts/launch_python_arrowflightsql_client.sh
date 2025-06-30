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
if ! pip show adbc_driver_flightsql > /dev/null 2>&1 || ! pip show duckdb > /dev/null 2>&1; then
    echo "Installing required packages..."
    pip install adbc_driver_manager adbc_driver_flightsql duckdb pyarrow
fi

# Create a Python startup script
cat > .python_startup.py << 'EOF'
import adbc_driver_flightsql.dbapi as dbapi
import duckdb
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
        reader = cur.fetch_record_batch()
        
        # Use basic DuckDB show() - for full output use run_sql_full() or run_sql_raw()
        # duckdb.sql("select * from reader").show(max_width=10000)
        duckdb.sql("select * from reader").show()
    except Exception as e:
        print(f"Error executing SQL query: {str(e)}")

def format_plan(plan_text):
    """
    Format the plan text by replacing \n with proper indentation
    """
    if not plan_text:
        return ""
    
    # If the plan_text doesn't contain \\n, return as is (it's likely already formatted)
    if '\\n' not in plan_text:
        return plan_text
    
    # Split the plan into lines and add proper indentation
    lines = plan_text.split('\\n')
    formatted_lines = []
    indent_level = 0
    indent_size = 2
    
    for line in lines:
        # Skip empty lines
        if not line.strip():
            continue
            
        # Count opening and closing parentheses to adjust indentation
        open_parens = line.count('(')
        close_parens = line.count(')')
        
        # Adjust indent level based on parentheses
        if close_parens > open_parens:
            indent_level = max(0, indent_level - (close_parens - open_parens))
        
        # Add the line with current indentation
        formatted_lines.append(' ' * (indent_level * indent_size) + line.strip())
        
        # Update indent level for next line
        if open_parens > close_parens:
            indent_level += (open_parens - close_parens)
    
    return '\n'.join(formatted_lines)

def explain_query(query_name):
    """
    Run EXPLAIN for a TPC-H query by name (e.g., 'q5' for EXPLAIN q5.sql)
    and display the formatted output
    """
    query_file = os.path.join(tpch_query_path, f"{query_name}.sql")
    try:
        with open(query_file, 'r') as f:
            sql = f.read()
        print(f"Executing EXPLAIN for query from {query_file}...")
        explain_sql(sql)
    except FileNotFoundError:
        print(f"Error: Query file {query_file} not found")
    except Exception as e:
        print(f"Error executing EXPLAIN: {str(e)}")
        import traceback
        traceback.print_exc()

def explain_sql(sql_query):
    """
    Run EXPLAIN on a given SQL query (passed as a string or variable) and display the formatted output.
    """
    try:
        print("Executing EXPLAIN...")
        cur.execute(f"EXPLAIN {sql_query}")
        results = cur.fetchall()
        if not results:
            print("No explain plan returned")
            return
        
        logical_plan = None
        physical_plan = None
        distributed_plan = None
        distributed_stages = None
        for row in results:
            if row[0] == 'logical_plan':
                logical_plan = row[1]
            elif row[0] == 'physical_plan':
                physical_plan = row[1]
            elif row[0] == 'distributed_plan':
                distributed_plan = row[1]
            elif row[0] == 'distributed_stages':
                distributed_stages = row[1]
        formatted_logical = format_plan(logical_plan) if logical_plan else "Logical plan not available"
        formatted_physical = format_plan(physical_plan) if physical_plan else "Physical plan not available"
        formatted_distributed = format_plan(distributed_plan) if distributed_plan else "Distributed plan not available"
        formatted_distributed_stages = format_plan(distributed_stages) if distributed_stages else "Distributed stages not available"
        print("\nExecution Plan:")
        print("=" * 100)
        print("Logical Plan:")
        print("-" * 100)
        print(formatted_logical)
        print("\nPhysical Plan:")
        print("-" * 100)
        print(formatted_physical)
        if distributed_plan:
            print("\nDistributed Plan:")
            print("-" * 100)
            print(formatted_distributed)
        if distributed_stages:
            print("\nDistributed Stages:")
            print("-" * 100)
            print(formatted_distributed_stages)
        print("=" * 100)
    except Exception as e:
        print(f"Error executing EXPLAIN: {e}")

def explain_analyze_query(query_name):
    """
    Run EXPLAIN ANALYZE for a TPC-H query by name (e.g., 'q5' for EXPLAIN ANALYZE q5.sql)
    and display the formatted output with execution statistics
    """
    query_file = os.path.join(tpch_query_path, f"{query_name}.sql")
    try:
        with open(query_file, 'r') as f:
            sql = f.read()
        print(f"Executing EXPLAIN ANALYZE for query from {query_file}...")
        explain_analyze_sql(sql)
    except FileNotFoundError:
        print(f"Error: Query file {query_file} not found")
    except Exception as e:
        print(f"Error executing EXPLAIN ANALYZE: {str(e)}")
        import traceback
        traceback.print_exc()

def explain_analyze_sql(sql_query):
    """
    Run EXPLAIN ANALYZE on a given SQL query (passed as a string or variable) and display the formatted output with execution statistics.
    """
    try:
        print("Executing EXPLAIN ANALYZE...")
        cur.execute(f"EXPLAIN ANALYZE {sql_query}")
        results = cur.fetchall()
        if not results:
            print("No explain analyze plan returned")
            return
        
        plan_with_metrics = None
        
        for row in results:
            if row[0] == 'Plan with Metrics':
                plan_with_metrics = row[1]
        
        if plan_with_metrics:
            # EXPLAIN ANALYZE returns execution statistics
            formatted_plan_with_metrics = format_plan(plan_with_metrics)
            print("\nExecution Plan with Analysis:")
            print("=" * 100)
            print("Physical Plan with Execution Statistics:")
            print("-" * 100)
            print(formatted_plan_with_metrics)
            print("=" * 100)
    except Exception as e:
        print(f"Error executing EXPLAIN ANALYZE: {e}")

# Add the run_query function to the global namespace
sys.ps1 = ">>> "
print("\nWelcome to the TPC-H Query Client!")
print("Available commands:")
print("  list_queries()  # List all available TPC-H queries")
print("  show_query('q5')  # Show SQL content of query 5")
print("  run_query('q5')  # Run TPC-H query 5")
print("  explain_query('q5')   # Show EXPLAIN plan for TPC-H query 5")
print("  explain_analyze_query('q5')   # Show EXPLAIN ANALYZE plan for TPC-H query 5")
print("  # Note: EXPLAIN ANALYZE does not work with distributed plans yet")
print("  run_sql('select * from nation')  # Run SQL query")
print("  explain_sql('select * from nation')   # Show EXPLAIN plan for SQL query")
print("  explain_analyze_sql('select * from nation')   # Show EXPLAIN ANALYZE plan for SQL query")
print("  # etc...")
print("\nConnected to database at grpc://localhost:20200")
print("Type 'exit()' to quit\n")
EOF

# Set the TPC-H query path environment variable
export TPCH_QUERY_PATH=$TPCH_QUERY_PATH

# Start Python with the startup script
python3 -i .python_startup.py