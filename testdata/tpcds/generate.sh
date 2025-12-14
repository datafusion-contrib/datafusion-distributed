#!/bin/bash

set -e

# Get the directory where this script is located
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

if [ $# -ne 1 ]; then
    echo "Usage: $0 <scale_factor>"
    echo "Scale factor must be greater than or equal to 0"
    exit 1
fi

SCALE_FACTOR=$1

if ! [[ "$SCALE_FACTOR" =~ ^[0-9]+(\.[0-9]+)?$ ]] || (( $(echo "$SCALE_FACTOR < 0" | bc -l) )); then
    echo "Error: Scale factor must be a number greater than or equal to 0"
    exit 1
fi

if ! command -v duckdb &> /dev/null; then
    echo "Error: duckdb CLI is not installed"
    echo "Please install duckdb: https://duckdb.org/docs/installation/"
    exit 1
fi

echo "Clearing testdata/tpcds/data directory..."
rm -rf "$SCRIPT_DIR/data"
mkdir "$SCRIPT_DIR/data"

echo "Removing existing database file..."
rm -f "$SCRIPT_DIR/tpcds.duckdb"

echo "Generating TPC-DS data with scale factor $SCALE_FACTOR..."
duckdb "$SCRIPT_DIR/tpcds.duckdb" -c "INSTALL tpcds; LOAD tpcds; CALL dsdgen(sf=$SCALE_FACTOR);"

FILE_SIZE_BYTES=16777216

echo "Exporting tables to parquet files..."
duckdb "$SCRIPT_DIR/tpcds.duckdb" << EOF
COPY store_sales TO '$SCRIPT_DIR/data/store_sales' (FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES ${FILE_SIZE_BYTES});
COPY catalog_sales TO '$SCRIPT_DIR/data/catalog_sales' (FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES ${FILE_SIZE_BYTES});
COPY web_sales TO '$SCRIPT_DIR/data/web_sales' (FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES ${FILE_SIZE_BYTES});
COPY store_returns TO '$SCRIPT_DIR/data/store_returns' (FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES ${FILE_SIZE_BYTES});
COPY catalog_returns TO '$SCRIPT_DIR/data/catalog_returns' (FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES ${FILE_SIZE_BYTES});
COPY web_returns TO '$SCRIPT_DIR/data/web_returns' (FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES ${FILE_SIZE_BYTES});
COPY inventory TO '$SCRIPT_DIR/data/inventory' (FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES ${FILE_SIZE_BYTES});
COPY date_dim TO '$SCRIPT_DIR/data/date_dim' (FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES ${FILE_SIZE_BYTES});
COPY time_dim TO '$SCRIPT_DIR/data/time_dim' (FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES ${FILE_SIZE_BYTES});
COPY item TO '$SCRIPT_DIR/data/item' (FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES ${FILE_SIZE_BYTES});
COPY customer TO '$SCRIPT_DIR/data/customer' (FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES ${FILE_SIZE_BYTES});
COPY customer_address TO '$SCRIPT_DIR/data/customer_address' (FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES ${FILE_SIZE_BYTES});
COPY customer_demographics TO '$SCRIPT_DIR/data/customer_demographics' (FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES ${FILE_SIZE_BYTES});
COPY household_demographics TO '$SCRIPT_DIR/data/household_demographics' (FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES ${FILE_SIZE_BYTES});
COPY income_band TO '$SCRIPT_DIR/data/income_band' (FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES ${FILE_SIZE_BYTES});
COPY warehouse TO '$SCRIPT_DIR/data/warehouse' (FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES ${FILE_SIZE_BYTES});
COPY store TO '$SCRIPT_DIR/data/store' (FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES ${FILE_SIZE_BYTES});
COPY call_center TO '$SCRIPT_DIR/data/call_center' (FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES ${FILE_SIZE_BYTES});
COPY web_site TO '$SCRIPT_DIR/data/web_site' (FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES ${FILE_SIZE_BYTES});
COPY web_page TO '$SCRIPT_DIR/data/web_page' (FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES ${FILE_SIZE_BYTES});
COPY ship_mode TO '$SCRIPT_DIR/data/ship_mode' (FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES ${FILE_SIZE_BYTES});
COPY reason TO '$SCRIPT_DIR/data/reason' (FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES ${FILE_SIZE_BYTES});
COPY promotion TO '$SCRIPT_DIR/data/promotion' (FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES ${FILE_SIZE_BYTES});
COPY catalog_page TO '$SCRIPT_DIR/data/catalog_page' (FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES ${FILE_SIZE_BYTES});
EOF

echo "Cleaning up temporary database..."
rm -f "$SCRIPT_DIR/tpcds.duckdb"

echo "TPC-DS data generation complete!"
