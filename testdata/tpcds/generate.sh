#!/bin/bash

set -e

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
rm -rf ./data
mkdir data

echo "Removing existing database file..."
rm -f tpcds.duckdb

echo "Generating TPC-DS data with scale factor $SCALE_FACTOR..."
duckdb tpcds.duckdb -c "INSTALL tpcds; LOAD tpcds; CALL dsdgen(sf=$SCALE_FACTOR);"

FILE_SIZE_BYTES=16777216

echo "Exporting tables to parquet files..."
duckdb tpcds.duckdb << EOF
COPY store_sales TO 'data/store_sales' (FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES ${FILE_SIZE_BYTES});
COPY catalog_sales TO 'data/catalog_sales' (FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES ${FILE_SIZE_BYTES});
COPY web_sales TO 'data/web_sales' (FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES ${FILE_SIZE_BYTES});
COPY store_returns TO 'data/store_returns' (FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES ${FILE_SIZE_BYTES});
COPY catalog_returns TO 'data/catalog_returns' (FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES ${FILE_SIZE_BYTES});
COPY web_returns TO 'data/web_returns' (FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES ${FILE_SIZE_BYTES});
COPY inventory TO 'data/inventory' (FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES ${FILE_SIZE_BYTES});
COPY date_dim TO 'data/date_dim' (FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES ${FILE_SIZE_BYTES});
COPY time_dim TO 'data/time_dim' (FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES ${FILE_SIZE_BYTES});
COPY item TO 'data/item' (FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES ${FILE_SIZE_BYTES});
COPY customer TO 'data/customer' (FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES ${FILE_SIZE_BYTES});
COPY customer_address TO 'data/customer_address' (FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES ${FILE_SIZE_BYTES});
COPY customer_demographics TO 'data/customer_demographics' (FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES ${FILE_SIZE_BYTES});
COPY household_demographics TO 'data/household_demographics' (FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES ${FILE_SIZE_BYTES});
COPY income_band TO 'data/income_band' (FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES ${FILE_SIZE_BYTES});
COPY warehouse TO 'data/warehouse' (FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES ${FILE_SIZE_BYTES});
COPY store TO 'data/store' (FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES ${FILE_SIZE_BYTES});
COPY call_center TO 'data/call_center' (FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES ${FILE_SIZE_BYTES});
COPY web_site TO 'data/web_site' (FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES ${FILE_SIZE_BYTES});
COPY web_page TO 'data/web_page' (FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES ${FILE_SIZE_BYTES});
COPY ship_mode TO 'data/ship_mode' (FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES ${FILE_SIZE_BYTES});
COPY reason TO 'data/reason' (FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES ${FILE_SIZE_BYTES});
COPY promotion TO 'data/promotion' (FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES ${FILE_SIZE_BYTES});
COPY catalog_page TO 'data/catalog_page' (FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES ${FILE_SIZE_BYTES});
EOF

echo "Cleaning up temporary database..."
rm -f tpcds.duckdb

echo "TPC-DS data generation complete!"
