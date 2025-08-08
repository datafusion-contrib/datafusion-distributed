#!/usr/bin/env bash

set -e

SCALE_FACTOR=1

# https://stackoverflow.com/questions/59895/how-do-i-get-the-directory-where-a-bash-script-is-located-from-within-the-script
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
DATA_DIR=${DATA_DIR:-$SCRIPT_DIR/data}
CARGO_COMMAND=${CARGO_COMMAND:-"cargo run --release"}

if [ -z "$SCALE_FACTOR" ] ; then
    echo "Internal error: Scale factor not specified"
    exit 1
fi

TPCH_DIR="${DATA_DIR}/tpch_sf${SCALE_FACTOR}"
echo "Creating tpch dataset at Scale Factor ${SCALE_FACTOR} in ${TPCH_DIR}..."

# Ensure the target data directory exists
mkdir -p "${TPCH_DIR}"

# Create 'tbl' (CSV format) data into $DATA_DIR if it does not already exist
FILE="${TPCH_DIR}/supplier.tbl"
if test -f "${FILE}"; then
    echo " tbl files exist ($FILE exists)."
else
    echo " creating tbl files with tpch_dbgen..."
    docker run -v "${TPCH_DIR}":/data -it --rm ghcr.io/scalytics/tpch-docker:main -vf -s "${SCALE_FACTOR}"
fi

# Copy expected answers into the ./data/answers directory if it does not already exist
FILE="${TPCH_DIR}/answers/q1.out"
if test -f "${FILE}"; then
    echo " Expected answers exist (${FILE} exists)."
else
    echo " Copying answers to ${TPCH_DIR}/answers"
    mkdir -p "${TPCH_DIR}/answers"
    docker run -v "${TPCH_DIR}":/data -it --entrypoint /bin/bash --rm ghcr.io/scalytics/tpch-docker:main  -c "cp -f /opt/tpch/2.18.0_rc2/dbgen/answers/* /data/answers/"
fi

# Create 'parquet' files from tbl
FILE="${TPCH_DIR}/supplier"
if test -d "${FILE}"; then
    echo " parquet files exist ($FILE exists)."
else
    echo " creating parquet files using benchmark binary ..."
    pushd "${SCRIPT_DIR}" > /dev/null
    $CARGO_COMMAND -- tpch-convert --input "${TPCH_DIR}" --output "${TPCH_DIR}" --format parquet
    popd > /dev/null
fi

# Create 'csv' files from tbl
FILE="${TPCH_DIR}/csv/supplier"
if test -d "${FILE}"; then
    echo " csv files exist ($FILE exists)."
else
    echo " creating csv files using benchmark binary ..."
    pushd "${SCRIPT_DIR}" > /dev/null
    $CARGO_COMMAND -- tpch-convert --input "${TPCH_DIR}" --output "${TPCH_DIR}/csv" --format csv
    popd > /dev/null
fi

