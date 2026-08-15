#!/usr/bin/env bash

set -e

SCALE_FACTOR=${SCALE_FACTOR:-1}
PARTITIONS=${PARTITIONS:-16}

echo "Generating TPC-DS dataset with SCALE_FACTOR=${SCALE_FACTOR} and PARTITIONS=${PARTITIONS}"

# https://stackoverflow.com/questions/59895/how-do-i-get-the-directory-where-a-bash-script-is-located-from-within-the-script
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
REPO_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)
DATA_DIR=${DATA_DIR:-${REPO_ROOT}/testdata/tpcds}
CARGO_COMMAND=${CARGO_COMMAND:-"cargo run -p datafusion-distributed-benchmarks --release"}
TPCDS_DIR="${DATA_DIR}/sf${SCALE_FACTOR}"

echo "Creating tpcds dataset at Scale Factor ${SCALE_FACTOR} in ${TPCDS_DIR}..."

FILE="${TPCDS_DIR}/store_sales"
if test -d "${FILE}"; then
    echo " parquet files exist ($FILE exists)."
else
    echo " generating parquet files..."
    mkdir -p "${TPCDS_DIR}"
    $CARGO_COMMAND -- prepare-tpcds --output "${TPCDS_DIR}" --scale-factor "${SCALE_FACTOR}" --partitions "$PARTITIONS"
fi
