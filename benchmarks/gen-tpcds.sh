#!/usr/bin/env bash

set -e

SCALE_FACTOR=${SCALE_FACTOR:-1}
PARTITIONS=${PARTITIONS:-16}

echo "Generating TPC-DS dataset with SCALE_FACTOR=${SCALE_FACTOR} and PARTITIONS=${PARTITIONS}"

# https://stackoverflow.com/questions/59895/how-do-i-get-the-directory-where-a-bash-script-is-located-from-within-the-script
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
DATA_DIR=${DATA_DIR:-$SCRIPT_DIR/data}
CARGO_COMMAND=${CARGO_COMMAND:-"cargo run --release"}
TPCDS_DIR="${DATA_DIR}/tpcds_sf${SCALE_FACTOR}"

echo "Creating tpcds dataset at Scale Factor ${SCALE_FACTOR} in ${TPCDS_DIR}..."

# Ensure the target data directory exists
mkdir -p "${TPCDS_DIR}"

$CARGO_COMMAND -- prepare-tpcds --output "${TPCDS_DIR}" --partitions "$PARTITIONS"
