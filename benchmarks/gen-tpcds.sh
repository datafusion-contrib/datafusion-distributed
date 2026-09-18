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

if [ "$#" -gt 0 ]; then
    if [ "$#" -ne 2 ] || [ "$1" != "--output" ]; then
        echo "Usage: $0 [--output <directory|s3://bucket/prefix>]" >&2
        exit 1
    fi
    TPCDS_DIR="$2"
fi

echo "Creating tpcds dataset at Scale Factor ${SCALE_FACTOR} in ${TPCDS_DIR}..."

$CARGO_COMMAND -- prepare-tpcds --output "${TPCDS_DIR}" --partitions "$PARTITIONS" --sf "$SCALE_FACTOR"
