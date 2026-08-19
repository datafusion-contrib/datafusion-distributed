#!/usr/bin/env bash

set -e

SCALE_FACTOR=${SCALE_FACTOR:-1}
PARTITIONS=${PARTITIONS:-16}

echo "Generating sorted TPCH dataset with SCALE_FACTOR=${SCALE_FACTOR} and PARTITIONS=${PARTITIONS}"

# https://stackoverflow.com/questions/59895/how-do-i-get-the-directory-where-a-bash-script-is-located-from-within-the-script
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
REPO_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)
DATA_DIR=${DATA_DIR:-${REPO_ROOT}/testdata/tpch}
CARGO_COMMAND=${CARGO_COMMAND:-"cargo run -p datafusion-distributed-benchmarks --release"}
TPCH_DIR="${DATA_DIR}/sorted_sf${SCALE_FACTOR}"
echo "Creating sorted tpch dataset at Scale Factor ${SCALE_FACTOR} in ${TPCH_DIR}..."

FILE="${TPCH_DIR}/supplier"
if test -d "${FILE}"; then
    echo " parquet files exist ($FILE exists)."
else
    echo " generating sorted parquet files using tpchgen-rs..."
    $CARGO_COMMAND -- prepare-tpch --output "${TPCH_DIR}" --scale-factor "${SCALE_FACTOR}" --partitions "$PARTITIONS" --sorted
fi
