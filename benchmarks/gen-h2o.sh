#!/usr/bin/env bash

set -e

SCALE_FACTOR=${SCALE_FACTOR:-1}
PARTITIONS=${PARTITIONS:-16}

echo "Generating h2o groupby dataset with SCALE_FACTOR=${SCALE_FACTOR} and PARTITIONS=${PARTITIONS}"

# https://stackoverflow.com/questions/59895/how-do-i-get-the-directory-where-a-bash-script-is-located-from-within-the-script
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
REPO_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)
DATA_DIR=${DATA_DIR:-${REPO_ROOT}/testdata/h2o}
CARGO_COMMAND=${CARGO_COMMAND:-"cargo run -p datafusion-distributed-benchmarks --release"}
H2O_DIR="${DATA_DIR}/sf${SCALE_FACTOR}"
echo "Creating h2o dataset at Scale Factor ${SCALE_FACTOR} in ${H2O_DIR}..."

FILE="${H2O_DIR}/x"
if test -d "${FILE}"; then
    echo " parquet files exist ($FILE exists)."
else
    echo " generating parquet files..."
    $CARGO_COMMAND -- prepare-h2o --output "${H2O_DIR}" --scale-factor "${SCALE_FACTOR}" --partitions "$PARTITIONS"
fi
