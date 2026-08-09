#!/usr/bin/env bash

set -e

PARTITION_START=${PARTITION_START:-0}
PARTITION_END=${PARTITION_END:-100}

echo "Generating ClickBench dataset"


# https://stackoverflow.com/questions/59895/how-do-i-get-the-directory-where-a-bash-script-is-located-from-within-the-script
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
REPO_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)
DATA_DIR=${DATA_DIR:-${REPO_ROOT}/testdata/clickbench}
CARGO_COMMAND=${CARGO_COMMAND:-"cargo run -p datafusion-distributed-benchmarks --release"}
CLICKBENCH_DIR="${DATA_DIR}/benchmark_range${PARTITION_START}-${PARTITION_END}"

echo "Creating clickbench dataset from partition ${PARTITION_START} to ${PARTITION_END}"

# Ensure the target data directory exists
mkdir -p "${CLICKBENCH_DIR}"

$CARGO_COMMAND -- prepare-clickbench --output "${CLICKBENCH_DIR}" --partition-start "$PARTITION_START" --partition-end "$PARTITION_END"
