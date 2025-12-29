#!/usr/bin/env bash

set -e

PARTITION_START=${PARTITION_START:-0}
PARTITION_END=${PARTITION_END:-100}

echo "Generating ClickBench dataset"


# https://stackoverflow.com/questions/59895/how-do-i-get-the-directory-where-a-bash-script-is-located-from-within-the-script
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
DATA_DIR=${DATA_DIR:-$SCRIPT_DIR/data}
CARGO_COMMAND=${CARGO_COMMAND:-"cargo run --release"}
CLICKBENCH_DIR="${DATA_DIR}/clickbench_${PARTITION_START}-${PARTITION_END}"

echo "Creating clickbench dataset from partition ${PARTITION_START} to ${PARTITION_END}"

# Ensure the target data directory exists
mkdir -p "${CLICKBENCH_DIR}"

$CARGO_COMMAND -- prepare-clickbench --output "${CLICKBENCH_DIR}" --partition-start "$PARTITION_START" --partition-end "$PARTITION_END"
