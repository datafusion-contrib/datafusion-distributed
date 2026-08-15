#!/usr/bin/env bash

set -e

PARTITION_START=${PARTITION_START:-0}
PARTITION_END=${PARTITION_END:-100}

echo "Generating sorted ClickBench dataset"

# https://stackoverflow.com/questions/59895/how-do-i-get-the-directory-where-a-bash-script-is-located-from-within-the-script
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
REPO_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)
DATA_DIR=${DATA_DIR:-${REPO_ROOT}/testdata/clickbench-sorted}
CARGO_COMMAND=${CARGO_COMMAND:-"cargo run -p datafusion-distributed-benchmarks --release"}
VARIANT="${PARTITION_START}-${PARTITION_END}"
CLICKBENCH_DIR="${DATA_DIR}/${VARIANT}"

echo "Creating sorted clickbench dataset from partition ${PARTITION_START} to ${PARTITION_END} in ${CLICKBENCH_DIR}"
echo "Global sort key: (CounterID, EventDate, UserID, EventTime, WatchID)"

# Ensure the target data directory exists
mkdir -p "${CLICKBENCH_DIR}"

$CARGO_COMMAND -- prepare-clickbench-sorted --output "${CLICKBENCH_DIR}" --partition-start "$PARTITION_START" --partition-end "$PARTITION_END"
