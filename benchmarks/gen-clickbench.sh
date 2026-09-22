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
CLICKBENCH_DIR="${DATA_DIR}/${PARTITION_START}-${PARTITION_END}"

if [ "$#" -gt 0 ]; then
    if [ "$#" -ne 2 ] || [ "$1" != "--output" ]; then
        echo "Usage: $0 [--output <directory|s3://bucket/prefix>]" >&2
        exit 1
    fi
    CLICKBENCH_DIR="$2"
fi

echo "Creating clickbench dataset from partition ${PARTITION_START} to ${PARTITION_END}"

$CARGO_COMMAND -- prepare-clickbench --output "${CLICKBENCH_DIR}" --partition-start "$PARTITION_START" --partition-end "$PARTITION_END"
