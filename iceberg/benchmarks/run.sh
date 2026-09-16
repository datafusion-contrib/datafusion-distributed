#!/usr/bin/env bash

set -e

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)
PACKAGE=datafusion-distributed-iceberg-benchmarks \
BINARY=dfbench-iceberg \
  "$SCRIPT_DIR"/../../benchmarks/run.sh "$@"
