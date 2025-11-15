#!/usr/bin/env bash

set -e

WORKERS=${WORKERS:-8}

# https://stackoverflow.com/questions/59895/how-do-i-get-the-directory-where-a-bash-script-is-located-from-within-the-script
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

if [ "$WORKERS" == "0" ]; then
  cargo run -p datafusion-distributed-benchmarks --release -- tpch "$@"
  exit
fi

cleanup() {
  echo "Cleaning up processes..."
  for i in $(seq 1 $((WORKERS))); do
    kill "%$i"
  done
}

wait_for_port() {
  local port=$1
  local timeout=30
  local elapsed=0
  while ! nc -z localhost "$port" 2>/dev/null; do
    if [ "$elapsed" -ge "$timeout" ]; then
      echo "Timeout waiting for port $port"
      return 1
    fi
    sleep 0.1
    elapsed=$((elapsed + 1))
  done
  echo "Port $port is ready"
}

cargo build -p datafusion-distributed-benchmarks --release
WORKER_URLS=$(seq 8000 $((8000+WORKERS-1)) | sed 's|^|http://localhost:|' | paste -sd,)

trap cleanup EXIT INT TERM
for i in $(seq 0 $((WORKERS-1))); do
  "$SCRIPT_DIR"/../target/release/dfbench tpch --workers "$WORKER_URLS" --spawn $((8000+i)) "$@" &
done

echo "Waiting for worker ports to be ready..."
for i in $(seq 0 $((WORKERS-1))); do
  wait_for_port $((8000+i))
done

"$SCRIPT_DIR"/../target/release/dfbench tpch --workers "$WORKER_URLS" "$@"
