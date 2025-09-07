#!/usr/bin/env bash

set -e

THREADS=${THREADS:-2}
WORKERS=${WORKERS:-8}

# https://stackoverflow.com/questions/59895/how-do-i-get-the-directory-where-a-bash-script-is-located-from-within-the-script
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

if [ "$WORKERS" == "0" ]; then
  cargo run -p datafusion-distributed-benchmarks --release -- tpch -m --threads "$THREADS"
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

trap cleanup EXIT INT TERM
for i in $(seq 0 $((WORKERS-1))); do
  "$SCRIPT_DIR"/../target/release/dfbench tpch -m --threads "$THREADS" --spawn $((8000+i)) &
done

echo "Waiting for worker ports to be ready..."
for i in $(seq 0 $((WORKERS-1))); do
  wait_for_port $((8000+i))
done

"$SCRIPT_DIR"/../target/release/dfbench tpch -m --threads "$THREADS" --workers $(seq -s, 8000 $((8000+WORKERS-1)))
