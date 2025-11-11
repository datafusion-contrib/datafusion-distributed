#!/bin/bash

echo "Running test 100 times to detect flakes..."
failures=0
for i in $(seq 1 100); do
    if ! cargo test test_concurrent_gc_and_access --lib -- --nocapture --test-threads=1 2>&1 | grep -q "test result: ok"; then
        echo "FAILED on run $i"
        failures=$((failures + 1))
    fi
    if [ $((i % 10)) -eq 0 ]; then
        echo "Completed $i runs, failures: $failures"
    fi
done
echo "Total failures: $failures/100"
