#!/bin/bash

# SQLLogicTest runner script for DataFusion Distributed
# This script runs the sqllogictest CLI against our test files to verify
# that distributed query execution produces expected results.

set -e  # Exit on any error

# Test basic queries (aggregations, filtering, etc.)
cargo run --features integration --bin logictest -- tests/sqllogictest/basic_queries.slt --num-workers 3

# Test EXPLAIN queries (distributed physical plans)
cargo run --features integration --bin logictest -- tests/sqllogictest/explain.slt --num-workers 3
