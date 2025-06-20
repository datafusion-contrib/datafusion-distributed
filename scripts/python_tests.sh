#!/bin/bash

# This script is meant to run within the context of CI pipelines
# and will run the tpch benchmark test and assert that all output
# are correct

set -ex

# get the directory of this script
DIRNAME=$(dirname $0)

# directory of our repo
REPO_DIR=$DIRNAME/../../

mkdir -p $TPCH_DATA_PATH

cd $REPO_DIR
python -m venv venv

source venv/bin/activate
pip install -r $REPO_DIR/tpch/requirements.txt
maturin develop

python tpch/make_data.py $TPCH_SCALING_FACTOR $TPCH_DATA_PATH

DATAFUSION_RAY_LOG_LEVEL=debug RAY_COLOR_PREFIX=1 RAY_DEDUP_LOGS=0 python tpch/tpcbench.py --data=file:///$TPCH_DATA_PATH/ --concurrency 3 --partitions-per-processor 2 --batch-size=8192 --worker-pool-min=20 --validate
