# Contributor Guide

Welcome to the DataFusion Distributed contributor guide!

## Prerequisites

- Rust toolchain (see `rust-toolchain.toml` for the required version)
- Git LFS for test data

## Setup

Clone the repository and set up test data:

```bash
git clone https://github.com/datafusion-contrib/datafusion-distributed
cd datafusion-distributed
git lfs install
git lfs checkout
```

## Running Tests

Running unit tests gives the shortest feedback loop during development.

```bash
# run unit tests
cargo test
```

Integration tests are slower, but cover a wide range of functionality.

```bash
# run unit and integration tests
cargo test --features integration

# run TPCH or TPC-DS integration tests
cargo test --features tpch
cargo test --features tpc-ds
```

## Running Benchmarks

There are two kind of benchmarks in this project:

### Local benchmarks

It's recommended to run these benchmarks locally when contributing for making sure there are no
performance regressions.

First, a TPCH dataset must be generated:

```shell
cd benchmarks 
SCALE_FACTOR=10 ./gen-tpch.sh
```

This might take a while. After that, it's recommended to use the run.sh script to run the benchmarks.
A good setup is to run 8 workers throttled at 2 physical threads per worker. This gives a relatively
accurate benchmarking environment for a distributed system locally.

```shell
WORKERS=8 ./benchmarks/run.sh --threads 2 --path benchmarks/data/tpch_sf10
```

Subsequent runs will compare results against the previous one, so a trick to measure the impact of a PR
is to first run the benchmarks on `main`, and then on the PR branch.

More info about these benchmarks
in https://github.com/datafusion-contrib/datafusion-distributed/blob/main/benchmarks/README.md

### Remote benchmarks

These benchmarks run in a remote EC2 cluster against parquet files stored in S3. These are the most realistic
benchmarks, but also the most expensive to run, in terms of dev iteration cycles (it requires AWS CDK deploys for
every code change), and in terms of money, as it's a real EC2 cluster.

For running these benchmarks, refer
to https://github.com/datafusion-contrib/datafusion-distributed/blob/main/benchmarks/cdk/README.md

## Running Examples

```bash
# In-memory cluster example
cargo run --example in_memory_cluster -- 'SELECT * FROM weather LIMIT 10'

# Localhost workers (requires starting workers first in separate terminals)
cargo run --example localhost_worker -- 8080 --cluster-ports 8080,8081
cargo run --example localhost_run -- 'SELECT * FROM weather LIMIT 10' --cluster-ports 8080,8081
```

## Resources

- [Examples directory](https://github.com/datafusion-contrib/datafusion-distributed/tree/main/examples) - Full working
  examples
- [Integration tests](https://github.com/datafusion-contrib/datafusion-distributed/tree/main/tests) - Feature-specific
  examples
