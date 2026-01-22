# Benchmarks

There are two kinds of benchmarks in this project.

## Local Benchmarks

It's recommended to run these benchmarks locally when contributing to ensure there are no performance regressions.

### Generating Test Data

First, a TPCH dataset must be generated:

```bash
cd benchmarks
SCALE_FACTOR=10 ./gen-tpch.sh
```

This might take a while.

### Running Benchmarks

After generating the data, it's recommended to use the `run.sh` script to run the benchmarks.
A good setup is to run 8 workers throttled at 2 physical threads per worker. This provides a relatively
accurate benchmarking environment for a distributed system locally.

```bash
WORKERS=8 ./benchmarks/run.sh --threads 2 --path benchmarks/data/tpch_sf10
```

Subsequent runs will compare results against the previous one, so a useful trick to measure the impact of a PR
is to first run the benchmarks on `main`, and then on the PR branch.

More information about these benchmarks can be found in the [benchmarks README](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/benchmarks/README.md).

## Remote Benchmarks

These benchmarks run on a remote EC2 cluster against parquet files stored in S3. These are the most realistic
benchmarks, but also the most expensive to run in terms of development iteration cycles (it requires AWS CDK deploys for
every code change) and cost, as it uses a real EC2 cluster.

For running these benchmarks, refer to the [CDK benchmarks README](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/benchmarks/cdk/README.md).
