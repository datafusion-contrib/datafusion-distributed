# Local Iceberg benchmarks

This package composes the generic DataFusion Distributed benchmark runner with the Iceberg table
backend. Keeping it separate prevents the benchmark library used by root integration tests from
depending on Iceberg.

From the repository root:

```shell
cargo run -p datafusion-distributed-iceberg-benchmarks --release -- prepare \
  --input testdata/tpch/sf1
WORKERS=2 ./iceberg/benchmarks/run.sh --dataset tpch/sf1_iceberg --threads 2 --partitions 2
```

Preparation writes an immutable sibling dataset named `<input>_iceberg` by default.
Use `--output <directory|s3://bucket/prefix>` to choose its destination. The source must be a
non-empty local Parquet dataset, and the destination must be empty. See the
[benchmark guide](../../benchmarks/README.md#iceberg-benchmarks) for generation and comparison.
