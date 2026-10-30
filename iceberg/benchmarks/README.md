# Local Iceberg benchmarks

This package composes the generic DataFusion Distributed benchmark runner with the Iceberg table
backend. Keeping it separate prevents the benchmark library used by root integration tests from
depending on Iceberg.

From the repository root:

```shell
DATASET=$(cargo run -p datafusion-distributed-benchmarks --release -- prepare-tpch)
ICEBERG=$(cargo run -p datafusion-distributed-iceberg-benchmarks --release -- prepare \
  --input $DATASET)
cargo run -p datafusion-distributed-iceberg-benchmarks --release -- run \
  --workers 2 --dataset $ICEBERG --partitions 2
```

Preparation writes an immutable `tpch_iceberg` dataset by default, whose path repeats the input's
parameters followed by `target_file_size`.
Use `--output <directory|s3://bucket/prefix>` to choose its destination. The source must be a
non-empty local Parquet dataset. Converting into a complete dataset does nothing, and any other
non-empty destination is rejected. See the
[benchmark guide](../../benchmarks/README.md#iceberg-benchmarks) for generation and comparison.
