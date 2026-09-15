# Iceberg benchmarks

This package composes the generic DataFusion Distributed benchmark runner with the Iceberg backend.
Keeping it separate prevents the benchmark library used by root integration tests from depending on
Iceberg.

## Local benchmarks

From the repository root:

```shell
cargo run -p datafusion-distributed-iceberg-benchmarks --release -- prepare \
  --input testdata/tpch/sf1
WORKERS=2 ./iceberg/benchmarks/run.sh --dataset tpch/sf1_iceberg --threads 2 --partitions 2
```

Preparation writes an immutable sibling dataset named `<input>_iceberg`. The source must be a
non-empty local Parquet dataset, and the destination must be empty. See the
[benchmark guide](../../benchmarks/README.md#iceberg-benchmarks) for generation and comparison.

## Remote benchmarks

Write the Iceberg dataset directly to S3 so every location in its metadata and manifests is remote:

```shell
AWS_REGION=us-east-1 cargo run -p datafusion-distributed-iceberg-benchmarks --release -- prepare \
  --input testdata/tpch/sf1 \
  --output s3://datafusion-distributed-benchmarks/tpch/sf1_iceberg
```

Use an unused output prefix. Preparation refuses a destination containing `_SUCCESS`, and writes
that marker only after every table commits.

Build the remote worker with Iceberg extensions installed in both its coordinator and execution
sessions:

```shell
cargo build -p datafusion-distributed-iceberg-benchmarks --release --bin iceberg-worker
```

The remote benchmark needs no catalog service. Preparation uses an ephemeral in-memory catalog to
write each snapshot; execution registers every immutable table directly from its stable metadata URI
through the existing HTTP SQL endpoint:

```shell
curl --get http://datafusion-worker:9000/ --data-urlencode \
  "sql=CREATE EXTERNAL TABLE lineitem STORED AS ICEBERG LOCATION 's3://datafusion-distributed-benchmarks/tpch/sf1_iceberg/lineitem/metadata.json'"
```

Repeat registration for the other TPC-H tables before issuing benchmark queries. The worker uses
the standard AWS environment or instance-role credential chain. Glue is only needed when testing
catalog operations or mutable tables, neither of which is part of these immutable read benchmarks.
