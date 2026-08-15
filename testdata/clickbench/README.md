This directory contains the 43 official ClickBench queries (`q0`–`q42`) from
https://github.com/ClickHouse/ClickBench.

Generated data is written to `testdata/clickbench/<start>-<end>/hits/` and is
run with `./benchmarks/run.sh --dataset clickbench/<start>-<end>`. The suite
root (`testdata/clickbench`) is not a valid `--dataset` path; it only holds
these query files.

```bash
# Default: partitions 0..100 of the official Athena-partitioned hits files
./benchmarks/gen-clickbench.sh

# Tiny smoke slice (partition 0 only)
PARTITION_START=0 PARTITION_END=1 ./benchmarks/gen-clickbench.sh

WORKERS=8 ./benchmarks/run.sh --threads 2 --dataset clickbench/0-100
```

Files are downloaded from
https://datasets.clickhouse.com/hits_compatible/athena_partitioned/.
`EventDate` is rewritten from the raw integer days-since-epoch type to
`Date32` so comparisons with date string literals (for example `'2013-07-01'`)
type-check at planning time. The rewrite is idempotent.
