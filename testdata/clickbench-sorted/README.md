# Sorted ClickBench

Globally sorted variant of the ClickBench `hits` dataset. Physical file order
is a controlled benchmark dimension; the query workload is the same as
`testdata/clickbench/queries`.

## Sort key

Parquet files under `<variant>/hits/` are written as one globally sorted
sequence using the official ClickHouse `hits` MergeTree `ORDER BY`:

```
(CounterID, EventDate, UserID, EventTime, WatchID)
```

Each key is sorted `ASC NULLS FIRST`. Every row in `i.parquet` compares `<=`
every row in `(i+1).parquet` under this key.

## Generating data

```shell
# Full 100-file dataset → testdata/clickbench-sorted/0-100
./benchmarks/gen-clickbench-sorted.sh

# Smaller subset, e.g. first 3 partitions → testdata/clickbench-sorted/0-3
PARTITION_START=0 PARTITION_END=3 ./benchmarks/gen-clickbench-sorted.sh
```

Then run:

```shell
WORKERS=8 ./benchmarks/run.sh --threads 2 --dataset clickbench-sorted/0-100
```
