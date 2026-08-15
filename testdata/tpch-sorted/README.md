# Sorted TPC-H dataset

Physical variant of the standard TPC-H workload. Generation uses the same
`tpchgen` data as `testdata/tpch`, then globally sorts each table by its
TPC-H primary key and writes range-partitioned Parquet files.

Queries are the standard TPC-H queries in `testdata/tpch/queries`.

## Sort keys

| Table    | Sort columns                   |
|----------|--------------------------------|
| region   | `r_regionkey`                  |
| nation   | `n_nationkey`                  |
| customer | `c_custkey`                    |
| supplier | `s_suppkey`                    |
| part     | `p_partkey`                    |
| partsupp | `ps_partkey`, `ps_suppkey`     |
| orders   | `o_orderkey`                   |
| lineitem | `l_orderkey`, `l_linenumber`   |

Rows are sorted ascending with nulls last. Files under each table directory
are range-partitioned: concatenating `1.parquet`, `2.parquet`, ... yields a
globally sorted sequence. Each file records those columns in Parquet
`sorting_columns` metadata.

## Generating

```bash
# Default SF1, 16 files per table
./benchmarks/gen-tpch-sorted.sh

# Tiny smoke-scale dataset
SCALE_FACTOR=0.01 ./benchmarks/gen-tpch-sorted.sh
```

Data is written to `testdata/tpch-sorted/sf<scale-factor>/`.

```bash
WORKERS=8 ./benchmarks/run.sh --threads 2 --dataset tpch-sorted/sf1
```
