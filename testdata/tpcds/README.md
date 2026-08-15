This directory contains 99 TPC-DS queries from https://github.com/duckdb/duckdb

Generated data is written to `testdata/tpcds/sf<scale-factor>/` and is run with
`./benchmarks/run.sh --dataset tpcds/sf<scale-factor>`.

```bash
# Default SF1, 16 files per table
./benchmarks/gen-tpcds.sh

# Tiny smoke-scale dataset
SCALE_FACTOR=0.01 ./benchmarks/gen-tpcds.sh

# SF10
SCALE_FACTOR=10 ./benchmarks/gen-tpcds.sh
```

SF1 is unpacked from the pre-built parquet in
[datafusion-benchmarks](https://github.com/apache/datafusion-benchmarks). Other
scale factors in `(0, 100000]` are generated with `tpcdsgen`.

## Modifications for DataFusion Compatibility

 - Queries 47 and 57 were modified to add explicit ORDER BY d_moy to avg() window function. DataFusion requires explicit ordering in window functions with PARTITION BY for deterministic results.
 - Query 72 was modified to support date functions in datafusion
