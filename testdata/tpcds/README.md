This directory contains 99 TPC-DS queries from https://github.com/duckdb/duckdb

## Modifications for DataFusion Compatibility

 - Queries 47 and 57 were modified to add explicit ORDER BY d_moy to avg() window function. DataFusion requires explicit ordering in window functions with PARTITION BY for deterministic results.
 - Query 72 was modified to support date functions in datafusion

`generate.sh {SCALE_FACTOR}` is a script which can generate TPC-DS parquet data. Requires the duckdb CLI: https://duckdb.org/install/
