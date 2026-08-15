This directory holds the [Join Order Benchmark](https://github.com/gregrahn/join-order-benchmark)
(JOB) queries over the IMDB snapshot published at
<https://event.cwi.nl/da/job/imdb.tgz>.

Unlike TPC-H / TPC-DS, IMDB is a fixed real-world dataset (no scale factor).
Generated parquet tables are written to `testdata/imdb/<table>/` and run with
`--dataset imdb`.

```bash
# Downloads ~1.2 GB, extracts 21 CSVs, converts each to parquet
./benchmarks/gen-imdb.sh

WORKERS=8 ./benchmarks/run.sh --threads 2 --dataset imdb
```

Queries are the 113 official JOB statements (`q1a`–`q33c`) from
[gregrahn/join-order-benchmark](https://github.com/gregrahn/join-order-benchmark),
matching Apache DataFusion's `benchmarks/queries/imdb`. Table schemas match
DataFusion's `get_imdb_table_schema`.

The downloaded `imdb.tgz` / CSV / parquet files are gitignored. Do not generate
the full snapshot in review unless needed.
