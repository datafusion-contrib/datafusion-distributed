This directory holds the [h2oai/db-benchmark](https://github.com/h2oai/db-benchmark)
groupby queries and generated data.

Generated data is written to `testdata/h2o/sf<scale-factor>/x/` and is run with
`./benchmarks/run.sh --dataset h2o/sf<scale-factor>`.

```bash
# Default SF1 = 10 million rows, 16 parquet files
./benchmarks/gen-h2o.sh

# Tiny smoke-scale dataset (100k rows)
SCALE_FACTOR=0.01 ./benchmarks/gen-h2o.sh

# Official "medium" size (100 million rows)
SCALE_FACTOR=10 ./benchmarks/gen-h2o.sh

WORKERS=8 ./benchmarks/run.sh --threads 2 --dataset h2o/sf1
```

SF1 matches the official "small" `G1_1e7_1e2_0_0` size (N=1e7, K=100). The
table schema and column cardinalities follow
[groupby-datagen.R](https://github.com/h2oai/db-benchmark/blob/master/_data/groupby-datagen.R).
Queries are the ten groupby statements from that suite.

Join and window variants are not included here.
