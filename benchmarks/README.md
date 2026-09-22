# Local DataFusion benchmarks

### Generating Benchmarking data

Generate datasets alongside the integration-test fixtures under `testdata/`.
For example, `tpch/sf1` is stored in `testdata/tpch/sf1`.
TPC-H generation partitions the scalable tables, but writes `nation` (25 rows) and `region`
(5 rows) only once. The preparation commands require an empty output directory or S3 prefix.
Prepare a new Iceberg copy separately after regenerating Parquet; an existing copy is not updated.

```shell
# TPC-H (default: SCALE_FACTOR=1, PARTITIONS=16, SORTED=false - override by setting these environment variables)
./gen-tpch.sh

# Sorted TPC-H (same generators, Parquet sorting_columns metadata)
SORTED=true ./gen-tpch.sh

# TPC-DS (default: SCALE_FACTOR=1, PARTITIONS=16 - override with environment variables)
./gen-tpcds.sh
```

`tpch/sorted_sf1` is written to `testdata/tpch/sorted_sf<scale-factor>`. Use a
small `SCALE_FACTOR` (for example `0.01`) for a cheap smoke generation; SF1 is
the default. Files record the columns tpchgen already emits in order:
`r_regionkey`, `n_nationkey`, `c_custkey`, `s_suppkey`, `p_partkey`,
`ps_partkey`, `o_orderkey`, and `(l_orderkey, l_linenumber)`.

### Writing datasets to S3

Pass `--output` to a generation script to choose a local directory or S3 prefix:

```shell
SCALE_FACTOR=1 PARTITIONS=16 ./benchmarks/gen-tpch.sh --output s3://my-bucket/tpch/sf1
./benchmarks/gen-tpcds.sh --output s3://my-bucket/tpcds/sf1
PARTITION_END=1 ./benchmarks/gen-clickbench.sh --output s3://my-bucket/clickbench/0-1
```

The corresponding `dfbench prepare-* --output` commands accept the same destinations.
Use an empty destination. `_SUCCESS` is written only after every table has finished;
if generation fails, use a new prefix or remove the incomplete output before retrying.
TPC-H and TPC-DS stream generated batches directly to Parquet in the destination.
ClickBench uses temporary storage for one partition at a time to correct its date column.

Parquet generation resolves credentials once using the official AWS SDK credential chain,
including environment credentials, profiles, SSO, and instance roles. For SSO, sign in and select the profile before generating:

```shell
aws sso login --profile my-profile
export AWS_PROFILE=my-profile
export AWS_REGION=us-east-1 # use your bucket's region
```

The region is read from the environment or selected profile. Credentials must remain
valid for the whole run; the generator does not refresh them.

Iceberg's storage library supports environment credentials and instance roles. For an
SSO session, export credentials before converting a local dataset to Iceberg:

```shell
eval "$(aws configure export-credentials --profile my-profile --format env)"
```

The destination bucket must already exist. Generation needs list, read and write access,
including multipart upload access. Iceberg metadata and manifests are created with their
final S3 paths; no metadata rewriting or filesystem sync is needed.

### Running Benchmarks in single-node mode

After generating the data with the command above, the benchmarks can be run with:

```shell
WORKERS=0 ./benchmarks/run.sh --threads 2 --dataset tpch/sf1
```

- `--threads`: This is the physical threads that the Tokio runtime will use for executing the
  binary. It's recommended to set `--threads` to something small, like `2`, for throttling each
  individual process running queries, and simulate how adding throttled workers can speed up the
  queries.
- `--dataset`: Logical dataset name (e.g. `tpch/sf1`, `tpch/sorted_sf1`,
  `tpcds/sf1`). It is resolved to the corresponding
  `testdata/<suite>/<variant>` directory.

### Running benchmarks with local workers

The same script is used for running distributed benchmarks:

```shell
WORKERS=8 ./benchmarks/run.sh --threads 2 --dataset tpch/sf1 --file-scan-config-bytes-per-partition 16777216
```

- `WORKERS`: Env variable that sets the amount of localhost workers used in the query.
- `--threads`: Sets the Tokio runtime threads for each individual worker and for the benchmarking
  binary.
- `--dataset`: Dataset directory name under `testdata`.
- `--file-scan-config-bytes-per-partition`: How many bytes each partition is expected to scan. Lower values
  produce more partitions/tasks. Defaults to the engine default when unset.

### Iceberg benchmarks

Iceberg preparation and execution live in a separate package so the benchmark crate used by the
root integration tests does not depend on Iceberg. Prepare the Parquet input, then convert it:

```shell
cargo run -p datafusion-distributed-benchmarks --release -- prepare-tpch \
  --output testdata/tpch/sf1 --scale-factor 1 --partitions 16
cargo run -p datafusion-distributed-iceberg-benchmarks --release -- prepare \
  --input testdata/tpch/sf1

# Or write the Iceberg representation directly to S3:
cargo run -p datafusion-distributed-iceberg-benchmarks --release -- prepare \
  --input testdata/tpch/sf1 --output s3://my-bucket/tpch/sf1_iceberg
```

Without `--output`, the conversion writes the sibling `testdata/tpch/sf1_iceberg/` dataset and leaves the source
unchanged. It streams one source file at a time into unpartitioned, append-only Iceberg tables.
Source file boundaries are preserved unless `--target-file-size` requests rolling. `_SUCCESS` is
written last, and the output directory must be empty.

Run each representation with its format-specific binary, then compare them with `dfbench`:

```shell
WORKERS=2 ./benchmarks/run.sh --dataset tpch/sf1 --threads 2 --partitions 2
WORKERS=2 ./iceberg/benchmarks/run.sh --dataset tpch/sf1_iceberg --threads 2 --partitions 2

dfbench compare tpch/sf1 tpch/sf1_iceberg
dfbench compare tpch/sf1@base tpch/sf1_iceberg@candidate
dfbench compare base candidate --dataset tpch/sf1
```

`compare` takes two `dataset[@branch]` states, [prev] then [new]. An omitted branch defaults to the
current branch. With `--dataset`, both positional arguments remain literal branch names. Each
dataset uses the existing `.results/<branch>/` and `previous.json` layout.

Absolute dataset paths are supported when they follow the same `<suite>/<variant>` convention.
Iceberg runs always load manifest column statistics. For larger scale factors, increase Parquet
generation `--partitions` to avoid oversized source files. Generated metadata contains absolute
local locations; cloud publication and remote harness support remain separate work.

### Remote worker

The remote benchmark harness uses the `worker` binary in the
`datafusion-distributed-remote-worker` package. It serves Parquet benchmarks by
default. Build the same binary with the `iceberg` feature to enable Iceberg
session configuration on both the coordinator and every distributed worker:

```shell
cargo build -p datafusion-distributed-remote-worker --release
cargo build -p datafusion-distributed-remote-worker --release --features iceberg
```
