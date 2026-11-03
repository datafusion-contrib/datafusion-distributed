# Distributed DataFusion CLI

This is the same CLI as https://github.com/apache/datafusion/blob/main/datafusion-cli/,
but enriched with distributed execution capabilities.

## Installation

```bash
cargo install datafusion-distributed-cli
```

Or from source, by cloning this repository:

```bash
git clone https://github.com/datafusion-contrib/datafusion-distributed
cd datafusion-distributed
```

And running:

```bash
cargo install --path cli
```

## Usage

The CLI is installed as `dfd`. Queries are run with:

```bash
dfd query
```

The best way of trying distributed queries is by issuing queries against the parquet files in the
`testdata` directory. For maximum parallelism, set the following config options:

```sql
SET distributed.files_per_task = 1;
```

The usage is exactly the same as the original CLI:
https://datafusion.apache.org/user-guide/cli/usage.html
