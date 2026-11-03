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

## Displaying plan dumps

`datafusion_distributed::generate_plan_dump` encodes an executed plan, with its metrics, in a
string that can be logged. `dfd plan` displays it as the tree of stages:

```bash
dfd plan <plan dump>
```

and `--stage` displays the nodes of one stage with their metrics, where stage 0 is the head of the
plan:

```bash
dfd plan <plan dump> --stage 2
```

Without an argument, the plan dump is read from stdin:

```bash
pbpaste | dfd plan --stage 2
```

The output is colored when printed to a terminal, unless `NO_COLOR` is set. `FORCE_COLOR=1` keeps
the colors through a pipe, e.g. into `less -R`.
