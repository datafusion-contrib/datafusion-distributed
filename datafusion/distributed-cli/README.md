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

Metrics are added up across all tasks, like `output_rows=17`. `--verbose` displays them per task
instead, like `output_rows={0:10, 1:7}`, which is longer but shows how the work was spread:

```bash
dfd plan <plan dump> --stage 2 --verbose
```

`--metric` displays only the metrics whose name contains a text, and can be repeated to display the
metrics matching any of them, like the bytes and rows of every node:

```bash
dfd plan <plan dump> --stage 2 --metric bytes --metric rows
```

Like `cargo test -- --exact` does with test names, `--exact` matches whole metric names instead, so
`output_bytes` does not display `estimated_output_bytes` too:

```bash
dfd plan <plan dump> --stage 2 --metric output_bytes --exact
```

Besides their own metrics, stages show metrics derived from the ones of their nodes, whose names
start with `stage_`: `stage_elapsed_compute`, the CPU time of all their nodes, `stage_output_rows`
and `stage_output_bytes`, what they returned, and, when their output was estimated for choosing
task counts dynamically, `stage_output_bytes_q_error`: the ratio between the estimated and the
actual bytes they returned, the larger over the smaller.

The argument can also be the path of a file containing the plan dump:

```bash
dfd plan plan_dump.txt --stage 2
```

Without an argument, the plan dump is read from stdin:

```bash
pbpaste | dfd plan --stage 2
```

The output is colored when printed to a terminal, unless `NO_COLOR` is set. `FORCE_COLOR=1` keeps
the colors through a pipe, e.g. into `less -R`.

### Inspecting plan dumps with coding agents

The `plan-dump` agent skill teaches coding agents like Claude Code, Codex or Cursor to inspect plan
dumps with `dfd plan`: finding where a query spends its time, or comparing two plans of the same
query. Add it to your own project with:

```bash
npx skills add datafusion-contrib/datafusion-distributed --skill plan-dump
```
