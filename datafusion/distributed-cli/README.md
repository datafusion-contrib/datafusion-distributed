# Distributed DataFusion CLI

`dfd` is the [DataFusion CLI](https://datafusion.apache.org/user-guide/cli/index.html) with
distributed execution:

- `dfd query` runs SQL like `datafusion-cli`, but distributes each query across 16 in-memory
  workers, so you can see how queries get distributed without setting up a cluster.
- `dfd plan` displays the plan dumps that applications built with DataFusion Distributed log.

```bash
cargo install datafusion-distributed-cli
```

## Running queries

Query a month of [NYC taxi trips](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
straight from its public URL:

```bash
dfd query -c "SET distributed.file_scan_config_bytes_per_partition = 1;" \
  "CREATE EXTERNAL TABLE trips STORED AS PARQUET LOCATION 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet';" \
  "EXPLAIN ANALYZE SELECT payment_type, count(*) FROM trips GROUP BY payment_type;"
```

Each partition scans 16 MiB by default, too much for a file this small to be worth distributing,
so the `SET` makes partitions as small as possible. `EXPLAIN ANALYZE` then shows the stages the
query ran in, connected by the network (a plain `EXPLAIN` shows the plan before it is distributed):

```text
┌───── DistributedExec ── ...
│ [Stage 2] => NetworkCoalesceExec: output_partitions=256, input_tasks=16, ...
└──────────────────────────────────────────────────
  ┌───── Stage 2 ── tasks=16, partitions=16 ...
  │ ProjectionExec: expr=[payment_type@0 as payment_type, count(Int64(1))@1 as count(*)], ...
  │   AggregateExec: mode=FinalPartitioned, gby=[payment_type@0 as payment_type], ...
  │     RepartitionExec: partitioning=Hash([payment_type@0], 16), ...
  │       [Stage 1] => NetworkShuffleExec: output_partitions=16, input_tasks=16, ...
  └──────────────────────────────────────────────────
    ┌───── Stage 1 ── tasks=16, partitions=16 ...
    │ RepartitionExec: partitioning=Hash([payment_type@0, ...], 16), ...
    │   AggregateExec: mode=Partial, gby=[payment_type@0 as payment_type], ...
    │     DistributedLeafExec: ...
    │       t0: DataSourceExec: file_groups={16 groups: [[trip-data/yellow_tripdata_2024-01.parquet:0..195163], ...
    │       ...
    └──────────────────────────────────────────────────
```

Without `-c`, `dfd query` opens an interactive shell. Everything else works like `datafusion-cli`;
see its [usage guide](https://datafusion.apache.org/user-guide/cli/usage.html).

## Displaying plan dumps

`datafusion_distributed::generate_plan_dump` encodes an executed plan and its metrics into a single
line of text, small enough to be logged with every query:

```rust
let plan = df.create_physical_plan().await?;
let batches = collect(plan.clone(), ctx.task_ctx()).await?;
log::info!("plan dump: {}", generate_plan_dump(&plan, GeneratePlanDumpOptions::default())?);
```

`dfd plan` displays it as the tree of stages. It takes the dump, a file containing it, or stdin:

```bash
dfd plan <plan dump>
dfd plan plan_dump.txt
pbpaste | dfd plan
```

Options:

- `--stage N` displays the nodes of one stage with their metrics. Stage 0 is the head of the plan,
  run by the coordinator.
- `--verbose` displays metrics per task, like `output_rows={0:10, 1:7}`, instead of added up across
  tasks, like `output_rows=17`. This shows how the work was spread.
- `--metric TEXT` displays only the metrics whose name contains `TEXT`. Repeat it to match any of
  several, like `--metric bytes --metric rows`. `--exact` matches whole names instead, so
  `--metric output_bytes --exact` leaves out `estimated_output_bytes`.
- `--totals` adds up the metrics of every node into totals for the whole query, like its CPU time
  with `--totals --metric elapsed_compute --exact`. With `--stage`, only that stage is added up.

Besides their own metrics, stages show metrics derived from the ones of their nodes, whose names
start with `stage_`: `stage_elapsed_compute`, the CPU time of all their nodes, `stage_output_rows`
and `stage_output_bytes`, what they returned, and, when their output was estimated for choosing
task counts dynamically, `stage_output_bytes_q_error`: the ratio between the estimated and the
actual bytes they returned, the larger over the smaller.

The output is colored in a terminal, unless `NO_COLOR` is set. `FORCE_COLOR=1` keeps the colors
through a pipe, like into `less -R`.

### Inspecting plan dumps with coding agents

The `plan-dump` agent skill teaches coding agents like Claude Code, Codex or Cursor to inspect plan
dumps with `dfd plan`: finding where a query spends its time, or comparing two plans of the same
query. Add it to your own project with:

```bash
npx skills add datafusion-contrib/datafusion-distributed --skill plan-dump
```
