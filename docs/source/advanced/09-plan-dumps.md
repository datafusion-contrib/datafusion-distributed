# Logging and Inspecting Plan Dumps

A distributed plan rendered with its metrics, as described in
[Collecting Runtime Metrics](../user-guide/05-metrics.md), can take hundreds of kilobytes, and
grows with every stage and task. That makes it expensive to log for every query, and hard to read once
it is in your logs.

A plan dump is a compact alternative: the executed plan, with its stages and per-task metrics,
encoded into a single base64 line that fits in a log field.

Plan dumps are meant to be read by coding agents. A dump holds hundreds of metrics across every
stage and task, which is slow to go through by hand but quick for an agent. With the
[`plan-dump` skill](#inspecting-dumps-with-coding-agents), an agent can tell where a query spent
its time, or what changed between two runs of it, and point you at the parts of the plan behind
its answer, which you can then look at yourself with the `dfd` CLI.

## Generating a dump

Generate the dump once the query has been fully executed and the worker metrics have been folded
back into the plan:

```rust
let plan = ctx.sql(sql).await?.create_physical_plan().await?;
let batches = execute_stream(plan.clone(), ctx.task_ctx())?
    .try_collect::<Vec<_>>()
    .await?;

let plan = rewrite_distributed_plan_with_metrics(plan, DistributedMetricsFormat::PerTask).await?;
tracing::info!(plan_dump = %generate_plan_dump(&plan, GeneratePlanDumpOptions::default())?, "query finished");
```

Log the dump as a field next to the rest of the query's telemetry, like its id and latency, so you
can find the dump of a slow query afterward.

## How much a dump costs

A dump is a protobuf message compressed with zstd, so it takes far less space than the rendered
plan while holding more: every metric of every task. Generating it also takes less time than
rendering the plan with the same per-task metrics:

- about 2x faster to generate
- up to 30x less size

## Inspecting a dump

`dfd` is the CLI of DataFusion Distributed:

```bash
cargo install datafusion-distributed-cli
```

`dfd plan` takes a dump as an argument, as the path of a file holding it, or through stdin. With no
other options, it shows the tree of stages with the metrics of each stage, and `--stage N` shows the
nodes of one stage with theirs. `dfd plan --help` lists the rest of its options.

### Try it yourself

This is the dump of [TPC-H query 3](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/testdata/tpch/queries/q3.sql)
at scale factor 1, run on 4 workers with the default configuration. It took 76 ms. The query joins
customers, their orders, and the line items of those orders, then adds up revenue per order. Save
the dump to a file:

```bash
echo 'KLUv/WIBgL8rdS0As+KF3/M7OD3zzk7PvOP9CtJnIpoDiAJdQtJ5XbRBdVA3usXrog0qF9AFJ85Xq712vcC82vlfZyPZplnrk7OAGxYmIjBDGBGBHKpAyJnpoiR2vcAE01X8c3DFcYDjAEctzEHd6BYuoAvOmscsDSdKPtNFSfy+UTZdLzCJVEe7+V/nCJzcINm8J6JY1XNsaAbQmP2+UTZEqqP0gE/ukjnyy9kb1je0LwOIkX9zfUCSJ6+bRwc44iOxCWwylHgZR/INku/0vJA+F5HM5NSsC6rsVLQ1W6080XAComvmofOJhhMepzMeaGZrtlrJ6pRR5k80nFAY1aMwqse9cyr8RujdFXmUT4aiIArzap9bCQ8/PfG9vTNRnDJswvqaZhN6ubMQ8CERxSmjAncvtwm9fBYXG0R0WALR4YQWFtnMC2zqqHdX3NTln8UFlZzCqF69hsV2BwYBQwqjek6HDrqRLC6w2XoNi3oNi+2CnA+D+zqPDLl6DYubzOnZh/6AzArHvq8hlOEx8/9v75jz/9A5/+/xIcytqCBR2zHGI3KGpUPIUdtxhgWHtKKCZIxHRIdBpCCabAvK7AvCLQ6O2g61HWdYzrDUDYDC4A1LsrKA2D/iBj+vLjsB7sz78535OohOrizvYBdRI6/QpIJdq+2igCWwiW8BGkh0wJpq7STssHUJQB62nT+gqSj3BaOIzJ2k9LqZQgrSfDbMqzex2IF3/E7g5joLwJmT9lWbK8GJNTirTHnsgTsvLHO9H577TZJVP5F0k9vbOXZoJlRfuTngV5PAPQITZ3KLgvnSJGdUMec02ybscV6r6a607Cc8DTPtgyOXArLUqOqIWJqSrVWyotC8lvgH4XqQTxhiSpp6R5e/rDImTBgDtb8cBwiYOIK1hGM/xD6O/olARAax0Ap6xQbmBXfDfBGO3tDlzhCY/Ueyg7FSwzg3oM1Sy2CmlklMPMMIBHVx7MxlGMKnxDWZHQQa4emM+YfExqUSfEK7Mii7vTafD9pg+EqVVar4w8jP3dZGzntlU0/sEPjj6rYjRVd2dHONnNgHQzAdpfQis46+XBIcSfPQHoH88v/TCwgUJdjjcSCeSZZjEqfWRpGBe9JjXNmdM47sAD/4e9aC15TbyW2mIseMCIIh0pG9aitGNnoiAyu+glWx8bjufHUSFra2/2EQyTqmiBKkAWVNvxStRW3yiJ6ONsM+QR1sGHdJJfSw+GJc+b+s9ErpugO4TCYhOjFjGAPQFUZEwggctYl+svoyIIYRyAJN8hnnIf/j0jGJTUElTg05boYOS6GRKghlYumV0X8L192eOLxeoytsuUQp7Mr9slZe9W9tRE0S//H+ltvhby2cP36gtRHLALLfwt0F32OYDPh2WWAvIbq7rguBifyWc4WT962V7tzaNZDOpxLgsl73mZN2mCGXOrGNTaCXjyLqf2CH4SKGi5YB+6BD+6vnrckzY2SLGfgD084+0652hi40cp2456vzpWVhL5coh9mzP0eckYCsTQoohdIbc6It7Wu/9nfwv6moTbeixUoME3ZofweupADUwOGVtR2kw1Zz5ACnLagERmJwDUnrjxVOgi4K3p/6m2d+lP7OSmfVz0ZAhvrptF0bIJ6lWfmTgT91as+hOJNW5rjXHOs0tIOWeEAQWswdqApujmDCDSDyrMuxp6GrgUS5f59htBSHHx5uVd4LXh6WJWgDh42vXz7Z4WBeLckk0vqzhTk6xnL5v7Krs/wQKhRO0EbQXoFfwZpvBo5uu5c+0XGUdV8hRES0rB1YfCdplZiS2uP40RzIUzg8p+wDVa03sBXzES4bRfTRvdncSO7nH6oH+hdlKLMgBMY1EvwWPvS9EvZlV7yRcZHHWh6n7T6esVMk1W/tkiRvlKnlMuhZaPOegsDcjhiMSPHSFbagmRj72Qop1BTalJoQT9tDyR7ot4Y=' > tpch-q3.dump.txt
```

Then follow the steps below to see where the query's rows, bytes and CPU went:

::::{tab-set}

:::{tab-item} 1. The stages

Start with the tree of stages. `-m` keeps only the metrics you name, and `--exact` matches whole
names:

```
dfd plan tpch-q3.dump.txt -m stage_output_rows -m stage_output_bytes --exact
```
```rust
DistributedExec
│ stage_output_rows=11.62 K
│ stage_output_bytes=362.7 KB
└─ Stage 3 (Coalesce) tasks=2 partitions=16
   │ stage_output_rows=11.62 K
   │ stage_output_bytes=362.7 KB
   ├─ Stage 1 (Shuffle) tasks=1 partitions=32
   │    stage_output_rows=147.1 K
   │    stage_output_bytes=2.2 MB
   └─ Stage 2 (Shuffle) tasks=2 partitions=32
        stage_output_rows=3.24 M
        stage_output_bytes=123.4 MB
```

Stage 1 joins customers with their orders, stage 2 reads the line items, and stage 3 joins both
and adds up the revenue. A stage's `stage_output_rows` and `stage_output_bytes` are what its
reader received over the network, so the line items of stage 2 are almost everything this query
sent between workers: 123.4 MB out of 126 MB.
:::

:::{tab-item} 2. Inside a stage

`--stage N` opens one stage, showing its nodes with their own metrics. Stage 3 is where both sides
meet:

```
dfd plan tpch-q3.dump.txt --stage 3 -m output_rows -m elapsed_compute --exact
```
```rust
Stage 3 (Coalesce) tasks=2 partitions=16
└─ ProjectionExec: expr=[...]
   │ output_rows=11.62 K
   │ elapsed_compute=32.67µs
   └─ SortExec: expr=[...], preserve_partitioning=[true]
      │ output_rows=11.62 K
      │ elapsed_compute=54.81µs
      └─ AggregateExec: mode=SinglePartitioned, gby=[...], aggr=[...]
         │ output_rows=11.62 K
         │ elapsed_compute=2.61ms
         └─ HashJoinExec: mode=Partitioned, join_type=Inner, on=[(o_orderkey@0, l_orderkey@0)], projection=[...]
            │ output_rows=30.52 K
            │ elapsed_compute=17.20ms
            ├─ [Stage 1] => NetworkShuffleExec: output_partitions=16, input_tasks=1
            │    output_rows=147.1 K
            │    elapsed_compute=1.56ms
            └─ [Stage 2] => NetworkShuffleExec: output_partitions=16, input_tasks=2
                 output_rows=3.24 M
                 elapsed_compute=33.73ms
```

The join keeps 30.52 K of the 3.24 M line items it receives, so most of what crossed the network
is discarded right away. Receiving those line items is also the most expensive node of the stage,
taking 33.73 ms of CPU, more than the join itself.
:::

:::{tab-item} 3. Per task

Every metric so far was added up over all the tasks of a stage. `-v` shows it per task instead,
which is how skew shows up:

```
dfd plan tpch-q3.dump.txt --stage 2 -v -m output_rows --exact
```
```rust
Stage 2 (Shuffle) tasks=2 partitions=32
└─ RepartitionExec: partitioning=Hash([l_orderkey@0], 32), input_partitions=16
   │ output_rows={0:941.5 K, 1:2.30 M}
   └─ FilterExec: l_shipdate@3 > 1995-03-15, projection=[...]
      │ output_rows={0:941.5 K, 1:2.30 M}
      └─ DistributedLeafExec:
         ├─ t0: DataSourceExec: file_groups={16 groups}, [...]
         │    output_rows=1.75 M
         └─ t1: DataSourceExec: file_groups={16 groups}, [...]
              output_rows={1:4.26 M}
```

The two tasks of stage 2 read different files, and task 1 got more than twice the rows of task 0.
The shuffle evens it out for the next stage: with the same flags, `--stage 3` shows both tasks of
stage 3 receiving 1.62 M line items each.
:::

:::{tab-item} 4. Query totals

`-t` adds up each metric over every node of the query, or of one stage with `--stage N`:

```
dfd plan tpch-q3.dump.txt -t -m elapsed_compute -m bytes_scanned --exact
```
```rust
Totals
  elapsed_compute=83.02ms
  bytes_scanned=117.9 M
```

The query used 83 ms of CPU across all workers, and stage 3 accounts for 55 ms of it, as
`-m stage_elapsed_compute --exact` on the tree of stages shows. `elapsed_compute` is the time a
node spends being polled, which includes waiting for a CPU, so only compare it between runs on
the same hosts.
:::

::::

## Inspecting dumps with coding agents

The `plan-dump` agent skill teaches coding agents like Claude Code, Codex or Cursor to work with
plan dumps: how to drive `dfd plan`, what each metric means, and the traps that make a plan easy to
misread, like broadcast stages counting every copy they send, or CPU time growing when a host is
busy. Add it to your project with:

```bash
npx skills add datafusion-contrib/datafusion-distributed --skill plan-dump
```

Then hand the agent one or more dumps, and ask it what you want to know. It can read a dump pasted
into the conversation or saved to a file, and, if it has access to your logs or observability stack,
fetch the dumps of the queries you point it at. For example:

- *"Here is the plan dump of a query that took 40 seconds. Where did the time go?"*
- *"These are two dumps of the same query, before and after enabling adaptive query execution.
  What did it change, and were those changes right?"*
- *"Find the plan dumps of yesterday's slowest queries in our logs, and tell me what they have in
  common."*

When comparing dumps, tell the agent what differs between them, like a setting, a version or the
data, as a dump does not record the settings it ran with.

The agent saves every dump it looks at in a temporary directory, under `<investigation>/`, as one file per
dump with a descriptive name, like `q3-replanning-on.dump.txt`, and a `NOTES.md` saying where each
one came from. Along with its findings, it gives you the `dfd plan` commands behind them, so you can
open the same plans yourself and keep exploring them:

```bash
dfd plan <temp dir>/2026-11-20-q3-regression/q3-replanning-on.dump.txt --stage 4
```
