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
tracing::info!(plan_dump = %generate_plan_dump(&plan)?, "query finished");
```

Log the dump as a field next to the rest of the query's telemetry, like its id and latency, so you
can find the dump of a slow query afterward.

## How much a dump costs

A dump is a protobuf message compressed with zstd, so it takes far less space than the rendered
plan while holding more: every metric of every task. Generating it also takes less time than
rendering the plan with the same per-task metrics:
- up to 3x times faster to generate
- up to 20x less size

## Inspecting a dump

`dfd` is the CLI of DataFusion Distributed:

```bash
cargo install datafusion-distributed-cli
```

`dfd plan` takes a dump as an argument, as the path of a file holding it, or through stdin. With no
other options, it shows the tree of stages with the metrics of each stage, and `--stage N` shows the
nodes of one stage with theirs:

```
$ dfd plan q3.dump.txt -m stage_output_rows -m stage_elapsed_compute
DistributedExec
│ stage_output_rows=114.0 K
│ stage_elapsed_compute=45.93ms
└─ Stage 5 (Coalesce) tasks=8 partitions=16
   │ stage_output_rows=114.0 K
   │ stage_elapsed_compute=958.29ms
   ├─ Stage 4 (Shuffle) tasks=8 partitions=8
   │  │ stage_output_rows=1.46 M
   │  │ stage_elapsed_compute=316.94ms
   │  ├─ Stage 1 (Broadcast) tasks=1 partitions=128
   │  │    stage_output_rows=2.40 M
   │  │    stage_elapsed_compute=3.95ms
   │  └─ Stage 2 (Shuffle) tasks=5 partitions=640
   │       stage_output_rows=7.29 M
   │       stage_elapsed_compute=121.84ms
   └─ Stage 3 (Shuffle) tasks=8 partitions=8
        stage_output_rows=32.33 M
        stage_elapsed_compute=539.34ms
```

`dfd plan --help` lists the rest of its options, like showing metrics per task or adding them up
into totals for the whole query.

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
