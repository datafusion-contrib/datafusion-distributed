---
name: plan-dump
description: Inspect executed distributed plans with `dfd plan`. Use when asked why a query is slow, where its time or rows go, how two plans of the same query differ (flags on/off, join reordering, branches), or whether a plan change could be returning wrong results.
---

# Inspecting plan dumps with `dfd plan`

A plan dump is one base64 line holding an executed plan with its stages and per-task metrics,
produced by `generate_plan_dump` after `rewrite_distributed_plan_with_metrics`. `dfd plan`
takes a dump as an argument, a file path, or stdin.

`dfd` is the CLI of DataFusion Distributed. Install it with `cargo install datafusion-distributed-cli`,
or, inside its repository, run it with `cargo run -p datafusion-distributed-cli -- plan`.

## Storing dumps

Dumps come from benchmark output, log files, or a remote observability stack. Save each one
locally before inspecting it, so that both you and the user can go back to it. They are temporary:
store them wherever your instructions put temporary files, like the session's scratchpad directory,
and always tell the user the full path:

- **One dump per file**, as `<temp dir>/<investigation>/<name>.dump.txt`, where
  `<investigation>` is a dated, descriptive directory like `2026-11-20-q10-regression`.
- **The file holds only the base64 line.** Strip log prefixes, JSON quoting and escapes. When
  output has a dump per line, like a benchmark run with several iterations, split it into one
  file per dump right after the run. `dfbench --debug -i N` prints N dumps per query on stdout, in
  query order, which `split -l 1` or `awk` can name as `q<query>-<variant>-iter<N>.dump.txt`.
- **Name files after what produced them**: the query, what differs from the other dumps, and an
  attempt number when the same thing ran more than once, like `q10.dump.txt`,
  `q10-replanning-off.dump.txt` or `q10-replanning-on-attempt-2.dump.txt`.
- **Record where each dump came from** in a `NOTES.md` next to them, written when you save them
  rather than at the end: the command that produced it, or the log query, host and timestamp it
  was found with, plus the settings that differ. A dump does not record its settings, so this is
  the only way to tell dumps apart later.
- **Keep them out of repositories.** Plans carry query literals and file paths.

Point the user at the files, and give them the `dfd plan` commands behind each finding, like
`dfd plan <temp dir>/2026-11-20-q10-regression/q10-replanning-on.dump.txt --stage 6`, so they
can check it and keep exploring the plan themselves.

## Reading a dump

- **Default view.** With no options, it prints the tree of stages with stage-level metrics.
  `--stage N` shows the nodes of one stage. Stage 0 is the head, run by the coordinator. To
  find nodes across the whole plan, run `--stage N` for every stage id in the tree.
- **Per-task metrics.** `-v` shows metrics per task rather than aggregated, which is how skew
  shows up.
- **Filtering metrics.** `-m TEXT` keeps metrics whose name contains `TEXT`, and is repeatable.
  `--exact` matches whole names.
  - `-m stage_output_rows --exact` on the stage tree gives a compact skeleton of the whole plan.
  - `-m none --exact` hides every metric, leaving plain node lines to grep.
- **Query totals.** `-t` adds up each metric over every node of the query, or of one stage with
  `--stage N`, like the total CPU time with `-t -m elapsed_compute --exact`. It leaves out the
  `stage_*` metrics, so its `output_rows` and `output_bytes` add up every node's output, not what
  crossed the network.
- **`elapsed_compute` includes waiting for a CPU.** It is the wall time a node spends being
  polled, so a busy host inflates it like latency. Compare totals only between runs from the
  same host and period. Shares within one dump hold up better.
- **Stage-level metrics.** The ones derived from the stage's nodes are named `stage_*`:
  - `stage_elapsed_compute` adds up the stage's nodes.
  - `stage_output_rows` and `stage_output_bytes` are measured where the stage is read: what
    crossed the network.
  - `plan_added_at`, `plan_executed_at` and `plan_finished_at` are offsets from query start of
    when the stage was sent, first pulled by its reader, and done. Its work can start later than
    `plan_executed_at`, and streams tend to finish together, so they rarely show where time went.
  - Dynamic task count adds `estimated_output_bytes` and `stage_output_bytes_q_error`: the sampled
    estimate of what the stage returns, and how wrong it was. Adaptive decisions about the stage
    reading it, like broadcasting a join's build side, come from it.
- **Joins.** The build side is the first child. `build_mem_used` is what the hash table takes
  in each task, and can be several times the bytes that crossed the network. Also useful:
  `build_input_rows`, `input_rows`, `probe_hit_rate`, and `selectivity` on filters.
- **`RepartitionExec` over-reports `output_bytes`,** as its batches can be slices of bigger
  ones.

## Comparing two plans of the same query

- **Stage ids don't necessarily match across runs.** They are renumbered whenever the plan's
  shape changes, so match nodes by content: node type plus its keys or expressions (join keys,
  grouping, predicates). Nested loop and cross joins have no keys, so match them by `filter=`
  and input sizes.
- **Work can move between stages.** Identical nodes cost more when stages merge or run with
  other task counts. Compare the number of stages, and CPU per operator type across all stages,
  before diffing nodes.
- **Broadcast counts multiply.** Whatever a broadcast sends is counted once per consumer task:
  the stage's `stage_output_rows` and `stage_output_bytes`, and the receiving join's
  `build_input_rows` (and `input_batches` for nested loop joins). A broadcast turning into a
  shuffle drops them by that factor without losing any rows.
- **Logical row counts should match.** A node computing the same thing in both plans (the same
  join, final aggregation or filter) should report the same `output_rows`, whatever its physical
  shape. When it differs, the results differ, unless a `LIMIT`/TopK above it stops execution
  early or dynamic filters prune its input differently.
- **Equal counts don't prove equal results.** Mixed-up columns keep every count intact. Check
  the column indices in projections and sort expressions where the plans diverge.
- **Runs can differ.** Adaptive decisions come from sampled runtime statistics, so runs of the
  same query can produce different plans. Group runs by plan shape before comparing timings.
