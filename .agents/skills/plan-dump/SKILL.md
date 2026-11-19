---
name: plan-dump
description: Inspect executed distributed plans with `dfd plan`. Use when asked why a query is slow, where its time or rows go, how two plans of the same query differ (flags on/off, join reordering, branches), or whether a plan change could be returning wrong results.
---

# Inspecting plan dumps with `dfd plan`

A plan dump is one base64 line holding an executed plan with its stages and per-task metrics,
produced by `generate_plan_dump` after `rewrite_distributed_plan_with_metrics`. `dfd plan`
takes a dump as an argument, a file path, or stdin.

## Reading a dump

- **Default view.** With no options, it prints the tree of stages with stage-level metrics.
  `--stage N` shows the nodes of one stage. Stage 0 is the head, run by the coordinator.
- **Per-task metrics.** `-v` shows metrics per task rather than aggregated, which is how skew
  shows up.
- **Filtering metrics.** `-m TEXT` keeps metrics whose name contains `TEXT`, and is repeatable.
  `--exact` matches whole names.
  - `-m output_rows --exact` on the stage tree gives a compact skeleton of the whole plan.
  - `-m none --exact` hides every metric, leaving plain node lines to grep.
- **Query totals.** `-t` prints each metric reported by any node added up across the query
  instead of the plan, like the total CPU time with `-t -m elapsed_compute --exact`. With
  `--stage N` it only adds up the nodes of that stage, and `-m` filters apply. Stage-level
  derived metrics are left out, so `output_rows`/`output_bytes` add up every node's output, not
  what crossed the network.
- **Long lines.** Node lines are long, so cut them when scanning.
- **Stage-level metrics:**
  - `elapsed_compute` is the CPU time of all the stage's nodes.
  - `output_rows` and `output_bytes` are measured where the stage is read: what crossed the
    network.
  - `plan_added_at`, `plan_executed_at` and `plan_finished_at` are offsets from query start.
    They give the timeline and the critical path.
  - Dynamic task count adds `estimated_output_bytes` and `output_bytes_q_error`. These show how
    wrong the sampled statistics were, and adaptive replanning decides from them.
- **Node metrics worth knowing:**
  - On joins: `build_input_rows`, `input_rows` and `probe_hit_rate`.
  - On filters: `selectivity`.

## Comparing two plans of the same query

- **Stage ids don't necessarily match across runs.** They are renumbered whenever the plan's
  shape changes, so match nodes by content: node type plus its keys or expressions (join keys,
  grouping, predicates).
- **Broadcast counts multiply.** Broadcast rows are counted once per consumer task, both in a
  broadcast stage's `output_rows` and in a `CollectLeft` join's `build_input_rows`. A broadcast
  turning into a shuffle drops them by that factor without losing any rows.
- **Logical row counts should match.** A node computing the same thing in both plans (the same
  join, final aggregation or filter) should report the same `output_rows`, whatever its physical
  shape. When it differs, the results differ, unless a `LIMIT`/TopK above it stops execution
  early or dynamic filters prune its input differently.
- **Equal counts don't prove equal results.** Mixed-up columns keep every count intact. Check
  the column indices in projections and sort expressions where the plans diverge.
- **Runs can differ.** Adaptive decisions come from sampled runtime statistics, so runs of the
  same query can produce different plans. Group runs by plan shape before comparing timings.
