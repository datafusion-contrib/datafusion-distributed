# Plan hooks

`Worker::add_on_plan_hook` registers callbacks that run after the worker session
has been built and the physical plan has been decoded, but before the task plan
is registered for execution. It's a hook for **worker-local** rewrites of the
fragment a worker is about to run.

Each hook receives the decoded `ExecutionPlan` and the per-query `SessionConfig`,
and returns the (possibly rewritten) plan:

```rust
worker.add_on_plan_hook(|plan, session_config| {
    let rule = your_physical_optimizer_rule();
    rule.optimize(plan, session_config.options())
});
```

Hooks run in registration order — each hook sees the plan produced by the
previous one.

## What hooks may and may not do

Treat hooks as trusted, worker-local rewrites. Transparent instrumentation
wrappers and semantics-preserving physical optimizer rules are appropriate uses.

The returned plan **must preserve the contract the coordinating context planned
for**: row semantics, output schema, partitioning, and ordering requirements. Do
**not** use a hook to add or remove rows or columns, repartition the stage, or
otherwise re-plan distributed execution. If a hook returns an error, the
distributed query fails when the coordinating context tries to execute that task.

```{note}
A plan hook acts on a single worker's copy of a stage, after distribution has
already been decided. It is **not** a way to change how a query is split across
the cluster — for that, see
[Building Custom Distributed Plans](05-custom-distributed-plans.md).
```
