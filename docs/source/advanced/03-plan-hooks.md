# Worker plan rewrite handlers

`DistributedExt::with_distributed_worker_plan_rewrite_handler` registers handlers that run after
the worker session has been built and the physical plan has been decoded, but before the task plan
is registered for execution. It is intended for **worker-local** rewrites of the fragment a worker
is about to run. Handlers are `async`, so they can perform I/O before the plan is handed off for
execution.

Register handlers in the `SessionStateBuilder` used by each worker's `WorkerSessionBuilder`.
Registering one on the coordinator's session has no effect: handlers are not sent to workers with
stage plans.

Each handler receives a `WorkerPlanRewriteEvent` and returns the possibly rewritten plan:

```rust
# use async_trait::async_trait;
# use datafusion::common::Result;
# use datafusion::execution::SessionState;
# use datafusion_distributed::{DistributedExt, Worker, WorkerPlanRewriteEvent, WorkerPlanRewriteEventResponse, WorkerPlanRewriteHandler, WorkerQueryContext};

struct Passthrough;

#[async_trait]
impl WorkerPlanRewriteHandler for Passthrough {
    async fn handle(
        &self,
        event: WorkerPlanRewriteEvent<'_>,
    ) -> Result<WorkerPlanRewriteEventResponse> {
        Ok(WorkerPlanRewriteEventResponse::new(event.plan))
    }
}

async fn build_worker_session(ctx: WorkerQueryContext) -> Result<SessionState> {
    Ok(ctx
        .builder
        .with_distributed_worker_plan_rewrite_handler(Passthrough)
        .build())
}

let _worker = Worker::from_session_builder(build_worker_session);
```

Handlers run in registration order—each handler sees the plan produced by the
previous one.

## What handlers may and may not do

Treat handlers as trusted, worker-local rewrites. Instrumentation and semantics-preserving
physical optimizer rules that retain the stage topology are appropriate uses. A handler must:

- Keep the same plan topology; do not add or remove nodes or edges.
- Produce the same rows, including multiplicity, from every output partition.
- Preserve the head node's output schema, partitioning, ordering, boundedness, and emission type.

Intermediate nodes may change their output schema. If a handler returns an error, the distributed
query fails when the coordinating context tries to execute that task.

```{note}
A worker plan rewrite handler acts on a single worker's copy of a stage, after distribution has
already been decided. It is **not** a way to change how a query is split across
the cluster — for that, see
[Building Custom Distributed Plans](05-custom-distributed-plans.md).
```

## Wrapping a stage root

Stage-result caching, tracing, progress reporting, runtime context, and resource
checks can apply to an entire producer stage. An explicit wrapper at the stage root
can cover the producer that distributes batches to downstream tasks.

Implement `StageRootWrapper`: `wrap` adds your node, and `matches` identifies an
existing instance of it, typically with `plan.is::<MyRootExec>()`. DFD needs this
recognition to avoid wrapping twice, reset task-local wrapper state, and rebuild the
producer beneath your wrapper while keeping the wrapper on top.

Register the same implementation with
`DistributedExt::with_distributed_stage_root_wrapper` on the coordinator session and
every worker session. The implementation stays local to each session; its node uses
the normal physical-plan codec.

For example, application session builders can share this configuration:

```rust
# use datafusion::execution::SessionStateBuilder;
# use datafusion_distributed::{DistributedExt, StageRootWrapper};
fn with_stage_wrapper(
    builder: SessionStateBuilder,
    wrapper: impl StageRootWrapper,
) -> SessionStateBuilder {
    builder.with_distributed_stage_root_wrapper(wrapper)
}
```

During execution preparation, the coordinator assembles each stage's producer before
ensuring its outer wrapper, including plans built directly with `DistributedExec::new`.
Wrappers within the original input stay in place, while an existing root can be reused
when no additional producer is needed. Producer replacement and sampler insertion
then operate below the outer wrapper:

```text
StageRootExec
  RepartitionExec or BroadcastExec
    SamplerExec, when sampling is enabled
      Input
```

The node's `children()` must expose the supplied plan as its sole child. Successful
execution produces the same rows and preserves the child's schema, partitioning,
ordering, boundedness, and emission type.

At worker task setup, DFD calls the root wrapper's `reset_state()` once after decoding,
before rewrite handlers and sampling. Stateful wrappers implement this DataFusion
method to create fresh task-local metrics or budgets while retaining their settings
and the same child. Later producer updates use `replace_children`, which retains that
task's state and the supplied child subtree. This preserves initialized sampler state.
Only the stage-root node is rebuilt. When no producer replacement is required, the
wrapped plan stays intact.

Keep the wrapper present on both the coordinator and workers so their metric layouts
agree. Configure its runtime behavior through the local session. Worker plan rewrite
handlers retain their existing position before sampling begins.

For dynamic shuffle and broadcast stages, the initial producer is in place before
wrapping. Sampling starts at the sampler's input and uses the behavior attached to
that subtree.
