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
    async fn rewrite_worker_plan(
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

# Task finished handlers

`DistributedExt::with_distributed_task_finished_handler` registers handlers that run once for each
task, after the coordinator closes its channel to the worker. Use them to record how each task
ended, or to report metrics for the task as a whole. Register them on the worker session, the
same way as plan rewrite handlers.

Each handler receives a `TaskFinishedEvent` with the task's `TaskKey`, its plan, and its
`TaskOutcome`:

- `Succeeded`: every partition stream of the task reached its end.
- `Failed(error)`: a partition stream returned an error. The event carries the first one.
- `Incomplete`: whatever reads the task stopped before all of its output was read, and no stream
  returned an error.

```rust
# use datafusion::common::Result;
# use datafusion::execution::SessionState;
# use datafusion::physical_plan::metrics::MetricsSet;
# use datafusion_distributed::{DistributedExt, TaskFinishedEvent, TaskFinishedEventResponse, TaskOutcome, WorkerQueryContext};

async fn build_worker_session(ctx: WorkerQueryContext) -> Result<SessionState> {
    Ok(ctx
        .builder
        .with_distributed_task_finished_handler(|event: TaskFinishedEvent<'_>| {
            if let TaskOutcome::Failed(err) = event.outcome {
                eprintln!("task {:?} failed: {err}", event.task_key);
            }
            Ok(TaskFinishedEventResponse::new(MetricsSet::new()))
        })
        .build())
}
```

Handlers are synchronous and must not wait on I/O. If a handler returns an error, the error is
dropped and the task is not affected. A task whose plan could not be set up on the worker produces
no event.
