# Distributing custom ExecutionPlans

File-backed `DataSourceExec` (Parquet, CSV, ...) is distributed out of the box.
But sometimes your data lives somewhere DataFusion can't natively scan — an
external service, a sharded database, a set of partitioned streams — so you write
your own leaf `ExecutionPlan` to read it. This page shows how to make such a leaf
**distributable**, so its work is spread across the cluster instead of running on
a single node.

## The running example

Suppose your `users` table isn't a set of files but rows stored across several
independent shards — `users-00`, `users-01`, `users-02`, ... — each reachable on
its own. You've written a `ShardedScanExec` leaf that reads a set of shards and
emits their rows:

```rust
#[derive(Debug)]
struct ShardedScanExec {
    shards: Vec<String>, // the shards this node reads
    schema: SchemaRef,
    properties: PlanProperties,
    // ...its ExecutionPlan impl (execute(), etc.) is omitted here...
}
```

On a single node, one `ShardedScanExec` reads every shard sequentially.
Distributed, we want to hand each worker a **subset** of the shards and read them
in parallel. Getting there takes three things:

1. [**A codec**](#1-make-it-serializable-across-the-network), so the node can
   cross the network.
2. [**A task-count estimate**](#2-choose-how-many-tasks-to-use), so the planner
   knows how many tasks to run it on.
3. [**A way to split its work**](#3-split-the-work-across-tasks) across those
   tasks.

## 1. Make it serializable across the network

When a stage runs on a worker, the coordinating context serializes that stage's plan,
ships it over gRPC, and the worker deserializes it. DataFusion's built-in nodes
already know how to do this; your node needs a `PhysicalExtensionCodec`:

```rust
use datafusion_proto::physical_plan::PhysicalExtensionCodec;

#[derive(Debug)]
struct ShardedScanCodec;

impl PhysicalExtensionCodec for ShardedScanCodec {
    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        let Some(scan) = node.downcast_ref::<ShardedScanExec>() else {
            return internal_err!("expected ShardedScanExec, got {}", node.name());
        };
        // ...serialize scan.shards and the schema into `buf` (e.g. with prost)...
        Ok(())
    }

    fn try_decode(
        &self,
        buf: &[u8],
        _inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // ...rebuild a ShardedScanExec (shard list + schema) from `buf`...
    }
}
```

The node is encoded on the coordinating context and decoded on the worker, so the
codec must be registered on **both** sides.

On the coordinating context, add it to the session builder:

```rust
let state = SessionStateBuilder::new()
    .with_default_features()
    .with_distributed_worker_resolver(resolver)
    .with_distributed_planner()
+   .with_distributed_user_codec(ShardedScanCodec)
    .build();
```

On each worker, register it in the session builder passed to
`Worker::from_session_builder`:

```rust
let worker = Worker::from_session_builder(|ctx: WorkerQueryContext| async move {
    Ok(ctx
        .builder
+       .with_distributed_user_codec(ShardedScanCodec)
        .build())
});
```

```{note}
Registering the codec on the coordinating context but forgetting the worker (or vice
versa) surfaces as a decode/deserialization error at execution time, since the
node is encoded on one side and decoded on the other.
```

## 2. Choose how many tasks to use

Register a `DesiredTaskCountHandler` with
`.with_distributed_desired_task_count_handler(...)`. It tells the planner how
many tasks the stage containing your leaf should run on.
For a sharded scan, one task per shard is a natural choice:

```rust
use datafusion::common::Result;

fn sharded_scan_desired_task_count(
    event: DesiredTaskCountEvent,
) -> Option<Result<DesiredTaskCountEventResponse>> {
    // Only handle our own node; returning None lets other handlers try.
    let scan = event.plan.downcast_ref::<ShardedScanExec>()?;
    // One task per shard — the planner caps this at the number of workers.
    Some(Ok(DesiredTaskCountEventResponse::desired(scan.shards.len())))
}
```

What the return value means:

- `DesiredTaskCountEventResponse::desired(n)` — a **soft** hint. The planner may land on a
  different number: within a stage the largest `desired` wins, and the count is
  capped at the number of available workers.
- `DesiredTaskCountEventResponse::maximum(n)` — a **hard** cap. `maximum(1)` means "this node
  cannot be distributed."
- `None` — defer to the other registered handlers (and finally the built-in
  file-scan handler).
- `Some(Err(...))` — stop planning and return the error to the caller.

To send each task to a specific worker instead of the default round-robin, see
[Routing tasks to workers](../advanced/06-worker-routing.md).

## 3. Split the work across tasks

Once the final task count is settled, the planner calls the registered
`ScaleUpLeafNodeHandler`. This is where you divide the leaf's work into
`event.task_count` **non-overlapping** pieces — here, by handing each task its
own subset of shards.

The recommended approach is to return a `DistributedLeafExec` wrapping one
**variant** of your node per task:

```rust
fn sharded_scan_scale_up_leaf_node(
    event: ScaleUpLeafNodeEvent,
) -> Option<Result<ScaleUpLeafNodeEventResponse>> {
    let scan = event.plan.downcast_ref::<ShardedScanExec>()?;

    // Spread the shards across `event.task_count` tasks, one variant each.
    let mut per_task: Vec<Vec<String>> = vec![Vec::new(); event.task_count];
    for (i, shard) in scan.shards.iter().enumerate() {
        per_task[i % event.task_count].push(shard.clone());
    }
    let variants = per_task.into_iter().map(|shards| {
        Arc::new(ShardedScanExec::new(shards, scan.schema())) as Arc<dyn ExecutionPlan>
    });

    let leaf = DistributedLeafExec::try_new(Arc::clone(event.plan), variants)
        .map(|leaf| ScaleUpLeafNodeEventResponse::new(Arc::new(leaf)));
    Some(leaf)
}
```

`DistributedLeafExec` holds the original node plus the per-task variants. Every
variant must expose the **same schema and partition count as every other variant**
(`try_new` enforces this) — that is what keeps the node transparent to the network
boundaries above it. Note that variants do **not** need to match the partition
count of the original leaf `ExecutionPlan` or `target_partitions`; variants can
scale their partition count higher or lower as needed for worker execution, as long
as all variants agree on the same partition count.

Trimming each variant to contain only its assigned slice of work ensures that each
worker task emits only its assigned data. If instead you pass untrimmed copies of the
original plan to all variants, every task will re-process all data and produce duplicate
rows.

At execution time only the variant for task `i` is serialized and shipped to
that task's worker:

```text
                    DistributedLeafExec
     ┌──────────────┬──────────────┬──────────────┐
     │  shards      │  shards      │  shards      │
     │  00, 03      │  01, 04      │  02, 05      │
     └──────┬───────┴──────┬───────┴──────┬───────┘
            │              │              │
         Worker 0       Worker 1       Worker 2
```

```{note}
If your node dispatches internally — reading
`DistributedTaskContext::from_ctx(&ctx).task_index` inside `execute()` to decide
what to produce — you can skip `DistributedLeafExec` and return a single prepared
plan directly. The runnable `numbers(start, end)` example linked below takes this
route; the choice is yours.
```

## Putting it together

On the coordinating context, the three pieces sit side by side on the builder:

```rust
let state = SessionStateBuilder::new()
    .with_default_features()
    .with_distributed_worker_resolver(resolver)
    .with_distributed_planner()
    .with_distributed_user_codec(ShardedScanCodec)
    .with_distributed_desired_task_count_handler(sharded_scan_desired_task_count)
    .with_distributed_scale_up_leaf_node_handler(sharded_scan_scale_up_leaf_node)
    .build();
```

Each worker only needs the codec — the planner, worker resolver, and task
event handlers are coordinating-context concerns; workers just decode and run
the plan variants they receive:

```rust
let worker = Worker::from_session_builder(|ctx: WorkerQueryContext| async move {
    Ok(ctx
        .builder
        .with_distributed_user_codec(ShardedScanCodec)
        .build())
});
```

For a complete, runnable program that follows this same pattern — a custom leaf
split across tasks, with its own codec and event handlers — see
[`custom_execution_plan.rs`](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/examples/custom_execution_plan.rs),
which distributes a `numbers(start, end)` source.

From here:

- [Routing tasks to workers](../advanced/06-worker-routing.md) — send each task to a specific
  worker URL instead of round-robin
- [Work Unit Feeds](../advanced/04-work-unit-feeds.md) — when a leaf's units of work are only
  discovered at runtime
- [Building Custom Distributed Plans](../advanced/05-custom-distributed-plans.md) — placing
  network boundaries yourself
