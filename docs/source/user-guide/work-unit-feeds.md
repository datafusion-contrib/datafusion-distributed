# Work Unit Feeds

A **work unit feed** lets a leaf `ExecutionPlan` be driven by work that is discovered on the
coordinator *at runtime* and streamed to the workers while the query executes, rather than being
fully known at planning time.

## When to use a work unit feed

Distributed DataFusion already has a mechanism for splitting a leaf node's work across tasks: the
[`TaskEstimator`](task-estimator.md). With a `TaskEstimator`, all of the work is decided **at planning
time** — `scale_up_leaf_node` produces `N` per-task plan variants, each pre-loaded with the slice of
data it is responsible for (e.g., a group of files). This is the right tool
whenever the units of work are known before execution begins.

A work unit feed solves the opposite problem: **the units of work are only discovered as the query
runs**. Typical cases:

- A paginated external API or message queue that hands out pages/offsets on demand.
- A catalog or metadata service that returns object-store keys progressively.
- A source where the *amount* of work per partition is not known until you start pulling from it.

With a feed, the leaf node is planned without knowing its work. During execution the coordinator pulls
work-unit descriptors from your provider and streams each one — over gRPC — to whichever worker
owns that partition. The worker turns each descriptor into rows. Slow partitions don't block fast ones,
and back-pressure is handled per partition.

|                         | `TaskEstimator`                             | Work unit feed                                |
|-------------------------|---------------------------------------------|-----------------------------------------------|
| When work is known      | Planning time                               | Runtime                                       |
| How work is distributed | Pre-built per-task plan variants            | Streamed per-partition from the coordinator   |
| Good for                | Files, ranges, anything enumerable up front | Paginated APIs, queues, progressive discovery |

The two mechanisms are complementary. A feed-backed leaf still provides a `TaskEstimator` to tell the
planner how many tasks to use; the feed only governs *what work flows into each partition at runtime*.

> **A `TaskEstimator` is always required for a feed-backed leaf.** The feed decides *what* work each
> partition receives at runtime, but something still has to tell Distributed DataFusion the *desired task
> count* for the node — that is the `TaskEstimator`'s job (`task_estimation`). Without it the leaf defaults
> to a single task. See [Building a TaskEstimator](task-estimator.md).

## How it works

A feed produces one stream of work units per partition. Inside the node's `execute()`, each partition pulls
its work units and turns them into `RecordBatch`es. In a single process that's a direct call into your
provider:

```text
┌──────────────────────────────────────────────────────┐
│                    ExecutionPlan                     │
│                                                      │
│                                                      │
│┌────────────────────────────────────────────────────┐│
││                    WorkUnitFeed                    ││
││ ┌───────────┐     ┌───────────┐     ┌───────────┐  ││
││ │ .feed(0)  │     │ .feed(1)  │     │ .feed(2)  │  ││
││ └────┬──────┘     └──┬────────┘     └──┬────────┘  ││
│└──────┼───────────────┼─────────────────┼───────────┘│  .─.
│┌──────┼─────────┐┌────┼───────────┐┌───.▼.──────────┐│ (   ) WorkUnit
││      │P0       ││   .▼. P1       ││  (   )P2       ││  `─'  (e.g., a file address)
││     .▼.        ││  (   )         ││   `┬'          ││
││    (   )       ││   `┬'          ││    │           ││
││     `─'        ││    │           ││   .▼.          ││
││      │         ││   .▼.          ││  (   )         ││
││      │         ││  (   )         ││   `┬'          ││
││      │         ││   `─'          ││    │           ││
││      ▼         ││    ▼           ││    ▼           ││
││  processing... ││  processing... ││  processing... ││
││      │         ││    │           ││    │           ││
││      │         ││    │           ││    │           ││
│└──────┼─────────┘└────┼───────────┘└────┼───────────┘│
└───────┼───────────────┼─────────────────┼────────────┘
  ┌─────▼─────┐     ┌───▼───────┐      ┌──▼────────┐
  │RecordBatch│     │RecordBatch│      │RecordBatch│
  └───────────┘     └───────────┘      └───────────┘
```

Under distributed execution the picture is the same from the node's point of view, but the provider lives
**only on the coordinator**. Each worker deserializes a *remote* `WorkUnitFeed` whose `.feed(partition)`
pulls the work units the coordinator streams to it over gRPC — so `WorkUnitFeedProvider::feed` itself only
ever runs on the coordinator:

```text
                                                                                                    ┌──────────────────┐
                                                                                                    │Coordinating Stage│
┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┴──────────────────┘
                                                                                                                       │
│
 ┌────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐│
││                                                    WorkUnitFeed                                                    │
 │  ┌───────────┐     ┌───────────┐     ┌───────────┐            ┌───────────┐      ┌───────────┐    ┌───────────┐    ││
││  │ .feed(0)  │     │ .feed(1)  │     │ .feed(2)  │            │ .feed(3)  │      │ .feed(4)  │    │ .feed(5)  │    │
 │  └────┬──────┘     └──┬────────┘     └──┬────────┘            └─────┬─────┘      └──┬────────┘    └───┬───────┘    ││
│└───────┼───────────────┼─────────────────┼───────────────────────────┼───────────────┼─────────────────┼────────────┘
         │               │                 │                           │               │                .┴.            │
└ ─ ─ ─ ─│─ ─ ─ ─ ─ ─ ─ ─│─ ─ ─ ─ ─ ─ ─ ─ ─│─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ .┴. ─ ─ ─ ─ ─ ─ ─│─ ─ ─ ─ ─ ─ ─ ─(   )─ ─ ─ ─ ─ ─
         │              .┴.                │                         (   )             │                `┬'
         │             (   )               │                          `┬'              │                .┴.
        .┴.             `┬'               .┴.                          │               │               (   )
       (   )             │               (   )                         │              .┴.               `┬'
        `┬'             .┴.               `┬'────────────┐             │             (   )               │┌────────────┐
         │             (   )               ││  Worker 1  │             │              `┬'                ││  Worker 2  │
┌ ─ ─ ─ ─│─ ─ ─ ─ ─ ─ ─ `┬' ─ ─ ─ ─ ─ ─ ─ ─│┴────────────┘    ┌ ─ ─ ─ ─│─ ─ ─ ─ ─ ─ ─ ─│─ ─ ─ ─ ─ ─ ─ ─ ─│┴────────────┘
 ┌───────┼───────────────┼─────────────────┼────────────┐│     ┌───────┼───────────────┼─────────────────┼────────────┐│
││       │            ExecutionPlan        │            │     ││       │            ExecutionPlan        │            │
 │       │               │                 │            ││     │       │               │                 │            ││
││┌──────┼───────────────┼─────────────────┼───────────┐│     ││┌──────┼───────────────┼─────────────────┼───────────┐│
 ││      │          RemoteWorkUnitFeed     │           │││     ││      │          RemoteWorkUnitFeed     │           │││
│││ ┌────▼──────┐     ┌──▼────────┐     ┌──▼────────┐  ││     │││ ┌────▼──────┐     ┌──▼────────┐     ┌──▼────────┐  ││
 ││ │ .feed(0)  │     │ .feed(1)  │     │ .feed(2)  │  │││     ││ │ .feed(0)  │     │ .feed(1)  │     │ .feed(2)  │  │││
│││ └────┬──────┘     └──┬────────┘     └──┬────────┘  ││     │││ └────┬──────┘     └──┬────────┘     └──┬────────┘  ││
 │└──────┼───────────────┼─────────────────┼───────────┘││     │└──────┼───────────────┼─────────────────┼───────────┘││
││       │               │                 │            │     ││       │               │                 │            │
 │┌──────┼─────────┐┌────┼───────────┐┌───.▼.──────────┐││     │┌──────┼─────────┐┌────┼───────────┐┌───.▼.──────────┐││
│││      │P0       ││   .▼. P1       ││  (   )P2       ││     │││      │P0       ││   .▼. P1       ││  (   )P2       ││
 ││     .▼.        ││  (   )         ││   `┬'          │││     ││     .▼.        ││  (   )         ││   `┬'          │││
│││    (   )       ││   `┬'          ││    │           ││     │││    (   )       ││   `┬'          ││    │           ││
 ││     `─'        ││    ┼           ││   .▼.          │││     ││     `─'        ││    ┼           ││   .▼.          │││
│││      │         ││   .▼.          ││  (   )         ││     │││      │         ││   .▼.          ││  (   )         ││
 ││      │         ││  (   )         ││   `┬'          │││     ││      │         ││  (   )         ││   `┬'          │││
│││      │         ││   `─'          ││    │           ││     │││      │         ││   `─'          ││    │           ││
 ││      ▼         ││                ││    ▼           │││     ││      ▼         ││                ││    ▼           │││
│││  processing... ││  processing... ││  processing... ││     │││  processing... ││  processing... ││  processing... ││
 ││      │         ││    │           ││    │           │││     ││      │         ││    │           ││    │           │││
│││      │         ││    │           ││    │           ││     │││      │         ││    │           ││    │           ││
 │└──────┼─────────┘└────┼───────────┘└────┼───────────┘││     │└──────┼─────────┘└────┼───────────┘└────┼───────────┘││
│└───────┼───────────────┼─────────────────┼────────────┘     │└───────┼───────────────┼─────────────────┼────────────┘
   ┌─────▼─────┐     ┌───▼───────┐      ┌──▼────────┐    │       ┌─────▼─────┐     ┌───▼───────┐      ┌──▼────────┐    │
│  │RecordBatch│     │RecordBatch│      │RecordBatch│         │  │RecordBatch│     │RecordBatch│      │RecordBatch│
   └───────────┘     └───────────┘      └───────────┘    │       └───────────┘     └───────────┘      └───────────┘    │
└ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─     └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
```

## The public API

The feed API lives behind a handful of types exported from the crate root:

```rust
use datafusion_distributed::{
    WorkUnitFeed, WorkUnitFeedProvider, WorkUnitFeedProto, DistributedExt,
};
```

### `WorkUnitFeedProvider`

You implement this trait on a type that produces work units, one stream per partition. It is only
ever called **on the coordinator**.

```rust
pub trait WorkUnitFeedProvider: Send + Sync + Debug {
    /// The work unit type. Any `prost` message works out of the box.
    type WorkUnit: WorkUnit + Default;

    /// Produce the stream of work units for `partition`. Called once per partition,
    /// on the coordinator only.
    fn feed(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<BoxStream<'static, Result<Self::WorkUnit>>>;

    /// Optional metrics surfaced on the coordinator while feeding.
    fn metrics(&self) -> ExecutionPlanMetricsSet { ExecutionPlanMetricsSet::new() }
}
```

The associated `WorkUnit` type is the descriptor that travels over the network. Any
[`prost`](https://docs.rs/prost) message automatically satisfies the `WorkUnit` trait — you don't need
to implement anything extra, just `#[derive(::prost::Message)]`.

### `WorkUnitFeed<P>`

A `WorkUnitFeed<P>` is the handle your custom `ExecutionPlan` stores. On the coordinator it wraps your
provider; on a worker it is reconstructed as a *remote* handle that pulls work units from the network.

```rust
// On the coordinator, while planning:
let feed = WorkUnitFeed::new(my_provider);

// Inside ExecutionPlan::execute(), on either side:
let mut stream = feed.feed(partition, ctx)?; // yields your WorkUnit type

// In your codec:
let proto: WorkUnitFeedProto = feed.to_proto();           // encode (just a handle/UUID)
let feed = WorkUnitFeed::<P>::from_proto(proto)?;          // decode into a remote handle
```

`to_proto()` serializes only the feed *handle* (a UUID), never the provider itself. That is what makes
runtime delivery work: the provider stays on the coordinator, and each worker receives a remote handle
that the runtime connects to the matching coordinator-side stream.

### Registering the feed

Finally, you tell Distributed DataFusion how to locate the feed inside your custom node, so the planner
can discover it while walking the plan and wire up coordinator → worker delivery:

```rust
let state = SessionStateBuilder::new()
    .with_default_features()
    .with_distributed_worker_resolver(/* ... */)
    .with_distributed_planner()
    .with_distributed_user_codec(MyExecCodec)        // so workers can deserialize the node
    .with_distributed_task_estimator(MyTaskEstimator) // how many tasks the leaf gets
    .with_distributed_work_unit_feed(|exec: &MyExec| Some(&exec.feed))
    .build();
```

`with_distributed_work_unit_feed` (and its in-place sibling `set_distributed_work_unit_feed`) take a
closure `Fn(&YourExec) -> Option<&WorkUnitFeed<P>>`. The planner calls it for every node in the plan;
return `Some(&feed)` for the nodes that own a feed and `None` otherwise.

## Putting it together

A feed-backed leaf node therefore wires up four collaborating pieces, exactly like any other custom
distributed node, plus the feed registration:

1. A `WorkUnitFeedProvider` that produces the per-partition work-unit streams on the coordinator.
2. A custom `ExecutionPlan` that holds a `WorkUnitFeed<P>` and, in `execute()`, consumes
   `feed.feed(partition, ctx)?` to produce `RecordBatch`es.
3. A `PhysicalExtensionCodec` that serializes the node, encoding the feed handle with `to_proto()` /
   `from_proto()`.
4. A `TaskEstimator` so the planner knows how many tasks the leaf stage should use.

There is a complete, runnable example in the `examples/` folder:

- [work_unit_feed.rs](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/examples/work_unit_feed.rs) —
  a `scan(...)` table function backed by a simulated external source that discovers chunks of work at
  runtime and streams them to the workers.

The integration tests are also a good reference for the range of plan shapes feeds support (unions,
joins, aggregations, broadcast joins, error and back-pressure behaviour):

- [tests/work_unit_feed.rs](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/tests/work_unit_feed.rs)
