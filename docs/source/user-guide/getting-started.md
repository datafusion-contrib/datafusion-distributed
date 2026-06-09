# Getting Started

Think of this library as vanilla DataFusion, except that certain nodes execute their children on remote
machines and retrieve data via the Arrow Flight protocol.

This library aims to provide an experience as close as possible to vanilla DataFusion.

## How to use Distributed DataFusion

Rather than imposing constraints on your infrastructure or query serving patterns, Distributed DataFusion
allows you to plug in your own networking stack and spawn your own gRPC servers that act as workers in the cluster.

This project heavily relies on the [Tonic](https://github.com/hyperium/tonic) ecosystem for the networking layer.
Users of this library are responsible for building their own Tonic server, adding the distributed
DataFusion service to it and spawning it on a port so that it can be reached by other workers in the cluster. A very
basic example of this would be:

```rust
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let worker = Worker::default();

    Server::builder()
        .add_service(worker.into_worker_server())
        .serve(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8000))
        .await?;

    Ok(())
}
```

Distributed DataFusion requires knowledge of worker locations. Implement the `WorkerResolver` trait to provide
this information. Here is a simple example of what this would look like with localhost workers:

```rust
#[derive(Clone)]
struct LocalhostWorkerResolver {
    ports: Vec<u16>,
}

impl WorkerResolver for LocalhostWorkerResolver {
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
        Ok(self
            .ports
            .iter()
            .map(|port| Url::parse(&format!("http://localhost:{port}")).unwrap())
            .collect())
    }
}
```

Register the `WorkerResolver` implementation and the distributed planner in DataFusion's
`SessionStateBuilder` to enable distributed query planning:

```rs
let localhost_worker_resolver = LocalhostWorkerResolver {
    ports: vec![8000, 8001, 8002]
}

let state = SessionStateBuilder::new()
    .with_default_features()
    .with_distributed_worker_resolver(localhost_worker_resolver)
    .with_distributed_planner()
    .build();

let ctx = SessionContext::from(state);
```

This will leave a DataFusion `SessionContext` ready for executing distributed queries.

> NOTE: `with_distributed_planner()` wraps whatever query planner is already set, so call it **after** any
> `with_query_planner(..)` and after registering your own physical optimizer rules — the distributed planner
> runs last, on top of the fully-built physical plan.

> NOTE: This example is not production-ready and is meant to showcase the basic concepts of the library.

## Troubleshooting

Common errors when first bringing up a cluster, and what they usually mean:

| Symptom                                                                                   | Likely cause                                                                                                                                        |
|-------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| `WorkerResolver not present in the session config`                                        | Forgot `with_distributed_worker_resolver(..)` on the coordinator.                                                                                   |
| A decode/deserialization error for one of your nodes at execution time                    | A custom `ExecutionPlan`'s `PhysicalExtensionCodec` isn't registered on **both** the coordinator and the workers (see [Spawn a Worker](worker.md)). |
| `Physical input schema should be the same as the one converted from logical input schema` | A custom `TableProvider`/leaf ignores the `projection` passed to `scan()` — honor it (return only the projected columns).                           |
| `Missing WorkUnit feed for id ...; Was the WorkUnitFeed registered?`                      | Forgot `with_distributed_work_unit_feed(..)`, or registered it on the wrong session (see [Work Unit Feeds](work-unit-feeds.md)).                    |
| The query runs but isn't distributed (no `DistributedExec` in the plan)                   | `get_urls()` returned no workers, or a custom leaf has no [`TaskEstimator`](task-estimator.md) (custom leaves default to a single task).            |

## Next steps

Depending on your needs, your setup can get more complicated, for example:

- You may want to resolve worker URLs dynamically using the Kubernetes API.
- You may want to wrap the Worker clients that connect workers with an observability layer.
- You may want to be able to execute your own custom ExecutionPlans in a distributed manner.
- etc...

To learn how to do all that, it's recommended to:

- [Continue reading this guide](worker.md)
- Learn how to distribute your own execution plans with a [TaskEstimator](task-estimator.md), or stream
  runtime-discovered work to them with [Work Unit Feeds](work-unit-feeds.md)
- [Build custom distributed plans](custom-distributed-plans.md) by injecting network boundaries yourself
- [Collect runtime metrics](metrics.md) (EXPLAIN ANALYZE) for distributed queries
- [Look at examples in the project](https://github.com/datafusion-contrib/datafusion-distributed/tree/main/examples)
- [Look at the integration tests for finer grained examples](https://github.com/datafusion-contrib/datafusion-distributed/tree/main/tests)

