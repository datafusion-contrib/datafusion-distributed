# Building a WorkerResolver

A `WorkerResolver` tells distributed DataFusion the location (URLs) of your worker nodes. This information is
used in two different places:

1. For planning: during distributed planning, the amount of worker nodes available (essentially, `Vec<Url>`.length()),
   is used for determined how to scale up the plan. The distributed planner will not use more tasks per stage than the
   amount of workers the cluster has.
2. Right before execution: each task in a distributed plan contains slots that the `DistributedExec` node needs
   to fill right before execution (with the URLs of the workers as updated as possible).

You need to pass your own `WorkerResolver` to DataFusion's `SessionStateBuilder` so that it's available in the
`SesionContext`:

```rust
struct CustomWorkerResolver;

#[async_trait]
impl WorkerResolver for CustomWorkerResolver {
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
        todo!()
    }
}

async fn main() {
    let state = SessionStateBuilder::new()
        .with_distributed_worker_resolver(CustomWorkerResolver)
        .with_physical_optimizer_rule(Arc::new(DistributedPhysicalOptimizerRule))
        .build();
}
```

> NOTE: It's not necessary to pass a WorkerResolver to the Worker session builder, it's just necessary on the
> SessionState that initiates and plans the query.

## Static WorkerResolver

This is the simplest thing you can do, although it will not fit some common use cases. An example of this can be
seen in the
[localhost_worker.rs](https://github.com/datafusion-contrib/datafusion-distributed/blob/fad9fa222d65b7d2ddae92fbc20082b5c434e4ff/examples/localhost_run.rs)
example:

```rust
#[derive(Clone)]
struct LocalhostChannelResolver {
    ports: Vec<u16>,
}

#[async_trait]
impl WorkerResolver for LocalhostChannelResolver {
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
        Ok(self
            .ports
            .iter()
            .map(|port| Url::parse(&format!("http://localhost:{port}")).unwrap())
            .collect())
    }
}
```

## Dynamic WorkerResolver

In a typical setup, you might have different pods running in a Kubernetes cluster, or you might be using a cloud
provider for hosting your Distributed DataFusion workers.

It's up to you to decide how the URLs should be resolved. One important implementation note is:

- Retrieving the worker URLs needs to be synchronous (as planning is synchronous), and therefore, you'll likely
  need to spawn background tasks that periodically refresh the list of available URLs.

A good example can be found
in [benchmarks/cdk/bin/worker.rs](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/benchmarks/cdk/bin/worker.rs),
where a cluster of AWS EC2 machines is discovered identified by tags with the AWS Rust SDK.