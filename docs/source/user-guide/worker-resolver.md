# Implementing a WorkerResolver

The `WorkerResolver` trait provides Distributed DataFusion with the locations (URLs) of your worker nodes. This
information is used in two different places:

1. **During planning**: The number of available workers (i.e., `Vec<Url>.len()`) determines how the plan scales.
   The planner will not allocate more tasks per stage than available workers.
2. **Before execution**: Each task in a distributed plan needs its worker URL assignment populated right before
   execution.

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

This is the simplest approach, though it doesn't accommodate dynamic worker discovery. An example of this can be
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

- Since planning is synchronous, `get_urls()` must return immediately. For dynamic worker discovery, spawn
  background tasks that periodically refresh the worker URL list, storing results that `get_urls()` can access
  synchronously.

A good example can be found
in [benchmarks/cdk/bin/worker.rs](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/benchmarks/cdk/bin/worker.rs),
where a cluster of AWS EC2 machines is discovered identified by tags with the AWS Rust SDK.