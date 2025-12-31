# Getting Started

The easiest way to think about this library is like vanilla DataFusion, with the exception that some nodes
happen to execute their children in remote machines getting data back through the Arrow Flight protocol.

This library aims to provide an experience as close as possible to vanilla DataFusion.

## How to use Distributed DataFusion

Rather than being opinionated about your setup and how you serve queries to users,
Distributed DataFusion allows you to plug in your own networking stack and spawn your own gRPC servers that act as
workers in the cluster.

This project heavily relies on the [Tonic](https://github.com/hyperium/tonic) ecosystem for the networking layer.
Users of this library are responsible for building their own Tonic server, adding the Arrow Flight distributed
DataFusion service to it and spawning it on a port so that it can be reached by other workers in the cluster. A very
basic example of this would be:

```rust
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let worker = Worker::default();

    Server::builder()
        .add_service(worker.into_flight_server())
        .serve(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8000))
        .await?;

    Ok(())
}
```

Distributed DataFusion needs to know how to reach other workers, so users are expected to implement the `WorkerResolver`
trait for this. Here is a simple example of what this would look like with localhost workers:

```rust
#[derive(Clone)]
struct LocalhostWorkerResolver {
    ports: Vec<u16>,
}

impl ChannelResolver for LocalhostWorkerResolver {
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
        Ok(self
            .ports
            .iter()
            .map(|port| Url::parse(&format!("http://localhost:{port}")).unwrap())
            .collect())
    }
}
```

The `WorkerResolver` implementation, along with the `DistributedPhysicalOptimizerRule`, needs to be provided in
DataFusion's `SessionStateBuilder` so that it's available during the planning step of distributed queries:

```rs
let localhost_worker_resolver = LocalhostWorkerResolver {
    ports: vec![8000, 8001, 8002]
}

let state = SessionStateBuilder::new()
    .with_distributed_worker_resolver(localhost_worker_resolver)
    .with_physical_optimizer_rule(Arc::new(DistributedPhysicalOptimizerRule))
    .build();

let ctx = SessionContext::from(state);
```

This will leave a DataFusion `SessionContext` ready for executing distributed queries.

> NOTE: This example is not production-ready and is meant to showcase the basic concepts of the library.

## Next steps

Depending on your needs, your setup can get more complicated, for example:

- You may want to resolve worker URLs dynamically using the Kubernetes API.
- You may want to wrap the Arrow Flight clients that connect workers with an observability layer.
- You may want to be able to execute your own custom ExecutionPlans in a distributed manner.
- etc...

To learn how to do all that, it's recommended to:

- [Continue reading this guide](worker.md)
- [Look at examples in the project](https://github.com/datafusion-contrib/datafusion-distributed/tree/main/examples)
- [Look at the integration tests for finer grained examples](https://github.com/datafusion-contrib/datafusion-distributed/tree/main/tests)

