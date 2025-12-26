# Getting Started

Rather than being opinionated about your setup and how you serve queries to users,
Distributed DataFusion allows you to plug in your own networking stack and spawn your own gRPC servers that act as
workers in the cluster.

This project heavily relies on the [Tonic](https://github.com/hyperium/tonic) ecosystem for the networking layer.
Users of this library are responsible for building their own Tonic server, adding the Arrow Flight distributed
DataFusion service to it and spawning it on a port so that it can be reached by other workers in the cluster.

For a basic setup, all you need to do is to enrich your DataFusion `SessionStateBuilder` with the tools this project
ships:

```rs
 let state = SessionStateBuilder::new()
+    .with_distributed_worker_resolver(my_custom_worker_resolver)
+    .with_physical_optimizer_rule(Arc::new(DistributedPhysicalOptimizerRule))
     .build();
```

And the `my_custom_worker_resolver` variable should be an implementation of
the [WorkerResolverResolver](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/src/networking/worker_resolver.rs)
trait, which tells Distributed DataFusion how to connect to other workers in the cluster.

A very basic example of such an implementation that resolves workers in the localhost machine is:

```rust
#[derive(Clone)]
struct LocalhostWorkerResolver {
    ports: Vec<u16>,
}

#[async_trait]
impl ChannelResolver for LocalhostChannelResolver {
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
        Ok(self.ports.iter().map(|port| Url::parse(&format!("http://localhost:{port}")).unwrap()).collect())
    }
}
```

> NOTE: This example is not production-ready and is meant to showcase the basic concepts of the library.

This `WorkerResolver` implementation should resolve URLs of Distributed DataFusion Arrow Flight servers, and it's
also the user of this library's responsibility to spawn a Tonic server that exposes the Arrow Flight service.

A basic example of such a server is:

```rust
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let endpoint = ArrowFlightEndpoint::default();

    Server::builder()
        .add_service(endpoint.into_flight_server())
        .serve(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8000))
        .await?;

    Ok(())
}
```

## Next steps

The next two sections of this guide will walk you through tailoring the library's traits to your own needs:

- [Build your own WorkerResolver](worker-resolver.md)
- [Build your own ChannelResolver](channel-resolver.md)
- [Build your own TaskEstimator](task-estimator.md)
- [Build your own distributed DataFusion Arrow Flight endpoint](arrow-flight-endpoint.md)

Here are some other resources in the codebase:

- [In-memory cluster example](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/examples/in_memory.md)
- [Localhost cluster example](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/examples/localhost.md)

A more advanced example can be found in the benchmarks that use a cluster of distributed DataFusion workers
deployed on AWS EC2 machines:

- [AWS EC2 based cluster example](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/benchmarks/cdk/bin/worker.rs)

Each feature in the project is showcased and tested in its own isolated integration test, so it's recommended to
review those for a better understanding of how specific features work:

- [Pass your own ConfigExtension implementations across network boundaries](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/tests/custom_config_extension.rs)
- [Provide custom protobuf codecs for your own nodes](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/tests/custom_extension_codec.rs)
- Provide a custom TaskEstimator for controlling the amount of parallelism (coming soon)

