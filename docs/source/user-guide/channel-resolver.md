# Building a ChannelResolver

This trait is optional—a sensible default implementation exists that handles most use cases.

The `ChannelResolver` trait controls how Distributed DataFusion builds Arrow Flight clients backed by
[Tonic](https://github.com/hyperium/tonic) channels for worker URLs.

The default implementation connects to each URL, builds an Arrow Flight client, and caches it for reuse on
subsequent requests to the same URL.

## Providing your own ChannelResolver

For providing your own implementation, you'll need to take into account the following points:

- You will need to provide your own implementation in two places:
    - in the `SessionContext` that first initiates and plans your queries.
    - while instantiating the `Worker` with the `from_session_builder()` constructor.
- If building from scratch, ensure Arrow Flight clients are reused across requests rather than recreated each time.
- You can extend `DefaultChannelResolver` as a foundation for custom implementations. This automatically handles
  gRPC channel reuse.

```rust
#[derive(Clone)]
struct CustomChannelResolver;

#[async_trait]
impl ChannelResolver for CustomChannelResolver {
    async fn get_flight_client_for_url(
        &self,
        url: &Url,
    ) -> Result<FlightServiceClient<BoxCloneSyncChannel>, DataFusionError> {
        // Build a custom FlightServiceClient wrapped with tower 
        // layers or something similar.
        todo!()
    }
}

async fn main() {
    // Build a single instance for your application's lifetime
    // to enable Arrow Flight client reuse across queries.
    let channel_resolver = CustomChannelResolver;

    let state = SessionStateBuilder::new()
        // these two are mandatory.
        .with_distributed_worker_resolver(todo!())
        .with_physical_optimizer_rule(Arc::new(DistributedPhysicalOptimizerRule))
        // the CustomChannelResolver needs to be passed here once...
        .with_distributed_channel_resolver(channel_resolver.clone())
        .build();

    // ... and here for each query the Worker handles.
    let endpoint = Worker::from_session_builder(move |ctx: WorkerQueryContext| {
        let channel_resolver = channel_resolver.clone();
        async move {
            Ok(ctx.builder.with_distributed_channel_resolver(channel_resolver).build())
        }
    });
    Server::builder()
        .add_service(endpoint.into_flight_server())
        .serve(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8000))
        .await?;

    Ok(())
}
```