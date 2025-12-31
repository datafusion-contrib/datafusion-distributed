# Building a ChannelResolver

This trait is optional, there's a sane default already in place that should work for most simple cases.

A `ChannelResolver` tells Distributed DataFusion how to build an Arrow Flight client baked by a
[Tonic](https://github.com/hyperium/tonic) channel given a worker URL.

There's a default implementation that simply connects to the given URL, builds the Arrow Flight client instance,
and caches it so that it gets reused upon reaching the URL again.

## Providing your own ChannelResolver

For providing your own implementation, you'll need to take into account the following points:

- You will need to provide your own implementation in two places:
    - in the `SessionContext` that first initiates and plans your queries.
    - while instantiating the `Worker` with the `from_session_builder()` constructor.
- If you decide to build it from scratch, make sure that Arrow Flight clients are reused across
  request rather than always building new ones.
- You can use this library's `DefaultChannelResolver` as a backbone for your own implementation.
  If you do that, gRPC channel reuse will be automatically managed for you.

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
    // Make sure you build just one for the whole lifetime of your application, 
    // as it needs to be able to reuse Arrow Flight client instances across 
    // different queries.
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