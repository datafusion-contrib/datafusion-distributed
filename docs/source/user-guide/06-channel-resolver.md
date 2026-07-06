# Custom worker connections

A `ChannelResolver` controls how connections to workers are opened. It turns a
worker URL into a Worker gRPC client backed by a
[Tonic](https://github.com/hyperium/tonic) channel — used both by the
coordinating context and by workers when they call other workers mid-query.

It is **optional**. A default implementation already connects to each URL, builds
a client, and caches it for reuse on later requests to the same URL — enough for
most setups. You only implement your own to customize the connection: wrap the
client in [tower](https://github.com/tower-rs/tower) layers (auth, retries,
tracing), tune timeouts or message-size limits, or run it on a dedicated I/O
runtime.

The trait has a single async method:

```rust
#[async_trait]
pub trait ChannelResolver {
    async fn get_worker_client_for_url(
        &self,
        url: &Url,
    ) -> Result<WorkerServiceClient<BoxCloneSyncChannel>, DataFusionError>;
}
```

```{note}
`get_worker_client_for_url` is called on **every** gRPC request. Reuse clients
rather than reconnecting each time, or you'll open a fresh connection per request.
The easiest way is to build on `DefaultChannelResolver` (which caches channels),
or to use the `create_worker_client` helper.
```

## Providing your own

The simplest custom resolver wraps `DefaultChannelResolver`, delegating to it for
channel caching and only customizing what you need:

```rust
#[derive(Clone)]
struct CustomChannelResolver {
    inner: DefaultChannelResolver,
}

#[async_trait]
impl ChannelResolver for CustomChannelResolver {
    async fn get_worker_client_for_url(
        &self,
        url: &Url,
    ) -> Result<WorkerServiceClient<BoxCloneSyncChannel>, DataFusionError> {
        // Delegate to the default (cached channels), or build your own client
        // here — e.g. wrapped in tower layers, or on a custom runtime.
        self.inner.get_worker_client_for_url(url).await
    }
}
```

Build a single instance for your application's lifetime (so clients are reused),
and register it in **two** places — on the coordinating context that plans
queries, and on every worker, since workers open connections to other workers
too:

```rust
let channel_resolver = CustomChannelResolver {
    inner: DefaultChannelResolver::default(),
};

// On the coordinating context:
let state = SessionStateBuilder::new()
    .with_default_features()
    .with_distributed_worker_resolver(/* ... */)
    .with_distributed_planner()
    .with_distributed_channel_resolver(channel_resolver.clone())
    .build();

// On every worker, via its session builder:
let worker = Worker::from_session_builder(move |ctx: WorkerQueryContext| {
    let channel_resolver = channel_resolver.clone();
    async move {
        Ok(ctx
            .builder
            .with_distributed_channel_resolver(channel_resolver)
            .build())
    }
});
```
