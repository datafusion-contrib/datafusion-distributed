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
    ) -> Result<Box<dyn WorkerChannel>, DataFusionError>;
}
```

```{note}
`get_worker_client_for_url` is called on **every** gRPC request. Reuse clients
rather than reconnecting each time, or you'll open a fresh connection per request.
The easiest way is to build on `grpc::DefaultChannelResolver` (which caches channels),
or to use the `grpc::create_worker_client` helper.
```

## Custom channel construction

When only connection establishment needs customization, use
`DefaultChannelResolver::with_channel_factory` rather than implementing a new
cache. The async factory receives the original worker URL on a cache miss and
returns a `grpc::BoxCloneSyncChannel`:

```rust
use datafusion::common::{config_datafusion_err, exec_datafusion_err};
use datafusion_distributed::grpc::{BoxCloneSyncChannel, DefaultChannelResolver};
use std::time::Duration;
use tonic::transport::Endpoint;

let channel_resolver = DefaultChannelResolver::with_channel_factory(|url| async move {
    let endpoint = Endpoint::from_shared(url.to_string())
        .map_err(|err| config_datafusion_err!("Invalid worker endpoint: {err}"))?
        .connect_timeout(Duration::from_secs(10));
    let channel = endpoint.connect().await
        .map_err(|err| exec_datafusion_err!("Worker connection failed: {err}"))?;
    Ok(BoxCloneSyncChannel::new(channel))
});
```

The factory can supply a custom connector or tower service, including
application-owned TLS credentials. The resolver still checks service readiness,
shares in-flight construction and cached channels across clones, and evicts
failures so a later request can retry. Existing `default()` behavior is
unchanged. Connection and authentication policy belong to the application;
changing captured credentials does not invalidate existing cached channels.

See the runnable [custom channel factory example](../../../examples/custom_channel_factory.md).
Register the resolver on both coordinator and worker sessions as shown below.

## Providing your own

The simplest custom resolver wraps `grpc::DefaultChannelResolver`, delegating to it for
channel caching and only customizing what you need:

```rust
#[derive(Clone)]
struct CustomChannelResolver {
    inner: grpc::DefaultChannelResolver,
}

#[async_trait]
impl ChannelResolver for CustomChannelResolver {
    async fn get_worker_client_for_url(
        &self,
        url: &Url,
    ) -> Result<Box<dyn WorkerChannel>, DataFusionError> {
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
    inner: grpc::DefaultChannelResolver::default(),
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
