# Spawn a Worker

The `Worker` is a gRPC server that handles distributed query execution. Worker nodes
run these endpoints to receive execution plans, execute them, and stream results back.

## Overview

The `Worker` is the core worker component in Distributed DataFusion. It:

- Receives serialized execution plans via gRPC
- Deserializes plans using protobuf and user-provided codecs
- Executes plans using the local DataFusion runtime
- Streams results back as Arrow record batches through the gRPC interface

## Launching the Worker server

The default `Worker` implementation satisfies most basic use cases:

```rust
use datafusion_distributed::Worker;

async fn main() {
    let endpoint = Worker::default();

    Server::builder()
        .add_service(endpoint.into_worker_server())
        .serve(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8000))
        .await?;

    Ok(())
}
```

However, most DataFusion deployments include custom UDFs, execution nodes, or configuration options.

You'll need to tell the `Worker` how to build your DataFusion sessions:

```rust
async fn build_state(ctx: WorkerQueryContext) -> Result<SessionState, DataFusionError> {
    Ok(ctx
        .builder
        .with_scalar_functions(vec![your_custom_udf()])
        .build())
}

async fn main() {
    let endpoint = Worker::from_session_builder(build_sate);

    Server::builder()
        .add_service(endpoint.into_worker_server())
        .serve(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8000))
        .await?;

    Ok(())
}
```

## WorkerSessionBuilder

The `WorkerSessionBuilder` is a closure or type that implements:

```rust
#[async_trait]
pub trait WorkerSessionBuilder {
    async fn build_session_state(
        &self,
        ctx: WorkerQueryContext,
    ) -> Result<SessionState, DataFusionError>;
}
```

It receives a `WorkerQueryContext` containing:

- `SessionStateBuilder`: A pre-populated session state builder in which you can inject your custom stuff
- `headers`: HTTP headers from the incoming request (useful for passing metadata like authentication tokens or
  configuration)

## Serving the Endpoint

Convert the endpoint to a gRPC service and serve it:

```rust
use tonic::transport::Server;
use datafusion_distributed::Worker;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

async fn main() {
    let endpoint = Worker::default();

    Server::builder()
        .add_service(endpoint.into_worker_server())
        .serve(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080))
        .await?;
}
```

The `into_worker_server()` method builds a `WorkerServiceServer` ready to be added as a Tonic service.

## Worker Versioning

Workers expose a `GetWorkerInfo` gRPC endpoint that reports metadata about the running worker,
including a user-defined version string. This is useful during rolling deployments, when
workers running different code versions coexist in the cluster, the coordinator can route queries
only to workers running compatible code.

### Setting a version

Use the `Worker::with_version()` builder method to tag a worker with a version string.

The version string is free-form — it can be a semver tag, a git SHA, a build number, or any
identifier that makes sense for your deployment workflow. Workers that don't call `with_version()`
report an empty string.

```rust
let worker = Worker::default()
    .with_version("2.0.0");
```

One way to avoid forgetting to bump the version on each deploy, derive it from an environment variable
set by your CI/CD pipeline:

```rust
let worker = Worker::default()
    .with_version(std::env::var("COMMIT_HASH").unwrap_or_default());
```

### Querying a worker's version

From the coordinator, use `DefaultChannelResolver` to get a cached channel
and `create_worker_client` to build a client, then call `get_worker_info`:

```rust
use datafusion_distributed::{DefaultChannelResolver, GetWorkerInfoRequest, create_worker_client};

let channel_resolver = DefaultChannelResolver::default();
let channel = channel_resolver.get_channel(&worker_url).await?;
let mut client = create_worker_client(channel);

let response = client.get_worker_info(GetWorkerInfoRequest {}).await?;
println!("version: {}", response.into_inner().version_number);
```

### Zero-downtime rolling deployments

During a rolling deployment, workers transition from version A to version B over time. To avoid
routing queries to workers running incompatible code, you can filter workers by version before
the planner sees them.

The recommended pattern is:

1. **Background polling loop**: Periodically query each worker's version and maintain a filtered
   list of compatible URLs.
2. **Version-aware WorkerResolver**: Implement `WorkerResolver::get_urls()` to return only the
   compatible URLs from the filtered list.

```rust
use std::sync::{Arc, RwLock};
use std::time::Duration;
use url::Url;
use datafusion::common::DataFusionError;
use datafusion_distributed::{
    DefaultChannelResolver, GetWorkerInfoRequest, WorkerResolver, create_worker_client,
};

struct VersionAwareWorkerResolver {
    compatible_urls: Arc<RwLock<Vec<Url>>>,
}

impl VersionAwareWorkerResolver {
    /// Starts a background task that periodically polls all known worker URLs
    /// and filters them by the expected version.
    fn start_version_filtering(
        known_urls: Vec<Url>,
        expected_version: String,
    ) -> Self {
        let compatible_urls = Arc::new(RwLock::new(vec![]));
        let urls_handle = compatible_urls.clone();

        tokio::spawn(async move {
            let channel_resolver = DefaultChannelResolver::default();
            loop {
                let mut filtered = vec![];
                for url in &known_urls {
                    if let Ok(channel) = channel_resolver.get_channel(url).await {
                        let mut client = create_worker_client(channel);
                        if let Ok(resp) = client.get_worker_info(GetWorkerInfoRequest {}).await {
                            if resp.into_inner().version_number == expected_version {
                                filtered.push(url.clone());
                            }
                        }
                    }
                }
                *urls_handle.write().unwrap() = filtered;
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        });

        Self { compatible_urls }
    }
}

impl WorkerResolver for VersionAwareWorkerResolver {
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
        Ok(self.compatible_urls.read().unwrap().clone())
    }
}
```

With the resolver in place, wire it into the session and tag each worker with a version.

```rust
use datafusion::execution::SessionStateBuilder;
use datafusion_distributed::{DistributedExt, DistributedPhysicalOptimizerRule, Worker};

let worker_version = std::env::var("COMMIT_HASH").unwrap_or_default();

// `known_urls` comes from your service discovery.
let resolver = VersionAwareWorkerResolver::start_version_filtering(
    known_urls,
    worker_version.clone(),
);

let state = SessionStateBuilder::new()
    .with_default_features()
    .with_distributed_worker_resolver(resolver)
    .with_physical_optimizer_rule(Arc::new(DistributedPhysicalOptimizerRule))
    .build();

let ctx = SessionContext::from(state);

let worker = Worker::default().with_version(worker_version);

Server::builder()
    .add_service(worker.into_worker_server())
    .serve(addr)
    .await?;
```

The coordinator's resolver continuously polls all known URLs in the background.
Only workers that respond with the correct version will appear in `get_urls()`.
