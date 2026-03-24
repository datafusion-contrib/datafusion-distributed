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

The version string is free-form, it can be any identifier that makes sense for your
deployment workflow. Workers that don't call `with_version()` report an empty string.

```rust
let worker = Worker::default()
    .with_version("2.0.0");
```

One way to avoid forgetting to bump the version on each deploy, is to derive it from an environment variable
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

1. **Background polling loop**: Concurrently query only workers whose version is still unknown.
   Once a worker's version is resolved, it is never polled again.
2. **Version-aware WorkerResolver**: Implement `WorkerResolver::get_urls()` to return only the
   compatible URLs from the resolved set.

```rust
use std::sync::{Arc, RwLock};
use std::time::Duration;
use url::Url;
use lru::LruCache;
use std::num::NonZeroUsize;
use futures::future::join_all;
use datafusion::common::DataFusionError;
use datafusion_distributed::{
    DefaultChannelResolver, GetWorkerInfoRequest, WorkerResolver, create_worker_client,
};

struct VersionAwareWorkerResolver {
    compatible_urls: Arc<RwLock<Vec<Url>>>,
}

/// Polls only unresolved workers and caches their versions permanently.
/// Once every worker has responded, the loop exits, no further network calls.
async fn background_version_resolver(
    known_urls: Vec<Url>,
    expected_version: String,
    compatible_urls: Arc<RwLock<Vec<Url>>>,
) {
    let Some(capacity) = NonZeroUsize::new(known_urls.len()) else {
        return; // No workers to resolve.
    };

    let channel_resolver = DefaultChannelResolver::default();
    // Cache of URL : version for workers that have already responded.
    let mut resolved = LruCache::new(capacity);

    loop {
        let unresolved: Vec<&Url> = known_urls
            .iter()
            .filter(|url| !resolved.contains(url))
            .collect();

        if unresolved.is_empty() {
            break;
        }

        // Fire all unresolved version checks concurrently.
        let futures: Vec<_> = unresolved
            .into_iter()
            .map(|url| {
                let channel_resolver = &channel_resolver;
                async move {
                    let channel = channel_resolver.get_channel(url).await.ok()?;
                    let mut client = create_worker_client(channel);
                    let resp = client
                        .get_worker_info(GetWorkerInfoRequest {})
                        .await
                        .ok()?;
                    Some((url.clone(), resp.into_inner().version))
                }
            })
            .collect();

        for (url, version) in join_all(futures).await.into_iter().flatten() {
            resolved.put(url, version);
        }

        let filtered = resolved
            .iter()
            .filter(|(_, v)| **v == expected_version)
            .map(|(url, _)| url.clone())
            .collect();
        *compatible_urls.write().unwrap() = filtered;

        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

impl VersionAwareWorkerResolver {
    fn start_version_filtering(
        known_urls: Vec<Url>,
        expected_version: String,
    ) -> Self {
        let compatible_urls = Arc::new(RwLock::new(vec![]));

        tokio::spawn(background_version_resolver(
            known_urls,
            expected_version,
            compatible_urls.clone(),
        ));

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

The coordinator's resolver concurrently polls only unresolved workers in the background.
Once a worker responds, its version is cached permanently and never queried again.
When every worker has responded, the background loop exits. Only workers whose version
matches the expected version will appear in `get_urls()`.
