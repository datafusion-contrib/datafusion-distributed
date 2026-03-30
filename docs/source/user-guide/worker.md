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
set by your infrastructure at runtime:

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
println!("version: {}", response.into_inner().version);
```

### Zero-downtime rolling deployments

During a rolling deployment, workers transition from version A to version B over time. To avoid
routing queries to workers running incompatible code, you can filter workers by version before
the planner sees them.

The recommended pattern is:

1. **Background polling loop**: Concurrently query **only workers whose version is still unknown**.
   Once a worker's version is resolved, it is never polled again. Clean up stale workers (e.g. disappeared from DNS).
   This can also happen within your implementation's discovery loop if that's desirable.
2. **Version-aware WorkerResolver**: Implement `WorkerResolver::get_urls()` to return only the
   compatible URLs from the resolved set.

**Note**: This example assumes that a version change corresponds to a new IP address
(e.g. Kubernetes pods). If your infrastructure reuses IPs across deploys (e.g. EC2 instances
restarting in-place), you will need to invalidate cached entries when the underlying process
restarts.

```rust
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use url::Url;
use datafusion::common::DataFusionError;
use datafusion_distributed::{
    ChannelResolver, WorkerResolver, get_worker_version,
};

struct VersionAwareWorkerResolver {
    compatible_urls: Arc<RwLock<Vec<Url>>>,
}

/// Polls only unresolved workers and caches their versions.
/// Workers that respond successfully are never polled again.
async fn background_version_resolver(
    all_worker_urls: Vec<Url>,
    local_version: String,
    compatible_urls: Arc<RwLock<Vec<Url>>>,
    channel_resolver: Arc<dyn ChannelResolver + Send + Sync>,
) {
    let mut version_cache: HashMap<Url, String> = HashMap::new();

    loop {
        let new_worker_urls: Vec<_> = all_worker_urls
            .iter()
            .filter(|url| !version_cache.contains_key(*url))
            .collect();

        let version_checks = futures::future::join_all(
            new_worker_urls
                .iter()
                .map(|url| get_worker_version(Arc::clone(&channel_resolver), url)),
        )
        .await;

        for (url, result) in new_worker_urls.iter().zip(version_checks) {
            if let Ok(version) = result {
                version_cache.insert((*url).clone(), version);
            }
        }

        let matching_urls = all_worker_urls
            .iter()
            .filter(|url| version_cache.get(*url).is_some_and(|v| v == &local_version))
            .cloned()
            .collect();
        *compatible_urls.write().unwrap() = matching_urls;

        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

impl VersionAwareWorkerResolver {
    fn start_version_filtering(
        all_worker_urls: Vec<Url>,
        expected_version: String,
        channel_resolver: Arc<dyn ChannelResolver + Send + Sync>,
    ) -> Self {
        let compatible_urls = Arc::new(RwLock::new(vec![]));

        tokio::spawn(background_version_resolver(
            all_worker_urls,
            expected_version,
            compatible_urls.clone(),
            channel_resolver,
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

// `all_worker_urls` and `channel_resolver` come from your service discovery.
let resolver = VersionAwareWorkerResolver::start_version_filtering(
    all_worker_urls,
    worker_version.clone(),
    channel_resolver,
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
Once a worker responds, its version is cached and not queried again. Only workers whose
version matches the expected version will appear in `get_urls()`.
