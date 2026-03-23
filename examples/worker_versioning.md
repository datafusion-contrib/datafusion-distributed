# Worker Versioning

Workers expose a `GetWorkerInfo` gRPC endpoint that reports metadata including a
user-defined version string. This is useful during **rolling deployments** when
workers running different code versions coexist in the cluster and you need the
coordinator to route queries only to workers running compatible code.

## Quick start

### 1. Start versioned workers

Open separate terminals and start workers with different version strings:

```bash
# Terminal 1 — v1.0 worker
cargo run --example localhost_worker -- 8080 --version 1.0

# Terminal 2 — v2.0 worker
cargo run --example localhost_worker -- 8081 --version 2.0

# Terminal 3 — v2.0 worker
cargo run --example localhost_worker -- 8082 --version 2.0
```

The `--version` flag is optional. Workers started without it report an empty
version string.

### 2. Check what versions are running

```bash
cargo run --example worker_versioning -- --ports 8080,8081,8082
```

```
Worker at :8080 -> version 1.0
Worker at :8081 -> version 2.0
Worker at :8082 -> version 2.0
```

### 3. Run a query against only v2.0 workers

```bash
cargo run --example localhost_run -- \
  "SELECT city, count(*) FROM weather GROUP BY city" \
  --cluster-ports 8080,8081,8082 \
  --version 2.0
```

```
Excluding worker on port 8080 (version mismatch)
Using 2/3 workers matching version '2.0'

+--------+----------+
| city   | count(*) |
...
```

The `--version` flag queries each worker's version via `GetWorkerInfo` and
excludes any worker that doesn't match before the query is planned.

Without `--version`, all workers in `--cluster-ports` are used regardless of
what version they report.

## How it works

### Setting a version on a worker

Use `Worker::with_version()` when building the worker:

```rust
let worker = Worker::default().with_version("2.0");

Server::builder()
    .add_service(worker.into_worker_server())
    .serve(addr)
    .await?;
```

The version string is free-form — it can be a semver tag, a git SHA, or a build
number.

### Querying a worker's version

Use `DefaultChannelResolver` to obtain a cached gRPC channel and
`create_worker_client` to build a client:

```rust
let channel_resolver = DefaultChannelResolver::default();
let channel = channel_resolver.get_channel(&worker_url).await?;
let mut client = create_worker_client(channel);

let response = client.get_worker_info(GetWorkerInfoRequest {}).await?;
println!("version: {}", response.into_inner().version_number);
```

### Checking version compatibility

The `worker_has_version` helper wraps the above into a single boolean check.
Returns `false` if the worker is unreachable, returns an error, or reports a
different version:

```rust
async fn worker_has_version(
    channel_resolver: &DefaultChannelResolver,
    url: &Url,
    expected_version: &str,
) -> bool {
    let Ok(channel) = channel_resolver.get_channel(url).await else {
        return false;
    };
    let mut client = create_worker_client(channel);
    let Ok(response) = client.get_worker_info(GetWorkerInfoRequest {}).await else {
        return false;
    };
    response.into_inner().version_number == expected_version
}
```

## Production pattern: background polling

In the examples above, version filtering happens once at startup. In production,
workers come and go during rolling deployments, so you need to poll continuously.

`WorkerResolver::get_urls()` is **synchronous** — it's called during planning
and cannot do async I/O. The recommended pattern is a background task that
periodically polls each worker's version and writes a filtered URL list into
shared state. The resolver reads from that shared state.

```rust
// In a background task, periodically filter worker URLs by version:
let channel_resolver = DefaultChannelResolver::default();
let compatible_urls: Arc<RwLock<Vec<Url>>> = /* shared with WorkerResolver */;

tokio::spawn(async move {
    loop {
        let mut filtered = vec![];
        for url in &all_known_urls {
            if worker_has_version(&channel_resolver, url, "2.0").await {
                filtered.push(url.clone());
            }
        }
        *compatible_urls.write().unwrap() = filtered;
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
});
```

The `WorkerResolver` then just reads from the shared list:

```rust
struct VersionAwareWorkerResolver {
    compatible_urls: Arc<RwLock<Vec<Url>>>,
}

impl WorkerResolver for VersionAwareWorkerResolver {
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
        Ok(self.compatible_urls.read().unwrap().clone())
    }
}
```

Wire it into a session:

```rust
let state = SessionStateBuilder::new()
    .with_default_features()
    .with_distributed_worker_resolver(resolver)
    .with_physical_optimizer_rule(Arc::new(DistributedPhysicalOptimizerRule))
    .build();
```

The planner calls `get_urls()` during planning and will only see workers that
passed the version check on the last polling cycle.
