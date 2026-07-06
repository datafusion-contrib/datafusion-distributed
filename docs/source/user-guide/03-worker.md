# Spawning workers

A `Worker` is a gRPC server that executes a fragment of a distributed plan. The
coordinating context hands each worker a serialized piece of the plan; the worker
runs it and streams the results back. Concretely, a worker:

- receives serialized execution plans over gRPC,
- decodes them using protobuf and any user-provided codecs,
- executes them on the local DataFusion runtime,
- streams the results back as Arrow record batches.

A worker is just a [Tonic](https://github.com/hyperium/tonic) service you spawn
on a port, so you can run it on its own or mount it onto a gRPC server you already
have.

## The default worker

`Worker::default()` covers the basic case — no custom functions or nodes:

```rust
use datafusion_distributed::Worker;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let worker = Worker::default();

    Server::builder()
        .add_service(worker.into_worker_server())
        .serve(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8000))
        .await?;

    Ok(())
}
```

`into_worker_server()` builds a `WorkerServiceServer` ready to add as a Tonic
service.

## Customizing the session

Most deployments need more than the default — custom UDFs, execution nodes, or
config options. Build the worker with `Worker::from_session_builder`, passing a
`WorkerSessionBuilder` that sets up the `SessionState` each query runs in:

```rust
async fn build_state(ctx: WorkerQueryContext) -> Result<SessionState, DataFusionError> {
    Ok(ctx
        .builder
        .with_scalar_functions(vec![your_custom_udf()])
        .build())
}

let worker = Worker::from_session_builder(build_state);
```

A `WorkerSessionBuilder` is any closure or type implementing:

```rust
#[async_trait]
pub trait WorkerSessionBuilder {
    async fn build_session_state(
        &self,
        ctx: WorkerQueryContext,
    ) -> Result<SessionState, DataFusionError>;
}
```

It receives a `WorkerQueryContext` with two fields:

- `builder` — a pre-populated `SessionStateBuilder` where you inject your custom
  UDFs, codecs, optimizer rules, config extensions, and so on.
- `headers` — the HTTP headers from the incoming request, handy for metadata like
  authentication tokens or per-query configuration.

```{note}
A worker only *executes* fragments — it never plans queries. So it needs your
codecs (to decode any custom nodes) but **not** the distributed planner or
`WorkerResolver`, which are coordinating-context concerns. Registering codecs is
covered in
[Distribute a custom ExecutionPlan](04-distribute-custom-plan.md).
```

## Spawning strategies

Because a worker is just a Tonic service, you have some freedom in where it runs.

**A dedicated process.** The examples above — a standalone binary that serves
only the worker. Simplest to operate and to scale on its own.

**Alongside an existing gRPC service.** If you already run a Tonic server, add the
worker as one more service on the same server and port:

```rust
Server::builder()
    .add_service(MyServiceServer::new(my_service))
+   .add_service(worker.into_worker_server())
    .serve(addr)
    .await?;
```

**On its own port inside an existing service.** To keep the worker isolated from
your app's main endpoint — separate port, separate lifecycle — spawn a second
server in the same process. Co-locating it inside a DataFusion-based service also
lets the worker share resources with your app, such as the same `RuntimeEnv` and
object-store registrations (pass them via `Worker::from_session_builder` /
`with_runtime_env`).

## Going further

A few more worker capabilities have their own pages:

- [Plan hooks](../advanced/03-plan-hooks.md) — run worker-local rewrites on each decoded plan
  before it executes.
- [Worker versioning](../advanced/07-worker-versioning.md) — tag workers with a version and
  route queries only to compatible workers during rolling deployments.
- [Propagating config extensions](../advanced/02-config-extensions.md) — make your custom
  `ConfigExtension`s readable on every worker.
- [Passthrough headers](../advanced/01-passthrough-headers.md) — forward arbitrary request
  headers across worker hops.
