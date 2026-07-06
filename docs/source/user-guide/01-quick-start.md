# Quick start

Distributed DataFusion is vanilla DataFusion, except that certain nodes execute
their children on remote machines and stream the data back in a zero-copy manner
over gRPC. The goal is an experience as close as possible to the DataFusion you
already know.

This library offers a wide set of customization options that gives you full
control over how your plans are distributed, but at the same time only a minimal
setup is needed for getting started:

## 1. Enable the distributed planner

Distributing queries is a one-line addition to DataFusion's
`SessionStateBuilder`:

```rust
let state = SessionStateBuilder::new()
    .with_default_features()
+   .with_distributed_planner()
    .build();

let ctx = SessionContext::from(state);
```

Calling `with_distributed_planner()` prompts the distributed planner to kick in
every query. This does not necessarily mean all queries will get distributed,
only queries that the distributed planner considers are worth distributing.

In this project, a `SessionContext` with the distributed planner enabled, is
typically referred as the "coordinating context", as it's in charge of planning
and distributing work across workers.

## 2. Tell it where your workers are

The planner needs to know which workers it can distribute work to. Provide that
by implementing the `WorkerResolver` trait — here, a simple resolver over a
fixed list of localhost URLs:

```rust
+ struct LocalhostWorkerResolver(Vec<Url>);

+ impl WorkerResolver for LocalhostWorkerResolver {
+   fn get_urls(&self) -> Result<Vec<Url>> { Ok(self.0.clone()) }
+ }

+ let resolver = LocalhostWorkerResolver(vec![
+   Url::parse("http://localhost:8000"),
+   Url::parse("http://localhost:8001"),
+   Url::parse("http://localhost:8002"),
+ ]);

let state = SessionStateBuilder::new()
    .with_default_features()
    .with_distributed_planner()
+   .with_distributed_worker_resolver(resolver)
    .build();

let ctx = SessionContext::from(state);
```

```{note}
The resolver here returns a fixed list to keep the example simple. In the real
world the set of workers is usually dynamic: a common pattern is a background
task that watches pod URLs from the Kubernetes API and stores them in shared
state (an `RwLock`, say), which `WorkerResolver::get_urls` then reads on each
call.
```

## 3. Run the worker gRPC servers separately

Each URL your resolver returns must point at a running worker. A worker is just
a gRPC service you mount onto a [Tonic](https://github.com/hyperium/tonic)
server and spawn on a port — add it to a server you already run, or stand one up
on its own:

```rust
+ #[tokio::main]
+ async fn main() -> Result<(), Box<dyn Error>> {
+     let worker = Worker::default();
+ 
+     let port: u16 = std::env::var("PORT")?.parse()?;
+     let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
+ 
+     tonic::Server::builder()
+         .add_service(worker.into_worker_server())
+         .serve(addr)
+         .await?;
+ 
+     Ok(())
+ }
```

That's it — with the workers running and the coordinator pointed at them,
queries against `ctx` are planned and executed distributed.

```{note}
An uncustomized default `Worker` is used here for simplicity. Real workers
usually need more — your own UDFs, custom `ExecutionPlan` codecs, a configured
`RuntimeEnv`, and so on. For that, build the worker with
`Worker::from_session_builder` for providing any customization.
```

