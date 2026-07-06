# Resolving workers

A `WorkerResolver` tells the distributed planner where your workers are — it
returns the list of worker URLs the cluster can distribute work to. The planner
uses that list in two places:

- **During planning**, to size the cluster: a stage is never given more tasks
  than there are workers.
- **Right before execution**, to assign a worker URL to each task. This happens
  as late as possible so the URLs are as fresh as they can be.

The trait has a single method:

```rust
pub trait WorkerResolver: Any + Send + Sync {
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError>;
}
```

Register your implementation on the coordinating context's `SessionStateBuilder`.
It's only needed there — on the context that plans and initiates queries — not on
the workers:

```rust
let state = SessionStateBuilder::new()
    .with_default_features()
    .with_distributed_worker_resolver(resolver)
    .with_distributed_planner()
    .build();
```

How you implement `get_urls()` depends on whether your worker set is fixed or
changes while the process is running.

## A static list

If the workers are known up front and don't change — a fixed set of hosts, a
local dev cluster — return them directly:

```rust
#[derive(Clone)]
struct StaticWorkerResolver {
    urls: Vec<Url>,
}

impl WorkerResolver for StaticWorkerResolver {
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
        Ok(self.urls.clone())
    }
}
```

The [quick start](01-quick-start.md) uses exactly this over a list of localhost
URLs. It's the simplest option, but it can't track workers that come and go.

## A dynamic resolver

In most real deployments the set of workers changes over time — pods come and go
in Kubernetes, instances scale in and out behind an autoscaler. The resolver has
to reflect that.

The one hard constraint comes from the trait: **`get_urls()` is synchronous and
must return immediately.** Planning calls it repeatedly and cannot await, so you
can't query an API from inside it. The pattern is instead:

- keep the current worker list in shared state (an `RwLock`, an `ArcSwap`, ...),
- run a background task that refreshes that state from your infrastructure,
- have `get_urls()` return a cheap snapshot of it.

### Example: a Kubernetes resolver

Say your workers run as pods labelled `app=datafusion-worker`, each serving the
worker gRPC service on port `8000`. The resolver holds an `Arc<RwLock<Vec<Url>>>`;
a background task lists the matching pods every few seconds and swaps in the
fresh URLs. `get_urls()` just clones the latest snapshot:

```rust
use std::sync::{Arc, RwLock};
use std::time::Duration;
use k8s_openapi::api::core::v1::Pod;
use kube::api::ListParams;
use url::Url;

#[derive(Clone)]
struct KubernetesWorkerResolver {
    // Refreshed in the background; read synchronously by `get_urls`.
    urls: Arc<RwLock<Vec<Url>>>,
}

impl WorkerResolver for KubernetesWorkerResolver {
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
        // Cheap, non-blocking read of the latest snapshot.
        Ok(self.urls.read().expect("resolver lock poisoned").clone())
    }
}

impl KubernetesWorkerResolver {
    /// Spawns the background refresher and returns a resolver backed by it.
    fn spawn(namespace: String, label_selector: String, port: u16) -> Self {
        let urls = Arc::new(RwLock::new(Vec::new()));

        tokio::spawn({
            let urls = Arc::clone(&urls);
            async move {
                let client = kube::Client::try_default().await.expect("kube client");
                let pods: kube::Api<Pod> = kube::Api::namespaced(client, &namespace);
                let params = ListParams::default().labels(&label_selector);

                loop {
                    match pods.list(&params).await {
                        Ok(list) => {
                            let fresh = list
                                .into_iter()
                                // Skip pods that don't have an IP assigned yet.
                                .filter_map(|pod| pod.status?.pod_ip)
                                .filter_map(|ip| Url::parse(&format!("http://{ip}:{port}")).ok())
                                .collect::<Vec<_>>();
                            *urls.write().expect("resolver lock poisoned") = fresh;
                        }
                        Err(err) => eprintln!("failed to list worker pods: {err}"),
                    }
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        });

        Self { urls }
    }
}
```

Register it exactly like the static one — the background task is already running
by the time planning starts:

```rust
let resolver = KubernetesWorkerResolver::spawn(
    "default".into(),
    "app=datafusion-worker".into(),
    8000,
);

let state = SessionStateBuilder::new()
    .with_default_features()
    .with_distributed_worker_resolver(resolver)
    .with_distributed_planner()
    .build();
```

```{note}
This is deliberately minimal. In production you'd usually want to:

- **Watch instead of poll** — the [`kube`](https://kube.rs) runtime's `watcher`
  reacts to pod churn immediately rather than on a fixed interval.
- **Only include _ready_ workers** — filter on the pod's `Ready` condition, or
  read a headless `Service`'s `EndpointSlice`s (which already exclude not-ready
  endpoints) so you never hand the planner a pod that can't serve.
- **Read lock-free** — [`ArcSwap`](https://docs.rs/arc-swap) is a drop-in for the
  `RwLock<Vec<Url>>` if planning reads the list very frequently.

The pod running the resolver also needs RBAC permission to `list` (and `watch`)
pods in that namespace.
```

For a complete, running example against real infrastructure — discovering a
cluster of AWS EC2 machines by tag with the AWS Rust SDK — see
[benchmarks/cdk/bin/worker.rs](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/benchmarks/cdk/bin/worker.rs).
