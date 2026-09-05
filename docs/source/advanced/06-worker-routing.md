# Routing tasks to workers

By default, each distributed task is routed to an available worker. When a
task's data has a *home* (e.g, a worker that already holds it in a cache or on
local disk) a custom routing handler can send the task there instead. As routing
handlers are responsible for establishing connection to remote workers, they are
`async`.

Implement `RouteTaskHandler` and register it on the coordinating session. The
handler is called once per task with a `RouteTaskEvent`, providing contextual
information about what task-specialized plan is getting routed, the task
identifier to which its routed, etc..

Return `None` when the handler does not apply, allowing the next custom or
built-in handler to run. Otherwise, call `dialer.dial(url).await` and return the
response for the selected connection. The dialer may be called more than once,
sequentially or concurrently, to implement retries.

`dialer.dial(url).await` connects to a remote worker under the hood, and returns
the already established connection. If this call succeeds, it means that the
worker is in a good state for being part of the query.

```rust
use async_trait::async_trait;
use datafusion::common::{Result, exec_err};
use datafusion::execution::SessionStateBuilder;
use datafusion_distributed::{
    DistributedExt, RouteTaskEvent, RouteTaskEventResponse, RouteTaskHandler,
    ok_or_some_err
};

struct RetryRouteTaskHandler;

#[async_trait]
impl RouteTaskHandler for RetryRouteTaskHandler {
    async fn handle(&self, event: RouteTaskEvent<'_>) -> Option<Result<RouteTaskEventResponse>> {
        let urls = match ok_or_some_err!(event.worker_resolver.get_urls());
        if urls.is_empty() {
            return Some(exec_err!("no workers available"));
        }

        let start = event.task_key.task_number % urls.len();
        let mut last_error = None;
        for offset in 0..urls.len() {
            let url = urls[(start + offset) % urls.len()].clone();
            match event.dialer.dial(url).await {
                Ok(response) => return Some(Ok(response)),
                Err(error) => last_error = Some(error),
            }
        }
        Some(Err(last_error.expect("at least one worker was attempted")))
    }
}

SessionStateBuilder::new()
    .with_distributed_route_task_handler(RetryRouteTaskHandler);
```

Routing pairs naturally with `ScaleUpLeafNodeHandler`: that decides *what* data
task `i` reads, and `RouteTaskHandler` decides *where* that specialized task
runs. The task index can be used to keep a stable slot-to-worker mapping for
cache affinity.

For a complete, runnable walkthrough where parquet files consistently routed to
workers by hashing the file path, so each worker can serve them from an
in-memory cache on repeat queries, see the
[custom_worker_url_routing.rs](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/examples/custom_worker_url_routing.rs)
example.
