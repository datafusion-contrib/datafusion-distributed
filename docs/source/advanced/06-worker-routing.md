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

## Affinity for feed-backed scans

Register `AffinityRouteTaskHandler` to prefer stable data homes for providers that
implement `WorkUnitFeedProvider::task_affinity()`:

```rust
use datafusion::execution::SessionStateBuilder;
use datafusion_distributed::{AffinityRouteTaskHandler, DistributedExt};

let builder = SessionStateBuilder::new()
    .with_distributed_route_task_handler(AffinityRouteTaskHandler);
```

The coordinator collects task-specific hints before converting native feeds to
remote handles. Each distinct key elects a worker by rendezvous hashing; the
whole task prefers the worker owning the greatest total hinted weight. Duplicate
keys use their maximum weight. Worker URL order and query/task IDs do not change
homes; worker membership or an upgrade of the hash implementation can.

Use URLs identifying individual stable workers, not a load balancer. There is no
cache lookup or cache-specific dependency. This is best-effort whole-task placement,
not per-file routing or memory admission. No hints defer to other routing handlers;
classified connection failures retain normal retry and fallback behavior. A custom
load-aware handler can inspect `event.work_unit_affinity()` and reuse
`AffinityRouteTaskHandler::rank_workers()`.

### Affinity metrics

The coordinator exposes these metrics on `DistributedExec`:

| Metric | Meaning |
| --- | --- |
| `work_unit_affinity_routed_tasks` | Non-deferred routing attempts, not connection attempts. |
| `work_unit_affinity_preferred_placements` | Successful connections to the preferred URL, including same-worker retries. |
| `work_unit_affinity_fallback_placements` | Successful connections to a different URL. |
| `work_unit_affinity_routing_failures` | Resolver, configuration, terminal or exhausted-retry errors. |
| `work_unit_affinity_routing_duration` | Summed selection/connection time including retries; not query wall time. |
| `work_unit_affinity_hinted_objects` | Distinct positive-weight keys per routing attempt. |
| `work_unit_affinity_hinted_bytes` | Candidate bytes per attempt, deduplicated by key at maximum weight. |

No hints, zero-weight hints or no workers defer without recording these metrics.
Cancellation after an attempt starts records elapsed time but no placement or
failure. Bytes are not actual backend reads or cache hits; monitor those and
per-worker memory separately.

Routing pairs naturally with `ScaleUpLeafNodeHandler`: that decides *what* data
task `i` reads, and `RouteTaskHandler` decides *where* that specialized task
runs. The task index can be used to keep a stable slot-to-worker mapping for
cache affinity.

For a complete, runnable walkthrough where parquet files consistently routed to
workers by hashing the file path, so each worker can serve them from an
in-memory cache on repeat queries, see the
[custom_worker_url_routing.rs](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/examples/custom_worker_url_routing.rs)
example.
