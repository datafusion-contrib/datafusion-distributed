# Routing tasks to workers

By default, the planner spreads a stage's tasks across the available workers
round-robin. When a task's data has a *home* — a worker that already holds it in a
cache or on local disk — you can send the task **there** instead, so it reads
locally instead of pulling data over the network.

Routing is handled by a registered `RouteTasksHandler`. It receives a
`RouteTasksEvent` (the head plan of the stage, the task count, and the active
`TaskContext`) and returns one worker URL per task, in task order:

```rust
fn route_tasks(event: RouteTasksEvent) -> Option<Result<RouteTasksEventResponse>>;
```

- `Some(Ok(RouteTasksEventResponse::new(urls)))` — task `i` is sent to `urls[i]`.
- `None` — defer to the next handler; the built-in fallback keeps the round-robin behaviour.

Register it on the coordinating session builder:

```rust
SessionStateBuilder::new()
    .with_distributed_route_tasks_handler(route_tasks);
```

Routing pairs naturally with `ScaleUpLeafNodeHandler`: that decides *what* data
task `i` reads, and `RouteTasksHandler` decides *where* it runs. If the leaf-scale
handler returns a `DistributedLeafExec`, its `variants()` are in task order too,
so you can line up each task's data with the worker that should serve it.

For a complete, runnable walkthrough — parquet files consistently routed to
workers by rendezvous hashing of the file path, so each worker can serve them from
an in-memory cache on repeat queries — see the
[custom_worker_url_routing.rs](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/examples/custom_worker_url_routing.rs)
example.
