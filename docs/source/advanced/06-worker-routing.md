# Routing tasks to workers

By default, the planner spreads a stage's tasks across the available workers
round-robin. When a task's data has a *home* — a worker that already holds it in a
cache or on local disk — you can send the task **there** instead, so it reads
locally instead of pulling data over the network.

Routing is the third method of the
[`TaskEstimator`](../user-guide/04-distribute-custom-plan.md) trait, `route_tasks`. It receives
a `TaskRoutingContext` (the head plan of the stage, the task count, and the active
`TaskContext`) and returns one worker URL per task, in task order:

```rust
fn route_tasks(&self, routing_ctx: &TaskRoutingContext<'_>) -> Result<Option<Vec<Url>>>;
```

- `Ok(Some(urls))` — task `i` is sent to `urls[i]`.
- `Ok(None)` — the default; keep the round-robin behaviour.

Because `route_tasks` is part of `TaskEstimator`, you implement it on the same
estimator you register with `with_distributed_task_estimator`.

Routing pairs naturally with `scale_up_leaf_node`: that decides *what* data task
`i` reads, and `route_tasks` decides *where* it runs. If your estimator returned a
`DistributedLeafExec`, its `variants()` are in task order too, so you can line up
each task's data with the worker that should serve it.

For a complete, runnable walkthrough — parquet files consistently routed to
workers by rendezvous hashing of the file path, so each worker can serve them from
an in-memory cache on repeat queries — see the
[custom_worker_url_routing.rs](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/examples/custom_worker_url_routing.rs)
example.
