# Passthrough headers

Sometimes you need to forward an arbitrary HTTP header — a request id, a priority
hint, a tenant tag — from the coordinating context down to the workers, untouched.
`with_distributed_passthrough_headers` takes a `HeaderMap` whose entries are added
to every outgoing Arrow Flight request to a worker:

```rust
use http::HeaderMap;

let mut passthrough = HeaderMap::new();
passthrough.insert("x-request-id", request_id.parse().unwrap());
passthrough.insert("x-priority", "high".parse().unwrap());

let state = SessionStateBuilder::new()
    .with_default_features()
    .with_distributed_passthrough_headers(passthrough)?
    .with_distributed_planner()
    .build();
```

Header names starting with `x-datafusion-distributed-config-` are rejected — that
prefix is reserved for [config extension propagation](02-config-extensions.md).

## Forwarding across worker jumps

By default these headers travel a single hop: the coordinating context attaches
them to the requests it makes to the first workers it talks to. A multi-stage
query, though, has workers calling other workers — and those second-hop requests
won't carry the headers unless each worker re-injects them.

To keep a header flowing, read it from the incoming request in your
`WorkerSessionBuilder` and set it again on the worker's session:

```rust
async fn build_state(ctx: WorkerQueryContext) -> Result<SessionState, DataFusionError> {
    // Forward just the headers you care about onto this worker's outgoing calls.
    let mut forwarded = HeaderMap::new();
    if let Some(value) = ctx.headers.get("x-request-id") {
        forwarded.insert("x-request-id", value.clone());
    }

    Ok(ctx
        .builder
        .with_distributed_passthrough_headers(forwarded)?
        .build())
}
```

Every worker that forwards the headers this way extends their reach one more hop,
so re-injecting on each worker propagates them across the whole plan.

```{note}
`ctx.headers` also contains the internal `x-datafusion-distributed-config-*`
entries used for [config extensions](02-config-extensions.md). Select only your
own headers (as above) rather than forwarding the whole map — both to avoid
leaking internal metadata and because `with_distributed_passthrough_headers`
rejects that reserved prefix.
```
