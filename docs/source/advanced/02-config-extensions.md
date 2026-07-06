# Propagating config extensions

DataFusion lets you attach custom configuration to a session with a
`ConfigExtension` (usually declared with the `extensions_options!` macro). In a
distributed query those values have to reach every worker, so the code running
there — your custom `ExecutionPlan`s, UDFs, or optimizer rules — reads the same
configuration the coordinating context did.

Declare the extension as you normally would:

```rust
use datafusion::common::extensions_options;
use datafusion::config::ConfigExtension;

extensions_options! {
    pub struct CustomExtension {
        pub foo: String, default = "".to_string()
        pub bar: usize, default = 0
    }
}

impl ConfigExtension for CustomExtension {
    const PREFIX: &'static str = "custom";
}
```

On the coordinating context, register it with
`with_distributed_option_extension` instead of DataFusion's plain
`with_option_extension`. That both adds it to the session and marks it for
propagation — it will be serialized into gRPC metadata and sent along with every
Arrow Flight request to a worker:

```rust
let state = SessionStateBuilder::new()
    .with_default_features()
    .with_distributed_option_extension(CustomExtension::default())
    .with_distributed_planner()
    .build();
```

On each worker, rebuild the extension from the incoming gRPC metadata inside your
`WorkerSessionBuilder`, with `with_distributed_option_extension_from_headers`:

```rust
async fn build_state(ctx: WorkerQueryContext) -> Result<SessionState, DataFusionError> {
    Ok(ctx
        .builder
        .with_distributed_option_extension_from_headers::<CustomExtension>(&ctx.headers)?
        .build())
}
```

That call does two things: it reconstructs the extension from the values that
came over the wire, and it re-marks it for propagation — so it keeps flowing
across any further worker-to-worker hops.

```{note}
Extension values travel as strings in gRPC metadata, under the reserved
`x-datafusion-distributed-config-<prefix>` namespace (`extensions_options!`
handles the string round-trip for you). That prefix is off-limits for
[passthrough headers](01-passthrough-headers.md).
```
