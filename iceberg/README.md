# DataFusion Distributed Iceberg

Read-only Apache Iceberg tables for DataFusion Distributed.

```rust
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use datafusion_distributed_iceberg::{IcebergExt, IcebergIntegrationOptions};

async fn example() -> datafusion::error::Result<()> {
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_iceberg_integration(IcebergIntegrationOptions::default())
        .build();
    let ctx = SessionContext::new_with_state(state);

    ctx.sql(
        "CREATE EXTERNAL TABLE taxi STORED AS ICEBERG \
     LOCATION 's3://warehouse/taxi/metadata/v1.metadata.json'",
    )
        .await?
        .collect()
        .await?;
    Ok(())
}
```

The default storage factory resolves `file://`, S3 (`s3://`, `s3a://`,
`s3n://`), and GCS (`gs://`, `gcs://`) URIs. Use
`IcebergIntegrationOptions` to supply custom storage or an Iceberg runtime.

By default, Iceberg discovers files progressively during execution. Set
`iceberg.plan_files=true` in the session to discover and retain the pruned file tasks
while building each physical scan, then deliver those same tasks to the reader at
execution. Retained metadata is charged to the query memory pool, without a
file-count limit. Eager planning uses metadata I/O up front; it does not change
reader settings, statistics, or task assignment. Both modes use the same dispatcher
and partition channels. Eager execution explicitly copies retained file descriptors
into owned work units; lazy execution moves newly discovered descriptors into them.

```text
Planning                          Execution
Eager: discover → retain files ──→ copy descriptors ─┐
Lazy:  retain scan definition ───→ discover files ──┤
                                                  ▼
                                   shared task-first dispatcher
                                                  ↓
                                         partition channels
                                                  ↓
                                      local or remote readers
```

Configure the session before creating the `SessionState`:

```rust
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionConfig;
use datafusion_distributed_iceberg::{IcebergConfig, IcebergExt, IcebergIntegrationOptions};

let config = SessionConfig::new()
    .with_option_extension(IcebergConfig::default())
    .set_bool("iceberg.plan_files", true);
let state = SessionStateBuilder::new()
    .with_default_features()
    .with_config(config)
    .with_iceberg_integration(IcebergIntegrationOptions::default())
    .build();
```

```bash
cargo test -p datafusion-distributed-iceberg --features integration
```

Local TPC-H Iceberg benchmarks use the separate
[`datafusion-distributed-iceberg-benchmarks`](benchmarks/README.md) package. This keeps benchmark
preparation and execution dependencies out of this read-only integration crate.
