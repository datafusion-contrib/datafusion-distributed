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

## Decode-time runtime selection

`IcebergCodec::new` keeps a fixed runtime. For per-query selection, use
`IcebergCodec::new_with_runtime_resolver` (see its Rustdoc example).
Its closure receives the decoding `TaskContext` and can read worker-local session
extensions, for example an `iceberg::Runtime::new_with_split(&io, &query_cpu)`.
The caller controls CPU/I/O routing and must keep those Tokio runtimes alive
through execution. Resolver errors fail decoding; encoding and the wire format
are unchanged.

Register the codec with `with_distributed_user_codec` **before** calling
`with_iceberg_integration`, which adds a fixed-runtime codec. Keep codec order
consistent on coordinator and workers. Install runtime extensions in each worker
query's session config; runtime handles are not sent from the coordinator.
This only changes decoded scans, not coordinator-side table planning.

```bash
cargo test -p datafusion-distributed-iceberg
```

Local TPC-H Iceberg benchmarks use the separate
[`datafusion-distributed-iceberg-benchmarks`](benchmarks/README.md) package. This keeps benchmark
preparation and execution dependencies out of this read-only integration crate.
