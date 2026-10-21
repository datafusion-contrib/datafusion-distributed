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
`iceberg.eager_file_planning=true` in the session to discover and retain the pruned file tasks
while building each physical scan, then deliver those same tasks to the reader at
execution. Retained metadata is charged to the query memory pool, without a
file-count limit. Eager planning uses metadata I/O up front. Scan statistics and
desired distributed task counts use selected file work instead of snapshot totals;
reader settings and file assignment are unchanged. Both modes use the same dispatcher
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
    .set_bool("iceberg.eager_file_planning", true);
let state = SessionStateBuilder::new()
    .with_default_features()
    .with_config(config)
    .with_iceberg_integration(IcebergIntegrationOptions::default())
    .build();
```

For opt-in whole-file balancing, also set `iceberg.greedy_file_assignment=true`.
It requires `iceberg.eager_file_planning=true` and assigns the largest candidate-byte files
to the least-loaded *distributed task*; it does not choose a worker or split a
file. These bytes are scheduling estimates, not decoded memory or filtered rows.

## Selected-file statistics

With `iceberg.column_stats_enabled=true`, eager scans reconstruct column bytes,
min/max bounds, and null counts from the **selected files**. Missing metrics remain
unknown. Bounds are inexact; null counts also become inexact when predicates or
deletes may remove rows. Nested column bytes sum leaf sizes without inventing
parent bounds or null counts.

Snapshot distinct-count estimates are retained as an inexact fallback, capped by
selected-file row counts when known. They are not recomputed for the selected
files and can overestimate the filtered result. Sum statistics remain unknown.

Another manifest pass is needed because the pinned Iceberg API does not retain
column metrics in file tasks. Scan task counts continue to use whole-file bytes.

```bash
cargo test -p datafusion-distributed-iceberg --features integration
```

Local TPC-H Iceberg benchmarks use the separate
[`datafusion-distributed-iceberg-benchmarks`](benchmarks/README.md) package. This keeps benchmark
preparation and execution dependencies out of this read-only integration crate.
