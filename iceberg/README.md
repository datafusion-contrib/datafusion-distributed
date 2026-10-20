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
execution. `iceberg.planning_max_files` (default 100,000) bounds retained work;
exceeding it fails planning rather than returning incomplete results. Eager file
planning uses query memory and metadata I/O up front. When enabled, scan statistics
and desired distributed task counts use selected file work instead of snapshot totals;
reader settings and default file assignment are unchanged. For example, configure
the session before creating the `SessionState`:

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

For opt-in whole-file balancing, also set `iceberg.greedy_file_assignment=true`.
It requires `iceberg.plan_files=true` and assigns the largest candidate-byte files
to the least-loaded *distributed task*; it does not choose a worker or split a
file. These bytes are scheduling estimates, not decoded memory or filtered rows.

## Column sizes and join costing

With `iceberg.column_stats_enabled=true`, filtered preplanned scans may retain
compressed column-byte estimates from the **selected files**. Missing metrics remain
unknown, and filtered scans do not publish whole-snapshot bounds, null counts or
distinct counts. Another manifest pass is needed to recover column sizes with the
pinned Iceberg API. Scan task counts continue to use whole-file bytes.

To retain these estimates through grouping keys and inner hash joins, register
`ColumnByteStatisticsProvider` on the planning session:

```rust
use std::sync::Arc;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::operator_statistics::StatisticsRegistry;
use datafusion_distributed::ColumnByteStatisticsProvider;
use datafusion_distributed_iceberg::IcebergExt;

fn enable_column_size_costing(mut builder: SessionStateBuilder) -> SessionStateBuilder {
    builder.config().get_or_insert_default()
        .options_mut().optimizer.use_statistics_registry = true;
    builder
        .with_iceberg_column_stats_enabled(true)
        .with_statistics_registry(StatisticsRegistry::with_providers(vec![
            Arc::new(ColumnByteStatisticsProvider),
        ]))
}
```

This replaces the statistics registry; compose the provider with any existing
providers rather than discarding them. Compressed bytes are not measured Arrow or
hash-table memory. A different build-side choice is not proof of lower latency;
validate representative queries before enabling this in production.

## File-affinity routing

`iceberg.file_task_affinity=true` publishes assigned immutable data-file paths
and candidate-byte weights for each distributed task; it requires
`iceberg.plan_files=true` but not greedy assignment. Register
`AffinityRouteTaskHandler` on the coordinator's session to place whole tasks
near their file homes:

```rust
use datafusion::execution::SessionStateBuilder;
use datafusion_distributed::{AffinityRouteTaskHandler, DistributedExt};
use datafusion_distributed_iceberg::IcebergConfig;

fn enable_file_routing(builder: SessionStateBuilder) -> SessionStateBuilder {
    let mut iceberg = IcebergConfig::default();
    iceberg.plan_files = true;
    iceberg.greedy_file_assignment = true;
    iceberg.file_task_affinity = true;
    builder
        .with_distributed_option_extension(iceberg)
        .with_distributed_route_task_handler(AffinityRouteTaskHandler)
}
```

Compose this with other Iceberg session options rather than replacing them.
Use stable URLs identifying individual cache owners, not a load-balanced URL.
The policy does not inspect a cache or enforce memory/concurrency admission;
failover and mixed-file tasks can move work. Routing metrics describe placement,
**not** cache hits or backend reads. See [The life of an Iceberg file](docs/the-life-of-an-iceberg-file.md)
and the [worker-routing guide](../docs/source/advanced/06-worker-routing.md).

```bash
cargo test -p datafusion-distributed-iceberg --features integration
```

Local TPC-H Iceberg benchmarks use the separate
[`datafusion-distributed-iceberg-benchmarks`](benchmarks/README.md) package. This keeps benchmark
preparation and execution dependencies out of this read-only integration crate.
